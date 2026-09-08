package live

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pion/webrtc/v4"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

func mediaClientOffer(t *testing.T) string {
	t.Helper()
	client := localMediaPeer(t, localMediaAPI(t))
	localMediaTrack(t, client)
	if _, err := client.CreateDataChannel(realtimeDataChannelLabel, nil); err != nil {
		t.Fatal(err)
	}
	return localMediaDescription(t, client, true)
}

func mediaFixtureAnswer(t *testing.T, upstream *webrtc.PeerConnection, r *http.Request) (string, bool) {
	t.Helper()
	var body struct {
		SDP string `json:"sdp"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		t.Error(err)
		return "", false
	}
	if err := upstream.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeOffer, SDP: body.SDP}); err != nil {
		t.Error(err)
		return "", false
	}
	gathered := webrtc.GatheringCompletePromise(upstream)
	answer, err := upstream.CreateAnswer(nil)
	if err != nil {
		t.Error(err)
		return "", false
	}
	if err := upstream.SetLocalDescription(answer); err != nil {
		t.Error(err)
		return "", false
	}
	select {
	case <-gathered:
	case <-r.Context().Done():
		return "", false
	case <-time.After(3 * time.Second):
		t.Error("local answer gathering did not finish")
		return "", false
	}
	return upstream.LocalDescription().SDP, true
}

func TestLiveMediaAllocatedSetupFailuresRollbackExactlyOnce(t *testing.T) {
	for _, mode := range []string{"bad-answer", "disabled", "disable-reenable", "cancelled", "delivery", "registry-capacity"} {
		t.Run(mode, func(t *testing.T) {
			offer := mediaClientOffer(t)
			upstream := localMediaPeer(t, localMediaAPI(t))
			localMediaTrack(t, upstream)
			upstream.OnDataChannel(func(*webrtc.DataChannel) {})
			var creates, hangups atomic.Int32
			cleaned := make(chan struct{}, 2)
			cfg := mediaTestHandlerConfig()
			h, _, e, _ := newLiveCallsFixture(t, cfg, func(w http.ResponseWriter, r *http.Request) {
				if strings.HasSuffix(r.URL.Path, "/hangup") {
					if r.URL.Path != "/v1/realtime/calls/call_incomplete/hangup" || r.Header.Get("Authorization") != "Bearer fixture-oauth" {
						t.Error("rollback changed call or credential")
					}
					hangups.Add(1)
					w.WriteHeader(204)
					cleaned <- struct{}{}
					return
				}
				creates.Add(1)
				answer, ok := mediaFixtureAnswer(t, upstream, r)
				if !ok {
					w.WriteHeader(500)
					return
				}
				if mode == "bad-answer" {
					answer = "invalid-answer"
				}
				w.Header().Set("Location", "/v1/realtime/calls/call_incomplete")
				w.WriteHeader(201)
				_, _ = w.Write([]byte(answer))
			})
			useLocalHandlerMedia(t, h)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			e.after = func(r *http.Request, _ *http.Response) {
				if strings.HasSuffix(r.URL.Path, "/hangup") {
					return
				}
				switch mode {
				case "disabled":
					h.UpdateConfig(&config.Config{})
				case "disable-reenable":
					h.UpdateConfig(&config.Config{})
					h.UpdateConfig(cfg)
				case "cancelled":
					cancel()
				}
			}
			if mode == "registry-capacity" {
				h.calls.capacity = 0
			}
			c, w := liveHandlerRequest(ctx, "fixture-caller", "")
			c.Set("accessProvider", "fixture-access")
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/live", strings.NewReader(offer)).WithContext(ctx)
			c.Request.Header.Set("Content-Type", "application/sdp")
			if mode == "delivery" {
				c.Writer = failedCallWriter{c.Writer}
			}
			h.HandleCall(c)
			want := http.StatusServiceUnavailable
			switch mode {
			case "bad-answer":
				want = 502
			case "cancelled":
				want = 499
			case "delivery":
				want = 201
			}
			if c.Writer.Status() != want {
				t.Fatalf("failed setup status=%d want=%d", c.Writer.Status(), want)
			}
			if strings.HasPrefix(mode, "disable") && !strings.Contains(w.Body.String(), liveDisabledCode) {
				t.Fatal("disabled generation lost its public error")
			}
			receiveMediaValue(t, cleaned)
			assertMediaHandlerReleased(t, h)
			if creates.Load() != 1 || hangups.Load() != 1 {
				t.Fatal("failed setup added an upstream attempt or duplicated cleanup")
			}
		})
	}
}

func TestLiveMediaPendingHTTPCancelsAndReleasesBeforeShutdownReturns(t *testing.T) {
	for _, shutdown := range []bool{false, true} {
		t.Run(map[bool]string{false: "disable", true: "shutdown"}[shutdown], func(t *testing.T) {
			offer := mediaClientOffer(t)
			started, cancelled := make(chan struct{}), make(chan struct{})
			h, _, _, _ := newLiveCallsFixture(t, mediaTestHandlerConfig(), func(_ http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				close(started)
				<-r.Context().Done()
				close(cancelled)
			})
			useLocalHandlerMedia(t, h)
			finished := make(chan *httptest.ResponseRecorder, 1)
			go func() { finished <- callHandlerRequest(t, h, "/v1/live", "application/sdp", strings.NewReader(offer)) }()
			receiveMediaValue(t, started)
			if shutdown {
				h.Close()
				assertMediaHandlerReleased(t, h)
			} else {
				h.UpdateConfig(&config.Config{})
			}
			receiveMediaValue(t, cancelled)
			w := receiveMediaValue(t, finished)
			if w.Code != 503 || !strings.Contains(w.Body.String(), liveDisabledCode) {
				t.Fatal("pending media setup escaped disabled admission")
			}
			assertMediaHandlerReleased(t, h)
		})
	}
}

func TestLiveMediaCredentialRetirementReleasesEstablishedCall(t *testing.T) {
	client := localMediaPeer(t, localMediaAPI(t))
	localMediaTrack(t, client)
	channel, err := client.CreateDataChannel(realtimeDataChannelLabel, nil)
	if err != nil {
		t.Fatal(err)
	}
	opened := make(chan struct{})
	channel.OnOpen(func() { close(opened) })
	upstream := localMediaPeer(t, localMediaAPI(t))
	localMediaTrack(t, upstream)
	remoteOpened := make(chan struct{})
	upstream.OnDataChannel(func(channel *webrtc.DataChannel) {
		channel.OnOpen(func() { close(remoteOpened) })
	})
	var attempts atomic.Int32
	h, manager, _, authID := newLiveCallsFixture(t, mediaTestHandlerConfig(), func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		answer, ok := mediaFixtureAnswer(t, upstream, r)
		if !ok {
			w.WriteHeader(500)
			return
		}
		w.Header().Set("Location", "/v1/realtime/calls/call_retired")
		_, _ = w.Write([]byte(answer))
	})
	useLocalHandlerMedia(t, h)
	w := callHandlerRequest(t, h, "/v1/live", "application/sdp", strings.NewReader(localMediaDescription(t, client, true)))
	if w.Code != 200 {
		t.Fatalf("media setup status=%d", w.Code)
	}
	if err := client.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeAnswer, SDP: w.Body.String()}); err != nil {
		t.Fatal(err)
	}
	receiveMediaValue(t, opened)
	receiveMediaValue(t, remoteOpened)
	if err := manager.Delete(t.Context(), authID); err != nil {
		t.Fatal(err)
	}
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		h.mediaLimiter.mu.Lock()
		active := h.mediaLimiter.active
		h.mediaLimiter.mu.Unlock()
		h.calls.mu.Lock()
		count := len(h.calls.entries)
		h.calls.mu.Unlock()
		if active == 0 && count == 0 {
			break
		}
		select {
		case <-ticker.C:
		case <-deadline.C:
			t.Fatal("credential retirement retained media resources")
		}
	}
	if attempts.Load() != 1 {
		t.Fatal("retired credential was reused for a new network operation")
	}
}
