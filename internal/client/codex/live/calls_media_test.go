package live

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/pion/webrtc/v4"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

func mediaTestHandlerConfig() *config.Config {
	cfg := &config.Config{}
	cfg.Codex.LiveEnabled = true
	cfg.Codex.LiveMediaRelay = config.CodexLiveMediaRelayConfig{Enabled: true, MaxSessions: 1}
	return cfg
}

func useLocalHandlerMedia(t *testing.T, h *Handler) {
	t.Helper()
	factory := h.runtime.Load().media
	if factory == nil {
		t.Fatal("media fixture was not enabled")
	}
	factory.downstreamAPI, factory.upstreamAPI = localMediaAPI(t), localMediaAPI(t)
}

func assertMediaHandlerReleased(t *testing.T, h *Handler) {
	t.Helper()
	h.calls.mu.Lock()
	count := len(h.calls.entries)
	h.calls.mu.Unlock()
	h.mediaLimiter.mu.Lock()
	active := h.mediaLimiter.active
	h.mediaLimiter.mu.Unlock()
	if count != 0 || active != 0 {
		t.Fatalf("media resources remain: calls=%d slots=%d", count, active)
	}
}

func TestLiveMediaFlagCombinationsKeepOldPassthroughAndRejectDisabledWork(t *testing.T) {
	for _, liveEnabled := range []bool{false, true} {
		for _, mediaEnabled := range []bool{false, true} {
			t.Run(fmt.Sprintf("live=%v/media=%v", liveEnabled, mediaEnabled), func(t *testing.T) {
				cfg := mediaTestHandlerConfig()
				cfg.Codex.LiveEnabled, cfg.Codex.LiveMediaRelay.Enabled = liveEnabled, mediaEnabled
				var attempts atomic.Int32
				h, _, _, _ := newLiveCallsFixture(t, cfg, func(w http.ResponseWriter, _ *http.Request) {
					attempts.Add(1)
					w.Header().Set("Location", "/v1/realtime/calls/call_flags")
					_, _ = w.Write([]byte("legacy-answer"))
				})
				w := callHandlerRequest(t, h, "/v1/live", "application/sdp", strings.NewReader("v=0"))
				if !liveEnabled {
					if w.Code != 503 || !strings.Contains(w.Body.String(), liveDisabledCode) || attempts.Load() != 0 {
						t.Fatal("media mode bypassed disabled Live")
					}
				} else if mediaEnabled {
					if w.Code != 400 || attempts.Load() != 0 {
						t.Fatal("invalid media offer reached upstream")
					}
				} else if w.Code != 200 || w.Body.String() != "legacy-answer" || attempts.Load() != 1 {
					t.Fatal("media disable changed existing SDP passthrough")
				}
				h.Close()
				assertMediaHandlerReleased(t, h)
			})
		}
	}
}

func TestLiveCallMediaTransfersAfterHotDisableAndCleansUp(t *testing.T) {
	for _, end := range []string{"hangup", "channel-close", "shutdown"} {
		t.Run(end, func(t *testing.T) {
			client := localMediaPeer(t, localMediaAPI(t))
			clientTrack := localMediaTrack(t, client)
			clientAudio := observeMediaAudio(client)
			clientData, err := client.CreateDataChannel(realtimeDataChannelLabel, nil)
			if err != nil {
				t.Fatal(err)
			}
			opened, clientMessages := make(chan struct{}), make(chan webrtc.DataChannelMessage, 1)
			clientData.OnOpen(func() { close(opened) })
			clientData.OnMessage(func(message webrtc.DataChannelMessage) { clientMessages <- message })
			clientOffer := localMediaDescription(t, client, true)
			upstream := localMediaPeer(t, localMediaAPI(t))
			upstreamTrack := localMediaTrack(t, upstream)
			upstreamAudio := observeMediaAudio(upstream)
			remoteData, remoteMessages := make(chan *webrtc.DataChannel, 1), make(chan webrtc.DataChannelMessage, 1)
			upstream.OnDataChannel(func(channel *webrtc.DataChannel) {
				channel.OnOpen(func() { remoteData <- channel })
				channel.OnMessage(func(message webrtc.DataChannelMessage) { remoteMessages <- message })
			})
			var creations, hangups atomic.Int32
			ended := make(chan struct{}, 2)
			h, _, _, _ := newLiveCallsFixture(t, mediaTestHandlerConfig(), func(w http.ResponseWriter, r *http.Request) {
				if strings.HasSuffix(r.URL.Path, "/hangup") {
					if r.Header.Get("Authorization") != "Bearer fixture-oauth" {
						t.Error("media cleanup changed credentials")
					}
					hangups.Add(1)
					w.WriteHeader(204)
					ended <- struct{}{}
					return
				}
				creations.Add(1)
				body, _ := io.ReadAll(r.Body)
				var payload map[string]json.RawMessage
				if err := json.Unmarshal(body, &payload); err != nil {
					t.Error(err)
					w.WriteHeader(400)
					return
				}
				var offer string
				if err := json.Unmarshal(payload["sdp"], &offer); err != nil || offer == clientOffer {
					t.Error("client SDP was not replaced by a relay offer")
					w.WriteHeader(400)
					return
				}
				if !bytes.Contains(payload["session"], []byte(`"model":"gpt-live-1-codex"`)) || !bytes.Contains(payload["session"], []byte("90071992547409931234")) || string(payload["custom"]) != `{"sdp":"unchanged"}` {
					t.Error("media setup changed native session fields")
				}
				if err := upstream.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeOffer, SDP: offer}); err != nil {
					t.Error(err)
					w.WriteHeader(400)
					return
				}
				answer := localMediaDescription(t, upstream, false)
				w.Header().Set("Location", "/v1/realtime/calls/call_media")
				w.Header().Set("Content-Type", "text/plain")
				w.WriteHeader(201)
				_, _ = w.Write([]byte(answer))
			})
			useLocalHandlerMedia(t, h)
			body, _ := json.Marshal(map[string]any{"sdp": clientOffer, "session": json.RawMessage(`{"model":"gpt-realtime","future":90071992547409931234}`), "custom": json.RawMessage(`{"sdp":"unchanged"}`)})
			w := callHandlerRequest(t, h, "/v1/realtime/calls", "application/json", bytes.NewReader(body))
			if w.Code != 201 || w.Header().Get("Content-Type") != "application/sdp" {
				t.Fatalf("media call status=%d", w.Code)
			}
			if err := client.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeAnswer, SDP: w.Body.String()}); err != nil {
				t.Fatal(err)
			}
			receiveMediaValue(t, opened)
			remote := receiveMediaValue(t, remoteData)
			// The HTTP request has ended. Disable both flags before sending media.
			h.UpdateConfig(&config.Config{})
			if err := clientData.SendText("after-disable"); err != nil {
				t.Fatal(err)
			}
			if got := receiveMediaValue(t, remoteMessages); !got.IsString || string(got.Data) != "after-disable" {
				t.Fatal("hot disable interrupted existing events")
			}
			if err := remote.Send([]byte{0, 255}); err != nil {
				t.Fatal(err)
			}
			if got := receiveMediaValue(t, clientMessages); got.IsString || string(got.Data) != string([]byte{0, 255}) {
				t.Fatal("hot disable interrupted reverse events")
			}
			sendMediaAudio(t, clientTrack, upstreamAudio, []byte{0xf8, 0xff, 0xfe})
			sendMediaAudio(t, upstreamTrack, clientAudio, []byte{0xf8, 0xfe, 0xfd})
			if w := callHandlerRequest(t, h, "/v1/live", "application/sdp", strings.NewReader(clientOffer)); w.Code != 503 || !strings.Contains(w.Body.String(), liveDisabledCode) {
				t.Fatal("new media call bypassed disable")
			}
			switch end {
			case "hangup":
				c, w := hangupHandlerContext(t, "call_media", "fixture-caller")
				h.HandleHangup(c)
				if w.Code != 204 {
					t.Fatalf("media hangup status=%d", w.Code)
				}
			case "channel-close":
				if err := clientData.Close(); err != nil {
					t.Fatal(err)
				}
				receiveMediaValue(t, ended)
			case "shutdown":
				h.Close()
			}
			assertMediaHandlerReleased(t, h)
			wantHangups := int32(1)
			if end == "shutdown" {
				wantHangups = 0
			}
			if creations.Load() != 1 || hangups.Load() != wantHangups {
				t.Fatal("media cleanup retried or reselected")
			}
		})
	}
}

func TestLiveMediaInvalidConfigurationIsLocalAndCapacityRecovers(t *testing.T) {
	cfg := mediaTestHandlerConfig()
	cfg.Codex.LiveMediaRelay.MaxSessions = -1
	h := NewHandler(cfg, nil)
	defer h.Close()
	w := callHandlerRequest(t, h, "/v1/live", "application/sdp", strings.NewReader("v=0"))
	if w.Code != 503 || !strings.Contains(w.Body.String(), "realtime_media_unavailable") {
		t.Fatal("invalid media configuration fell through to credential selection")
	}
	assertMediaHandlerReleased(t, h)
	var attempts atomic.Int32
	h, _, _, _ = newLiveCallsFixture(t, mediaTestHandlerConfig(), func(w http.ResponseWriter, _ *http.Request) { attempts.Add(1); w.WriteHeader(403) })
	useLocalHandlerMedia(t, h)
	client := localMediaPeer(t, localMediaAPI(t))
	if _, err := client.CreateDataChannel(realtimeDataChannelLabel, nil); err != nil {
		t.Fatal(err)
	}
	offer := localMediaDescription(t, client, true)
	release, ok := h.mediaLimiter.acquire(1)
	if !ok {
		t.Fatal("fixture could not occupy capacity")
	}
	w = callHandlerRequest(t, h, "/v1/live", "application/sdp", strings.NewReader(offer))
	release()
	if w.Code != 503 || !strings.Contains(w.Body.String(), "realtime_media_capacity") || attempts.Load() != 0 {
		t.Fatal("capacity rejection contacted upstream")
	}
	w = callHandlerRequest(t, h, "/v1/live", "application/sdp", strings.NewReader(offer))
	if w.Code != 403 || attempts.Load() != 1 {
		t.Fatal("released capacity was not restored or upstream rejection retried")
	}
	assertMediaHandlerReleased(t, h)
}
