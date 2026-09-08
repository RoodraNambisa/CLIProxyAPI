package live

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/pion/sdp/v3"
	"github.com/pion/webrtc/v4"
)

func mediaPublicTCPAnswer(t *testing.T, answer string) string {
	t.Helper()
	var description sdp.SessionDescription
	if err := description.UnmarshalString(answer); err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, media := range description.MediaDescriptions {
		for i := range media.Attributes {
			attribute := &media.Attributes[i]
			if !attribute.IsICECandidate() {
				continue
			}
			fields := strings.Fields(attribute.Value)
			if len(fields) < 8 || !strings.EqualFold(fields[2], "tcp") || !strings.Contains(attribute.Value, "tcptype passive") {
				continue
			}
			fields[4], fields[5] = "20.42.0.20", "443"
			attribute.Value = strings.Join(fields, " ")
			count++
		}
	}
	if count == 0 {
		t.Fatal("local fixture did not gather passive TCP")
	}
	encoded, err := description.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	return string(encoded)
}

func TestMediaSessionUsesHTTPProxyForPionAudioAndEvents(t *testing.T) {
	for _, reject := range []bool{false, true} {
		t.Run(map[bool]string{false: "bridge", true: "proxy-rejection"}[reject], func(t *testing.T) {
			listener, err := net.Listen("tcp4", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			mux := webrtc.NewICETCPMux(nil, listener, 8)
			defer func() { _ = mux.Close() }()
			settings := webrtc.SettingEngine{}
			settings.SetNetworkTypes([]webrtc.NetworkType{webrtc.NetworkTypeTCP4})
			settings.SetIncludeLoopbackCandidate(true)
			settings.SetIPFilter(func(ip net.IP) bool { return ip != nil && ip.IsLoopback() })
			settings.SetICETCPMux(mux)
			upstream := localMediaPeer(t, webrtc.NewAPI(webrtc.WithSettingEngine(settings)))
			upstreamTrack := localMediaTrack(t, upstream)
			upstreamAudio := observeMediaAudio(upstream)
			upstreamData := make(chan *webrtc.DataChannel, 1)
			messages := make(chan webrtc.DataChannelMessage, 1)
			upstream.OnDataChannel(func(channel *webrtc.DataChannel) {
				channel.OnOpen(func() { upstreamData <- channel })
				channel.OnMessage(func(message webrtc.DataChannelMessage) { messages <- message })
			})
			var requests atomic.Int32
			proxyServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				if r.Method != http.MethodConnect || r.Host != "20.42.0.20:443" {
					t.Error("media proxy request escaped its fixed target")
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				if reject {
					w.WriteHeader(http.StatusBadGateway)
					return
				}
				// The asserted public target is redirected only inside this fixture.
				remote, err := (&net.Dialer{}).DialContext(r.Context(), "tcp4", listener.Addr().String())
				if err != nil {
					t.Error(err)
					w.WriteHeader(http.StatusBadGateway)
					return
				}
				defer func() { _ = remote.Close() }()
				local, buffered, err := w.(http.Hijacker).Hijack()
				if err != nil {
					t.Error(err)
					return
				}
				defer func() { _ = local.Close() }()
				if _, err := buffered.WriteString("HTTP/1.1 200 Connection Established\r\n\r\n"); err != nil {
					return
				}
				if err := buffered.Flush(); err != nil {
					return
				}
				done := make(chan struct{})
				go func() { _, _ = io.Copy(local, remote); _ = local.Close(); close(done) }()
				_, _ = io.Copy(remote, buffered)
				_ = remote.Close()
				<-done
			}))
			defer proxyServer.Close()
			client := localMediaPeer(t, localMediaAPI(t))
			clientTrack := localMediaTrack(t, client)
			clientAudio := observeMediaAudio(client)
			clientData, err := client.CreateDataChannel(realtimeDataChannelLabel, nil)
			if err != nil {
				t.Fatal(err)
			}
			clientOpen, clientMessages := make(chan struct{}), make(chan webrtc.DataChannelMessage, 1)
			clientData.OnOpen(func() { close(clientOpen) })
			clientData.OnMessage(func(message webrtc.DataChannelMessage) { clientMessages <- message })
			setup, cancelSetup := context.WithCancel(t.Context())
			defer cancelSetup()
			session, offer, err := localMediaFactory(t, 1).NewSession(setup, t.Context(), localMediaDescription(t, client, true), proxyServer.URL)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = session.Close() }()
			failed := make(chan error, 1)
			session.SetCloseHandler(func(err error) { failed <- err })
			if err := upstream.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeOffer, SDP: offer}); err != nil {
				t.Fatal(err)
			}
			answer, err := session.AcceptUpstreamAnswer(setup, mediaPublicTCPAnswer(t, localMediaDescription(t, upstream, false)))
			if reject {
				if receiveMediaValue(t, failed) == nil {
					t.Fatal("proxy rejection lost its failure")
				}
				if requests.Load() != 1 {
					t.Fatal("proxy rejection was retried")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if err := session.Commit(func() error { return nil }); err != nil {
				t.Fatal(err)
			}
			cancelSetup()
			if err := client.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeAnswer, SDP: answer}); err != nil {
				t.Fatal(err)
			}
			receiveMediaValue(t, clientOpen)
			remoteData := receiveMediaValue(t, upstreamData)
			if err := clientData.SendText("through-proxy"); err != nil {
				t.Fatal(err)
			}
			if got := receiveMediaValue(t, messages); !got.IsString || string(got.Data) != "through-proxy" {
				t.Fatal("proxy event changed")
			}
			if err := remoteData.Send([]byte{0, 255}); err != nil {
				t.Fatal(err)
			}
			if got := receiveMediaValue(t, clientMessages); got.IsString || string(got.Data) != string([]byte{0, 255}) {
				t.Fatal("proxy binary event changed")
			}
			sendMediaAudio(t, clientTrack, upstreamAudio, []byte{0xf8, 0xff, 0xfe})
			sendMediaAudio(t, upstreamTrack, clientAudio, []byte{0xf8, 0xfe, 0xfd})
			if requests.Load() != 1 {
				t.Fatal("media did not use exactly one proxy connection")
			}
		})
	}
}
