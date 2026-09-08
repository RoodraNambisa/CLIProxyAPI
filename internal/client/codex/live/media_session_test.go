package live

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

func localMediaAPI(t *testing.T) *webrtc.API {
	t.Helper()
	api, err := newMediaAPI(config.CodexLiveMediaRelayConfig{}, false, true)
	if err != nil {
		t.Fatal(err)
	}
	return api
}

func localMediaPeer(t *testing.T, api *webrtc.API) *webrtc.PeerConnection {
	t.Helper()
	peer, err := api.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := peer.Close(); err != nil {
			t.Error(err)
		}
	})
	return peer
}

func localMediaDescription(t *testing.T, peer *webrtc.PeerConnection, offer bool) string {
	t.Helper()
	gathered := webrtc.GatheringCompletePromise(peer)
	var description webrtc.SessionDescription
	var err error
	if offer {
		description, err = peer.CreateOffer(nil)
	} else {
		description, err = peer.CreateAnswer(nil)
	}
	if err != nil {
		t.Fatal(err)
	}
	if err := peer.SetLocalDescription(description); err != nil {
		t.Fatal(err)
	}
	select {
	case <-gathered:
	case <-time.After(3 * time.Second):
		t.Fatal("local media candidate gathering did not complete")
	}
	return peer.LocalDescription().SDP
}

func localMediaTrack(t *testing.T, peer *webrtc.PeerConnection) *webrtc.TrackLocalStaticRTP {
	t.Helper()
	track, err := webrtc.NewTrackLocalStaticRTP(mediaOpusCodec, "audio", "fixture")
	if err != nil {
		t.Fatal(err)
	}
	sender, err := peer.AddTrack(track)
	if err != nil {
		t.Fatal(err)
	}
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		for {
			if _, _, err := sender.ReadRTCP(); err != nil {
				return
			}
		}
	}()
	t.Cleanup(func() { _ = sender.Stop(); <-finished })
	return track
}

func localMediaFactory(t *testing.T, limit int) *pionMediaRelay {
	t.Helper()
	factory, err := newPionMediaRelay(config.CodexLiveMediaRelayConfig{MaxSessions: limit}, nil)
	if err != nil {
		t.Fatal(err)
	}
	factory.downstreamAPI, factory.upstreamAPI = localMediaAPI(t), localMediaAPI(t)
	return factory
}

func receiveMediaValue[T any](t *testing.T, channel <-chan T) T {
	t.Helper()
	select {
	case value := <-channel:
		return value
	case <-time.After(3 * time.Second):
		t.Fatal("local media event did not arrive")
	}
	var zero T
	return zero
}

func observeMediaAudio(peer *webrtc.PeerConnection) <-chan []byte {
	packets := make(chan []byte, 1)
	peer.OnTrack(func(track *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
		packet, _, err := track.ReadRTP()
		if err == nil {
			packets <- append([]byte(nil), packet.Payload...)
		}
	})
	return packets
}

func sendMediaAudio(t *testing.T, source *webrtc.TrackLocalStaticRTP, destination <-chan []byte, payload []byte) {
	t.Helper()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	var sequence uint16
	for {
		select {
		case <-ticker.C:
			sequence++
			packet := &rtp.Packet{Header: rtp.Header{Version: 2, SequenceNumber: sequence, Timestamp: uint32(sequence) * 960, SSRC: 12345}, Payload: payload}
			if err := packet.SetExtension(3, []byte{1, 2, 3}); err != nil {
				t.Fatal(err)
			}
			if err := source.WriteRTP(packet); err != nil {
				t.Fatal(err)
			}
		case got := <-destination:
			if string(got) != string(payload) {
				t.Fatal("media audio payload changed")
			}
			return
		case <-deadline.C:
			t.Fatal("Opus payload did not cross local media peers")
		}
	}
}

func TestMediaSessionBridgesAudioEventsAndDetachesCommittedSetup(t *testing.T) {
	client := localMediaPeer(t, localMediaAPI(t))
	clientTrack := localMediaTrack(t, client)
	clientAudio := observeMediaAudio(client)
	clientData, err := client.CreateDataChannel(realtimeDataChannelLabel, nil)
	if err != nil {
		t.Fatal(err)
	}
	clientOpen, clientMessages := make(chan struct{}), make(chan webrtc.DataChannelMessage, 4)
	clientData.OnOpen(func() { close(clientOpen) })
	clientData.OnMessage(func(message webrtc.DataChannelMessage) {
		message.Data = append([]byte(nil), message.Data...)
		clientMessages <- message
	})
	clientOffer := localMediaDescription(t, client, true)
	factory := localMediaFactory(t, 1)
	setup, cancelSetup := context.WithCancel(t.Context())
	defer cancelSetup()
	lifetime, cancelLifetime := context.WithCancelCause(t.Context())
	defer cancelLifetime(context.Canceled)
	session, offer, err := factory.NewSession(setup, lifetime, clientOffer, "direct")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := session.Close(); err != nil {
			t.Error(err)
		}
	})
	if _, _, err := factory.NewSession(t.Context(), t.Context(), clientOffer, "direct"); !errors.Is(err, errMediaCapacity) {
		t.Fatal("active media session did not occupy capacity")
	}
	if err := session.Commit(func() error { t.Error("unanswered session published"); return nil }); err == nil {
		t.Fatal("session committed without an answer")
	}
	upstream := localMediaPeer(t, localMediaAPI(t))
	upstreamTrack := localMediaTrack(t, upstream)
	upstreamAudio := observeMediaAudio(upstream)
	upstreamReady := make(chan *webrtc.DataChannel, 1)
	upstreamMessages := make(chan webrtc.DataChannelMessage, 4)
	upstream.OnDataChannel(func(channel *webrtc.DataChannel) {
		channel.OnOpen(func() { upstreamReady <- channel })
		channel.OnMessage(func(message webrtc.DataChannelMessage) {
			message.Data = append([]byte(nil), message.Data...)
			upstreamMessages <- message
		})
	})
	if err := upstream.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeOffer, SDP: offer}); err != nil {
		t.Fatal(err)
	}
	answer, err := session.AcceptUpstreamAnswer(setup, localMediaDescription(t, upstream, false))
	if err != nil {
		t.Fatal(err)
	}
	publishError := errors.New("fixture registration failed")
	if err := session.Commit(func() error { return publishError }); !errors.Is(err, publishError) {
		t.Fatal("failed publication lost its cause")
	}
	if err := session.Commit(func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	cancelSetup()
	if err := client.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeAnswer, SDP: answer}); err != nil {
		t.Fatal(err)
	}
	receiveMediaValue(t, clientOpen)
	upstreamData := receiveMediaValue(t, upstreamReady)
	for _, direction := range []struct {
		source *webrtc.DataChannel
		target <-chan webrtc.DataChannelMessage
	}{{clientData, upstreamMessages}, {upstreamData, clientMessages}} {
		if err := direction.source.SendText("local-event"); err != nil {
			t.Fatal(err)
		}
		message := receiveMediaValue(t, direction.target)
		if !message.IsString || string(message.Data) != "local-event" {
			t.Fatal("text event did not cross media relay")
		}
		if err := direction.source.Send([]byte{0, 1, 255}); err != nil {
			t.Fatal(err)
		}
		message = receiveMediaValue(t, direction.target)
		if message.IsString || string(message.Data) != string([]byte{0, 1, 255}) {
			t.Fatal("binary event did not cross media relay")
		}
	}
	sendMediaAudio(t, clientTrack, upstreamAudio, []byte{0xf8, 0xff, 0xfe})
	sendMediaAudio(t, upstreamTrack, clientAudio, []byte{0xf8, 0xfe, 0xfd})
	session.mu.Lock()
	detached := session.setup == nil && session.stopSetup == nil && session.committed
	session.mu.Unlock()
	if !detached {
		t.Fatal("committed session retained its setup request")
	}
	failures := make(chan error, 2)
	lifetimeError := errors.New("fixture credential retired")
	session.SetCloseHandler(func(err error) {
		_ = session.Close()
		failures <- err
	})
	cancelLifetime(lifetimeError)
	if err := receiveMediaValue(t, failures); !errors.Is(err, lifetimeError) {
		t.Fatal("lifetime cancellation lost its original cause")
	}
	var closers sync.WaitGroup
	for range 4 {
		closers.Go(func() {
			if err := session.Close(); err != nil {
				t.Error(err)
			}
		})
	}
	closers.Wait()
	session.SetCloseHandler(func(err error) { failures <- err })
	select {
	case <-failures:
		t.Fatal("media failure callback ran more than once")
	default:
	}
	replacement, _, err := factory.NewSession(t.Context(), t.Context(), clientOffer, "direct")
	if err != nil {
		t.Fatal("closed media session retained capacity")
	}
	if err := replacement.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestMediaSessionSetupCancellationAndInvalidSDPReleaseResources(t *testing.T) {
	client := localMediaPeer(t, localMediaAPI(t))
	if _, err := client.CreateDataChannel(realtimeDataChannelLabel, nil); err != nil {
		t.Fatal(err)
	}
	offer := localMediaDescription(t, client, true)
	factory := localMediaFactory(t, 1)
	if _, _, err := factory.NewSession(t.Context(), t.Context(), "invalid-sdp", "direct"); err == nil {
		t.Fatal("invalid SDP created a media session")
	}
	setup, cancel := context.WithCancel(t.Context())
	session, _, err := factory.NewSession(setup, t.Context(), offer, "direct")
	if err != nil {
		t.Fatal("failed setup retained media capacity")
	}
	cancel()
	receiveMediaValue(t, session.finished)
	if err := session.Commit(func() error { t.Error("cancelled session published"); return nil }); err == nil {
		t.Fatal("cancelled session committed")
	}
	replacement, _, err := factory.NewSession(t.Context(), t.Context(), offer, "direct")
	if err != nil {
		t.Fatal("cancelled media setup retained capacity")
	}
	if _, err := replacement.AcceptUpstreamAnswer(t.Context(), "invalid-answer"); err == nil {
		t.Fatal("invalid answer accepted")
	}
	select {
	case <-replacement.finished:
	default:
		t.Fatal("invalid answer returned before cleanup finished")
	}
}
