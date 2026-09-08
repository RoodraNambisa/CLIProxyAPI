package live

import (
	"errors"
	"strings"

	"github.com/pion/webrtc/v4"
)

func (s *pionMediaSession) installMediaCallbacks() {
	for _, peer := range []*webrtc.PeerConnection{s.downstream, s.upstream} {
		peer.OnConnectionStateChange(func(state webrtc.PeerConnectionState) {
			if state == webrtc.PeerConnectionStateFailed || state == webrtc.PeerConnectionStateClosed {
				go s.fail(errors.New("realtime media peer closed or failed"))
			}
		})
	}
	s.downstream.OnDataChannel(func(channel *webrtc.DataChannel) {
		s.mu.Lock()
		if s.closed || s.downstreamChannel != nil || channel.Label() != realtimeDataChannelLabel {
			s.mu.Unlock()
			_ = channel.Close()
			return
		}
		s.downstreamChannel = channel
		s.mu.Unlock()
		s.upToDown.setDestination(channel)
		s.bindMediaSource(channel, s.downToUp)
	})
}

func (s *pionMediaSession) bindMediaSource(channel *webrtc.DataChannel, destination *mediaDataPipe) {
	channel.OnMessage(func(message webrtc.DataChannelMessage) { destination.enqueue(message.Data, message.IsString) })
	channel.OnError(func(err error) { go s.fail(&mediaDataWriteError{cause: err}) })
	channel.OnClose(func() { go s.fail(errors.New("realtime media event channel closed")) })
}

func (s *pionMediaSession) installAudioTracks() error {
	toClient, errClient := webrtc.NewTrackLocalStaticRTP(mediaOpusCodec, "audio", "codex-live")
	if errClient != nil {
		return errClient
	}
	toUpstream, errUpstream := webrtc.NewTrackLocalStaticRTP(mediaOpusCodec, "audio", "codex-live")
	if errUpstream != nil {
		return errUpstream
	}
	for _, route := range []struct {
		peer  *webrtc.PeerConnection
		track *webrtc.TrackLocalStaticRTP
	}{{s.downstream, toClient}, {s.upstream, toUpstream}} {
		sender, err := route.peer.AddTrack(route.track)
		if err != nil {
			return err
		}
		s.run(func() {
			for {
				if _, _, err := sender.ReadRTCP(); err != nil {
					return
				}
			}
		})
	}
	forward := func(destination *webrtc.TrackLocalStaticRTP) func(*webrtc.TrackRemote, *webrtc.RTPReceiver) {
		return func(source *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
			if !strings.EqualFold(source.Codec().MimeType, webrtc.MimeTypeOpus) {
				return
			}
			s.run(func() {
				for {
					packet, _, errRead := source.ReadRTP()
					if errRead != nil {
						return
					}
					// RTP extension IDs are negotiated separately on the two peers.
					packet.Extension, packet.ExtensionProfile, packet.Extensions = false, 0, nil
					if errWrite := destination.WriteRTP(packet); errWrite != nil {
						go s.fail(&mediaDataWriteError{cause: errWrite})
						return
					}
				}
			})
		}
	}
	s.downstream.OnTrack(forward(toUpstream))
	s.upstream.OnTrack(forward(toClient))
	return nil
}
