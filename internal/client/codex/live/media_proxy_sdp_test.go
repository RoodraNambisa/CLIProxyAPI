package live

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"testing"

	"github.com/pion/ice/v4"
	"github.com/pion/sdp/v3"
)

func mediaCandidateSDP(candidates []string) string {
	body := mediaProxyTestSDP("remote", "fixture-password")
	return strings.Replace(body, "a=mid:0\r\n", "a=mid:0\r\na=x-fixture:keep\r\n"+strings.Join(candidates, "\r\n")+"\r\n", 1)
}

func noMediaProxyDial(t *testing.T) mediaTunnelDialer {
	t.Helper()
	return mediaTunnelDialer{dial: func(context.Context, string, string) (net.Conn, error) {
		t.Error("SDP validation unexpectedly connected upstream")
		return nil, context.Canceled
	}}
}

func TestMediaProxySDPFiltersAndRewritesOnlyCandidates(t *testing.T) {
	candidates := []string{
		"a=candidate:1 1 udp 2130706431 20.42.0.10 3478 typ host",
		"a=candidate:2 1 tcp 1671430143 20.42.0.10 443 typ host tcptype passive",
		"a=candidate:3 1 tcp 1671430143 2606:4700:4700::1111 443 typ host tcptype passive",
	}
	answer, tunnels, err := prepareProxiedUpstreamAnswer(t.Context(), mediaCandidateSDP(candidates), mediaProxyTestSDP("local", "local-password"), noMediaProxyDial(t), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := closeCandidateTunnels(tunnels); err != nil {
			t.Error(err)
		}
	})
	if len(tunnels) != 2 || !strings.Contains(answer, "a=x-fixture:keep") || !strings.Contains(answer, "a=ice-ufrag:remote") || !strings.Contains(answer, "a=ice-pwd:fixture-password") {
		t.Fatal("SDP attributes changed outside candidate transport")
	}
	var description sdp.SessionDescription
	if err := description.UnmarshalString(answer); err != nil {
		t.Fatal("rewritten SDP is invalid")
	}
	index := 0
	for _, media := range description.MediaDescriptions {
		for _, attribute := range media.Attributes {
			if !attribute.IsICECandidate() {
				continue
			}
			candidate, err := ice.UnmarshalCandidate(attribute.Value)
			if err != nil || !net.ParseIP(candidate.Address()).IsLoopback() || candidate.TCPType() != ice.TCPTypePassive {
				t.Fatal("rewritten proxy candidate is not local passive TCP")
			}
			_, port, errPort := net.SplitHostPort(tunnels[index].listener.Addr().String())
			if errPort != nil || strconv.Itoa(candidate.Port()) != port {
				t.Fatal("SDP does not refer to its owned listener")
			}
			if tunnels[index].expectedUser != "remote:local" || tunnels[index].remotePassword != "fixture-password" {
				t.Fatal("tunnel is not bound to its ICE exchange")
			}
			index++
		}
	}
	if index != 2 {
		t.Fatal("unexpected surviving candidates")
	}
}

func TestMediaProxySDPRejectsInvalidAndExcessCandidates(t *testing.T) {
	for _, count := range []int{16, 17, 64, 65} {
		protocol, port := "tcp", 443
		if count >= 64 {
			protocol, port = "udp", 3478
		}
		candidates := make([]string, count)
		for i := range candidates {
			candidates[i] = fmt.Sprintf("a=candidate:%d 1 %s 1671430143 20.42.0.10 %d typ host tcptype passive", i+1, protocol, port)
		}
		if count >= 64 {
			candidates[0] = "a=candidate:1 1 tcp 1671430143 20.42.0.10 443 typ host tcptype passive"
		}
		_, tunnels, err := prepareProxiedUpstreamAnswer(t.Context(), mediaCandidateSDP(candidates), mediaProxyTestSDP("local", "local-password"), noMediaProxyDial(t), nil)
		if count == 16 || count == 64 {
			want := 16
			if count == 64 {
				want = 1
			}
			if err != nil || len(tunnels) != want {
				t.Fatal("legal candidate boundary rejected")
			}
			if err := closeCandidateTunnels(tunnels); err != nil {
				t.Fatal(err)
			}
		} else if err == nil || len(tunnels) != 0 {
			_ = closeCandidateTunnels(tunnels)
			t.Fatal("invalid candidate set allocated listeners")
		}
	}
	for _, answer := range []string{"invalid", mediaCandidateSDP(nil), mediaCandidateSDP([]string{"a=candidate:1 1 tcp 1671430143 127.0.0.1 443 typ host tcptype passive"})} {
		if _, tunnels, err := prepareProxiedUpstreamAnswer(t.Context(), answer, mediaProxyTestSDP("local", "local-password"), noMediaProxyDial(t), nil); err == nil || len(tunnels) != 0 {
			_ = closeCandidateTunnels(tunnels)
			t.Fatal("invalid answer created a proxy plan")
		}
	}
}

func TestMediaProxySDPParentCancellationReleasesOwnedListeners(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	answer := mediaCandidateSDP([]string{"a=candidate:1 1 tcp 1671430143 20.42.0.10 443 typ host tcptype passive"})
	_, tunnels, err := prepareProxiedUpstreamAnswer(ctx, answer, mediaProxyTestSDP("local", "local-password"), noMediaProxyDial(t), nil)
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	if err := closeCandidateTunnels(tunnels); err != nil {
		t.Fatal(err)
	}
	for _, tunnel := range tunnels {
		if client, err := net.Dial("tcp", tunnel.listener.Addr().String()); err == nil {
			_ = client.Close()
			t.Fatal("cancelled listener remained open")
		}
	}
	if _, tunnels, err := prepareProxiedUpstreamAnswer(ctx, answer, mediaProxyTestSDP("local", "local-password"), noMediaProxyDial(t), nil); err == nil || len(tunnels) != 0 {
		t.Fatal("cancelled SDP operation created listeners")
	}
}
