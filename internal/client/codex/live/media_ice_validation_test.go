package live

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net/netip"
	"strings"
	"testing"

	"github.com/pion/sdp/v3"
	"github.com/pion/stun/v3"
)

func TestMediaProxyCandidatesOnlyAdmitPublicPassiveTCP443(t *testing.T) {
	if isPublicProxyTarget(netip.MustParseAddr("::ffff:203.0.113.10")) {
		t.Fatal("mapped reserved address bypassed the target validator")
	}
	for _, address := range []string{"20.42.0.10", "2606:4700:4700::1111", "::ffff:20.42.0.10"} {
		plan, keep, err := proxiedTCPCandidatePlan("1 1 tcp 1671430143 " + address + " 443 typ host tcptype passive")
		if err != nil || !keep || plan.target != netip.AddrPortFrom(netip.MustParseAddr(address).Unmap(), 443) {
			t.Fatal("valid public TCP candidate rejected")
		}
	}
	for _, address := range []string{"0.0.0.1", "10.0.0.1", "100.64.0.1", "127.0.0.1", "169.254.1.1", "192.0.2.1", "198.18.0.1", "203.0.113.10", "224.0.0.1", "240.0.0.1", "::1", "::ffff:127.0.0.1", "64:ff9b::7f00:1", "2001:db8::1", "2002::1", "fc00::1", "fec0::1", "ff02::1"} {
		if _, keep, err := proxiedTCPCandidatePlan("1 1 tcp 1671430143 " + address + " 443 typ host tcptype passive"); err == nil || keep {
			t.Fatal("unsafe candidate became a proxy target")
		}
	}
	for _, candidate := range []string{
		"1 1 tcp 1671430143 fixture.invalid 443 typ host tcptype passive",
		"1 1 udp 1671430143 20.42.0.10 443 typ host",
		"1 1 tcp 1671430143 20.42.0.10 443 typ host tcptype active",
		"1 1 tcp 1671430143 20.42.0.10 443 typ relay raddr 192.0.2.1 rport 5000 tcptype passive",
		"1 2 tcp 1671430143 20.42.0.10 443 typ host tcptype passive",
	} {
		if _, keep, err := proxiedTCPCandidatePlan(candidate); err != nil || keep {
			t.Fatal("unsupported candidate was not filtered")
		}
	}
	for _, candidate := range []string{"invalid", "1 1 tcp 1671430143 20.42.0.10 8443 typ host tcptype passive"} {
		if _, keep, err := proxiedTCPCandidatePlan(candidate); err == nil || keep {
			t.Fatal("invalid candidate was accepted")
		}
	}
}

func mediaProxyTestSDP(ufrag, password string) string {
	return fmt.Sprintf("v=0\r\no=- 1 1 IN IP4 127.0.0.1\r\ns=-\r\nt=0 0\r\na=group:BUNDLE 0 1\r\nm=audio 9 UDP/TLS/RTP/SAVPF 111\r\nc=IN IP4 0.0.0.0\r\na=mid:0\r\na=ice-ufrag:%s\r\na=ice-pwd:%s\r\nm=application 9 UDP/DTLS/SCTP webrtc-datachannel\r\nc=IN IP4 0.0.0.0\r\na=mid:1\r\na=ice-ufrag:%s\r\na=ice-pwd:%s\r\n", ufrag, password, ufrag, password)
}

func TestMediaBundledICECredentialsUseOneSession(t *testing.T) {
	valid := mediaProxyTestSDP("fixture", "fixture-password")
	for _, test := range []struct {
		body  string
		valid bool
	}{
		{valid, true},
		{strings.Replace(valid, "a=mid:1\r\na=ice-ufrag:fixture", "a=mid:1\r\na=ice-ufrag:other", 1), false},
		{strings.Replace(valid, "a=mid:1\r\na=ice-ufrag:fixture\r\na=ice-pwd:fixture-password", "a=mid:1\r\na=ice-ufrag:fixture\r\na=ice-pwd:other", 1), false},
		{strings.ReplaceAll(valid, "a=ice-pwd:fixture-password\r\n", ""), false},
		{"v=0\r\no=- 1 1 IN IP4 127.0.0.1\r\ns=-\r\nt=0 0\r\n", false},
		{"v=0\r\no=- 1 1 IN IP4 127.0.0.1\r\ns=-\r\nt=0 0\r\na=ice-ufrag:fixture\r\na=ice-pwd:fixture-password\r\n", true},
	} {
		var description sdp.SessionDescription
		if err := description.UnmarshalString(test.body); err != nil {
			t.Fatal("invalid SDP fixture")
		}
		credentials, err := bundledICECredentials(&description)
		if (err == nil) != test.valid {
			t.Fatal("incorrect bundled ICE validation")
		}
		if test.valid && (credentials.ufrag != "fixture" || credentials.password != "fixture-password") {
			t.Fatal("ICE credentials were not preserved")
		}
	}
}

func mediaTestICEFrame(t *testing.T, username, password string, fingerprint bool) []byte {
	t.Helper()
	setters := []stun.Setter{stun.BindingRequest, stun.TransactionID, stun.NewUsername(username), stun.NewShortTermIntegrity(password)}
	if fingerprint {
		setters = append(setters, stun.Fingerprint)
	}
	message, err := stun.Build(setters...)
	if err != nil {
		t.Fatal(err)
	}
	frame := make([]byte, 2+len(message.Raw))
	binary.BigEndian.PutUint16(frame, uint16(len(message.Raw)))
	copy(frame[2:], message.Raw)
	return frame
}

type mediaFragmentedReader struct{ data []byte }

func (r *mediaFragmentedReader) Read(destination []byte) (int, error) {
	if len(r.data) == 0 {
		return 0, io.EOF
	}
	count := min(len(destination), len(r.data), 3)
	copy(destination, r.data[:count])
	r.data = r.data[count:]
	return count, nil
}

func TestMediaICEBindingFrameAuthenticatesBeforeForwarding(t *testing.T) {
	valid := mediaTestICEFrame(t, "remote:local", "fixture-password", true)
	got, err := readValidatedICEBindingFrame(&mediaFragmentedReader{data: valid}, "remote:local", "fixture-password")
	if err != nil || !bytes.Equal(got, valid) {
		t.Fatal("valid fragmented ICE frame changed")
	}
	for _, pair := range [][2]string{{"local:remote", "fixture-password"}, {"remote:local", "wrong"}} {
		if _, err := readValidatedICEBindingFrame(bytes.NewReader(valid), pair[0], pair[1]); err == nil {
			t.Fatal("incorrect ICE credentials accepted")
		}
	}
	for i := range len(valid) {
		if _, err := readValidatedICEBindingFrame(bytes.NewReader(valid[:i]), "remote:local", "fixture-password"); err == nil {
			t.Fatal("truncated ICE frame accepted")
		}
	}
	oversized := []byte{0, 0}
	binary.BigEndian.PutUint16(oversized, maxInitialSTUNFrameSize+1)
	trailing := append(append([]byte(nil), valid...), 0)
	binary.BigEndian.PutUint16(trailing, uint16(len(trailing)-2))
	tampered := append([]byte(nil), valid...)
	tampered[len(tampered)-1] ^= 1
	for _, invalid := range [][]byte{oversized, trailing, tampered, {0, 1, 0}, mediaTestICEFrame(t, "remote:local", "fixture-password", false)} {
		if _, err := readValidatedICEBindingFrame(bytes.NewReader(invalid), "remote:local", "fixture-password"); err == nil {
			t.Fatal("invalid ICE frame accepted")
		}
	}
}
