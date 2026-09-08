package live

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net/netip"
	"strings"

	"github.com/pion/ice/v4"
	"github.com/pion/sdp/v3"
	"github.com/pion/stun/v3"
)

const (
	maxUpstreamICECandidates = 64
	maxProxiedTCPCandidates  = 16
	maxInitialSTUNFrameSize  = 4096
	stunMessageHeaderSize    = 20
)

var nonRoutableProxyTargetPrefixes = []netip.Prefix{
	netip.MustParsePrefix("0.0.0.0/8"),
	netip.MustParsePrefix("10.0.0.0/8"),
	netip.MustParsePrefix("100.64.0.0/10"),
	netip.MustParsePrefix("127.0.0.0/8"),
	netip.MustParsePrefix("169.254.0.0/16"),
	netip.MustParsePrefix("172.16.0.0/12"),
	netip.MustParsePrefix("192.0.0.0/24"),
	netip.MustParsePrefix("192.0.2.0/24"),
	netip.MustParsePrefix("192.88.99.0/24"),
	netip.MustParsePrefix("192.168.0.0/16"),
	netip.MustParsePrefix("198.18.0.0/15"),
	netip.MustParsePrefix("198.51.100.0/24"),
	netip.MustParsePrefix("203.0.113.0/24"),
	netip.MustParsePrefix("224.0.0.0/4"),
	netip.MustParsePrefix("240.0.0.0/4"),
	netip.MustParsePrefix("::/96"),
	netip.MustParsePrefix("::ffff:0:0:0/96"),
	netip.MustParsePrefix("64:ff9b::/96"),
	netip.MustParsePrefix("64:ff9b:1::/48"),
	netip.MustParsePrefix("100::/64"),
	netip.MustParsePrefix("2001::/23"),
	netip.MustParsePrefix("2001:db8::/32"),
	netip.MustParsePrefix("2002::/16"),
	netip.MustParsePrefix("3fff::/20"),
	netip.MustParsePrefix("5f00::/16"),
	netip.MustParsePrefix("fc00::/7"),
	netip.MustParsePrefix("fe80::/10"),
	netip.MustParsePrefix("fec0::/10"),
	netip.MustParsePrefix("ff00::/8"),
}

type iceCredentials struct {
	ufrag    string
	password string
}

type tcpCandidatePlan struct {
	mediaIndex     int
	attributeIndex int
	fields         []string
	target         netip.AddrPort
}

func proxiedTCPCandidatePlan(rawCandidate string) (tcpCandidatePlan, bool, error) {
	trimmed := strings.TrimSpace(rawCandidate)
	candidate, errCandidate := ice.UnmarshalCandidate(trimmed)
	if errCandidate != nil {
		return tcpCandidatePlan{}, false, fmt.Errorf("parse upstream WebRTC candidate: %w", errCandidate)
	}
	if candidate.NetworkType() != ice.NetworkTypeTCP4 && candidate.NetworkType() != ice.NetworkTypeTCP6 {
		return tcpCandidatePlan{}, false, nil
	}
	if candidate.TCPType() != ice.TCPTypePassive {
		return tcpCandidatePlan{}, false, nil
	}
	if candidate.Component() != uint16(ice.ComponentRTP) || candidate.Type() != ice.CandidateTypeHost {
		return tcpCandidatePlan{}, false, nil
	}
	if candidate.Port() != 443 {
		return tcpCandidatePlan{}, false, fmt.Errorf("upstream WebRTC TCP proxy candidate uses disallowed port %d", candidate.Port())
	}
	address, errAddress := netip.ParseAddr(candidate.Address())
	if errAddress != nil {
		return tcpCandidatePlan{}, false, errors.New("upstream WebRTC TCP proxy candidate address must be an IP")
	}
	address = address.Unmap()
	if !isPublicProxyTarget(address) {
		return tcpCandidatePlan{}, false, errors.New("upstream WebRTC TCP proxy candidate address must be globally routable")
	}
	fields := strings.Fields(trimmed)
	if len(fields) < 8 {
		return tcpCandidatePlan{}, false, errors.New("upstream WebRTC TCP proxy candidate is malformed")
	}
	return tcpCandidatePlan{
		fields: fields,
		target: netip.AddrPortFrom(address, uint16(candidate.Port())),
	}, true, nil
}

func isPublicProxyTarget(address netip.Addr) bool {
	address = address.Unmap()
	if !address.IsValid() || !address.IsGlobalUnicast() || address.IsUnspecified() || address.IsLoopback() ||
		address.IsPrivate() || address.IsLinkLocalUnicast() || address.IsLinkLocalMulticast() || address.IsMulticast() {
		return false
	}
	for _, prefix := range nonRoutableProxyTargetPrefixes {
		if prefix.Contains(address) {
			return false
		}
	}
	return true
}

func bundledICECredentials(description *sdp.SessionDescription) (iceCredentials, error) {
	if description == nil {
		return iceCredentials{}, errors.New("SDP is unavailable")
	}
	sessionUfrag, _ := description.Attribute("ice-ufrag")
	sessionPassword, _ := description.Attribute("ice-pwd")
	var selected iceCredentials
	for _, media := range description.MediaDescriptions {
		if media == nil {
			continue
		}
		ufrag := sessionUfrag
		if mediaUfrag, ok := media.Attribute("ice-ufrag"); ok {
			ufrag = mediaUfrag
		}
		password := sessionPassword
		if mediaPassword, ok := media.Attribute("ice-pwd"); ok {
			password = mediaPassword
		}
		ufrag = strings.TrimSpace(ufrag)
		password = strings.TrimSpace(password)
		if ufrag == "" && password == "" {
			continue
		}
		if ufrag == "" || password == "" {
			return iceCredentials{}, errors.New("SDP contains incomplete ICE credentials")
		}
		current := iceCredentials{ufrag: ufrag, password: password}
		if selected.ufrag == "" {
			selected = current
			continue
		}
		if selected != current {
			return iceCredentials{}, errors.New("SDP contains inconsistent bundled ICE credentials")
		}
	}
	if selected.ufrag == "" {
		selected = iceCredentials{ufrag: strings.TrimSpace(sessionUfrag), password: strings.TrimSpace(sessionPassword)}
	}
	if selected.ufrag == "" || selected.password == "" {
		return iceCredentials{}, errors.New("SDP is missing ICE credentials")
	}
	return selected, nil
}

func readValidatedICEBindingFrame(connection io.Reader, expectedUser, remotePassword string) ([]byte, error) {
	var header [2]byte
	if _, errRead := io.ReadFull(connection, header[:]); errRead != nil {
		return nil, fmt.Errorf("read ICE-TCP frame header: %w", errRead)
	}
	frameSize := int(binary.BigEndian.Uint16(header[:]))
	if frameSize < stunMessageHeaderSize || frameSize > maxInitialSTUNFrameSize {
		return nil, fmt.Errorf("invalid initial ICE-TCP STUN frame size %d", frameSize)
	}
	payload := make([]byte, frameSize)
	if _, errRead := io.ReadFull(connection, payload); errRead != nil {
		return nil, fmt.Errorf("read ICE-TCP STUN frame: %w", errRead)
	}
	message := stun.NewWithOptions(stun.WithStrict(true))
	if errDecode := stun.Decode(payload, message); errDecode != nil {
		return nil, fmt.Errorf("decode initial ICE-TCP STUN message: %w", errDecode)
	}
	if len(payload) != stunMessageHeaderSize+int(message.Length) {
		return nil, errors.New("initial ICE-TCP STUN message contains trailing data")
	}
	if message.Type != stun.BindingRequest {
		return nil, fmt.Errorf("initial ICE-TCP STUN message has unexpected type %s", message.Type)
	}
	var username stun.Username
	if errUsername := username.GetFrom(message); errUsername != nil {
		return nil, fmt.Errorf("read initial ICE-TCP STUN username: %w", errUsername)
	}
	if string(username) != expectedUser {
		return nil, errors.New("initial ICE-TCP STUN username does not match the media session")
	}
	if errIntegrity := stun.NewShortTermIntegrity(remotePassword).Check(message); errIntegrity != nil {
		return nil, fmt.Errorf("verify initial ICE-TCP STUN integrity: %w", errIntegrity)
	}
	if errFingerprint := stun.Fingerprint.Check(message); errFingerprint != nil {
		return nil, fmt.Errorf("verify initial ICE-TCP STUN fingerprint: %w", errFingerprint)
	}
	frame := make([]byte, len(header)+len(payload))
	copy(frame, header[:])
	copy(frame[len(header):], payload)
	return frame, nil
}
