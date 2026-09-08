package live

import (
	"context"
	"errors"
	"net"
	"strconv"
	"strings"

	"github.com/pion/sdp/v3"
	"golang.org/x/net/proxy"
)

// Validate every candidate before opening any listener. The rewritten answer
// belongs only to the internal upstream peer; it is never sent to the client.
func prepareProxiedUpstreamAnswer(ctx context.Context, answer, localOffer string, dialer proxy.ContextDialer, onFailure func(error)) (string, []*tcpCandidateTunnel, error) {
	if ctx == nil || dialer == nil {
		return "", nil, errors.New("media proxy requires session context and dialer")
	}
	if err := context.Cause(ctx); err != nil {
		return "", nil, err
	}
	if len(answer) > maxCallBodySize || len(localOffer) > maxCallBodySize {
		return "", nil, errCallBodyTooLarge
	}
	var remote, local sdp.SessionDescription
	if err := remote.UnmarshalString(answer); err != nil {
		return "", nil, &mediaProxyError{cause: err}
	}
	if err := local.UnmarshalString(localOffer); err != nil {
		return "", nil, &mediaProxyError{cause: err}
	}
	remoteCredentials, errRemote := bundledICECredentials(&remote)
	if errRemote != nil {
		return "", nil, errRemote
	}
	localCredentials, errLocal := bundledICECredentials(&local)
	if errLocal != nil {
		return "", nil, errLocal
	}
	plans := make([]tcpCandidatePlan, 0, 4)
	count := 0
	for mediaIndex, media := range remote.MediaDescriptions {
		if media == nil {
			continue
		}
		filtered := make([]sdp.Attribute, 0, len(media.Attributes))
		for _, attribute := range media.Attributes {
			if !attribute.IsICECandidate() {
				filtered = append(filtered, attribute)
				continue
			}
			count++
			if count > maxUpstreamICECandidates {
				return "", nil, errors.New("upstream media SDP exceeds 64 ICE candidates")
			}
			plan, keep, err := proxiedTCPCandidatePlan(attribute.Value)
			if err != nil {
				return "", nil, err
			}
			if !keep {
				continue
			}
			if len(plans) >= maxProxiedTCPCandidates {
				return "", nil, errors.New("upstream media SDP exceeds 16 TCP proxy candidates")
			}
			plan.mediaIndex, plan.attributeIndex = mediaIndex, len(filtered)
			filtered = append(filtered, attribute)
			plans = append(plans, plan)
		}
		media.Attributes = filtered
	}
	if len(plans) == 0 {
		return "", nil, errors.New("upstream media SDP has no supported public TCP443 candidate")
	}
	tunnels := make([]*tcpCandidateTunnel, 0, len(plans))
	cleanup := func(cause error) (string, []*tcpCandidateTunnel, error) {
		return "", nil, errors.Join(cause, closeCandidateTunnels(tunnels))
	}
	for _, plan := range plans {
		tunnel, err := newTCPCandidateTunnel(ctx, plan.target, dialer, remoteCredentials.ufrag+":"+localCredentials.ufrag, remoteCredentials.password, onFailure)
		if err != nil {
			return cleanup(err)
		}
		tunnels = append(tunnels, tunnel)
		address, ok := tunnel.listener.Addr().(*net.TCPAddr)
		if !ok || address.IP == nil {
			return cleanup(errors.New("media proxy listener returned an invalid address"))
		}
		fields := append([]string(nil), plan.fields...)
		fields[4], fields[5] = address.IP.String(), strconv.Itoa(address.Port)
		remote.MediaDescriptions[plan.mediaIndex].Attributes[plan.attributeIndex].Value = strings.Join(fields, " ")
	}
	encoded, errEncode := remote.Marshal()
	if errEncode != nil {
		return cleanup(&mediaProxyError{cause: errEncode})
	}
	if err := context.Cause(ctx); err != nil {
		return cleanup(err)
	}
	return string(encoded), tunnels, nil
}

func closeCandidateTunnels(tunnels []*tcpCandidateTunnel) error {
	var failures []error
	for _, tunnel := range tunnels {
		if err := tunnel.Close(); err != nil {
			failures = append(failures, err)
		}
	}
	return errors.Join(failures...)
}
