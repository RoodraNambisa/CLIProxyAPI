package live

import (
	"encoding/json"
	"errors"
	"strings"
)

// These helpers operate only on the top-level SDP protocol field after native
// payload normalization. Session settings and arbitrary user JSON remain opaque.
func (p callPayload) mediaOffer() (string, error) {
	if p.contentType != "application/json" {
		if strings.TrimSpace(string(p.body)) == "" {
			return "", errors.New("realtime call requires an SDP offer")
		}
		return string(p.body), nil
	}
	object, err := callJSONObject(p.body, "call")
	if err != nil {
		return "", err
	}
	var offer string
	if err := json.Unmarshal(object["sdp"], &offer); err != nil || strings.TrimSpace(offer) == "" {
		return "", errors.New("realtime sdp must be a non-empty string")
	}
	return offer, nil
}

func (p callPayload) withMediaOffer(offer string) (callPayload, error) {
	if len(offer) > maxCallBodySize {
		return callPayload{}, errCallBodyTooLarge
	}
	object, err := callJSONObject(p.body, "call")
	if err != nil {
		return callPayload{}, err
	}
	object["sdp"], _ = json.Marshal(offer)
	encoded, err := json.Marshal(object)
	if len(encoded) > maxCallBodySize {
		return callPayload{}, errCallBodyTooLarge
	}
	return callPayload{body: encoded, contentType: "application/json", model: p.model}, err
}
