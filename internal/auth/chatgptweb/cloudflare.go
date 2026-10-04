package chatgptweb

import (
	"bytes"
	"encoding/json"
	"strings"
)

// IsCloudflareChallengePage distinguishes an interstitial response from the
// background JS detections injected into otherwise normal HTML pages.
func IsCloudflareChallengePage(cfMitigated, server, cfRay string, payload []byte) bool {
	if strings.EqualFold(strings.TrimSpace(cfMitigated), "challenge") {
		return true
	}
	if len(payload) == 0 || json.Valid(payload) {
		return false
	}
	body := bytes.ToLower(payload)
	for _, marker := range []string{
		"_cf_chl_opt",
		"/orchestrate/chl_page/",
		"/orchestrate/managed/",
		"<title>attention required! | cloudflare",
	} {
		if bytes.Contains(body, []byte(marker)) {
			return true
		}
	}
	// A challenge-platform script or Cloudflare edge header alone is not proof
	// of an interstitial; normal sign-in pages include /scripts/jsd/main.js.
	edge := strings.TrimSpace(cfRay) != "" || strings.Contains(strings.ToLower(server), "cloudflare") || bytes.Contains(body, []byte("cloudflare"))
	return edge && (bytes.Contains(body, []byte("<title>just a moment")) ||
		bytes.Contains(body, []byte(`id="challenge-error-text"`)) ||
		bytes.Contains(body, []byte(`id='challenge-error-text'`)))
}
