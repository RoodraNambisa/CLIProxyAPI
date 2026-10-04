package chatgptweb

import "testing"

func TestCloudflareChallengePageEvidence(t *testing.T) {
	for _, test := range []struct {
		name, mitigated, server, body string
		challenge                     bool
	}{
		{"background detection", "", "cloudflare", `<html><script src="/cdn-cgi/challenge-platform/scripts/jsd/main.js"></script></html>`, false},
		{"generic platform reference", "", "cloudflare", `<html>challenge-platform<script src="/cdn-cgi/challenge-platform/x"></script></html>`, false},
		{"unknown forbidden", "", "cloudflare", `<html>Access denied</html>`, false},
		{"empty", "", "cloudflare", "", false},
		{"official deactivation", "", "cloudflare", `{"error":{"code":"account_deactivated","message":"Just a moment"}}`, false},
		{"mitigated", " challenge ", "", `<html>arbitrary challenge template</html>`, true},
		{"challenge options and jsd", "", "", `<script>window._cf_chl_opt={};</script><script src="/cdn-cgi/challenge-platform/scripts/jsd/main.js"></script>`, true},
		{"orchestrator", "", "", `<script src="/cdn-cgi/challenge-platform/h/g/orchestrate/chl_page/v1"></script>`, true},
		{"edge challenge title", "", "cloudflare", `<title>Just a moment...</title>`, true},
		{"ordinary title", "", "", `<title>Just a moment</title>`, false},
		{"error text", "", "cloudflare", `<span id="challenge-error-text">Enable JavaScript and cookies to continue</span>`, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := IsCloudflareChallengePage(test.mitigated, test.server, "", []byte(test.body)); got != test.challenge {
				t.Fatalf("challenge=%v, want %v", got, test.challenge)
			}
		})
	}
}
