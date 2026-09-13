package helps

import (
	"math"
	"net/http"
	"strconv"
	"strings"
	"unicode/utf8"

	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	"github.com/tidwall/gjson"
)

const maxCodexQuotaEventBytes = 64 << 10

// ParseCodexQuotaEventHeaders reads passive quota metadata only. An oversized or
// malformed event is ignored for observation without changing protocol handling.
func ParseCodexQuotaEventHeaders(payload []byte) http.Header {
	if len(payload) == 0 || len(payload) > maxCodexQuotaEventBytes {
		return nil
	}
	kind := gjson.GetBytes(payload, "type").String()
	if (kind != "codex.rate_limits" && kind != "error") || !gjson.ValidBytes(payload) {
		return nil
	}
	root := gjson.ParseBytes(payload)
	headers := make(http.Header)
	if kind == "error" {
		if values := root.Get("headers"); values.IsObject() {
			values.ForEach(func(key, value gjson.Result) bool {
				if raw := codexQuotaScalar(value); raw != "" {
					headers[key.String()] = []string{raw}
				}
				return true
			})
		}
		return filteredCodexQuotaEventHeaders(headers)
	}
	addCodexQuotaWindows(headers, "X-Codex-", firstCodexQuotaValue(root, "rate_limits", "rateLimit"))
	addCodexQuotaWindows(headers, "X-Codex-Code-Review-", firstCodexQuotaValue(root, "code_review_rate_limits", "codeReviewRateLimits"))
	additional := firstCodexQuotaValue(root, "additional_rate_limits", "additionalRateLimits")
	if additional.IsArray() || additional.IsObject() {
		count := 0
		seen := make(map[string]bool)
		additional.ForEach(func(key, value gjson.Result) bool {
			if count >= 8 {
				return false
			}
			name := key.String()
			if additional.IsArray() {
				name = codexQuotaScalar(firstCodexQuotaValue(value, "limit_name", "limitName", "name"))
			}
			identifier := codexQuotaHeaderIdentifier(name)
			if identifier == "" || seen[strings.ToLower(identifier)] {
				return true
			}
			rate := firstCodexQuotaValue(value, "rate_limit", "rateLimit")
			if !rate.Exists() {
				rate = value
			}
			prefix := "X-Codex-Additional-" + identifier + "-"
			before := len(headers)
			addCodexQuotaWindows(headers, prefix, rate)
			if len(headers) > before {
				headers.Set(prefix+"Limit-Name", strings.TrimSpace(name))
				id := codexQuotaScalar(firstCodexQuotaValue(value, "metered_feature", "meteredFeature", "limit_id", "limitId"))
				if identifier := codexQuotaHeaderIdentifier(id); identifier != "" && identifier == id {
					headers.Set(prefix+"Limit-Id", id)
				}
				seen[strings.ToLower(identifier)] = true
				count++
			}
			return true
		})
	}
	credits := root.Get("credits")
	setCodexQuotaScalar(headers, "X-Codex-Credits-Has-Credits", firstCodexQuotaValue(credits, "has_credits", "hasCredits"))
	setCodexQuotaScalar(headers, "X-Codex-Credits-Unlimited", credits.Get("unlimited"))
	setCodexQuotaScalar(headers, "X-Codex-Credits-Balance", credits.Get("balance"))
	if len(headers) == 0 {
		return nil
	}
	active := codexQuotaScalar(firstCodexQuotaValue(root, "metered_limit_name", "meteredLimitName", "limit_name", "limitName"))
	if identifier := codexQuotaHeaderIdentifier(active); identifier != "" && identifier == active {
		headers.Set("X-Codex-Active-Limit", active)
		if name := firstCodexQuotaValue(root, "limit_name", "limitName"); name.Exists() && codexQuotaScalar(name) != active {
			setCodexQuotaScalar(headers, "X-Codex-Limit-Name", name)
		}
	}
	setCodexQuotaScalar(headers, "X-Codex-Plan-Type", firstCodexQuotaValue(root, "plan_type", "planType"))
	return filteredCodexQuotaEventHeaders(headers)
}

func filteredCodexQuotaEventHeaders(headers http.Header) http.Header {
	signals := cliproxyauth.CollectCodexQuotaSignals(headers)
	if len(signals) == 0 {
		return nil
	}
	out := make(http.Header, len(signals))
	for key, value := range signals {
		out[key] = []string{value}
	}
	return out
}

func addCodexQuotaWindows(headers http.Header, prefix string, rate gjson.Result) {
	if !rate.IsObject() {
		return
	}
	setCodexQuotaScalar(headers, prefix+"Allowed", rate.Get("allowed"))
	setCodexQuotaScalar(headers, prefix+"Limit-Reached", firstCodexQuotaValue(rate, "limit_reached", "limitReached"))
	for _, name := range []string{"primary", "secondary"} {
		window := rate.Get(name)
		used := firstCodexQuotaValue(window, "used_percent", "usedPercent")
		minutes := firstCodexQuotaValue(window, "window_minutes", "windowMinutes")
		after := firstCodexQuotaValue(window, "reset_after_seconds", "resetAfterSeconds")
		at := firstCodexQuotaValue(window, "reset_at", "resetAt")
		percent, errPercent := strconv.ParseFloat(codexQuotaScalar(used), 64)
		windowMinutes, errMinutes := strconv.ParseInt(codexQuotaScalar(minutes), 10, 64)
		resetAfter, errAfter := strconv.ParseInt(codexQuotaScalar(after), 10, 64)
		resetAt, errAt := strconv.ParseInt(codexQuotaScalar(at), 10, 64)
		validAfter, validAt := errAfter == nil && resetAfter >= 0, errAt == nil && resetAt > 0
		if errPercent != nil || math.IsNaN(percent) || math.IsInf(percent, 0) || percent < 0 || percent > 100 || errMinutes != nil || windowMinutes <= 0 || (!validAfter && !validAt) {
			continue
		}
		windowPrefix := prefix + name + "-"
		setCodexQuotaScalar(headers, windowPrefix+"Used-Percent", used)
		setCodexQuotaScalar(headers, windowPrefix+"Window-Minutes", minutes)
		if validAfter {
			setCodexQuotaScalar(headers, windowPrefix+"Reset-After-Seconds", after)
		}
		if validAt {
			setCodexQuotaScalar(headers, windowPrefix+"Reset-At", at)
		}
	}
}

func firstCodexQuotaValue(node gjson.Result, paths ...string) gjson.Result {
	for _, path := range paths {
		if value := node.Get(path); value.Exists() && value.Type != gjson.Null {
			return value
		}
	}
	return gjson.Result{}
}

func codexQuotaScalar(value gjson.Result) string {
	var raw string
	switch value.Type {
	case gjson.String:
		raw = value.Str
	case gjson.Number, gjson.True, gjson.False:
		raw = value.Raw
	default:
		return ""
	}
	if !utf8.ValidString(raw) || len(raw) > 512 {
		return ""
	}
	for _, char := range raw {
		if char < 0x20 || char == 0x7f {
			return ""
		}
	}
	return strings.TrimSpace(raw)
}

func setCodexQuotaScalar(headers http.Header, name string, value gjson.Result) {
	if raw := codexQuotaScalar(value); raw != "" {
		headers.Set(name, raw)
	}
}

func codexQuotaHeaderIdentifier(name string) string {
	if name == "" || len(name) > 128 || !utf8.ValidString(name) {
		return ""
	}
	var out strings.Builder
	for _, char := range name {
		if char < 0x20 || char == 0x7f {
			return ""
		}
		if (char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') || (char >= '0' && char <= '9') || char == '-' || char == '_' || char == '.' {
			out.WriteRune(char)
		} else {
			out.WriteByte('-')
		}
	}
	return strings.Trim(out.String(), "-_.")
}
