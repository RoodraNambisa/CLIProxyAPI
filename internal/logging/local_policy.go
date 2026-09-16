package logging

import "errors"

// LocalPolicyReason reads trusted error metadata, never a public status or body.
// It is diagnostic-only and must not be used for routing, retry, or cooldown.
func LocalPolicyReason(err error) string {
	var source interface{ LocalPolicyReason() string }
	if !errors.As(err, &source) {
		return ""
	}
	reason := source.LocalPolicyReason()
	if len(reason) == 0 || len(reason) > 64 {
		return ""
	}
	for _, char := range reason {
		if (char < 'a' || char > 'z') && (char < '0' || char > '9') && char != '_' {
			return ""
		}
	}
	return reason
}
