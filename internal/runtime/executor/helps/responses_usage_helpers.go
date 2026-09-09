package helps

import translatorcommon "github.com/router-for-me/CLIProxyAPI/v6/internal/translator/common"

// EnsureResponsesUsageDetails normalizes existing usage without inventing it.
func EnsureResponsesUsageDetails(payload []byte) []byte {
	return translatorcommon.EnsureResponsesUsageDetails(payload)
}
