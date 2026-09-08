package watcher

import (
	"reflect"
	"slices"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

// Labels refresh the runtime catalog separately and must not force credential
// replacement. Keep every existing routing field and nil/empty distinction.
func oauthModelAliasRoutingEqual(left, right map[string][]config.OAuthModelAlias) bool {
	withoutLabels := func(source map[string][]config.OAuthModelAlias) map[string][]config.OAuthModelAlias {
		if source == nil {
			return nil
		}
		result := make(map[string][]config.OAuthModelAlias, len(source))
		for provider, aliases := range source {
			cloned := slices.Clone(aliases)
			for index := range cloned {
				cloned[index].DisplayName = ""
			}
			result[provider] = cloned
		}
		return result
	}
	return reflect.DeepEqual(withoutLabels(left), withoutLabels(right))
}
