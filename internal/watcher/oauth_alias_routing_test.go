package watcher

import (
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

func TestOAuthLabelDoesNotForceCredentialRefresh(t *testing.T) {
	left := map[string][]config.OAuthModelAlias{"codex": {{Name: "upstream", Alias: "alias", DisplayName: "Before"}}}
	for _, next := range []config.OAuthModelAlias{
		{Name: "upstream", Alias: "alias", DisplayName: "After"},
		{Name: "upstream", Alias: "alias"},
	} {
		if !oauthModelAliasRoutingEqual(left, map[string][]config.OAuthModelAlias{"codex": {next}}) {
			t.Fatal("label edit forced auth reload")
		}
	}
	if left["codex"][0].DisplayName != "Before" {
		t.Fatal("comparison modified saved labels")
	}
	for _, next := range []config.OAuthModelAlias{
		{Name: "changed", Alias: "alias"}, {Name: "upstream", Alias: "changed"},
		{Name: "upstream", Alias: "alias", Fork: true}, {Name: "upstream", Alias: "alias", ForceMapping: true},
	} {
		if oauthModelAliasRoutingEqual(left, map[string][]config.OAuthModelAlias{"codex": {next}}) {
			t.Fatal("routing edit failed to refresh auth")
		}
	}
	if oauthModelAliasRoutingEqual(nil, map[string][]config.OAuthModelAlias{}) {
		t.Fatal("comparison changed nil/empty semantics")
	}
}
