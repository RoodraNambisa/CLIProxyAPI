package auth

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/auth/codex"
)

func TestCodexOAuthRecordsSeparateAccountsWithSameEmailAndPlan(t *testing.T) {
	for _, plan := range []string{"free", "plus", "pro", "team", "k12", ""} {
		t.Run(plan, func(t *testing.T) {
			var previous string
			for _, account := range []string{"account-one", "account-two"} {
				claims, _ := json.Marshal(map[string]any{"https://api.openai.com/auth": map[string]string{"chatgpt_account_id": account, "chatgpt_plan_type": plan}})
				token := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"none"}`)) + "." + base64.RawURLEncoding.EncodeToString(claims) + ".fixture"
				bundle := &codex.CodexAuthBundle{TokenData: codex.CodexTokenData{IDToken: token, Email: "same@example.com", AccountID: account}}
				record, err := (&CodexAuthenticator{}).buildAuthRecord(&codex.CodexAuth{}, bundle)
				if err != nil {
					t.Fatal(err)
				}
				digest := fmt.Sprintf("%x", sha256.Sum256([]byte(account)))[:8]
				want := "codex-" + digest + "-same@example.com"
				if plan != "" {
					want += "-" + plan
				}
				want += ".json"
				if record.FileName != want || record.ID != want || record.FileName == previous {
					t.Fatal("OAuth records sharing an email still overwrite one filename")
				}
				previous = record.FileName
			}
		})
	}
	record, err := (&CodexAuthenticator{}).buildAuthRecord(&codex.CodexAuth{}, &codex.CodexAuthBundle{TokenData: codex.CodexTokenData{Email: "legacy@example.com"}})
	if err != nil || record.FileName != "codex-legacy@example.com.json" {
		t.Fatal("legacy identity metadata fallback changed")
	}
}
