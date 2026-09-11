package live

import (
	"crypto/sha256"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	"github.com/tidwall/gjson"
)

func TestClientSecretKeepsConfiguredIssuerPriorityScope(t *testing.T) {
	cfg := &config.Config{SDKConfig: config.SDKConfig{APIKeys: []string{"fixture-issuer"}, APIKeyGroups: []config.APIKeyGroup{{APIKey: "fixture-issuer", AllowedPriorities: []int{1}}}}}
	cfg.Codex.LiveEnabled = true
	manager := auth.NewManager(nil, &auth.RoundRobinSelector{}, nil)
	manager.SetConfig(cfg)
	h := NewHandler(cfg, manager)
	defer h.Close()
	c, w := secretHandlerContext(t, `{"session":{"model":"voice"}}`)
	h.CreateClientSecret(c)
	if w.Code != 200 {
		t.Fatal("local client secret could not be created")
	}
	token := gjson.GetBytes(w.Body.Bytes(), "value").Str
	grant, err := h.clientSecrets.authenticate(token)
	if err != nil || grant.priorityScope != sha256.Sum256([]byte("fixture-issuer")) {
		t.Fatal("grant lost the configured issuer scope", err)
	}
	modified := grant.clone()
	modified.priorityScope = [sha256.Size]byte{}
	if h.clientSecrets.valid(modified) {
		t.Fatal("modified grant escaped its issuer restrictions")
	}
	request, _ := secretHandlerContext(t, "")
	if !h.ApplyClientSecretAuthorization(request, grant) {
		t.Fatal("valid grant rejected")
	}
	value, exists := request.Get(auth.ClientAPIKeyScopeContextKey)
	if !exists || value != grant.priorityScope {
		t.Fatal("authentication lost issuer priority scope")
	}
}
