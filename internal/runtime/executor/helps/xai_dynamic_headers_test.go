package helps

import (
	"net/http"
	"os"
	"sync"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

func TestXAIDynamicHeadersDefaultPriorityAndSnapshot(t *testing.T) {
	cfg := &config.Config{XAI: config.XAIConfig{Headers: map[string]string{"X-Client": "$X-Test", "X-Session": "prefix-$CPA-SESSION-ID", "X-Override": "$X-Test"}}}
	auth := &coreauth.Auth{Attributes: map[string]string{"header:X-Override": "credential"}}
	req := &http.Request{Header: make(http.Header)}
	ApplyXAIResourceHeaders(req, auth, cfg)
	if req.Header.Get("X-Client") != "$X-Test" {
		t.Fatal("dynamic headers enabled by default")
	}
	cfg.XAI.DynamicHeaders = true
	useHistory := false
	cfg.Routing.SessionAffinityUseHistory = &useHistory
	client := http.Header{"X-Test": []string{"first"}, "X-Session-Id": []string{"stable"}, "X-Grok-Turn-Idx": []string{"not-a-number"}}
	plan, err := NewXAIRequestPlan(t.Context(), cfg, core.Request{Payload: []byte(`{"input":"hello"}`)}, core.Options{Headers: client})
	if err != nil {
		t.Fatal(err)
	}
	client.Set("X-Test", "changed")
	ApplyXAIResourceHeaders(req, auth, cfg)
	if req.Header.Get("X-Client") != "" {
		t.Fatal("unresolved resource template sent literally")
	}
	ApplyXAIDynamicHeaders(req.Header, auth, plan.Config, plan.ClientHeaders, plan.Basis)
	if req.Header.Get("X-Client") != "first" || req.Header.Get("X-Override") != "credential" || req.Header.Get("X-Session") != "prefix-"+plan.Basis || plan.Basis == "" {
		t.Fatal("dynamic header snapshot or precedence changed")
	}
}

func TestXAIConfigKeySeedPersistsAcrossPreparationAndConcurrentReaders(t *testing.T) {
	directory := t.TempDir()
	cfg := &config.Config{AuthDir: directory}
	auth := &coreauth.Auth{ID: "stable-config-key", Attributes: map[string]string{"runtime_only": "true", "source": "config:xai[fixture]"}}
	plan := &XAIRequestPlan{Config: cfg}
	if _, err := plan.PrepareRequestAuth(t.Context(), auth); err != nil {
		t.Fatal(err)
	}
	files, _ := os.ReadDir(directory)
	if len(files) != 0 {
		t.Fatal("disabled identity created state")
	}
	cfg.XAI.SessionIdentityConvergence = true
	var wg sync.WaitGroup
	seeds := make(chan string, 8)
	for range 8 {
		wg.Go(func() {
			updated, err := plan.PrepareRequestAuth(t.Context(), auth)
			if err != nil {
				t.Error(err)
				return
			}
			seeds <- string(XAIIdentitySeed(updated))
		})
	}
	wg.Wait()
	close(seeds)
	var first string
	for seed := range seeds {
		if first == "" {
			first = seed
		}
		if first != seed || len(seed) != 32 {
			t.Fatal("concurrent identity changed")
		}
	}
	seed, err := persistentXAIKeySeed(directory, auth.ID)
	if err != nil || string(seed) != first {
		t.Fatal("saved identity changed after reconstruction")
	}
}
