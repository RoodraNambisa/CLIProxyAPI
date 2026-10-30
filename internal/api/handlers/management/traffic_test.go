package management

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/admission"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

func TestTrafficBatchUsesOpaqueIDsAndCapsRequests(t *testing.T) {
	m := coreauth.NewManager(nil, nil, nil)
	defer m.TrafficController().Close()
	m.SetConfig(&config.Config{SDKConfig: config.SDKConfig{APIKeys: []string{"private-test-key"}}})
	lease, err := m.TrafficController().Acquire(t.Context(), "private-test-key", 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Close()
	h := &Handler{authManager: m}
	ctx, w := newUsagePricingRequest(http.MethodPost, fmt.Sprintf(`{"ids":[%q]}`, admission.KeyID("private-test-key")))
	h.GetKeyTraffic(ctx)
	if w.Code != 200 || strings.Contains(w.Body.String(), "private-test-key") || !strings.Contains(w.Body.String(), `"active":1`) {
		t.Fatalf("unsafe/incomplete snapshot: %s", w.Body.String())
	}
	ids := make([]string, 201)
	body, _ := json.Marshal(map[string]any{"ids": ids})
	ctx, w = newUsagePricingRequest(http.MethodPost, string(body))
	h.GetCredentialTraffic(ctx)
	if w.Code != 400 {
		t.Fatalf("unbounded batch: %d", w.Code)
	}
}
func TestTrafficOverviewBoundsPointsAndValidatesWindow(t *testing.T) {
	m := coreauth.NewManager(nil, nil, nil)
	defer m.TrafficController().Close()
	h := &Handler{authManager: m}
	ctx, w := newUsagePricingRequest(http.MethodGet, "")
	ctx.Request.URL.RawQuery = "window_minutes=1440&basis=completed"
	h.GetTraffic(ctx)
	var result admission.Overview
	if json.Unmarshal(w.Body.Bytes(), &result) != nil || w.Code != 200 || len(result.Items) > 1200 || result.StartedAt.IsZero() {
		t.Fatalf("bad overview: %d", w.Code)
	}
	for _, query := range []string{"window_minutes=0", "window_minutes=1441", "basis=upstream", "mode=invalid"} {
		ctx, w = newUsagePricingRequest(http.MethodGet, "")
		ctx.Request.URL.RawQuery = query
		h.GetTraffic(ctx)
		if w.Code != 400 {
			t.Fatalf("accepted %s", query)
		}
	}
	if result.AsOf.After(time.Now().Add(time.Second)) {
		t.Fatal("future snapshot")
	}
}
func TestPatchKeyAdmissionRetainsUnrelatedAccessFields(t *testing.T) {
	cfg := &config.Config{SDKConfig: config.SDKConfig{APIKeys: []string{"fixture"}, APIKeyGroups: []config.APIKeyGroup{{APIKey: "fixture", Name: "named", Providers: []string{"codex"}}}}}
	h := &Handler{cfg: cfg, configFilePath: writeTestConfigFile(t)}
	ctx, w := newUsagePricingRequest(http.MethodPatch, `{"api-key":"fixture","concurrency-limit":2,"rpm-limit":10,"queue":{"enabled":true,"max-waiting":3,"wait-timeout-seconds":0}}`)
	h.PatchAPIKeyGroups(ctx)
	if w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	group := h.cfg.APIKeyGroups[0]
	if group.Name != "named" || len(group.Providers) != 1 || group.ConcurrencyLimit != 2 || group.RPMLimit != 10 || !group.Queue.Enabled {
		t.Fatalf("lost config: %+v", group)
	}
	ctx, w = newUsagePricingRequest(http.MethodPatch, `{"api-key":"fixture","concurrency-limit":-1}`)
	h.PatchAPIKeyGroups(ctx)
	if w.Code != 400 || h.cfg.APIKeyGroups[0].ConcurrencyLimit != 2 {
		t.Fatal("invalid config mutated active limits")
	}
}
