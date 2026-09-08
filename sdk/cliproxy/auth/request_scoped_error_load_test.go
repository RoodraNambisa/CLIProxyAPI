package auth

import (
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

func TestRequestScopedErrorStoreLoadAndProjection(t *testing.T) {
	for _, path := range []string{"", "rules.json"} {
		t.Run(path, func(t *testing.T) {
			valid := authWithRequestScopedRules("stop")
			valid.FileName = path
			bad := authWithRequestScopedRules("invalid")
			bad.ID, bad.FileName = "bad-rules", path
			store := &lifecycleLoadStore{auths: []*Auth{valid, bad, {ID: "legacy", Provider: "codex"}}}
			m := NewManager(store, nil, nil)
			report, err := m.LoadWithReport(t.Context())
			if err != nil || report.Scanned != 3 || report.Loaded != 2 || report.Skipped != 1 {
				t.Fatalf("load report: %+v %v", report, err)
			}
			loaded, ok := m.GetByID(valid.ID)
			if !ok || loaded.requestScopedErrorRules == nil {
				t.Fatal("store load omitted rule preparation")
			}
			if action, _ := loaded.requestScopedErrorRules.rules.Match(400, "fixture"); action != config.RequestScopedActionStop {
				t.Fatal("loaded rules did not match")
			}
			legacy, _ := m.GetByID("legacy")
			if legacy.requestScopedErrorRules != nil {
				t.Fatal("old credential enabled a rule policy")
			}
			if _, ok := m.GetByID("bad-rules"); ok {
				t.Fatal("invalid stored rules became selectable")
			}
		})
	}
}

func TestRequestScopedErrorProjectionRejectsBeforeIdentityMutation(t *testing.T) {
	bad := authWithRequestScopedRules("invalid")
	if err := ApplyFileAuthProjection(bad, FileAuthProjectionOptions{Path: "different.json"}); err == nil {
		t.Fatal("projection accepted invalid rules")
	}
	if bad.ID != "rule-lifecycle" || bad.FileName != "" || bad.Attributes != nil {
		t.Fatal("rejected projection changed credential identity")
	}
}
