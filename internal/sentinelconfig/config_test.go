package sentinelconfig

import (
	"encoding/json"
	"gopkg.in/yaml.v3"
	"testing"
)

func TestRejectedPartialJSONDoesNotMutatePreviousSnapshot(t *testing.T) {
	scopes := []string{"images"}
	budget := 30
	remote := Remote{Scopes: &scopes, BudgetSeconds: &budget, Nodes: []Node{{Name: "original", URL: "https://example.com", APIKey: "key"}}}
	before, _ := json.Marshal(remote)
	if err := json.Unmarshal([]byte(`{"budget-seconds":12,"nodes":[{"name":"changed"}],"unknown":true}`), &remote); err == nil {
		t.Fatal("unknown field accepted")
	}
	after, _ := json.Marshal(remote)
	if string(before) != string(after) || budget != 30 {
		t.Fatal("rejected update mutated snapshot")
	}
	server := Server{MemoryBudgetMiB: &budget}
	if err := json.Unmarshal([]byte(`{"memory-budget-mib":1024,"unknown":true}`), &server); err == nil {
		t.Fatal("unknown field accepted")
	}
	if budget != 30 {
		t.Fatal("rejected server update mutated snapshot")
	}
}

func TestSolverAccessPathAndNodeURL(t *testing.T) {
	for _, path := range []string{"", "/v1/sentinel", "/afhkajf/Sentinel", "/private/solver_v2"} {
		cfg := Server{AccessPath: path}
		if err := cfg.Validate(); err != nil {
			t.Fatalf("valid path %q: %v", path, err)
		}
		data, err := yaml.Marshal(cfg)
		if err != nil {
			t.Fatal(err)
		}
		var loaded Server
		if err = yaml.Unmarshal(data, &loaded); err != nil || loaded.Path() != cfg.Path() {
			t.Fatalf("roundtrip %q: %v", path, err)
		}
	}
	for _, path := range []string{"/", "relative", "/trailing/", "/a//b", "/a/../b", "/a/%2f/b", "/a?key=x", "/a#b", "/a\\b", "/v1", "/v1/images", "/v0/management", "/healthz", "/api/solver"} {
		if err := (Server{AccessPath: path}).Validate(); err == nil {
			t.Errorf("accepted %q", path)
		}
	}
	for raw, expected := range map[string]string{
		"https://solver.example.com":                   "https://solver.example.com/v1/sentinel",
		"http://127.0.0.1:8317/":                       "http://127.0.0.1:8317/v1/sentinel",
		"https://solver.example.com/afhkajf/Sentinel/": "https://solver.example.com/afhkajf/Sentinel",
	} {
		if got, err := NodeBaseURL(raw); err != nil || got != expected {
			t.Errorf("%s -> %s (%v)", raw, got, err)
		}
	}
	for _, raw := range []string{"http://public.example.com/path", "https://x/p?", "https://x/%2fother", "https://x/a/../b"} {
		if _, err := NodeBaseURL(raw); err == nil {
			t.Errorf("accepted node URL %s", raw)
		}
	}
	for _, legacy := range []string{`{"listen":"127.0.0.1:8318"}`, `{"tls":{"enable":false}}`} {
		var cfg Server
		if err := json.Unmarshal([]byte(legacy), &cfg); err == nil {
			t.Fatal("removed listener setting accepted")
		}
	}
}
