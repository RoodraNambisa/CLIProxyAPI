package config

import "testing"

func TestXAIModelRoutingValidation(t *testing.T) {
	for _, target := range []string{"", "unknown", "file:///secret", "https://user:secret@host/v1", "https://host/v1?token=secret"} {
		if _, err := NormalizeXAICatalogSources([]string{target}); err == nil {
			t.Errorf("accepted source %q", target)
		}
		if _, err := NormalizeXAIModelRoutes([]XAIModelRoute{{Models: []string{"grok-*"}, Upstream: target}}); err == nil {
			t.Errorf("accepted route %q", target)
		}
	}
	for _, patterns := range [][]string{nil, {""}, {"grok-?"}, {"one,two"}, {"[ab]"}} {
		if _, err := NormalizeXAIModelRoutes([]XAIModelRoute{{Models: patterns, Upstream: "api"}}); err == nil {
			t.Errorf("accepted patterns %v", patterns)
		}
	}
	if _, err := NormalizeXAICatalogSources(make([]string, 9)); err == nil {
		t.Fatal("accepted too many sources")
	}
	if _, err := NormalizeXAIModelRoutes(make([]XAIModelRoute, 65)); err == nil {
		t.Fatal("accepted too many rules")
	}
	routes, err := NormalizeXAIModelRoutes([]XAIModelRoute{{Models: []string{" grok-4.6 ", "grok-4.6", "grok-*"}, Upstream: " API "}})
	if err != nil || routes[0].Upstream != "api" || len(routes[0].Models) != 2 {
		t.Fatalf("normalization: %v %v", routes, err)
	}
}
