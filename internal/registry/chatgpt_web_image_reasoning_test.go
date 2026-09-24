package registry

import "testing"

func TestChatGPTWebImageThinkingModelUsesNativeCatalogEfforts(t *testing.T) {
	r := newTestModelRegistry()
	r.clientModelInfos["paid"] = map[string]*ModelInfo{
		"a-old": {ID: "a-old", ChatGPTWebThinkingEfforts: []string{"low", "medium", "high", "extra_high"}},
		"alias": {ID: "alias", UpstreamID: "preferred-thinking", ChatGPTWebThinkingDefault: true, ChatGPTWebThinkingEfforts: []string{"min", "standard", "extended", "max"}},
	}
	r.clientModelInfos["free"] = map[string]*ModelInfo{"instant": {ID: "instant", ChatGPTWebInstant: true}}
	for _, tc := range []struct{ mode, native, modern string }{{"low", "min", "low"}, {"medium", "standard", "medium"}, {"high", "extended", "high"}, {"xhigh", "max", "extra_high"}} {
		for range 10 {
			model, effort := r.ChatGPTWebImageThinkingModel("paid", "auto", tc.mode)
			if model != "preferred-thinking" || effort != tc.native {
				t.Fatalf("mode=%s model=%s effort=%s", tc.mode, model, effort)
			}
		}
		model, effort := r.ChatGPTWebImageThinkingModel("paid", "a-old", tc.mode)
		if model != "a-old" || effort != tc.modern {
			t.Fatalf("explicit carrier replaced: %s/%s", model, effort)
		}
		for _, client := range []string{"free", "unknown"} {
			if model, effort := r.ChatGPTWebImageThinkingModel(client, "auto", tc.mode); model != "" || effort != "" {
				t.Fatal("used another credential's capabilities")
			}
		}
	}
	if model, _ := r.ChatGPTWebImageThinkingModel("paid", "missing-carrier", "high"); model != "" {
		t.Fatal("silently replaced custom carrier")
	}
	clone := cloneModelInfo(r.clientModelInfos["paid"]["alias"])
	clone.ChatGPTWebThinkingEfforts[0] = "modified"
	if r.clientModelInfos["paid"]["alias"].ChatGPTWebThinkingEfforts[0] != "min" {
		t.Fatal("shared mutable catalog")
	}
}
