package registry

import "testing"

func TestNotFoundRoutesKeepProviderBoundaries(t *testing.T) {
	for _, reason := range []string{"not_found", "model_not_found"} {
		t.Run(reason, func(t *testing.T) {
			r := newTestModelRegistry()
			for _, entry := range []struct{ id, provider string }{{"recoverable", "codex"}, {"disabled", "codex"}, {"other", "claude"}} {
				r.RegisterClient(entry.id, entry.provider, []*ModelInfo{{ID: "shared", DisplayName: entry.provider}})
			}
			r.SuspendClientModel("recoverable", "shared", reason)
			r.SuspendClientModel("disabled", "shared", "unauthorized")
			r.SuspendClientModel("other", "shared", "disabled")
			if providers := r.GetModelProviders("shared"); len(providers) != 1 || providers[0] != "codex" {
				t.Fatalf("routes = %v, want only the recoverable provider", providers)
			}
			if models := r.GetAvailableModelsForProviders("openai", []string{"claude"}); len(models) != 0 {
				t.Fatal("not-found handling exposed an unavailable provider")
			}
			if models := r.GetAvailableModelsForProviders("openai", []string{"codex"}); len(models) != 1 {
				t.Fatal("recoverable provider was hidden by another disabled credential")
			}
			r.UnregisterClient("recoverable")
			if providers := r.GetModelProviders("shared"); len(providers) != 0 {
				t.Fatal("removed credentials retained a recoverable route")
			}
		})
	}
}

func TestNotFoundRoutesUseLatestSuspensionReason(t *testing.T) {
	r := newTestModelRegistry()
	r.RegisterClient("changing", "codex", []*ModelInfo{{ID: "changing-model"}})
	r.SuspendClientModel("changing", "changing-model", "unauthorized")
	if len(r.GetModelProviders("changing-model")) != 0 {
		t.Fatal("authentication suspension unexpectedly retained a route")
	}
	r.SuspendClientModel("changing", "changing-model", "model_not_found")
	if len(r.GetModelProviders("changing-model")) != 1 {
		t.Fatal("old suspension reason hid the new recoverable 404 route")
	}
	if len(r.GetAvailableModels("openai")) != 1 {
		t.Fatal("cached catalog retained the old suspension reason")
	}
	r.SuspendClientModel("changing", "changing-model", "disabled")
	if len(r.GetModelProviders("changing-model")) != 0 || len(r.GetAvailableModels("openai")) != 0 {
		t.Fatal("new nonrecoverable suspension kept the old 404 route")
	}
}
