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
