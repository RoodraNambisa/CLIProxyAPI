package helps

import (
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

func TestResponseGuardEmptyStreamPreservesReadFailure(t *testing.T) {
	cfg := &config.Config{Codex: config.CodexConfig{ResponseGuard: config.CodexResponseGuardConfig{Enabled: true, CodexResponseGuardSettings: config.CodexResponseGuardSettings{Mode: new("enforce"), MissingModel: new("reject"), MissingState: new("reject")}}}}
	rejected := false
	g := NewCodexResponseGuard(t.Context(), cfg, &auth.Auth{ID: "a", Provider: "codex"}, "model", core.Options{}, true, "sse", 200, nil, func(config.CodexResponseEvidence) { rejected = true })
	for _, body := range []io.ReadCloser{io.NopCloser(strings.NewReader("")), io.NopCloser(strings.NewReader("data: {\"type\":\"response.created\"}\n\n"))} {
		replay, failure, err := ProbeCodexSSEBootstrap(t.Context(), body, func() bool { return false }, g)
		if err != nil || failure != nil || rejected {
			t.Fatalf("incomplete stream became a guard rejection: %v", err)
		}
		_, _ = io.ReadAll(replay)
		_ = replay.Close()
	}
}

func TestResponseGuardLateRecordKeepsPublicModel(t *testing.T) {
	c := config.CodexResponseGuardConfig{Enabled: true, CodexResponseGuardSettings: config.CodexResponseGuardSettings{Mode: new("enforce"), LateMismatch: new("abort")}}
	a := core.NewResponseGuardAttempt(c, "client-model", 1, nil)
	g := NewCodexResponseGuard(t.Context(), &config.Config{Codex: config.CodexConfig{ResponseGuard: c}}, &auth.Auth{ID: "a", Provider: "codex"}, "model", core.Options{ResponseGuard: a}, true, "sse", 200, nil, nil)
	if err := g.Observe([]byte(`{"type":"response.created","response":{"model":"model"}}`), false); err != nil {
		t.Fatal(err)
	}
	g.Commit()
	a.SetClientModel("public-model", false, 0)
	err := g.Observe([]byte(`{"type":"response.completed","response":{"model":"wrong","status":"completed"}}`), true)
	var rejected *core.ResponseGuardError
	if !errors.As(err, &rejected) || !rejected.Committed {
		t.Fatalf("late failure=%v", err)
	}
	r, _ := a.Snapshot()
	if r.ResponseModel != "public-model" || r.OriginalModel != "wrong" || r.Outcome != "aborted" {
		t.Fatalf("blocked model was falsely published: %+v", r)
	}
}

func TestResponseGuardDiagnosticUsesConfiguredAcceptanceWhenProductionDisabled(t *testing.T) {
	c := config.CodexResponseGuardConfig{Enabled: false, CodexResponseGuardSettings: config.CodexResponseGuardSettings{Mode: new("off"), AllowedReturnedModels: new([]string{"alternate"}), LengthMode: new("deny"), Lengths: new([]int{312})}}
	ctx := core.WithResponseGuardMode(core.WithSingleAttempt(t.Context()), "enforce")
	g := NewCodexResponseGuard(ctx, &config.Config{Codex: config.CodexConfig{ResponseGuard: c}}, &auth.Auth{ID: "a", Provider: "codex"}, "model", core.Options{}, false, "http", 200, nil, nil)
	if err := g.Observe([]byte(`{"type":"response.completed","response":{"model":"alternate","status":"completed"}}`), true); err != nil {
		t.Fatal(err)
	}
	if g.policy.OnReject != "error" || g.policy.LengthMode != "deny" {
		t.Fatal("diagnostic ignored settings or enabled retries")
	}
}

func TestResponseGuardUpstreamFailureHasUnknownVerdict(t *testing.T) {
	c := config.CodexResponseGuardConfig{Enabled: true, CodexResponseGuardSettings: config.CodexResponseGuardSettings{Mode: new("enforce")}}
	a := core.NewResponseGuardAttempt(c, "model", 1, nil)
	g := NewCodexResponseGuard(t.Context(), &config.Config{Codex: config.CodexConfig{ResponseGuard: c}}, &auth.Auth{ID: "a", Provider: "codex"}, "model", core.Options{ResponseGuard: a}, false, "http", 401, nil, nil)
	g.UpstreamFailure(401)
	r, _ := a.Snapshot()
	if r.Verdict.Model != "unknown" || r.Verdict.State != "unknown" || r.Verdict.Reasons == nil || r.Outcome != "upstream_error" || r.Status != 401 {
		t.Fatalf("upstream failure must not invent a guard verdict: %+v", r)
	}
}
