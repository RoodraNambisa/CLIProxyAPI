package auth

import (
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	"github.com/tidwall/gjson"
)

func TestLegacyCooldownErrorRetainsRequestedModel(t *testing.T) {
	previous := quotaCooldownDisabled.Load()
	quotaCooldownDisabled.Store(false)
	t.Cleanup(func() { quotaCooldownDisabled.Store(previous) })
	for _, mixed := range []bool{false, true} {
		for name, selector := range map[string]Selector{"round-robin": &RoundRobinSelector{}, "weighted": &WeightedRoundRobinSelector{}, "fill-first": &FillFirstSelector{}, "random": &RandomSelector{}} {
			t.Run(fmt.Sprintf("%s/mixed=%t", name, mixed), func(t *testing.T) {
				manager := NewManager(nil, selector, nil)
				manager.RegisterExecutor(&authFallbackExecutor{id: "codex"})
				manager.RegisterExecutor(&authFallbackExecutor{id: "openai"})
				model := "route-" + uuid.NewString()
				credential := &Auth{ID: uuid.NewString(), Provider: "codex", Unavailable: true, NextRetryAfter: time.Now().Add(time.Minute), Quota: QuotaState{Exceeded: true}}
				registerFallbackAuthForModel(t, manager, credential, model)
				var err error
				if mixed {
					_, _, _, err = manager.pickNextMixedLegacy(t.Context(), []string{"codex", "openai"}, model, core.Options{}, nil, nil)
				} else {
					_, _, err = manager.pickNextLegacy(t.Context(), "codex", model, core.Options{}, nil)
				}
				var cooldown *modelCooldownError
				if !errors.As(err, &cooldown) {
					t.Fatalf("expected cooldown: %v", err)
				}
				if cooldown.model != model || gjson.Get(err.Error(), "error.model").String() != model {
					t.Fatal("cooldown lost the requested route model")
				}
			})
		}
	}
}

func TestRestoreCooldownModelKeepsCauseAndBoundaries(t *testing.T) {
	original := newModelCooldownError("", "codex", 1234*time.Millisecond)
	wrapped := fmt.Errorf("selection: %w", original)
	restored := restoreModelCooldownErrorModel(wrapped, "requested")
	var cooldown *modelCooldownError
	if !errors.As(restored, &cooldown) || cooldown == original || cooldown.model != "requested" || original.model != "" {
		t.Fatal("restoration mutated the source error")
	}
	if !errors.Is(restored, wrapped) || !errors.Is(restored, original) || cooldown.resetIn != original.resetIn || cooldown.provider != original.provider || !reflect.DeepEqual(cooldown.Headers(), original.Headers()) {
		t.Fatal("restoration lost cause or cooldown metadata")
	}
	for _, tc := range []struct {
		err   error
		model string
	}{
		{nil, "model"}, {original, ""}, {newModelCooldownError("existing", "codex", time.Second), "different"},
		{(*modelCooldownError)(nil), "model"},
		{errors.New("unrelated"), "model"},
		{&cooldownModelStatusWrapper{error: original, status: http.StatusUnauthorized}, "model"},
	} {
		if got := restoreModelCooldownErrorModel(tc.err, tc.model); got != tc.err {
			t.Fatal("nonmatching error was changed")
		}
	}
}

type cooldownModelStatusWrapper struct {
	error
	status int
}

func (e *cooldownModelStatusWrapper) StatusCode() int { return e.status }
func (e *cooldownModelStatusWrapper) Unwrap() error   { return e.error }
