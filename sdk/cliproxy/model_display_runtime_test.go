package cliproxy

import (
	"context"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

func TestDisplayNameHotReloadPreservesCredentialAndCooldown(t *testing.T) {
	for _, oauth := range []bool{false, true} {
		t.Run(map[bool]string{false: "api-key", true: "oauth"}[oauth], func(t *testing.T) {
			makeConfig := func(label string) *config.Config {
				if oauth {
					return &config.Config{OAuthModelAlias: map[string][]config.OAuthModelAlias{"codex": {{Name: "gpt-5.5", Alias: "display-live-alias", DisplayName: label}}}}
				}
				return &config.Config{CodexKey: []config.CodexKey{{APIKey: "fixture", Models: []config.CodexModel{{Name: "gpt-5.5", Alias: "display-live-alias", DisplayName: label}}}}}
			}
			manager := coreauth.NewManager(nil, nil, nil)
			a := &coreauth.Auth{ID: "display-runtime-auth", Provider: "codex", Status: coreauth.StatusActive, Metadata: map[string]any{"type": "codex"}}
			if !oauth {
				a.Attributes = map[string]string{"api_key": "fixture", "auth_kind": "apikey"}
			}
			a.Unavailable = true
			a.NextRetryAfter = time.Now().Add(time.Hour)
			installed, err := manager.Register(t.Context(), a)
			if err != nil {
				t.Fatal(err)
			}
			s := &Service{cfg: makeConfig("Before"), coreManager: manager}
			s.registerModelsForAuth(installed)
			r := registry.GetGlobalRegistry()
			r.SuspendClientModel(installed.ID, "display-live-alias", "fixture")
			t.Cleanup(func() { r.UnregisterClient(installed.ID) })
			label := func() string {
				for _, model := range r.GetModelsForClient(installed.ID) {
					if model.ID == "display-live-alias" {
						return model.DisplayName
					}
				}
				return ""
			}
			if label() != "Before" {
				t.Fatal("initial display label missing")
			}
			executionCtx, finish, ok := installed.BeginRuntimeExecution(t.Context())
			if !ok {
				t.Fatal("fixture execution could not start")
			}
			defer finish()
			if _, err := s.ApplyRuntimeConfig(t.Context(), makeConfig("After")); err != nil {
				t.Fatal(err)
			}
			if label() != "After" {
				t.Fatal("hot reload did not update catalog label")
			}
			readCtx, stopReading := context.WithCancel(t.Context())
			readDone := make(chan struct{})
			readFailure := make(chan struct{}, 1)
			go func() {
				defer close(readDone)
				for readCtx.Err() == nil {
					if got := label(); got != "After" && got != "Again" {
						readFailure <- struct{}{}
						return
					}
				}
			}()
			defer func() { stopReading(); <-readDone }()
			for index := range 8 {
				next := "Again"
				if index%2 == 1 {
					next = "After"
				}
				if _, err := s.ApplyRuntimeConfig(t.Context(), makeConfig(next)); err != nil {
					t.Fatal(err)
				}
			}
			stopReading()
			<-readDone
			select {
			case <-readFailure:
				t.Fatal("concurrent query observed an incomplete catalog")
			default:
			}
			current, _ := manager.GetByID(installed.ID)
			if current.RuntimeInstanceID() != installed.RuntimeInstanceID() || executionCtx.Err() != nil || !current.Unavailable || !current.NextRetryAfter.Equal(installed.NextRetryAfter) {
				t.Fatal("display edit changed active credential lifecycle or cooldown")
			}
			for _, model := range r.GetAvailableModels("openai") {
				if model["id"] == "display-live-alias" {
					t.Fatal("display edit cleared registry suspension")
				}
			}
			cancelled, cancel := context.WithCancel(t.Context())
			cancel()
			if _, err := s.ApplyRuntimeConfig(cancelled, makeConfig("Cancelled")); err == nil {
				t.Fatal("cancelled catalog update was reported successful")
			}
			if label() != "After" {
				t.Fatal("cancelled update failed to restore prior label")
			}
		})
	}
}
