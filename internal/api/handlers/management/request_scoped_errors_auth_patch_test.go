package management

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	sdkAuth "github.com/router-for-me/CLIProxyAPI/v6/sdk/auth"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

func TestAuthFileErrorRulesPatchAliasesAndClear(t *testing.T) {
	for _, batch := range []bool{false, true} {
		for _, key := range []string{"request_scoped_errors", "request-scoped-errors"} {
			t.Run(key+map[bool]string{false: "/legacy", true: "/batch"}[batch], func(t *testing.T) {
				manager := coreauth.NewManager(&memoryAuthStore{}, nil, nil)
				for _, id := range []string{"first.json", "second.json"} {
					provider := "codex"
					if id == "second.json" {
						provider = "claude"
					}
					_, err := manager.Register(t.Context(), &coreauth.Auth{ID: id, FileName: id, Provider: provider, Metadata: map[string]any{"type": provider, "refresh_token": "test-refresh", "future": "keep"}})
					if err != nil {
						t.Fatal(err)
					}
				}
				h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: t.TempDir()}, manager)
				patch := func(fields string) int {
					body := `{"name":"first.json",` + fields + `}`
					if batch {
						body = `{"names":["first.json","second.json"],"fields":{` + fields + `}}`
					}
					r := httptest.NewRecorder()
					c, _ := gin.CreateTestContext(r)
					c.Request = httptest.NewRequest(http.MethodPatch, "/auth-files/fields", strings.NewReader(body))
					c.Request.Header.Set("Content-Type", "application/json")
					h.PatchAuthFileFields(c)
					return r.Code
				}
				rule := `[{"status":500,"match":[" exact "],"match-regexr":["(?i)busy"],"action":"stop","future":9007199254740993}]`
				if status := patch(`"` + key + `":` + rule); status != http.StatusOK {
					t.Fatalf("valid patch status %d", status)
				}
				ids := []string{"first.json"}
				if batch {
					ids = append(ids, "second.json")
				}
				for _, id := range ids {
					current, _ := manager.GetByID(id)
					encoded, _ := json.Marshal(current.Metadata["request_scoped_errors"])
					if !strings.Contains(string(encoded), `9007199254740993`) || !strings.Contains(string(encoded), `" exact "`) {
						t.Fatal("rule data was changed")
					}
					if _, exists := current.Metadata["request-scoped-errors"]; exists {
						t.Fatal("legacy rule alias was retained")
					}
				}
				before, _ := manager.GetByID("first.json")
				for _, invalid := range []string{`true`, `{}`, `[{"status":1.5}]`, `[{"status":500,"match":[true],"action":"stop"}]`, `[{"status":500,"match-regexr":["[private"],"action":"stop"}]`, `[{"status":500,"match":["x"],"action":"invalid"}]`} {
					if status := patch(`"` + key + `":` + invalid + `,"prefix":"must-not-apply"`); status != http.StatusBadRequest {
						t.Fatalf("invalid patch status %d", status)
					}
					current, _ := manager.GetByID("first.json")
					if !reflect.DeepEqual(current.Metadata, before.Metadata) || current.Prefix != before.Prefix {
						t.Fatal("invalid patch partially updated auth")
					}
				}
				if patch(`"request_scoped_errors":null,"request-scoped-errors":`+rule) != http.StatusOK {
					t.Fatal("canonical clear failed")
				}
				current, _ := manager.GetByID("first.json")
				if _, exists := current.Metadata["request_scoped_errors"]; exists {
					t.Fatal("clear did not remove canonical rules")
				}
				if _, exists := current.Metadata["request-scoped-errors"]; exists {
					t.Fatal("clear resurrected alias rules")
				}
				if current.Metadata["refresh_token"] != "test-refresh" || current.Metadata["future"] != "keep" {
					t.Fatal("unrelated credential data changed")
				}
				if patch(`"request_scoped_errors":null,"request-scoped-errors":[{"status":500}]`) != http.StatusBadRequest {
					t.Fatal("invalid shadowed alias was accepted")
				}
				if patch(`"`+key+`":[]`) != http.StatusOK {
					t.Fatal("empty rules failed")
				}
			})
		}
	}
}

func TestAuthFileErrorRulesPatchPreservesCurrentTokensAndPersists(t *testing.T) {
	dir := t.TempDir()
	store := sdkAuth.NewFileTokenStore()
	store.SetBaseDir(dir)
	manager := coreauth.NewManager(store, nil, nil)
	stale, err := manager.Register(t.Context(), &coreauth.Auth{ID: "rules.json", FileName: "rules.json", Provider: "codex", Metadata: map[string]any{"type": "codex", "access_token": "test-before", "future": "keep"}})
	if err != nil {
		t.Fatal(err)
	}
	advanced := stale.Clone()
	advanced.Metadata["access_token"] = "test-after"
	if _, current, errUpdate := manager.UpdateIfCurrentSourceHash(t.Context(), stale, advanced); errUpdate != nil || !current {
		t.Fatal("failed to advance fixture")
	}
	values, err := decodeAuthFileFieldValues(json.RawMessage(`{"request_scoped_errors":[{"status":500,"match":["busy"],"action":"stop","future":9007199254740993}]}`))
	if err != nil {
		t.Fatal(err)
	}
	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: dir}, manager)
	updated, status, _ := h.updateAuthFileFields(t.Context(), stale, values)
	if status != http.StatusOK || updated == nil {
		t.Fatalf("update status %d", status)
	}
	data, err := os.ReadFile(filepath.Join(dir, stale.FileName))
	if err != nil {
		t.Fatal(err)
	}
	if updated.Metadata["access_token"] != "test-after" || !strings.Contains(string(data), `"test-after"`) || !strings.Contains(string(data), `9007199254740993`) {
		t.Fatal("rules replaced current auth data or lost exact values")
	}
	clear, err := decodeAuthFileFieldValues(json.RawMessage(`{"request_scoped_errors":null}`))
	if err != nil {
		t.Fatal(err)
	}
	unlock, err := h.chatGPTWebDependencyMu.lock(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	_, status, _ = h.updateAuthFileFields(cancelled, updated, clear)
	unlock()
	if status != http.StatusRequestTimeout {
		t.Fatalf("cancelled status %d", status)
	}
	afterCancel, err := os.ReadFile(filepath.Join(dir, stale.FileName))
	if err != nil || string(afterCancel) != string(data) {
		t.Fatal("cancelled update changed persisted rules")
	}
	if _, status, _ := h.updateAuthFileFields(t.Context(), updated, clear); status != http.StatusOK {
		t.Fatalf("clear status %d", status)
	}
	cleared, err := os.ReadFile(filepath.Join(dir, stale.FileName))
	if err != nil || strings.Contains(string(cleared), "request_scoped_errors") || strings.Contains(string(cleared), "request-scoped-errors") {
		t.Fatal("cleared rules reappeared on disk")
	}
}
