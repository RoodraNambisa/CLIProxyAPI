package cliproxy

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	sdkauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/auth"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
)

func TestServiceAuthMaintenanceDirtyQueueDeduplicatesAndBatches(t *testing.T) {
	service := &Service{}
	for index := range 300 {
		id := fmt.Sprintf("auth-%03d", index)
		if !service.markAuthMaintenanceDirty(id) {
			t.Fatalf("first mark for %s was not queued", id)
		}
		if service.markAuthMaintenanceDirty(id) {
			t.Fatalf("duplicate mark for %s was queued", id)
		}
	}

	first := service.takeAuthMaintenanceDirtyIDs(authMaintenanceDirtyBatchSize)
	if len(first) != authMaintenanceDirtyBatchSize {
		t.Fatalf("first batch length = %d, want %d", len(first), authMaintenanceDirtyBatchSize)
	}
	if !service.hasAuthMaintenanceDirtyIDs() {
		t.Fatal("remaining dirty IDs were lost")
	}
	second := service.takeAuthMaintenanceDirtyIDs(authMaintenanceDirtyBatchSize)
	if len(second) != 44 {
		t.Fatalf("second batch length = %d, want 44", len(second))
	}
	if service.hasAuthMaintenanceDirtyIDs() {
		t.Fatal("dirty queue was not drained")
	}
	if !service.markAuthMaintenanceDirty(first[0]) {
		t.Fatal("processed ID could not be queued again")
	}
}

func TestServiceProcessDirtyAuthMaintenanceIDDisablesWithoutFullScan(t *testing.T) {
	authDir := t.TempDir()
	store := sdkauth.NewFileTokenStore()
	store.SetBaseDir(authDir)
	service := &Service{
		cfg:         &config.Config{AuthDir: authDir},
		coreManager: coreauth.NewManager(store, nil, nil),
	}
	path := filepath.Join(authDir, "dirty-disable.json")
	if err := os.WriteFile(path, []byte(`{"type":"claude"}`), 0o600); err != nil {
		t.Fatalf("write auth file: %v", err)
	}
	auth := &coreauth.Auth{
		ID:            "dirty-disable.json",
		Provider:      "claude",
		Status:        coreauth.StatusError,
		StatusMessage: "unauthorized",
		LastError:     &coreauth.Error{HTTPStatus: http.StatusUnauthorized, Message: "unauthorized"},
		FileName:      path,
		Attributes:    map[string]string{"path": path},
		Metadata:      map[string]any{"type": "claude"},
	}
	if _, err := service.coreManager.Register(context.Background(), auth); err != nil {
		t.Fatalf("register auth: %v", err)
	}
	service.markAuthMaintenanceDirty(auth.ID)
	cfg := config.AuthMaintenanceConfig{Enable: true, DisableStatusCodes: []int{http.StatusUnauthorized}}
	if processed := service.processDirtyAuthMaintenanceIDs(cfg, chatGPTWebDeadAuthDeletePolicy{}, authDir, 1); processed != 1 {
		t.Fatalf("processed dirty IDs = %d, want 1", processed)
	}
	current, ok := service.coreManager.GetByID(auth.ID)
	if !ok || current == nil || !current.Disabled || current.Status != coreauth.StatusDisabled {
		t.Fatalf("current auth = %#v, want disabled", current)
	}
	if service.hasQueuedAuthMaintenanceCandidates() {
		t.Fatal("disable-only policy unexpectedly queued a delete")
	}
}

func TestServiceProcessDirtyAuthMaintenanceIDQueuesDeadWebCredential(t *testing.T) {
	authDir := t.TempDir()
	store := sdkauth.NewFileTokenStore()
	store.SetBaseDir(authDir)
	service := &Service{
		cfg:         &config.Config{AuthDir: authDir},
		coreManager: coreauth.NewManager(store, nil, nil),
	}
	dead := chatGPTWebAutoDeleteTestAuth("dirty-dead-web.json", 0, coreauth.LifecycleStateDead)
	dead.FileName = filepath.Join(authDir, dead.ID)
	dead.Attributes["path"] = dead.FileName
	if err := os.WriteFile(dead.FileName, []byte(`{"type":"chatgpt-web"}`), 0o600); err != nil {
		t.Fatalf("write auth file: %v", err)
	}
	if _, err := service.coreManager.Register(context.Background(), dead); err != nil {
		t.Fatalf("register auth: %v", err)
	}
	service.markAuthMaintenanceDirty(dead.ID)
	policy := chatGPTWebDeadAuthDeletePolicy{enabled: true}
	if processed := service.processDirtyAuthMaintenanceIDs(config.AuthMaintenanceConfig{}, policy, authDir, 1); processed != 1 {
		t.Fatalf("processed dirty IDs = %d, want 1", processed)
	}
	current, ok := service.coreManager.GetByID(dead.ID)
	if !ok || current == nil || !authMaintenancePendingDelete(current) {
		t.Fatalf("current auth = %#v, want pending delete", current)
	}
	service.maintenanceMu.Lock()
	defer service.maintenanceMu.Unlock()
	if len(service.maintenanceQueue) != 1 || service.maintenanceQueue[0].Reason != "chatgpt_web_dead_account_deactivated" {
		t.Fatalf("maintenance queue = %#v", service.maintenanceQueue)
	}
}

func TestServiceMarkAuthMaintenanceChangeOnlyWhenPolicyEnabled(t *testing.T) {
	service := &Service{cfg: &config.Config{}}
	service.markAuthMaintenanceChange("disabled")
	if service.hasAuthMaintenanceDirtyIDs() {
		t.Fatal("disabled maintenance policy queued a dirty auth")
	}
	service.cfg.AuthMaintenance.Enable = true
	service.markAuthMaintenanceChange("enabled")
	if got := service.takeAuthMaintenanceDirtyIDs(1); len(got) != 1 || got[0] != "enabled" {
		t.Fatalf("dirty IDs = %#v, want enabled", got)
	}
}

func TestServiceProcessDirtyAuthMaintenanceIDUsesCurrentInstallation(t *testing.T) {
	authDir := t.TempDir()
	store := sdkauth.NewFileTokenStore()
	store.SetBaseDir(authDir)
	service := &Service{
		cfg:         &config.Config{AuthDir: authDir},
		coreManager: coreauth.NewManager(store, nil, nil),
	}
	path := filepath.Join(authDir, "dirty-replaced.json")
	if err := os.WriteFile(path, []byte(`{"type":"claude"}`), 0o600); err != nil {
		t.Fatalf("write auth file: %v", err)
	}
	stale := &coreauth.Auth{
		ID:         "dirty-replaced.json",
		Provider:   "claude",
		Status:     coreauth.StatusError,
		LastError:  &coreauth.Error{HTTPStatus: http.StatusUnauthorized, Message: "unauthorized"},
		FileName:   path,
		Attributes: map[string]string{"path": path},
		Metadata:   map[string]any{"type": "claude"},
	}
	installed, err := service.coreManager.Register(context.Background(), stale)
	if err != nil {
		t.Fatalf("register stale auth: %v", err)
	}
	service.markAuthMaintenanceDirty(installed.ID)

	replacement := installed.Clone()
	replacement.Status = coreauth.StatusActive
	replacement.LastError = nil
	replacement.Disabled = false
	replacement.Unavailable = false
	replaceCtx := coreauth.WithSkipStateCarryForward(
		coreauth.WithForceRuntimeReplacement(coreauth.WithSkipPersist(context.Background())),
	)
	if _, err = service.coreManager.Update(replaceCtx, replacement); err != nil {
		t.Fatalf("install replacement auth: %v", err)
	}
	cfg := config.AuthMaintenanceConfig{Enable: true, DisableStatusCodes: []int{http.StatusUnauthorized}}
	if processed := service.processDirtyAuthMaintenanceIDs(cfg, chatGPTWebDeadAuthDeletePolicy{}, authDir, 1); processed != 1 {
		t.Fatalf("processed dirty IDs = %d, want 1", processed)
	}
	current, ok := service.coreManager.GetByID(stale.ID)
	if !ok || current == nil || current.Disabled || current.Status != coreauth.StatusActive {
		t.Fatalf("replacement auth = %#v, want active", current)
	}
}
