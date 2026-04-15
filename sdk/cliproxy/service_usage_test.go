package cliproxy

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	internalusage "github.com/router-for-me/CLIProxyAPI/v6/internal/usage"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/watcher"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	coreusage "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/usage"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
)

func TestServiceReconcileUsageStatistics_PreservesDisabledRuntimeOnlyAuthRecords(t *testing.T) {
	stats := internalusage.NewRequestStatistics()
	service := &Service{
		cfg:         &config.Config{UsageStatisticsEnabled: true},
		coreManager: coreauth.NewManager(nil, nil, nil),
		usageStats:  stats,
	}

	auth := &coreauth.Auth{
		ID:         "runtime-only-disabled-auth",
		Provider:   "claude",
		Disabled:   true,
		Status:     coreauth.StatusDisabled,
		Attributes: map[string]string{"runtime_only": "true"},
	}
	if _, errRegister := service.coreManager.Register(context.Background(), auth); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}

	stats.Record(context.Background(), coreusage.Record{
		APIKey:      "test-key",
		Model:       "gpt-5.4",
		RequestedAt: time.Date(2026, time.March, 20, 12, 0, 0, 0, time.UTC),
		Detail: coreusage.Detail{
			TotalTokens: 10,
		},
		AuthIndex: auth.EnsureIndex(),
	})

	if removed := service.reconcileUsageStatistics("test"); removed != 0 {
		t.Fatalf("reconcileUsageStatistics() removed = %d, want 0", removed)
	}

	snapshot := stats.Snapshot()
	if snapshot.TotalRequests != 1 {
		t.Fatalf("total_requests = %d, want 1", snapshot.TotalRequests)
	}
	details := snapshot.APIs["test-key"].Models["gpt-5.4"].Details
	if len(details) != 1 || details[0].AuthIndex != auth.EnsureIndex() {
		t.Fatalf("remaining details = %+v, want runtime-only auth record preserved", details)
	}
}

func TestServiceReconcileUsageStatistics_PreservesFileBackedAuthDuringMissingMirrorWindow(t *testing.T) {
	stats := internalusage.NewRequestStatistics()
	service := &Service{
		cfg:         &config.Config{UsageStatisticsEnabled: true},
		coreManager: coreauth.NewManager(nil, nil, nil),
		usageStats:  stats,
	}

	auth := &coreauth.Auth{
		ID:       "file-backed-auth",
		Provider: "claude",
		Attributes: map[string]string{
			"path": filepath.Join(t.TempDir(), "auth.json"),
		},
	}
	if _, errRegister := service.coreManager.Register(context.Background(), auth); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}

	stats.Record(context.Background(), coreusage.Record{
		APIKey:      "test-key",
		Model:       "gpt-5.4",
		RequestedAt: time.Date(2026, time.March, 20, 12, 0, 0, 0, time.UTC),
		Detail: coreusage.Detail{
			TotalTokens: 10,
		},
		AuthIndex: auth.EnsureIndex(),
	})

	if removed := service.reconcileUsageStatistics("test"); removed != 0 {
		t.Fatalf("reconcileUsageStatistics() removed = %d, want 0", removed)
	}

	snapshot := stats.Snapshot()
	if snapshot.TotalRequests != 1 {
		t.Fatalf("total_requests = %d, want 1", snapshot.TotalRequests)
	}
	details := snapshot.APIs["test-key"].Models["gpt-5.4"].Details
	if len(details) != 1 || details[0].AuthIndex != auth.EnsureIndex() {
		t.Fatalf("remaining details = %+v, want file-backed auth record preserved while auth is still registered", details)
	}
}

func TestServiceHandleAuthUpdateDelete_RemovesRuntimeAuthAndUsage(t *testing.T) {
	stats := internalusage.NewRequestStatistics()
	authDir := t.TempDir()
	service := &Service{
		cfg:         &config.Config{UsageStatisticsEnabled: true, AuthDir: authDir},
		coreManager: coreauth.NewManager(nil, nil, nil),
		usageStats:  stats,
	}

	auth := &coreauth.Auth{
		ID:       "deleted-auth",
		Provider: "claude",
		FileName: filepath.Join(authDir, "deleted-auth.json"),
		Metadata: map[string]any{"type": "claude"},
	}
	if _, errRegister := service.coreManager.Register(context.Background(), auth); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}

	stats.Record(context.Background(), coreusage.Record{
		APIKey:      "test-key",
		Model:       "gpt-5.4",
		RequestedAt: time.Date(2026, time.March, 20, 12, 0, 0, 0, time.UTC),
		Detail: coreusage.Detail{
			TotalTokens: 10,
		},
		AuthIndex: auth.EnsureIndex(),
	})

	service.handleAuthUpdate(coreauth.WithSkipPersist(context.Background()), watcher.AuthUpdate{
		Action: watcher.AuthUpdateActionDelete,
		ID:     auth.ID,
	})

	if _, ok := service.coreManager.GetByID(auth.ID); ok {
		t.Fatalf("expected auth %q to be removed from runtime state", auth.ID)
	}
	snapshot := stats.Snapshot()
	if snapshot.TotalRequests != 0 {
		t.Fatalf("total_requests = %d, want 0 after deleting auth", snapshot.TotalRequests)
	}
}

func TestServicePersistUsageStatistics_DisableWritesFinalSnapshotWithoutExistingFile(t *testing.T) {
	baseDir := t.TempDir()
	t.Setenv("WRITABLE_PATH", baseDir)

	stats := internalusage.NewRequestStatistics()
	service := &Service{
		cfg: &config.Config{
			UsageStatisticsEnabled:                false,
			UsageStatisticsPersistIntervalSeconds: 0,
		},
		usageStats: stats,
	}

	stats.Record(context.Background(), coreusage.Record{
		APIKey:      "disable-key",
		Model:       "gpt-5.4",
		RequestedAt: time.Date(2026, time.March, 21, 8, 0, 0, 0, time.UTC),
		Detail: coreusage.Detail{
			TotalTokens: 42,
		},
	})

	path := service.usageStatisticsFilePath()
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("expected no pre-existing usage snapshot, stat err=%v", err)
	}

	service.persistUsageStatistics("disable")

	if _, err := os.Stat(path); err != nil {
		t.Fatalf("expected disable persistence to create snapshot file, stat err=%v", err)
	}
	snapshot, err := internalusage.LoadSnapshotFile(path)
	if err != nil {
		t.Fatalf("LoadSnapshotFile() error = %v", err)
	}
	if snapshot.TotalRequests != 1 {
		t.Fatalf("total_requests = %d, want 1", snapshot.TotalRequests)
	}
	if snapshot.TotalTokens != 42 {
		t.Fatalf("total_tokens = %d, want 42", snapshot.TotalTokens)
	}
}

func TestServicePersistUsageStatistics_DisabledTickWithoutSnapshotDoesNotCreateFile(t *testing.T) {
	baseDir := t.TempDir()
	t.Setenv("WRITABLE_PATH", baseDir)

	stats := internalusage.NewRequestStatistics()
	service := &Service{
		cfg: &config.Config{
			UsageStatisticsEnabled:                false,
			UsageStatisticsPersistIntervalSeconds: 0,
		},
		usageStats: stats,
	}

	stats.Record(context.Background(), coreusage.Record{
		APIKey:      "tick-key",
		Model:       "gpt-5.4",
		RequestedAt: time.Date(2026, time.March, 21, 9, 0, 0, 0, time.UTC),
		Detail: coreusage.Detail{
			TotalTokens: 21,
		},
	})

	path := service.usageStatisticsFilePath()
	service.persistUsageStatistics("tick")

	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("expected disabled periodic persistence not to create snapshot file, stat err=%v", err)
	}
}

func TestServicePersistUsageStatistics_DisabledAutoPathsDoNotRewriteExistingSnapshot(t *testing.T) {
	baseDir := t.TempDir()
	t.Setenv("WRITABLE_PATH", baseDir)

	stats := internalusage.NewRequestStatistics()
	service := &Service{
		cfg: &config.Config{
			UsageStatisticsEnabled:                true,
			UsageStatisticsPersistIntervalSeconds: 0,
		},
		usageStats: stats,
	}

	path := service.usageStatisticsFilePath()
	existingSnapshot := internalusage.StatisticsSnapshot{
		TotalRequests: 7,
		TotalTokens:   70,
	}
	if err := internalusage.SaveSnapshotFile(path, existingSnapshot); err != nil {
		t.Fatalf("SaveSnapshotFile() error = %v", err)
	}

	stats.Record(context.Background(), coreusage.Record{
		APIKey:      "startup-key",
		Model:       "gpt-5.4",
		RequestedAt: time.Date(2026, time.March, 21, 10, 0, 0, 0, time.UTC),
		Detail: coreusage.Detail{
			TotalTokens: 11,
		},
	})

	service.persistUsageStatistics("startup-reconcile")

	snapshot, err := internalusage.LoadSnapshotFile(path)
	if err != nil {
		t.Fatalf("LoadSnapshotFile() error = %v", err)
	}
	if snapshot.TotalRequests != existingSnapshot.TotalRequests {
		t.Fatalf("total_requests = %d, want existing snapshot %d", snapshot.TotalRequests, existingSnapshot.TotalRequests)
	}
	if snapshot.TotalTokens != existingSnapshot.TotalTokens {
		t.Fatalf("total_tokens = %d, want existing snapshot %d", snapshot.TotalTokens, existingSnapshot.TotalTokens)
	}
}
