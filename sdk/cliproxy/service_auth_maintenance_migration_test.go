package cliproxy

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/authfileguard"
)

func TestMigrateLegacyAuthMaintenanceFilesRestoresMissingOriginal(t *testing.T) {
	authDir := t.TempDir()
	original := filepath.Join(authDir, "restored.json")
	staged := original + ".auth-maintenance.123"
	contents := []byte(`{"type":"chatgpt-web","auth_maintenance_pending_delete":true}`)
	if errWrite := os.WriteFile(staged, contents, 0o600); errWrite != nil {
		t.Fatal(errWrite)
	}
	if errMigration := migrateLegacyAuthMaintenanceFiles(authDir); errMigration != nil {
		t.Fatal(errMigration)
	}
	got, errRead := os.ReadFile(original)
	if errRead != nil || string(got) != string(contents) {
		t.Fatalf("restored contents = %q, err=%v", got, errRead)
	}
	if _, errStat := os.Stat(staged); !errors.Is(errStat, os.ErrNotExist) {
		t.Fatalf("legacy staged file still exists: %v", errStat)
	}
}

func TestMigrateLegacyAuthMaintenanceFilesRemovesIdenticalDuplicate(t *testing.T) {
	authDir := t.TempDir()
	original := filepath.Join(authDir, "duplicate.json")
	staged := original + ".auth-maintenance.456"
	contents := []byte(`{"type":"claude"}`)
	for _, path := range []string{original, staged} {
		if errWrite := os.WriteFile(path, contents, 0o600); errWrite != nil {
			t.Fatal(errWrite)
		}
	}
	if errMigration := migrateLegacyAuthMaintenanceFiles(authDir); errMigration != nil {
		t.Fatal(errMigration)
	}
	if _, errStat := os.Stat(original); errStat != nil {
		t.Fatalf("original was removed: %v", errStat)
	}
	if _, errStat := os.Stat(staged); !errors.Is(errStat, os.ErrNotExist) {
		t.Fatalf("duplicate staged file still exists: %v", errStat)
	}
}

func TestMigrateLegacyAuthMaintenanceFilesQuarantinesConflict(t *testing.T) {
	authDir := t.TempDir()
	original := filepath.Join(authDir, "conflict.json")
	staged := original + ".auth-maintenance.789"
	if errWrite := os.WriteFile(original, []byte(`{"version":"new"}`), 0o600); errWrite != nil {
		t.Fatal(errWrite)
	}
	if errWrite := os.WriteFile(staged, []byte(`{"version":"old"}`), 0o600); errWrite != nil {
		t.Fatal(errWrite)
	}
	t.Cleanup(func() {
		authfileguard.ClearQuarantined(original)
		authfileguard.ClearQuarantined(staged)
	})
	if errMigration := migrateLegacyAuthMaintenanceFiles(authDir); errMigration == nil {
		t.Fatal("conflicting legacy files were accepted")
	}
	if !authfileguard.IsQuarantined(original) || !authfileguard.IsQuarantined(staged) {
		t.Fatalf("conflicting paths were not quarantined: original=%v staged=%v", authfileguard.IsQuarantined(original), authfileguard.IsQuarantined(staged))
	}
	if _, errStat := os.Stat(original); errStat != nil {
		t.Fatalf("newer original was changed: %v", errStat)
	}
	if _, errStat := os.Stat(staged); errStat != nil {
		t.Fatalf("conflicting staged file was removed: %v", errStat)
	}
}

func TestMigrateLegacyAuthMaintenanceFilesQuarantinesInvalidStagedFile(t *testing.T) {
	authDir := t.TempDir()
	staged := filepath.Join(authDir, "invalid.json.auth-maintenance.999")
	if errWrite := os.WriteFile(staged, []byte(`not-json`), 0o600); errWrite != nil {
		t.Fatal(errWrite)
	}
	t.Cleanup(func() { authfileguard.ClearQuarantined(staged) })
	if errMigration := migrateLegacyAuthMaintenanceFiles(authDir); errMigration == nil {
		t.Fatal("invalid legacy staged file was accepted")
	}
	if !authfileguard.IsQuarantined(staged) {
		t.Fatal("invalid legacy staged file was not quarantined")
	}
}
