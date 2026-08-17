package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadConfigUsagePersistenceDefaultsAndOverrides(t *testing.T) {
	if !(&Config{}).UsageStatisticsPersistence() {
		t.Fatal("programmatic zero-value config did not preserve legacy persistence behavior")
	}
	path := filepath.Join(t.TempDir(), "config.yaml")
	if errWrite := os.WriteFile(path, []byte("usage-statistics-enabled: true\n"), 0o600); errWrite != nil {
		t.Fatalf("WriteFile() error = %v", errWrite)
	}
	cfg, errLoad := LoadConfig(path)
	if errLoad != nil {
		t.Fatalf("LoadConfig() error = %v", errLoad)
	}
	if !cfg.UsageStatisticsPersistence() {
		t.Fatal("missing usage-statistics-persistence-enabled did not preserve legacy enabled behavior")
	}

	raw := `usage-statistics-persistence-enabled: false
usage-statistics-persist-interval-seconds: -1
usage-statistics-detail-retention-days: -2
usage-statistics-max-storage-megabytes: -3
`
	if errWrite := os.WriteFile(path, []byte(raw), 0o600); errWrite != nil {
		t.Fatalf("WriteFile() error = %v", errWrite)
	}
	cfg, errLoad = LoadConfig(path)
	if errLoad != nil {
		t.Fatalf("LoadConfig() error = %v", errLoad)
	}
	if cfg.UsageStatisticsPersistence() {
		t.Fatal("explicit persistence disable was not preserved")
	}
	if cfg.UsageStatisticsPersistIntervalSeconds != 0 || cfg.UsageStatisticsDetailRetentionDays != 0 || cfg.UsageStatisticsMaxStorageMB != 0 {
		t.Fatalf("negative usage storage values were not normalized: %+v", cfg)
	}
}

func TestSaveConfigPreserveCommentsKeepsExplicitUsagePersistenceDisable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	if errWrite := os.WriteFile(path, []byte("usage-statistics-enabled: true\n"), 0o600); errWrite != nil {
		t.Fatalf("WriteFile() error = %v", errWrite)
	}
	cfg, errLoad := LoadConfig(path)
	if errLoad != nil {
		t.Fatalf("LoadConfig() error = %v", errLoad)
	}
	disabled := false
	cfg.UsageStatisticsPersistenceEnabled = &disabled
	if errSave := SaveConfigPreserveComments(path, cfg); errSave != nil {
		t.Fatalf("SaveConfigPreserveComments() error = %v", errSave)
	}
	reloaded, errReload := LoadConfig(path)
	if errReload != nil {
		t.Fatalf("LoadConfig() after save error = %v", errReload)
	}
	if reloaded.UsageStatisticsPersistence() {
		t.Fatal("explicit usage persistence disable was dropped during save")
	}
}
