// config_reload.go implements debounced configuration hot reload.
// It detects material changes and reloads clients when the config changes.
package watcher

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"reflect"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/util"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/watcher/diff"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/watcher/synthesizer"
	log "github.com/sirupsen/logrus"
	"gopkg.in/yaml.v3"
)

func (w *Watcher) stopConfigReloadTimer() {
	w.configReloadMu.Lock()
	if w.configReloadTimer != nil {
		w.configReloadTimer.Stop()
		w.configReloadTimer = nil
	}
	w.configReloadMu.Unlock()
}

func (w *Watcher) scheduleConfigReload() {
	w.configReloadMu.Lock()
	defer w.configReloadMu.Unlock()
	if w.configReloadTimer != nil {
		w.configReloadTimer.Stop()
	}
	w.configReloadTimer = time.AfterFunc(configReloadDebounce, func() {
		w.configReloadMu.Lock()
		w.configReloadTimer = nil
		w.configReloadMu.Unlock()
		w.reloadConfigIfChanged()
	})
}

func (w *Watcher) reloadConfigIfChanged() {
	data, err := os.ReadFile(w.configPath)
	if err != nil {
		log.Errorf("failed to read config file for hash check: %v", err)
		return
	}
	if len(data) == 0 {
		log.Debugf("ignoring empty config file write event")
		return
	}
	sum := sha256.Sum256(data)
	newHash := hex.EncodeToString(sum[:])

	w.clientsMutex.RLock()
	currentHash := w.lastConfigHash
	w.clientsMutex.RUnlock()

	if currentHash != "" && currentHash == newHash {
		log.Debugf("config file content unchanged (hash match), skipping reload")
		return
	}
	log.Infof("config file changed, reloading: %s", w.configPath)
	if w.reloadConfig() {
		finalHash := newHash
		if updatedData, errRead := os.ReadFile(w.configPath); errRead == nil && len(updatedData) > 0 {
			sumUpdated := sha256.Sum256(updatedData)
			finalHash = hex.EncodeToString(sumUpdated[:])
		} else if errRead != nil {
			log.WithError(errRead).Debug("failed to compute updated config hash after reload")
		}
		w.clientsMutex.Lock()
		w.lastConfigHash = finalHash
		w.clientsMutex.Unlock()
		w.persistConfigAsync()
	}
}

func (w *Watcher) reloadConfig() bool {
	log.Debug("=========================== CONFIG RELOAD ============================")
	log.Debugf("starting config reload from: %s", w.configPath)

	newConfig, errLoadConfig := config.LoadConfig(w.configPath)
	if errLoadConfig != nil {
		log.Errorf("failed to reload config: %v", errLoadConfig)
		return false
	}

	if w.mirroredAuthDir != "" {
		newConfig.AuthDir = w.mirroredAuthDir
	} else {
		if resolvedAuthDir, errResolveAuthDir := util.ResolveAuthDir(newConfig.AuthDir); errResolveAuthDir != nil {
			log.Errorf("failed to resolve auth directory from config: %v", errResolveAuthDir)
		} else {
			newConfig.AuthDir = resolvedAuthDir
		}
	}

	w.clientsMutex.RLock()
	oldRuntimeConfig := w.config
	configApply := w.configApply
	var oldConfig *config.Config
	_ = yaml.Unmarshal(w.oldConfigYaml, &oldConfig)
	w.clientsMutex.RUnlock()

	rollbackIdentities, errIdentities := w.prepareReloadedAPIIdentities(newConfig, oldRuntimeConfig)
	if errIdentities != nil {
		log.WithError(errIdentities).Error("failed to prepare API credentials; previous runtime retained")
		return false
	}
	appliedConfig := newConfig
	if configApply != nil {
		var errApply error
		appliedConfig, errApply = configApply(newConfig)
		if errApply != nil {
			rollbackIdentities()
			log.WithError(errApply).Error("failed to apply reloaded config; previous runtime retained")
			return false
		}
		if appliedConfig == nil {
			rollbackIdentities()
			log.Error("config apply returned no runtime snapshot; previous runtime retained")
			return false
		}
	}

	w.clientsMutex.Lock()
	w.oldConfigYaml, _ = yaml.Marshal(newConfig)
	w.config = appliedConfig
	w.clientsMutex.Unlock()

	var affectedOAuthProviders []string
	if oldConfig != nil {
		_, affectedOAuthProviders = diff.DiffOAuthExcludedModelChanges(oldConfig.OAuthExcludedModels, newConfig.OAuthExcludedModels)
	}

	util.SetLogLevel(appliedConfig)
	if oldRuntimeConfig != nil && oldRuntimeConfig.Debug != appliedConfig.Debug {
		log.Debugf("log level updated - debug mode changed from %t to %t", oldRuntimeConfig.Debug, appliedConfig.Debug)
	}

	if oldConfig != nil {
		details := diff.BuildConfigChangeDetails(oldConfig, newConfig)
		if len(details) > 0 {
			log.Debugf("config changes detected:")
			for _, d := range details {
				log.Debugf("  %s", d)
			}
		} else {
			log.Debugf("no material config field changes detected")
		}
	}

	authDirChanged := oldRuntimeConfig == nil || oldRuntimeConfig.AuthDir != appliedConfig.AuthDir
	retryConfigChanged := oldRuntimeConfig != nil && (oldRuntimeConfig.RequestRetry != appliedConfig.RequestRetry || oldRuntimeConfig.MaxRetryInterval != appliedConfig.MaxRetryInterval || oldRuntimeConfig.MaxRetryCredentials != appliedConfig.MaxRetryCredentials)
	forceAuthRefresh := oldRuntimeConfig != nil && (oldRuntimeConfig.ForceModelPrefix != appliedConfig.ForceModelPrefix || !oauthModelAliasRoutingEqual(oldRuntimeConfig.OAuthModelAlias, appliedConfig.OAuthModelAlias) || !reflect.DeepEqual(oldRuntimeConfig.AuthModelExclusions, appliedConfig.AuthModelExclusions) || retryConfigChanged)

	log.Infof("config successfully reloaded, triggering client reload")
	w.reloadClientsWithOptions(authDirChanged, affectedOAuthProviders, forceAuthRefresh, false, configApply == nil)
	return true
}

func (w *Watcher) prepareReloadedAPIIdentities(next, previous *config.Config) (func(), error) {
	changed, err := synthesizer.PrepareManagedCredentialsWithPrevious(next, previous)
	if err != nil || !changed {
		return func() {}, err
	}
	original, err := os.ReadFile(w.configPath)
	if err != nil {
		return nil, err
	}
	if err := config.SaveConfigPreserveComments(w.configPath, next); err != nil {
		return nil, fmt.Errorf("persist API credential identities: %w", err)
	}
	prepared, err := os.ReadFile(w.configPath)
	if err != nil {
		return nil, err
	}
	return func() {
		current, errRead := os.ReadFile(w.configPath)
		if errRead == nil && bytes.Equal(current, prepared) {
			if errRestore := os.WriteFile(w.configPath, original, 0600); errRestore != nil {
				log.WithError(errRestore).Error("failed to restore config after API credential migration")
			}
		}
	}, nil
}
