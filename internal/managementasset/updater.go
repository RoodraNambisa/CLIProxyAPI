package managementasset

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/util"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/singleflight"
)

const (
	defaultManagementReleaseURL  = "https://api.github.com/repos/RoodraNambisa/Cli-Proxy-API-Management-Center/releases/latest"
	defaultManagementFallbackURL = ""
	managementAssetName          = "management.html"
	httpUserAgent                = "CLIProxyAPI-management-updater"
	managementSyncMinInterval    = 30 * time.Second
	updateCheckInterval          = 3 * time.Hour
	maxAssetDownloadSize         = 10 << 20 // 10 MB safety limit for management asset downloads
)

// ManagementFileName exposes the control panel asset filename.
const ManagementFileName = managementAssetName

var (
	lastUpdateCheckMu   sync.Mutex
	lastUpdateCheckTime time.Time
	currentConfigPtr    atomic.Pointer[config.Config]
	schedulerOnce       sync.Once
	schedulerConfigPath atomic.Value
	sfGroup             singleflight.Group
)

// SetCurrentConfig stores the latest configuration snapshot for management asset decisions.
func SetCurrentConfig(cfg *config.Config) {
	if cfg == nil {
		currentConfigPtr.Store(nil)
		return
	}
	currentConfigPtr.Store(cfg)
}

// StartAutoUpdater launches a background goroutine that periodically ensures the management asset is up to date.
// It respects the disable-control-panel flag on every iteration and supports hot-reloaded configurations.
func StartAutoUpdater(ctx context.Context, configFilePath string) {
	configFilePath = strings.TrimSpace(configFilePath)
	if configFilePath == "" {
		log.Debug("management asset auto-updater skipped: empty config path")
		return
	}

	schedulerConfigPath.Store(configFilePath)

	schedulerOnce.Do(func() {
		go runAutoUpdater(ctx)
	})
}

func runAutoUpdater(ctx context.Context) {
	if ctx == nil {
		ctx = context.Background()
	}

	ticker := time.NewTicker(updateCheckInterval)
	defer ticker.Stop()

	runOnce := func() {
		cfg := currentConfigPtr.Load()
		if cfg == nil {
			log.Debug("management asset auto-updater skipped: config not yet available")
			return
		}
		if cfg.RemoteManagement.DisableControlPanel {
			log.Debug("management asset auto-updater skipped: control panel disabled")
			return
		}
		if cfg.RemoteManagement.DisableAutoUpdatePanel {
			log.Debug("management asset auto-updater skipped: disable-auto-update-panel is enabled")
			return
		}

		configPath, _ := schedulerConfigPath.Load().(string)
		staticDir := StaticDir(configPath)
		EnsureLatestManagementHTML(ctx, staticDir, cfg.ProxyURL, cfg.RemoteManagement.PanelGitHubRepository)
	}

	runOnce()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			runOnce()
		}
	}
}

func newHTTPClient(proxyURL string) *http.Client {
	client := &http.Client{Timeout: 15 * time.Second}

	sdkCfg := &sdkconfig.SDKConfig{ProxyURL: strings.TrimSpace(proxyURL)}
	util.SetProxy(sdkCfg, client)

	return client
}

type releaseAsset struct {
	Name               string `json:"name"`
	BrowserDownloadURL string `json:"browser_download_url"`
	Digest             string `json:"digest"`
}

type releaseResponse struct {
	Assets []releaseAsset `json:"assets"`
}

// ManagementHTMLStatus describes the local and remote management panel asset state.
type ManagementHTMLStatus struct {
	Disabled              bool       `json:"disabled,omitempty"`
	AutoUpdateDisabled    bool       `json:"auto_update_disabled,omitempty"`
	LocalExists           bool       `json:"local_exists"`
	LocalHash             string     `json:"local_hash,omitempty"`
	LocalModifiedAt       *time.Time `json:"local_modified_at,omitempty"`
	RemoteHash            string     `json:"remote_hash,omitempty"`
	RemoteDigestAvailable bool       `json:"remote_digest_available"`
	UpdateAvailable       bool       `json:"update_available"`
	Updated               bool       `json:"updated"`
	CheckedAt             time.Time  `json:"checked_at"`
	ReleaseURL            string     `json:"release_url,omitempty"`
	AssetURL              string     `json:"asset_url,omitempty"`
	Error                 string     `json:"error,omitempty"`
}

// StaticDir resolves the directory that stores the management control panel asset.
func StaticDir(configFilePath string) string {
	if override := strings.TrimSpace(os.Getenv("MANAGEMENT_STATIC_PATH")); override != "" {
		cleaned := filepath.Clean(override)
		if strings.EqualFold(filepath.Base(cleaned), managementAssetName) {
			return filepath.Dir(cleaned)
		}
		return cleaned
	}

	if writable := util.WritablePath(); writable != "" {
		return filepath.Join(writable, "static")
	}

	configFilePath = strings.TrimSpace(configFilePath)
	if configFilePath == "" {
		return ""
	}

	base := filepath.Dir(configFilePath)
	fileInfo, err := os.Stat(configFilePath)
	if err == nil {
		if fileInfo.IsDir() {
			base = configFilePath
		}
	}

	return filepath.Join(base, "static")
}

// FilePath resolves the absolute path to the management control panel asset.
func FilePath(configFilePath string) string {
	if override := strings.TrimSpace(os.Getenv("MANAGEMENT_STATIC_PATH")); override != "" {
		cleaned := filepath.Clean(override)
		if strings.EqualFold(filepath.Base(cleaned), managementAssetName) {
			return cleaned
		}
		return filepath.Join(cleaned, ManagementFileName)
	}

	dir := StaticDir(configFilePath)
	if dir == "" {
		return ""
	}
	return filepath.Join(dir, ManagementFileName)
}

// EnsureLatestManagementHTML checks the latest management.html asset and updates the local copy when needed.
// It coalesces concurrent sync attempts and returns whether the asset exists after the sync attempt.
func EnsureLatestManagementHTML(ctx context.Context, staticDir string, proxyURL string, panelRepository string) bool {
	status := UpdateManagementHTML(ctx, staticDir, proxyURL, panelRepository, false)
	return status.LocalExists
}

// CheckManagementHTMLStatus compares the local management.html asset with the latest release metadata.
func CheckManagementHTMLStatus(ctx context.Context, staticDir string, proxyURL string, panelRepository string) ManagementHTMLStatus {
	if ctx == nil {
		ctx = context.Background()
	}

	staticDir = strings.TrimSpace(staticDir)
	status := ManagementHTMLStatus{
		CheckedAt:  time.Now().UTC(),
		ReleaseURL: resolveReleaseURL(panelRepository),
	}
	if staticDir == "" {
		status.Error = "empty static directory"
		return status
	}
	localPath := filepath.Join(staticDir, managementAssetName)

	if info, errStat := os.Stat(localPath); errStat == nil {
		status.LocalExists = true
		modTime := info.ModTime().UTC()
		status.LocalModifiedAt = &modTime
		if hash, errHash := fileSHA256(localPath); errHash == nil {
			status.LocalHash = hash
		} else {
			status.Error = fmt.Sprintf("read local management asset hash: %v", errHash)
		}
	} else if !errors.Is(errStat, os.ErrNotExist) {
		status.Error = fmt.Sprintf("stat local management asset: %v", errStat)
	}

	client := newHTTPClient(proxyURL)
	asset, remoteHash, errFetch := fetchLatestAsset(ctx, client, status.ReleaseURL)
	if errFetch != nil {
		remoteError := errFetch.Error()
		if status.Error != "" {
			status.Error = fmt.Sprintf("%s; local status error: %s", remoteError, status.Error)
		} else {
			status.Error = remoteError
		}
		return status
	}
	status.AssetURL = asset.BrowserDownloadURL
	status.RemoteHash = remoteHash
	status.RemoteDigestAvailable = remoteHash != ""
	if !status.LocalExists {
		status.UpdateAvailable = true
	} else if status.LocalHash != "" && remoteHash != "" {
		status.UpdateAvailable = !strings.EqualFold(status.LocalHash, remoteHash)
	}
	return status
}

// UpdateManagementHTML checks and updates the local management.html asset.
// Set force to true for user-triggered updates that should bypass the periodic throttle.
func UpdateManagementHTML(ctx context.Context, staticDir string, proxyURL string, panelRepository string, force bool) ManagementHTMLStatus {
	if ctx == nil {
		ctx = context.Background()
	}

	staticDir = strings.TrimSpace(staticDir)
	if staticDir == "" {
		return ManagementHTMLStatus{
			CheckedAt:  time.Now().UTC(),
			ReleaseURL: resolveReleaseURL(panelRepository),
			Error:      "empty static directory",
		}
	}
	localPath := filepath.Join(staticDir, managementAssetName)

	result, _, _ := sfGroup.Do(localPath, func() (interface{}, error) {
		lastUpdateCheckMu.Lock()
		now := time.Now()
		timeSinceLastAttempt := now.Sub(lastUpdateCheckTime)
		if !force && !lastUpdateCheckTime.IsZero() && timeSinceLastAttempt < managementSyncMinInterval {
			lastUpdateCheckMu.Unlock()
			log.Debugf(
				"management asset sync skipped by throttle: last attempt %v ago (interval %v)",
				timeSinceLastAttempt.Round(time.Second),
				managementSyncMinInterval,
			)
			return localManagementHTMLStatus(staticDir, panelRepository), nil
		}
		lastUpdateCheckTime = now
		lastUpdateCheckMu.Unlock()

		if errMkdirAll := os.MkdirAll(staticDir, 0o755); errMkdirAll != nil {
			log.WithError(errMkdirAll).Warn("failed to prepare static directory for management asset")
			status := localManagementHTMLStatus(staticDir, panelRepository)
			status.Error = errMkdirAll.Error()
			return status, nil
		}

		status := CheckManagementHTMLStatus(ctx, staticDir, proxyURL, panelRepository)
		localFileMissing := !status.LocalExists
		if status.Error != "" {
			if status.AssetURL == "" {
				if localFileMissing {
					log.WithField("error", status.Error).Warn("failed to fetch latest management release information, trying fallback page")
					if ensureFallbackManagementHTML(ctx, newHTTPClient(proxyURL), localPath) {
						fallbackStatus := localManagementHTMLStatus(staticDir, panelRepository)
						fallbackStatus.Updated = true
						return fallbackStatus, nil
					}
				} else {
					log.Warnf("failed to fetch latest management release information: %s", status.Error)
				}
				return status, nil
			}
			log.Debugf("continuing management asset update despite local status error: %s", status.Error)
			status.Error = ""
		}
		if status.LocalExists && status.LocalHash != "" && !status.UpdateAvailable && status.RemoteDigestAvailable {
			log.Debug("management asset is already up to date")
			return status, nil
		}
		if status.AssetURL == "" {
			status.Error = "latest management asset download url is empty"
			return status, nil
		}
		client := newHTTPClient(proxyURL)
		data, downloadedHash, err := downloadAsset(ctx, client, status.AssetURL)
		if err != nil {
			if localFileMissing {
				log.WithError(err).Warn("failed to download management asset, trying fallback page")
				if ensureFallbackManagementHTML(ctx, client, localPath) {
					fallbackStatus := localManagementHTMLStatus(staticDir, panelRepository)
					fallbackStatus.Updated = true
					return fallbackStatus, nil
				}
				status.Error = err.Error()
				return status, nil
			}
			log.WithError(err).Warn("failed to download management asset")
			status.Error = err.Error()
			return status, nil
		}

		if status.RemoteHash != "" && !strings.EqualFold(status.RemoteHash, downloadedHash) {
			status.Error = fmt.Sprintf("management asset digest mismatch: expected %s got %s", status.RemoteHash, downloadedHash)
			log.Errorf("%s — aborting update for safety", status.Error)
			return status, nil
		}

		if err = atomicWriteFile(localPath, data); err != nil {
			log.WithError(err).Warn("failed to update management asset on disk")
			status.Error = err.Error()
			return status, nil
		}

		assetURL := status.AssetURL
		remoteHash := status.RemoteHash
		remoteDigestAvailable := status.RemoteDigestAvailable
		status = localManagementHTMLStatus(staticDir, panelRepository)
		status.AssetURL = assetURL
		status.RemoteHash = remoteHash
		status.RemoteDigestAvailable = remoteDigestAvailable
		status.UpdateAvailable = false
		status.Updated = true
		log.Infof("management asset updated successfully (hash=%s)", downloadedHash)
		return status, nil
	})

	if status, ok := result.(ManagementHTMLStatus); ok {
		return status
	}
	return localManagementHTMLStatus(staticDir, panelRepository)
}

func localManagementHTMLStatus(staticDir string, panelRepository string) ManagementHTMLStatus {
	status := ManagementHTMLStatus{
		CheckedAt:  time.Now().UTC(),
		ReleaseURL: resolveReleaseURL(panelRepository),
	}
	staticDir = strings.TrimSpace(staticDir)
	if staticDir == "" {
		status.Error = "empty static directory"
		return status
	}
	localPath := filepath.Join(staticDir, managementAssetName)
	info, err := os.Stat(localPath)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			status.Error = err.Error()
		}
		return status
	}
	status.LocalExists = true
	modTime := info.ModTime().UTC()
	status.LocalModifiedAt = &modTime
	if hash, errHash := fileSHA256(localPath); errHash == nil {
		status.LocalHash = hash
	} else {
		status.Error = errHash.Error()
	}
	return status
}

func ensureFallbackManagementHTML(ctx context.Context, client *http.Client, localPath string) bool {
	if strings.TrimSpace(defaultManagementFallbackURL) == "" {
		log.Debug("management asset fallback download skipped: no fallback URL configured")
		return false
	}
	data, downloadedHash, err := downloadAsset(ctx, client, defaultManagementFallbackURL)
	if err != nil {
		log.WithError(err).Warn("failed to download fallback management control panel page")
		return false
	}

	log.Warnf("management asset downloaded from fallback URL without digest verification (hash=%s) — "+
		"enable verified GitHub updates by keeping disable-auto-update-panel set to false", downloadedHash)

	if err = atomicWriteFile(localPath, data); err != nil {
		log.WithError(err).Warn("failed to persist fallback management control panel page")
		return false
	}

	log.Infof("management asset updated from fallback page successfully (hash=%s)", downloadedHash)
	return true
}

func resolveReleaseURL(repo string) string {
	repo = strings.TrimSpace(repo)
	if repo == "" {
		return defaultManagementReleaseURL
	}

	parsed, err := url.Parse(repo)
	if err != nil || parsed.Host == "" {
		return defaultManagementReleaseURL
	}

	host := strings.ToLower(parsed.Host)
	parsed.Path = strings.TrimSuffix(parsed.Path, "/")
	if strings.HasSuffix(strings.ToLower(parsed.Path), "/releases/latest") {
		return parsed.String()
	}

	if host == "api.github.com" {
		parsed.Path = parsed.Path + "/releases/latest"
		return parsed.String()
	}

	if host == "github.com" {
		parts := strings.Split(strings.Trim(parsed.Path, "/"), "/")
		if len(parts) >= 2 && parts[0] != "" && parts[1] != "" {
			repoName := strings.TrimSuffix(parts[1], ".git")
			return fmt.Sprintf("https://api.github.com/repos/%s/%s/releases/latest", parts[0], repoName)
		}
	}

	return defaultManagementReleaseURL
}

func fetchLatestAsset(ctx context.Context, client *http.Client, releaseURL string) (*releaseAsset, string, error) {
	if strings.TrimSpace(releaseURL) == "" {
		releaseURL = defaultManagementReleaseURL
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, releaseURL, nil)
	if err != nil {
		return nil, "", fmt.Errorf("create release request: %w", err)
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("User-Agent", httpUserAgent)
	gitURL := strings.ToLower(strings.TrimSpace(os.Getenv("GITSTORE_GIT_URL")))
	if tok := strings.TrimSpace(os.Getenv("GITSTORE_GIT_TOKEN")); tok != "" && strings.Contains(gitURL, "github.com") {
		req.Header.Set("Authorization", "Bearer "+tok)
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, "", fmt.Errorf("execute release request: %w", err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return nil, "", fmt.Errorf("unexpected release status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var release releaseResponse
	if err = json.NewDecoder(resp.Body).Decode(&release); err != nil {
		return nil, "", fmt.Errorf("decode release response: %w", err)
	}

	for i := range release.Assets {
		asset := &release.Assets[i]
		if strings.EqualFold(asset.Name, managementAssetName) {
			remoteHash := parseDigest(asset.Digest)
			return asset, remoteHash, nil
		}
	}

	return nil, "", fmt.Errorf("management asset %s not found in latest release", managementAssetName)
}

func downloadAsset(ctx context.Context, client *http.Client, downloadURL string) ([]byte, string, error) {
	if strings.TrimSpace(downloadURL) == "" {
		return nil, "", fmt.Errorf("empty download url")
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, downloadURL, nil)
	if err != nil {
		return nil, "", fmt.Errorf("create download request: %w", err)
	}
	req.Header.Set("User-Agent", httpUserAgent)

	resp, err := client.Do(req)
	if err != nil {
		return nil, "", fmt.Errorf("execute download request: %w", err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return nil, "", fmt.Errorf("unexpected download status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	if resp.ContentLength > maxAssetDownloadSize {
		return nil, "", fmt.Errorf("download exceeds maximum allowed size of %d bytes", maxAssetDownloadSize)
	}

	data, err := io.ReadAll(io.LimitReader(resp.Body, maxAssetDownloadSize+1))
	if err != nil {
		return nil, "", fmt.Errorf("read download body: %w", err)
	}
	if int64(len(data)) > maxAssetDownloadSize {
		return nil, "", fmt.Errorf("download exceeds maximum allowed size of %d bytes", maxAssetDownloadSize)
	}

	sum := sha256.Sum256(data)
	return data, hex.EncodeToString(sum[:]), nil
}

func fileSHA256(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer func() {
		_ = file.Close()
	}()

	h := sha256.New()
	if _, err = io.Copy(h, file); err != nil {
		return "", err
	}

	return hex.EncodeToString(h.Sum(nil)), nil
}

func atomicWriteFile(path string, data []byte) error {
	tmpFile, err := os.CreateTemp(filepath.Dir(path), "management-*.html")
	if err != nil {
		return err
	}

	tmpName := tmpFile.Name()
	defer func() {
		_ = tmpFile.Close()
		_ = os.Remove(tmpName)
	}()

	if _, err = tmpFile.Write(data); err != nil {
		return err
	}

	if err = tmpFile.Chmod(0o644); err != nil {
		return err
	}

	if err = tmpFile.Close(); err != nil {
		return err
	}

	if err = os.Rename(tmpName, path); err != nil {
		return err
	}

	return nil
}

func parseDigest(digest string) string {
	digest = strings.TrimSpace(digest)
	if digest == "" {
		return ""
	}

	if idx := strings.Index(digest, ":"); idx >= 0 {
		digest = digest[idx+1:]
	}

	return strings.ToLower(strings.TrimSpace(digest))
}
