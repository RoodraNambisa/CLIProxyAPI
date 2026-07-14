package management

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"mime/multipart"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/auth/antigravity"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/auth/claude"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/auth/codex"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/auth/kimi"
	xaiauth "github.com/router-for-me/CLIProxyAPI/v6/internal/auth/xai"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/authfileguard"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/misc"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	internalstore "github.com/router-for-me/CLIProxyAPI/v6/internal/store"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/util"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/watcher"
	sdkAuth "github.com/router-for-me/CLIProxyAPI/v6/sdk/auth"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	log "github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"
)

var lastRefreshKeys = []string{"last_refresh", "lastRefresh", "last_refreshed_at", "lastRefreshedAt"}

const (
	anthropicCallbackPort = 54545
	codexCallbackPort     = 1455
)

type callbackForwarder struct {
	provider string
	server   *http.Server
	done     chan struct{}
}

var (
	callbackForwardersMu   sync.Mutex
	callbackForwarders     = make(map[int]*callbackForwarder)
	errAuthFileMustBeJSON  = errors.New("auth file must be .json")
	errInvalidAuthFileData = errors.New("invalid auth file")
	errAuthFileNotFound    = errors.New("auth file not found")
	errInvalidAuthFileName = errors.New("invalid auth file name")
	errGeminiCLIAuthGone   = errors.New("Gemini CLI credentials are no longer supported")
	errAuthFileQuarantined = errors.New("auth file deletion is still pending")
)

func extractLastRefreshTimestamp(meta map[string]any) (time.Time, bool) {
	if len(meta) == 0 {
		return time.Time{}, false
	}
	for _, key := range lastRefreshKeys {
		if val, ok := meta[key]; ok {
			if ts, ok1 := parseLastRefreshValue(val); ok1 {
				return ts, true
			}
		}
	}
	return time.Time{}, false
}

func parseLastRefreshValue(v any) (time.Time, bool) {
	switch val := v.(type) {
	case string:
		s := strings.TrimSpace(val)
		if s == "" {
			return time.Time{}, false
		}
		layouts := []string{time.RFC3339, time.RFC3339Nano, "2006-01-02 15:04:05", "2006-01-02T15:04:05Z07:00"}
		for _, layout := range layouts {
			if ts, err := time.Parse(layout, s); err == nil {
				return ts.UTC(), true
			}
		}
		if unix, err := strconv.ParseInt(s, 10, 64); err == nil && unix > 0 {
			return time.Unix(unix, 0).UTC(), true
		}
	case float64:
		if val <= 0 {
			return time.Time{}, false
		}
		return time.Unix(int64(val), 0).UTC(), true
	case int64:
		if val <= 0 {
			return time.Time{}, false
		}
		return time.Unix(val, 0).UTC(), true
	case int:
		if val <= 0 {
			return time.Time{}, false
		}
		return time.Unix(int64(val), 0).UTC(), true
	case json.Number:
		if i, err := val.Int64(); err == nil && i > 0 {
			return time.Unix(i, 0).UTC(), true
		}
	}
	return time.Time{}, false
}

func isWebUIRequest(c *gin.Context) bool {
	raw := strings.TrimSpace(c.Query("is_webui"))
	if raw == "" {
		return false
	}
	switch strings.ToLower(raw) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

func startCallbackForwarder(port int, provider, targetBase string) (*callbackForwarder, error) {
	callbackForwardersMu.Lock()
	prev := callbackForwarders[port]
	if prev != nil {
		delete(callbackForwarders, port)
	}
	callbackForwardersMu.Unlock()

	if prev != nil {
		stopForwarderInstance(port, prev)
	}

	addr := fmt.Sprintf("0.0.0.0:%d", port)
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("failed to listen on %s: %w", addr, err)
	}

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		target := targetBase
		if raw := r.URL.RawQuery; raw != "" {
			if strings.Contains(target, "?") {
				target = target + "&" + raw
			} else {
				target = target + "?" + raw
			}
		}
		w.Header().Set("Cache-Control", "no-store")
		http.Redirect(w, r, target, http.StatusFound)
	})

	srv := &http.Server{
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		WriteTimeout:      5 * time.Second,
	}
	done := make(chan struct{})

	go func() {
		if errServe := srv.Serve(ln); errServe != nil && !errors.Is(errServe, http.ErrServerClosed) {
			log.WithError(errServe).Warnf("callback forwarder for %s stopped unexpectedly", provider)
		}
		close(done)
	}()

	forwarder := &callbackForwarder{
		provider: provider,
		server:   srv,
		done:     done,
	}

	callbackForwardersMu.Lock()
	callbackForwarders[port] = forwarder
	callbackForwardersMu.Unlock()

	log.Infof("callback forwarder for %s listening on %s", provider, addr)

	return forwarder, nil
}

func stopCallbackForwarderInstance(port int, forwarder *callbackForwarder) {
	if forwarder == nil {
		return
	}
	callbackForwardersMu.Lock()
	if current := callbackForwarders[port]; current == forwarder {
		delete(callbackForwarders, port)
	}
	callbackForwardersMu.Unlock()

	stopForwarderInstance(port, forwarder)
}

func stopForwarderInstance(port int, forwarder *callbackForwarder) {
	if forwarder == nil || forwarder.server == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := forwarder.server.Shutdown(ctx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.WithError(err).Warnf("failed to shut down callback forwarder on port %d", port)
	}

	select {
	case <-forwarder.done:
	case <-time.After(2 * time.Second):
	}

	log.Infof("callback forwarder on port %d stopped", port)
}

func (h *Handler) managementCallbackURL(path string) (string, error) {
	if h == nil || h.cfg == nil || h.cfg.Port <= 0 {
		return "", fmt.Errorf("server port is not configured")
	}
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	path = config.JoinManagementAccessPath(h.cfg.RemoteManagement.AccessPath, path)
	scheme := "http"
	if h.cfg.TLS.Enable {
		scheme = "https"
	}
	return fmt.Sprintf("%s://127.0.0.1:%d%s", scheme, h.cfg.Port, path), nil
}

func (h *Handler) ListAuthFiles(c *gin.Context) {
	if h == nil {
		c.JSON(500, gin.H{"error": "handler not initialized"})
		return
	}
	if h.authManager == nil {
		h.listAuthFilesFromDisk(c)
		return
	}
	auths := h.authManager.List()
	files := make([]gin.H, 0, len(auths))
	managedEntryIndexes := make(map[string]int, len(auths))
	managedEntryNames := make(map[string]int, len(auths))
	for _, auth := range auths {
		managedName, managed := h.managedAuthBackingFileName(auth)
		if managed && !isTopLevelManagedAuthName(managedName) {
			continue
		}
		if entry := h.buildAuthFileEntry(auth); entry != nil {
			files = append(files, entry)
			if managed && managedName != "" {
				managedEntryIndexes[managedAuthNameKey(managedName)] = len(files) - 1
				managedEntryNames[managedName] = len(files) - 1
			}
		}
	}
	retiredFiles, errRetired := h.listRetiredGeminiCLIAuthFiles()
	if errRetired != nil {
		log.WithError(errRetired).Warn("failed to list retired Gemini CLI auth files")
	} else {
		for _, entry := range retiredFiles {
			name, _ := entry["name"].(string)
			index, exists := managedEntryIndexes[managedAuthNameKey(name)]
			if !exists {
				for managedName, managedIndex := range managedEntryNames {
					if h.sameManagedAuthFile(managedName, name) {
						index = managedIndex
						exists = true
						break
					}
				}
			}
			if exists {
				files[index] = entry
				continue
			}
			files = append(files, entry)
		}
	}
	sort.Slice(files, func(i, j int) bool {
		nameI, _ := files[i]["name"].(string)
		nameJ, _ := files[j]["name"].(string)
		return strings.ToLower(nameI) < strings.ToLower(nameJ)
	})
	c.JSON(200, gin.H{"files": files})
}

func (h *Handler) listRetiredGeminiCLIAuthFiles() ([]gin.H, error) {
	if h == nil || h.cfg == nil || strings.TrimSpace(h.cfg.AuthDir) == "" {
		return nil, nil
	}
	root, _, authDir, errRoot := h.openManagedAuthRootSnapshot()
	if errRoot != nil {
		if errors.Is(errRoot, fs.ErrNotExist) {
			return nil, nil
		}
		return nil, errRoot
	}
	defer closeManagedAuthRoot(root)
	diskFiles, errList := listAuthJSONDiskFilesAtRoot(root)
	if errList != nil {
		return nil, errList
	}
	files := make([]gin.H, 0)
	for _, diskFile := range diskFiles {
		path := filepath.Join(authDir, filepath.FromSlash(diskFile.Name))
		unlockPath := authfileguard.Lock(path)
		data, _, _, errRead := readManagedAuthFileAtRoot(root, authDir, diskFile.Name)
		if errRead != nil {
			unlockPath()
			continue
		}
		retired, metadata, errParse := parseRetiredGeminiCLIAuthFile(data)
		if errParse != nil || !retired {
			unlockPath()
			continue
		}
		authfileguard.MarkRetired(path)
		unlockPath()
		fileEntry := gin.H{
			"name":             diskFile.Name,
			"size":             diskFile.Info.Size(),
			"modtime":          diskFile.Info.ModTime(),
			"type":             strings.TrimSpace(stringValue(metadata, "type")),
			"provider":         "gemini-cli",
			"email":            strings.TrimSpace(stringValue(metadata, "email")),
			"unsupported":      true,
			"retired":          true,
			"runtime_eligible": false,
			"support_status":   "unsupported",
			"status":           "unsupported",
			"status_message":   "Gemini CLI is no longer supported",
		}
		if disabled, ok := metadata["disabled"].(bool); ok {
			fileEntry["disabled"] = disabled
		}
		files = append(files, fileEntry)
	}
	return files, nil
}

// GetAuthFileModels returns the models supported by a specific auth file
func (h *Handler) GetAuthFileModels(c *gin.Context) {
	name := c.Query("name")
	if name == "" {
		c.JSON(400, gin.H{"error": "name is required"})
		return
	}
	if h.rejectRetiredGeminiCLIAuthFileOperation(c, name) {
		return
	}

	var authID string
	if auth := h.findManagedAuth(name); auth != nil {
		authID = auth.ID
	}

	if authID == "" {
		if exists, errPresence := h.managedAuthFileExists(name); errPresence != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": errPresence.Error()})
			return
		} else if exists {
			c.JSON(http.StatusOK, gin.H{"models": []gin.H{}})
			return
		}
		authID = name
	}

	// Get models from registry
	reg := registry.GetGlobalRegistry()
	models := reg.GetModelsForClient(authID)

	result := make([]gin.H, 0, len(models))
	for _, m := range models {
		entry := gin.H{
			"id": m.ID,
		}
		if m.DisplayName != "" {
			entry["display_name"] = m.DisplayName
		}
		if m.Type != "" {
			entry["type"] = m.Type
		}
		if m.OwnedBy != "" {
			entry["owned_by"] = m.OwnedBy
		}
		result = append(result, entry)
	}

	c.JSON(200, gin.H{"models": result})
}

// List auth files from disk when the auth manager is unavailable.
func (h *Handler) listAuthFilesFromDisk(c *gin.Context) {
	diskFiles, err := h.listAuthJSONDiskFiles()
	if err != nil {
		c.JSON(500, gin.H{"error": fmt.Sprintf("failed to read auth dir: %v", err)})
		return
	}
	files := make([]gin.H, 0)
	for _, diskFile := range diskFiles {
		fileData := gin.H{"name": diskFile.Name, "size": diskFile.Info.Size(), "modtime": diskFile.Info.ModTime()}

		// Read file to get type field
		path, _, errPath := h.resolveManagedAuthFilePath(diskFile.Name)
		if errPath == nil {
			unlockPath := authfileguard.Lock(path)
			data, _, _, errRead := h.readManagedAuthFile(diskFile.Name)
			if errRead != nil {
				unlockPath()
				files = append(files, fileData)
				continue
			}
			typeValue := gjson.GetBytes(data, "type").String()
			emailValue := gjson.GetBytes(data, "email").String()
			fileData["type"] = typeValue
			fileData["email"] = emailValue
			if retired, _, errParse := parseRetiredGeminiCLIAuthFile(data); errParse == nil && retired {
				authfileguard.MarkRetired(path)
				fileData["provider"] = "gemini-cli"
				fileData["unsupported"] = true
				fileData["retired"] = true
				fileData["runtime_eligible"] = false
				fileData["support_status"] = "unsupported"
				fileData["status"] = "unsupported"
				fileData["status_message"] = "Gemini CLI is no longer supported"
			}
			if strings.EqualFold(strings.TrimSpace(typeValue), "xai") {
				fileData["using_api"] = effectiveXAIUsingAPIFromJSON(data)
				fileData["websockets"] = jsonBoolField(data, "websockets")
			}
			if strings.EqualFold(strings.TrimSpace(typeValue), "codex") {
				var metadata map[string]any
				if errUnmarshal := json.Unmarshal(data, &metadata); errUnmarshal == nil {
					if planType := codex.EffectivePlanType(metadata); planType != "" {
						fileData["plan_type"] = planType
					}
				}
			}
			if pv := gjson.GetBytes(data, "priority"); pv.Exists() {
				switch pv.Type {
				case gjson.Number:
					fileData["priority"] = int(pv.Int())
				case gjson.String:
					if parsed, errAtoi := strconv.Atoi(strings.TrimSpace(pv.String())); errAtoi == nil {
						fileData["priority"] = parsed
					}
				}
			}
			if nv := gjson.GetBytes(data, "note"); nv.Exists() && nv.Type == gjson.String {
				if trimmed := strings.TrimSpace(nv.String()); trimmed != "" {
					fileData["note"] = trimmed
				}
			}
			unlockPath()
		}

		files = append(files, fileData)
	}
	c.JSON(200, gin.H{"files": files})
}

func (h *Handler) buildAuthFileEntry(auth *coreauth.Auth) gin.H {
	if auth == nil {
		return nil
	}
	auth.EnsureIndex()
	runtimeOnly := isRuntimeOnlyAuth(auth)
	if runtimeOnly && (auth.Disabled || auth.Status == coreauth.StatusDisabled) {
		return nil
	}
	path := strings.TrimSpace(authAttribute(auth, "path"))
	if path == "" && !runtimeOnly {
		return nil
	}
	name := strings.TrimSpace(auth.FileName)
	if managedName, ok := h.managedAuthFileName(path); ok {
		name = managedName
	}
	if name == "" {
		name = auth.ID
	}
	entry := gin.H{
		"id":             auth.ID,
		"auth_index":     auth.Index,
		"name":           name,
		"type":           strings.TrimSpace(auth.Provider),
		"provider":       strings.TrimSpace(auth.Provider),
		"label":          auth.Label,
		"status":         auth.Status,
		"status_message": auth.StatusMessage,
		"disabled":       auth.Disabled,
		"unavailable":    auth.Unavailable,
		"runtime_only":   runtimeOnly,
		"source":         "memory",
		"size":           int64(0),
	}
	if email := authEmail(auth); email != "" {
		entry["email"] = email
	}
	if planType := effectiveCodexPlanType(auth); planType != "" {
		entry["plan_type"] = planType
	}
	if strings.EqualFold(strings.TrimSpace(auth.Provider), "xai") {
		entry["using_api"] = effectiveXAIUsingAPI(auth)
		entry["websockets"] = authBooleanValue(auth, "websockets")
	}
	if accountType, account := auth.AccountInfo(); accountType != "" || account != "" {
		if accountType != "" {
			entry["account_type"] = accountType
		}
		if account != "" {
			entry["account"] = account
		}
	}
	if !auth.CreatedAt.IsZero() {
		entry["created_at"] = auth.CreatedAt
	}
	if !auth.UpdatedAt.IsZero() {
		entry["modtime"] = auth.UpdatedAt
		entry["updated_at"] = auth.UpdatedAt
	}
	if !auth.LastRefreshedAt.IsZero() {
		entry["last_refresh"] = auth.LastRefreshedAt
	}
	if auth.LastError != nil {
		entry["last_error"] = auth.LastError
		if auth.LastError.HTTPStatus > 0 {
			entry["last_error_status_code"] = auth.LastError.HTTPStatus
		}
	}
	if !auth.NextRetryAfter.IsZero() {
		entry["next_retry_after"] = auth.NextRetryAfter
	}
	if path != "" {
		entry["path"] = path
		entry["source"] = "file"
		if info, err := os.Stat(path); err == nil {
			entry["size"] = info.Size()
			entry["modtime"] = info.ModTime()
		} else if os.IsNotExist(err) {
			// Hide credentials removed from disk but still lingering in memory.
			if !runtimeOnly && (auth.Disabled || auth.Status == coreauth.StatusDisabled || strings.EqualFold(strings.TrimSpace(auth.StatusMessage), "removed via management api")) {
				return nil
			}
			entry["source"] = "memory"
		} else {
			log.WithError(err).Warnf("failed to stat auth file %s", path)
		}
	}
	if claims := extractCodexIDTokenClaims(auth); claims != nil {
		entry["id_token"] = claims
	}
	// Expose priority from Attributes (set by synthesizer from JSON "priority" field).
	// Fall back to Metadata for auths registered via UploadAuthFile (no synthesizer).
	if p := strings.TrimSpace(authAttribute(auth, "priority")); p != "" {
		if parsed, err := strconv.Atoi(p); err == nil {
			entry["priority"] = parsed
		}
	} else if auth.Metadata != nil {
		if rawPriority, ok := auth.Metadata["priority"]; ok {
			switch v := rawPriority.(type) {
			case float64:
				entry["priority"] = int(v)
			case int:
				entry["priority"] = v
			case string:
				if parsed, err := strconv.Atoi(strings.TrimSpace(v)); err == nil {
					entry["priority"] = parsed
				}
			}
		}
	}
	// Expose note from Attributes (set by synthesizer from JSON "note" field).
	// Fall back to Metadata for auths registered via UploadAuthFile (no synthesizer).
	if note := strings.TrimSpace(authAttribute(auth, "note")); note != "" {
		entry["note"] = note
	} else if auth.Metadata != nil {
		if rawNote, ok := auth.Metadata["note"].(string); ok {
			if trimmed := strings.TrimSpace(rawNote); trimmed != "" {
				entry["note"] = trimmed
			}
		}
	}
	return entry
}

func extractCodexIDTokenClaims(auth *coreauth.Auth) gin.H {
	if auth == nil || auth.Metadata == nil {
		return nil
	}
	if !strings.EqualFold(strings.TrimSpace(auth.Provider), "codex") {
		return nil
	}
	claims := codex.IDTokenClaimsFromMetadata(auth.Metadata)
	if claims == nil {
		return nil
	}

	result := gin.H{}
	if v := strings.TrimSpace(claims.CodexAuthInfo.ChatgptAccountID); v != "" {
		result["chatgpt_account_id"] = v
	}
	if v := strings.TrimSpace(claims.CodexAuthInfo.ChatgptPlanType); v != "" {
		result["plan_type"] = v
	}
	if v := claims.CodexAuthInfo.ChatgptSubscriptionActiveStart; v != nil {
		result["chatgpt_subscription_active_start"] = v
	}
	if v := claims.CodexAuthInfo.ChatgptSubscriptionActiveUntil; v != nil {
		result["chatgpt_subscription_active_until"] = v
	}

	if len(result) == 0 {
		return nil
	}
	return result
}

func effectiveCodexPlanType(auth *coreauth.Auth) string {
	if auth == nil {
		return ""
	}
	if !strings.EqualFold(strings.TrimSpace(auth.Provider), "codex") {
		return ""
	}
	if auth.Attributes != nil {
		if planType := strings.TrimSpace(auth.Attributes["plan_type"]); planType != "" {
			return planType
		}
	}
	return codex.EffectivePlanType(auth.Metadata)
}

func authEmail(auth *coreauth.Auth) string {
	if auth == nil {
		return ""
	}
	if auth.Metadata != nil {
		if v, ok := auth.Metadata["email"].(string); ok {
			return strings.TrimSpace(v)
		}
	}
	if auth.Attributes != nil {
		if v := strings.TrimSpace(auth.Attributes["email"]); v != "" {
			return v
		}
		if v := strings.TrimSpace(auth.Attributes["account_email"]); v != "" {
			return v
		}
	}
	return ""
}

func authAttribute(auth *coreauth.Auth, key string) string {
	if auth == nil || len(auth.Attributes) == 0 {
		return ""
	}
	return auth.Attributes[key]
}

func authBooleanValue(auth *coreauth.Auth, key string) bool {
	value, _ := authOptionalBooleanValue(auth, key)
	return value
}

func authOptionalBooleanValue(auth *coreauth.Auth, key string) (bool, bool) {
	if auth == nil {
		return false, false
	}
	if auth.Attributes != nil {
		if raw := strings.TrimSpace(auth.Attributes[key]); raw != "" {
			if parsed, errParse := strconv.ParseBool(raw); errParse == nil {
				return parsed, true
			}
		}
	}
	if auth.Metadata == nil {
		return false, false
	}
	switch value := auth.Metadata[key].(type) {
	case bool:
		return value, true
	case string:
		parsed, errParse := strconv.ParseBool(strings.TrimSpace(value))
		if errParse == nil {
			return parsed, true
		}
	default:
	}
	return false, false
}

func effectiveXAIUsingAPI(auth *coreauth.Auth) bool {
	if auth == nil {
		return true
	}
	if value, ok := authOptionalBooleanValue(auth, "using_api"); ok {
		return value
	}
	authKind := strings.TrimSpace(authAttribute(auth, "auth_kind"))
	if authKind == "" && auth.Metadata != nil {
		if value, ok := auth.Metadata["auth_kind"].(string); ok {
			authKind = strings.TrimSpace(value)
		}
	}
	return !strings.EqualFold(authKind, "oauth")
}

func jsonBoolField(data []byte, key string) bool {
	value, _ := jsonOptionalBoolField(data, key)
	return value
}

func jsonOptionalBoolField(data []byte, key string) (bool, bool) {
	value := gjson.GetBytes(data, key)
	switch value.Type {
	case gjson.True:
		return true, true
	case gjson.False:
		return false, true
	case gjson.String:
		parsed, errParse := strconv.ParseBool(strings.TrimSpace(value.String()))
		if errParse == nil {
			return parsed, true
		}
	default:
	}
	return false, false
}

func effectiveXAIUsingAPIFromJSON(data []byte) bool {
	if value, ok := jsonOptionalBoolField(data, "using_api"); ok {
		return value
	}
	authKind := strings.TrimSpace(gjson.GetBytes(data, "auth_kind").String())
	return !strings.EqualFold(authKind, "oauth")
}

func isRuntimeOnlyAuth(auth *coreauth.Auth) bool {
	if auth == nil || len(auth.Attributes) == 0 {
		return false
	}
	return strings.EqualFold(strings.TrimSpace(auth.Attributes["runtime_only"]), "true")
}

func isUnsafeAuthFileName(name string) bool {
	if strings.TrimSpace(name) == "" {
		return true
	}
	if hasWindowsVolumePathShape(name) {
		return true
	}
	if strings.ContainsAny(name, "/\\") {
		return true
	}
	if filepath.VolumeName(name) != "" {
		return true
	}
	return false
}

type authDiskFile struct {
	Name string
	Info os.FileInfo
}

func (h *Handler) resolvedAuthDir() (string, error) {
	_, resolved, err := h.authDirSnapshot()
	return resolved, err
}

func (h *Handler) authDirSnapshot() (string, string, error) {
	if h == nil {
		return "", "", fmt.Errorf("handler not initialized")
	}
	h.mu.Lock()
	configuredAuthDir := ""
	if h.cfg != nil {
		configuredAuthDir = h.cfg.AuthDir
	}
	h.mu.Unlock()
	if strings.TrimSpace(configuredAuthDir) == "" {
		return "", "", fmt.Errorf("handler not initialized")
	}
	authDir, errResolve := util.ResolveAuthDir(strings.TrimSpace(configuredAuthDir))
	if errResolve != nil {
		return "", "", errResolve
	}
	if authDir == "" {
		return "", "", fmt.Errorf("auth dir is not configured")
	}
	authDir, errAbs := filepath.Abs(authDir)
	if errAbs != nil {
		return "", "", fmt.Errorf("resolve auth dir: %w", errAbs)
	}
	lexicalAuthDir := filepath.Clean(authDir)
	resolvedAuthDir := lexicalAuthDir
	if resolved, errEval := filepath.EvalSymlinks(lexicalAuthDir); errEval == nil {
		resolvedAuthDir = filepath.Clean(resolved)
	} else if !os.IsNotExist(errEval) {
		return "", "", fmt.Errorf("resolve auth dir symlink: %w", errEval)
	}
	return lexicalAuthDir, resolvedAuthDir, nil
}

func (h *Handler) openManagedAuthRoot() (*os.Root, string, error) {
	root, _, authDir, errRoot := h.openManagedAuthRootSnapshot()
	return root, authDir, errRoot
}

func (h *Handler) openManagedAuthRootSnapshot() (*os.Root, string, string, error) {
	lexicalAuthDir, authDir, errAuthDir := h.authDirSnapshot()
	if errAuthDir != nil {
		return nil, "", "", errAuthDir
	}
	root, errOpen := os.OpenRoot(authDir)
	if errOpen != nil {
		return nil, "", "", fmt.Errorf("open auth root: %w", errOpen)
	}
	return root, lexicalAuthDir, authDir, nil
}

func closeManagedAuthRoot(root *os.Root) {
	if root == nil {
		return
	}
	if errClose := root.Close(); errClose != nil {
		log.WithError(errClose).Debug("failed to close auth root")
	}
}

func pathWithinDirectory(root, path string) bool {
	rel, errRel := filepath.Rel(root, path)
	if errRel != nil || rel == ".." {
		return false
	}
	return !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

func rootPathContainsSymlink(root *os.Root, relativePath string) (bool, error) {
	if root == nil {
		return false, fmt.Errorf("auth root is nil")
	}
	current := ""
	for _, part := range strings.Split(filepath.Clean(relativePath), string(filepath.Separator)) {
		if part == "" || part == "." {
			continue
		}
		current = filepath.Join(current, part)
		info, errInfo := root.Lstat(current)
		if errInfo != nil {
			if os.IsNotExist(errInfo) {
				return false, nil
			}
			return false, errInfo
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return true, nil
		}
	}
	return false, nil
}

func normalizeManagedAuthFileName(name string) (string, error) {
	name = strings.TrimSpace(name)
	if name == "" || !strings.HasSuffix(strings.ToLower(name), ".json") {
		return "", errInvalidAuthFileName
	}
	if hasWindowsVolumePathShape(name) {
		return "", errInvalidAuthFileName
	}
	if runtime.GOOS == "windows" {
		name = strings.ReplaceAll(name, "\\", "/")
	} else {
		portableClean := filepath.ToSlash(filepath.Clean(filepath.FromSlash(strings.ReplaceAll(name, "\\", "/"))))
		if portableClean == ".." || strings.HasPrefix(portableClean, "../") {
			return "", errInvalidAuthFileName
		}
	}
	normalized := filepath.Clean(filepath.FromSlash(name))
	if normalized == "." || filepath.IsAbs(normalized) || filepath.VolumeName(normalized) != "" {
		return "", errInvalidAuthFileName
	}
	return filepath.ToSlash(normalized), nil
}

func hasWindowsVolumePathShape(name string) bool {
	name = strings.TrimSpace(name)
	if strings.HasPrefix(name, `\`) || strings.HasPrefix(name, "/") {
		return true
	}
	if len(name) < 2 || name[1] != ':' {
		return false
	}
	first := name[0]
	return first >= 'a' && first <= 'z' || first >= 'A' && first <= 'Z'
}

func (h *Handler) resolveManagedAuthFilePath(name string) (string, string, error) {
	root, _, authDir, errRoot := h.openManagedAuthRootSnapshot()
	if errRoot != nil {
		return "", "", errRoot
	}
	defer closeManagedAuthRoot(root)
	return resolveManagedAuthFilePathAtRoot(root, authDir, name)
}

func resolveManagedAuthFilePathAtRoot(root *os.Root, authDir, name string) (string, string, error) {
	normalizedName, errNormalize := normalizeManagedAuthFileName(name)
	if errNormalize != nil {
		return "", "", errNormalize
	}
	normalized := filepath.FromSlash(normalizedName)
	path := filepath.Clean(filepath.Join(authDir, normalized))
	if !pathWithinDirectory(authDir, path) {
		return "", "", errInvalidAuthFileName
	}
	containsSymlink, errSymlink := rootPathContainsSymlink(root, normalized)
	if errSymlink != nil {
		return "", "", fmt.Errorf("inspect auth path: %w", errSymlink)
	}
	if containsSymlink {
		return "", "", errInvalidAuthFileName
	}
	rel, errRel := filepath.Rel(authDir, filepath.Clean(filepath.Join(authDir, normalized)))
	if errRel != nil {
		return "", "", fmt.Errorf("resolve auth relative path: %w", errRel)
	}
	if !pathWithinDirectory(authDir, filepath.Join(authDir, rel)) {
		return "", "", errInvalidAuthFileName
	}
	return path, filepath.ToSlash(rel), nil
}

func actualManagedAuthFileNameAtRoot(root *os.Root, name string) (string, error) {
	return actualManagedAuthFileNameAtRootForOS(root, name, runtime.GOOS)
}

func actualManagedAuthFileNameAtRootForOS(root *os.Root, name, goos string) (string, error) {
	if goos != "windows" || root == nil {
		return name, nil
	}
	parts := strings.Split(filepath.ToSlash(name), "/")
	actualParts := make([]string, 0, len(parts))
	for _, part := range parts {
		currentDir := "."
		if len(actualParts) > 0 {
			currentDir = filepath.FromSlash(strings.Join(actualParts, "/"))
		}
		dir, errOpen := root.Open(currentDir)
		if errOpen != nil {
			if os.IsNotExist(errOpen) {
				return name, nil
			}
			return "", errOpen
		}
		entries, errRead := dir.ReadDir(-1)
		errClose := dir.Close()
		if errRead != nil {
			return "", errRead
		}
		if errClose != nil {
			return "", errClose
		}
		actualPart := ""
		for _, entry := range entries {
			if entry.Name() == part {
				actualPart = entry.Name()
				break
			}
			if actualPart == "" && strings.EqualFold(entry.Name(), part) {
				actualPart = entry.Name()
			}
		}
		if actualPart == "" {
			return name, nil
		}
		actualParts = append(actualParts, actualPart)
	}
	return strings.Join(actualParts, "/"), nil
}

func managedAuthPathErrorStatus(err error) int {
	if errors.Is(err, errInvalidAuthFileName) {
		return http.StatusBadRequest
	}
	return http.StatusInternalServerError
}

func (h *Handler) managedAuthFileName(path string) (string, bool) {
	path = strings.TrimSpace(path)
	if path == "" {
		return "", false
	}
	root, lexicalAuthDir, authDir, errRoot := h.openManagedAuthRootSnapshot()
	if errRoot != nil {
		return "", false
	}
	defer closeManagedAuthRoot(root)
	return managedAuthFileNameAtRoot(root, lexicalAuthDir, authDir, path)
}

func managedAuthFileNameAtRoot(root *os.Root, lexicalAuthDir, authDir, path string) (string, bool) {
	path = strings.TrimSpace(path)
	if path == "" {
		return "", false
	}
	if !filepath.IsAbs(path) {
		if abs, errAbs := filepath.Abs(path); errAbs == nil {
			path = abs
		}
	}
	path = filepath.Clean(path)
	rel, errRel := filepath.Rel(authDir, path)
	if errRel != nil || rel == "." || !pathWithinDirectory(authDir, path) {
		rel, errRel = filepath.Rel(lexicalAuthDir, path)
		if errRel != nil || rel == "." || !pathWithinDirectory(lexicalAuthDir, path) {
			return "", false
		}
	}
	_, displayName, errResolve := resolveManagedAuthFilePathAtRoot(root, authDir, filepath.ToSlash(rel))
	if errResolve != nil {
		return "", false
	}
	return displayName, true
}

func (h *Handler) listAuthJSONDiskFiles() ([]authDiskFile, error) {
	root, _, errRoot := h.openManagedAuthRoot()
	if errRoot != nil {
		if errors.Is(errRoot, fs.ErrNotExist) {
			return nil, nil
		}
		return nil, errRoot
	}
	defer closeManagedAuthRoot(root)
	return listAuthJSONDiskFilesAtRoot(root)
}

func listAuthJSONDiskFilesAtRoot(root *os.Root) ([]authDiskFile, error) {
	if root == nil {
		return nil, errors.New("auth root is nil")
	}
	directory, errOpen := root.Open(".")
	if errOpen != nil {
		return nil, errOpen
	}
	entries, errRead := directory.ReadDir(-1)
	errClose := directory.Close()
	if errRead != nil || errClose != nil {
		return nil, errors.Join(errRead, errClose)
	}
	files := make([]authDiskFile, 0, len(entries))
	for _, entry := range entries {
		if entry == nil || entry.IsDir() || entry.Type()&os.ModeSymlink != 0 || !strings.HasSuffix(strings.ToLower(entry.Name()), ".json") {
			continue
		}
		info, errInfo := entry.Info()
		if errInfo != nil {
			if os.IsNotExist(errInfo) {
				continue
			}
			return nil, errInfo
		}
		files = append(files, authDiskFile{Name: entry.Name(), Info: info})
	}
	sort.Slice(files, func(i, j int) bool {
		return strings.ToLower(files[i].Name) < strings.ToLower(files[j].Name)
	})
	return files, nil
}

func listAllManageableAuthFileNamesAtRoot(root *os.Root, authDir string) ([]string, error) {
	diskFiles, errList := listAuthJSONDiskFilesAtRoot(root)
	if errList != nil {
		return nil, errList
	}
	names := make([]string, 0, len(diskFiles))
	for _, diskFile := range diskFiles {
		names = append(names, diskFile.Name)
	}

	nestedNames, errNested := listNestedAuthJSONFileNamesAtRoot(root)
	if errNested != nil {
		return nil, errNested
	}
	for _, name := range nestedNames {
		data, displayName, _, errRead := readManagedAuthFileAtRoot(root, authDir, name)
		if errors.Is(errRead, fs.ErrNotExist) {
			continue
		}
		if errRead != nil {
			return nil, fmt.Errorf("read nested auth file %s: %w", name, errRead)
		}
		if coreauth.IsRetiredGeminiCLIAuthFileData(data) {
			names = append(names, displayName)
		}
	}
	sort.Slice(names, func(i, j int) bool {
		return strings.ToLower(names[i]) < strings.ToLower(names[j])
	})
	return names, nil
}

func listNestedAuthJSONFileNamesAtRoot(root *os.Root) ([]string, error) {
	if root == nil {
		return nil, errors.New("auth root is nil")
	}
	names := make([]string, 0)
	if errWalk := walkNestedAuthJSONFileNames(root, "", &names); errWalk != nil {
		return nil, errWalk
	}
	return names, nil
}

func walkNestedAuthJSONFileNames(root *os.Root, prefix string, names *[]string) (err error) {
	directory, errOpen := root.Open(".")
	if errOpen != nil {
		return errOpen
	}
	entries, errRead := directory.ReadDir(-1)
	errClose := directory.Close()
	if errRead != nil || errClose != nil {
		return errors.Join(errRead, errClose)
	}
	for _, entry := range entries {
		if entry == nil || entry.Type()&os.ModeSymlink != 0 {
			continue
		}
		name := entry.Name()
		relativeName := name
		if prefix != "" {
			relativeName = prefix + "/" + name
		}
		if entry.IsDir() {
			before, errBefore := root.Lstat(name)
			if errors.Is(errBefore, fs.ErrNotExist) {
				continue
			}
			if errBefore != nil || before.Mode()&os.ModeSymlink != 0 || !before.IsDir() {
				if errBefore != nil {
					return errBefore
				}
				continue
			}
			child, errChild := root.OpenRoot(name)
			if errors.Is(errChild, fs.ErrNotExist) {
				continue
			}
			if errChild != nil {
				return errChild
			}
			opened, errOpened := child.Stat(".")
			after, errAfter := root.Lstat(name)
			if errOpened != nil || errAfter != nil || after.Mode()&os.ModeSymlink != 0 || !after.IsDir() || !os.SameFile(before, opened) || !os.SameFile(after, opened) {
				return errors.Join(errOpened, errAfter, child.Close(), errors.New("managed auth directory changed while opening"))
			}
			errWalk := walkNestedAuthJSONFileNames(child, relativeName, names)
			errChildClose := child.Close()
			if errWalk != nil || errChildClose != nil {
				return errors.Join(errWalk, errChildClose)
			}
			continue
		}
		if prefix == "" || !strings.HasSuffix(strings.ToLower(name), ".json") {
			continue
		}
		info, errInfo := entry.Info()
		if errors.Is(errInfo, fs.ErrNotExist) {
			continue
		}
		if errInfo != nil {
			return errInfo
		}
		if info.Mode().IsRegular() {
			*names = append(*names, relativeName)
		}
	}
	return nil
}

type authDownloadFile struct {
	Name string
	Data []byte
}

type managedAuthFileSnapshot struct {
	data []byte
	info fs.FileInfo
}

type authFilesArchiveRequest struct {
	All   bool     `json:"all"`
	Names []string `json:"names"`
}

func (h *Handler) readManagedAuthFile(name string) ([]byte, string, string, error) {
	root, _, authDir, errRoot := h.openManagedAuthRootSnapshot()
	if errRoot != nil {
		return nil, "", "", errRoot
	}
	defer closeManagedAuthRoot(root)
	return readManagedAuthFileAtRoot(root, authDir, name)
}

func readManagedAuthFileAtRoot(root *os.Root, authDir, name string) ([]byte, string, string, error) {
	return readManagedAuthFileAtRootForOS(root, authDir, name, runtime.GOOS)
}

func readManagedAuthFileAtRootForOS(root *os.Root, authDir, name, goos string) ([]byte, string, string, error) {
	_, displayName, errResolve := resolveManagedAuthFilePathAtRoot(root, authDir, name)
	if errResolve != nil {
		return nil, "", "", errResolve
	}
	actualName, errActual := actualManagedAuthFileNameAtRootForOS(root, displayName, goos)
	if errActual != nil {
		return nil, displayName, filepath.Join(authDir, filepath.FromSlash(displayName)), fmt.Errorf("resolve auth file name: %w", errActual)
	}
	displayName = actualName
	relativePath := filepath.FromSlash(displayName)
	snapshot, errRead := captureManagedAuthFileSnapshotAtRoot(root, relativePath)
	if errRead != nil {
		return nil, displayName, filepath.Join(authDir, relativePath), errRead
	}
	return snapshot.data, displayName, filepath.Join(authDir, relativePath), nil
}

func captureManagedAuthFileSnapshotAtRoot(root *os.Root, relativePath string) (snapshot managedAuthFileSnapshot, err error) {
	parentRoot, leaf, closeParent, errParent := openManagedAuthSnapshotParent(root, relativePath)
	if errParent != nil {
		return managedAuthFileSnapshot{}, errParent
	}
	defer func() {
		if errClose := closeParent(); errClose != nil {
			err = errors.Join(err, fmt.Errorf("close managed auth parent: %w", errClose))
		}
	}()
	before, errBefore := parentRoot.Lstat(leaf)
	if errBefore != nil {
		return managedAuthFileSnapshot{}, errBefore
	}
	if errValidate := validateManagedAuthSnapshotFile(before); errValidate != nil {
		return managedAuthFileSnapshot{}, errValidate
	}
	file, errOpen := parentRoot.Open(leaf)
	if errOpen != nil {
		return managedAuthFileSnapshot{}, errOpen
	}
	defer func() {
		if errClose := file.Close(); errClose != nil {
			err = errors.Join(err, fmt.Errorf("close managed auth file: %w", errClose))
		}
	}()
	opened, errOpened := file.Stat()
	after, errAfter := parentRoot.Lstat(leaf)
	if errOpened != nil || errAfter != nil {
		return managedAuthFileSnapshot{}, errors.Join(errOpened, errAfter)
	}
	if errValidate := validateManagedAuthSnapshotFile(opened); errValidate != nil {
		return managedAuthFileSnapshot{}, errValidate
	}
	if errValidate := validateManagedAuthSnapshotFile(after); errValidate != nil {
		return managedAuthFileSnapshot{}, errValidate
	}
	if !os.SameFile(before, opened) || !os.SameFile(after, opened) {
		return managedAuthFileSnapshot{}, errors.New("managed auth path changed while opening")
	}
	data, errRead := io.ReadAll(file)
	if errRead != nil {
		return managedAuthFileSnapshot{}, errRead
	}
	return managedAuthFileSnapshot{data: data, info: opened}, nil
}

func openManagedAuthSnapshotParent(root *os.Root, relativePath string) (*os.Root, string, func() error, error) {
	if root == nil {
		return nil, "", nil, errors.New("managed auth root is nil")
	}
	clean := filepath.Clean(filepath.FromSlash(strings.TrimSpace(relativePath)))
	if clean == "." || clean == ".." || filepath.IsAbs(clean) || filepath.VolumeName(clean) != "" || strings.HasPrefix(clean, ".."+string(os.PathSeparator)) {
		return nil, "", nil, errInvalidAuthFileName
	}
	parts := strings.Split(clean, string(os.PathSeparator))
	current := root
	owned := false
	closeCurrent := func() error {
		if !owned {
			return nil
		}
		return current.Close()
	}
	for _, component := range parts[:len(parts)-1] {
		before, errBefore := current.Lstat(component)
		if errBefore != nil {
			return nil, "", nil, errors.Join(errBefore, closeCurrent())
		}
		if before.Mode()&os.ModeSymlink != 0 || !before.IsDir() {
			return nil, "", nil, errors.Join(errors.New("managed auth path component is not a stable directory"), closeCurrent())
		}
		next, errOpen := current.OpenRoot(component)
		if errOpen != nil {
			return nil, "", nil, errors.Join(errOpen, closeCurrent())
		}
		opened, errOpened := next.Stat(".")
		after, errAfter := current.Lstat(component)
		if errOpened != nil || errAfter != nil || after.Mode()&os.ModeSymlink != 0 || !after.IsDir() || !os.SameFile(before, opened) || !os.SameFile(after, opened) {
			return nil, "", nil, errors.Join(errOpened, errAfter, next.Close(), closeCurrent(), errors.New("managed auth path component changed while opening"))
		}
		if errClose := closeCurrent(); errClose != nil {
			return nil, "", nil, errors.Join(errClose, next.Close())
		}
		current = next
		owned = true
	}
	return current, parts[len(parts)-1], closeCurrent, nil
}

func validateManagedAuthSnapshotFile(info fs.FileInfo) error {
	if info == nil || info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return errors.New("managed auth path is not a regular file")
	}
	return nil
}

func sameManagedAuthFileSnapshot(a, b managedAuthFileSnapshot) bool {
	return a.info != nil && b.info != nil && os.SameFile(a.info, b.info) && bytes.Equal(a.data, b.data)
}

func (h *Handler) readDownloadAuthFile(name string) (*authDownloadFile, int, error) {
	root, _, authDir, errRoot := h.openManagedAuthRootSnapshot()
	if errRoot != nil {
		if errors.Is(errRoot, fs.ErrNotExist) {
			return nil, http.StatusNotFound, fmt.Errorf("file not found")
		}
		return nil, http.StatusInternalServerError, errRoot
	}
	defer closeManagedAuthRoot(root)
	return readDownloadAuthFileAtRoot(root, authDir, name)
}

func readDownloadAuthFileAtRoot(root *os.Root, authDir, name string) (*authDownloadFile, int, error) {
	data, displayName, _, err := readManagedAuthFileAtRoot(root, authDir, name)
	if err != nil {
		if errors.Is(err, errInvalidAuthFileName) {
			return nil, http.StatusBadRequest, err
		}
		if errors.Is(err, fs.ErrNotExist) {
			return nil, http.StatusNotFound, fmt.Errorf("file not found")
		}
		return nil, http.StatusInternalServerError, fmt.Errorf("failed to read file: %w", err)
	}
	if errAccess := validateExplicitManagedAuthFileAccess(displayName, data); errAccess != nil {
		return nil, http.StatusBadRequest, errAccess
	}
	return &authDownloadFile{Name: displayName, Data: data}, http.StatusOK, nil
}

func validateExplicitManagedAuthFileAccess(name string, data []byte) error {
	if isTopLevelManagedAuthName(name) || coreauth.IsRetiredGeminiCLIAuthFileData(data) {
		return nil
	}
	return errInvalidAuthFileName
}

func (h *Handler) listAllDownloadAuthFiles() ([]authDownloadFile, error) {
	root, _, authDir, errRoot := h.openManagedAuthRootSnapshot()
	if errRoot != nil {
		return nil, errRoot
	}
	defer closeManagedAuthRoot(root)
	names, err := listAllManageableAuthFileNamesAtRoot(root, authDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read auth dir: %w", err)
	}
	files := make([]authDownloadFile, 0, len(names))
	for _, name := range names {
		file, _, errRead := readDownloadAuthFileAtRoot(root, authDir, name)
		if errRead != nil {
			return nil, errRead
		}
		files = append(files, *file)
	}
	sort.Slice(files, func(i, j int) bool {
		return strings.ToLower(files[i].Name) < strings.ToLower(files[j].Name)
	})
	return files, nil
}

func (h *Handler) loadDownloadAuthFiles(names []string) ([]authDownloadFile, int, error) {
	root, lexicalAuthDir, authDir, errRoot := h.openManagedAuthRootSnapshot()
	if errRoot != nil {
		status := http.StatusInternalServerError
		if errors.Is(errRoot, fs.ErrNotExist) {
			status = http.StatusNotFound
		}
		return nil, status, errRoot
	}
	defer closeManagedAuthRoot(root)
	var errCanonical error
	names, errCanonical = h.canonicalAuthFileNamesAtRoot(root, lexicalAuthDir, authDir, names)
	if errCanonical != nil {
		return nil, managedAuthPathErrorStatus(errCanonical), errCanonical
	}
	if len(names) == 0 {
		return nil, http.StatusBadRequest, fmt.Errorf("names is required")
	}
	files := make([]authDownloadFile, 0, len(names))
	for _, name := range names {
		file, status, err := readDownloadAuthFileAtRoot(root, authDir, name)
		if err != nil {
			return nil, status, err
		}
		files = append(files, *file)
	}
	return files, http.StatusOK, nil
}

func buildAuthFilesArchive(files []authDownloadFile) ([]byte, error) {
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	for _, file := range files {
		w, err := zw.Create(file.Name)
		if err != nil {
			_ = zw.Close()
			return nil, fmt.Errorf("create zip entry %s: %w", file.Name, err)
		}
		if _, err = w.Write(file.Data); err != nil {
			_ = zw.Close()
			return nil, fmt.Errorf("write zip entry %s: %w", file.Name, err)
		}
	}
	if err := zw.Close(); err != nil {
		return nil, fmt.Errorf("finalize zip archive: %w", err)
	}
	return buf.Bytes(), nil
}

// Download single auth file by name
func (h *Handler) DownloadAuthFile(c *gin.Context) {
	name := strings.TrimSpace(c.Query("name"))
	file, status, err := h.readDownloadAuthFile(name)
	if err != nil {
		c.JSON(status, gin.H{"error": err.Error()})
		return
	}
	c.Header("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s\"", filepath.Base(file.Name)))
	c.Data(http.StatusOK, "application/json", file.Data)
}

// DownloadAuthFilesArchive downloads all or selected auth files as a zip archive.
func (h *Handler) DownloadAuthFilesArchive(c *gin.Context) {
	var req authFilesArchiveRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request body"})
		return
	}
	if req.All && len(req.Names) > 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "all and names are mutually exclusive"})
		return
	}
	if !req.All && len(req.Names) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "all or names is required"})
		return
	}

	var (
		files []authDownloadFile
		err   error
	)
	if req.All {
		files, err = h.listAllDownloadAuthFiles()
		if err != nil {
			status := http.StatusInternalServerError
			if errors.Is(err, fs.ErrNotExist) {
				status = http.StatusNotFound
			}
			c.JSON(status, gin.H{"error": err.Error()})
			return
		}
	} else {
		var status int
		files, status, err = h.loadDownloadAuthFiles(req.Names)
		if err != nil {
			c.JSON(status, gin.H{"error": err.Error()})
			return
		}
	}

	archive, err := buildAuthFilesArchive(files)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.Header("Content-Disposition", `attachment; filename="auth-files.zip"`)
	c.Data(http.StatusOK, "application/zip", archive)
}

// Upload auth file: multipart or raw JSON with ?name=
func (h *Handler) UploadAuthFile(c *gin.Context) {
	if h.authManager == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "core auth manager unavailable"})
		return
	}
	ctx := c.Request.Context()

	fileHeaders, errMultipart := h.multipartAuthFileHeaders(c)
	if errMultipart != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("invalid multipart form: %v", errMultipart)})
		return
	}
	if len(fileHeaders) == 1 {
		if _, errUpload := h.storeUploadedAuthFile(ctx, fileHeaders[0]); errUpload != nil {
			if errors.Is(errUpload, errAuthFileMustBeJSON) || errors.Is(errUpload, errInvalidAuthFileName) || errors.Is(errUpload, errInvalidAuthFileData) {
				message := errUpload.Error()
				if errors.Is(errUpload, errAuthFileMustBeJSON) {
					message = "file must be .json"
				}
				c.JSON(http.StatusBadRequest, gin.H{"error": message})
				return
			}
			status := http.StatusInternalServerError
			if errors.Is(errUpload, errGeminiCLIAuthGone) {
				status = http.StatusGone
			} else if errors.Is(errUpload, errAuthFileQuarantined) {
				status = http.StatusConflict
			}
			c.JSON(status, gin.H{"error": errUpload.Error()})
			return
		}
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
		return
	}
	if len(fileHeaders) > 1 {
		uploaded := make([]string, 0, len(fileHeaders))
		failed := make([]gin.H, 0)
		for _, file := range fileHeaders {
			name, errUpload := h.storeUploadedAuthFile(ctx, file)
			if errUpload != nil {
				failureName := ""
				if file != nil {
					failureName = filepath.Base(file.Filename)
				}
				msg := errUpload.Error()
				if errors.Is(errUpload, errAuthFileMustBeJSON) {
					msg = "file must be .json"
				}
				failure := gin.H{"name": failureName, "error": msg}
				if errors.Is(errUpload, errAuthFileMustBeJSON) || errors.Is(errUpload, errInvalidAuthFileName) || errors.Is(errUpload, errInvalidAuthFileData) {
					failure["status"] = http.StatusBadRequest
				} else if errors.Is(errUpload, errGeminiCLIAuthGone) {
					failure["status"] = http.StatusGone
				} else if errors.Is(errUpload, errAuthFileQuarantined) {
					failure["status"] = http.StatusConflict
				}
				failed = append(failed, failure)
				continue
			}
			uploaded = append(uploaded, name)
		}
		if len(failed) > 0 {
			c.JSON(http.StatusMultiStatus, gin.H{
				"status":   "partial",
				"uploaded": len(uploaded),
				"files":    uploaded,
				"failed":   failed,
			})
			return
		}
		c.JSON(http.StatusOK, gin.H{"status": "ok", "uploaded": len(uploaded), "files": uploaded})
		return
	}
	if c.ContentType() == "multipart/form-data" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "no files uploaded"})
		return
	}
	name := strings.TrimSpace(c.Query("name"))
	if isUnsafeAuthFileName(name) {
		c.JSON(400, gin.H{"error": "invalid name"})
		return
	}
	if !strings.HasSuffix(strings.ToLower(name), ".json") {
		c.JSON(400, gin.H{"error": "name must end with .json"})
		return
	}
	data, err := io.ReadAll(c.Request.Body)
	if err != nil {
		c.JSON(400, gin.H{"error": "failed to read body"})
		return
	}
	if err = h.writeAuthFile(ctx, filepath.Base(name), data); err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, errInvalidAuthFileName) || errors.Is(err, errInvalidAuthFileData) {
			status = http.StatusBadRequest
		} else if errors.Is(err, errGeminiCLIAuthGone) {
			status = http.StatusGone
		} else if errors.Is(err, errAuthFileQuarantined) {
			status = http.StatusConflict
		}
		c.JSON(status, gin.H{"error": err.Error()})
		return
	}
	c.JSON(200, gin.H{"status": "ok"})
}

// Delete auth files: single by name or all
func (h *Handler) DeleteAuthFile(c *gin.Context) {
	ctx := c.Request.Context()
	root, lexicalAuthDir, authDir, errRoot := h.openManagedAuthRootSnapshot()
	if errRoot != nil {
		status := http.StatusInternalServerError
		if errors.Is(errRoot, fs.ErrNotExist) {
			status = http.StatusNotFound
		}
		c.JSON(status, gin.H{"error": errRoot.Error()})
		return
	}
	defer closeManagedAuthRoot(root)
	if all := c.Query("all"); all == "true" || all == "1" || all == "*" {
		names, err := listAllManageableAuthFileNamesAtRoot(root, authDir)
		if err != nil {
			c.JSON(500, gin.H{"error": fmt.Sprintf("failed to read auth dir: %v", err)})
			return
		}
		deleted := 0
		failed := make([]gin.H, 0)
		for _, name := range names {
			if _, status, errDelete := h.deleteAuthFileByNameAtRoot(ctx, root, lexicalAuthDir, authDir, name); errDelete != nil {
				if errors.Is(errDelete, errAuthFileNotFound) || errors.Is(errDelete, fs.ErrNotExist) {
					continue
				}
				failed = append(failed, gin.H{"name": name, "status": status, "error": errDelete.Error()})
				continue
			}
			deleted++
		}
		if len(failed) > 0 {
			c.JSON(http.StatusMultiStatus, gin.H{"status": "partial", "deleted": deleted, "failed": failed})
			return
		}
		c.JSON(200, gin.H{"status": "ok", "deleted": deleted})
		return
	}

	names, errNames := requestedAuthFileNamesForDelete(c)
	if errNames != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": errNames.Error()})
		return
	}
	if len(names) == 0 {
		c.JSON(400, gin.H{"error": "invalid name"})
		return
	}
	names, errNames = h.canonicalAuthFileNamesAtRoot(root, lexicalAuthDir, authDir, names)
	if errNames != nil {
		c.JSON(managedAuthPathErrorStatus(errNames), gin.H{"error": errNames.Error()})
		return
	}
	if len(names) == 1 {
		if _, status, errDelete := h.deleteAuthFileByNameAtRoot(ctx, root, lexicalAuthDir, authDir, names[0]); errDelete != nil {
			c.JSON(status, gin.H{"error": errDelete.Error()})
			return
		}
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
		return
	}

	deletedFiles := make([]string, 0, len(names))
	failed := make([]gin.H, 0)
	for _, name := range names {
		deletedName, status, errDelete := h.deleteAuthFileByNameAtRoot(ctx, root, lexicalAuthDir, authDir, name)
		if errDelete != nil {
			failed = append(failed, gin.H{"name": name, "status": status, "error": errDelete.Error()})
			continue
		}
		deletedFiles = append(deletedFiles, deletedName)
	}
	if len(failed) > 0 {
		c.JSON(http.StatusMultiStatus, gin.H{
			"status":  "partial",
			"deleted": len(deletedFiles),
			"files":   deletedFiles,
			"failed":  failed,
		})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "ok", "deleted": len(deletedFiles), "files": deletedFiles})
}

func (h *Handler) multipartAuthFileHeaders(c *gin.Context) ([]*multipart.FileHeader, error) {
	if h == nil || c == nil || c.ContentType() != "multipart/form-data" {
		return nil, nil
	}
	form, err := c.MultipartForm()
	if err != nil {
		return nil, err
	}
	if form == nil || len(form.File) == 0 {
		return nil, nil
	}

	keys := make([]string, 0, len(form.File))
	for key := range form.File {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	headers := make([]*multipart.FileHeader, 0)
	for _, key := range keys {
		headers = append(headers, form.File[key]...)
	}
	return headers, nil
}

func (h *Handler) storeUploadedAuthFile(ctx context.Context, file *multipart.FileHeader) (string, error) {
	if file == nil {
		return "", fmt.Errorf("no file uploaded")
	}
	name := filepath.Base(strings.TrimSpace(file.Filename))
	if isUnsafeAuthFileName(name) {
		return "", errInvalidAuthFileName
	}
	if !strings.HasSuffix(strings.ToLower(name), ".json") {
		return "", errAuthFileMustBeJSON
	}
	src, err := file.Open()
	if err != nil {
		return "", fmt.Errorf("failed to open uploaded file: %w", err)
	}
	defer src.Close()

	data, err := io.ReadAll(src)
	if err != nil {
		return "", fmt.Errorf("failed to read uploaded file: %w", err)
	}
	if err := h.writeAuthFile(ctx, name, data); err != nil {
		return "", err
	}
	return name, nil
}

func (h *Handler) writeAuthFile(ctx context.Context, name string, data []byte) error {
	root, _, authDir, errRoot := h.openManagedAuthRootSnapshot()
	if errRoot != nil {
		return errRoot
	}
	defer closeManagedAuthRoot(root)
	dst, displayName, errResolve := resolveManagedAuthFilePathAtRoot(root, authDir, filepath.Base(name))
	if errResolve != nil {
		return errResolve
	}
	unlockOperation := lockManagedAuthFileOperation(dst)
	defer unlockOperation()
	relativePath := filepath.FromSlash(displayName)
	retired, _, errParse := parseRetiredGeminiCLIAuthFile(data)
	if errParse != nil {
		return fmt.Errorf("%w: %v", errInvalidAuthFileData, errParse)
	}
	if retired {
		return errGeminiCLIAuthGone
	}
	auth, errBuild := h.buildAuthFromFileData(dst, data)
	if errBuild != nil {
		return errBuild
	}
	errWrite := func() error {
		unlockPath := authfileguard.Lock(dst)
		defer unlockPath()
		if authfileguard.IsRetired(dst) {
			return coreauth.ErrRetiredGeminiCLIAuthReadOnly
		}
		if authfileguard.IsQuarantined(dst) {
			return errAuthFileQuarantined
		}
		if existingData, errRead := root.ReadFile(relativePath); errRead == nil {
			if errRetired := coreauth.RejectRetiredGeminiCLIAuthFileMutation(existingData); errRetired != nil {
				authfileguard.MarkRetired(dst)
				return errRetired
			}
		} else if !errors.Is(errRead, os.ErrNotExist) {
			return fmt.Errorf("failed to read existing auth file: %w", errRead)
		}
		return writeAuthFileSafely(root, relativePath, data)
	}()
	if errWrite != nil {
		if errors.Is(errWrite, coreauth.ErrRetiredGeminiCLIAuthReadOnly) {
			return errGeminiCLIAuthGone
		}
		return fmt.Errorf("failed to write file: %w", errWrite)
	}
	if err := h.upsertAuthRecord(ctx, auth); err != nil {
		return err
	}
	return nil
}

func writeAuthFileSafely(root *os.Root, relativePath string, data []byte) (err error) {
	if root == nil {
		return fmt.Errorf("auth root is nil")
	}
	tempPath := filepath.Join(filepath.Dir(relativePath), ".auth-upload-"+uuid.NewString())
	tempFile, errCreate := root.OpenFile(tempPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if errCreate != nil {
		return errCreate
	}
	defer func() { _ = root.Remove(tempPath) }()
	if errChmod := tempFile.Chmod(0o600); errChmod != nil {
		_ = tempFile.Close()
		return errChmod
	}
	if _, errWrite := tempFile.Write(data); errWrite != nil {
		_ = tempFile.Close()
		return errWrite
	}
	if errSync := tempFile.Sync(); errSync != nil {
		_ = tempFile.Close()
		return errSync
	}
	if errClose := tempFile.Close(); errClose != nil {
		return errClose
	}
	unlockTarget, errLock := authfileguard.LockRootTarget(root, relativePath)
	if errLock != nil {
		return fmt.Errorf("lock auth upload target: %w", errLock)
	}
	defer func() { err = errors.Join(err, unlockTarget()) }()
	if existing, errRead := root.ReadFile(relativePath); errRead == nil {
		if errRetired := coreauth.RejectRetiredGeminiCLIAuthFileMutation(existing); errRetired != nil {
			return errRetired
		}
	} else if !errors.Is(errRead, os.ErrNotExist) {
		return errRead
	}
	if errRename := root.Rename(tempPath, relativePath); errRename != nil {
		return errRename
	}
	return syncManagedAuthDirectory(root, filepath.Dir(relativePath))
}

func requestedAuthFileNamesForDelete(c *gin.Context) ([]string, error) {
	if c == nil {
		return nil, nil
	}
	names := uniqueAuthFileNames(c.QueryArray("name"))
	if len(names) > 0 {
		return names, nil
	}

	body, err := io.ReadAll(c.Request.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read body")
	}
	body = bytes.TrimSpace(body)
	if len(body) == 0 {
		return nil, nil
	}

	var objectBody struct {
		Name  string   `json:"name"`
		Names []string `json:"names"`
	}
	if body[0] == '[' {
		var arrayBody []string
		if err := json.Unmarshal(body, &arrayBody); err != nil {
			return nil, fmt.Errorf("invalid request body")
		}
		return uniqueAuthFileNames(arrayBody), nil
	}
	if err := json.Unmarshal(body, &objectBody); err != nil {
		return nil, fmt.Errorf("invalid request body")
	}

	out := make([]string, 0, len(objectBody.Names)+1)
	if strings.TrimSpace(objectBody.Name) != "" {
		out = append(out, objectBody.Name)
	}
	out = append(out, objectBody.Names...)
	return uniqueAuthFileNames(out), nil
}

func uniqueAuthFileNames(names []string) []string {
	if len(names) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(names))
	out := make([]string, 0, len(names))
	for _, name := range names {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		out = append(out, name)
	}
	return out
}

func (h *Handler) canonicalAuthFileNames(names []string) ([]string, error) {
	root, lexicalAuthDir, authDir, errRoot := h.openManagedAuthRootSnapshot()
	if errRoot != nil {
		return nil, errRoot
	}
	defer closeManagedAuthRoot(root)
	return h.canonicalAuthFileNamesAtRoot(root, lexicalAuthDir, authDir, names)
}

func (h *Handler) canonicalAuthFileNamesAtRoot(root *os.Root, lexicalAuthDir, authDir string, names []string) ([]string, error) {
	seen := make(map[string]struct{}, len(names))
	out := make([]string, 0, len(names))
	for _, name := range names {
		candidate := name
		diskFileExists, errPresence := managedAuthFileExistsAtRoot(root, authDir, name)
		if errPresence != nil {
			return nil, errPresence
		}
		if !diskFileExists && h.authManager != nil {
			lookupName := strings.TrimSpace(name)
			auth, ok := h.authManager.GetByID(lookupName)
			if !ok && runtime.GOOS == "windows" {
				auth, ok = h.authManager.GetByID(strings.ToLower(lookupName))
			}
			if ok && auth != nil && !isRuntimeOnlyAuth(auth) {
				if backingName, managed := managedAuthBackingFileNameAtRoot(root, lexicalAuthDir, authDir, auth); managed {
					candidate = backingName
				}
			}
		}
		_, canonicalName, errResolve := resolveManagedAuthFilePathAtRoot(root, authDir, candidate)
		if errResolve != nil {
			return nil, errResolve
		}
		key := managedAuthNameKey(canonicalName)
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, canonicalName)
	}
	return out, nil
}

func (h *Handler) managedAuthFileExists(name string) (bool, error) {
	root, _, authDir, errRoot := h.openManagedAuthRootSnapshot()
	if errRoot != nil {
		return false, errRoot
	}
	defer closeManagedAuthRoot(root)
	return managedAuthFileExistsAtRoot(root, authDir, name)
}

func managedAuthFileExistsAtRoot(root *os.Root, authDir, name string) (bool, error) {
	_, normalizedName, errNormalize := resolveManagedAuthFilePathAtRoot(root, authDir, name)
	if errNormalize != nil {
		if errors.Is(errNormalize, errInvalidAuthFileName) {
			return false, nil
		}
		return false, errNormalize
	}
	_, errStat := root.Lstat(filepath.FromSlash(normalizedName))
	if errStat == nil {
		return true, nil
	}
	if os.IsNotExist(errStat) {
		return false, nil
	}
	return false, fmt.Errorf("inspect auth file: %w", errStat)
}

func (h *Handler) deleteAuthFileByName(ctx context.Context, name string) (string, int, error) {
	name = strings.TrimSpace(name)
	root, lexicalAuthDir, authDir, errRoot := h.openManagedAuthRootSnapshot()
	if errRoot != nil {
		if errors.Is(errRoot, fs.ErrNotExist) {
			return "", http.StatusNotFound, errAuthFileNotFound
		}
		return "", http.StatusInternalServerError, errRoot
	}
	defer closeManagedAuthRoot(root)
	return h.deleteAuthFileByNameAtRoot(ctx, root, lexicalAuthDir, authDir, name)
}

func (h *Handler) deleteAuthFileByNameAtRoot(ctx context.Context, root *os.Root, lexicalAuthDir, authDir, name string) (string, int, error) {
	targetPath, displayName, errResolve := resolveManagedAuthFilePathAtRoot(root, authDir, name)
	if errResolve != nil {
		return "", managedAuthPathErrorStatus(errResolve), errResolve
	}
	actualName, errActualName := actualManagedAuthFileNameAtRoot(root, displayName)
	if errActualName != nil {
		return "", http.StatusInternalServerError, fmt.Errorf("resolve auth file name: %w", errActualName)
	}
	displayName = actualName
	targetPath = filepath.Join(authDir, filepath.FromSlash(displayName))
	unlockOperation := lockManagedAuthFileOperation(targetPath)
	defer unlockOperation()
	relativePath := filepath.FromSlash(displayName)
	originalSnapshot, errRead := captureManagedAuthFileSnapshotAtRoot(root, relativePath)
	if errRead != nil {
		if errors.Is(errRead, fs.ErrNotExist) {
			return displayName, http.StatusNotFound, errAuthFileNotFound
		}
		return displayName, http.StatusInternalServerError, fmt.Errorf("inspect auth file before deletion: %w", errRead)
	}
	data := originalSnapshot.data
	if errAccess := validateExplicitManagedAuthFileAccess(displayName, data); errAccess != nil {
		return displayName, http.StatusBadRequest, errAccess
	}
	if coreauth.IsRetiredGeminiCLIAuthFileData(data) {
		authfileguard.MarkRetired(targetPath)
	}
	retiredSnapshot := authfileguard.CaptureRetired(targetPath)
	targetID := ""
	if h.authManager != nil {
		for _, auth := range h.authManager.List() {
			if auth == nil || isRuntimeOnlyAuth(auth) {
				continue
			}
			managedName, managed := managedAuthBackingFileNameAtRoot(root, lexicalAuthDir, authDir, auth)
			if managed && managedAuthNameEqual(managedName, displayName) {
				targetID = strings.TrimSpace(auth.ID)
				break
			}
		}
	}
	storeID := targetID
	if storeID == "" {
		storeID = h.authIDForPath(targetPath)
	}
	deleteStore := func(operationCtx context.Context) error {
		return h.withTokenStore(func(store coreauth.Store) error {
			switch builtin := store.(type) {
			case *sdkAuth.FileTokenStore:
				return h.deleteLocalAuthFileDurably(builtin, root, authDir, filepath.FromSlash(displayName))
			case *internalstore.GitTokenStore:
				return builtin.DeleteAuthFileAtRoot(operationCtx, authDir, root, filepath.FromSlash(displayName))
			case *internalstore.ObjectTokenStore:
				return builtin.DeleteAuthFileAtRoot(operationCtx, root, filepath.FromSlash(displayName))
			case *internalstore.PostgresStore:
				return builtin.DeleteAuthFileAtRoot(operationCtx, root, filepath.FromSlash(displayName))
			}
			if dirSetter, ok := store.(interface{ SetBaseDir(string) }); ok {
				dirSetter.SetBaseDir(lexicalAuthDir)
			}
			conditionalStore, supportsConditionalDelete := store.(coreauth.SourceConditionalDeleteStore)
			if !supportsConditionalDelete {
				return coreauth.NewDeleteOutcomeError(coreauth.DeleteOutcomeRolledBack, errors.New("custom auth store does not support source-conditional deletion"))
			}
			if storeID == "" {
				return coreauth.NewDeleteOutcomeError(coreauth.DeleteOutcomeRolledBack, errors.New("managed auth store identifier is empty"))
			}
			return h.deleteCustomAuthFileDurably(operationCtx, conditionalStore, storeID, root, authDir, relativePath, targetPath, originalSnapshot)
		})
	}
	var errDelete error
	if targetID != "" && h.authManager != nil {
		errDelete = h.authManager.DeleteWithOperationFailClosed(ctx, targetID, deleteStore)
	} else {
		errDelete = deleteStore(ctx)
		if outcome, ok := coreauth.DeleteOutcomeFromError(errDelete); ok && outcome == coreauth.DeleteOutcomeCommitted {
			errDelete = nil
		}
	}
	if errDelete != nil {
		if errors.Is(errDelete, errAuthFileNotFound) || errors.Is(errDelete, fs.ErrNotExist) {
			return displayName, http.StatusNotFound, errAuthFileNotFound
		}
		return displayName, http.StatusInternalServerError, errDelete
	}
	authfileguard.ClearRetiredSnapshot(retiredSnapshot)
	if targetID == "" && h.authManager != nil {
		for _, auth := range h.authManager.List() {
			if !authBackedByManagedPath(auth, targetPath, authDir) {
				continue
			}
			if errRuntimeDelete := h.authManager.Delete(coreauth.WithSkipPersist(ctx), auth.ID); errRuntimeDelete != nil {
				return displayName, http.StatusInternalServerError, fmt.Errorf("remove deleted auth from runtime: %w", errRuntimeDelete)
			}
		}
	}
	return displayName, http.StatusOK, nil
}

func removeManagedAuthFileSnapshot(root *os.Root, relativePath string, original managedAuthFileSnapshot) (resultErr error) {
	parentRoot, leaf, closeParent, errParent := openManagedAuthSnapshotParent(root, relativePath)
	if errParent != nil {
		return coreauth.NewDeleteOutcomeError(coreauth.DeleteOutcomeUncertain, fmt.Errorf("open auth parent after store deletion: %w", errParent))
	}
	defer func() {
		if errClose := closeParent(); errClose != nil {
			outcome := coreauth.DeleteOutcomeCommitted
			if resultErr != nil {
				if current, explicit := coreauth.DeleteOutcomeFromError(resultErr); explicit {
					outcome = current
				} else {
					outcome = coreauth.DeleteOutcomeUncertain
				}
			}
			resultErr = coreauth.NewDeleteOutcomeError(outcome, errors.Join(resultErr, fmt.Errorf("close auth parent after store deletion: %w", errClose)))
		}
	}()
	return removeManagedAuthFileSnapshotAtParent(parentRoot, leaf, original)
}

func removeManagedAuthFileSnapshotAtParent(parentRoot *os.Root, leaf string, original managedAuthFileSnapshot) (resultErr error) {
	unlockTarget, errLock := authfileguard.LockRootTarget(parentRoot, leaf)
	if errLock != nil {
		return coreauth.NewDeleteOutcomeError(coreauth.DeleteOutcomeUncertain, fmt.Errorf("lock auth file after store deletion: %w", errLock))
	}
	defer func() {
		if errUnlock := unlockTarget(); errUnlock != nil {
			outcome := coreauth.DeleteOutcomeCommitted
			if resultErr != nil {
				if current, explicit := coreauth.DeleteOutcomeFromError(resultErr); explicit {
					outcome = current
				} else {
					outcome = coreauth.DeleteOutcomeUncertain
				}
			}
			resultErr = coreauth.NewDeleteOutcomeError(outcome, errors.Join(resultErr, fmt.Errorf("unlock auth file after store deletion: %w", errUnlock)))
		}
	}()

	current, errCurrent := captureManagedAuthFileSnapshotAtRoot(parentRoot, leaf)
	if errCurrent != nil {
		if errors.Is(errCurrent, fs.ErrNotExist) {
			return nil
		}
		return coreauth.NewDeleteOutcomeError(coreauth.DeleteOutcomeUncertain, fmt.Errorf("revalidate auth file after store deletion: %w", errCurrent))
	}
	if !sameManagedAuthFileSnapshot(current, original) {
		return coreauth.NewDeleteOutcomeError(coreauth.DeleteOutcomeUncertain, authfileguard.ErrPersistGenerationStale)
	}
	if errRemove := parentRoot.Remove(leaf); errRemove != nil {
		if os.IsNotExist(errRemove) {
			return nil
		}
		return coreauth.NewDeleteOutcomeError(coreauth.DeleteOutcomeUncertain, fmt.Errorf("failed to remove file: %w", errRemove))
	}
	if errSync := syncManagedAuthDirectory(parentRoot, "."); errSync != nil {
		return coreauth.NewDeleteOutcomeError(coreauth.DeleteOutcomeUncertain, fmt.Errorf("sync removed auth file: %w", errSync))
	}
	return nil
}

func (h *Handler) deleteCustomAuthFileDurably(ctx context.Context, store coreauth.SourceConditionalDeleteStore, storeID string, root *os.Root, authDir, relativePath, targetPath string, original managedAuthFileSnapshot) error {
	expectedHash := coreauth.SourceHashFromBytes(original.data)
	generation := authfileguard.NewDeleteGeneration(expectedHash)
	if errPersist := watcher.PersistAuthDeleteQuarantine(h.configFilePath, authDir, targetPath, generation); errPersist != nil {
		return coreauth.NewDeleteOutcomeError(coreauth.DeleteOutcomeRolledBack, fmt.Errorf("persist custom store delete quarantine: %w", errPersist))
	}

	deleteCtx := authfileguard.WithDeleteGeneration(ctx, generation)
	errDelete := store.DeleteIfSourceHashMatches(deleteCtx, storeID, expectedHash)
	if errDelete != nil {
		outcome, explicit := coreauth.DeleteOutcomeFromError(errDelete)
		if !explicit || outcome == coreauth.DeleteOutcomeUncertain {
			return errDelete
		}
		if outcome == coreauth.DeleteOutcomeRolledBack {
			if errClear := watcher.ClearAuthDeleteQuarantine(h.configFilePath, authDir, targetPath, generation); errClear != nil {
				return coreauth.NewDeleteOutcomeError(coreauth.DeleteOutcomeUncertain, errors.Join(errDelete, fmt.Errorf("clear rolled-back custom store delete quarantine: %w", errClear)))
			}
			return errDelete
		}
	}

	if errRemove := removeManagedAuthFileSnapshot(root, relativePath, original); errRemove != nil {
		outcome, explicit := coreauth.DeleteOutcomeFromError(errRemove)
		if !explicit {
			outcome = coreauth.DeleteOutcomeUncertain
		}
		return coreauth.NewDeleteOutcomeError(outcome, errors.Join(errDelete, errRemove))
	}
	if errClear := watcher.ClearAuthDeleteQuarantine(h.configFilePath, authDir, targetPath, generation); errClear != nil {
		return coreauth.NewDeleteOutcomeError(coreauth.DeleteOutcomeCommitted, errors.Join(errDelete, fmt.Errorf("clear committed custom store delete quarantine: %w", errClear)))
	}
	return errDelete
}

func (h *Handler) deleteLocalAuthFileDurably(store *sdkAuth.FileTokenStore, root *os.Root, authDir, name string) error {
	if store == nil {
		return coreauth.NewDeleteOutcomeError(coreauth.DeleteOutcomeRolledBack, errors.New("auth filestore is nil"))
	}
	preparedPath := ""
	var deleteGeneration *authfileguard.DeleteGeneration
	errDelete := store.DeleteAuthFileAtRootPrepared(authDir, root, name, func(_ string, data []byte) error {
		generation := authfileguard.NewDeleteGeneration(coreauth.SourceHashFromBytes(data))
		path := filepath.Join(authDir, name)
		if errPersist := watcher.PersistAuthDeleteQuarantine(h.configFilePath, authDir, path, generation); errPersist != nil {
			return errPersist
		}
		preparedPath = path
		deleteGeneration = generation
		return nil
	})
	if errDelete != nil || preparedPath == "" {
		return errDelete
	}
	if errClear := watcher.ClearAuthDeleteQuarantine(h.configFilePath, authDir, preparedPath, deleteGeneration); errClear != nil {
		return coreauth.NewDeleteOutcomeError(coreauth.DeleteOutcomeCommitted, fmt.Errorf("clear local auth deletion quarantine: %w", errClear))
	}
	return nil
}

func authBackedByManagedPath(auth *coreauth.Auth, targetPath, authDir string) bool {
	if auth == nil || isRuntimeOnlyAuth(auth) {
		return false
	}
	path := strings.TrimSpace(authAttribute(auth, "path"))
	if path == "" {
		path = strings.TrimSpace(auth.FileName)
	}
	if path == "" {
		return false
	}
	if !filepath.IsAbs(path) {
		path = filepath.Join(authDir, filepath.FromSlash(path))
	}
	pathAbs, errPath := filepath.Abs(path)
	targetAbs, errTarget := filepath.Abs(targetPath)
	if errPath != nil || errTarget != nil {
		return false
	}
	pathAbs = filepath.Clean(pathAbs)
	targetAbs = filepath.Clean(targetAbs)
	if runtime.GOOS == "windows" {
		return strings.EqualFold(pathAbs, targetAbs)
	}
	return pathAbs == targetAbs
}

func (h *Handler) findManagedFileAuth(name string) *coreauth.Auth {
	if h == nil || h.authManager == nil {
		return nil
	}
	root, lexicalAuthDir, authDir, errRoot := h.openManagedAuthRootSnapshot()
	if errRoot != nil {
		return nil
	}
	defer closeManagedAuthRoot(root)
	_, normalizedName, errNormalize := resolveManagedAuthFilePathAtRoot(root, authDir, name)
	if errNormalize != nil {
		return nil
	}
	for _, auth := range h.authManager.List() {
		if auth == nil || isRuntimeOnlyAuth(auth) {
			continue
		}
		managedName, managed := managedAuthBackingFileNameAtRoot(root, lexicalAuthDir, authDir, auth)
		if managed && managedAuthNameEqual(managedName, normalizedName) {
			return auth
		}
	}
	return nil
}

func (h *Handler) findManagedAuth(name string) *coreauth.Auth {
	if h == nil || h.authManager == nil {
		return nil
	}
	name = strings.TrimSpace(name)
	if name == "" {
		return nil
	}
	exactAuth, _ := h.authManager.GetByID(name)
	if exactAuth != nil && isRuntimeOnlyAuth(exactAuth) {
		return exactAuth
	}
	if runtime.GOOS == "windows" && exactAuth == nil {
		exactAuth, _ = h.authManager.GetByID(strings.ToLower(name))
		if exactAuth != nil && isRuntimeOnlyAuth(exactAuth) {
			return exactAuth
		}
	}
	if exists, errPresence := h.managedAuthFileExists(name); errPresence == nil && exists {
		return h.findManagedFileAuth(name)
	}
	if exactAuth != nil {
		return exactAuth
	}
	auths := h.authManager.List()
	for _, auth := range auths {
		if auth != nil && isRuntimeOnlyAuth(auth) && managedAuthNameEqual(strings.TrimSpace(auth.FileName), name) {
			return auth
		}
	}
	normalizedName, errNormalize := normalizeManagedAuthFileName(name)
	if errNormalize != nil {
		return nil
	}
	lookupID := normalizedName
	if runtime.GOOS == "windows" {
		lookupID = strings.ToLower(lookupID)
	}
	if auth, ok := h.authManager.GetByID(lookupID); ok {
		return auth
	}
	for _, auth := range auths {
		if auth == nil {
			continue
		}
		if managedAuthNameEqual(filepath.ToSlash(strings.TrimSpace(auth.ID)), normalizedName) {
			return auth
		}
		managedName, managed := h.managedAuthBackingFileName(auth)
		if managed && managedAuthNameEqual(managedName, normalizedName) {
			return auth
		}
	}
	return nil
}

func managedAuthNameEqual(a, b string) bool {
	if runtime.GOOS == "windows" {
		return strings.EqualFold(a, b)
	}
	return a == b
}

func managedAuthNameKey(name string) string {
	return managedAuthNameKeyForOS(name, runtime.GOOS)
}

func isTopLevelManagedAuthName(name string) bool {
	name = filepath.ToSlash(strings.TrimSpace(name))
	return name != "" && !strings.Contains(name, "/")
}

func managedAuthNameKeyForOS(name, goos string) string {
	if goos == "windows" {
		return strings.ToLower(name)
	}
	return name
}

func (h *Handler) sameManagedAuthFile(a, b string) bool {
	if managedAuthNameEqual(a, b) {
		return true
	}
	root, _, authDir, errRoot := h.openManagedAuthRootSnapshot()
	if errRoot != nil {
		return false
	}
	defer closeManagedAuthRoot(root)
	_, nameA, errA := resolveManagedAuthFilePathAtRoot(root, authDir, a)
	_, nameB, errB := resolveManagedAuthFilePathAtRoot(root, authDir, b)
	if errA != nil || errB != nil {
		return false
	}
	infoA, errA := root.Stat(filepath.FromSlash(nameA))
	infoB, errB := root.Stat(filepath.FromSlash(nameB))
	return errA == nil && errB == nil && os.SameFile(infoA, infoB)
}

func (h *Handler) authIDForPath(path string) string {
	path = strings.TrimSpace(path)
	if path == "" {
		return ""
	}
	path = filepath.Clean(path)
	if !filepath.IsAbs(path) {
		if abs, errAbs := filepath.Abs(path); errAbs == nil {
			path = abs
		}
	}
	lexicalPath := filepath.Clean(path)
	resolvedPath := lexicalPath
	if resolved, errEval := filepath.EvalSymlinks(lexicalPath); errEval == nil {
		resolvedPath = filepath.Clean(resolved)
	}
	id := lexicalPath
	setRelativeID := func(root, candidate string) bool {
		if root == "" {
			return false
		}
		if rel, errRel := filepath.Rel(root, candidate); errRel == nil && rel != "" && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
			id = rel
			return true
		}
		return false
	}
	if authDir, errAuthDir := h.resolvedAuthDir(); errAuthDir == nil {
		if !setRelativeID(authDir, resolvedPath) && h != nil && h.cfg != nil {
			if lexicalAuthDir, errResolve := util.ResolveAuthDir(strings.TrimSpace(h.cfg.AuthDir)); errResolve == nil && lexicalAuthDir != "" {
				if !filepath.IsAbs(lexicalAuthDir) {
					if abs, errAbs := filepath.Abs(lexicalAuthDir); errAbs == nil {
						lexicalAuthDir = abs
					}
				}
				setRelativeID(filepath.Clean(lexicalAuthDir), lexicalPath)
			}
		}
	}
	// On Windows, normalize ID casing to avoid duplicate auth entries caused by case-insensitive paths.
	if runtime.GOOS == "windows" {
		id = strings.ToLower(id)
	}
	return id
}

func (h *Handler) registerAuthFromFile(ctx context.Context, path string, data []byte) error {
	if h.authManager == nil {
		return nil
	}
	retired, _, errParse := parseRetiredGeminiCLIAuthFile(data)
	if errParse != nil {
		return fmt.Errorf("invalid auth file: %w", errParse)
	}
	if retired {
		return nil
	}
	auth, err := h.buildAuthFromFileData(path, data)
	if err != nil {
		return err
	}
	return h.upsertAuthRecord(ctx, auth)
}

func parseRetiredGeminiCLIAuthFile(data []byte) (bool, map[string]any, error) {
	metadata := make(map[string]any)
	if errUnmarshal := json.Unmarshal(data, &metadata); errUnmarshal != nil {
		return false, nil, errUnmarshal
	}
	return coreauth.IsRetiredGeminiCLIAuthFileData(data), metadata, nil
}

func (h *Handler) rejectRetiredGeminiCLIAuthFileOperation(c *gin.Context, name string) bool {
	auth := h.findManagedAuth(name)
	if auth != nil {
		if isRuntimeOnlyAuth(auth) {
			return false
		}
		retired, errCheck := h.authBackedByRetiredGeminiCLIFile(auth)
		if errCheck != nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "unable to verify auth file"})
			return true
		}
		if retired {
			c.JSON(http.StatusGone, gin.H{"error": errGeminiCLIAuthGone.Error()})
			return true
		}
		if backingName, managed := h.managedAuthBackingFileName(auth); managed && !isTopLevelManagedAuthName(backingName) {
			c.JSON(http.StatusBadRequest, gin.H{"error": errInvalidAuthFileName.Error()})
			return true
		}
		return false
	}
	if !strings.HasSuffix(strings.ToLower(strings.TrimSpace(name)), ".json") {
		return false
	}
	retired, errCheck := h.isRetiredGeminiCLIManagedFile(name)
	if errCheck != nil {
		status := http.StatusServiceUnavailable
		message := "unable to verify auth file"
		switch {
		case errors.Is(errCheck, errInvalidAuthFileName):
			status = http.StatusBadRequest
			message = errInvalidAuthFileName.Error()
		case errors.Is(errCheck, fs.ErrNotExist):
			status = http.StatusNotFound
			message = "auth file not found"
		}
		c.JSON(status, gin.H{"error": message})
		return true
	}
	if retired {
		c.JSON(http.StatusGone, gin.H{"error": errGeminiCLIAuthGone.Error()})
		return true
	}
	normalizedName, errNormalize := normalizeManagedAuthFileName(name)
	if errNormalize == nil && !isTopLevelManagedAuthName(normalizedName) {
		c.JSON(http.StatusBadRequest, gin.H{"error": errInvalidAuthFileName.Error()})
		return true
	}
	return false
}

func (h *Handler) isRetiredGeminiCLIManagedFile(name string) (bool, error) {
	data, _, _, errRead := h.readManagedAuthFile(name)
	if errRead != nil {
		return false, errRead
	}
	retired, _, errParse := parseRetiredGeminiCLIAuthFile(data)
	if errParse != nil {
		return false, errParse
	}
	return retired, nil
}

func (h *Handler) authBackedByRetiredGeminiCLIFile(auth *coreauth.Auth) (bool, error) {
	if auth == nil || isRuntimeOnlyAuth(auth) {
		return false, nil
	}
	if isProvablyConfigBackedAuth(auth) {
		return false, nil
	}
	root, lexicalAuthDir, authDir, errRoot := h.openManagedAuthRootSnapshot()
	if errRoot != nil {
		return false, errRoot
	}
	defer closeManagedAuthRoot(root)
	explicitPath := strings.TrimSpace(authAttribute(auth, "path")) != "" || strings.TrimSpace(authAttribute(auth, "source")) != ""
	name, managed := managedAuthBackingFileNameAtRoot(root, lexicalAuthDir, authDir, auth)
	if !managed {
		return false, nil
	}
	data, _, _, errRead := readManagedAuthFileAtRoot(root, authDir, name)
	if errRead != nil {
		if errors.Is(errRead, fs.ErrNotExist) && !explicitPath {
			return false, nil
		}
		return false, errRead
	}
	retired, _, errParse := parseRetiredGeminiCLIAuthFile(data)
	if errParse != nil {
		return false, errParse
	}
	return retired, nil
}

func isProvablyConfigBackedAuth(auth *coreauth.Auth) bool {
	if auth == nil || strings.TrimSpace(auth.FileName) != "" || strings.TrimSpace(authAttribute(auth, "path")) != "" {
		return false
	}

	const configSourcePrefix = "config:"
	source := strings.TrimSpace(authAttribute(auth, "source"))
	if len(source) <= len(configSourcePrefix) || !strings.EqualFold(source[:len(configSourcePrefix)], configSourcePrefix) || !strings.HasSuffix(source, "]") {
		return false
	}
	sourceIdentity := source[len(configSourcePrefix) : len(source)-1]
	openBracket := strings.LastIndexByte(sourceIdentity, '[')
	return openBracket > 0 && openBracket < len(sourceIdentity)-1
}

func (h *Handler) managedAuthBackingFileName(auth *coreauth.Auth) (string, bool) {
	if auth == nil || isRuntimeOnlyAuth(auth) {
		return "", false
	}
	root, lexicalAuthDir, authDir, errRoot := h.openManagedAuthRootSnapshot()
	if errRoot != nil {
		return "", false
	}
	defer closeManagedAuthRoot(root)
	return managedAuthBackingFileNameAtRoot(root, lexicalAuthDir, authDir, auth)
}

func managedAuthBackingFileNameAtRoot(root *os.Root, lexicalAuthDir, authDir string, auth *coreauth.Auth) (string, bool) {
	if auth == nil || isRuntimeOnlyAuth(auth) {
		return "", false
	}
	path := strings.TrimSpace(authAttribute(auth, "path"))
	if path != "" {
		if name, managed := managedAuthFileNameAtRoot(root, lexicalAuthDir, authDir, path); managed {
			return name, true
		}
		return "", false
	}
	fileName := strings.TrimSpace(auth.FileName)
	if name, managed := managedAuthFileNameAtRoot(root, lexicalAuthDir, authDir, fileName); managed {
		return name, true
	}
	if _, name, errResolve := resolveManagedAuthFilePathAtRoot(root, authDir, fileName); errResolve == nil {
		return name, true
	}
	return "", false
}

func (h *Handler) buildAuthFromFileData(path string, data []byte) (*coreauth.Auth, error) {
	if path == "" {
		return nil, fmt.Errorf("auth path is empty")
	}
	if data == nil {
		var err error
		data, err = os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("failed to read auth file: %w", err)
		}
	}
	metadata := make(map[string]any)
	if err := json.Unmarshal(data, &metadata); err != nil {
		return nil, fmt.Errorf("%w: %v", errInvalidAuthFileData, err)
	}
	provider, _ := metadata["type"].(string)
	if provider == "" {
		provider = "unknown"
	}
	label := provider
	if email, ok := metadata["email"].(string); ok && email != "" {
		label = email
	}
	lastRefresh, hasLastRefresh := extractLastRefreshTimestamp(metadata)

	authID := h.authIDForPath(path)
	if authID == "" {
		authID = path
	}
	attr := map[string]string{
		"path":   path,
		"source": path,
	}
	if strings.EqualFold(strings.TrimSpace(provider), "codex") {
		if planType := codex.EffectivePlanType(metadata); planType != "" {
			attr["plan_type"] = planType
		}
	}
	disabled, _ := metadata["disabled"].(bool)
	status := coreauth.StatusActive
	if disabled {
		status = coreauth.StatusDisabled
	}
	auth := &coreauth.Auth{
		ID:         authID,
		Provider:   provider,
		FileName:   filepath.Base(path),
		Label:      label,
		Status:     status,
		Disabled:   disabled,
		Attributes: attr,
		Metadata:   metadata,
		CreatedAt:  time.Now(),
		UpdatedAt:  time.Now(),
	}
	coreauth.ApplyFileBackedGeminiAPIKey(auth)
	if hasLastRefresh {
		auth.LastRefreshedAt = lastRefresh
	}
	if err := coreauth.SetCanonicalSourceHashAttribute(auth); err != nil {
		return nil, fmt.Errorf("failed to canonicalize auth metadata: %w", err)
	}
	if h != nil && h.authManager != nil {
		if existing, ok := h.authManager.GetByID(authID); ok {
			auth.CreatedAt = existing.CreatedAt
			if !hasLastRefresh {
				auth.LastRefreshedAt = existing.LastRefreshedAt
			}
			auth.NextRefreshAfter = existing.NextRefreshAfter
			auth.Runtime = existing.Runtime
		}
	}
	coreauth.ApplyCustomHeadersFromMetadata(auth)
	return auth, nil
}

func (h *Handler) upsertAuthRecord(ctx context.Context, auth *coreauth.Auth) error {
	if h == nil || h.authManager == nil || auth == nil {
		return nil
	}
	if existing, ok := h.authManager.GetByID(auth.ID); ok {
		auth.CreatedAt = existing.CreatedAt
		_, err := h.authManager.Update(ctx, auth)
		return err
	}
	_, err := h.authManager.Register(ctx, auth)
	return err
}

// PatchAuthFileStatus toggles the disabled state of an auth file
func (h *Handler) PatchAuthFileStatus(c *gin.Context) {
	if h.authManager == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "core auth manager unavailable"})
		return
	}

	var req struct {
		Name     string `json:"name"`
		Disabled *bool  `json:"disabled"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request body"})
		return
	}

	name := strings.TrimSpace(req.Name)
	if name == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "name is required"})
		return
	}
	if req.Disabled == nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "disabled is required"})
		return
	}
	if h.rejectRetiredGeminiCLIAuthFileOperation(c, name) {
		return
	}

	ctx := c.Request.Context()

	targetAuth := h.findManagedAuth(name)

	if targetAuth == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "auth file not found"})
		return
	}

	// Update disabled state
	targetAuth.Disabled = *req.Disabled
	if *req.Disabled {
		targetAuth.Status = coreauth.StatusDisabled
		targetAuth.StatusMessage = "disabled via management API"
		if targetAuth.Metadata == nil {
			targetAuth.Metadata = make(map[string]any)
		}
		targetAuth.Metadata["disabled"] = true
	} else {
		now := time.Now()
		targetAuth.Status = coreauth.StatusActive
		targetAuth.StatusMessage = ""
		targetAuth.Unavailable = false
		targetAuth.NextRetryAfter = time.Time{}
		targetAuth.LastError = nil
		targetAuth.Quota = coreauth.QuotaState{}
		for _, state := range targetAuth.ModelStates {
			if state == nil {
				continue
			}
			state.Status = coreauth.StatusActive
			state.StatusMessage = ""
			state.Unavailable = false
			state.NextRetryAfter = time.Time{}
			state.LastError = nil
			state.Quota = coreauth.QuotaState{}
			state.UpdatedAt = now
		}
		if targetAuth.Metadata != nil {
			delete(targetAuth.Metadata, "disabled")
			for key := range targetAuth.Metadata {
				if strings.HasPrefix(key, "auth_maintenance_") {
					delete(targetAuth.Metadata, key)
				}
			}
		}
	}
	targetAuth.UpdatedAt = time.Now()

	updateCtx := ctx
	if !*req.Disabled {
		updateCtx = coreauth.WithSkipStateCarryForward(updateCtx)
	}
	if _, err := h.authManager.Update(updateCtx, targetAuth); err != nil {
		if errors.Is(err, coreauth.ErrRetiredGeminiCLIAuthReadOnly) {
			c.JSON(http.StatusGone, gin.H{"error": errGeminiCLIAuthGone.Error()})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("failed to update auth: %v", err)})
		return
	}
	if h.authStatusHook != nil {
		h.authStatusHook(ctx, targetAuth.Clone())
	}

	c.JSON(http.StatusOK, gin.H{"status": "ok", "disabled": *req.Disabled})
}

// PatchAuthFileFields updates editable fields of an auth file.
func (h *Handler) PatchAuthFileFields(c *gin.Context) {
	if h.authManager == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "core auth manager unavailable"})
		return
	}

	var req struct {
		Name       string            `json:"name"`
		Prefix     *string           `json:"prefix"`
		ProxyURL   *string           `json:"proxy_url"`
		Headers    map[string]string `json:"headers"`
		Priority   *int              `json:"priority"`
		Note       *string           `json:"note"`
		UsingAPI   *bool             `json:"using_api"`
		Websockets *bool             `json:"websockets"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request body"})
		return
	}

	name := strings.TrimSpace(req.Name)
	if name == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "name is required"})
		return
	}
	if h.rejectRetiredGeminiCLIAuthFileOperation(c, name) {
		return
	}

	ctx := c.Request.Context()

	targetAuth := h.findManagedAuth(name)

	if targetAuth == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "auth file not found"})
		return
	}
	if (req.UsingAPI != nil || req.Websockets != nil) && !strings.EqualFold(strings.TrimSpace(targetAuth.Provider), "xai") {
		c.JSON(http.StatusBadRequest, gin.H{"error": "using_api and websockets are only supported for xai auth files"})
		return
	}

	changed := false
	if req.Prefix != nil {
		prefix := strings.TrimSpace(*req.Prefix)
		targetAuth.Prefix = prefix
		if targetAuth.Metadata == nil {
			targetAuth.Metadata = make(map[string]any)
		}
		if prefix == "" {
			delete(targetAuth.Metadata, "prefix")
		} else {
			targetAuth.Metadata["prefix"] = prefix
		}
		changed = true
	}
	if req.ProxyURL != nil {
		proxyURL := strings.TrimSpace(*req.ProxyURL)
		targetAuth.ProxyURL = proxyURL
		if targetAuth.Metadata == nil {
			targetAuth.Metadata = make(map[string]any)
		}
		if proxyURL == "" {
			delete(targetAuth.Metadata, "proxy_url")
		} else {
			targetAuth.Metadata["proxy_url"] = proxyURL
		}
		changed = true
	}
	if len(req.Headers) > 0 {
		existingHeaders := coreauth.ExtractCustomHeadersFromMetadata(targetAuth.Metadata)
		nextHeaders := make(map[string]string, len(existingHeaders))
		for k, v := range existingHeaders {
			nextHeaders[k] = v
		}
		headerChanged := false

		for key, value := range req.Headers {
			name := strings.TrimSpace(key)
			if name == "" {
				continue
			}
			val := strings.TrimSpace(value)
			attrKey := "header:" + name
			if val == "" {
				if _, ok := nextHeaders[name]; ok {
					delete(nextHeaders, name)
					headerChanged = true
				}
				if targetAuth.Attributes != nil {
					if _, ok := targetAuth.Attributes[attrKey]; ok {
						headerChanged = true
					}
				}
				continue
			}
			if prev, ok := nextHeaders[name]; !ok || prev != val {
				headerChanged = true
			}
			nextHeaders[name] = val
			if targetAuth.Attributes != nil {
				if prev, ok := targetAuth.Attributes[attrKey]; !ok || prev != val {
					headerChanged = true
				}
			} else {
				headerChanged = true
			}
		}

		if headerChanged {
			if targetAuth.Metadata == nil {
				targetAuth.Metadata = make(map[string]any)
			}
			if targetAuth.Attributes == nil {
				targetAuth.Attributes = make(map[string]string)
			}

			for key, value := range req.Headers {
				name := strings.TrimSpace(key)
				if name == "" {
					continue
				}
				val := strings.TrimSpace(value)
				attrKey := "header:" + name
				if val == "" {
					delete(nextHeaders, name)
					delete(targetAuth.Attributes, attrKey)
					continue
				}
				nextHeaders[name] = val
				targetAuth.Attributes[attrKey] = val
			}

			if len(nextHeaders) == 0 {
				delete(targetAuth.Metadata, "headers")
			} else {
				metaHeaders := make(map[string]any, len(nextHeaders))
				for k, v := range nextHeaders {
					metaHeaders[k] = v
				}
				targetAuth.Metadata["headers"] = metaHeaders
			}
			changed = true
		}
	}
	if req.Priority != nil || req.Note != nil {
		if targetAuth.Metadata == nil {
			targetAuth.Metadata = make(map[string]any)
		}
		if targetAuth.Attributes == nil {
			targetAuth.Attributes = make(map[string]string)
		}

		if req.Priority != nil {
			if *req.Priority == 0 {
				delete(targetAuth.Metadata, "priority")
				delete(targetAuth.Attributes, "priority")
			} else {
				targetAuth.Metadata["priority"] = *req.Priority
				targetAuth.Attributes["priority"] = strconv.Itoa(*req.Priority)
			}
		}
		if req.Note != nil {
			trimmedNote := strings.TrimSpace(*req.Note)
			if trimmedNote == "" {
				delete(targetAuth.Metadata, "note")
				delete(targetAuth.Attributes, "note")
			} else {
				targetAuth.Metadata["note"] = trimmedNote
				targetAuth.Attributes["note"] = trimmedNote
			}
		}
		changed = true
	}
	if req.UsingAPI != nil || req.Websockets != nil {
		if targetAuth.Metadata == nil {
			targetAuth.Metadata = make(map[string]any)
		}
		if targetAuth.Attributes == nil {
			targetAuth.Attributes = make(map[string]string)
		}
		if req.UsingAPI != nil {
			targetAuth.Metadata["using_api"] = *req.UsingAPI
			targetAuth.Attributes["using_api"] = strconv.FormatBool(*req.UsingAPI)
		}
		if req.Websockets != nil {
			targetAuth.Metadata["websockets"] = *req.Websockets
			targetAuth.Attributes["websockets"] = strconv.FormatBool(*req.Websockets)
		}
		changed = true
	}

	if !changed {
		c.JSON(http.StatusBadRequest, gin.H{"error": "no fields to update"})
		return
	}

	targetAuth.UpdatedAt = time.Now()

	if _, err := h.authManager.Update(ctx, targetAuth); err != nil {
		if errors.Is(err, coreauth.ErrRetiredGeminiCLIAuthReadOnly) {
			c.JSON(http.StatusGone, gin.H{"error": errGeminiCLIAuthGone.Error()})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("failed to update auth: %v", err)})
		return
	}

	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

func (h *Handler) disableAuth(ctx context.Context, id string) {
	if h == nil || h.authManager == nil {
		return
	}
	id = strings.TrimSpace(id)
	if id == "" {
		return
	}
	if auth, ok := h.authManager.GetByID(id); ok {
		auth.Disabled = true
		auth.Status = coreauth.StatusDisabled
		auth.StatusMessage = "removed via management API"
		auth.UpdatedAt = time.Now()
		if _, err := h.authManager.Update(ctx, auth); err != nil {
			log.Errorf("failed to disable auth %s: %v", id, err)
		}
		return
	}
	authID := h.authIDForPath(id)
	if authID == "" {
		return
	}
	if auth, ok := h.authManager.GetByID(authID); ok {
		auth.Disabled = true
		auth.Status = coreauth.StatusDisabled
		auth.StatusMessage = "removed via management API"
		auth.UpdatedAt = time.Now()
		if _, err := h.authManager.Update(ctx, auth); err != nil {
			log.Errorf("failed to disable auth %s: %v", authID, err)
		}
	}
}

func (h *Handler) configuredAuthDirSnapshot() string {
	if h == nil {
		return ""
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.cfg == nil {
		return ""
	}
	return h.cfg.AuthDir
}

func (h *Handler) withTokenStoreBaseDir(baseDir string, operation func(coreauth.Store) error) error {
	return h.withTokenStore(func(store coreauth.Store) error {
		if dirSetter, ok := store.(interface{ SetBaseDir(string) }); ok {
			dirSetter.SetBaseDir(baseDir)
		}
		return operation(store)
	})
}

func (h *Handler) withTokenStore(operation func(coreauth.Store) error) error {
	if h == nil || operation == nil {
		return fmt.Errorf("token store unavailable")
	}
	h.tokenStoreMu.Lock()
	defer h.tokenStoreMu.Unlock()

	h.mu.Lock()
	store := h.tokenStore
	if store == nil {
		store = sdkAuth.GetTokenStore()
		h.tokenStore = store
	}
	h.mu.Unlock()
	if store == nil {
		return fmt.Errorf("token store unavailable")
	}
	return operation(store)
}

func (h *Handler) saveTokenRecord(ctx context.Context, record *coreauth.Auth) (string, error) {
	if record == nil {
		return "", fmt.Errorf("token record is nil")
	}
	if h.postAuthHook != nil {
		if err := h.postAuthHook(ctx, record); err != nil {
			return "", fmt.Errorf("post-auth hook failed: %w", err)
		}
	}
	var savedPath string
	errSave := h.withTokenStoreBaseDir(h.configuredAuthDirSnapshot(), func(store coreauth.Store) error {
		var err error
		savedPath, err = store.Save(ctx, record)
		return err
	})
	return savedPath, errSave
}

func (h *Handler) RequestAnthropicToken(c *gin.Context) {
	ctx := context.Background()
	ctx = PopulateAuthContext(ctx, c)

	fmt.Println("Initializing Claude authentication...")

	// Generate PKCE codes
	pkceCodes, err := claude.GeneratePKCECodes()
	if err != nil {
		log.Errorf("Failed to generate PKCE codes: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to generate PKCE codes"})
		return
	}

	// Generate random state parameter
	state, err := misc.GenerateRandomState()
	if err != nil {
		log.Errorf("Failed to generate state parameter: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to generate state parameter"})
		return
	}

	// Initialize Claude auth service
	anthropicAuth := claude.NewClaudeAuth(h.cfg)

	// Generate authorization URL (then override redirect_uri to reuse server port)
	authURL, state, err := anthropicAuth.GenerateAuthURL(state, pkceCodes)
	if err != nil {
		log.Errorf("Failed to generate authorization URL: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to generate authorization url"})
		return
	}

	RegisterOAuthSession(state, "anthropic")

	isWebUI := isWebUIRequest(c)
	var forwarder *callbackForwarder
	if isWebUI {
		targetURL, errTarget := h.managementCallbackURL("/anthropic/callback")
		if errTarget != nil {
			log.WithError(errTarget).Error("failed to compute anthropic callback target")
			c.JSON(http.StatusInternalServerError, gin.H{"error": "callback server unavailable"})
			return
		}
		var errStart error
		if forwarder, errStart = startCallbackForwarder(anthropicCallbackPort, "anthropic", targetURL); errStart != nil {
			log.WithError(errStart).Error("failed to start anthropic callback forwarder")
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to start callback server"})
			return
		}
	}

	go func() {
		if isWebUI {
			defer stopCallbackForwarderInstance(anthropicCallbackPort, forwarder)
		}

		// Helper: wait for callback file
		waitFile := filepath.Join(h.cfg.AuthDir, fmt.Sprintf(".oauth-anthropic-%s.oauth", state))
		waitForFile := func(path string, timeout time.Duration) (map[string]string, error) {
			deadline := time.Now().Add(timeout)
			for {
				if !IsOAuthSessionPending(state, "anthropic") {
					return nil, errOAuthSessionNotPending
				}
				if time.Now().After(deadline) {
					SetOAuthSessionError(state, "Timeout waiting for OAuth callback")
					return nil, fmt.Errorf("timeout waiting for OAuth callback")
				}
				data, errRead := os.ReadFile(path)
				if errRead == nil {
					var m map[string]string
					_ = json.Unmarshal(data, &m)
					_ = os.Remove(path)
					return m, nil
				}
				time.Sleep(500 * time.Millisecond)
			}
		}

		fmt.Println("Waiting for authentication callback...")
		// Wait up to 5 minutes
		resultMap, errWait := waitForFile(waitFile, 5*time.Minute)
		if errWait != nil {
			if errors.Is(errWait, errOAuthSessionNotPending) {
				return
			}
			authErr := claude.NewAuthenticationError(claude.ErrCallbackTimeout, errWait)
			log.Error(claude.GetUserFriendlyMessage(authErr))
			return
		}
		if errStr := resultMap["error"]; errStr != "" {
			oauthErr := claude.NewOAuthError(errStr, "", http.StatusBadRequest)
			log.Error(claude.GetUserFriendlyMessage(oauthErr))
			SetOAuthSessionError(state, "Bad request")
			return
		}
		if resultMap["state"] != state {
			authErr := claude.NewAuthenticationError(claude.ErrInvalidState, fmt.Errorf("expected %s, got %s", state, resultMap["state"]))
			log.Error(claude.GetUserFriendlyMessage(authErr))
			SetOAuthSessionError(state, "State code error")
			return
		}

		// Parse code (Claude may append state after '#')
		rawCode := resultMap["code"]
		code := strings.Split(rawCode, "#")[0]

		// Exchange code for tokens using internal auth service
		bundle, errExchange := anthropicAuth.ExchangeCodeForTokens(ctx, code, state, pkceCodes)
		if errExchange != nil {
			authErr := claude.NewAuthenticationError(claude.ErrCodeExchangeFailed, errExchange)
			log.Errorf("Failed to exchange authorization code for tokens: %v", authErr)
			SetOAuthSessionError(state, "Failed to exchange authorization code for tokens")
			return
		}

		// Create token storage
		tokenStorage := anthropicAuth.CreateTokenStorage(bundle)
		record := &coreauth.Auth{
			ID:       fmt.Sprintf("claude-%s.json", tokenStorage.Email),
			Provider: "claude",
			FileName: fmt.Sprintf("claude-%s.json", tokenStorage.Email),
			Storage:  tokenStorage,
			Metadata: map[string]any{"email": tokenStorage.Email},
		}
		if errGuard := beginOAuthSessionSave(state, "anthropic"); errGuard != nil {
			return
		}
		savedPath, errSave := h.saveTokenRecord(ctx, record)
		if errSave != nil {
			log.Errorf("Failed to save authentication tokens: %v", errSave)
			SetOAuthSessionError(state, "Failed to save authentication tokens")
			return
		}

		fmt.Printf("Authentication successful! Token saved to %s\n", savedPath)
		if bundle.APIKey != "" {
			fmt.Println("API key obtained and saved")
		}
		fmt.Println("You can now use Claude services through this CLI")
		CompleteOAuthSession(state)
	}()

	c.JSON(200, gin.H{"status": "ok", "url": authURL, "state": state})
}

func (h *Handler) RequestCodexToken(c *gin.Context) {
	ctx := context.Background()
	ctx = PopulateAuthContext(ctx, c)

	fmt.Println("Initializing Codex authentication...")

	// Generate PKCE codes
	pkceCodes, err := codex.GeneratePKCECodes()
	if err != nil {
		log.Errorf("Failed to generate PKCE codes: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to generate PKCE codes"})
		return
	}

	// Generate random state parameter
	state, err := misc.GenerateRandomState()
	if err != nil {
		log.Errorf("Failed to generate state parameter: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to generate state parameter"})
		return
	}

	// Initialize Codex auth service
	openaiAuth := codex.NewCodexAuth(h.cfg)

	// Generate authorization URL
	authURL, err := openaiAuth.GenerateAuthURL(state, pkceCodes)
	if err != nil {
		log.Errorf("Failed to generate authorization URL: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to generate authorization url"})
		return
	}

	RegisterOAuthSession(state, "codex")

	isWebUI := isWebUIRequest(c)
	var forwarder *callbackForwarder
	if isWebUI {
		targetURL, errTarget := h.managementCallbackURL("/codex/callback")
		if errTarget != nil {
			log.WithError(errTarget).Error("failed to compute codex callback target")
			c.JSON(http.StatusInternalServerError, gin.H{"error": "callback server unavailable"})
			return
		}
		var errStart error
		if forwarder, errStart = startCallbackForwarder(codexCallbackPort, "codex", targetURL); errStart != nil {
			log.WithError(errStart).Error("failed to start codex callback forwarder")
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to start callback server"})
			return
		}
	}

	go func() {
		if isWebUI {
			defer stopCallbackForwarderInstance(codexCallbackPort, forwarder)
		}

		// Wait for callback file
		waitFile := filepath.Join(h.cfg.AuthDir, fmt.Sprintf(".oauth-codex-%s.oauth", state))
		deadline := time.Now().Add(5 * time.Minute)
		var code string
		for {
			if !IsOAuthSessionPending(state, "codex") {
				return
			}
			if time.Now().After(deadline) {
				authErr := codex.NewAuthenticationError(codex.ErrCallbackTimeout, fmt.Errorf("timeout waiting for OAuth callback"))
				log.Error(codex.GetUserFriendlyMessage(authErr))
				SetOAuthSessionError(state, "Timeout waiting for OAuth callback")
				return
			}
			if data, errR := os.ReadFile(waitFile); errR == nil {
				var m map[string]string
				_ = json.Unmarshal(data, &m)
				_ = os.Remove(waitFile)
				if errStr := m["error"]; errStr != "" {
					oauthErr := codex.NewOAuthError(errStr, "", http.StatusBadRequest)
					log.Error(codex.GetUserFriendlyMessage(oauthErr))
					SetOAuthSessionError(state, "Bad Request")
					return
				}
				if m["state"] != state {
					authErr := codex.NewAuthenticationError(codex.ErrInvalidState, fmt.Errorf("expected %s, got %s", state, m["state"]))
					SetOAuthSessionError(state, "State code error")
					log.Error(codex.GetUserFriendlyMessage(authErr))
					return
				}
				code = m["code"]
				break
			}
			time.Sleep(500 * time.Millisecond)
		}

		log.Debug("Authorization code received, exchanging for tokens...")
		// Exchange code for tokens using internal auth service
		bundle, errExchange := openaiAuth.ExchangeCodeForTokens(ctx, code, pkceCodes)
		if errExchange != nil {
			authErr := codex.NewAuthenticationError(codex.ErrCodeExchangeFailed, errExchange)
			SetOAuthSessionError(state, "Failed to exchange authorization code for tokens")
			log.Errorf("Failed to exchange authorization code for tokens: %v", authErr)
			return
		}

		// Extract additional info for filename generation
		claims, _ := codex.ParseJWTToken(bundle.TokenData.IDToken)
		planType := ""
		hashAccountID := ""
		if claims != nil {
			planType = strings.TrimSpace(claims.CodexAuthInfo.ChatgptPlanType)
			if accountID := claims.GetAccountID(); accountID != "" {
				digest := sha256.Sum256([]byte(accountID))
				hashAccountID = hex.EncodeToString(digest[:])[:8]
			}
		}

		// Create token storage and persist
		tokenStorage := openaiAuth.CreateTokenStorage(bundle)
		fileName := codex.CredentialFileName(tokenStorage.Email, planType, hashAccountID, true)
		record := &coreauth.Auth{
			ID:       fileName,
			Provider: "codex",
			FileName: fileName,
			Storage:  tokenStorage,
			Metadata: map[string]any{
				"email":      tokenStorage.Email,
				"account_id": tokenStorage.AccountID,
			},
		}
		if errGuard := beginOAuthSessionSave(state, "codex"); errGuard != nil {
			return
		}
		savedPath, errSave := h.saveTokenRecord(ctx, record)
		if errSave != nil {
			SetOAuthSessionError(state, "Failed to save authentication tokens")
			log.Errorf("Failed to save authentication tokens: %v", errSave)
			return
		}
		fmt.Printf("Authentication successful! Token saved to %s\n", savedPath)
		if bundle.APIKey != "" {
			fmt.Println("API key obtained and saved")
		}
		fmt.Println("You can now use Codex services through this CLI")
		CompleteOAuthSession(state)
	}()

	c.JSON(200, gin.H{"status": "ok", "url": authURL, "state": state})
}

func (h *Handler) RequestAntigravityToken(c *gin.Context) {
	ctx := context.Background()
	ctx = PopulateAuthContext(ctx, c)

	fmt.Println("Initializing Antigravity authentication...")

	authSvc := antigravity.NewAntigravityAuth(h.cfg, nil)

	state, errState := misc.GenerateRandomState()
	if errState != nil {
		log.Errorf("Failed to generate state parameter: %v", errState)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to generate state parameter"})
		return
	}

	redirectURI := fmt.Sprintf("http://localhost:%d/oauth-callback", antigravity.CallbackPort)
	authURL := authSvc.BuildAuthURL(state, redirectURI)

	RegisterOAuthSession(state, "antigravity")

	isWebUI := isWebUIRequest(c)
	var forwarder *callbackForwarder
	if isWebUI {
		targetURL, errTarget := h.managementCallbackURL("/antigravity/callback")
		if errTarget != nil {
			log.WithError(errTarget).Error("failed to compute antigravity callback target")
			c.JSON(http.StatusInternalServerError, gin.H{"error": "callback server unavailable"})
			return
		}
		var errStart error
		if forwarder, errStart = startCallbackForwarder(antigravity.CallbackPort, "antigravity", targetURL); errStart != nil {
			log.WithError(errStart).Error("failed to start antigravity callback forwarder")
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to start callback server"})
			return
		}
	}

	go func() {
		if isWebUI {
			defer stopCallbackForwarderInstance(antigravity.CallbackPort, forwarder)
		}

		waitFile := filepath.Join(h.cfg.AuthDir, fmt.Sprintf(".oauth-antigravity-%s.oauth", state))
		deadline := time.Now().Add(5 * time.Minute)
		var authCode string
		for {
			if !IsOAuthSessionPending(state, "antigravity") {
				return
			}
			if time.Now().After(deadline) {
				log.Error("oauth flow timed out")
				SetOAuthSessionError(state, "OAuth flow timed out")
				return
			}
			if data, errReadFile := os.ReadFile(waitFile); errReadFile == nil {
				var payload map[string]string
				_ = json.Unmarshal(data, &payload)
				_ = os.Remove(waitFile)
				if errStr := strings.TrimSpace(payload["error"]); errStr != "" {
					log.Errorf("Authentication failed: %s", errStr)
					SetOAuthSessionError(state, "Authentication failed")
					return
				}
				if payloadState := strings.TrimSpace(payload["state"]); payloadState != "" && payloadState != state {
					log.Errorf("Authentication failed: state mismatch")
					SetOAuthSessionError(state, "Authentication failed: state mismatch")
					return
				}
				authCode = strings.TrimSpace(payload["code"])
				if authCode == "" {
					log.Error("Authentication failed: code not found")
					SetOAuthSessionError(state, "Authentication failed: code not found")
					return
				}
				break
			}
			time.Sleep(500 * time.Millisecond)
		}

		tokenResp, errToken := authSvc.ExchangeCodeForTokens(ctx, authCode, redirectURI)
		if errToken != nil {
			log.Errorf("Failed to exchange token: %v", errToken)
			SetOAuthSessionError(state, "Failed to exchange token")
			return
		}

		accessToken := strings.TrimSpace(tokenResp.AccessToken)
		if accessToken == "" {
			log.Error("antigravity: token exchange returned empty access token")
			SetOAuthSessionError(state, "Failed to exchange token")
			return
		}

		email, errInfo := authSvc.FetchUserInfo(ctx, accessToken)
		if errInfo != nil {
			log.Errorf("Failed to fetch user info: %v", errInfo)
			SetOAuthSessionError(state, "Failed to fetch user info")
			return
		}
		email = strings.TrimSpace(email)
		if email == "" {
			log.Error("antigravity: user info returned empty email")
			SetOAuthSessionError(state, "Failed to fetch user info")
			return
		}

		projectID := ""
		if accessToken != "" {
			fetchedProjectID, errProject := authSvc.FetchProjectID(ctx, accessToken)
			if errProject != nil {
				log.Errorf("antigravity: failed to fetch project ID: %v", errProject)
				SetOAuthSessionError(state, "Failed to fetch project ID")
				return
			} else {
				projectID = strings.TrimSpace(fetchedProjectID)
				log.Infof("antigravity: obtained project ID %s", util.HideAPIKey(projectID))
			}
		}
		if projectID == "" {
			log.Error("antigravity: project ID discovery returned empty project")
			SetOAuthSessionError(state, "Failed to fetch project ID")
			return
		}

		now := time.Now()
		metadata := map[string]any{
			"type":          "antigravity",
			"access_token":  tokenResp.AccessToken,
			"refresh_token": tokenResp.RefreshToken,
			"expires_in":    tokenResp.ExpiresIn,
			"timestamp":     now.UnixMilli(),
			"expired":       now.Add(time.Duration(tokenResp.ExpiresIn) * time.Second).Format(time.RFC3339),
		}
		if email != "" {
			metadata["email"] = email
		}
		if projectID != "" {
			metadata["project_id"] = projectID
		}

		fileName := antigravity.CredentialFileName(email)
		label := strings.TrimSpace(email)
		if label == "" {
			label = "antigravity"
		}

		record := &coreauth.Auth{
			ID:       fileName,
			Provider: "antigravity",
			FileName: fileName,
			Label:    label,
			Metadata: metadata,
		}
		if errGuard := beginOAuthSessionSave(state, "antigravity"); errGuard != nil {
			return
		}
		savedPath, errSave := h.saveTokenRecord(ctx, record)
		if errSave != nil {
			log.Errorf("Failed to save token to file: %v", errSave)
			SetOAuthSessionError(state, "Failed to save token to file")
			return
		}

		CompleteOAuthSession(state)
		fmt.Printf("Authentication successful! Token saved to %s\n", savedPath)
		if projectID != "" {
			fmt.Printf("Using GCP project: %s\n", util.HideAPIKey(projectID))
		}
		fmt.Println("You can now use Antigravity services through this CLI")
	}()

	c.JSON(200, gin.H{"status": "ok", "url": authURL, "state": state})
}

// RequestXAIToken starts the xAI OAuth device-code flow.
func (h *Handler) RequestXAIToken(c *gin.Context) {
	ctx := PopulateAuthContext(context.Background(), c)

	fmt.Println("Initializing xAI authentication...")

	state := fmt.Sprintf("xai-%d", time.Now().UnixNano())
	authSvc := xaiauth.NewXAIAuth(h.cfg)
	deviceFlow, errStartDeviceFlow := authSvc.StartDeviceFlow(ctx)
	if errStartDeviceFlow != nil {
		log.Errorf("Failed to start xAI device flow: %v", errStartDeviceFlow)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to start device authorization flow"})
		return
	}
	authURL := strings.TrimSpace(deviceFlow.VerificationURIComplete)
	if authURL == "" {
		authURL = strings.TrimSpace(deviceFlow.VerificationURI)
	}

	RegisterOAuthSession(state, "xai")

	go func() {
		pollCtx, cancelPoll := context.WithCancel(ctx)
		defer cancelPoll()
		go watchOAuthSessionCancel(pollCtx, cancelPoll, state, "xai")

		fmt.Println("Waiting for xAI authentication...")
		bundle, errWaitForAuthorization := authSvc.WaitForAuthorization(pollCtx, deviceFlow)
		if errWaitForAuthorization != nil {
			if !IsOAuthSessionPending(state, "xai") {
				return
			}
			log.Errorf("xAI authentication failed: %v", errWaitForAuthorization)
			SetOAuthSessionError(state, oauthSessionErrorWithCause("Authentication failed", errWaitForAuthorization))
			return
		}
		if !IsOAuthSessionPending(state, "xai") {
			return
		}

		tokenStorage := authSvc.CreateTokenStorage(bundle)
		if tokenStorage == nil || strings.TrimSpace(tokenStorage.AccessToken) == "" {
			log.Error("xAI token exchange returned empty access token")
			SetOAuthSessionError(state, "Failed to exchange token")
			return
		}

		fileName := xaiauth.CredentialFileName(tokenStorage.Email, tokenStorage.Subject)
		label := strings.TrimSpace(tokenStorage.Email)
		if label == "" {
			label = "xAI"
		}

		metadata := map[string]any{
			"type":           "xai",
			"access_token":   tokenStorage.AccessToken,
			"refresh_token":  tokenStorage.RefreshToken,
			"id_token":       tokenStorage.IDToken,
			"token_type":     tokenStorage.TokenType,
			"expires_in":     tokenStorage.ExpiresIn,
			"expired":        tokenStorage.Expire,
			"last_refresh":   tokenStorage.LastRefresh,
			"base_url":       tokenStorage.BaseURL,
			"token_endpoint": tokenStorage.TokenEndpoint,
			"auth_kind":      "oauth",
			"using_api":      false,
			"websockets":     false,
		}
		if tokenStorage.Email != "" {
			metadata["email"] = tokenStorage.Email
		}
		if tokenStorage.Subject != "" {
			metadata["sub"] = tokenStorage.Subject
		}

		record := &coreauth.Auth{
			ID:       fileName,
			Provider: "xai",
			FileName: fileName,
			Label:    label,
			Storage:  tokenStorage,
			Metadata: metadata,
			Attributes: map[string]string{
				"auth_kind":  "oauth",
				"base_url":   tokenStorage.BaseURL,
				"using_api":  "false",
				"websockets": "false",
			},
		}
		if errGuard := beginOAuthSessionSave(state, "xai"); errGuard != nil {
			return
		}
		savedPath, errSave := h.saveTokenRecord(ctx, record)
		if errSave != nil {
			log.Errorf("Failed to save xAI token to file: %v", errSave)
			SetOAuthSessionError(state, "Failed to save token to file")
			return
		}

		CompleteOAuthSession(state)
		fmt.Printf("Authentication successful! Token saved to %s\n", savedPath)
		fmt.Println("You can now use xAI services through this CLI")
	}()

	response := gin.H{"status": "ok", "url": authURL, "state": state, "flow": "device"}
	if userCode := strings.TrimSpace(deviceFlow.UserCode); userCode != "" {
		response["user_code"] = userCode
	}
	if deviceFlow.ExpiresIn > 0 {
		response["expires_in"] = deviceFlow.ExpiresIn
	} else {
		response["expires_in"] = int(xaiauth.MaxPollDuration / time.Second)
	}
	c.JSON(http.StatusOK, response)
}

func (h *Handler) RequestKimiToken(c *gin.Context) {
	ctx := context.Background()
	ctx = PopulateAuthContext(ctx, c)

	fmt.Println("Initializing Kimi authentication...")

	state := fmt.Sprintf("kmi-%d", time.Now().UnixNano())
	// Initialize Kimi auth service
	kimiAuth := kimi.NewKimiAuth(h.cfg)

	// Generate authorization URL
	deviceFlow, errStartDeviceFlow := kimiAuth.StartDeviceFlow(ctx)
	if errStartDeviceFlow != nil {
		log.Errorf("Failed to generate authorization URL: %v", errStartDeviceFlow)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to generate authorization url"})
		return
	}
	authURL := deviceFlow.VerificationURIComplete
	if authURL == "" {
		authURL = deviceFlow.VerificationURI
	}

	RegisterOAuthSession(state, "kimi")

	go func() {
		fmt.Println("Waiting for authentication...")
		authBundle, errWaitForAuthorization := kimiAuth.WaitForAuthorization(ctx, deviceFlow)
		if errWaitForAuthorization != nil {
			SetOAuthSessionError(state, "Authentication failed")
			fmt.Printf("Authentication failed: %v\n", errWaitForAuthorization)
			return
		}

		// Create token storage
		tokenStorage := kimiAuth.CreateTokenStorage(authBundle)

		metadata := map[string]any{
			"type":          "kimi",
			"access_token":  authBundle.TokenData.AccessToken,
			"refresh_token": authBundle.TokenData.RefreshToken,
			"token_type":    authBundle.TokenData.TokenType,
			"scope":         authBundle.TokenData.Scope,
			"timestamp":     time.Now().UnixMilli(),
		}
		if authBundle.TokenData.ExpiresAt > 0 {
			expired := time.Unix(authBundle.TokenData.ExpiresAt, 0).UTC().Format(time.RFC3339)
			metadata["expired"] = expired
		}
		if strings.TrimSpace(authBundle.DeviceID) != "" {
			metadata["device_id"] = strings.TrimSpace(authBundle.DeviceID)
		}

		fileName := fmt.Sprintf("kimi-%d.json", time.Now().UnixMilli())
		record := &coreauth.Auth{
			ID:       fileName,
			Provider: "kimi",
			FileName: fileName,
			Label:    "Kimi User",
			Storage:  tokenStorage,
			Metadata: metadata,
		}
		if errGuard := beginOAuthSessionSave(state, "kimi"); errGuard != nil {
			return
		}
		savedPath, errSave := h.saveTokenRecord(ctx, record)
		if errSave != nil {
			log.Errorf("Failed to save authentication tokens: %v", errSave)
			SetOAuthSessionError(state, "Failed to save authentication tokens")
			return
		}

		fmt.Printf("Authentication successful! Token saved to %s\n", savedPath)
		fmt.Println("You can now use Kimi services through this CLI")
		CompleteOAuthSession(state)
	}()

	c.JSON(200, gin.H{"status": "ok", "url": authURL, "state": state})
}

func watchOAuthSessionCancel(pollCtx context.Context, cancel context.CancelFunc, state, provider string) {
	if cancel == nil {
		return
	}
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-pollCtx.Done():
			return
		case <-ticker.C:
			if !IsOAuthSessionPending(state, provider) {
				cancel()
				return
			}
		}
	}
}

// CancelAuthSession cancels a pending OAuth session identified by state.
func (h *Handler) CancelAuthSession(c *gin.Context) {
	state := strings.TrimSpace(c.Query("state"))
	if state == "" {
		c.JSON(http.StatusBadRequest, gin.H{"status": "error", "error": "missing state"})
		return
	}
	if err := ValidateOAuthState(state); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"status": "error", "error": "invalid state"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "ok", "cancelled": CancelOAuthSession(state)})
}

func (h *Handler) GetAuthStatus(c *gin.Context) {
	state := strings.TrimSpace(c.Query("state"))
	if state == "" {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
		return
	}
	if err := ValidateOAuthState(state); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"status": "error", "error": "invalid state"})
		return
	}

	_, status, ok := GetOAuthSession(state)
	if !ok {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
		return
	}
	if status != "" {
		c.JSON(http.StatusOK, gin.H{"status": "error", "error": status})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "wait"})
}

// PopulateAuthContext extracts request info and adds it to the context
func PopulateAuthContext(ctx context.Context, c *gin.Context) context.Context {
	info := &coreauth.RequestInfo{
		Query:   c.Request.URL.Query(),
		Headers: c.Request.Header,
	}
	return coreauth.WithRequestInfo(ctx, info)
}
