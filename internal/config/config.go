// Package config provides configuration management for the CLI Proxy API server.
// It handles loading and parsing YAML configuration files, and provides structured
// access to application settings including server port, authentication directory,
// debug settings, proxy configuration, and API keys.
package config

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"reflect"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/proxyutil"
	log "github.com/sirupsen/logrus"
	"golang.org/x/crypto/bcrypt"
	"gopkg.in/yaml.v3"
)

const (
	DefaultPanelGitHubRepository        = "https://github.com/RoodraNambisa/Cli-Proxy-API-Management-Center"
	DefaultPprofAddr                    = "127.0.0.1:8316"
	DefaultCodexFingerprintMode         = "device"
	DefaultCodexSessionIdentityPoolSize = 1
	MaxCodexSessionIdentityPoolSize     = 64

	DefaultRequestBodyAuditStatusCode = http.StatusBadRequest
	DefaultRequestBodyAuditMessage    = "Request body was rejected by policy."
	DefaultRequestBodyAuditType       = "invalid_request_error"
	DefaultRequestBodyAuditCode       = "request_body_blocked"

	DisabledImageGenerationToolActionRemove = "remove"
	DisabledImageGenerationToolActionError  = "error"

	DefaultDisabledImageGenerationToolStatusCode = http.StatusBadRequest
	DefaultDisabledImageGenerationToolMessage    = "image_generation tool is disabled for this credential"
	DefaultDisabledImageGenerationToolType       = "image_generation_disabled"
	DefaultDisabledImageGenerationToolCode       = "image_generation_disabled"

	DefaultChatGPTWebSentinelSDKQueueSize      = 32
	DefaultChatGPTWebSentinelSDKCacheVersions  = 3
	RecommendedChatGPTWebSentinelSDKWorkers    = 16
	MaxChatGPTWebSentinelSDKQueueSize          = 1024
	MaxChatGPTWebSentinelSDKCacheVersions      = 5
	DefaultChatGPTWebUsageCacheThresholdMB     = 1
	DefaultChatGPTWebUsageCacheMaxDiskSizeMB   = 1024
	DefaultChatGPTWebUsageCacheMinAvailableMB  = 1024
	DefaultChatGPTWebUsageCacheMaxUsedPercent  = 95
	MaxChatGPTWebUsageCacheOrphanRetention     = 10080
	MaxChatGPTWebUsageCacheMegabytes           = (1<<63 - 1) >> 20
	DefaultChatGPTWebAutoOutputQuality         = "medium"
	DefaultChatGPTWebFallbackOutputImageTokens = 2000
	DefaultChatGPTWebImageUpstreamModel        = "gpt-5-5"
	DefaultChatGPTWebAutoReloginMaxRetries     = 3
	DefaultChatGPTWebAutoReloginJitterPercent  = 20
	DefaultChatGPTWebAutoReloginWorkers        = 4
	DefaultChatGPTWebAutoReloginQueueSize      = 4096
	DefaultChatGPTWebManualReloginConcurrency  = 4
	MaxChatGPTWebAutoReloginRetries            = 10
	MaxChatGPTWebAutoReloginJitterPercent      = 100
	MaxChatGPTWebAutoReloginWorkers            = 256
	MaxChatGPTWebAutoReloginQueueSize          = 1_000_000
	MinChatGPTWebManualReloginConcurrency      = 1
	// Deprecated: this advisory alias is not a validation limit. Use RecommendedMaxChatGPTWebManualReloginConcurrency.
	MaxChatGPTWebManualReloginConcurrency      = RecommendedMaxChatGPTWebManualReloginConcurrency
	DefaultChatGPTWebImportWorkers             = 4
	MaxChatGPTWebImportWorkers                 = 32
	DefaultChatGPTWebTimezone                  = "Asia/Shanghai"
	DefaultChatGPTWebTimezoneOffsetMinutes     = -480
	MinChatGPTWebTimezoneOffsetMinutes         = -840
	MaxChatGPTWebTimezoneOffsetMinutes         = 840
	DefaultChatGPTWebAccountInfoWorkers        = 4
	DefaultChatGPTWebAccountInfoQueueSize      = 256
	DefaultChatGPTWebAccountInfoTTLMinutes     = 15
	DefaultChatGPTWebAccountInfoPeriodMinutes  = 0
	DefaultChatGPTWebAccountInfoJitterSeconds  = 30
	DefaultChatGPTWebAccountInfoMaxRetries     = 3
	MaxChatGPTWebAccountInfoWorkers            = 32
	MaxChatGPTWebAccountInfoQueueSize          = 10000
	MaxChatGPTWebAccountInfoTTLMinutes         = 1440
	MaxChatGPTWebAccountInfoPeriodMinutes      = 10080
	MaxChatGPTWebAccountInfoJitterSeconds      = 300
	MaxChatGPTWebAccountInfoRetries            = 10
	DefaultChatGPTWebLoginProxyRequestAttempts = 3
	DefaultChatGPTWebLoginProxyFlowAttempts    = 2
	DefaultChatGPTWebLoginProxyRetryDelayMS    = 800
	DefaultChatGPTWebLoginProxyTimeoutSeconds  = 90
	MinChatGPTWebLoginProxyRequestAttempts     = 1
	MaxChatGPTWebLoginProxyRequestAttempts     = 10
	MinChatGPTWebLoginProxyFlowAttempts        = 1
	MaxChatGPTWebLoginProxyFlowAttempts        = 5
	MinChatGPTWebLoginProxyRetryDelayMS        = 0
	MaxChatGPTWebLoginProxyRetryDelayMS        = 10000
	MinChatGPTWebLoginProxyTimeoutSeconds      = 30
	MaxChatGPTWebLoginProxyTimeoutSeconds      = 600
)

const RecommendedMaxChatGPTWebManualReloginConcurrency = 512

var (
	deprecatedAmpConfigWarning       sync.Once
	deprecatedGeminiCLIConfigWarning sync.Once
	chatGPTWebTimezoneLocations      sync.Map
)

func warnDeprecatedAmpConfig(data []byte) {
	warnDeprecatedAmpConfigOnce(data, &deprecatedAmpConfigWarning, func() {
		log.Warn("amp integration has been removed; ampcode configuration is ignored; remove or rotate any retired Amp credentials")
	})
}

func warnDeprecatedGeminiCLIConfig(data []byte) {
	warnDeprecatedGeminiCLIConfigOnce(data, &deprecatedGeminiCLIConfigWarning, func() {
		log.Warn("Gemini CLI integration has been removed; legacy endpoint, model alias, and excluded-model configuration is ignored")
	})
}

func warnDeprecatedGeminiCLIConfigOnce(data []byte, once *sync.Once, warn func()) {
	if !containsDeprecatedGeminiCLIConfig(data) {
		return
	}
	once.Do(warn)
}

func containsDeprecatedGeminiCLIConfig(data []byte) bool {
	var document yaml.Node
	if err := yaml.Unmarshal(data, &document); err != nil || len(document.Content) == 0 {
		return false
	}
	return yamlConfigMappingContainsDeprecatedGeminiCLI(document.Content[0], make(map[*yaml.Node]bool))
}

func yamlConfigMappingContainsDeprecatedGeminiCLI(node *yaml.Node, visited map[*yaml.Node]bool) bool {
	node = yamlAliasTarget(node)
	if node == nil || node.Kind != yaml.MappingNode || visited[node] {
		return false
	}
	visited[node] = true
	for index := 0; index+1 < len(node.Content); index += 2 {
		key := strings.ToLower(strings.TrimSpace(node.Content[index].Value))
		value := node.Content[index+1]
		switch key {
		case "enable-gemini-cli-endpoint":
			return true
		case "oauth-model-alias", "oauth-excluded-models":
			if yamlMappingContainsKey(value, "gemini-cli", make(map[*yaml.Node]bool)) {
				return true
			}
		case "<<":
			if yamlMergedConfigContainsDeprecatedGeminiCLI(value, visited) {
				return true
			}
		}
	}
	return false
}

func yamlMergedConfigContainsDeprecatedGeminiCLI(node *yaml.Node, visited map[*yaml.Node]bool) bool {
	node = yamlAliasTarget(node)
	if node == nil {
		return false
	}
	if node.Kind == yaml.SequenceNode {
		if visited[node] {
			return false
		}
		visited[node] = true
		for _, item := range node.Content {
			if yamlMergedConfigContainsDeprecatedGeminiCLI(item, visited) {
				return true
			}
		}
		return false
	}
	return yamlConfigMappingContainsDeprecatedGeminiCLI(node, visited)
}

func yamlMappingContainsKey(node *yaml.Node, target string, visited map[*yaml.Node]bool) bool {
	node = yamlAliasTarget(node)
	if node == nil || node.Kind != yaml.MappingNode || visited[node] {
		return false
	}
	visited[node] = true
	for index := 0; index+1 < len(node.Content); index += 2 {
		key := strings.ToLower(strings.TrimSpace(node.Content[index].Value))
		if key == target {
			return true
		}
		if key == "<<" && yamlMergedMappingContainsKey(node.Content[index+1], target, visited) {
			return true
		}
	}
	return false
}

func yamlMergedMappingContainsKey(node *yaml.Node, target string, visited map[*yaml.Node]bool) bool {
	node = yamlAliasTarget(node)
	if node == nil {
		return false
	}
	if node.Kind == yaml.SequenceNode {
		if visited[node] {
			return false
		}
		visited[node] = true
		for _, item := range node.Content {
			if yamlMergedMappingContainsKey(item, target, visited) {
				return true
			}
		}
		return false
	}
	return yamlMappingContainsKey(node, target, visited)
}

func yamlAliasTarget(node *yaml.Node) *yaml.Node {
	for node != nil && node.Kind == yaml.AliasNode && node.Alias != nil {
		node = node.Alias
	}
	return node
}

func warnDeprecatedAmpConfigOnce(data []byte, once *sync.Once, warn func()) {
	if !containsDeprecatedAmpConfig(data) {
		return
	}
	once.Do(warn)
}

func containsDeprecatedAmpConfig(data []byte) bool {
	var document yaml.Node
	if err := yaml.Unmarshal(data, &document); err != nil || len(document.Content) == 0 {
		return false
	}
	return mappingContainsDeprecatedAmpConfig(document.Content[0], make(map[*yaml.Node]struct{}))
}

func mappingContainsDeprecatedAmpConfig(node *yaml.Node, visited map[*yaml.Node]struct{}) bool {
	if node == nil {
		return false
	}
	if node.Kind == yaml.AliasNode {
		node = node.Alias
	}
	if node == nil || node.Kind != yaml.MappingNode {
		return false
	}
	if _, ok := visited[node]; ok {
		return false
	}
	visited[node] = struct{}{}
	for i := 0; i+1 < len(node.Content); i += 2 {
		key := strings.TrimSpace(node.Content[i].Value)
		switch key {
		case "ampcode", "amp-upstream-url", "amp-upstream-api-key", "amp-restrict-management-to-localhost", "amp-model-mappings":
			return true
		}
		if key == "<<" && mergedConfigContainsDeprecatedAmp(node.Content[i+1], visited) {
			return true
		}
	}
	return false
}

func mergedConfigContainsDeprecatedAmp(node *yaml.Node, visited map[*yaml.Node]struct{}) bool {
	if node == nil {
		return false
	}
	if node.Kind == yaml.SequenceNode {
		for _, item := range node.Content {
			if mappingContainsDeprecatedAmpConfig(item, visited) {
				return true
			}
		}
		return false
	}
	return mappingContainsDeprecatedAmpConfig(node, visited)
}

// Config represents the application's configuration, loaded from a YAML file.
type Config struct {
	SDKConfig `yaml:",inline"`
	// Host is the network host/interface on which the API server will bind.
	// Default is empty ("") to bind all interfaces (IPv4 + IPv6). Use "127.0.0.1" or "localhost" for local-only access.
	Host string `yaml:"host" json:"-"`
	// Port is the network port on which the API server will listen.
	Port int `yaml:"port" json:"-"`

	// TLS config controls HTTPS server settings.
	TLS TLSConfig `yaml:"tls" json:"tls"`

	// RemoteManagement nests management-related options under 'remote-management'.
	RemoteManagement RemoteManagement `yaml:"remote-management" json:"-"`

	// AuthDir is the directory where authentication token files are stored.
	AuthDir string `yaml:"auth-dir" json:"-"`

	// Debug enables or disables debug-level logging and other debug features.
	Debug bool `yaml:"debug" json:"debug"`

	// Pprof config controls the optional pprof HTTP debug server.
	Pprof PprofConfig `yaml:"pprof" json:"pprof"`

	// CommercialMode disables high-overhead HTTP middleware features to minimize per-request memory usage.
	CommercialMode bool `yaml:"commercial-mode" json:"commercial-mode"`

	// LoggingToFile controls whether application logs are written to rotating files or stdout.
	LoggingToFile bool `yaml:"logging-to-file" json:"logging-to-file"`

	// LogsMaxTotalSizeMB limits the total size (in MB) of log files under the logs directory.
	// When exceeded, the oldest log files are deleted until within the limit. Set to 0 to disable.
	LogsMaxTotalSizeMB int `yaml:"logs-max-total-size-mb" json:"logs-max-total-size-mb"`

	// LogsRetentionDays removes inactive log files older than the configured number of days.
	// Set to 0 to disable age-based cleanup.
	LogsRetentionDays int `yaml:"logs-retention-days" json:"logs-retention-days"`

	// ErrorLogsMaxFiles limits the number of error log files retained when request logging is disabled.
	// When exceeded, the oldest error log files are deleted. Default is 10. Set to 0 to disable cleanup.
	ErrorLogsMaxFiles int `yaml:"error-logs-max-files" json:"error-logs-max-files"`

	// UsageStatisticsEnabled toggles in-memory usage aggregation; when false, usage data is discarded.
	UsageStatisticsEnabled bool `yaml:"usage-statistics-enabled" json:"usage-statistics-enabled"`

	// UsageStatisticsPersistenceEnabled controls automatic snapshot restore and writes when
	// in-memory collection is enabled. Missing configuration keeps the legacy enabled behavior.
	UsageStatisticsPersistenceEnabled *bool `yaml:"usage-statistics-persistence-enabled,omitempty" json:"usage-statistics-persistence-enabled,omitempty"`

	// UsageStatisticsPersistIntervalSeconds controls how often usage statistics
	// are flushed to disk automatically. Set to 0 for restore plus shutdown-only persistence.
	UsageStatisticsPersistIntervalSeconds int `yaml:"usage-statistics-persist-interval-seconds" json:"usage-statistics-persist-interval-seconds"`

	// UsageStatisticsDetailRetentionDays removes detail rows older than this many days before
	// persistence. Set to 0 to retain details without a time limit.
	UsageStatisticsDetailRetentionDays int `yaml:"usage-statistics-detail-retention-days" json:"usage-statistics-detail-retention-days"`

	// UsageStatisticsMaxStorageMB limits the automatic snapshot size by removing the oldest
	// detail rows before persistence. Set to 0 to disable the size limit.
	UsageStatisticsMaxStorageMB int `yaml:"usage-statistics-max-storage-megabytes" json:"usage-statistics-max-storage-megabytes"`

	// UsagePricing defines shared per-model prices for usage cost aggregation.
	UsagePricing UsagePricingConfig `yaml:"usage-pricing" json:"usage-pricing"`

	// DisableCooling disables quota cooldown scheduling when true.
	DisableCooling bool `yaml:"disable-cooling" json:"disable-cooling"`

	// NoCooldownStatusCodes lists HTTP status codes that should record failures without scheduling auth/model cooldowns.
	NoCooldownStatusCodes []int `yaml:"no-cooldown-status-codes" json:"no-cooldown-status-codes"`

	// FixedErrorCooldowns overrides auth/model cooldown duration for matching status and error text.
	FixedErrorCooldowns []FixedErrorCooldownRule `yaml:"fixed-error-cooldowns" json:"fixed-error-cooldowns"`

	// NonRetryableErrors marks stable request errors that should not trigger credential or request-round retry.
	NonRetryableErrors []NonRetryableErrorRule `yaml:"non-retryable-errors" json:"non-retryable-errors"`

	// AuthModelExclusions removes models from matching credentials before registry registration.
	AuthModelExclusions []AuthModelExclusionRule `yaml:"auth-model-exclusions" json:"auth-model-exclusions"`

	// DisabledImageGenerationToolAction controls text requests that carry an image_generation tool for credentials with image generation disabled.
	DisabledImageGenerationToolAction string `yaml:"disabled-image-generation-tool-action" json:"disabled-image-generation-tool-action"`

	// DisabledImageGenerationToolFallback routes image_generation tool requests to image-capable Codex credentials before applying the action.
	DisabledImageGenerationToolFallback bool `yaml:"disabled-image-generation-tool-fallback" json:"disabled-image-generation-tool-fallback"`

	// DisabledImageGenerationToolError defines the response returned when the disabled image_generation tool action is error.
	DisabledImageGenerationToolError DisabledImageGenerationToolErrorConfig `yaml:"disabled-image-generation-tool-error" json:"disabled-image-generation-tool-error"`

	// RequestBodyAudit blocks API requests whose raw body contains configured byte keywords.
	RequestBodyAudit RequestBodyAuditConfig `yaml:"request-body-audit" json:"request-body-audit"`

	// AuthAutoRefreshWorkers overrides the size of the core auth auto-refresh worker pool.
	// When <= 0, the default worker count is used.
	AuthAutoRefreshWorkers int `yaml:"auth-auto-refresh-workers" json:"auth-auto-refresh-workers"`

	// RequestRetry defines additional request rounds after a retryable execution failure.
	RequestRetry int `yaml:"request-retry" json:"request-retry"`
	// MaxRetryCredentials defines the maximum number of credentials to try in each request round.
	// Set to 0 or a negative value to try all available credentials in the target priority.
	MaxRetryCredentials int `yaml:"max-retry-credentials" json:"max-retry-credentials"`
	// MaxRetryInterval defines the maximum wait time in seconds for target-priority cooldown recovery.
	MaxRetryInterval int `yaml:"max-retry-interval" json:"max-retry-interval"`

	// QuotaExceeded defines the behavior when a quota is exceeded.
	QuotaExceeded QuotaExceeded `yaml:"quota-exceeded" json:"quota-exceeded"`

	// AuthMaintenance controls optional background handling of broken or exhausted auth files.
	AuthMaintenance AuthMaintenanceConfig `yaml:"auth-maintenance" json:"auth-maintenance"`

	// Routing controls credential selection behavior.
	Routing RoutingConfig `yaml:"routing" json:"routing"`

	// WebsocketAuth enables or disables authentication for the WebSocket API.
	WebsocketAuth bool `yaml:"ws-auth" json:"ws-auth"`

	// AntigravitySignatureCacheEnabled controls whether signature cache validation is enabled for thinking blocks.
	// When true (default), cached signatures are preferred and validated.
	// When false, client signatures are used directly after normalization (bypass mode).
	AntigravitySignatureCacheEnabled *bool `yaml:"antigravity-signature-cache-enabled,omitempty" json:"antigravity-signature-cache-enabled,omitempty"`

	AntigravitySignatureBypassStrict *bool `yaml:"antigravity-signature-bypass-strict,omitempty" json:"antigravity-signature-bypass-strict,omitempty"`

	// GeminiKey defines Gemini API key configurations with optional routing overrides.
	GeminiKey []GeminiKey `yaml:"gemini-api-key" json:"gemini-api-key"`

	// InteractionsKey defines native Google Interactions API key configurations.
	InteractionsKey []GeminiKey `yaml:"interactions-api-key" json:"interactions-api-key"`

	// Codex defines a list of Codex API key configurations as specified in the YAML configuration file.
	CodexKey []CodexKey `yaml:"codex-api-key" json:"codex-api-key"`

	// Codex configures provider-wide Codex request behavior.
	Codex CodexConfig `yaml:"codex" json:"codex"`

	// ChatGPTWeb configures provider-wide ChatGPT Web credential behavior.
	ChatGPTWeb ChatGPTWebConfig `yaml:"chatgpt-web" json:"chatgpt-web"`

	// CodexHeaderDefaults configures canonical software identity and fallback
	// feature headers for Codex OAuth model requests.
	CodexHeaderDefaults CodexHeaderDefaults `yaml:"codex-header-defaults" json:"codex-header-defaults"`

	// CodexFingerprint controls optional browser-style request fingerprinting for Codex upstreams.
	CodexFingerprint CodexFingerprintConfig `yaml:"codex-fingerprint" json:"codex-fingerprint"`

	// CodexCustomModels defines additional Codex OAuth/file-backed models.
	CodexCustomModels []CodexCustomModel `yaml:"codex-custom-models,omitempty" json:"codex-custom-models,omitempty"`

	// ClaudeKey defines a list of Claude API key configurations as specified in the YAML configuration file.
	ClaudeKey []ClaudeKey `yaml:"claude-api-key" json:"claude-api-key"`

	// ClaudeHeaderDefaults configures default header values for Claude API requests.
	// These are used as fallbacks when the client does not send its own headers.
	ClaudeHeaderDefaults ClaudeHeaderDefaults `yaml:"claude-header-defaults" json:"claude-header-defaults"`

	// OpenAICompatibility defines OpenAI API compatibility configurations for external providers.
	OpenAICompatibility []OpenAICompatibility `yaml:"openai-compatibility" json:"openai-compatibility"`

	// VertexCompatAPIKey defines Vertex AI-compatible API key configurations for third-party providers.
	// Used for services that use Vertex AI-style paths but with simple API key authentication.
	VertexCompatAPIKey []VertexCompatKey `yaml:"vertex-api-key" json:"vertex-api-key"`

	// OAuthExcludedModels defines per-provider global model exclusions applied to OAuth/file-backed auth entries.
	OAuthExcludedModels map[string][]string `yaml:"oauth-excluded-models,omitempty" json:"oauth-excluded-models,omitempty"`

	// OAuthModelAlias defines global model name aliases for OAuth/file-backed auth channels.
	// These aliases affect both model listing and model routing for supported channels:
	// vertex, aistudio, antigravity, claude, codex, kimi, xai.
	//
	// NOTE: This does not apply to existing per-credential model alias features under:
	// gemini-api-key, interactions-api-key, codex-api-key, claude-api-key, openai-compatibility, and vertex-api-key.
	OAuthModelAlias map[string][]OAuthModelAlias `yaml:"oauth-model-alias,omitempty" json:"oauth-model-alias,omitempty"`

	// Payload defines default and override rules for provider payload parameters.
	Payload PayloadConfig `yaml:"payload" json:"payload"`

	legacyMigrationPending bool `yaml:"-" json:"-"`
}

// RuntimeApplyResult describes whether a persisted configuration was fully
// applied to the running process. RestartFields contains settings that were
// saved but intentionally left unchanged until the next process start.
type RuntimeApplyResult struct {
	RestartRequired bool     `json:"restart_required"`
	RestartFields   []string `json:"restart_fields,omitempty"`
	Applied         bool     `json:"applied"`
	Deduplicated    bool     `json:"deduplicated,omitempty"`
}

// Clone creates an independent configuration snapshot. Configuration holders
// use this before crossing subsystem boundaries so management edits cannot
// mutate a live runtime configuration through a shared pointer.
func Clone(input *Config) (*Config, error) {
	if input == nil {
		return nil, nil
	}
	// Use YAML rather than JSON because startup and management-only fields such
	// as host, port, auth-dir and remote-management are intentionally excluded
	// from JSON responses but still belong to a complete runtime snapshot.
	data, errMarshal := yaml.Marshal(input)
	if errMarshal != nil {
		return nil, fmt.Errorf("clone config: marshal: %w", errMarshal)
	}
	var snapshot Config
	if errDecode := yaml.Unmarshal(data, &snapshot); errDecode != nil {
		return nil, fmt.Errorf("clone config: decode: %w", errDecode)
	}
	snapshot.legacyMigrationPending = input.legacyMigrationPending
	clonePayloadRawValues(snapshot.Payload.DefaultRaw, input.Payload.DefaultRaw)
	clonePayloadRawValues(snapshot.Payload.OverrideRaw, input.Payload.OverrideRaw)
	return &snapshot, nil
}

func clonePayloadRawValues(snapshot, source []PayloadRule) {
	for ruleIndex := range source {
		if ruleIndex >= len(snapshot) || snapshot[ruleIndex].Params == nil {
			continue
		}
		for key, value := range source[ruleIndex].Params {
			switch typed := value.(type) {
			case json.RawMessage:
				snapshot[ruleIndex].Params[key] = append(json.RawMessage(nil), typed...)
			case []byte:
				snapshot[ruleIndex].Params[key] = append([]byte(nil), typed...)
			}
		}
	}
}

// ClaudeHeaderDefaults configures default header values injected into Claude API requests.
// In legacy mode, UserAgent/PackageVersion/RuntimeVersion/Timeout act as fallbacks when
// the client omits them, while OS/Arch remain runtime-derived. When stabilized device
// profiles are enabled, OS/Arch become the pinned platform baseline, while
// UserAgent/PackageVersion/RuntimeVersion seed the upgradeable software fingerprint.
type ClaudeHeaderDefaults struct {
	UserAgent              string `yaml:"user-agent" json:"user-agent"`
	PackageVersion         string `yaml:"package-version" json:"package-version"`
	RuntimeVersion         string `yaml:"runtime-version" json:"runtime-version"`
	OS                     string `yaml:"os" json:"os"`
	Arch                   string `yaml:"arch" json:"arch"`
	Timeout                string `yaml:"timeout" json:"timeout"`
	StabilizeDeviceProfile *bool  `yaml:"stabilize-device-profile,omitempty" json:"stabilize-device-profile,omitempty"`
}

// CodexHeaderDefaults configures header values injected into Codex model
// requests for OAuth/file-backed auth. With software identity enforcement,
// UserAgent is canonical; BetaFeatures remains a client fallback.
type CodexHeaderDefaults struct {
	UserAgent    string `yaml:"user-agent" json:"user-agent"`
	Originator   string `yaml:"originator" json:"originator"`
	BetaFeatures string `yaml:"beta-features" json:"beta-features"`
}

// CodexConfig configures provider-wide Codex request behavior.
type CodexConfig struct {
	IdentityConfuse         bool                 `yaml:"identity-confuse" json:"identity-confuse"`
	SpoofSessionIdentity    bool                 `yaml:"spoof-session-identity" json:"spoof-session-identity"`
	TurnStatePolicy         CodexTurnStatePolicy `yaml:"turn-state-policy" json:"turn-state-policy"`
	EnforceSoftwareIdentity *bool                `yaml:"enforce-software-identity,omitempty" json:"enforce-software-identity,omitempty"`
}

// CodexTurnStatePolicy controls how client turn-state headers are forwarded to Codex OAuth upstreams.
type CodexTurnStatePolicy string

const (
	CodexTurnStatePolicyPassthrough       CodexTurnStatePolicy = "passthrough"
	CodexTurnStatePolicyGuardCrossAccount CodexTurnStatePolicy = "guard-cross-account"
	CodexTurnStatePolicySameAccountOnly   CodexTurnStatePolicy = "same-account-only"
	CodexTurnStatePolicyStrip             CodexTurnStatePolicy = "strip"
)

// NormalizeCodexTurnStatePolicy validates and canonicalizes the configured policy.
func NormalizeCodexTurnStatePolicy(value CodexTurnStatePolicy) (CodexTurnStatePolicy, error) {
	normalized := CodexTurnStatePolicy(strings.ToLower(strings.TrimSpace(string(value))))
	if normalized == "" {
		return CodexTurnStatePolicyGuardCrossAccount, nil
	}
	switch normalized {
	case CodexTurnStatePolicyPassthrough,
		CodexTurnStatePolicyGuardCrossAccount,
		CodexTurnStatePolicySameAccountOnly,
		CodexTurnStatePolicyStrip:
		return normalized, nil
	default:
		return "", fmt.Errorf("codex.turn-state-policy must be passthrough, guard-cross-account, same-account-only, or strip")
	}
}

// ResolvedTurnStatePolicy returns the effective policy for runtime-created configurations.
func (c CodexConfig) ResolvedTurnStatePolicy() CodexTurnStatePolicy {
	policy, err := NormalizeCodexTurnStatePolicy(c.TurnStatePolicy)
	if err != nil {
		return CodexTurnStatePolicyGuardCrossAccount
	}
	return policy
}

// ResolvedEnforceSoftwareIdentity returns whether Codex OAuth requests use a
// normalized User-Agent, Originator, and Version tuple. The default is enabled.
func (c CodexConfig) ResolvedEnforceSoftwareIdentity() bool {
	return c.EnforceSoftwareIdentity == nil || *c.EnforceSoftwareIdentity
}

// ChatGPTWebConfig configures ChatGPT Web credential, usage, and Sentinel behavior.
type ChatGPTWebConfig struct {
	// AutoRelogin starts a background password login after a terminal refresh failure.
	AutoRelogin bool `yaml:"auto-relogin" json:"auto-relogin"`
	// SessionCookieRefreshOnTokenFailure allows incomplete password credentials to refresh access tokens with a complete session cookie.
	SessionCookieRefreshOnTokenFailure bool `yaml:"session-cookie-refresh-on-token-failure" json:"session-cookie-refresh-on-token-failure"`
	// ForceSessionRefreshOnImport queues a background refresh for session-backed imports even when their access token is still valid.
	// An omitted value remains enabled for backward compatibility.
	ForceSessionRefreshOnImport *bool `yaml:"force-session-refresh-on-import,omitempty" json:"force-session-refresh-on-import,omitempty"`
	// Import controls the shared local import pool and optional post-upload maintenance.
	Import ChatGPTWebImportConfig `yaml:"import,omitempty" json:"import,omitempty"`
	// AutoDeleteDeadAuths removes credentials whose lifecycle is permanently dead.
	AutoDeleteDeadAuths bool `yaml:"auto-delete-dead-auths" json:"auto-delete-dead-auths"`
	// InvalidPasskeyResponseAsDead allows exhausted Passkey-only recovery to mark the credential dead.
	InvalidPasskeyResponseAsDead bool `yaml:"invalid-passkey-response-as-dead" json:"invalid-passkey-response-as-dead"`
	// API798AutoLoginEnabled allows legacy auto credentials with no Passkey or password to use an API798 mailbox URL.
	API798AutoLoginEnabled bool `yaml:"api798-auto-login-enabled" json:"api798-auto-login-enabled"`
	// AutoDeleteDeadPriorities limits dead credential deletion to these priorities.
	// An empty list matches every priority.
	AutoDeleteDeadPriorities []int `yaml:"auto-delete-dead-priorities,omitempty" json:"auto-delete-dead-priorities,omitempty"`
	// AutoReloginMaxRetries limits retries after the initial background login attempt.
	AutoReloginMaxRetries *int `yaml:"auto-relogin-max-retries,omitempty" json:"auto-relogin-max-retries,omitempty"`
	// AutoReloginJitterPercent applies symmetric jitter to background retry delays.
	AutoReloginJitterPercent *int `yaml:"auto-relogin-jitter-percent,omitempty" json:"auto-relogin-jitter-percent,omitempty"`
	// AutoReloginWorkers controls concurrent background re-login attempts.
	AutoReloginWorkers *int `yaml:"auto-relogin-workers,omitempty" json:"auto-relogin-workers,omitempty"`
	// AutoReloginQueueSize bounds queued and running background re-login generations.
	AutoReloginQueueSize *int `yaml:"auto-relogin-queue-size,omitempty" json:"auto-relogin-queue-size,omitempty"`
	// ManualReloginConcurrency bounds management re-login operations without adding a server-side wait queue.
	ManualReloginConcurrency *int `yaml:"manual-relogin-concurrency,omitempty" json:"manual-relogin-concurrency,omitempty"`
	// Timezone is the browser IANA timezone sent to ChatGPT Web.
	Timezone string `yaml:"timezone,omitempty" json:"timezone,omitempty"`
	// TimezoneOffsetMinutes overrides the browser offset. Omitted values follow Timezone and DST.
	TimezoneOffsetMinutes *int `yaml:"timezone-offset-minutes,omitempty" json:"timezone-offset-minutes,omitempty"`
	// EstimateTokenUsage controls local tiktoken usage estimation for Web responses.
	// An omitted value remains enabled for backward compatibility.
	EstimateTokenUsage *bool                       `yaml:"estimate-token-usage,omitempty" json:"estimate-token-usage,omitempty"`
	UsageCache         ChatGPTWebUsageCacheConfig  `yaml:"usage-cache,omitempty" json:"usage-cache,omitempty"`
	ImageUsage         ChatGPTWebImageUsageConfig  `yaml:"image-usage,omitempty" json:"image-usage,omitempty"`
	LoginProxy         ChatGPTWebLoginProxyConfig  `yaml:"login-proxy,omitempty" json:"login-proxy,omitempty"`
	AccountInfo        ChatGPTWebAccountInfoConfig `yaml:"account-info,omitempty" json:"account-info,omitempty"`

	Sentinel ChatGPTWebSentinelConfig `yaml:"sentinel" json:"sentinel"`
}

// ResolvedChatGPTWebAutoReloginConfig contains effective background re-login values.
type ResolvedChatGPTWebAutoReloginConfig struct {
	MaxRetries    int
	JitterPercent int
	Workers       int
	QueueSize     int
}

// ResolvedAutoRelogin returns effective background re-login settings.
func (cfg ChatGPTWebConfig) ResolvedAutoRelogin() ResolvedChatGPTWebAutoReloginConfig {
	resolved := ResolvedChatGPTWebAutoReloginConfig{
		MaxRetries:    DefaultChatGPTWebAutoReloginMaxRetries,
		JitterPercent: DefaultChatGPTWebAutoReloginJitterPercent,
		Workers:       DefaultChatGPTWebAutoReloginWorkers,
		QueueSize:     DefaultChatGPTWebAutoReloginQueueSize,
	}
	if cfg.AutoReloginMaxRetries != nil {
		resolved.MaxRetries = *cfg.AutoReloginMaxRetries
	}
	if cfg.AutoReloginJitterPercent != nil {
		resolved.JitterPercent = *cfg.AutoReloginJitterPercent
	}
	if cfg.AutoReloginWorkers != nil {
		resolved.Workers = *cfg.AutoReloginWorkers
	}
	if cfg.AutoReloginQueueSize != nil {
		resolved.QueueSize = *cfg.AutoReloginQueueSize
	}
	return resolved
}

// ResolvedManualReloginConcurrency returns the management re-login operation limit.
func (cfg ChatGPTWebConfig) ResolvedManualReloginConcurrency() int {
	if cfg.ManualReloginConcurrency != nil {
		return *cfg.ManualReloginConcurrency
	}
	return DefaultChatGPTWebManualReloginConcurrency
}

// ResolvedChatGPTWebTimezone contains browser timezone fields sent upstream.
type ResolvedChatGPTWebTimezone struct {
	Timezone      string
	OffsetMinutes int
}

// ResolvedTimezone returns the configured browser timezone and effective offset.
func (cfg ChatGPTWebConfig) ResolvedTimezone(at time.Time) ResolvedChatGPTWebTimezone {
	timezone := strings.TrimSpace(cfg.Timezone)
	if timezone == "" {
		timezone = DefaultChatGPTWebTimezone
	}
	offset := DefaultChatGPTWebTimezoneOffsetMinutes
	if cfg.TimezoneOffsetMinutes != nil {
		offset = *cfg.TimezoneOffsetMinutes
	} else if location, errLoad := loadChatGPTWebTimezone(timezone); errLoad == nil {
		if at.IsZero() {
			at = time.Now()
		}
		_, eastOfUTCSeconds := at.In(location).Zone()
		offset = -eastOfUTCSeconds / 60
	}
	return ResolvedChatGPTWebTimezone{
		Timezone:      timezone,
		OffsetMinutes: offset,
	}
}

// TokenUsageEstimationEnabled returns whether Web response usage is estimated locally.
func (cfg ChatGPTWebConfig) TokenUsageEstimationEnabled() bool {
	return cfg.EstimateTokenUsage == nil || *cfg.EstimateTokenUsage
}

// ForceSessionRefreshOnImportEnabled reports whether valid session-backed imports are refreshed.
func (cfg ChatGPTWebConfig) ForceSessionRefreshOnImportEnabled() bool {
	return cfg.ForceSessionRefreshOnImport == nil || *cfg.ForceSessionRefreshOnImport
}

// ChatGPTWebImportConfig controls fast local imports and optional background maintenance.
type ChatGPTWebImportConfig struct {
	Workers                       *int  `yaml:"workers,omitempty" json:"workers,omitempty"`
	ValidateModelsAfterUpload     *bool `yaml:"validate-models-after-upload,omitempty" json:"validate-models-after-upload,omitempty"`
	RefreshAccountInfoAfterUpload *bool `yaml:"refresh-account-info-after-upload,omitempty" json:"refresh-account-info-after-upload,omitempty"`
}

// ResolvedChatGPTWebImportConfig contains effective import settings.
type ResolvedChatGPTWebImportConfig struct {
	Workers                       int  `json:"workers"`
	ValidateModelsAfterUpload     bool `json:"validate-models-after-upload"`
	RefreshAccountInfoAfterUpload bool `json:"refresh-account-info-after-upload"`
}

// UnmarshalYAML rejects unknown and null import settings.
func (cfg *ChatGPTWebImportConfig) UnmarshalYAML(node *yaml.Node) error {
	if cfg == nil || node == nil || node.Kind != yaml.MappingNode && node.Kind != yaml.AliasNode {
		return fmt.Errorf("chatgpt-web.import must be an object")
	}
	var effective map[string]any
	if err := node.Decode(&effective); err != nil {
		return fmt.Errorf("chatgpt-web.import: %w", err)
	}
	for name, value := range effective {
		switch name {
		case "workers", "validate-models-after-upload", "refresh-account-info-after-upload":
			if value == nil {
				return fmt.Errorf("chatgpt-web.import.%s must not be null", name)
			}
		default:
			return fmt.Errorf("chatgpt-web.import.%s is not supported", name)
		}
	}
	type plainChatGPTWebImportConfig ChatGPTWebImportConfig
	var decoded plainChatGPTWebImportConfig
	if err := node.Decode(&decoded); err != nil {
		return fmt.Errorf("chatgpt-web.import: %w", err)
	}
	*cfg = ChatGPTWebImportConfig(decoded)
	return nil
}

// Resolved returns effective upload and maintenance settings.
func (cfg ChatGPTWebImportConfig) Resolved() ResolvedChatGPTWebImportConfig {
	resolved := ResolvedChatGPTWebImportConfig{Workers: DefaultChatGPTWebImportWorkers}
	if cfg.Workers != nil {
		resolved.Workers = *cfg.Workers
	}
	if cfg.ValidateModelsAfterUpload != nil {
		resolved.ValidateModelsAfterUpload = *cfg.ValidateModelsAfterUpload
	}
	if cfg.RefreshAccountInfoAfterUpload != nil {
		resolved.RefreshAccountInfoAfterUpload = *cfg.RefreshAccountInfoAfterUpload
	}
	return resolved
}

// Validate rejects unsupported import worker counts.
func (cfg ChatGPTWebImportConfig) Validate() error {
	workers := cfg.Resolved().Workers
	if workers < 1 || workers > MaxChatGPTWebImportWorkers {
		return fmt.Errorf("chatgpt-web.import.workers must be between 1 and %d", MaxChatGPTWebImportWorkers)
	}
	return nil
}

// ChatGPTWebUsageCacheConfig controls optional disk spill for compact usage projections.
type ChatGPTWebUsageCacheConfig struct {
	Enabled                  *bool  `yaml:"enabled,omitempty" json:"enabled,omitempty"`
	DiskThresholdMB          *int64 `yaml:"disk-threshold-mb,omitempty" json:"disk-threshold-mb,omitempty"`
	MaxDiskSizeMB            *int64 `yaml:"max-disk-size-mb,omitempty" json:"max-disk-size-mb,omitempty"`
	ResourceGuardEnabled     *bool  `yaml:"resource-guard-enabled,omitempty" json:"resource-guard-enabled,omitempty"`
	MinAvailableDiskMB       *int64 `yaml:"min-available-disk-mb,omitempty" json:"min-available-disk-mb,omitempty"`
	MaxFilesystemUsedPercent *int   `yaml:"max-filesystem-used-percent,omitempty" json:"max-filesystem-used-percent,omitempty"`
	OrphanRetentionMinutes   *int   `yaml:"orphan-retention-minutes,omitempty" json:"orphan-retention-minutes,omitempty"`
	Path                     string `yaml:"path,omitempty" json:"path,omitempty"`
}

// ResolvedChatGPTWebUsageCacheConfig contains effective usage-cache values.
type ResolvedChatGPTWebUsageCacheConfig struct {
	Enabled                  bool   `json:"enabled"`
	DiskThresholdMB          int64  `json:"disk-threshold-mb"`
	MaxDiskSizeMB            int64  `json:"max-disk-size-mb"`
	ResourceGuardEnabled     bool   `json:"resource-guard-enabled"`
	MinAvailableDiskMB       int64  `json:"min-available-disk-mb"`
	MaxFilesystemUsedPercent int    `json:"max-filesystem-used-percent"`
	OrphanRetentionMinutes   int    `json:"orphan-retention-minutes"`
	Path                     string `json:"path"`
}

// Resolved returns effective usage-cache settings.
func (cfg ChatGPTWebUsageCacheConfig) Resolved() ResolvedChatGPTWebUsageCacheConfig {
	resolved := ResolvedChatGPTWebUsageCacheConfig{
		DiskThresholdMB:          DefaultChatGPTWebUsageCacheThresholdMB,
		MaxDiskSizeMB:            DefaultChatGPTWebUsageCacheMaxDiskSizeMB,
		ResourceGuardEnabled:     true,
		MinAvailableDiskMB:       DefaultChatGPTWebUsageCacheMinAvailableMB,
		MaxFilesystemUsedPercent: DefaultChatGPTWebUsageCacheMaxUsedPercent,
		Path:                     strings.TrimSpace(cfg.Path),
	}
	if cfg.Enabled != nil {
		resolved.Enabled = *cfg.Enabled
	}
	if cfg.DiskThresholdMB != nil {
		resolved.DiskThresholdMB = *cfg.DiskThresholdMB
	}
	if cfg.MaxDiskSizeMB != nil {
		resolved.MaxDiskSizeMB = *cfg.MaxDiskSizeMB
	}
	if cfg.ResourceGuardEnabled != nil {
		resolved.ResourceGuardEnabled = *cfg.ResourceGuardEnabled
	}
	if cfg.MinAvailableDiskMB != nil {
		resolved.MinAvailableDiskMB = *cfg.MinAvailableDiskMB
	}
	if cfg.MaxFilesystemUsedPercent != nil {
		resolved.MaxFilesystemUsedPercent = *cfg.MaxFilesystemUsedPercent
	}
	if cfg.OrphanRetentionMinutes != nil {
		resolved.OrphanRetentionMinutes = *cfg.OrphanRetentionMinutes
	}
	return resolved
}

// Validate rejects unsafe or internally inconsistent usage-cache settings.
func (cfg ChatGPTWebUsageCacheConfig) Validate() error {
	resolved := cfg.Resolved()
	if resolved.DiskThresholdMB < 1 {
		return fmt.Errorf("chatgpt-web.usage-cache.disk-threshold-mb must be at least 1")
	}
	if resolved.MaxDiskSizeMB < 1 {
		return fmt.Errorf("chatgpt-web.usage-cache.max-disk-size-mb must be at least 1")
	}
	if resolved.DiskThresholdMB > resolved.MaxDiskSizeMB {
		return fmt.Errorf("chatgpt-web.usage-cache.disk-threshold-mb must not exceed max-disk-size-mb")
	}
	if resolved.DiskThresholdMB > MaxChatGPTWebUsageCacheMegabytes ||
		resolved.MaxDiskSizeMB > MaxChatGPTWebUsageCacheMegabytes ||
		resolved.MinAvailableDiskMB > MaxChatGPTWebUsageCacheMegabytes {
		return fmt.Errorf("chatgpt-web.usage-cache size values exceed the supported byte range")
	}
	if resolved.MinAvailableDiskMB < 0 {
		return fmt.Errorf("chatgpt-web.usage-cache.min-available-disk-mb must not be negative")
	}
	if resolved.MaxFilesystemUsedPercent < 1 || resolved.MaxFilesystemUsedPercent > 100 {
		return fmt.Errorf("chatgpt-web.usage-cache.max-filesystem-used-percent must be between 1 and 100")
	}
	if resolved.OrphanRetentionMinutes < 0 || resolved.OrphanRetentionMinutes > MaxChatGPTWebUsageCacheOrphanRetention {
		return fmt.Errorf("chatgpt-web.usage-cache.orphan-retention-minutes must be between 0 and %d", MaxChatGPTWebUsageCacheOrphanRetention)
	}
	return nil
}

// ChatGPTWebImageUsageConfig controls local image token accounting.
type ChatGPTWebImageUsageConfig struct {
	AutoOutputQuality string                             `yaml:"auto-output-quality,omitempty" json:"auto-output-quality,omitempty"`
	FallbackUsage     ChatGPTWebImageFallbackUsageConfig `yaml:"fallback-usage,omitempty" json:"fallback-usage,omitempty"`
}

// ChatGPTWebImageFallbackUsageConfig controls fixed image usage returned when estimation is disabled.
type ChatGPTWebImageFallbackUsageConfig struct {
	Enabled           *bool  `yaml:"enabled,omitempty" json:"enabled,omitempty"`
	InputTextTokens   *int64 `yaml:"input-text-tokens,omitempty" json:"input-text-tokens,omitempty"`
	InputImageTokens  *int64 `yaml:"input-image-tokens,omitempty" json:"input-image-tokens,omitempty"`
	OutputTextTokens  *int64 `yaml:"output-text-tokens,omitempty" json:"output-text-tokens,omitempty"`
	OutputImageTokens *int64 `yaml:"output-image-tokens,omitempty" json:"output-image-tokens,omitempty"`
}

// ResolvedChatGPTWebImageFallbackUsageConfig contains effective fixed image usage values.
type ResolvedChatGPTWebImageFallbackUsageConfig struct {
	Enabled           bool  `json:"enabled"`
	InputTextTokens   int64 `json:"input-text-tokens"`
	InputImageTokens  int64 `json:"input-image-tokens"`
	OutputTextTokens  int64 `json:"output-text-tokens"`
	OutputImageTokens int64 `json:"output-image-tokens"`
}

// Resolved returns effective fixed image usage values.
func (cfg ChatGPTWebImageFallbackUsageConfig) Resolved() ResolvedChatGPTWebImageFallbackUsageConfig {
	resolved := ResolvedChatGPTWebImageFallbackUsageConfig{
		OutputImageTokens: DefaultChatGPTWebFallbackOutputImageTokens,
	}
	if cfg.Enabled != nil {
		resolved.Enabled = *cfg.Enabled
	}
	if cfg.InputTextTokens != nil {
		resolved.InputTextTokens = *cfg.InputTextTokens
	}
	if cfg.InputImageTokens != nil {
		resolved.InputImageTokens = *cfg.InputImageTokens
	}
	if cfg.OutputTextTokens != nil {
		resolved.OutputTextTokens = *cfg.OutputTextTokens
	}
	if cfg.OutputImageTokens != nil {
		resolved.OutputImageTokens = *cfg.OutputImageTokens
	}
	return resolved
}

// ResolvedAutoOutputQuality returns the local billing quality used for auto output.
func (cfg ChatGPTWebImageUsageConfig) ResolvedAutoOutputQuality() string {
	quality := strings.ToLower(strings.TrimSpace(cfg.AutoOutputQuality))
	if quality == "" {
		return DefaultChatGPTWebAutoOutputQuality
	}
	return quality
}

// Validate rejects unsupported automatic output quality mappings.
func (cfg ChatGPTWebImageUsageConfig) Validate() error {
	switch cfg.ResolvedAutoOutputQuality() {
	case "low", "medium", "high":
	default:
		return fmt.Errorf("chatgpt-web.image-usage.auto-output-quality must be low, medium, or high")
	}
	fallback := cfg.FallbackUsage.Resolved()
	if fallback.InputTextTokens < 0 ||
		fallback.InputImageTokens < 0 ||
		fallback.OutputTextTokens < 0 ||
		fallback.OutputImageTokens < 0 {
		return fmt.Errorf("chatgpt-web.image-usage.fallback-usage token values must not be negative")
	}
	return nil
}

// ChatGPTWebLoginProxyConfig controls a dynamic proxy used only by password login flows.
type ChatGPTWebLoginProxyConfig struct {
	Enabled                   bool   `yaml:"enabled" json:"enabled"`
	URLTemplate               string `yaml:"url-template" json:"url-template,omitempty"`
	PlaceholderCharset        string `yaml:"placeholder-charset,omitempty" json:"placeholder-charset,omitempty"`
	RotateOnRetry             *bool  `yaml:"rotate-on-retry,omitempty" json:"rotate-on-retry,omitempty"`
	RequestAttempts           *int   `yaml:"request-attempts,omitempty" json:"request-attempts,omitempty"`
	FlowAttempts              *int   `yaml:"flow-attempts,omitempty" json:"flow-attempts,omitempty"`
	RetryDelayMilliseconds    *int   `yaml:"retry-delay-milliseconds,omitempty" json:"retry-delay-milliseconds,omitempty"`
	AcquisitionTimeoutSeconds *int   `yaml:"acquisition-timeout-seconds,omitempty" json:"acquisition-timeout-seconds,omitempty"`
}

// ResolvedChatGPTWebLoginProxyConfig contains effective login-only proxy values.
type ResolvedChatGPTWebLoginProxyConfig struct {
	Enabled                   bool   `json:"enabled"`
	URLTemplate               string `json:"url-template"`
	PlaceholderCharset        string `json:"placeholder-charset"`
	RotateOnRetry             bool   `json:"rotate-on-retry"`
	RequestAttempts           int    `json:"request-attempts"`
	FlowAttempts              int    `json:"flow-attempts"`
	RetryDelayMilliseconds    int    `json:"retry-delay-milliseconds"`
	AcquisitionTimeoutSeconds int    `json:"acquisition-timeout-seconds"`
}

// Resolved returns the effective login-only proxy configuration.
func (cfg ChatGPTWebLoginProxyConfig) Resolved() ResolvedChatGPTWebLoginProxyConfig {
	resolved := ResolvedChatGPTWebLoginProxyConfig{
		Enabled:                   cfg.Enabled,
		URLTemplate:               strings.TrimSpace(cfg.URLTemplate),
		PlaceholderCharset:        strings.TrimSpace(cfg.PlaceholderCharset),
		RotateOnRetry:             true,
		RequestAttempts:           DefaultChatGPTWebLoginProxyRequestAttempts,
		FlowAttempts:              DefaultChatGPTWebLoginProxyFlowAttempts,
		RetryDelayMilliseconds:    DefaultChatGPTWebLoginProxyRetryDelayMS,
		AcquisitionTimeoutSeconds: DefaultChatGPTWebLoginProxyTimeoutSeconds,
	}
	if cfg.RotateOnRetry != nil {
		resolved.RotateOnRetry = *cfg.RotateOnRetry
	}
	if cfg.RequestAttempts != nil {
		resolved.RequestAttempts = *cfg.RequestAttempts
	}
	if cfg.FlowAttempts != nil {
		resolved.FlowAttempts = *cfg.FlowAttempts
	}
	if cfg.RetryDelayMilliseconds != nil {
		resolved.RetryDelayMilliseconds = *cfg.RetryDelayMilliseconds
	}
	if cfg.AcquisitionTimeoutSeconds != nil {
		resolved.AcquisitionTimeoutSeconds = *cfg.AcquisitionTimeoutSeconds
	}
	return resolved
}

// Validate rejects invalid login-only proxy templates and retry bounds.
func (cfg ChatGPTWebLoginProxyConfig) Validate() error {
	resolved := cfg.Resolved()
	if _, errCharset := proxyutil.NormalizePlaceholderCharset(resolved.PlaceholderCharset); errCharset != nil {
		return fmt.Errorf("chatgpt-web.login-proxy.placeholder-charset: %w", errCharset)
	}
	if resolved.URLTemplate != "" {
		if _, _, errTemplate := proxyutil.ValidateURLTemplate(resolved.URLTemplate, "", resolved.PlaceholderCharset); errTemplate != nil {
			return fmt.Errorf("chatgpt-web.login-proxy.url-template: %w", errTemplate)
		}
	} else if resolved.Enabled {
		return fmt.Errorf("chatgpt-web.login-proxy.url-template is required when enabled")
	}
	if resolved.RequestAttempts < MinChatGPTWebLoginProxyRequestAttempts ||
		resolved.RequestAttempts > MaxChatGPTWebLoginProxyRequestAttempts {
		return fmt.Errorf(
			"chatgpt-web.login-proxy.request-attempts must be between %d and %d",
			MinChatGPTWebLoginProxyRequestAttempts,
			MaxChatGPTWebLoginProxyRequestAttempts,
		)
	}
	if resolved.FlowAttempts < MinChatGPTWebLoginProxyFlowAttempts ||
		resolved.FlowAttempts > MaxChatGPTWebLoginProxyFlowAttempts {
		return fmt.Errorf(
			"chatgpt-web.login-proxy.flow-attempts must be between %d and %d",
			MinChatGPTWebLoginProxyFlowAttempts,
			MaxChatGPTWebLoginProxyFlowAttempts,
		)
	}
	if resolved.RetryDelayMilliseconds < MinChatGPTWebLoginProxyRetryDelayMS ||
		resolved.RetryDelayMilliseconds > MaxChatGPTWebLoginProxyRetryDelayMS {
		return fmt.Errorf(
			"chatgpt-web.login-proxy.retry-delay-milliseconds must be between %d and %d",
			MinChatGPTWebLoginProxyRetryDelayMS,
			MaxChatGPTWebLoginProxyRetryDelayMS,
		)
	}
	if resolved.AcquisitionTimeoutSeconds < MinChatGPTWebLoginProxyTimeoutSeconds ||
		resolved.AcquisitionTimeoutSeconds > MaxChatGPTWebLoginProxyTimeoutSeconds {
		return fmt.Errorf(
			"chatgpt-web.login-proxy.acquisition-timeout-seconds must be between %d and %d",
			MinChatGPTWebLoginProxyTimeoutSeconds,
			MaxChatGPTWebLoginProxyTimeoutSeconds,
		)
	}
	return nil
}

// Validate checks all ChatGPT Web runtime configuration.
func (cfg ChatGPTWebConfig) Validate() error {
	relogin := cfg.ResolvedAutoRelogin()
	if relogin.MaxRetries < 0 || relogin.MaxRetries > MaxChatGPTWebAutoReloginRetries {
		return fmt.Errorf("chatgpt-web.auto-relogin-max-retries must be between 0 and %d", MaxChatGPTWebAutoReloginRetries)
	}
	if relogin.JitterPercent < 0 || relogin.JitterPercent > MaxChatGPTWebAutoReloginJitterPercent {
		return fmt.Errorf("chatgpt-web.auto-relogin-jitter-percent must be between 0 and %d", MaxChatGPTWebAutoReloginJitterPercent)
	}
	if relogin.Workers < 1 || relogin.Workers > MaxChatGPTWebAutoReloginWorkers {
		return fmt.Errorf("chatgpt-web.auto-relogin-workers must be between 1 and %d", MaxChatGPTWebAutoReloginWorkers)
	}
	if relogin.QueueSize < 1 || relogin.QueueSize > MaxChatGPTWebAutoReloginQueueSize {
		return fmt.Errorf("chatgpt-web.auto-relogin-queue-size must be between 1 and %d", MaxChatGPTWebAutoReloginQueueSize)
	}
	manualReloginConcurrency := cfg.ResolvedManualReloginConcurrency()
	if manualReloginConcurrency < MinChatGPTWebManualReloginConcurrency {
		return fmt.Errorf(
			"chatgpt-web.manual-relogin-concurrency must be at least %d",
			MinChatGPTWebManualReloginConcurrency,
		)
	}
	timezone := strings.TrimSpace(cfg.Timezone)
	if timezone != "" {
		if _, errLoad := loadChatGPTWebTimezone(timezone); errLoad != nil {
			return fmt.Errorf("chatgpt-web.timezone must be a valid IANA timezone: %w", errLoad)
		}
	}
	if cfg.TimezoneOffsetMinutes != nil &&
		(*cfg.TimezoneOffsetMinutes < MinChatGPTWebTimezoneOffsetMinutes ||
			*cfg.TimezoneOffsetMinutes > MaxChatGPTWebTimezoneOffsetMinutes) {
		return fmt.Errorf(
			"chatgpt-web.timezone-offset-minutes must be between %d and %d",
			MinChatGPTWebTimezoneOffsetMinutes,
			MaxChatGPTWebTimezoneOffsetMinutes,
		)
	}
	if err := cfg.UsageCache.Validate(); err != nil {
		return err
	}
	if err := cfg.ImageUsage.Validate(); err != nil {
		return err
	}
	if err := cfg.LoginProxy.Validate(); err != nil {
		return err
	}
	if err := cfg.Import.Validate(); err != nil {
		return err
	}
	if err := cfg.AccountInfo.Validate(); err != nil {
		return err
	}
	return cfg.Sentinel.Validate()
}

func loadChatGPTWebTimezone(name string) (*time.Location, error) {
	name = strings.TrimSpace(name)
	if location, ok := chatGPTWebTimezoneLocations.Load(name); ok {
		return location.(*time.Location), nil
	}
	location, errLoad := time.LoadLocation(name)
	if errLoad != nil {
		return nil, errLoad
	}
	actual, _ := chatGPTWebTimezoneLocations.LoadOrStore(name, location)
	return actual.(*time.Location), nil
}

// ChatGPTWebAccountInfoConfig controls bounded account profile and image quota refreshes.
type ChatGPTWebAccountInfoConfig struct {
	AutoRefreshEnabled      *bool `yaml:"auto-refresh-enabled,omitempty" json:"auto-refresh-enabled,omitempty"`
	DiagnosticsEnabled      *bool `yaml:"diagnostics-enabled,omitempty" json:"diagnostics-enabled,omitempty"`
	RawQuotaResponseEnabled *bool `yaml:"raw-quota-response-enabled,omitempty" json:"raw-quota-response-enabled,omitempty"`
	RefreshWorkers          *int  `yaml:"refresh-workers,omitempty" json:"refresh-workers,omitempty"`
	RefreshQueueSize        *int  `yaml:"refresh-queue-size,omitempty" json:"refresh-queue-size,omitempty"`
	RefreshTTLMinutes       *int  `yaml:"refresh-ttl-minutes,omitempty" json:"refresh-ttl-minutes,omitempty"`
	PeriodicRefreshMinutes  *int  `yaml:"periodic-refresh-minutes,omitempty" json:"periodic-refresh-minutes,omitempty"`
	RecoveryJitterSeconds   *int  `yaml:"recovery-jitter-seconds,omitempty" json:"recovery-jitter-seconds,omitempty"`
	MaxRetries              *int  `yaml:"max-retries,omitempty" json:"max-retries,omitempty"`
}

// UnmarshalYAML rejects unknown and null account-info settings.
func (cfg *ChatGPTWebAccountInfoConfig) UnmarshalYAML(node *yaml.Node) error {
	if cfg == nil || node == nil {
		return fmt.Errorf("chatgpt-web.account-info must be an object")
	}
	if node.Kind != yaml.MappingNode && node.Kind != yaml.AliasNode {
		return fmt.Errorf("chatgpt-web.account-info must be an object")
	}
	var effective map[string]any
	if err := node.Decode(&effective); err != nil {
		return fmt.Errorf("chatgpt-web.account-info: %w", err)
	}
	for name, value := range effective {
		switch name {
		case "auto-refresh-enabled", "diagnostics-enabled", "raw-quota-response-enabled", "refresh-workers", "refresh-queue-size", "refresh-ttl-minutes", "periodic-refresh-minutes", "recovery-jitter-seconds", "max-retries":
			if value == nil {
				return fmt.Errorf("chatgpt-web.account-info.%s must not be null", name)
			}
		default:
			return fmt.Errorf("chatgpt-web.account-info.%s is not supported", name)
		}
	}
	type plainChatGPTWebAccountInfoConfig ChatGPTWebAccountInfoConfig
	var decoded plainChatGPTWebAccountInfoConfig
	if err := node.Decode(&decoded); err != nil {
		return fmt.Errorf("chatgpt-web.account-info: %w", err)
	}
	*cfg = ChatGPTWebAccountInfoConfig(decoded)
	return nil
}

// ResolvedChatGPTWebAccountInfoConfig contains effective refresh-pool values.
type ResolvedChatGPTWebAccountInfoConfig struct {
	AutoRefreshEnabled      bool `json:"auto-refresh-enabled"`
	DiagnosticsEnabled      bool `json:"diagnostics-enabled"`
	RawQuotaResponseEnabled bool `json:"raw-quota-response-enabled"`
	RefreshWorkers          int  `json:"refresh-workers"`
	RefreshQueueSize        int  `json:"refresh-queue-size"`
	RefreshTTLMinutes       int  `json:"refresh-ttl-minutes"`
	PeriodicRefreshMinutes  int  `json:"periodic-refresh-minutes"`
	RecoveryJitterSeconds   int  `json:"recovery-jitter-seconds"`
	MaxRetries              int  `json:"max-retries"`
	autoRefreshConfigured   bool
}

// AutomaticRefreshEnabled reports the effective automatic refresh setting.
// A zero-value resolved config keeps the historical enabled behavior.
func (cfg ResolvedChatGPTWebAccountInfoConfig) AutomaticRefreshEnabled() bool {
	return !cfg.autoRefreshConfigured || cfg.AutoRefreshEnabled
}

// Resolved returns effective account profile and quota refresh settings.
func (cfg ChatGPTWebAccountInfoConfig) Resolved() ResolvedChatGPTWebAccountInfoConfig {
	resolved := ResolvedChatGPTWebAccountInfoConfig{
		AutoRefreshEnabled:     true,
		RefreshWorkers:         DefaultChatGPTWebAccountInfoWorkers,
		RefreshQueueSize:       DefaultChatGPTWebAccountInfoQueueSize,
		RefreshTTLMinutes:      DefaultChatGPTWebAccountInfoTTLMinutes,
		PeriodicRefreshMinutes: DefaultChatGPTWebAccountInfoPeriodMinutes,
		RecoveryJitterSeconds:  DefaultChatGPTWebAccountInfoJitterSeconds,
		MaxRetries:             DefaultChatGPTWebAccountInfoMaxRetries,
	}
	if cfg.AutoRefreshEnabled != nil {
		resolved.AutoRefreshEnabled = *cfg.AutoRefreshEnabled
		resolved.autoRefreshConfigured = true
	}
	if cfg.DiagnosticsEnabled != nil {
		resolved.DiagnosticsEnabled = *cfg.DiagnosticsEnabled
	}
	if cfg.RawQuotaResponseEnabled != nil {
		resolved.RawQuotaResponseEnabled = *cfg.RawQuotaResponseEnabled
	}
	if cfg.RefreshWorkers != nil {
		resolved.RefreshWorkers = *cfg.RefreshWorkers
	}
	if cfg.RefreshQueueSize != nil {
		resolved.RefreshQueueSize = *cfg.RefreshQueueSize
	}
	if cfg.RefreshTTLMinutes != nil {
		resolved.RefreshTTLMinutes = *cfg.RefreshTTLMinutes
	}
	if cfg.PeriodicRefreshMinutes != nil {
		resolved.PeriodicRefreshMinutes = *cfg.PeriodicRefreshMinutes
	}
	if cfg.RecoveryJitterSeconds != nil {
		resolved.RecoveryJitterSeconds = *cfg.RecoveryJitterSeconds
	}
	if cfg.MaxRetries != nil {
		resolved.MaxRetries = *cfg.MaxRetries
	}
	return resolved
}

// Validate rejects account-info pool values outside their documented bounds.
func (cfg ChatGPTWebAccountInfoConfig) Validate() error {
	resolved := cfg.Resolved()
	if resolved.RefreshWorkers < 1 || resolved.RefreshWorkers > MaxChatGPTWebAccountInfoWorkers {
		return fmt.Errorf("chatgpt-web.account-info.refresh-workers must be between 1 and %d", MaxChatGPTWebAccountInfoWorkers)
	}
	if resolved.RefreshQueueSize < 0 || resolved.RefreshQueueSize > MaxChatGPTWebAccountInfoQueueSize {
		return fmt.Errorf("chatgpt-web.account-info.refresh-queue-size must be between 0 and %d", MaxChatGPTWebAccountInfoQueueSize)
	}
	if resolved.RefreshTTLMinutes < 1 || resolved.RefreshTTLMinutes > MaxChatGPTWebAccountInfoTTLMinutes {
		return fmt.Errorf("chatgpt-web.account-info.refresh-ttl-minutes must be between 1 and %d", MaxChatGPTWebAccountInfoTTLMinutes)
	}
	if resolved.PeriodicRefreshMinutes < 0 || resolved.PeriodicRefreshMinutes > MaxChatGPTWebAccountInfoPeriodMinutes {
		return fmt.Errorf("chatgpt-web.account-info.periodic-refresh-minutes must be between 0 and %d", MaxChatGPTWebAccountInfoPeriodMinutes)
	}
	if resolved.RecoveryJitterSeconds < 0 || resolved.RecoveryJitterSeconds > MaxChatGPTWebAccountInfoJitterSeconds {
		return fmt.Errorf("chatgpt-web.account-info.recovery-jitter-seconds must be between 0 and %d", MaxChatGPTWebAccountInfoJitterSeconds)
	}
	if resolved.MaxRetries < 0 || resolved.MaxRetries > MaxChatGPTWebAccountInfoRetries {
		return fmt.Errorf("chatgpt-web.account-info.max-retries must be between 0 and %d", MaxChatGPTWebAccountInfoRetries)
	}
	return nil
}

// ChatGPTWebSentinelConfig preserves whether values were explicitly configured.
// This matters because disabled and zero-length queue are both valid overrides.
type ChatGPTWebSentinelConfig struct {
	SDKRuntimeEnabled *bool `yaml:"sdk-runtime-enabled,omitempty" json:"sdk-runtime-enabled,omitempty"`
	SDKWorkers        *int  `yaml:"sdk-workers,omitempty" json:"sdk-workers,omitempty"`
	SDKQueueSize      *int  `yaml:"sdk-queue-size,omitempty" json:"sdk-queue-size,omitempty"`
	SDKCacheVersions  *int  `yaml:"sdk-cache-versions,omitempty" json:"sdk-cache-versions,omitempty"`
}

// UnmarshalYAML distinguishes an omitted setting from an explicit null value.
func (cfg *ChatGPTWebSentinelConfig) UnmarshalYAML(node *yaml.Node) error {
	if cfg == nil || node == nil {
		return fmt.Errorf("chatgpt-web.sentinel must be an object")
	}
	if err := validateChatGPTWebSentinelMappingFields(node); err != nil {
		return err
	}
	type plainChatGPTWebSentinelConfig ChatGPTWebSentinelConfig
	var decoded plainChatGPTWebSentinelConfig
	if err := node.Decode(&decoded); err != nil {
		return fmt.Errorf("chatgpt-web.sentinel: %w", err)
	}
	*cfg = ChatGPTWebSentinelConfig(decoded)
	return nil
}

func validateChatGPTWebSentinelMappingFields(node *yaml.Node) error {
	if node.Kind != yaml.MappingNode && node.Kind != yaml.AliasNode {
		return fmt.Errorf("chatgpt-web.sentinel must be an object")
	}
	var effective map[string]any
	if err := node.Decode(&effective); err != nil {
		return fmt.Errorf("chatgpt-web.sentinel: %w", err)
	}
	for name, value := range effective {
		switch name {
		case "sdk-runtime-enabled", "sdk-workers", "sdk-queue-size", "sdk-cache-versions":
			if value == nil {
				return fmt.Errorf("chatgpt-web.sentinel.%s must not be null", name)
			}
		default:
			return fmt.Errorf("chatgpt-web.sentinel.%s is not supported", name)
		}
	}
	return nil
}

// ResolvedChatGPTWebSentinelConfig contains effective runtime values.
type ResolvedChatGPTWebSentinelConfig struct {
	SDKRuntimeEnabled bool `json:"sdk-runtime-enabled"`
	SDKWorkers        int  `json:"sdk-workers"`
	SDKQueueSize      int  `json:"sdk-queue-size"`
	SDKCacheVersions  int  `json:"sdk-cache-versions"`
}

// Resolved returns the effective Sentinel SDK configuration.
func (cfg ChatGPTWebSentinelConfig) Resolved() ResolvedChatGPTWebSentinelConfig {
	out := ResolvedChatGPTWebSentinelConfig{
		SDKRuntimeEnabled: true,
		SDKQueueSize:      DefaultChatGPTWebSentinelSDKQueueSize,
		SDKCacheVersions:  DefaultChatGPTWebSentinelSDKCacheVersions,
	}
	if cfg.SDKRuntimeEnabled != nil {
		out.SDKRuntimeEnabled = *cfg.SDKRuntimeEnabled
	}
	if cfg.SDKWorkers != nil {
		out.SDKWorkers = *cfg.SDKWorkers
	}
	if cfg.SDKQueueSize != nil {
		out.SDKQueueSize = *cfg.SDKQueueSize
	}
	if cfg.SDKCacheVersions != nil {
		out.SDKCacheVersions = *cfg.SDKCacheVersions
	}
	return out
}

// Validate rejects structurally invalid Sentinel SDK values.
func (cfg ChatGPTWebSentinelConfig) Validate() error {
	resolved := cfg.Resolved()
	if resolved.SDKWorkers < 0 {
		return fmt.Errorf("chatgpt-web.sentinel.sdk-workers must not be negative")
	}
	if resolved.SDKQueueSize < 0 || resolved.SDKQueueSize > MaxChatGPTWebSentinelSDKQueueSize {
		return fmt.Errorf("chatgpt-web.sentinel.sdk-queue-size must be between 0 and %d", MaxChatGPTWebSentinelSDKQueueSize)
	}
	if resolved.SDKCacheVersions < 1 || resolved.SDKCacheVersions > MaxChatGPTWebSentinelSDKCacheVersions {
		return fmt.Errorf("chatgpt-web.sentinel.sdk-cache-versions must be between 1 and %d", MaxChatGPTWebSentinelSDKCacheVersions)
	}
	return nil
}

// CodexFingerprintConfig controls optional Codex upstream fingerprinting.
type CodexFingerprintConfig struct {
	JA3                     bool `yaml:"ja3" json:"ja3"`
	ForceHTTP1              bool `yaml:"force-http1" json:"force-http1"`
	ImagesForceHTTP1        bool `yaml:"images-force-http1" json:"images-force-http1"`
	SessionIdentityPoolSize int  `yaml:"session-identity-pool-size,omitempty" json:"session-identity-pool-size,omitempty"`
	// DefaultMode is persisted only on future Codex uploads that omit codex_fingerprint_mode.
	DefaultMode string `yaml:"default-mode" json:"default-mode"`
}

// ResolvedDefaultMode returns the upload default while preserving the legacy device behavior.
func (cfg CodexFingerprintConfig) ResolvedDefaultMode() string {
	mode, err := NormalizeCodexFingerprintMode(cfg.DefaultMode)
	if err != nil {
		return DefaultCodexFingerprintMode
	}
	return mode
}

// ResolvedSessionIdentityPoolSize returns the configured pool size or the
// default for zero-valued in-memory configurations.
func (cfg CodexFingerprintConfig) ResolvedSessionIdentityPoolSize() int {
	if cfg.SessionIdentityPoolSize < 1 || cfg.SessionIdentityPoolSize > MaxCodexSessionIdentityPoolSize {
		return DefaultCodexSessionIdentityPoolSize
	}
	return cfg.SessionIdentityPoolSize
}

// NormalizeCodexFingerprintMode validates a Codex credential fingerprint mode.
func NormalizeCodexFingerprintMode(value string) (string, error) {
	mode := strings.ToLower(strings.TrimSpace(value))
	if mode == "" {
		return DefaultCodexFingerprintMode, nil
	}
	switch mode {
	case "off", "device", "session", "full":
		return mode, nil
	default:
		return "", fmt.Errorf("codex-fingerprint.default-mode must be off, device, session, or full")
	}
}

// TLSConfig holds HTTPS server settings.
type TLSConfig struct {
	// Enable toggles HTTPS server mode.
	Enable bool `yaml:"enable" json:"enable"`
	// Cert is the path to the TLS certificate file.
	Cert string `yaml:"cert" json:"cert"`
	// Key is the path to the TLS private key file.
	Key string `yaml:"key" json:"key"`
}

// PprofConfig holds pprof HTTP server settings.
type PprofConfig struct {
	// Enable toggles the pprof HTTP debug server.
	Enable bool `yaml:"enable" json:"enable"`
	// Addr is the host:port address for the pprof HTTP server.
	Addr string `yaml:"addr" json:"addr"`
}

// RemoteManagement holds management API configuration under 'remote-management'.
type RemoteManagement struct {
	// AllowRemote toggles remote (non-localhost) access to management API.
	AllowRemote bool `yaml:"allow-remote"`
	// SecretKey is the management key (plaintext or bcrypt hashed). YAML key intentionally 'secret-key'.
	SecretKey string `yaml:"secret-key"`
	// AccessPath optionally hides management routes behind a custom single path segment.
	AccessPath string `yaml:"access-path"`
	// DisableControlPanel skips serving and syncing the bundled management UI when true.
	DisableControlPanel bool `yaml:"disable-control-panel"`
	// DisableAutoUpdatePanel disables automatic periodic background updates of the management panel asset from GitHub.
	// When false (the default), the background updater remains enabled; when true, the panel is only downloaded on first access if missing.
	DisableAutoUpdatePanel bool `yaml:"disable-auto-update-panel"`
	// PanelGitHubRepository overrides the GitHub repository used to fetch the management panel asset.
	// Accepts either a repository URL (https://github.com/org/repo) or an API releases endpoint.
	PanelGitHubRepository string `yaml:"panel-github-repository"`
	// AuthFilesPagination controls opt-in server-side pagination for the auth files management page.
	AuthFilesPagination AuthFilesPaginationConfig `yaml:"auth-files-pagination" json:"auth-files-pagination"`
	// LiveLogs controls the in-memory management log stream.
	LiveLogs LiveLogsConfig `yaml:"live-logs" json:"live-logs"`
	// Diagnostics controls authenticated management-only diagnostic detail.
	Diagnostics ManagementDiagnosticsConfig `yaml:"diagnostics" json:"diagnostics"`
}

// UnmarshalYAML preserves existing remote-management compatibility while rejecting
// explicit null nested configuration objects.
func (cfg *RemoteManagement) UnmarshalYAML(node *yaml.Node) error {
	if cfg == nil || node == nil {
		return fmt.Errorf("remote-management must be an object")
	}
	if node.Kind == yaml.ScalarNode && node.Tag == "!!null" {
		*cfg = RemoteManagement{}
		return nil
	}
	if node.Kind != yaml.MappingNode && node.Kind != yaml.AliasNode {
		return fmt.Errorf("remote-management must be an object")
	}
	var effective map[string]any
	if err := node.Decode(&effective); err != nil {
		return fmt.Errorf("remote-management: %w", err)
	}
	if value, present := effective["auth-files-pagination"]; present && value == nil {
		return fmt.Errorf("remote-management.auth-files-pagination must be an object")
	}
	if value, present := effective["live-logs"]; present && value == nil {
		return fmt.Errorf("remote-management.live-logs must be an object")
	}
	if value, present := effective["diagnostics"]; present && value == nil {
		return fmt.Errorf("remote-management.diagnostics must be an object")
	}
	type plainRemoteManagement RemoteManagement
	decoded := plainRemoteManagement(*cfg)
	if err := node.Decode(&decoded); err != nil {
		return fmt.Errorf("remote-management: %w", err)
	}
	*cfg = RemoteManagement(decoded)
	return nil
}

const (
	ManagementDiagnosticsDetailSafe = "safe"
	ManagementDiagnosticsDetailFull = "full"
)

// ManagementDiagnosticsConfig controls management-only diagnostic content.
type ManagementDiagnosticsConfig struct {
	DetailLevel string `yaml:"detail-level" json:"detail-level"`
}

// ResolvedDetailLevel returns the effective detail level. The zero value is safe.
func (cfg ManagementDiagnosticsConfig) ResolvedDetailLevel() string {
	if strings.EqualFold(strings.TrimSpace(cfg.DetailLevel), ManagementDiagnosticsDetailFull) {
		return ManagementDiagnosticsDetailFull
	}
	return ManagementDiagnosticsDetailSafe
}

// MarshalYAML keeps cloned and generated zero-value configurations valid.
func (cfg ManagementDiagnosticsConfig) MarshalYAML() (any, error) {
	return struct {
		DetailLevel string `yaml:"detail-level"`
	}{DetailLevel: cfg.ResolvedDetailLevel()}, nil
}

// UnmarshalYAML rejects null, scalar, unknown, and unsupported diagnostics settings.
func (cfg *ManagementDiagnosticsConfig) UnmarshalYAML(node *yaml.Node) error {
	if cfg == nil || node == nil || (node.Kind != yaml.MappingNode && node.Kind != yaml.AliasNode) {
		return fmt.Errorf("remote-management.diagnostics must be an object")
	}
	var effective map[string]any
	if err := node.Decode(&effective); err != nil {
		return fmt.Errorf("remote-management.diagnostics: %w", err)
	}
	for name, value := range effective {
		if name != "detail-level" {
			return fmt.Errorf("remote-management.diagnostics.%s is not supported", name)
		}
		if value == nil {
			return fmt.Errorf("remote-management.diagnostics.detail-level must not be null")
		}
	}
	decoded := struct {
		DetailLevel string `yaml:"detail-level"`
	}{DetailLevel: ManagementDiagnosticsDetailSafe}
	if err := node.Decode(&decoded); err != nil {
		return fmt.Errorf("remote-management.diagnostics: %w", err)
	}
	decoded.DetailLevel = strings.ToLower(strings.TrimSpace(decoded.DetailLevel))
	if decoded.DetailLevel != ManagementDiagnosticsDetailSafe && decoded.DetailLevel != ManagementDiagnosticsDetailFull {
		return fmt.Errorf("remote-management.diagnostics.detail-level must be safe or full")
	}
	*cfg = ManagementDiagnosticsConfig{DetailLevel: decoded.DetailLevel}
	return nil
}

// LiveLogsConfig controls the authenticated in-memory management log stream.
type LiveLogsConfig struct {
	Enabled bool `yaml:"enabled" json:"enabled"`
}

// UnmarshalYAML rejects null, scalar, and unknown live-logs settings.
func (cfg *LiveLogsConfig) UnmarshalYAML(node *yaml.Node) error {
	if cfg == nil || node == nil || (node.Kind != yaml.MappingNode && node.Kind != yaml.AliasNode) {
		return fmt.Errorf("remote-management.live-logs must be an object")
	}
	var effective map[string]any
	if err := node.Decode(&effective); err != nil {
		return fmt.Errorf("remote-management.live-logs: %w", err)
	}
	for name, value := range effective {
		if name != "enabled" {
			return fmt.Errorf("remote-management.live-logs.%s is not supported", name)
		}
		if value == nil {
			return fmt.Errorf("remote-management.live-logs.enabled must not be null")
		}
	}
	type plainLiveLogsConfig LiveLogsConfig
	var decoded plainLiveLogsConfig
	if err := node.Decode(&decoded); err != nil {
		return fmt.Errorf("remote-management.live-logs: %w", err)
	}
	*cfg = LiveLogsConfig(decoded)
	return nil
}

// AuthFilesPaginationConfig controls server-side pagination for auth file listings.
type AuthFilesPaginationConfig struct {
	Enabled bool `yaml:"enabled" json:"enabled"`
}

// UnmarshalYAML rejects null, scalar, and unknown auth-files-pagination settings.
func (cfg *AuthFilesPaginationConfig) UnmarshalYAML(node *yaml.Node) error {
	if cfg == nil || node == nil || (node.Kind != yaml.MappingNode && node.Kind != yaml.AliasNode) {
		return fmt.Errorf("remote-management.auth-files-pagination must be an object")
	}
	var effective map[string]any
	if err := node.Decode(&effective); err != nil {
		return fmt.Errorf("remote-management.auth-files-pagination: %w", err)
	}
	for name, value := range effective {
		if name != "enabled" {
			return fmt.Errorf("remote-management.auth-files-pagination.%s is not supported", name)
		}
		if value == nil {
			return fmt.Errorf("remote-management.auth-files-pagination.enabled must not be null")
		}
	}
	type plainAuthFilesPaginationConfig AuthFilesPaginationConfig
	var decoded plainAuthFilesPaginationConfig
	if err := node.Decode(&decoded); err != nil {
		return fmt.Errorf("remote-management.auth-files-pagination: %w", err)
	}
	*cfg = AuthFilesPaginationConfig(decoded)
	return nil
}

// QuotaExceeded defines the behavior when API quota limits are exceeded.
// It provides configuration options for automatic failover mechanisms.
type QuotaExceeded struct {
	// SwitchProject indicates whether to automatically switch to another project when a quota is exceeded.
	SwitchProject bool `yaml:"switch-project" json:"switch-project"`

	// SwitchPreviewModel indicates whether to automatically switch to a preview model when a quota is exceeded.
	SwitchPreviewModel bool `yaml:"switch-preview-model" json:"switch-preview-model"`

	// AntigravityCredits indicates whether to retry Antigravity quota_exhausted 429s once
	// on the same credential with enabledCreditTypes=["GOOGLE_ONE_AI"].
	AntigravityCredits bool `yaml:"antigravity-credits" json:"antigravity-credits"`
}

// AuthMaintenanceConfig controls optional background handling of auth files
// that repeatedly fail with terminal or quota-related errors.
type AuthMaintenanceConfig struct {
	// Enable starts the background maintenance queue when true.
	Enable bool `yaml:"enable" json:"enable"`
	// ScanIntervalSeconds defines how often the runtime auth set is scanned for delete candidates.
	ScanIntervalSeconds int `yaml:"scan-interval-seconds" json:"scan-interval-seconds"`
	// DeleteIntervalSeconds defines the stagger interval between queued deletions.
	DeleteIntervalSeconds int `yaml:"delete-interval-seconds" json:"delete-interval-seconds"`
	// DeleteStatusCodes defines HTTP status codes that should trigger deletion immediately.
	DeleteStatusCodes []int `yaml:"delete-status-codes" json:"delete-status-codes"`
	// DisableStatusCodes defines HTTP status codes that should disable auths without deleting files.
	DisableStatusCodes []int `yaml:"disable-status-codes" json:"disable-status-codes"`
	// DeleteQuotaExceeded enables deletion for auths that repeatedly hit quota limits.
	DeleteQuotaExceeded bool `yaml:"delete-quota-exceeded" json:"delete-quota-exceeded"`
	// QuotaStrikeThreshold is the minimum number of 429 hits required before the delete path triggers.
	QuotaStrikeThreshold int `yaml:"quota-strike-threshold" json:"quota-strike-threshold"`
	// DisableQuotaExceeded enables disable-only handling for auths that repeatedly hit quota limits.
	DisableQuotaExceeded bool `yaml:"disable-quota-exceeded" json:"disable-quota-exceeded"`
	// DisableQuotaStrikeThreshold is the minimum number of 429 hits required before the disable-only path triggers.
	DisableQuotaStrikeThreshold int `yaml:"disable-quota-strike-threshold" json:"disable-quota-strike-threshold"`
}

// FixedErrorCooldownRule defines a custom cooldown for a stable upstream error.
type FixedErrorCooldownRule struct {
	// StatusCode optionally restricts the rule to one HTTP status code.
	// Set to 0 or omit it to match by message only.
	StatusCode int `yaml:"status-code" json:"status-code"`
	// MessageContains optionally matches a substring in the upstream error message, case-insensitively.
	MessageContains string `yaml:"message-contains,omitempty" json:"message-contains,omitempty"`
	// CooldownSeconds is the cooldown duration applied when the rule matches. Must be greater than 0.
	CooldownSeconds int `yaml:"cooldown-seconds" json:"cooldown-seconds"`
	// Scope controls whether the rule cools only the model or the whole auth. Valid values: model, auth.
	Scope string `yaml:"scope,omitempty" json:"scope,omitempty"`
}

// NonRetryableErrorRule marks a stable upstream error as request-scoped so the router will not retry it.
type NonRetryableErrorRule struct {
	// StatusCode optionally restricts the rule to one HTTP status code. Set to 0 or omit it to ignore status.
	StatusCode int `yaml:"status-code" json:"status-code"`
	// Type matches error.type or top-level type case-insensitively.
	Type string `yaml:"type,omitempty" json:"type,omitempty"`
	// Code matches error.code or top-level code case-insensitively.
	Code string `yaml:"code,omitempty" json:"code,omitempty"`
	// MessageContains matches a substring in error.message, message, or the raw error string case-insensitively.
	MessageContains string `yaml:"message-contains,omitempty" json:"message-contains,omitempty"`
}

// AuthModelExclusionRule removes models from credentials that match non-secret auth metadata.
type AuthModelExclusionRule struct {
	// Models are route model IDs to remove before applying model prefixes. Use "-all"
	// as the first item and "+model-id" entries to keep only selected models.
	Models []string `yaml:"models" json:"models"`
	// Providers optionally restricts the rule to provider keys.
	Providers []string `yaml:"providers,omitempty" json:"providers,omitempty"`
	// Priorities optionally restricts the rule to credential priorities.
	Priorities []int `yaml:"priorities,omitempty" json:"priorities,omitempty"`
	// KeywordContains matches non-secret identity fields case-insensitively.
	KeywordContains []string `yaml:"keyword-contains,omitempty" json:"keyword-contains,omitempty"`
	// DisableImageGeneration disables Codex image-generation capability for matching credentials.
	DisableImageGeneration bool `yaml:"disable-image-generation,omitempty" json:"disable-image-generation,omitempty"`
}

// DisabledImageGenerationToolErrorConfig defines the response for disabled image_generation tool requests.
type DisabledImageGenerationToolErrorConfig struct {
	StatusCode int    `yaml:"status-code" json:"status-code"`
	Message    string `yaml:"message" json:"message"`
	Type       string `yaml:"type" json:"type"`
	Code       string `yaml:"code" json:"code"`
}

// RequestBodyReleaseConfig controls timed release of retained request body copies.
type RequestBodyReleaseConfig struct {
	Enable       bool  `yaml:"enable" json:"enable"`
	LogOnly      bool  `yaml:"log-only" json:"log-only"`
	AfterSeconds int   `yaml:"after-seconds" json:"after-seconds"`
	MinBodyBytes int64 `yaml:"min-body-bytes" json:"min-body-bytes"`
}

// RequestBodyAuditConfig defines byte-level request body keyword blocking for model APIs.
type RequestBodyAuditConfig struct {
	Enable bool `yaml:"enable" json:"enable"`
	// Keywords are UTF-8 strings matched against the raw request body bytes.
	Keywords []string `yaml:"keywords,omitempty" json:"keywords,omitempty"`
	// KeywordsBase64 are raw byte keywords encoded as base64 for YAML/JSON transport.
	KeywordsBase64 []string `yaml:"keywords-base64,omitempty" json:"keywords-base64,omitempty"`
	// CaseSensitive controls case folding before byte matching.
	CaseSensitive bool `yaml:"case-sensitive" json:"case-sensitive"`
	// MaxBodyBytes limits bytes read for auditing. Set 0 to read the complete request body.
	MaxBodyBytes int64 `yaml:"max-body-bytes,omitempty" json:"max-body-bytes,omitempty"`
	// RejectOversize rejects bodies larger than MaxBodyBytes when MaxBodyBytes > 0.
	RejectOversize bool                        `yaml:"reject-oversize" json:"reject-oversize"`
	Error          RequestBodyAuditErrorConfig `yaml:"error" json:"error"`

	compiledKeywords [][]byte `yaml:"-" json:"-"`
}

// RequestBodyAuditErrorConfig defines the response returned when request body audit blocks a request.
type RequestBodyAuditErrorConfig struct {
	StatusCode int    `yaml:"status-code" json:"status-code"`
	Message    string `yaml:"message" json:"message"`
	Type       string `yaml:"type" json:"type"`
	Code       string `yaml:"code,omitempty" json:"code,omitempty"`
}

// RoutingSubscriptionOverride overrides generic request limits for subscriptions
// within one credential priority.
type RoutingSubscriptionOverride struct {
	// Providers optionally limits this rule to exact runtime provider IDs.
	// An empty list matches every provider.
	Providers []string `yaml:"providers,omitempty" json:"providers,omitempty"`
	// PlanTypes matches normalized credential plan_type values.
	PlanTypes []string `yaml:"plan-types" json:"plan-types"`
	// PerAuthRequestLimit optionally overrides the inherited per-auth request limit.
	// A non-nil zero value disables the generic request limit for matching credentials.
	PerAuthRequestLimit *int `yaml:"per-auth-request-limit,omitempty" json:"per-auth-request-limit,omitempty"`
	// PerAuthRequestWindowMinutes optionally overrides the inherited fixed-window size.
	PerAuthRequestWindowMinutes *int `yaml:"per-auth-request-window-minutes,omitempty" json:"per-auth-request-window-minutes,omitempty"`
}

// RoutingPriorityOverride overrides routing behavior for one credential priority.
type RoutingPriorityOverride struct {
	// Priority is the credential priority this rule applies to.
	Priority int `yaml:"priority" json:"priority"`
	// Strategy optionally overrides the global credential selection strategy.
	// Supported values: "round-robin", "fill-first", "random".
	Strategy string `yaml:"strategy,omitempty" json:"strategy,omitempty"`
	// MaxRetryCredentials optionally overrides the global per-round credential limit.
	// Set to 0 to try all available credentials in this priority.
	MaxRetryCredentials *int `yaml:"max-retry-credentials,omitempty" json:"max-retry-credentials,omitempty"`
	// FillFirstRange optionally overrides routing.fill-first-range for this priority.
	FillFirstRange *int `yaml:"fill-first-range,omitempty" json:"fill-first-range,omitempty"`
	// FillFirstPerAuthRPM optionally overrides routing.fill-first-per-auth-rpm for this priority.
	FillFirstPerAuthRPM *int `yaml:"fill-first-per-auth-rpm,omitempty" json:"fill-first-per-auth-rpm,omitempty"`
	// PerAuthRequestLimit optionally overrides routing.per-auth-request-limit for this priority.
	// A non-nil zero value disables the generic request limit for this priority.
	PerAuthRequestLimit *int `yaml:"per-auth-request-limit,omitempty" json:"per-auth-request-limit,omitempty"`
	// PerAuthRequestWindowMinutes optionally overrides routing.per-auth-request-window-minutes for this priority.
	PerAuthRequestWindowMinutes *int `yaml:"per-auth-request-window-minutes,omitempty" json:"per-auth-request-window-minutes,omitempty"`
	// SubscriptionOverrides optionally refines generic request limits by provider and plan type.
	SubscriptionOverrides []RoutingSubscriptionOverride `yaml:"subscription-overrides,omitempty" json:"subscription-overrides,omitempty"`
}

// RoutingConfig configures how credentials are selected for requests.
type RoutingConfig struct {
	// Strategy selects the credential selection strategy.
	// Supported values: "round-robin" (default), "fill-first", "random".
	Strategy string `yaml:"strategy,omitempty" json:"strategy,omitempty"`

	// FillFirstRange groups credentials for fill-first routing; 1 preserves legacy fill-first.
	FillFirstRange int `yaml:"fill-first-range,omitempty" json:"fill-first-range,omitempty"`

	// FillFirstPerAuthRPM caps selected requests per auth per fixed minute for fill-first routing.
	FillFirstPerAuthRPM int `yaml:"fill-first-per-auth-rpm,omitempty" json:"fill-first-per-auth-rpm,omitempty"`

	// PerAuthRequestLimit caps selected requests per auth in a fixed window for all built-in strategies.
	PerAuthRequestLimit int `yaml:"per-auth-request-limit" json:"per-auth-request-limit,omitempty"`

	// PerAuthRequestWindowMinutes configures the fixed request-limit window in minutes.
	PerAuthRequestWindowMinutes int `yaml:"per-auth-request-window-minutes,omitempty" json:"per-auth-request-window-minutes,omitempty"`

	// PriorityOverrides customizes routing behavior for specific credential priorities.
	PriorityOverrides []RoutingPriorityOverride `yaml:"priority-overrides,omitempty" json:"priority-overrides,omitempty"`

	// ClaudeCodeSessionAffinity enables session-sticky routing for Claude Code clients.
	// When enabled, requests with the same session ID (extracted from metadata.user_id)
	// are routed to the same auth credential when available.
	// Deprecated: Use SessionAffinity instead for universal session support.
	ClaudeCodeSessionAffinity bool `yaml:"claude-code-session-affinity,omitempty" json:"claude-code-session-affinity,omitempty"`

	// SessionAffinity enables universal session-sticky routing for all clients.
	// Session IDs are extracted from multiple sources:
	// metadata.user_id, X-Session-ID, Session-Id/Session_id, X-Client-Request-Id,
	// conversation_id, or message hash.
	SessionAffinity bool `yaml:"session-affinity,omitempty" json:"session-affinity,omitempty"`

	// SessionAffinityFailover controls whether a session may move to another credential
	// when the bound credential is unavailable or the request fails. Defaults to true.
	SessionAffinityFailover *bool `yaml:"session-affinity-failover,omitempty" json:"session-affinity-failover,omitempty"`

	// SessionAffinityTTL specifies how long session-to-auth bindings are retained.
	// Default: 1h. Accepts duration strings like "30m", "1h", "2h30m".
	SessionAffinityTTL string `yaml:"session-affinity-ttl,omitempty" json:"session-affinity-ttl,omitempty"`
}

// OAuthModelAlias defines a model ID alias for a specific channel.
// It maps the upstream model name (Name) to the client-visible alias (Alias).
// When Fork is true, the alias is added as an additional model in listings while
// keeping the original model ID available.
type OAuthModelAlias struct {
	Name         string `yaml:"name" json:"name"`
	Alias        string `yaml:"alias" json:"alias"`
	Fork         bool   `yaml:"fork,omitempty" json:"fork,omitempty"`
	ForceMapping bool   `yaml:"force-mapping,omitempty" json:"force-mapping,omitempty"`
}

// CodexCustomModel defines a client-visible Codex OAuth model and the plan groups that may use it.
type CodexCustomModel struct {
	ID          string   `yaml:"id" json:"id"`
	DisplayName string   `yaml:"display-name,omitempty" json:"display-name,omitempty"`
	Groups      []string `yaml:"groups" json:"groups"`
}

// PayloadConfig defines default and override parameter rules applied to provider payloads.
type PayloadConfig struct {
	// Default defines rules that only set parameters when they are missing in the payload.
	Default []PayloadRule `yaml:"default" json:"default"`
	// DefaultRaw defines rules that set raw JSON values only when they are missing.
	DefaultRaw []PayloadRule `yaml:"default-raw" json:"default-raw"`
	// Override defines rules that always set parameters, overwriting any existing values.
	Override []PayloadRule `yaml:"override" json:"override"`
	// OverrideRaw defines rules that always set raw JSON values, overwriting any existing values.
	OverrideRaw []PayloadRule `yaml:"override-raw" json:"override-raw"`
	// Filter defines rules that remove parameters from the payload by JSON path.
	Filter []PayloadFilterRule `yaml:"filter" json:"filter"`
}

// PayloadFilterRule describes a rule to remove specific JSON paths from matching model payloads.
type PayloadFilterRule struct {
	// Models lists model entries with name pattern and protocol constraint.
	Models []PayloadModelRule `yaml:"models" json:"models"`
	// Params lists JSON paths (gjson/sjson syntax) to remove from the payload.
	Params []string `yaml:"params" json:"params"`
}

// PayloadRule describes a single rule targeting a list of models with parameter updates.
type PayloadRule struct {
	// Models lists model entries with name pattern and protocol constraint.
	Models []PayloadModelRule `yaml:"models" json:"models"`
	// Params maps JSON paths (gjson/sjson syntax) to values written into the payload.
	// For *-raw rules, values are treated as raw JSON fragments (strings are used as-is).
	Params map[string]any `yaml:"params" json:"params"`
}

// PayloadModelRule ties a model name pattern to a specific translator protocol.
type PayloadModelRule struct {
	// Name is the model name or wildcard pattern (e.g., "gpt-*", "*-5", "gemini-*-pro").
	Name string `yaml:"name" json:"name"`
	// Protocol restricts the rule to a specific translator format (e.g., "gemini", "responses").
	Protocol string `yaml:"protocol" json:"protocol"`
}

// CloakConfig configures request cloaking for non-Claude-Code clients.
// Cloaking disguises API requests to appear as originating from the official Claude Code CLI.
type CloakConfig struct {
	// Mode controls cloaking behavior: "auto" (default), "always", or "never".
	// - "auto": cloak only when client is not Claude Code (based on User-Agent)
	// - "always": always apply cloaking regardless of client
	// - "never": never apply cloaking
	Mode string `yaml:"mode,omitempty" json:"mode,omitempty"`

	// StrictMode controls how system prompts are handled when cloaking.
	// - false (default): prepend Claude Code prompt to user system messages
	// - true: strip all user system messages, keep only Claude Code prompt
	StrictMode bool `yaml:"strict-mode,omitempty" json:"strict-mode,omitempty"`

	// SensitiveWords is a list of words to obfuscate with zero-width characters.
	// This can help bypass certain content filters.
	SensitiveWords []string `yaml:"sensitive-words,omitempty" json:"sensitive-words,omitempty"`

	// CacheUserID controls whether Claude user_id values are cached per API key.
	// When false, a fresh random user_id is generated for every request.
	CacheUserID *bool `yaml:"cache-user-id,omitempty" json:"cache-user-id,omitempty"`
}

// ClaudeKey represents the configuration for a Claude API key,
// including the API key itself and an optional base URL for the API endpoint.
type ClaudeKey struct {
	// APIKey is the authentication key for accessing Claude API services.
	APIKey string `yaml:"api-key" json:"api-key"`

	// Priority controls selection preference when multiple credentials match.
	// Higher values are preferred; defaults to 0.
	Priority int `yaml:"priority,omitempty" json:"priority,omitempty"`

	// Prefix optionally namespaces models for this credential (e.g., "teamA/claude-sonnet-4").
	Prefix string `yaml:"prefix,omitempty" json:"prefix,omitempty"`

	// BaseURL is the base URL for the Claude API endpoint.
	// If empty, the default Claude API URL will be used.
	BaseURL string `yaml:"base-url" json:"base-url"`

	// ProxyURL overrides the global proxy setting for this API key if provided.
	ProxyURL string `yaml:"proxy-url" json:"proxy-url"`

	// Models defines upstream model names and aliases for request routing.
	Models []ClaudeModel `yaml:"models" json:"models"`

	// Headers optionally adds extra HTTP headers for requests sent with this key.
	Headers map[string]string `yaml:"headers,omitempty" json:"headers,omitempty"`

	// ExcludedModels lists model IDs that should be excluded for this provider.
	ExcludedModels []string `yaml:"excluded-models,omitempty" json:"excluded-models,omitempty"`

	// Cloak configures request cloaking for non-Claude-Code clients.
	Cloak *CloakConfig `yaml:"cloak,omitempty" json:"cloak,omitempty"`

	// ExperimentalCCHSigning enables opt-in final-body cch signing for cloaked
	// Claude /v1/messages requests. It is disabled by default so upstream seed
	// changes do not alter the proxy's legacy behavior.
	ExperimentalCCHSigning bool `yaml:"experimental-cch-signing,omitempty" json:"experimental-cch-signing,omitempty"`
}

func (k ClaudeKey) GetAPIKey() string  { return k.APIKey }
func (k ClaudeKey) GetBaseURL() string { return k.BaseURL }

// ClaudeModel describes a mapping between an alias and the actual upstream model name.
type ClaudeModel struct {
	// Name is the upstream model identifier used when issuing requests.
	Name string `yaml:"name" json:"name"`

	// Alias is the client-facing model name that maps to Name.
	Alias string `yaml:"alias" json:"alias"`

	// ForceMapping rewrites upstream response model fields back to Alias.
	ForceMapping bool `yaml:"force-mapping,omitempty" json:"force-mapping,omitempty"`
}

func (m ClaudeModel) GetName() string       { return m.Name }
func (m ClaudeModel) GetAlias() string      { return m.Alias }
func (m ClaudeModel) GetForceMapping() bool { return m.ForceMapping }

// CodexKey represents the configuration for a Codex API key,
// including the API key itself and an optional base URL for the API endpoint.
type CodexKey struct {
	// APIKey is the authentication key for accessing Codex API services.
	APIKey string `yaml:"api-key" json:"api-key"`

	// Priority controls selection preference when multiple credentials match.
	// Higher values are preferred; defaults to 0.
	Priority int `yaml:"priority,omitempty" json:"priority,omitempty"`

	// Prefix optionally namespaces models for this credential (e.g., "teamA/gpt-5-codex").
	Prefix string `yaml:"prefix,omitempty" json:"prefix,omitempty"`

	// BaseURL is the base URL for the Codex API endpoint.
	// If empty, the default Codex API URL will be used.
	BaseURL string `yaml:"base-url" json:"base-url"`

	// Websockets enables the Responses API websocket transport for this credential.
	Websockets bool `yaml:"websockets,omitempty" json:"websockets,omitempty"`

	// ProxyURL overrides the global proxy setting for this API key if provided.
	ProxyURL string `yaml:"proxy-url" json:"proxy-url"`

	// Models defines upstream model names and aliases for request routing.
	Models []CodexModel `yaml:"models" json:"models"`

	// Headers optionally adds extra HTTP headers for requests sent with this key.
	Headers map[string]string `yaml:"headers,omitempty" json:"headers,omitempty"`

	// ExcludedModels lists model IDs that should be excluded for this provider.
	ExcludedModels []string `yaml:"excluded-models,omitempty" json:"excluded-models,omitempty"`
}

func (k CodexKey) GetAPIKey() string  { return k.APIKey }
func (k CodexKey) GetBaseURL() string { return k.BaseURL }

// CodexModel describes a mapping between an alias and the actual upstream model name.
type CodexModel struct {
	// Name is the upstream model identifier used when issuing requests.
	Name string `yaml:"name" json:"name"`

	// Alias is the client-facing model name that maps to Name.
	Alias string `yaml:"alias" json:"alias"`

	// ForceMapping rewrites upstream response model fields back to Alias.
	ForceMapping bool `yaml:"force-mapping,omitempty" json:"force-mapping,omitempty"`
}

func (m CodexModel) GetName() string       { return m.Name }
func (m CodexModel) GetAlias() string      { return m.Alias }
func (m CodexModel) GetForceMapping() bool { return m.ForceMapping }

// GeminiKey represents the configuration for a Gemini API key,
// including optional overrides for upstream base URL, proxy routing, and headers.
type GeminiKey struct {
	// APIKey is the authentication key for accessing Gemini API services.
	APIKey string `yaml:"api-key" json:"api-key"`

	// Priority controls selection preference when multiple credentials match.
	// Higher values are preferred; defaults to 0.
	Priority int `yaml:"priority,omitempty" json:"priority,omitempty"`

	// Prefix optionally namespaces models for this credential (e.g., "teamA/gemini-3-pro-preview").
	Prefix string `yaml:"prefix,omitempty" json:"prefix,omitempty"`

	// BaseURL optionally overrides the Gemini API endpoint.
	BaseURL string `yaml:"base-url,omitempty" json:"base-url,omitempty"`

	// ProxyURL optionally overrides the global proxy for this API key.
	ProxyURL string `yaml:"proxy-url,omitempty" json:"proxy-url,omitempty"`

	// Models defines upstream model names and aliases for request routing.
	Models []GeminiModel `yaml:"models,omitempty" json:"models,omitempty"`

	// Headers optionally adds extra HTTP headers for requests sent with this key.
	Headers map[string]string `yaml:"headers,omitempty" json:"headers,omitempty"`

	// ExcludedModels lists model IDs that should be excluded for this provider.
	ExcludedModels []string `yaml:"excluded-models,omitempty" json:"excluded-models,omitempty"`
}

func (k GeminiKey) GetAPIKey() string  { return k.APIKey }
func (k GeminiKey) GetBaseURL() string { return k.BaseURL }

// GeminiModel describes a mapping between an alias and the actual upstream model name.
type GeminiModel struct {
	// Name is the upstream model identifier used when issuing requests.
	Name string `yaml:"name" json:"name"`

	// Alias is the client-facing model name that maps to Name.
	Alias string `yaml:"alias" json:"alias"`

	// ForceMapping rewrites upstream response model fields back to Alias.
	ForceMapping bool `yaml:"force-mapping,omitempty" json:"force-mapping,omitempty"`
}

func (m GeminiModel) GetName() string       { return m.Name }
func (m GeminiModel) GetAlias() string      { return m.Alias }
func (m GeminiModel) GetForceMapping() bool { return m.ForceMapping }

// OpenAICompatibility represents the configuration for OpenAI API compatibility
// with external providers, allowing model aliases to be routed through OpenAI API format.
type OpenAICompatibility struct {
	// Name is the identifier for this OpenAI compatibility configuration.
	Name string `yaml:"name" json:"name"`

	// Priority controls selection preference when multiple providers or credentials match.
	// Higher values are preferred; defaults to 0.
	Priority int `yaml:"priority,omitempty" json:"priority,omitempty"`

	// Disabled keeps the provider configuration persisted but prevents it from being registered.
	Disabled bool `yaml:"disabled,omitempty" json:"disabled,omitempty"`

	// Prefix optionally namespaces model aliases for this provider (e.g., "teamA/kimi-k2").
	Prefix string `yaml:"prefix,omitempty" json:"prefix,omitempty"`

	// BaseURL is the base URL for the external OpenAI-compatible API endpoint.
	BaseURL string `yaml:"base-url" json:"base-url"`

	// APIKeyEntries defines API keys with optional per-key proxy configuration.
	APIKeyEntries []OpenAICompatibilityAPIKey `yaml:"api-key-entries,omitempty" json:"api-key-entries,omitempty"`

	// Models defines the model configurations including aliases for routing.
	Models []OpenAICompatibilityModel `yaml:"models" json:"models"`

	// Headers optionally adds extra HTTP headers for requests sent to this provider.
	Headers map[string]string `yaml:"headers,omitempty" json:"headers,omitempty"`
}

// OpenAICompatibilityAPIKey represents an API key configuration with optional proxy setting.
type OpenAICompatibilityAPIKey struct {
	// APIKey is the authentication key for accessing the external API services.
	APIKey string `yaml:"api-key" json:"api-key"`

	// ProxyURL overrides the global proxy setting for this API key if provided.
	ProxyURL string `yaml:"proxy-url,omitempty" json:"proxy-url,omitempty"`
}

// OpenAICompatibilityModel represents a model configuration for OpenAI compatibility,
// including the actual model name and its alias for API routing.
type OpenAICompatibilityModel struct {
	// Name is the actual model name used by the external provider.
	Name string `yaml:"name" json:"name"`

	// Alias is the model name alias that clients will use to reference this model.
	Alias string `yaml:"alias" json:"alias"`

	// ForceMapping rewrites upstream response model fields back to Alias.
	ForceMapping bool `yaml:"force-mapping,omitempty" json:"force-mapping,omitempty"`

	// Thinking configures the thinking/reasoning capability for this model.
	// If nil, the model defaults to level-based reasoning with levels ["low", "medium", "high"].
	Thinking *registry.ThinkingSupport `yaml:"thinking,omitempty" json:"thinking,omitempty"`
}

func (m OpenAICompatibilityModel) GetName() string       { return m.Name }
func (m OpenAICompatibilityModel) GetAlias() string      { return m.Alias }
func (m OpenAICompatibilityModel) GetForceMapping() bool { return m.ForceMapping }

// LoadConfig reads a YAML configuration file from the given path,
// unmarshals it into a Config struct, applies environment variable overrides,
// and returns it.
//
// Parameters:
//   - configFile: The path to the YAML configuration file
//
// Returns:
//   - *Config: The loaded configuration
//   - error: An error if the configuration could not be loaded
func LoadConfig(configFile string) (*Config, error) {
	return LoadConfigOptional(configFile, false)
}

// LoadConfigOptional reads YAML from configFile.
// If optional is true and the file is missing, it returns an empty Config.
// If optional is true and the file is empty or invalid, it returns an empty Config.
func LoadConfigOptional(configFile string, optional bool) (*Config, error) {
	// Read the entire configuration file into memory.
	data, err := os.ReadFile(configFile)
	if err != nil {
		if optional {
			if os.IsNotExist(err) || errors.Is(err, syscall.EISDIR) {
				// Missing and optional: return empty config (cloud deploy standby).
				return &Config{}, nil
			}
		}
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	// In cloud deploy mode (optional=true), if file is empty or contains only whitespace, return empty config.
	if optional && len(data) == 0 {
		return &Config{}, nil
	}

	// Unmarshal the YAML data into the Config struct.
	var cfg Config
	// Set defaults before unmarshal so that absent keys keep defaults.
	cfg.Host = "" // Default empty: binds to all interfaces (IPv4 + IPv6)
	cfg.LoggingToFile = false
	cfg.LogsMaxTotalSizeMB = 0
	cfg.LogsRetentionDays = 0
	cfg.ErrorLogsMaxFiles = 10
	cfg.UsageStatisticsEnabled = false
	cfg.DisableCooling = false
	cfg.Pprof.Enable = false
	cfg.Pprof.Addr = DefaultPprofAddr
	cfg.RemoteManagement.PanelGitHubRepository = DefaultPanelGitHubRepository
	cfg.RequestBodyAudit.RejectOversize = true
	cfg.RequestBodyAudit.Error = RequestBodyAuditErrorConfig{
		StatusCode: DefaultRequestBodyAuditStatusCode,
		Message:    DefaultRequestBodyAuditMessage,
		Type:       DefaultRequestBodyAuditType,
		Code:       DefaultRequestBodyAuditCode,
	}
	cfg.DisabledImageGenerationToolAction = DisabledImageGenerationToolActionRemove
	cfg.DisabledImageGenerationToolError = DisabledImageGenerationToolErrorConfig{
		StatusCode: DefaultDisabledImageGenerationToolStatusCode,
		Message:    DefaultDisabledImageGenerationToolMessage,
		Type:       DefaultDisabledImageGenerationToolType,
		Code:       DefaultDisabledImageGenerationToolCode,
	}
	cfg.NonRetryableErrors = DefaultNonRetryableErrorRules()
	cfg.Images.CodexModel = "gpt-5.4"
	cfg.Images.ImageModel = "gpt-image-2"
	cfg.Images.ChatGPTWeb.UpstreamModel = DefaultChatGPTWebImageUpstreamModel
	cfg.Images.Native.Generations.Models = defaultNativeImageModels()
	cfg.Images.Native.Generations.UnsupportedModelStatusCode = http.StatusBadRequest
	cfg.Images.Native.Generations.UnsupportedModelMessage = "Native image generation is not enabled for model {model}"
	cfg.Images.Native.Edits.Models = defaultNativeImageModels()
	cfg.Images.Native.Edits.UnsupportedModelStatusCode = http.StatusBadRequest
	cfg.Images.Native.Edits.UnsupportedModelMessage = "Native image edit is not enabled for model {model}"
	cfg.CodexFingerprint.DefaultMode = DefaultCodexFingerprintMode
	cfg.CodexFingerprint.SessionIdentityPoolSize = DefaultCodexSessionIdentityPoolSize
	defaultImagesNAggregation := false
	cfg.Images.EnableNAggregation = &defaultImagesNAggregation
	cfg.Images.UnsupportedStatusCode = http.StatusBadRequest
	if lifecycleErr := validateChatGPTWebLifecycleOptionsYAML(data); lifecycleErr != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", lifecycleErr)
	}
	if err = yaml.Unmarshal(data, &cfg); err != nil {
		if optional {
			if accountInfoErr := validateChatGPTWebAccountInfoYAML(data); accountInfoErr != nil {
				return nil, fmt.Errorf("failed to parse config file: %w", accountInfoErr)
			}
			if sentinelErr := validateChatGPTWebSentinelYAML(data); sentinelErr != nil {
				return nil, fmt.Errorf("failed to parse config file: %w", sentinelErr)
			}
			// In cloud deploy mode, if YAML parsing fails, return empty config instead of error.
			return &Config{}, nil
		}
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}
	warnDeprecatedAmpConfig(data)
	warnDeprecatedGeminiCLIConfig(data)

	// NOTE: Startup legacy key migration is intentionally disabled.
	// Reason: avoid mutating config.yaml during server startup.
	// Re-enable the block below if automatic startup migration is needed again.
	// var legacy legacyConfigData
	// if errLegacy := yaml.Unmarshal(data, &legacy); errLegacy == nil {
	// 	if cfg.migrateLegacyGeminiKeys(legacy.LegacyGeminiKeys) {
	// 		cfg.legacyMigrationPending = true
	// 	}
	// 	if cfg.migrateLegacyOpenAICompatibilityKeys(legacy.OpenAICompat) {
	// 		cfg.legacyMigrationPending = true
	// 	}
	// }

	// Hash remote management key if plaintext is detected (nested)
	// We consider a value to be already hashed if it looks like a bcrypt hash ($2a$, $2b$, or $2y$ prefix).
	if cfg.RemoteManagement.SecretKey != "" && !looksLikeBcrypt(cfg.RemoteManagement.SecretKey) {
		hashed, errHash := hashSecret(cfg.RemoteManagement.SecretKey)
		if errHash != nil {
			return nil, fmt.Errorf("failed to hash remote management key: %w", errHash)
		}
		cfg.RemoteManagement.SecretKey = hashed

		// Persist the hashed value back to the config file to avoid re-hashing on next startup.
		// Preserve YAML comments and ordering; update only the nested key.
		_ = SaveConfigPreserveCommentsUpdateNestedScalar(configFile, []string{"remote-management", "secret-key"}, hashed)
	}

	cfg.RemoteManagement.PanelGitHubRepository = strings.TrimSpace(cfg.RemoteManagement.PanelGitHubRepository)
	if cfg.RemoteManagement.PanelGitHubRepository == "" {
		cfg.RemoteManagement.PanelGitHubRepository = DefaultPanelGitHubRepository
	}
	accessPath, errAccessPath := NormalizeManagementAccessPath(cfg.RemoteManagement.AccessPath)
	if errAccessPath != nil {
		return nil, fmt.Errorf("invalid remote-management.access-path: %w", errAccessPath)
	}
	cfg.RemoteManagement.AccessPath = accessPath
	if cfg.Codex.TurnStatePolicy, err = NormalizeCodexTurnStatePolicy(cfg.Codex.TurnStatePolicy); err != nil {
		return nil, err
	}
	if cfg.CodexFingerprint.DefaultMode, err = NormalizeCodexFingerprintMode(cfg.CodexFingerprint.DefaultMode); err != nil {
		return nil, err
	}
	if cfg.CodexFingerprint.SessionIdentityPoolSize < 1 || cfg.CodexFingerprint.SessionIdentityPoolSize > MaxCodexSessionIdentityPoolSize {
		return nil, fmt.Errorf("codex-fingerprint.session-identity-pool-size must be between 1 and %d", MaxCodexSessionIdentityPoolSize)
	}

	cfg.Images.CodexModel = strings.TrimSpace(cfg.Images.CodexModel)
	if cfg.Images.CodexModel == "" {
		cfg.Images.CodexModel = "gpt-5.4"
	}
	cfg.Images.ImageModel = strings.TrimSpace(cfg.Images.ImageModel)
	if cfg.Images.ImageModel == "" {
		cfg.Images.ImageModel = "gpt-image-2"
	}
	cfg.Images.ChatGPTWeb.UpstreamModel = cfg.Images.ChatGPTWeb.ResolvedUpstreamModel()
	if err = cfg.Images.ChatGPTWeb.Validate(); err != nil {
		return nil, err
	}
	if cfg.Images.EnableNAggregation == nil {
		enableNAggregation := false
		cfg.Images.EnableNAggregation = &enableNAggregation
	}
	if cfg.Images.EnableStreamFlush == nil {
		enableStreamFlush := true
		cfg.Images.EnableStreamFlush = &enableStreamFlush
	}
	if cfg.Images.UnsupportedStatusCode < http.StatusBadRequest || cfg.Images.UnsupportedStatusCode > 599 {
		cfg.Images.UnsupportedStatusCode = http.StatusBadRequest
	}
	normalizeNativeImageEndpointConfig(&cfg.Images.Native.Generations, "Native image generation is not enabled for model {model}")
	normalizeNativeImageEndpointConfig(&cfg.Images.Native.Edits, "Native image edit is not enabled for model {model}")
	if cfg.Streaming.StreamFlushIntervalMS < 0 {
		cfg.Streaming.StreamFlushIntervalMS = 0
	}
	if cfg.Streaming.StreamFlushMinBytes < 0 {
		cfg.Streaming.StreamFlushMinBytes = 0
	}
	if cfg.Images.StreamFlushIntervalMS < 0 {
		cfg.Images.StreamFlushIntervalMS = 0
	}
	if cfg.Images.StreamFlushMinBytes < 0 {
		cfg.Images.StreamFlushMinBytes = 0
	}

	cfg.Pprof.Addr = strings.TrimSpace(cfg.Pprof.Addr)
	if cfg.Pprof.Addr == "" {
		cfg.Pprof.Addr = DefaultPprofAddr
	}

	if cfg.LogsMaxTotalSizeMB < 0 {
		cfg.LogsMaxTotalSizeMB = 0
	}
	if cfg.LogsRetentionDays < 0 {
		cfg.LogsRetentionDays = 0
	}

	if cfg.ErrorLogsMaxFiles < 0 {
		cfg.ErrorLogsMaxFiles = 10
	}
	if cfg.UsageStatisticsPersistIntervalSeconds < 0 {
		cfg.UsageStatisticsPersistIntervalSeconds = 0
	}
	if cfg.UsageStatisticsDetailRetentionDays < 0 {
		cfg.UsageStatisticsDetailRetentionDays = 0
	}
	if cfg.UsageStatisticsMaxStorageMB < 0 {
		cfg.UsageStatisticsMaxStorageMB = 0
	}

	if cfg.UsagePricing, err = NormalizeUsagePricing(cfg.UsagePricing); err != nil {
		if optional {
			return &Config{}, nil
		}
		return nil, err
	}
	cfg.ChatGPTWeb.UsageCache.Path = strings.TrimSpace(cfg.ChatGPTWeb.UsageCache.Path)
	cfg.ChatGPTWeb.ImageUsage.AutoOutputQuality = strings.ToLower(strings.TrimSpace(cfg.ChatGPTWeb.ImageUsage.AutoOutputQuality))
	cfg.ChatGPTWeb.LoginProxy.URLTemplate = strings.TrimSpace(cfg.ChatGPTWeb.LoginProxy.URLTemplate)
	cfg.ChatGPTWeb.LoginProxy.PlaceholderCharset = strings.TrimSpace(cfg.ChatGPTWeb.LoginProxy.PlaceholderCharset)
	if err = cfg.ChatGPTWeb.Validate(); err != nil {
		return nil, err
	}

	if cfg.MaxRetryCredentials < 0 {
		cfg.MaxRetryCredentials = 0
	}
	if err = cfg.NormalizeRouting(); err != nil {
		if optional {
			return &Config{}, nil
		}
		return nil, err
	}
	if err = cfg.NormalizeProxyConfiguration(); err != nil {
		if optional {
			return &Config{}, nil
		}
		return nil, err
	}
	cfg.NoCooldownStatusCodes = NormalizeStatusCodes(cfg.NoCooldownStatusCodes)
	cfg.FixedErrorCooldowns = NormalizeFixedErrorCooldowns(cfg.FixedErrorCooldowns)
	if cfg.ErrorResponseRewrites, err = NormalizeErrorResponseRewrites(cfg.ErrorResponseRewrites); err != nil {
		if optional {
			return &Config{}, nil
		}
		return nil, err
	}
	cfg.NonRetryableErrors = NormalizeNonRetryableErrorRules(cfg.NonRetryableErrors)
	cfg.AuthModelExclusions = NormalizeAuthModelExclusionRules(cfg.AuthModelExclusions)
	if err = cfg.NormalizeDisabledImageGenerationTool(); err != nil {
		if optional {
			return &Config{}, nil
		}
		return nil, err
	}
	cfg.RequestBodyRelease = NormalizeRequestBodyRelease(cfg.RequestBodyRelease)
	cfg.NormalizeRequestBodyAudit()
	cfg.APIKeyGroups, err = NormalizeAPIKeyGroups(cfg.APIKeyGroups, cfg.APIKeys)
	if err != nil {
		return nil, err
	}

	// Sanitize Gemini API key configuration and migrate legacy entries.
	cfg.SanitizeGeminiKeys()

	// Sanitize native Interactions API key configuration.
	cfg.SanitizeInteractionsKeys()

	// Sanitize Vertex-compatible API keys.
	cfg.SanitizeVertexCompatKeys()

	// Sanitize Codex keys: drop entries without base-url
	cfg.SanitizeCodexKeys()

	// Sanitize Codex header defaults.
	cfg.SanitizeCodexHeaderDefaults()

	// Sanitize Codex custom OAuth models.
	cfg.SanitizeCodexCustomModels()

	// Sanitize Claude header defaults.
	cfg.SanitizeClaudeHeaderDefaults()

	// Sanitize Claude key headers
	cfg.SanitizeClaudeKeys()

	// Sanitize OpenAI compatibility providers: drop entries without base-url
	cfg.SanitizeOpenAICompatibility()

	// Normalize OAuth provider model exclusion map.
	cfg.OAuthExcludedModels = NormalizeOAuthExcludedModels(cfg.OAuthExcludedModels)

	// Normalize global OAuth model name aliases.
	cfg.SanitizeOAuthModelAlias()

	// Validate raw payload rules and drop invalid entries.
	cfg.SanitizePayloadRules()

	// NOTE: Legacy migration persistence is intentionally disabled together with
	// startup legacy migration to keep startup read-only for config.yaml.
	// Re-enable the block below if automatic startup migration is needed again.
	// if cfg.legacyMigrationPending {
	// 	fmt.Println("Detected legacy configuration keys, attempting to persist the normalized config...")
	// 	if !optional && configFile != "" {
	// 		if err := SaveConfigPreserveComments(configFile, &cfg); err != nil {
	// 			return nil, fmt.Errorf("failed to persist migrated legacy config: %w", err)
	// 		}
	// 		fmt.Println("Legacy configuration normalized and persisted.")
	// 	} else {
	// 		fmt.Println("Legacy configuration normalized in memory; persistence skipped.")
	// 	}
	// }

	// Return the populated configuration struct.
	return &cfg, nil
}

// UsageStatisticsPersistence reports whether automatic Usage snapshot restore
// and writes are enabled. An omitted setting preserves the legacy enabled behavior.
func (cfg *Config) UsageStatisticsPersistence() bool {
	return cfg == nil || cfg.UsageStatisticsPersistenceEnabled == nil || *cfg.UsageStatisticsPersistenceEnabled
}

func validateChatGPTWebAccountInfoYAML(data []byte) error {
	var document yaml.Node
	if err := yaml.Unmarshal(data, &document); err != nil {
		return nil
	}
	var envelope struct {
		ChatGPTWeb yaml.Node `yaml:"chatgpt-web"`
	}
	if err := document.Decode(&envelope); err != nil {
		return nil
	}
	if envelope.ChatGPTWeb.Kind != yaml.MappingNode && envelope.ChatGPTWeb.Kind != yaml.AliasNode {
		return nil
	}
	var section struct {
		AccountInfo yaml.Node `yaml:"account-info"`
	}
	if err := envelope.ChatGPTWeb.Decode(&section); err != nil || section.AccountInfo.Kind == 0 {
		return nil
	}
	var accountInfo ChatGPTWebAccountInfoConfig
	if err := section.AccountInfo.Decode(&accountInfo); err != nil {
		return err
	}
	return accountInfo.Validate()
}

func validateChatGPTWebLifecycleOptionsYAML(data []byte) error {
	var document yaml.Node
	if err := yaml.Unmarshal(data, &document); err != nil {
		return nil
	}
	var envelope struct {
		ChatGPTWeb yaml.Node `yaml:"chatgpt-web"`
	}
	if err := document.Decode(&envelope); err != nil {
		return nil
	}
	if envelope.ChatGPTWeb.Kind != yaml.MappingNode && envelope.ChatGPTWeb.Kind != yaml.AliasNode {
		return nil
	}
	var section struct {
		InvalidPasskeyResponseAsDead yaml.Node `yaml:"invalid-passkey-response-as-dead"`
		API798AutoLoginEnabled       yaml.Node `yaml:"api798-auto-login-enabled"`
	}
	if err := envelope.ChatGPTWeb.Decode(&section); err != nil {
		return nil
	}
	for _, field := range []struct {
		name string
		node yaml.Node
	}{
		{name: "invalid-passkey-response-as-dead", node: section.InvalidPasskeyResponseAsDead},
		{name: "api798-auto-login-enabled", node: section.API798AutoLoginEnabled},
	} {
		if field.node.Kind == 0 {
			continue
		}
		if field.node.Tag == "!!null" {
			return fmt.Errorf("chatgpt-web.%s must not be null", field.name)
		}
		var enabled bool
		if err := field.node.Decode(&enabled); err != nil {
			return fmt.Errorf("chatgpt-web.%s must be a boolean: %w", field.name, err)
		}
	}
	return nil
}

func validateChatGPTWebSentinelYAML(data []byte) error {
	var document yaml.Node
	if err := yaml.Unmarshal(data, &document); err != nil {
		return nil
	}
	var envelope struct {
		ChatGPTWeb yaml.Node `yaml:"chatgpt-web"`
	}
	if err := document.Decode(&envelope); err != nil {
		return nil
	}
	if envelope.ChatGPTWeb.Kind != yaml.MappingNode && envelope.ChatGPTWeb.Kind != yaml.AliasNode {
		return nil
	}
	var section struct {
		Sentinel yaml.Node `yaml:"sentinel"`
	}
	if err := envelope.ChatGPTWeb.Decode(&section); err != nil || section.Sentinel.Kind == 0 {
		return nil
	}
	var sentinel ChatGPTWebSentinelConfig
	if err := section.Sentinel.Decode(&sentinel); err != nil {
		return err
	}
	return sentinel.Validate()
}

func defaultNativeImageModels() []string {
	return []string{"gpt-image-2", "gpt-image-1.5"}
}

func normalizeNativeImageEndpointConfig(endpoint *NativeImageEndpointConfig, defaultMessage string) {
	if endpoint == nil {
		return
	}
	endpoint.Models = normalizeStringList(endpoint.Models)
	if len(endpoint.Models) == 0 {
		endpoint.Models = defaultNativeImageModels()
	}
	endpoint.ParamRules = normalizeStringList(endpoint.ParamRules)
	if endpoint.UnsupportedModelStatusCode < http.StatusBadRequest || endpoint.UnsupportedModelStatusCode > 599 {
		endpoint.UnsupportedModelStatusCode = http.StatusBadRequest
	}
	endpoint.UnsupportedModelMessage = strings.TrimSpace(endpoint.UnsupportedModelMessage)
	if endpoint.UnsupportedModelMessage == "" {
		endpoint.UnsupportedModelMessage = defaultMessage
	}
}

func normalizeStringList(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	out := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		key := strings.ToLower(value)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, value)
	}
	return out
}

// SanitizePayloadRules validates raw JSON payload rule params and drops invalid rules.
func (cfg *Config) SanitizePayloadRules() {
	if cfg == nil {
		return
	}
	cfg.Payload.DefaultRaw = sanitizePayloadRawRules(cfg.Payload.DefaultRaw, "default-raw")
	cfg.Payload.OverrideRaw = sanitizePayloadRawRules(cfg.Payload.OverrideRaw, "override-raw")
}

func sanitizePayloadRawRules(rules []PayloadRule, section string) []PayloadRule {
	if len(rules) == 0 {
		return rules
	}
	out := make([]PayloadRule, 0, len(rules))
	for i := range rules {
		rule := rules[i]
		if len(rule.Params) == 0 {
			continue
		}
		invalid := false
		for path, value := range rule.Params {
			raw, ok := payloadRawString(value)
			if !ok {
				continue
			}
			trimmed := bytes.TrimSpace(raw)
			if len(trimmed) == 0 || !json.Valid(trimmed) {
				log.WithFields(log.Fields{
					"section":    section,
					"rule_index": i + 1,
					"param":      path,
				}).Warn("payload rule dropped: invalid raw JSON")
				invalid = true
				break
			}
		}
		if invalid {
			continue
		}
		out = append(out, rule)
	}
	return out
}

func payloadRawString(value any) ([]byte, bool) {
	switch typed := value.(type) {
	case string:
		return []byte(typed), true
	case []byte:
		return typed, true
	default:
		return nil, false
	}
}

// SanitizeCodexHeaderDefaults trims surrounding whitespace from configured
// Codex software identity and feature header values.
func (cfg *Config) SanitizeCodexHeaderDefaults() {
	if cfg == nil {
		return
	}
	cfg.CodexHeaderDefaults.UserAgent = strings.TrimSpace(cfg.CodexHeaderDefaults.UserAgent)
	cfg.CodexHeaderDefaults.Originator = strings.TrimSpace(cfg.CodexHeaderDefaults.Originator)
	cfg.CodexHeaderDefaults.BetaFeatures = strings.TrimSpace(cfg.CodexHeaderDefaults.BetaFeatures)
}

// SanitizeCodexCustomModels normalizes user-defined Codex OAuth model entries.
func (cfg *Config) SanitizeCodexCustomModels() {
	if cfg == nil || len(cfg.CodexCustomModels) == 0 {
		return
	}
	out := make([]CodexCustomModel, 0, len(cfg.CodexCustomModels))
	byID := make(map[string]int, len(cfg.CodexCustomModels))
	for _, entry := range cfg.CodexCustomModels {
		id := strings.TrimSpace(entry.ID)
		groups := normalizeCodexCustomModelGroups(entry.Groups)
		if id == "" || len(groups) == 0 {
			continue
		}
		displayName := strings.TrimSpace(entry.DisplayName)
		if displayName == "" {
			displayName = id
		}
		key := strings.ToLower(id)
		if idx, ok := byID[key]; ok {
			out[idx].Groups = mergeCodexCustomModelGroups(out[idx].Groups, groups)
			if out[idx].DisplayName == out[idx].ID && displayName != id {
				out[idx].DisplayName = displayName
			}
			continue
		}
		byID[key] = len(out)
		out = append(out, CodexCustomModel{
			ID:          id,
			DisplayName: displayName,
			Groups:      groups,
		})
	}
	cfg.CodexCustomModels = out
	if len(cfg.CodexCustomModels) == 0 {
		cfg.CodexCustomModels = nil
	}
}

// SanitizeClaudeHeaderDefaults trims surrounding whitespace from the
// configured Claude fingerprint baseline values.
func (cfg *Config) SanitizeClaudeHeaderDefaults() {
	if cfg == nil {
		return
	}
	cfg.ClaudeHeaderDefaults.UserAgent = strings.TrimSpace(cfg.ClaudeHeaderDefaults.UserAgent)
	cfg.ClaudeHeaderDefaults.PackageVersion = strings.TrimSpace(cfg.ClaudeHeaderDefaults.PackageVersion)
	cfg.ClaudeHeaderDefaults.RuntimeVersion = strings.TrimSpace(cfg.ClaudeHeaderDefaults.RuntimeVersion)
	cfg.ClaudeHeaderDefaults.OS = strings.TrimSpace(cfg.ClaudeHeaderDefaults.OS)
	cfg.ClaudeHeaderDefaults.Arch = strings.TrimSpace(cfg.ClaudeHeaderDefaults.Arch)
	cfg.ClaudeHeaderDefaults.Timeout = strings.TrimSpace(cfg.ClaudeHeaderDefaults.Timeout)
}

// SanitizeOAuthModelAlias normalizes and deduplicates global OAuth model name aliases.
// It trims whitespace, normalizes channel keys to lower-case, drops empty entries,
// allows multiple aliases per upstream name, and ensures aliases are unique within each channel.
func (cfg *Config) SanitizeOAuthModelAlias() {
	if cfg == nil || len(cfg.OAuthModelAlias) == 0 {
		return
	}
	out := make(map[string][]OAuthModelAlias, len(cfg.OAuthModelAlias))
	for rawChannel, aliases := range cfg.OAuthModelAlias {
		channel := strings.ToLower(strings.TrimSpace(rawChannel))
		if channel == "" || channel == "gemini-cli" || len(aliases) == 0 {
			continue
		}
		seenAlias := make(map[string]struct{}, len(aliases))
		clean := make([]OAuthModelAlias, 0, len(aliases))
		for _, entry := range aliases {
			name := strings.TrimSpace(entry.Name)
			alias := strings.TrimSpace(entry.Alias)
			if name == "" || alias == "" {
				continue
			}
			if strings.EqualFold(name, alias) {
				continue
			}
			aliasKey := strings.ToLower(alias)
			if _, ok := seenAlias[aliasKey]; ok {
				continue
			}
			seenAlias[aliasKey] = struct{}{}
			clean = append(clean, OAuthModelAlias{Name: name, Alias: alias, Fork: entry.Fork, ForceMapping: entry.ForceMapping})
		}
		if len(clean) > 0 {
			out[channel] = clean
		}
	}
	cfg.OAuthModelAlias = out
}

func normalizeCodexCustomModelGroups(groups []string) []string {
	if len(groups) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(groups))
	for _, raw := range groups {
		group := strings.ToLower(strings.TrimSpace(raw))
		if !isCodexCustomModelGroup(group) {
			continue
		}
		seen[group] = struct{}{}
	}
	if len(seen) == 0 {
		return nil
	}
	ordered := []string{"free", "plus", "pro", "team", "business", "go"}
	out := make([]string, 0, len(seen))
	for _, group := range ordered {
		if _, ok := seen[group]; ok {
			out = append(out, group)
		}
	}
	return out
}

func mergeCodexCustomModelGroups(a, b []string) []string {
	merged := make([]string, 0, len(a)+len(b))
	merged = append(merged, a...)
	merged = append(merged, b...)
	return normalizeCodexCustomModelGroups(merged)
}

func isCodexCustomModelGroup(group string) bool {
	switch group {
	case "free", "plus", "pro", "team", "business", "go":
		return true
	default:
		return false
	}
}

// NormalizeStatusCodes keeps valid HTTP status codes in first-seen order and removes duplicates.
func NormalizeStatusCodes(codes []int) []int {
	if len(codes) == 0 {
		return nil
	}
	seen := make(map[int]struct{}, len(codes))
	out := make([]int, 0, len(codes))
	for _, code := range codes {
		if code < 100 || code > 599 {
			continue
		}
		if _, ok := seen[code]; ok {
			continue
		}
		seen[code] = struct{}{}
		out = append(out, code)
	}
	return out
}

// NormalizeFixedErrorCooldowns keeps valid custom cooldown rules in first-seen order.
func NormalizeFixedErrorCooldowns(rules []FixedErrorCooldownRule) []FixedErrorCooldownRule {
	if len(rules) == 0 {
		return nil
	}
	out := make([]FixedErrorCooldownRule, 0, len(rules))
	for _, rule := range rules {
		statusCode := rule.StatusCode
		if statusCode != 0 && (statusCode < 100 || statusCode > 599) {
			continue
		}
		message := strings.TrimSpace(rule.MessageContains)
		if statusCode == 0 && message == "" {
			continue
		}
		if rule.CooldownSeconds <= 0 {
			continue
		}
		scope := strings.ToLower(strings.TrimSpace(rule.Scope))
		switch scope {
		case "", "model":
			scope = "model"
		case "auth":
		default:
			continue
		}
		out = append(out, FixedErrorCooldownRule{
			StatusCode:      statusCode,
			MessageContains: message,
			CooldownSeconds: rule.CooldownSeconds,
			Scope:           scope,
		})
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// NormalizeErrorResponseRewrites validates final client-facing execution error projections.
func NormalizeErrorResponseRewrites(rules []ErrorResponseRewriteRule) ([]ErrorResponseRewriteRule, error) {
	if len(rules) == 0 {
		return nil, nil
	}
	out := make([]ErrorResponseRewriteRule, 0, len(rules))
	for index, rule := range rules {
		statusCode := rule.StatusCode
		if statusCode != 0 && (statusCode < 100 || statusCode > 599) {
			return nil, fmt.Errorf("error-response-rewrites[%d].status-code must be between 100 and 599", index)
		}
		message := strings.TrimSpace(rule.MessageContains)
		if statusCode == 0 && message == "" {
			return nil, fmt.Errorf("error-response-rewrites[%d] requires status-code or message-contains", index)
		}
		responseStatusCode := rule.ResponseStatusCode
		if responseStatusCode != 0 && (responseStatusCode < 400 || responseStatusCode > 599) {
			return nil, fmt.Errorf("error-response-rewrites[%d].response-status-code must be between 400 and 599", index)
		}
		if responseStatusCode == 0 && rule.ResponseBody == nil {
			return nil, fmt.Errorf("error-response-rewrites[%d] requires response-status-code or response-body", index)
		}

		var responseBody *map[string]any
		if rule.ResponseBody != nil {
			if *rule.ResponseBody == nil {
				return nil, fmt.Errorf("error-response-rewrites[%d].response-body must be a JSON object", index)
			}
			_, errMarshal := json.Marshal(*rule.ResponseBody)
			if errMarshal != nil {
				return nil, fmt.Errorf("error-response-rewrites[%d].response-body must be a JSON object: %w", index, errMarshal)
			}
			normalizedBody := cloneErrorResponseBody(*rule.ResponseBody)
			responseBody = &normalizedBody
		}

		out = append(out, ErrorResponseRewriteRule{
			StatusCode:         statusCode,
			MessageContains:    message,
			ResponseStatusCode: responseStatusCode,
			ResponseBody:       responseBody,
		})
	}
	return out, nil
}

func cloneErrorResponseBody(src map[string]any) map[string]any {
	dst := make(map[string]any, len(src))
	for key, value := range src {
		cloned := cloneErrorResponseValue(reflect.ValueOf(value))
		if cloned.IsValid() {
			dst[key] = cloned.Interface()
		} else {
			dst[key] = nil
		}
	}
	return dst
}

func cloneErrorResponseValue(value reflect.Value) reflect.Value {
	if !value.IsValid() {
		return reflect.Value{}
	}
	switch value.Kind() {
	case reflect.Interface:
		if value.IsNil() {
			return reflect.Zero(value.Type())
		}
		cloned := cloneErrorResponseValue(value.Elem())
		out := reflect.New(value.Type()).Elem()
		out.Set(cloned)
		return out
	case reflect.Pointer:
		if value.IsNil() {
			return reflect.Zero(value.Type())
		}
		out := reflect.New(value.Type().Elem())
		out.Elem().Set(cloneErrorResponseValue(value.Elem()))
		return out
	case reflect.Map:
		if value.IsNil() {
			return reflect.Zero(value.Type())
		}
		out := reflect.MakeMapWithSize(value.Type(), value.Len())
		iter := value.MapRange()
		for iter.Next() {
			out.SetMapIndex(cloneErrorResponseValue(iter.Key()), cloneErrorResponseValue(iter.Value()))
		}
		return out
	case reflect.Slice:
		if value.IsNil() {
			return reflect.Zero(value.Type())
		}
		out := reflect.MakeSlice(value.Type(), value.Len(), value.Len())
		for index := 0; index < value.Len(); index++ {
			out.Index(index).Set(cloneErrorResponseValue(value.Index(index)))
		}
		return out
	case reflect.Array:
		out := reflect.New(value.Type()).Elem()
		for index := 0; index < value.Len(); index++ {
			out.Index(index).Set(cloneErrorResponseValue(value.Index(index)))
		}
		return out
	default:
		return value
	}
}

// DefaultNonRetryableErrorRules returns stable upstream request errors that should not be retried by default.
func DefaultNonRetryableErrorRules() []NonRetryableErrorRule {
	return []NonRetryableErrorRule{
		{StatusCode: http.StatusBadRequest, Type: "image_generation_user_error", Code: "invalid_value"},
		{StatusCode: http.StatusBadRequest, Type: "image_generation_user_error", Code: "moderation_blocked"},
	}
}

// NormalizeNonRetryableErrorRules keeps valid non-retryable error rules in first-seen order.
func NormalizeNonRetryableErrorRules(rules []NonRetryableErrorRule) []NonRetryableErrorRule {
	if rules == nil {
		return nil
	}
	out := make([]NonRetryableErrorRule, 0, len(rules))
	seen := make(map[string]struct{}, len(rules))
	for _, rule := range rules {
		statusCode := rule.StatusCode
		if statusCode != 0 && (statusCode < 100 || statusCode > 599) {
			continue
		}
		errType := strings.ToLower(strings.TrimSpace(rule.Type))
		code := strings.ToLower(strings.TrimSpace(rule.Code))
		message := strings.ToLower(strings.TrimSpace(rule.MessageContains))
		if statusCode == 0 && errType == "" && code == "" && message == "" {
			continue
		}
		key := fmt.Sprintf("%d\x00%s\x00%s\x00%s", statusCode, errType, code, message)
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, NonRetryableErrorRule{
			StatusCode:      statusCode,
			Type:            errType,
			Code:            code,
			MessageContains: message,
		})
	}
	return out
}

// NormalizeAuthModelExclusionRules keeps active exclusion rules with at least one matcher.
func NormalizeAuthModelExclusionRules(rules []AuthModelExclusionRule) []AuthModelExclusionRule {
	if len(rules) == 0 {
		return nil
	}
	out := make([]AuthModelExclusionRule, 0, len(rules))
	seen := make(map[string]struct{}, len(rules))
	for _, rule := range rules {
		models := NormalizeExcludedModels(rule.Models)
		if len(models) == 0 && !rule.DisableImageGeneration {
			continue
		}
		providers := normalizeStringListLower(rule.Providers)
		priorities := normalizeIntList(rule.Priorities)
		keywords := normalizeStringListLower(rule.KeywordContains)
		if len(providers) == 0 && len(priorities) == 0 && len(keywords) == 0 {
			continue
		}
		key := strings.Join(models, ",") + "\x00" + strings.Join(providers, ",") + "\x00" + intsSignature(priorities) + "\x00" + strings.Join(keywords, ",") + fmt.Sprintf("\x00%t", rule.DisableImageGeneration)
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, AuthModelExclusionRule{
			Models:                 models,
			Providers:              providers,
			Priorities:             priorities,
			KeywordContains:        keywords,
			DisableImageGeneration: rule.DisableImageGeneration,
		})
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// NormalizeDisabledImageGenerationToolAction validates the configured disabled image tool behavior.
func NormalizeDisabledImageGenerationToolAction(action string) (string, bool) {
	action = strings.ToLower(strings.TrimSpace(action))
	if action == "" {
		return DisabledImageGenerationToolActionRemove, true
	}
	switch action {
	case DisabledImageGenerationToolActionRemove, DisabledImageGenerationToolActionError:
		return action, true
	default:
		return "", false
	}
}

// NormalizeDisabledImageGenerationToolError fills defaults for disabled image tool error responses.
func NormalizeDisabledImageGenerationToolError(in DisabledImageGenerationToolErrorConfig) DisabledImageGenerationToolErrorConfig {
	out := in
	if out.StatusCode < 100 || out.StatusCode > 599 {
		out.StatusCode = DefaultDisabledImageGenerationToolStatusCode
	}
	out.Message = strings.TrimSpace(out.Message)
	if out.Message == "" {
		out.Message = DefaultDisabledImageGenerationToolMessage
	}
	out.Type = strings.TrimSpace(out.Type)
	if out.Type == "" {
		out.Type = DefaultDisabledImageGenerationToolType
	}
	out.Code = strings.TrimSpace(out.Code)
	if out.Code == "" {
		out.Code = DefaultDisabledImageGenerationToolCode
	}
	return out
}

// NormalizeDisabledImageGenerationTool canonicalizes disabled image tool settings.
func (cfg *Config) NormalizeDisabledImageGenerationTool() error {
	if cfg == nil {
		return nil
	}
	action, ok := NormalizeDisabledImageGenerationToolAction(cfg.DisabledImageGenerationToolAction)
	if !ok {
		return fmt.Errorf("invalid disabled-image-generation-tool-action %q: must be remove or error", cfg.DisabledImageGenerationToolAction)
	}
	cfg.DisabledImageGenerationToolAction = action
	cfg.DisabledImageGenerationToolError = NormalizeDisabledImageGenerationToolError(cfg.DisabledImageGenerationToolError)
	return nil
}

func normalizeStringListLower(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.ToLower(strings.TrimSpace(value))
		if value == "" {
			continue
		}
		if _, exists := seen[value]; exists {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func normalizeIntList(values []int) []int {
	if len(values) == 0 {
		return nil
	}
	seen := make(map[int]struct{}, len(values))
	out := make([]int, 0, len(values))
	for _, value := range values {
		if _, exists := seen[value]; exists {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}

func intsSignature(values []int) string {
	if len(values) == 0 {
		return ""
	}
	parts := make([]string, 0, len(values))
	for _, value := range values {
		parts = append(parts, fmt.Sprintf("%d", value))
	}
	return strings.Join(parts, ",")
}

// NormalizeRequestBodyRelease clamps request body release settings.
func NormalizeRequestBodyRelease(in RequestBodyReleaseConfig) RequestBodyReleaseConfig {
	out := in
	if out.AfterSeconds < 0 {
		out.AfterSeconds = 0
	}
	if out.MinBodyBytes < 0 {
		out.MinBodyBytes = 0
	}
	return out
}

// NormalizeRequestBodyAudit canonicalizes request body audit settings and compiles byte keywords.
func (cfg *Config) NormalizeRequestBodyAudit() {
	if cfg == nil {
		return
	}
	cfg.RequestBodyAudit = NormalizeRequestBodyAudit(cfg.RequestBodyAudit)
}

// NormalizeRequestBodyAudit canonicalizes request body audit settings and compiles byte keywords.
func NormalizeRequestBodyAudit(in RequestBodyAuditConfig) RequestBodyAuditConfig {
	out := in
	out.Keywords = normalizeRequestBodyAuditKeywords(in.Keywords)
	out.KeywordsBase64 = normalizeRequestBodyAuditBase64Keywords(in.KeywordsBase64)
	if out.MaxBodyBytes < 0 {
		out.MaxBodyBytes = 0
	}
	out.Error = NormalizeRequestBodyAuditError(out.Error)
	out.compiledKeywords = compileRequestBodyAuditKeywords(out.Keywords, out.KeywordsBase64, out.CaseSensitive)
	return out
}

// NormalizeRequestBodyAuditError fills safe defaults for a request body audit error response.
func NormalizeRequestBodyAuditError(in RequestBodyAuditErrorConfig) RequestBodyAuditErrorConfig {
	out := in
	if out.StatusCode < 100 || out.StatusCode > 599 {
		out.StatusCode = DefaultRequestBodyAuditStatusCode
	}
	out.Message = strings.TrimSpace(out.Message)
	if out.Message == "" {
		out.Message = DefaultRequestBodyAuditMessage
	}
	out.Type = strings.TrimSpace(out.Type)
	if out.Type == "" {
		out.Type = DefaultRequestBodyAuditType
	}
	out.Code = strings.TrimSpace(out.Code)
	if out.Code == "" {
		out.Code = DefaultRequestBodyAuditCode
	}
	return out
}

// CompiledRequestBodyAuditKeywords returns byte keywords ready for request-body scanning.
func CompiledRequestBodyAuditKeywords(cfg RequestBodyAuditConfig) [][]byte {
	if len(cfg.compiledKeywords) > 0 {
		return cloneByteSlices(cfg.compiledKeywords)
	}
	normalized := NormalizeRequestBodyAudit(cfg)
	return cloneByteSlices(normalized.compiledKeywords)
}

func normalizeRequestBodyAuditKeywords(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, raw := range values {
		value := strings.TrimSpace(raw)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}

func normalizeRequestBodyAuditBase64Keywords(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, raw := range values {
		value := strings.TrimSpace(raw)
		if value == "" {
			continue
		}
		if _, err := base64.StdEncoding.DecodeString(value); err != nil {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}

func compileRequestBodyAuditKeywords(keywords []string, base64Keywords []string, caseSensitive bool) [][]byte {
	total := len(keywords) + len(base64Keywords)
	if total == 0 {
		return nil
	}
	out := make([][]byte, 0, total)
	for _, keyword := range keywords {
		data := []byte(keyword)
		if !caseSensitive {
			data = bytes.ToLower(data)
		}
		if len(data) > 0 {
			out = append(out, data)
		}
	}
	for _, encoded := range base64Keywords {
		data, err := base64.StdEncoding.DecodeString(encoded)
		if err != nil || len(data) == 0 {
			continue
		}
		if !caseSensitive {
			data = bytes.ToLower(data)
		}
		out = append(out, data)
	}
	return out
}

func cloneByteSlices(values [][]byte) [][]byte {
	if len(values) == 0 {
		return nil
	}
	out := make([][]byte, 0, len(values))
	for _, value := range values {
		if len(value) == 0 {
			continue
		}
		out = append(out, bytes.Clone(value))
	}
	return out
}

// NormalizeRoutingStrategy returns the canonical routing strategy name.
func NormalizeRoutingStrategy(strategy string) (string, bool) {
	normalized := strings.ToLower(strings.TrimSpace(strategy))
	switch normalized {
	case "", "round-robin", "roundrobin", "rr":
		return "round-robin", true
	case "fill-first", "fillfirst", "ff":
		return "fill-first", true
	case "random", "rand", "r":
		return "random", true
	default:
		return "", false
	}
}

// NormalizeFillFirstRange returns a valid fill-first grouping size.
func NormalizeFillFirstRange(value int) int {
	if value < 1 {
		return 1
	}
	return value
}

// NormalizeFillFirstPerAuthRPM returns a valid per-auth RPM fill limit.
func NormalizeFillFirstPerAuthRPM(value int) int {
	if value < 0 {
		return 0
	}
	return value
}

// NormalizePerAuthRequestLimit returns a valid generic per-auth request limit.
func NormalizePerAuthRequestLimit(value int) int {
	if value < 0 {
		return 0
	}
	return value
}

// NormalizePerAuthRequestWindowMinutes returns a valid fixed-window size.
func NormalizePerAuthRequestWindowMinutes(value int) int {
	if value < 1 {
		return 1
	}
	maxWindowMinutes := int(^uint(0)>>1) / 60
	maxDurationMinutes := int(time.Duration(1<<63-1) / time.Minute)
	if maxWindowMinutes > maxDurationMinutes {
		maxWindowMinutes = maxDurationMinutes
	}
	if value > maxWindowMinutes {
		return maxWindowMinutes
	}
	return value
}

// NormalizeRoutingPlanType canonicalizes subscription values used by routing rules.
func NormalizeRoutingPlanType(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	if value == "" {
		return ""
	}
	compact := strings.NewReplacer("-", "", "_", "", " ", "").Replace(value)
	switch compact {
	case "chatgptfreeplan":
		return "free"
	case "chatgptplusplan":
		return "plus"
	case "chatgptproplan":
		return "pro"
	case "chatgptteamplan", "chatgptbusinessplan", "selfservebusiness", "selfservebusinessusagebased":
		return "team"
	case "chatgptenterpriseplan":
		return "enterprise"
	default:
		return value
	}
}

func normalizeRoutingPlanTypes(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		normalized := NormalizeRoutingPlanType(value)
		if normalized == "" {
			continue
		}
		if _, exists := seen[normalized]; exists {
			continue
		}
		seen[normalized] = struct{}{}
		out = append(out, normalized)
	}
	return out
}

func routingProviderScopesOverlap(left, right []string) bool {
	if len(left) == 0 || len(right) == 0 {
		return true
	}
	rightSet := make(map[string]struct{}, len(right))
	for _, provider := range right {
		rightSet[provider] = struct{}{}
	}
	for _, provider := range left {
		if _, ok := rightSet[provider]; ok {
			return true
		}
	}
	return false
}

func normalizeRoutingSubscriptionOverrides(priorityIndex, priority int, overrides []RoutingSubscriptionOverride) ([]RoutingSubscriptionOverride, error) {
	if len(overrides) == 0 {
		return nil, nil
	}
	out := make([]RoutingSubscriptionOverride, 0, len(overrides))
	for index, override := range overrides {
		providers := normalizeStringListLower(override.Providers)
		planTypes := normalizeRoutingPlanTypes(override.PlanTypes)
		if len(planTypes) == 0 {
			return nil, fmt.Errorf("routing.priority-overrides[%d] (priority %d).subscription-overrides[%d].plan-types: at least one plan type is required", priorityIndex, priority, index)
		}
		if override.PerAuthRequestLimit == nil && override.PerAuthRequestWindowMinutes == nil {
			return nil, fmt.Errorf("routing.priority-overrides[%d] (priority %d).subscription-overrides[%d]: at least one request limit field is required", priorityIndex, priority, index)
		}

		var perAuthRequestLimit *int
		if override.PerAuthRequestLimit != nil {
			value := NormalizePerAuthRequestLimit(*override.PerAuthRequestLimit)
			perAuthRequestLimit = &value
		}
		var perAuthRequestWindowMinutes *int
		if override.PerAuthRequestWindowMinutes != nil {
			value := NormalizePerAuthRequestWindowMinutes(*override.PerAuthRequestWindowMinutes)
			perAuthRequestWindowMinutes = &value
		}

		for previousIndex, previous := range out {
			if !routingProviderScopesOverlap(previous.Providers, providers) {
				continue
			}
			previousPlans := make(map[string]struct{}, len(previous.PlanTypes))
			for _, planType := range previous.PlanTypes {
				previousPlans[planType] = struct{}{}
			}
			for _, planType := range planTypes {
				if _, duplicate := previousPlans[planType]; duplicate {
					return nil, fmt.Errorf("routing.priority-overrides[%d] (priority %d).subscription-overrides: plan type %q has overlapping provider scopes in rules %d and %d", priorityIndex, priority, planType, previousIndex, index)
				}
			}
		}

		out = append(out, RoutingSubscriptionOverride{
			Providers:                   providers,
			PlanTypes:                   planTypes,
			PerAuthRequestLimit:         perAuthRequestLimit,
			PerAuthRequestWindowMinutes: perAuthRequestWindowMinutes,
		})
	}
	return out, nil
}

// NormalizeRoutingPriorityOverrides validates and canonicalizes per-priority routing overrides.
func NormalizeRoutingPriorityOverrides(overrides []RoutingPriorityOverride) ([]RoutingPriorityOverride, error) {
	if len(overrides) == 0 {
		return nil, nil
	}
	seen := make(map[int]struct{}, len(overrides))
	out := make([]RoutingPriorityOverride, 0, len(overrides))
	for overrideIndex, override := range overrides {
		if _, ok := seen[override.Priority]; ok {
			return nil, fmt.Errorf("routing.priority-overrides: duplicate priority %d", override.Priority)
		}
		seen[override.Priority] = struct{}{}

		strategy := strings.TrimSpace(override.Strategy)
		if strategy != "" {
			normalized, ok := NormalizeRoutingStrategy(strategy)
			if !ok {
				return nil, fmt.Errorf("routing.priority-overrides[%d].strategy: invalid strategy %q", override.Priority, override.Strategy)
			}
			strategy = normalized
		}

		var maxRetryCredentials *int
		if override.MaxRetryCredentials != nil {
			value := *override.MaxRetryCredentials
			if value < 0 {
				value = 0
			}
			maxRetryCredentials = &value
		}
		var fillFirstRange *int
		if override.FillFirstRange != nil {
			value := NormalizeFillFirstRange(*override.FillFirstRange)
			fillFirstRange = &value
		}
		var fillFirstPerAuthRPM *int
		if override.FillFirstPerAuthRPM != nil {
			value := NormalizeFillFirstPerAuthRPM(*override.FillFirstPerAuthRPM)
			fillFirstPerAuthRPM = &value
		}
		var perAuthRequestLimit *int
		if override.PerAuthRequestLimit != nil {
			value := NormalizePerAuthRequestLimit(*override.PerAuthRequestLimit)
			perAuthRequestLimit = &value
		}
		var perAuthRequestWindowMinutes *int
		if override.PerAuthRequestWindowMinutes != nil {
			value := NormalizePerAuthRequestWindowMinutes(*override.PerAuthRequestWindowMinutes)
			perAuthRequestWindowMinutes = &value
		}
		subscriptionOverrides, errSubscriptionOverrides := normalizeRoutingSubscriptionOverrides(overrideIndex, override.Priority, override.SubscriptionOverrides)
		if errSubscriptionOverrides != nil {
			return nil, errSubscriptionOverrides
		}

		out = append(out, RoutingPriorityOverride{
			Priority:                    override.Priority,
			Strategy:                    strategy,
			MaxRetryCredentials:         maxRetryCredentials,
			FillFirstRange:              fillFirstRange,
			FillFirstPerAuthRPM:         fillFirstPerAuthRPM,
			PerAuthRequestLimit:         perAuthRequestLimit,
			PerAuthRequestWindowMinutes: perAuthRequestWindowMinutes,
			SubscriptionOverrides:       subscriptionOverrides,
		})
	}
	return out, nil
}

func routingStrategyForFillFirstControl(strategy string) string {
	strategy = strings.TrimSpace(strategy)
	if normalized, ok := NormalizeRoutingStrategy(strategy); ok {
		return normalized
	}
	return strings.ToLower(strategy)
}

// NormalizeRoutingConfig validates and canonicalizes routing configuration.
func NormalizeRoutingConfig(routing RoutingConfig) (RoutingConfig, error) {
	routing.FillFirstRange = NormalizeFillFirstRange(routing.FillFirstRange)
	routing.FillFirstPerAuthRPM = NormalizeFillFirstPerAuthRPM(routing.FillFirstPerAuthRPM)
	routing.PerAuthRequestLimit = NormalizePerAuthRequestLimit(routing.PerAuthRequestLimit)
	routing.PerAuthRequestWindowMinutes = NormalizePerAuthRequestWindowMinutes(routing.PerAuthRequestWindowMinutes)
	normalized, err := NormalizeRoutingPriorityOverrides(routing.PriorityOverrides)
	if err != nil {
		return routing, err
	}
	routing.PriorityOverrides = normalized
	if err := ValidateRoutingFillFirstControls(routing); err != nil {
		return routing, err
	}
	return routing, nil
}

// ValidateRoutingFillFirstControls enforces mutually exclusive fill-first controls.
func ValidateRoutingFillFirstControls(routing RoutingConfig) error {
	globalStrategy := routingStrategyForFillFirstControl(routing.Strategy)
	if globalStrategy == "fill-first" && NormalizeFillFirstRange(routing.FillFirstRange) > 1 && NormalizeFillFirstPerAuthRPM(routing.FillFirstPerAuthRPM) > 0 {
		return fmt.Errorf("routing.fill-first-range and routing.fill-first-per-auth-rpm are mutually exclusive for fill-first strategy")
	}
	for _, override := range routing.PriorityOverrides {
		strategy := globalStrategy
		if strings.TrimSpace(override.Strategy) != "" {
			strategy = routingStrategyForFillFirstControl(override.Strategy)
		}
		if strategy != "fill-first" {
			continue
		}
		fillFirstRange := NormalizeFillFirstRange(routing.FillFirstRange)
		if override.FillFirstRange != nil {
			fillFirstRange = NormalizeFillFirstRange(*override.FillFirstRange)
		}
		fillFirstPerAuthRPM := NormalizeFillFirstPerAuthRPM(routing.FillFirstPerAuthRPM)
		if override.FillFirstPerAuthRPM != nil {
			fillFirstPerAuthRPM = NormalizeFillFirstPerAuthRPM(*override.FillFirstPerAuthRPM)
		}
		if fillFirstRange > 1 && fillFirstPerAuthRPM > 0 {
			return fmt.Errorf("routing.priority-overrides[%d]: fill-first-range and fill-first-per-auth-rpm are mutually exclusive for fill-first strategy", override.Priority)
		}
	}
	return nil
}

// NormalizeRouting validates and stores routing configuration.
func (cfg *Config) NormalizeRouting() error {
	if cfg == nil {
		return nil
	}
	normalized, err := NormalizeRoutingConfig(cfg.Routing)
	if err != nil {
		return err
	}
	cfg.Routing = normalized
	return nil
}

// NormalizeRoutingPriorityOverrides validates and stores per-priority routing overrides.
func (cfg *Config) NormalizeRoutingPriorityOverrides() error {
	return cfg.NormalizeRouting()
}

// SanitizeOpenAICompatibility removes OpenAI-compatibility provider entries that are
// not actionable, specifically those missing a BaseURL. It trims whitespace before
// evaluation and preserves the relative order of remaining entries.
func (cfg *Config) SanitizeOpenAICompatibility() {
	if cfg == nil || len(cfg.OpenAICompatibility) == 0 {
		return
	}
	out := make([]OpenAICompatibility, 0, len(cfg.OpenAICompatibility))
	for i := range cfg.OpenAICompatibility {
		e := cfg.OpenAICompatibility[i]
		e.Name = strings.TrimSpace(e.Name)
		e.Prefix = normalizeModelPrefix(e.Prefix)
		e.BaseURL = strings.TrimSpace(e.BaseURL)
		e.Headers = NormalizeHeaders(e.Headers)
		if e.BaseURL == "" {
			// Skip providers with no base-url; treated as removed
			continue
		}
		out = append(out, e)
	}
	cfg.OpenAICompatibility = out
}

// SanitizeCodexKeys removes Codex API key entries missing a BaseURL.
// It trims whitespace and preserves order for remaining entries.
func (cfg *Config) SanitizeCodexKeys() {
	if cfg == nil || len(cfg.CodexKey) == 0 {
		return
	}
	out := make([]CodexKey, 0, len(cfg.CodexKey))
	for i := range cfg.CodexKey {
		e := cfg.CodexKey[i]
		e.Prefix = normalizeModelPrefix(e.Prefix)
		e.BaseURL = strings.TrimSpace(e.BaseURL)
		e.Headers = NormalizeHeaders(e.Headers)
		e.ExcludedModels = NormalizeExcludedModels(e.ExcludedModels)
		if e.BaseURL == "" {
			continue
		}
		out = append(out, e)
	}
	cfg.CodexKey = out
}

// SanitizeClaudeKeys normalizes headers for Claude credentials.
func (cfg *Config) SanitizeClaudeKeys() {
	if cfg == nil || len(cfg.ClaudeKey) == 0 {
		return
	}
	for i := range cfg.ClaudeKey {
		entry := &cfg.ClaudeKey[i]
		entry.Prefix = normalizeModelPrefix(entry.Prefix)
		entry.Headers = NormalizeHeaders(entry.Headers)
		entry.ExcludedModels = NormalizeExcludedModels(entry.ExcludedModels)
	}
}

func sanitizeGeminiKeyEntries(entries []GeminiKey) []GeminiKey {
	seen := make(map[string]struct{}, len(entries))
	out := entries[:0]
	for i := range entries {
		entry := entries[i]
		entry.APIKey = strings.TrimSpace(entry.APIKey)
		if entry.APIKey == "" {
			continue
		}
		entry.Prefix = normalizeModelPrefix(entry.Prefix)
		entry.BaseURL = strings.TrimSpace(entry.BaseURL)
		entry.ProxyURL = strings.TrimSpace(entry.ProxyURL)
		entry.Headers = NormalizeHeaders(entry.Headers)
		entry.ExcludedModels = NormalizeExcludedModels(entry.ExcludedModels)
		uniqueKey := entry.APIKey + "|" + entry.BaseURL
		if _, exists := seen[uniqueKey]; exists {
			continue
		}
		seen[uniqueKey] = struct{}{}
		out = append(out, entry)
	}
	return out
}

// SanitizeGeminiKeys deduplicates and normalizes Gemini credentials.
// It uses API key + base URL as the uniqueness key.
func (cfg *Config) SanitizeGeminiKeys() {
	if cfg == nil {
		return
	}
	cfg.GeminiKey = sanitizeGeminiKeyEntries(cfg.GeminiKey)
}

// SanitizeInteractionsKeys deduplicates and normalizes native Interactions credentials.
// It uses API key + base URL as the uniqueness key.
func (cfg *Config) SanitizeInteractionsKeys() {
	if cfg == nil {
		return
	}
	cfg.InteractionsKey = sanitizeGeminiKeyEntries(cfg.InteractionsKey)
}

func normalizeModelPrefix(prefix string) string {
	trimmed := strings.TrimSpace(prefix)
	trimmed = strings.Trim(trimmed, "/")
	if trimmed == "" {
		return ""
	}
	if strings.Contains(trimmed, "/") {
		return ""
	}
	return trimmed
}

// looksLikeBcrypt returns true if the provided string appears to be a bcrypt hash.
func looksLikeBcrypt(s string) bool {
	return len(s) > 4 && (s[:4] == "$2a$" || s[:4] == "$2b$" || s[:4] == "$2y$")
}

// NormalizeHeaders trims header keys and values and removes empty pairs.
func NormalizeHeaders(headers map[string]string) map[string]string {
	if len(headers) == 0 {
		return nil
	}
	clean := make(map[string]string, len(headers))
	for k, v := range headers {
		key := strings.TrimSpace(k)
		val := strings.TrimSpace(v)
		if key == "" || val == "" {
			continue
		}
		clean[key] = val
	}
	if len(clean) == 0 {
		return nil
	}
	return clean
}

// NormalizeExcludedModels trims, lowercases, and deduplicates model exclusion patterns.
// It preserves the order of first occurrences and drops empty entries.
func NormalizeExcludedModels(models []string) []string {
	if len(models) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(models))
	out := make([]string, 0, len(models))
	for _, raw := range models {
		trimmed := strings.ToLower(strings.TrimSpace(raw))
		if trimmed == "" {
			continue
		}
		if _, exists := seen[trimmed]; exists {
			continue
		}
		seen[trimmed] = struct{}{}
		out = append(out, trimmed)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// NormalizeAPIKeyGroups validates provider restrictions against configured API keys.
func NormalizeAPIKeyGroups(groups []APIKeyGroup, apiKeys []string) ([]APIKeyGroup, error) {
	return normalizeAPIKeyGroups(groups, apiKeys, false)
}

// PruneAPIKeyGroups normalizes restrictions while removing mappings for deleted API keys.
func PruneAPIKeyGroups(groups []APIKeyGroup, apiKeys []string) ([]APIKeyGroup, error) {
	return normalizeAPIKeyGroups(groups, apiKeys, true)
}

func normalizeAPIKeyGroups(groups []APIKeyGroup, apiKeys []string, pruneUnknown bool) ([]APIKeyGroup, error) {
	if len(groups) == 0 {
		return nil, nil
	}
	configuredKeys := make(map[string]struct{}, len(apiKeys))
	for _, rawKey := range apiKeys {
		if key := strings.TrimSpace(rawKey); key != "" {
			configuredKeys[key] = struct{}{}
		}
	}
	seenKeys := make(map[string]struct{}, len(groups))
	normalized := make([]APIKeyGroup, 0, len(groups))
	for _, group := range groups {
		key := strings.TrimSpace(group.APIKey)
		if key == "" {
			return nil, fmt.Errorf("api-key-groups contains an empty api-key")
		}
		if _, exists := configuredKeys[key]; !exists {
			if pruneUnknown {
				continue
			}
			return nil, fmt.Errorf("api-key-groups references unknown api-key %q", key)
		}
		if _, exists := seenKeys[key]; exists {
			return nil, fmt.Errorf("api-key-groups contains duplicate api-key %q", key)
		}
		seenKeys[key] = struct{}{}

		providers := make([]string, 0, len(group.Providers))
		seenProviders := make(map[string]struct{}, len(group.Providers))
		unrestricted := false
		for _, rawProvider := range group.Providers {
			provider := strings.ToLower(strings.TrimSpace(rawProvider))
			if provider == "" {
				continue
			}
			if provider == "all" || provider == "*" {
				unrestricted = true
				break
			}
			if _, exists := seenProviders[provider]; exists {
				continue
			}
			seenProviders[provider] = struct{}{}
			providers = append(providers, provider)
		}
		if unrestricted || len(providers) == 0 {
			continue
		}
		normalized = append(normalized, APIKeyGroup{APIKey: key, Providers: providers})
	}
	if len(normalized) == 0 {
		return nil, nil
	}
	return normalized, nil
}

// NormalizeOAuthExcludedModels cleans provider -> excluded models mappings by normalizing provider keys
// and applying model exclusion normalization to each entry.
func NormalizeOAuthExcludedModels(entries map[string][]string) map[string][]string {
	if len(entries) == 0 {
		return nil
	}
	out := make(map[string][]string, len(entries))
	for provider, models := range entries {
		key := strings.ToLower(strings.TrimSpace(provider))
		if key == "" || key == "gemini-cli" {
			continue
		}
		normalized := NormalizeExcludedModels(models)
		if len(normalized) == 0 {
			continue
		}
		out[key] = normalized
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// hashSecret hashes the given secret using bcrypt.
func hashSecret(secret string) (string, error) {
	// Use default cost for simplicity.
	hashedBytes, err := bcrypt.GenerateFromPassword([]byte(secret), bcrypt.DefaultCost)
	if err != nil {
		return "", err
	}
	return string(hashedBytes), nil
}

// SaveConfigPreserveComments writes the config back to YAML while preserving existing comments
// and key ordering by loading the original file into a yaml.Node tree and updating values in-place.
func SaveConfigPreserveComments(configFile string, cfg *Config) error {
	if cfg == nil {
		return fmt.Errorf("config is nil")
	}
	persistCfg := *cfg
	groups, errNormalizeGroups := NormalizeAPIKeyGroups(persistCfg.APIKeyGroups, persistCfg.APIKeys)
	if errNormalizeGroups != nil {
		return errNormalizeGroups
	}
	persistCfg.APIKeyGroups = groups
	// Load original YAML as a node tree to preserve comments and ordering.
	data, err := os.ReadFile(configFile)
	if err != nil {
		return err
	}

	var original yaml.Node
	if err = yaml.Unmarshal(data, &original); err != nil {
		return err
	}
	if original.Kind != yaml.DocumentNode || len(original.Content) == 0 {
		return fmt.Errorf("invalid yaml document structure")
	}
	if original.Content[0] == nil || original.Content[0].Kind != yaml.MappingNode {
		return fmt.Errorf("expected root mapping node")
	}

	// Marshal the current cfg to YAML, then unmarshal to a yaml.Node we can merge from.
	rendered, err := yaml.Marshal(&persistCfg)
	if err != nil {
		return err
	}
	var generated yaml.Node
	if err = yaml.Unmarshal(rendered, &generated); err != nil {
		return err
	}
	if generated.Kind != yaml.DocumentNode || len(generated.Content) == 0 || generated.Content[0] == nil {
		return fmt.Errorf("invalid generated yaml structure")
	}
	if generated.Content[0].Kind != yaml.MappingNode {
		return fmt.Errorf("expected generated root mapping node")
	}

	// Remove deprecated sections before merging back the sanitized config.
	removeLegacyAuthBlock(original.Content[0])
	removeLegacyOpenAICompatAPIKeys(original.Content[0])
	removeLegacyGenerativeLanguageKeys(original.Content[0])
	removeDeprecatedGeminiCLIConfigRoot(original.Content[0])
	removeDeprecatedGeminiCLIConfigRoot(generated.Content[0])

	pruneMappingToGeneratedKeys(original.Content[0], generated.Content[0], "oauth-excluded-models")
	pruneMappingToGeneratedKeys(original.Content[0], generated.Content[0], "oauth-model-alias")
	if findMapKeyIndex(generated.Content[0], "error-response-rewrites") < 0 {
		removeMapKey(original.Content[0], "error-response-rewrites")
	}
	normalizeUsagePricingModelKeys(original.Content[0])
	pruneNestedMappingToGeneratedKeys(original.Content[0], generated.Content[0], "usage-pricing", "models")

	// Merge generated into original in-place, preserving comments/order of existing nodes.
	mergeMappingPreserve(original.Content[0], generated.Content[0])
	normalizeCollectionNodeStyles(original.Content[0])

	// Write back.
	f, err := os.Create(configFile)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	var buf bytes.Buffer
	enc := yaml.NewEncoder(&buf)
	enc.SetIndent(2)
	if err = enc.Encode(&original); err != nil {
		_ = enc.Close()
		return err
	}
	if err = enc.Close(); err != nil {
		return err
	}
	data = NormalizeCommentIndentation(buf.Bytes())
	_, err = f.Write(data)
	return err
}

// SaveConfigPreserveCommentsUpdateNestedScalar updates a nested scalar key path like ["a","b"]
// while preserving comments and positions.
func SaveConfigPreserveCommentsUpdateNestedScalar(configFile string, path []string, value string) error {
	data, err := os.ReadFile(configFile)
	if err != nil {
		return err
	}
	var root yaml.Node
	if err = yaml.Unmarshal(data, &root); err != nil {
		return err
	}
	if root.Kind != yaml.DocumentNode || len(root.Content) == 0 {
		return fmt.Errorf("invalid yaml document structure")
	}
	node := root.Content[0]
	// descend mapping nodes following path
	for i, key := range path {
		if i == len(path)-1 {
			// set final scalar
			v := getOrCreateMapValue(node, key)
			v.Kind = yaml.ScalarNode
			v.Tag = "!!str"
			v.Value = value
		} else {
			next := getOrCreateMapValue(node, key)
			if next.Kind != yaml.MappingNode {
				next.Kind = yaml.MappingNode
				next.Tag = "!!map"
			}
			node = next
		}
	}
	f, err := os.Create(configFile)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	var buf bytes.Buffer
	enc := yaml.NewEncoder(&buf)
	enc.SetIndent(2)
	if err = enc.Encode(&root); err != nil {
		_ = enc.Close()
		return err
	}
	if err = enc.Close(); err != nil {
		return err
	}
	data = NormalizeCommentIndentation(buf.Bytes())
	_, err = f.Write(data)
	return err
}

// NormalizeCommentIndentation removes indentation from standalone YAML comment lines to keep them left aligned.
func NormalizeCommentIndentation(data []byte) []byte {
	lines := bytes.Split(data, []byte("\n"))
	changed := false
	for i, line := range lines {
		trimmed := bytes.TrimLeft(line, " \t")
		if len(trimmed) == 0 || trimmed[0] != '#' {
			continue
		}
		if len(trimmed) == len(line) {
			continue
		}
		lines[i] = append([]byte(nil), trimmed...)
		changed = true
	}
	if !changed {
		return data
	}
	return bytes.Join(lines, []byte("\n"))
}

// getOrCreateMapValue finds the value node for a given key in a mapping node.
// If not found, it appends a new key/value pair and returns the new value node.
func getOrCreateMapValue(mapNode *yaml.Node, key string) *yaml.Node {
	if mapNode.Kind != yaml.MappingNode {
		mapNode.Kind = yaml.MappingNode
		mapNode.Tag = "!!map"
		mapNode.Content = nil
	}
	for i := 0; i+1 < len(mapNode.Content); i += 2 {
		k := mapNode.Content[i]
		if k.Value == key {
			return mapNode.Content[i+1]
		}
	}
	// append new key/value
	mapNode.Content = append(mapNode.Content, &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: key})
	val := &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: ""}
	mapNode.Content = append(mapNode.Content, val)
	return val
}

// mergeMappingPreserve merges keys from src into dst mapping node while preserving
// key order and comments of existing keys in dst. New keys are only added if their
// value is non-zero and not a known default to avoid polluting the config with defaults.
func mergeMappingPreserve(dst, src *yaml.Node, path ...[]string) {
	var currentPath []string
	if len(path) > 0 {
		currentPath = path[0]
	}

	if dst == nil || src == nil {
		return
	}
	if dst.Kind != yaml.MappingNode || src.Kind != yaml.MappingNode {
		// If kinds do not match, prefer replacing dst with src semantics in-place
		// but keep dst node object to preserve any attached comments at the parent level.
		copyNodeShallow(dst, src)
		return
	}
	for i := 0; i+1 < len(src.Content); i += 2 {
		sk := src.Content[i]
		sv := src.Content[i+1]
		idx := findMapKeyIndex(dst, sk.Value)
		childPath := appendPath(currentPath, sk.Value)
		if idx >= 0 {
			// Merge into existing value node (always update, even to zero values)
			dv := dst.Content[idx+1]
			mergeNodePreserve(dv, sv, childPath)
		} else {
			// New key: only add if value is non-zero and not a known default
			candidate := deepCopyNode(sv)
			pruneKnownDefaultsInNewNode(childPath, candidate)
			if isKnownDefaultValue(childPath, candidate) {
				continue
			}
			dst.Content = append(dst.Content, deepCopyNode(sk), candidate)
		}
	}
}

// mergeNodePreserve merges src into dst for scalars, mappings and sequences while
// reusing destination nodes to keep comments and anchors. For sequences, it updates
// in-place by index.
func mergeNodePreserve(dst, src *yaml.Node, path ...[]string) {
	var currentPath []string
	if len(path) > 0 {
		currentPath = path[0]
	}

	if dst == nil || src == nil {
		return
	}
	switch src.Kind {
	case yaml.MappingNode:
		if dst.Kind != yaml.MappingNode {
			copyNodeShallow(dst, src)
		}
		mergeMappingPreserve(dst, src, currentPath)
	case yaml.SequenceNode:
		// Preserve explicit null style if dst was null and src is empty sequence
		if dst.Kind == yaml.ScalarNode && dst.Tag == "!!null" && len(src.Content) == 0 {
			// Keep as null to preserve original style
			return
		}
		if dst.Kind != yaml.SequenceNode {
			dst.Kind = yaml.SequenceNode
			dst.Tag = "!!seq"
			dst.Content = nil
		}
		reorderSequenceForMerge(dst, src)
		// Update elements in place
		minContent := len(dst.Content)
		if len(src.Content) < minContent {
			minContent = len(src.Content)
		}
		for i := 0; i < minContent; i++ {
			if dst.Content[i] == nil {
				dst.Content[i] = deepCopyNode(src.Content[i])
				continue
			}
			mergeNodePreserve(dst.Content[i], src.Content[i], currentPath)
			if dst.Content[i] != nil && src.Content[i] != nil &&
				dst.Content[i].Kind == yaml.MappingNode && src.Content[i].Kind == yaml.MappingNode {
				pruneMissingMapKeys(dst.Content[i], src.Content[i])
			}
		}
		// Append any extra items from src
		for i := len(dst.Content); i < len(src.Content); i++ {
			dst.Content = append(dst.Content, deepCopyNode(src.Content[i]))
		}
		// Truncate if dst has extra items not in src
		if len(src.Content) < len(dst.Content) {
			dst.Content = dst.Content[:len(src.Content)]
		}
	case yaml.ScalarNode, yaml.AliasNode:
		// For scalars, update Tag and Value but keep Style from dst to preserve quoting
		dst.Kind = src.Kind
		dst.Tag = src.Tag
		dst.Value = src.Value
		// Keep dst.Style as-is intentionally
	case 0:
		// Unknown/empty kind; do nothing
	default:
		// Fallback: replace shallowly
		copyNodeShallow(dst, src)
	}
}

// findMapKeyIndex returns the index of key node in dst mapping (index of key, not value).
// Returns -1 when not found.
func findMapKeyIndex(mapNode *yaml.Node, key string) int {
	if mapNode == nil || mapNode.Kind != yaml.MappingNode {
		return -1
	}
	for i := 0; i+1 < len(mapNode.Content); i += 2 {
		if mapNode.Content[i] != nil && mapNode.Content[i].Value == key {
			return i
		}
	}
	return -1
}

// appendPath appends a key to the path, returning a new slice to avoid modifying the original.
func appendPath(path []string, key string) []string {
	if len(path) == 0 {
		return []string{key}
	}
	newPath := make([]string, len(path)+1)
	copy(newPath, path)
	newPath[len(path)] = key
	return newPath
}

// isKnownDefaultValue returns true if the given node at the specified path
// represents a known default value that should not be written to the config file.
// This prevents non-zero defaults from polluting the config.
func isKnownDefaultValue(path []string, node *yaml.Node) bool {
	fullPath := strings.Join(path, ".")
	if fullPath == "usage-statistics-persistence-enabled" && node != nil && node.Kind == yaml.ScalarNode && node.Tag == "!!bool" {
		return node.Value == "true"
	}
	if preservesExplicitChatGPTWebValue(fullPath, node) {
		return false
	}
	if fullPath == "routing.priority-overrides" && node != nil && node.Kind == yaml.SequenceNode && len(node.Content) > 0 {
		return false
	}
	if fullPath == "routing" && node != nil && node.Kind == yaml.MappingNode {
		if index := findMapKeyIndex(node, "priority-overrides"); index >= 0 {
			value := node.Content[index+1]
			if value != nil && value.Kind == yaml.SequenceNode && len(value.Content) > 0 {
				return false
			}
		}
	}
	if node != nil && node.Kind == yaml.ScalarNode && node.Tag == "!!int" {
		switch fullPath {
		case "routing.priority-overrides.priority":
			// Priority zero is an identity, not an omitted default.
			return false
		case "routing.priority-overrides.per-auth-request-limit":
			// This pointer field uses an explicit zero to disable an inherited limit.
			return false
		case "routing.priority-overrides.subscription-overrides.per-auth-request-limit":
			// Subscription rules also use an explicit zero to disable the inherited limit.
			return false
		}
	}
	// First check if it's a zero value
	if isZeroValueNode(node) {
		return true
	}

	// Match known non-zero defaults by exact dotted path.
	if len(path) == 0 {
		return false
	}

	// Check string defaults
	if node.Kind == yaml.ScalarNode && node.Tag == "!!str" {
		switch fullPath {
		case "pprof.addr":
			return node.Value == DefaultPprofAddr
		case "remote-management.panel-github-repository":
			return node.Value == DefaultPanelGitHubRepository
		case "routing.strategy":
			return node.Value == "round-robin"
		}
	}

	// Check integer defaults
	if node.Kind == yaml.ScalarNode && node.Tag == "!!int" {
		switch fullPath {
		case "error-logs-max-files":
			return node.Value == "10"
		case "routing.per-auth-request-window-minutes":
			return node.Value == "1"
		}
	}

	return false
}

func preservesExplicitChatGPTWebValue(fullPath string, node *yaml.Node) bool {
	if node == nil {
		return false
	}
	switch fullPath {
	case "chatgpt-web":
		for _, key := range []string{
			"api798-auto-login-enabled",
			"auto-relogin-max-retries",
			"auto-relogin-jitter-percent",
			"force-session-refresh-on-import",
			"import",
			"timezone",
			"timezone-offset-minutes",
			"estimate-token-usage",
			"usage-cache",
			"image-usage",
			"login-proxy",
			"account-info",
			"sentinel",
		} {
			if index := findMapKeyIndex(node, key); index >= 0 && index+1 < len(node.Content) &&
				preservesExplicitChatGPTWebValue("chatgpt-web."+key, node.Content[index+1]) {
				return true
			}
		}
		return false
	case "chatgpt-web.auto-relogin-max-retries",
		"chatgpt-web.auto-relogin-jitter-percent",
		"chatgpt-web.force-session-refresh-on-import",
		"chatgpt-web.api798-auto-login-enabled",
		"chatgpt-web.timezone",
		"chatgpt-web.timezone-offset-minutes":
		return true
	case "chatgpt-web.estimate-token-usage":
		return node.Kind == yaml.ScalarNode && node.Tag == "!!bool"
	case "chatgpt-web.import":
		return node.Kind == yaml.MappingNode && len(node.Content) > 0
	case "chatgpt-web.import.workers",
		"chatgpt-web.import.validate-models-after-upload",
		"chatgpt-web.import.refresh-account-info-after-upload":
		return true
	case "chatgpt-web.image-usage":
		return node.Kind == yaml.MappingNode && len(node.Content) > 0
	case "chatgpt-web.image-usage.auto-output-quality":
		return true
	case "chatgpt-web.image-usage.fallback-usage":
		return node.Kind == yaml.MappingNode && len(node.Content) > 0
	case "chatgpt-web.image-usage.fallback-usage.enabled",
		"chatgpt-web.image-usage.fallback-usage.input-text-tokens",
		"chatgpt-web.image-usage.fallback-usage.input-image-tokens",
		"chatgpt-web.image-usage.fallback-usage.output-text-tokens",
		"chatgpt-web.image-usage.fallback-usage.output-image-tokens":
		return true
	case "chatgpt-web.login-proxy":
		return node.Kind == yaml.MappingNode && len(node.Content) > 0
	case "chatgpt-web.login-proxy.enabled",
		"chatgpt-web.login-proxy.url-template",
		"chatgpt-web.login-proxy.placeholder-charset",
		"chatgpt-web.login-proxy.rotate-on-retry",
		"chatgpt-web.login-proxy.request-attempts",
		"chatgpt-web.login-proxy.flow-attempts",
		"chatgpt-web.login-proxy.retry-delay-milliseconds",
		"chatgpt-web.login-proxy.acquisition-timeout-seconds":
		return true
	case "chatgpt-web.usage-cache.enabled",
		"chatgpt-web.usage-cache.disk-threshold-mb",
		"chatgpt-web.usage-cache.max-disk-size-mb",
		"chatgpt-web.usage-cache.resource-guard-enabled",
		"chatgpt-web.usage-cache.min-available-disk-mb",
		"chatgpt-web.usage-cache.max-filesystem-used-percent":
		return true
	case "chatgpt-web.account-info":
		return node.Kind == yaml.MappingNode && len(node.Content) > 0
	case "chatgpt-web.account-info.auto-refresh-enabled",
		"chatgpt-web.account-info.diagnostics-enabled",
		"chatgpt-web.account-info.refresh-workers",
		"chatgpt-web.account-info.refresh-queue-size",
		"chatgpt-web.account-info.refresh-ttl-minutes",
		"chatgpt-web.account-info.periodic-refresh-minutes",
		"chatgpt-web.account-info.recovery-jitter-seconds",
		"chatgpt-web.account-info.max-retries":
		return true
	case "chatgpt-web.sentinel":
		return node.Kind == yaml.MappingNode && len(node.Content) > 0
	case "chatgpt-web.sentinel.sdk-runtime-enabled",
		"chatgpt-web.sentinel.sdk-workers",
		"chatgpt-web.sentinel.sdk-queue-size",
		"chatgpt-web.sentinel.sdk-cache-versions":
		return true
	default:
		return false
	}
}

// pruneKnownDefaultsInNewNode removes default-valued descendants from a new node
// before it is appended into the destination YAML tree.
func pruneKnownDefaultsInNewNode(path []string, node *yaml.Node) {
	if node == nil {
		return
	}

	switch node.Kind {
	case yaml.MappingNode:
		filtered := make([]*yaml.Node, 0, len(node.Content))
		for i := 0; i+1 < len(node.Content); i += 2 {
			keyNode := node.Content[i]
			valueNode := node.Content[i+1]
			if keyNode == nil || valueNode == nil {
				continue
			}

			childPath := appendPath(path, keyNode.Value)
			if isKnownDefaultValue(childPath, valueNode) {
				continue
			}

			pruneKnownDefaultsInNewNode(childPath, valueNode)
			if (valueNode.Kind == yaml.MappingNode || valueNode.Kind == yaml.SequenceNode) &&
				len(valueNode.Content) == 0 {
				continue
			}

			filtered = append(filtered, keyNode, valueNode)
		}
		node.Content = filtered
	case yaml.SequenceNode:
		for _, child := range node.Content {
			pruneKnownDefaultsInNewNode(path, child)
		}
	}
}

// isZeroValueNode returns true if the YAML node represents a zero/default value
// that should not be written as a new key to preserve config cleanliness.
// For mappings and sequences, recursively checks if all children are zero values.
func isZeroValueNode(node *yaml.Node) bool {
	if node == nil {
		return true
	}
	switch node.Kind {
	case yaml.ScalarNode:
		switch node.Tag {
		case "!!bool":
			return node.Value == "false"
		case "!!int", "!!float":
			return node.Value == "0" || node.Value == "0.0"
		case "!!str":
			return node.Value == ""
		case "!!null":
			return true
		}
	case yaml.SequenceNode:
		if len(node.Content) == 0 {
			return true
		}
		// Check if all elements are zero values
		for _, child := range node.Content {
			if !isZeroValueNode(child) {
				return false
			}
		}
		return true
	case yaml.MappingNode:
		if len(node.Content) == 0 {
			return true
		}
		// Check if all values are zero values (values are at odd indices)
		for i := 1; i < len(node.Content); i += 2 {
			if !isZeroValueNode(node.Content[i]) {
				return false
			}
		}
		return true
	}
	return false
}

// deepCopyNode creates a deep copy of a yaml.Node graph.
func deepCopyNode(n *yaml.Node) *yaml.Node {
	if n == nil {
		return nil
	}
	cp := *n
	if len(n.Content) > 0 {
		cp.Content = make([]*yaml.Node, len(n.Content))
		for i := range n.Content {
			cp.Content[i] = deepCopyNode(n.Content[i])
		}
	}
	return &cp
}

// copyNodeShallow copies type/tag/value and resets content to match src, but
// keeps the same destination node pointer to preserve parent relations/comments.
func copyNodeShallow(dst, src *yaml.Node) {
	if dst == nil || src == nil {
		return
	}
	dst.Kind = src.Kind
	dst.Tag = src.Tag
	dst.Value = src.Value
	// Replace content with deep copy from src
	if len(src.Content) > 0 {
		dst.Content = make([]*yaml.Node, len(src.Content))
		for i := range src.Content {
			dst.Content[i] = deepCopyNode(src.Content[i])
		}
	} else {
		dst.Content = nil
	}
}

func reorderSequenceForMerge(dst, src *yaml.Node) {
	if dst == nil || src == nil {
		return
	}
	if len(dst.Content) == 0 {
		return
	}
	if len(src.Content) == 0 {
		return
	}
	original := append([]*yaml.Node(nil), dst.Content...)
	used := make([]bool, len(original))
	ordered := make([]*yaml.Node, len(src.Content))
	for i := range src.Content {
		if idx := matchSequenceElement(original, used, src.Content[i]); idx >= 0 {
			ordered[i] = original[idx]
			used[idx] = true
		}
	}
	dst.Content = ordered
}

func matchSequenceElement(original []*yaml.Node, used []bool, target *yaml.Node) int {
	if target == nil {
		return -1
	}
	switch target.Kind {
	case yaml.MappingNode:
		id := sequenceElementIdentity(target)
		if id != "" {
			for i := range original {
				if used[i] || original[i] == nil || original[i].Kind != yaml.MappingNode {
					continue
				}
				if sequenceElementIdentity(original[i]) == id {
					return i
				}
			}
		}
	case yaml.ScalarNode:
		val := strings.TrimSpace(target.Value)
		if val != "" {
			for i := range original {
				if used[i] || original[i] == nil || original[i].Kind != yaml.ScalarNode {
					continue
				}
				if strings.TrimSpace(original[i].Value) == val {
					return i
				}
			}
		}
	default:
	}
	// Fallback to structural equality to preserve nodes lacking explicit identifiers.
	for i := range original {
		if used[i] || original[i] == nil {
			continue
		}
		if nodesStructurallyEqual(original[i], target) {
			return i
		}
	}
	return -1
}

func sequenceElementIdentity(node *yaml.Node) string {
	if node == nil || node.Kind != yaml.MappingNode {
		return ""
	}
	identityKeys := []string{"id", "name", "alias", "api-key", "api_key", "apikey", "key", "provider", "model"}
	for _, k := range identityKeys {
		if v := mappingScalarValue(node, k); v != "" {
			return k + "=" + v
		}
	}
	for i := 0; i+1 < len(node.Content); i += 2 {
		keyNode := node.Content[i]
		valNode := node.Content[i+1]
		if keyNode == nil || valNode == nil || valNode.Kind != yaml.ScalarNode {
			continue
		}
		val := strings.TrimSpace(valNode.Value)
		if val != "" {
			return strings.ToLower(strings.TrimSpace(keyNode.Value)) + "=" + val
		}
	}
	return ""
}

func mappingScalarValue(node *yaml.Node, key string) string {
	if node == nil || node.Kind != yaml.MappingNode {
		return ""
	}
	lowerKey := strings.ToLower(key)
	for i := 0; i+1 < len(node.Content); i += 2 {
		keyNode := node.Content[i]
		valNode := node.Content[i+1]
		if keyNode == nil || valNode == nil || valNode.Kind != yaml.ScalarNode {
			continue
		}
		if strings.ToLower(strings.TrimSpace(keyNode.Value)) == lowerKey {
			return strings.TrimSpace(valNode.Value)
		}
	}
	return ""
}

func nodesStructurallyEqual(a, b *yaml.Node) bool {
	if a == nil || b == nil {
		return a == b
	}
	if a.Kind != b.Kind {
		return false
	}
	switch a.Kind {
	case yaml.MappingNode:
		if len(a.Content) != len(b.Content) {
			return false
		}
		for i := 0; i+1 < len(a.Content); i += 2 {
			if !nodesStructurallyEqual(a.Content[i], b.Content[i]) {
				return false
			}
			if !nodesStructurallyEqual(a.Content[i+1], b.Content[i+1]) {
				return false
			}
		}
		return true
	case yaml.SequenceNode:
		if len(a.Content) != len(b.Content) {
			return false
		}
		for i := range a.Content {
			if !nodesStructurallyEqual(a.Content[i], b.Content[i]) {
				return false
			}
		}
		return true
	case yaml.ScalarNode:
		return strings.TrimSpace(a.Value) == strings.TrimSpace(b.Value)
	case yaml.AliasNode:
		return nodesStructurallyEqual(a.Alias, b.Alias)
	default:
		return strings.TrimSpace(a.Value) == strings.TrimSpace(b.Value)
	}
}

func removeMapKey(mapNode *yaml.Node, key string) {
	if mapNode == nil || mapNode.Kind != yaml.MappingNode || key == "" {
		return
	}
	for i := 0; i+1 < len(mapNode.Content); i += 2 {
		if mapNode.Content[i] != nil && mapNode.Content[i].Value == key {
			mapNode.Content = append(mapNode.Content[:i], mapNode.Content[i+2:]...)
			return
		}
	}
}

func removeDeprecatedGeminiCLIConfigRoot(root *yaml.Node) {
	removeDeprecatedGeminiCLIConfigMapping(root, make(map[*yaml.Node]struct{}))
}

func removeDeprecatedGeminiCLIConfigMapping(node *yaml.Node, visited map[*yaml.Node]struct{}) {
	if node == nil {
		return
	}
	if _, seen := visited[node]; seen {
		return
	}
	visited[node] = struct{}{}
	if node.Kind == yaml.AliasNode {
		removeDeprecatedGeminiCLIConfigMapping(node.Alias, visited)
		return
	}
	if node.Kind == yaml.SequenceNode {
		for _, child := range node.Content {
			removeDeprecatedGeminiCLIConfigMapping(child, visited)
		}
		return
	}
	if node.Kind != yaml.MappingNode {
		return
	}

	for index := 0; index+1 < len(node.Content); {
		key := strings.ToLower(strings.TrimSpace(node.Content[index].Value))
		value := node.Content[index+1]
		switch key {
		case "enable-gemini-cli-endpoint":
			node.Content = append(node.Content[:index], node.Content[index+2:]...)
			continue
		case "oauth-model-alias", "oauth-excluded-models":
			removeMapKeyFold(value, "gemini-cli")
		case "<<":
			removeDeprecatedGeminiCLIConfigMapping(value, visited)
		}
		index += 2
	}
}

func removeMapKeyFold(mapNode *yaml.Node, key string) {
	removeMapKeyFoldVisited(mapNode, key, make(map[*yaml.Node]struct{}))
}

func removeMapKeyFoldVisited(node *yaml.Node, key string, visited map[*yaml.Node]struct{}) {
	if node == nil || key == "" {
		return
	}
	if _, seen := visited[node]; seen {
		return
	}
	visited[node] = struct{}{}
	if node.Kind == yaml.AliasNode {
		removeMapKeyFoldVisited(node.Alias, key, visited)
		return
	}
	if node.Kind == yaml.SequenceNode {
		for _, child := range node.Content {
			removeMapKeyFoldVisited(child, key, visited)
		}
		return
	}
	if node.Kind != yaml.MappingNode {
		return
	}
	for index := 0; index+1 < len(node.Content); {
		currentKey := strings.TrimSpace(node.Content[index].Value)
		if strings.EqualFold(currentKey, key) {
			node.Content = append(node.Content[:index], node.Content[index+2:]...)
			continue
		}
		if currentKey == "<<" {
			removeMapKeyFoldVisited(node.Content[index+1], key, visited)
		}
		index += 2
	}
}

func pruneMappingToGeneratedKeys(dstRoot, srcRoot *yaml.Node, key string) {
	if key == "" || dstRoot == nil || srcRoot == nil {
		return
	}
	if dstRoot.Kind != yaml.MappingNode || srcRoot.Kind != yaml.MappingNode {
		return
	}
	dstIdx := findMapKeyIndex(dstRoot, key)
	if dstIdx < 0 || dstIdx+1 >= len(dstRoot.Content) {
		return
	}
	srcIdx := findMapKeyIndex(srcRoot, key)
	if srcIdx < 0 {
		// Keep an explicit empty mapping for oauth-model-alias when it was previously present.
		// When users delete the last channel from oauth-model-alias via the management API,
		// we want that deletion to persist across hot reloads and restarts.
		if key == "oauth-model-alias" {
			dstRoot.Content[dstIdx+1] = &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}
			return
		}
		removeMapKey(dstRoot, key)
		return
	}
	if srcIdx+1 >= len(srcRoot.Content) {
		return
	}
	srcVal := srcRoot.Content[srcIdx+1]
	dstVal := dstRoot.Content[dstIdx+1]
	if srcVal == nil {
		dstRoot.Content[dstIdx+1] = nil
		return
	}
	if srcVal.Kind != yaml.MappingNode {
		dstRoot.Content[dstIdx+1] = deepCopyNode(srcVal)
		return
	}
	if dstVal == nil || dstVal.Kind != yaml.MappingNode {
		dstRoot.Content[dstIdx+1] = deepCopyNode(srcVal)
		return
	}
	pruneMissingMapKeys(dstVal, srcVal)
}

func pruneNestedMappingToGeneratedKeys(dstRoot, srcRoot *yaml.Node, parentKey, key string) {
	if dstRoot == nil || srcRoot == nil || dstRoot.Kind != yaml.MappingNode || srcRoot.Kind != yaml.MappingNode {
		return
	}
	dstIndex := findMapKeyIndex(dstRoot, parentKey)
	srcIndex := findMapKeyIndex(srcRoot, parentKey)
	if dstIndex < 0 || srcIndex < 0 || dstIndex+1 >= len(dstRoot.Content) || srcIndex+1 >= len(srcRoot.Content) {
		return
	}
	pruneMappingToGeneratedKeys(dstRoot.Content[dstIndex+1], srcRoot.Content[srcIndex+1], key)
}

func normalizeUsagePricingModelKeys(root *yaml.Node) {
	if root == nil || root.Kind != yaml.MappingNode {
		return
	}
	pricingIndex := findMapKeyIndex(root, "usage-pricing")
	if pricingIndex < 0 || pricingIndex+1 >= len(root.Content) {
		return
	}
	pricing := root.Content[pricingIndex+1]
	modelsIndex := findMapKeyIndex(pricing, "models")
	if modelsIndex < 0 || modelsIndex+1 >= len(pricing.Content) {
		return
	}
	models := pricing.Content[modelsIndex+1]
	if models == nil || models.Kind != yaml.MappingNode {
		return
	}
	for index := 0; index+1 < len(models.Content); index += 2 {
		if models.Content[index] != nil {
			models.Content[index].Value = strings.ToLower(strings.TrimSpace(models.Content[index].Value))
		}
	}
}

func pruneMissingMapKeys(dstMap, srcMap *yaml.Node) {
	if dstMap == nil || srcMap == nil || dstMap.Kind != yaml.MappingNode || srcMap.Kind != yaml.MappingNode {
		return
	}
	keep := make(map[string]struct{}, len(srcMap.Content)/2)
	for i := 0; i+1 < len(srcMap.Content); i += 2 {
		keyNode := srcMap.Content[i]
		if keyNode == nil {
			continue
		}
		key := strings.TrimSpace(keyNode.Value)
		if key == "" {
			continue
		}
		keep[key] = struct{}{}
	}
	for i := 0; i+1 < len(dstMap.Content); {
		keyNode := dstMap.Content[i]
		if keyNode == nil {
			i += 2
			continue
		}
		key := strings.TrimSpace(keyNode.Value)
		if _, ok := keep[key]; !ok {
			dstMap.Content = append(dstMap.Content[:i], dstMap.Content[i+2:]...)
			continue
		}
		i += 2
	}
}

// normalizeCollectionNodeStyles forces YAML collections to use block notation, keeping
// lists and maps readable. Empty sequences retain flow style ([]) so empty list markers
// remain compact.
func normalizeCollectionNodeStyles(node *yaml.Node) {
	if node == nil {
		return
	}
	switch node.Kind {
	case yaml.MappingNode:
		node.Style = 0
		for i := range node.Content {
			normalizeCollectionNodeStyles(node.Content[i])
		}
	case yaml.SequenceNode:
		if len(node.Content) == 0 {
			node.Style = yaml.FlowStyle
		} else {
			node.Style = 0
		}
		for i := range node.Content {
			normalizeCollectionNodeStyles(node.Content[i])
		}
	default:
		// Scalars keep their existing style to preserve quoting
	}
}

// Legacy migration helpers (move deprecated config keys into structured fields).
type legacyConfigData struct {
	LegacyGeminiKeys []string                    `yaml:"generative-language-api-key"`
	OpenAICompat     []legacyOpenAICompatibility `yaml:"openai-compatibility"`
}

type legacyOpenAICompatibility struct {
	Name    string   `yaml:"name"`
	BaseURL string   `yaml:"base-url"`
	APIKeys []string `yaml:"api-keys"`
}

func (cfg *Config) migrateLegacyGeminiKeys(legacy []string) bool {
	if cfg == nil || len(legacy) == 0 {
		return false
	}
	changed := false
	seen := make(map[string]struct{}, len(cfg.GeminiKey))
	for i := range cfg.GeminiKey {
		key := strings.TrimSpace(cfg.GeminiKey[i].APIKey)
		if key == "" {
			continue
		}
		seen[key] = struct{}{}
	}
	for _, raw := range legacy {
		key := strings.TrimSpace(raw)
		if key == "" {
			continue
		}
		if _, exists := seen[key]; exists {
			continue
		}
		cfg.GeminiKey = append(cfg.GeminiKey, GeminiKey{APIKey: key})
		seen[key] = struct{}{}
		changed = true
	}
	return changed
}

func (cfg *Config) migrateLegacyOpenAICompatibilityKeys(legacy []legacyOpenAICompatibility) bool {
	if cfg == nil || len(cfg.OpenAICompatibility) == 0 || len(legacy) == 0 {
		return false
	}
	changed := false
	for _, legacyEntry := range legacy {
		if len(legacyEntry.APIKeys) == 0 {
			continue
		}
		target := findOpenAICompatTarget(cfg.OpenAICompatibility, legacyEntry.Name, legacyEntry.BaseURL)
		if target == nil {
			continue
		}
		if mergeLegacyOpenAICompatAPIKeys(target, legacyEntry.APIKeys) {
			changed = true
		}
	}
	return changed
}

func mergeLegacyOpenAICompatAPIKeys(entry *OpenAICompatibility, keys []string) bool {
	if entry == nil || len(keys) == 0 {
		return false
	}
	changed := false
	existing := make(map[string]struct{}, len(entry.APIKeyEntries))
	for i := range entry.APIKeyEntries {
		key := strings.TrimSpace(entry.APIKeyEntries[i].APIKey)
		if key == "" {
			continue
		}
		existing[key] = struct{}{}
	}
	for _, raw := range keys {
		key := strings.TrimSpace(raw)
		if key == "" {
			continue
		}
		if _, ok := existing[key]; ok {
			continue
		}
		entry.APIKeyEntries = append(entry.APIKeyEntries, OpenAICompatibilityAPIKey{APIKey: key})
		existing[key] = struct{}{}
		changed = true
	}
	return changed
}

func findOpenAICompatTarget(entries []OpenAICompatibility, legacyName, legacyBase string) *OpenAICompatibility {
	nameKey := strings.ToLower(strings.TrimSpace(legacyName))
	baseKey := strings.ToLower(strings.TrimSpace(legacyBase))
	if nameKey != "" && baseKey != "" {
		for i := range entries {
			if strings.ToLower(strings.TrimSpace(entries[i].Name)) == nameKey &&
				strings.ToLower(strings.TrimSpace(entries[i].BaseURL)) == baseKey {
				return &entries[i]
			}
		}
	}
	if baseKey != "" {
		for i := range entries {
			if strings.ToLower(strings.TrimSpace(entries[i].BaseURL)) == baseKey {
				return &entries[i]
			}
		}
	}
	if nameKey != "" {
		for i := range entries {
			if strings.ToLower(strings.TrimSpace(entries[i].Name)) == nameKey {
				return &entries[i]
			}
		}
	}
	return nil
}

func removeLegacyOpenAICompatAPIKeys(root *yaml.Node) {
	if root == nil || root.Kind != yaml.MappingNode {
		return
	}
	idx := findMapKeyIndex(root, "openai-compatibility")
	if idx < 0 || idx+1 >= len(root.Content) {
		return
	}
	seq := root.Content[idx+1]
	if seq == nil || seq.Kind != yaml.SequenceNode {
		return
	}
	for i := range seq.Content {
		if seq.Content[i] != nil && seq.Content[i].Kind == yaml.MappingNode {
			removeMapKey(seq.Content[i], "api-keys")
		}
	}
}

func removeLegacyGenerativeLanguageKeys(root *yaml.Node) {
	if root == nil || root.Kind != yaml.MappingNode {
		return
	}
	removeMapKey(root, "generative-language-api-key")
}

func removeLegacyAuthBlock(root *yaml.Node) {
	if root == nil || root.Kind != yaml.MappingNode {
		return
	}
	removeMapKey(root, "auth")
}
