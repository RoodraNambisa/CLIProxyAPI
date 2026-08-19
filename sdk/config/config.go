// Package config provides the public SDK configuration API.
//
// It re-exports the server configuration types and helpers so external projects can
// embed CLIProxyAPI without importing internal packages.
package config

import internalconfig "github.com/router-for-me/CLIProxyAPI/v6/internal/config"

type SDKConfig = internalconfig.SDKConfig
type APIKeyGroup = internalconfig.APIKeyGroup
type ProxyPoolConfig = internalconfig.ProxyPoolConfig
type ProxyPoolEntryConfig = internalconfig.ProxyPoolEntryConfig
type ProxyRuleConfig = internalconfig.ProxyRuleConfig

type Config = internalconfig.Config
type RuntimeApplyResult = internalconfig.RuntimeApplyResult

type StreamingConfig = internalconfig.StreamingConfig
type CodexFingerprintConfig = internalconfig.CodexFingerprintConfig
type ImagesConfig = internalconfig.ImagesConfig
type ChatGPTWebImageConfig = internalconfig.ChatGPTWebImageConfig
type ResolvedChatGPTWebImageConfig = internalconfig.ResolvedChatGPTWebImageConfig
type NativeImagesConfig = internalconfig.NativeImagesConfig
type NativeImageEndpointConfig = internalconfig.NativeImageEndpointConfig
type TLSConfig = internalconfig.TLSConfig
type RemoteManagement = internalconfig.RemoteManagement
type AuthMaintenanceConfig = internalconfig.AuthMaintenanceConfig
type RequestBodyReleaseConfig = internalconfig.RequestBodyReleaseConfig
type ErrorResponseRewriteRule = internalconfig.ErrorResponseRewriteRule
type DisabledImageGenerationToolErrorConfig = internalconfig.DisabledImageGenerationToolErrorConfig
type NonRetryableErrorRule = internalconfig.NonRetryableErrorRule
type AuthModelExclusionRule = internalconfig.AuthModelExclusionRule
type OAuthModelAlias = internalconfig.OAuthModelAlias
type PayloadConfig = internalconfig.PayloadConfig
type PayloadRule = internalconfig.PayloadRule
type PayloadFilterRule = internalconfig.PayloadFilterRule
type PayloadModelRule = internalconfig.PayloadModelRule

const (
	DefaultCodexFingerprintMode               = internalconfig.DefaultCodexFingerprintMode
	DefaultChatGPTWebAutoReloginMaxRetries    = internalconfig.DefaultChatGPTWebAutoReloginMaxRetries
	DefaultChatGPTWebAutoReloginJitterPercent = internalconfig.DefaultChatGPTWebAutoReloginJitterPercent
	DefaultChatGPTWebAutoReloginWorkers       = internalconfig.DefaultChatGPTWebAutoReloginWorkers
	DefaultChatGPTWebAutoReloginQueueSize     = internalconfig.DefaultChatGPTWebAutoReloginQueueSize
	DefaultChatGPTWebManualReloginConcurrency = internalconfig.DefaultChatGPTWebManualReloginConcurrency
	MaxChatGPTWebAutoReloginRetries           = internalconfig.MaxChatGPTWebAutoReloginRetries
	MaxChatGPTWebAutoReloginJitterPercent     = internalconfig.MaxChatGPTWebAutoReloginJitterPercent
	MaxChatGPTWebAutoReloginWorkers           = internalconfig.MaxChatGPTWebAutoReloginWorkers
	MaxChatGPTWebAutoReloginQueueSize         = internalconfig.MaxChatGPTWebAutoReloginQueueSize
	MinChatGPTWebManualReloginConcurrency     = internalconfig.MinChatGPTWebManualReloginConcurrency
	// Deprecated: this advisory alias is not a validation limit. Use RecommendedMaxChatGPTWebManualReloginConcurrency.
	MaxChatGPTWebManualReloginConcurrency = internalconfig.MaxChatGPTWebManualReloginConcurrency
)

const RecommendedMaxChatGPTWebManualReloginConcurrency = internalconfig.RecommendedMaxChatGPTWebManualReloginConcurrency

type GeminiKey = internalconfig.GeminiKey
type CodexKey = internalconfig.CodexKey
type CodexCustomModel = internalconfig.CodexCustomModel
type ClaudeKey = internalconfig.ClaudeKey
type VertexCompatKey = internalconfig.VertexCompatKey
type VertexCompatModel = internalconfig.VertexCompatModel
type OpenAICompatibility = internalconfig.OpenAICompatibility
type OpenAICompatibilityAPIKey = internalconfig.OpenAICompatibilityAPIKey
type OpenAICompatibilityModel = internalconfig.OpenAICompatibilityModel

type TLS = internalconfig.TLSConfig

const (
	DefaultPanelGitHubRepository               = internalconfig.DefaultPanelGitHubRepository
	ChatGPTWebResizeFilterCatmullRom           = internalconfig.ChatGPTWebResizeFilterCatmullRom
	ChatGPTWebResizeFilterApproxBiLinear       = internalconfig.ChatGPTWebResizeFilterApproxBiLinear
	DefaultChatGPTWebResizeFilter              = internalconfig.DefaultChatGPTWebResizeFilter
	DefaultChatGPTWebMaxImageResponseMegabytes = internalconfig.DefaultChatGPTWebMaxImageResponseMegabytes
	MinChatGPTWebMaxImageResponseMegabytes     = internalconfig.MinChatGPTWebMaxImageResponseMegabytes
	MaxChatGPTWebMaxImageResponseMegabytes     = internalconfig.MaxChatGPTWebMaxImageResponseMegabytes
	DefaultChatGPTWebMaxN                      = internalconfig.DefaultChatGPTWebMaxN
	DefaultChatGPTWebImageMaxInFlight          = internalconfig.DefaultChatGPTWebImageMaxInFlight
	MinChatGPTWebImageMaxInFlight              = internalconfig.MinChatGPTWebImageMaxInFlight
	RecommendedMaxChatGPTWebImageMaxInFlight   = internalconfig.RecommendedMaxChatGPTWebImageMaxInFlight
	// Deprecated: this advisory alias is not a validation limit. Use RecommendedMaxChatGPTWebImageMaxInFlight.
	MaxChatGPTWebImageMaxInFlight                   = internalconfig.MaxChatGPTWebImageMaxInFlight
	DefaultChatGPTWebImageAdmissionQueueSize        = internalconfig.DefaultChatGPTWebImageAdmissionQueueSize
	MinChatGPTWebImageAdmissionQueueSize            = internalconfig.MinChatGPTWebImageAdmissionQueueSize
	RecommendedMaxChatGPTWebImageAdmissionQueueSize = internalconfig.RecommendedMaxChatGPTWebImageAdmissionQueueSize
	// Deprecated: this advisory alias is not a validation limit. Use RecommendedMaxChatGPTWebImageAdmissionQueueSize.
	MaxChatGPTWebImageAdmissionQueueSize         = internalconfig.MaxChatGPTWebImageAdmissionQueueSize
	DefaultChatGPTWebImageAdmissionWaitMS        = internalconfig.DefaultChatGPTWebImageAdmissionWaitMS
	MinChatGPTWebImageAdmissionWaitMS            = internalconfig.MinChatGPTWebImageAdmissionWaitMS
	RecommendedMaxChatGPTWebImageAdmissionWaitMS = internalconfig.RecommendedMaxChatGPTWebImageAdmissionWaitMS
	// Deprecated: this advisory alias is not a validation limit. Use RecommendedMaxChatGPTWebImageAdmissionWaitMS.
	MaxChatGPTWebImageAdmissionWaitMS          = internalconfig.MaxChatGPTWebImageAdmissionWaitMS
	DefaultChatGPTWebImageMaxFinalizers        = internalconfig.DefaultChatGPTWebImageMaxFinalizers
	MinChatGPTWebImageMaxFinalizers            = internalconfig.MinChatGPTWebImageMaxFinalizers
	RecommendedMaxChatGPTWebImageMaxFinalizers = internalconfig.RecommendedMaxChatGPTWebImageMaxFinalizers
	// Deprecated: this advisory alias is not a validation limit. Use RecommendedMaxChatGPTWebImageMaxFinalizers.
	MaxChatGPTWebImageMaxFinalizers                  = internalconfig.MaxChatGPTWebImageMaxFinalizers
	DefaultChatGPTWebImageCompletionReserveMB        = internalconfig.DefaultChatGPTWebImageCompletionReserveMB
	MinChatGPTWebImageCompletionReserveMB            = internalconfig.MinChatGPTWebImageCompletionReserveMB
	RecommendedMaxChatGPTWebImageCompletionReserveMB = internalconfig.RecommendedMaxChatGPTWebImageCompletionReserveMB
	// Deprecated: this advisory alias is not a validation limit. Use RecommendedMaxChatGPTWebImageCompletionReserveMB.
	MaxChatGPTWebImageCompletionReserveMB         = internalconfig.MaxChatGPTWebImageCompletionReserveMB
	DefaultChatGPTWebImageMemoryCapacityMB        = internalconfig.DefaultChatGPTWebImageMemoryCapacityMB
	MinChatGPTWebImageMemoryCapacityMB            = internalconfig.MinChatGPTWebImageMemoryCapacityMB
	RecommendedMinChatGPTWebImageMemoryCapacityMB = internalconfig.RecommendedMinChatGPTWebImageMemoryCapacityMB
	RecommendedMaxChatGPTWebImageMemoryCapacityMB = internalconfig.RecommendedMaxChatGPTWebImageMemoryCapacityMB
	// Deprecated: this advisory alias is not a validation limit. Use RecommendedMaxChatGPTWebImageMemoryCapacityMB.
	MaxChatGPTWebImageMemoryCapacityMB = internalconfig.MaxChatGPTWebImageMemoryCapacityMB
	MinChatGPTWebMaxN                  = internalconfig.MinChatGPTWebMaxN
	MaxChatGPTWebMaxN                  = internalconfig.MaxChatGPTWebMaxN
)

const (
	DefaultChatGPTWebImagePollConcurrency        = internalconfig.DefaultChatGPTWebImagePollConcurrency
	MinChatGPTWebImagePollConcurrency            = internalconfig.MinChatGPTWebImagePollConcurrency
	RecommendedMaxChatGPTWebImagePollConcurrency = internalconfig.RecommendedMaxChatGPTWebImagePollConcurrency
	// Deprecated: this advisory alias is not a validation limit. Use RecommendedMaxChatGPTWebImagePollConcurrency.
	MaxChatGPTWebImagePollConcurrency                       = internalconfig.MaxChatGPTWebImagePollConcurrency
	DefaultChatGPTWebImageMemoryFinalizerConcurrency        = internalconfig.DefaultChatGPTWebImageMemoryFinalizerConcurrency
	MinChatGPTWebImageMemoryFinalizerConcurrency            = internalconfig.MinChatGPTWebImageMemoryFinalizerConcurrency
	RecommendedMaxChatGPTWebImageMemoryFinalizerConcurrency = internalconfig.RecommendedMaxChatGPTWebImageMemoryFinalizerConcurrency
	// Deprecated: this advisory alias is not a validation limit. Use RecommendedMaxChatGPTWebImageMemoryFinalizerConcurrency.
	MaxChatGPTWebImageMemoryFinalizerConcurrency = internalconfig.MaxChatGPTWebImageMemoryFinalizerConcurrency
)

func LoadConfig(configFile string) (*Config, error) { return internalconfig.LoadConfig(configFile) }

func LoadConfigOptional(configFile string, optional bool) (*Config, error) {
	return internalconfig.LoadConfigOptional(configFile, optional)
}

func Clone(input *Config) (*Config, error) { return internalconfig.Clone(input) }

func SaveConfigPreserveComments(configFile string, cfg *Config) error {
	return internalconfig.SaveConfigPreserveComments(configFile, cfg)
}

func SaveConfigPreserveCommentsUpdateNestedScalar(configFile string, path []string, value string) error {
	return internalconfig.SaveConfigPreserveCommentsUpdateNestedScalar(configFile, path, value)
}

func NormalizeCommentIndentation(data []byte) []byte {
	return internalconfig.NormalizeCommentIndentation(data)
}

func NormalizeRequestBodyRelease(in RequestBodyReleaseConfig) RequestBodyReleaseConfig {
	return internalconfig.NormalizeRequestBodyRelease(in)
}
