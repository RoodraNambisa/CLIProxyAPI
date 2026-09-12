package helps

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	translatorcommon "github.com/router-for-me/CLIProxyAPI/v6/internal/translator/common"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/usage"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

type UsageReporter struct {
	provider             string
	model                string
	authID               string
	authIndex            string
	apiKey               string
	source               string
	requestServiceTier   string
	stream               bool
	generate             bool
	requestedAt          time.Time
	timing               usageResponseTiming
	mu                   sync.Mutex
	published            bool
	observedDetail       usage.Detail
	observed             bool
	claudeUsage          translatorcommon.ClaudeUsage
	claudeUsageDone      bool
	observedAdditional   []observedModelUsage
	executionDiagnostics *cliproxyexecutor.RequestExecutionDiagnostics
	requestUsageOutcome  *cliproxyexecutor.RequestUsageOutcome
	requestUsageAttempt  *cliproxyexecutor.RequestUsageAttempt
	diagnosticSecrets    []string
}

type observedModelUsage struct {
	model  string
	detail usage.Detail
}

type usageExecutor interface {
	Identifier() string
}

// NewExecutorUsageReporter constructs a reporter from a provider executor.
func NewExecutorUsageReporter(ctx context.Context, executor usageExecutor, model string, auth *cliproxyauth.Auth, stream ...bool) *UsageReporter {
	provider := ""
	if executor != nil {
		provider = executor.Identifier()
	}
	return NewUsageReporter(ctx, provider, model, auth, stream...)
}

// NewUsageReporter snapshots usage metadata. The optional stream argument is
// an entry-point fallback. An existing client
// mode in ctx takes precedence over the transport used by an executor.
func NewUsageReporter(ctx context.Context, provider, model string, auth *cliproxyauth.Auth, stream ...bool) *UsageReporter {
	if len(stream) > 0 {
		ctx = usage.WithStreamDefault(ctx, stream[0])
	}
	apiKey := APIKeyFromContext(ctx)
	reporter := &UsageReporter{
		provider:             provider,
		model:                model,
		requestedAt:          time.Now(),
		apiKey:               apiKey,
		source:               resolveUsageSource(auth, apiKey),
		requestServiceTier:   usage.ServiceTierFromContext(ctx),
		stream:               usage.StreamFromContext(ctx),
		generate:             usage.GenerateFromContext(ctx),
		requestUsageOutcome:  cliproxyexecutor.RequestUsageOutcomeFromContext(ctx),
		requestUsageAttempt:  cliproxyexecutor.RequestUsageAttemptFromContext(ctx),
		executionDiagnostics: cliproxyexecutor.RequestExecutionDiagnosticsFromContext(ctx),
	}
	if auth != nil {
		reporter.authID = auth.ID
		reporter.authIndex = auth.EnsureIndex()
		for _, name := range []string{"api_key", "access_token", "refresh_token", "id_token"} {
			if value := auth.Attributes[name]; value != "" {
				reporter.diagnosticSecrets = append(reporter.diagnosticSecrets, value)
			}
			if value, ok := auth.Metadata[name].(string); ok && value != "" {
				reporter.diagnosticSecrets = append(reporter.diagnosticSecrets, value)
			}
		}
	}
	return reporter
}

// SetExecutionDiagnostics attaches request-scoped execution ownership metadata.
func (r *UsageReporter) SetExecutionDiagnostics(diagnostics *cliproxyexecutor.RequestExecutionDiagnostics) {
	if r != nil {
		r.executionDiagnostics = diagnostics
	}
}

// SetRequestUsageOutcome coordinates primary records across internal auth
// attempts that belong to one downstream request.
func (r *UsageReporter) SetRequestUsageOutcome(outcome *cliproxyexecutor.RequestUsageOutcome) {
	if r != nil {
		r.requestUsageOutcome = outcome
	}
}

// SetRequestServiceTierFromPayload records the translated upstream service tier.
func (r *UsageReporter) SetRequestServiceTierFromPayload(payload []byte) {
	if r == nil {
		return
	}
	if serviceTier, ok := extractServiceTierFromPayload(payload); ok {
		r.requestServiceTier = serviceTier
	}
}

func (r *UsageReporter) Publish(ctx context.Context, detail usage.Detail) {
	r.publishWithOutcome(ctx, detail, false, true)
}

// Observe stores usage seen before a stream reaches a successful terminal
// event. A later failure can still win without emitting a second record.
func (r *UsageReporter) Observe(detail usage.Detail) {
	if r == nil {
		return
	}
	detail = normalizeUsageDetailTotal(detail)
	r.mu.Lock()
	if !r.published {
		r.observedDetail = mergeObservedUsageDetail(r.observedDetail, detail, reportsReasoningSeparately(r.provider))
		r.observed = true
	}
	r.mu.Unlock()
}

func mergeObservedUsageDetail(current, next usage.Detail, reasoningSeparate bool) usage.Detail {
	merged := current
	if next.InputTokens != 0 {
		merged.InputTokens = next.InputTokens
	}
	if reasoningSeparate {
		baseOutput := merged.OutputTokens - merged.ReasoningTokens
		if baseOutput < 0 {
			baseOutput = 0
		}
		if next.OutputTokens != 0 {
			nextBaseOutput := next.OutputTokens - next.ReasoningTokens
			if nextBaseOutput < 0 {
				nextBaseOutput = 0
			}
			if nextBaseOutput > 0 || next.ReasoningTokens == 0 {
				baseOutput = nextBaseOutput
			}
		}
		if next.ReasoningTokens != 0 {
			merged.ReasoningTokens = next.ReasoningTokens
		}
		merged.OutputTokens = sumUsageTokens(baseOutput, merged.ReasoningTokens)
	} else {
		if next.OutputTokens != 0 {
			merged.OutputTokens = next.OutputTokens
		}
		if next.ReasoningTokens != 0 {
			merged.ReasoningTokens = next.ReasoningTokens
		}
	}
	if next.CachedTokens != 0 {
		merged.CachedTokens = next.CachedTokens
	}
	if next.CacheCreationTokens != 0 {
		merged.CacheCreationTokens = next.CacheCreationTokens
	}
	if tier := strings.TrimSpace(next.ResponseServiceTier); tier != "" {
		merged.ResponseServiceTier = tier
	}
	total := sumUsageTokens(merged.InputTokens, merged.OutputTokens)
	if next.TotalTokens > total {
		total = next.TotalTokens
	}
	if current.TotalTokens > total {
		total = current.TotalTokens
	}
	merged.TotalTokens = total
	return merged
}

func reportsReasoningSeparately(provider string) bool {
	switch strings.ToLower(strings.TrimSpace(provider)) {
	case "aistudio", "antigravity", "gemini", "gemini-interactions", "vertex":
		return true
	default:
		return false
	}
}

// SetTranslatedReasoningEffort is retained for executor compatibility. The v6
// usage record does not persist reasoning-effort metadata.
func (r *UsageReporter) SetTranslatedReasoningEffort(_ []byte, _ string) {}

func (r *UsageReporter) PublishAdditionalModel(ctx context.Context, model string, detail usage.Detail) {
	record, ok := r.buildAdditionalModelRecord(model, detail)
	if !ok {
		return
	}
	usage.PublishRecord(ctx, record)
}

// ObserveAdditionalModel stages secondary-model usage until the primary
// stream reaches a successful terminal event.
func (r *UsageReporter) ObserveAdditionalModel(model string, detail usage.Detail) {
	if r == nil {
		return
	}
	model = strings.TrimSpace(model)
	detail = normalizeUsageDetailTotal(detail)
	if model == "" || !hasNonZeroTokenUsage(detail) {
		return
	}
	r.mu.Lock()
	if !r.published {
		r.observedAdditional = append(r.observedAdditional, observedModelUsage{model: model, detail: detail})
	}
	r.mu.Unlock()
}

func (r *UsageReporter) buildAdditionalModelRecord(model string, detail usage.Detail) (usage.Record, bool) {
	if r == nil {
		return usage.Record{}, false
	}
	model = strings.TrimSpace(model)
	if model == "" {
		return usage.Record{}, false
	}
	detail = normalizeUsageDetailTotal(detail)
	if !hasNonZeroTokenUsage(detail) {
		return usage.Record{}, false
	}
	record := r.buildRecordForModel(model, detail, false)
	record.Auxiliary = true
	return record, true
}

func (r *UsageReporter) PublishFailure(ctx context.Context, causes ...error) {
	r.publishWithOutcome(ctx, usage.Detail{}, true, true, causes...)
}

// PublishFailureWithUsage preserves token counts explicitly reported by an
// upstream error response while retaining the final request failure outcome.
func (r *UsageReporter) PublishFailureWithUsage(ctx context.Context, detail usage.Detail, causes ...error) {
	r.publishWithOutcome(ctx, detail, true, true, causes...)
}

func (r *UsageReporter) TrackFailure(ctx context.Context, errPtr *error) {
	if r == nil || errPtr == nil {
		return
	}
	if *errPtr != nil {
		r.PublishFailure(ctx, *errPtr)
	}
}

func (r *UsageReporter) publishWithOutcome(ctx context.Context, detail usage.Detail, failed, explicitDetail bool, causes ...error) {
	if r == nil {
		return
	}
	detail = normalizeUsageDetailTotal(detail)
	r.mu.Lock()
	if r.published {
		r.mu.Unlock()
		return
	}
	r.published = true
	if !failed && !explicitDetail && r.observed {
		detail = r.observedDetail
	}
	additional := r.observedAdditional
	r.observedAdditional = nil
	r.mu.Unlock()

	if !failed && r.executionDiagnostics != nil {
		r.executionDiagnostics.ClearFailure()
	}
	primaryRecord := r.buildRecord(detail, failed)
	if failed {
		var cause error
		for _, candidate := range causes {
			if candidate != nil {
				cause = candidate
				break
			}
		}
		populateUsageFailure(ctx, &primaryRecord, cause, r.diagnosticSecrets...)
		logUsageAttemptFailure(ctx, primaryRecord)
	}
	additionalRecords := make([]usage.Record, 0, len(additional))
	if !failed {
		for i := range additional {
			record := r.buildRecordForModel(additional[i].model, additional[i].detail, false)
			record.Auxiliary = true
			additionalRecords = append(additionalRecords, record)
		}
	}
	publish := func() {
		usage.PublishRecord(ctx, primaryRecord)
		if failed {
			return
		}
		for i := range additionalRecords {
			usage.PublishRecord(ctx, additionalRecords[i])
		}
	}
	if r.requestUsageOutcome == nil {
		publish()
		return
	}
	if failed {
		r.requestUsageOutcome.StageFailureForAttempt(r.requestUsageAttempt, publish)
		return
	}
	r.requestUsageOutcome.PublishSuccessForAttempt(r.requestUsageAttempt, publish)
}

func normalizeUsageDetailTotal(detail usage.Detail) usage.Detail {
	if detail.TotalTokens == 0 {
		input := maxUsageTokens(detail.InputTokens, detail.CachedTokens, detail.CacheCreationTokens)
		output := maxUsageTokens(detail.OutputTokens, detail.ReasoningTokens)
		total := sumUsageTokens(input, output)
		if total > 0 {
			detail.TotalTokens = total
		}
	}
	return detail
}

func hasNonZeroTokenUsage(detail usage.Detail) bool {
	return detail.InputTokens != 0 ||
		detail.OutputTokens != 0 ||
		detail.ReasoningTokens != 0 ||
		detail.CachedTokens != 0 ||
		detail.CacheCreationTokens != 0 ||
		detail.TotalTokens != 0
}

// EnsurePublished guarantees that a successful usage record is emitted once.
// Any usage observed before the terminal event is included in that record.
// This is used to ensure request counting even when upstream responses do not
// include any usage fields (tokens), especially for streaming paths.
func (r *UsageReporter) EnsurePublished(ctx context.Context) {
	if r == nil {
		return
	}
	r.publishWithOutcome(ctx, usage.Detail{}, false, false)
}

func (r *UsageReporter) buildRecord(detail usage.Detail, failed bool) usage.Record {
	if r == nil {
		return usage.Record{Detail: detail, Failed: failed, Generate: usage.GenerateFlag(true)}
	}
	return r.buildRecordForModel(r.model, detail, failed)
}

func (r *UsageReporter) buildRecordForModel(model string, detail usage.Detail, failed bool) usage.Record {
	if r == nil {
		return usage.Record{Model: model, Detail: detail, Failed: failed, Generate: usage.GenerateFlag(true)}
	}
	ttft, firstPacket := r.responseTimings()
	record := usage.Record{
		Provider:            r.provider,
		Model:               model,
		Source:              r.source,
		APIKey:              r.apiKey,
		AuthID:              r.authID,
		AuthIndex:           r.authIndex,
		RequestServiceTier:  r.requestServiceTier,
		Stream:              r.stream,
		Generate:            usage.GenerateFlag(r.generate),
		ResponseServiceTier: strings.TrimSpace(detail.ResponseServiceTier),
		RequestedAt:         r.requestedAt,
		Latency:             r.latency(),
		TTFT:                ttft,
		FirstPacketLatency:  firstPacket,
		Failed:              failed,
		Detail:              detail,
	}
	if r.executionDiagnostics != nil {
		diagnostics := r.executionDiagnostics.Snapshot()
		record.FailureStage = diagnostics.FailureStage
		record.ErrorCode = diagnostics.ErrorCode
		record.CredentialSelected = diagnostics.CredentialSelected
		record.UpstreamCommitted = diagnostics.UpstreamCommitted
		record.AuthRequestSlotConsumed = diagnostics.AuthRequestSlotConsumed
	}
	return record
}

func extractServiceTierFromPayload(payload []byte) (string, bool) {
	for _, path := range []string{"service_tier", "request.service_tier"} {
		if tier := strings.TrimSpace(gjson.GetBytes(payload, path).String()); tier != "" {
			return tier, true
		}
	}
	return "", false
}

func (r *UsageReporter) latency() time.Duration {
	if r == nil || r.requestedAt.IsZero() {
		return 0
	}
	latency := time.Since(r.requestedAt)
	if latency < 0 {
		return 0
	}
	return latency
}

func APIKeyFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	ginCtx, ok := ctx.Value("gin").(*gin.Context)
	if !ok || ginCtx == nil {
		return ""
	}
	if v, exists := ginCtx.Get("apiKey"); exists {
		switch value := v.(type) {
		case string:
			return value
		case fmt.Stringer:
			return value.String()
		default:
			return fmt.Sprintf("%v", value)
		}
	}
	return ""
}

func resolveUsageSource(auth *cliproxyauth.Auth, ctxAPIKey string) string {
	if auth != nil {
		provider := strings.TrimSpace(auth.Provider)
		if strings.EqualFold(provider, "vertex") {
			if auth.Metadata != nil {
				if projectID, ok := auth.Metadata["project_id"].(string); ok {
					if trimmed := strings.TrimSpace(projectID); trimmed != "" {
						return trimmed
					}
				}
				if project, ok := auth.Metadata["project"].(string); ok {
					if trimmed := strings.TrimSpace(project); trimmed != "" {
						return trimmed
					}
				}
			}
		}
		if _, value := auth.AccountInfo(); value != "" {
			return strings.TrimSpace(value)
		}
		if auth.Metadata != nil {
			if email, ok := auth.Metadata["email"].(string); ok {
				if trimmed := strings.TrimSpace(email); trimmed != "" {
					return trimmed
				}
			}
		}
		if auth.Attributes != nil {
			if key := strings.TrimSpace(auth.Attributes["api_key"]); key != "" {
				return key
			}
		}
	}
	if trimmed := strings.TrimSpace(ctxAPIKey); trimmed != "" {
		return trimmed
	}
	return ""
}

// StreamUsageBuffer keeps the final token usage while retaining a service tier
// reported on an earlier stream event.
type StreamUsageBuffer struct {
	detail usage.Detail
	ok     bool
}

var (
	openAIStreamUsageMarker       = []byte(`"usage"`)
	openAIStreamServiceTierMarker = []byte(`"service_tier"`)
)

// Observe records a candidate usage detail.
func (b *StreamUsageBuffer) Observe(detail usage.Detail, ok bool) {
	if b == nil || !ok {
		return
	}
	responseServiceTier := strings.TrimSpace(detail.ResponseServiceTier)
	if responseServiceTier == "" || hasNonZeroTokenUsage(detail) {
		preservedTier := b.detail.ResponseServiceTier
		b.detail = detail
		if b.detail.ResponseServiceTier == "" {
			b.detail.ResponseServiceTier = preservedTier
		}
	} else {
		b.detail.ResponseServiceTier = responseServiceTier
	}
	b.ok = true
}

// ObserveOpenAIStream avoids JSON parsing for chunks without usage or service tier markers.
func (b *StreamUsageBuffer) ObserveOpenAIStream(line []byte) {
	if b == nil {
		return
	}
	b.Observe(ParseOpenAIStreamUsage(line))
}

// Publish stages the buffered usage detail until the reporter records a
// successful terminal event. The method name is retained for compatibility.
func (b *StreamUsageBuffer) Publish(_ context.Context, reporter *UsageReporter) bool {
	if b == nil || !b.ok || reporter == nil {
		return false
	}
	reporter.Observe(b.detail)
	return true
}

// Detail returns the buffered usage detail.
func (b *StreamUsageBuffer) Detail() (usage.Detail, bool) {
	if b == nil || !b.ok {
		return usage.Detail{}, false
	}
	return b.detail, true
}

func ParseCodexUsage(data []byte) (usage.Detail, bool) {
	responseServiceTier := extractResponseServiceTier(data)
	usageNode := gjson.ParseBytes(data).Get("response.usage")
	if !hasOpenAIStyleUsageTokenFields(usageNode) {
		if responseServiceTier == "" {
			return usage.Detail{}, false
		}
		return usage.Detail{ResponseServiceTier: responseServiceTier}, true
	}
	detail := parseOpenAIStyleUsageNode(usageNode)
	detail.ResponseServiceTier = responseServiceTier
	return detail, true
}

func extractResponseServiceTier(payload []byte) string {
	if len(payload) == 0 || !gjson.ValidBytes(payload) {
		return ""
	}
	return extractResponseServiceTierFromValidJSON(payload)
}

func extractResponseServiceTierFromValidJSON(payload []byte) string {
	for _, path := range []string{"response.service_tier", "service_tier", "interaction.service_tier"} {
		if tier := strings.TrimSpace(gjson.GetBytes(payload, path).String()); tier != "" {
			return tier
		}
	}
	return ""
}

func ParseCodexImageToolUsage(data []byte) (usage.Detail, bool) {
	usageNode := gjson.ParseBytes(data).Get("response.tool_usage.image_gen")
	if !hasOpenAIStyleUsageTokenFields(usageNode) {
		return usage.Detail{}, false
	}
	return parseOpenAIStyleUsageNode(usageNode), true
}

func ParseOpenAIUsage(data []byte) usage.Detail {
	responseServiceTier := extractResponseServiceTier(data)
	usageNode := gjson.ParseBytes(data).Get("usage")
	if !hasOpenAIStyleUsageTokenFields(usageNode) {
		return usage.Detail{ResponseServiceTier: responseServiceTier}
	}
	detail := parseOpenAIStyleUsageNode(usageNode)
	detail.ResponseServiceTier = responseServiceTier
	return detail
}

func hasOpenAIStyleUsageTokenFields(usageNode gjson.Result) bool {
	if !usageNode.Exists() || !usageNode.IsObject() {
		return false
	}
	return usageNode.Get("prompt_tokens").Exists() ||
		usageNode.Get("input_tokens").Exists() ||
		usageNode.Get("completion_tokens").Exists() ||
		usageNode.Get("output_tokens").Exists() ||
		usageNode.Get("total_tokens").Exists() ||
		usageNode.Get("prompt_tokens_details.cached_tokens").Exists() ||
		openAIUsageCacheCreationNode(usageNode).Exists() ||
		usageNode.Get("input_tokens_details.cached_tokens").Exists() ||
		usageNode.Get("completion_tokens_details.reasoning_tokens").Exists() ||
		usageNode.Get("output_tokens_details.reasoning_tokens").Exists()
}

func parseOpenAIStyleUsageNode(usageNode gjson.Result) usage.Detail {
	inputNode := usageNode.Get("prompt_tokens")
	if !inputNode.Exists() {
		inputNode = usageNode.Get("input_tokens")
	}
	outputNode := usageNode.Get("completion_tokens")
	if !outputNode.Exists() {
		outputNode = usageNode.Get("output_tokens")
	}
	detail := usage.Detail{
		InputTokens:  inputNode.Int(),
		OutputTokens: outputNode.Int(),
		TotalTokens:  usageNode.Get("total_tokens").Int(),
	}
	cached := usageNode.Get("prompt_tokens_details.cached_tokens")
	if !cached.Exists() {
		cached = usageNode.Get("input_tokens_details.cached_tokens")
	}
	if cached.Exists() {
		detail.CachedTokens = cached.Int()
	}
	cacheCreation := openAIUsageCacheCreationNode(usageNode)
	if cacheCreation.Exists() {
		detail.CacheCreationTokens = cacheCreation.Int()
	}
	reasoning := usageNode.Get("completion_tokens_details.reasoning_tokens")
	if !reasoning.Exists() {
		reasoning = usageNode.Get("output_tokens_details.reasoning_tokens")
	}
	if reasoning.Exists() {
		detail.ReasoningTokens = reasoning.Int()
	}
	return detail
}

func ParseOpenAIStreamUsage(line []byte) (usage.Detail, bool) {
	if !bytes.Contains(line, openAIStreamUsageMarker) && !bytes.Contains(line, openAIStreamServiceTierMarker) && !bytes.Contains(line, []byte(`\u`)) {
		return usage.Detail{}, false
	}
	payload := jsonPayload(line)
	if len(payload) == 0 || !gjson.ValidBytes(payload) {
		return usage.Detail{}, false
	}
	responseServiceTier := extractResponseServiceTierFromValidJSON(payload)
	usageNode := gjson.GetBytes(payload, "usage")
	hasTokens := hasOpenAIStyleUsageTokenFields(usageNode)
	if !hasTokens {
		usageNode = gjson.GetBytes(payload, "response.usage")
		hasTokens = hasOpenAIStyleUsageTokenFields(usageNode)
	}
	if !hasTokens {
		if responseServiceTier == "" {
			return usage.Detail{}, false
		}
		return usage.Detail{ResponseServiceTier: responseServiceTier}, true
	}
	detail := parseOpenAIStyleUsageNode(usageNode)
	detail.ResponseServiceTier = responseServiceTier
	return detail, true
}

func openAIUsageCacheCreationNode(node gjson.Result) gjson.Result {
	// Keep the existing local aliases first when an upstream sends conflicting fields.
	for _, path := range []string{
		"input_tokens_details.cache_write_tokens",
		"prompt_tokens_details.cached_creation_tokens",
		"input_tokens_details.cache_creation_tokens",
		"prompt_tokens_details.cache_creation_tokens",
		"prompt_tokens_details.cache_write_tokens",
	} {
		if value := node.Get(path); value.Exists() {
			return value
		}
	}
	return gjson.Result{}
}

func ParseClaudeUsage(data []byte) usage.Detail {
	var value translatorcommon.ClaudeUsage
	value.Merge(gjson.ParseBytes(data).Get("usage"))
	return claudeUsageDetail(value)
}

func ParseClaudeStreamUsage(line []byte) (usage.Detail, bool) {
	node, _ := claudeStreamUsageNode(line)
	var value translatorcommon.ClaudeUsage
	valid := value.Merge(node)
	return claudeUsageDetail(value), valid
}

func parseGeminiFamilyUsageDetail(node gjson.Result) usage.Detail {
	reasoningTokens := node.Get("thoughtsTokenCount").Int()
	detail := usage.Detail{
		InputTokens:     translatorcommon.GeminiInputTokens(node),
		OutputTokens:    sumUsageTokens(node.Get("candidatesTokenCount").Int(), reasoningTokens),
		ReasoningTokens: reasoningTokens,
		TotalTokens:     node.Get("totalTokenCount").Int(),
		CachedTokens:    node.Get("cachedContentTokenCount").Int(),
	}
	if detail.TotalTokens == 0 {
		detail.TotalTokens = sumUsageTokens(detail.InputTokens, detail.OutputTokens)
	}
	return detail
}

func parseInteractionsUsageDetail(node gjson.Result) usage.Detail {
	cacheReadTokens := firstInteractionsUsageNode(node, "cache_read_tokens", "cacheReadTokens").Int()
	cachedTokens := firstInteractionsUsageNode(node, "cached_tokens", "cachedContentTokenCount", "total_cached_tokens").Int()
	if cachedTokens == 0 {
		cachedTokens = cacheReadTokens
	}
	reasoningTokens := firstInteractionsUsageNode(node, "reasoning_tokens", "thoughtsTokenCount", "total_thought_tokens").Int()
	detail := usage.Detail{
		InputTokens: sumUsageTokens(
			firstInteractionsUsageNode(node, "input_tokens", "prompt_tokens", "total_input_tokens").Int(),
			firstInteractionsUsageNode(node, "tool_use_tokens", "total_tool_use_tokens", "toolUseTokens", "totalToolUseTokens").Int(),
		),
		OutputTokens:        sumUsageTokens(firstInteractionsUsageNode(node, "output_tokens", "completion_tokens", "total_output_tokens").Int(), reasoningTokens),
		ReasoningTokens:     reasoningTokens,
		TotalTokens:         firstInteractionsUsageNode(node, "total_tokens", "totalTokenCount").Int(),
		CachedTokens:        cachedTokens,
		CacheCreationTokens: firstInteractionsUsageNode(node, "cache_creation_tokens", "cacheCreationTokens", "cache_write_tokens", "cacheWriteTokens").Int(),
	}
	if detail.TotalTokens == 0 {
		detail.TotalTokens = sumUsageTokens(detail.InputTokens, detail.OutputTokens)
	}
	return detail
}

func sumUsageTokens(values ...int64) int64 {
	var total int64
	for _, value := range values {
		if value <= 0 {
			continue
		}
		if total > math.MaxInt64-value {
			return math.MaxInt64
		}
		total += value
	}
	return total
}

func maxUsageTokens(values ...int64) int64 {
	var maximum int64
	for _, value := range values {
		if value > maximum {
			maximum = value
		}
	}
	return maximum
}

func firstInteractionsUsageNode(root gjson.Result, paths ...string) gjson.Result {
	for _, path := range paths {
		if value := root.Get(path); value.Exists() {
			return value
		}
	}
	return gjson.Result{}
}

func hasUsageDetail(detail usage.Detail) bool {
	return hasNonZeroTokenUsage(detail)
}

// ParseInteractionsUsage extracts usage from native Interactions response envelopes.
func ParseInteractionsUsage(data []byte) usage.Detail {
	root := gjson.ParseBytes(data)
	node := firstInteractionsUsageNode(root, "usage", "total_usage", "metadata.total_usage", "metadata.usage", "usageMetadata", "usage_metadata", "interaction.usage", "interaction.total_usage", "interaction.metadata.total_usage", "interaction.metadata.usage")
	if !node.Exists() {
		return usage.Detail{}
	}
	if node.Get("promptTokenCount").Exists() || node.Get("candidatesTokenCount").Exists() {
		detail := parseGeminiFamilyUsageDetail(node)
		detail.ResponseServiceTier = extractResponseServiceTier(data)
		return detail
	}
	detail := parseInteractionsUsageDetail(node)
	detail.ResponseServiceTier = extractResponseServiceTier(data)
	return detail
}

// ParseInteractionsStreamUsage extracts usage from a native Interactions stream event.
func ParseInteractionsStreamUsage(line []byte) (usage.Detail, bool) {
	payload := jsonPayload(line)
	if len(payload) == 0 {
		payload = line
	}
	if len(payload) == 0 || !gjson.ValidBytes(payload) {
		return usage.Detail{}, false
	}
	detail := ParseInteractionsUsage(payload)
	if !hasUsageDetail(detail) {
		return usage.Detail{}, false
	}
	return detail, true
}

func ParseGeminiUsage(data []byte) usage.Detail {
	usageNode := gjson.ParseBytes(data)
	node := usageNode.Get("usageMetadata")
	if !node.Exists() {
		node = usageNode.Get("usage_metadata")
	}
	if !node.Exists() {
		return usage.Detail{}
	}
	return parseGeminiFamilyUsageDetail(node)
}

func ParseGeminiStreamUsage(line []byte) (usage.Detail, bool) {
	payload := jsonPayload(line)
	if len(payload) == 0 || !gjson.ValidBytes(payload) {
		return usage.Detail{}, false
	}
	node := gjson.GetBytes(payload, "usageMetadata")
	if !node.Exists() {
		node = gjson.GetBytes(payload, "usage_metadata")
	}
	if !node.Exists() {
		return usage.Detail{}, false
	}
	return parseGeminiFamilyUsageDetail(node), true
}

func ParseAntigravityUsage(data []byte) usage.Detail {
	usageNode := gjson.ParseBytes(data)
	node := usageNode.Get("response.usageMetadata")
	if !node.Exists() {
		node = usageNode.Get("response.usage_metadata")
	}
	if !node.Exists() {
		node = usageNode.Get("usageMetadata")
	}
	if !node.Exists() {
		node = usageNode.Get("usage_metadata")
	}
	if !node.Exists() {
		return usage.Detail{}
	}
	return parseGeminiFamilyUsageDetail(node)
}

func ParseAntigravityStreamUsage(line []byte) (usage.Detail, bool) {
	payload := jsonPayload(line)
	if len(payload) == 0 || !gjson.ValidBytes(payload) {
		return usage.Detail{}, false
	}
	node := gjson.GetBytes(payload, "response.usageMetadata")
	if !node.Exists() {
		node = gjson.GetBytes(payload, "response.usage_metadata")
	}
	if !node.Exists() {
		node = gjson.GetBytes(payload, "usageMetadata")
	}
	if !node.Exists() {
		node = gjson.GetBytes(payload, "usage_metadata")
	}
	if !node.Exists() {
		return usage.Detail{}, false
	}
	return parseGeminiFamilyUsageDetail(node), true
}

var stopChunkWithoutUsage sync.Map

func rememberStopWithoutUsage(traceID string) {
	stopChunkWithoutUsage.Store(traceID, struct{}{})
	time.AfterFunc(10*time.Minute, func() { stopChunkWithoutUsage.Delete(traceID) })
}

// GeminiTerminalAwaitsUsage reports whether a terminal stop event omits usage
// that may arrive in a follow-up event.
func GeminiTerminalAwaitsUsage(payload []byte) bool {
	if len(payload) == 0 || !gjson.ValidBytes(payload) {
		return false
	}
	return isStopChunkWithoutUsage(payload)
}

// FilterSSEUsageMetadata removes usageMetadata from SSE events that are not
// terminal (finishReason != "stop"). Stop chunks are left untouched. This
// function is shared between aistudio and antigravity executors.
func FilterSSEUsageMetadata(payload []byte) []byte {
	if len(payload) == 0 {
		return payload
	}

	lines := bytes.Split(payload, []byte("\n"))
	modified := false
	foundData := false
	for idx, line := range lines {
		trimmed := bytes.TrimSpace(line)
		if len(trimmed) == 0 || !bytes.HasPrefix(trimmed, []byte("data:")) {
			continue
		}
		foundData = true
		dataIdx := bytes.Index(line, []byte("data:"))
		if dataIdx < 0 {
			continue
		}
		rawJSON := bytes.TrimSpace(line[dataIdx+5:])
		traceID := gjson.GetBytes(rawJSON, "traceId").String()
		if isStopChunkWithoutUsage(rawJSON) && traceID != "" {
			rememberStopWithoutUsage(traceID)
			continue
		}
		if traceID != "" {
			if _, ok := stopChunkWithoutUsage.Load(traceID); ok && hasUsageMetadata(rawJSON) {
				stopChunkWithoutUsage.Delete(traceID)
				continue
			}
		}

		cleaned, changed := StripUsageMetadataFromJSON(rawJSON)
		if !changed {
			continue
		}
		var rebuilt []byte
		rebuilt = append(rebuilt, line[:dataIdx]...)
		rebuilt = append(rebuilt, []byte("data:")...)
		if len(cleaned) > 0 {
			rebuilt = append(rebuilt, ' ')
			rebuilt = append(rebuilt, cleaned...)
		}
		lines[idx] = rebuilt
		modified = true
	}
	if !modified {
		if !foundData {
			// Handle payloads that are raw JSON without SSE data: prefix.
			trimmed := bytes.TrimSpace(payload)
			cleaned, changed := StripUsageMetadataFromJSON(trimmed)
			if !changed {
				return payload
			}
			return cleaned
		}
		return payload
	}
	return bytes.Join(lines, []byte("\n"))
}

// StripUsageMetadataFromJSON drops usageMetadata unless finishReason is present (terminal).
// It handles both formats:
// - Aistudio: candidates.0.finishReason
// - Antigravity: response.candidates.0.finishReason
func StripUsageMetadataFromJSON(rawJSON []byte) ([]byte, bool) {
	jsonBytes := bytes.TrimSpace(rawJSON)
	if len(jsonBytes) == 0 || !gjson.ValidBytes(jsonBytes) {
		return rawJSON, false
	}

	// Check for finishReason in both aistudio and antigravity formats
	finishReason := gjson.GetBytes(jsonBytes, "candidates.0.finishReason")
	if !finishReason.Exists() {
		finishReason = gjson.GetBytes(jsonBytes, "response.candidates.0.finishReason")
	}
	terminalReason := finishReason.Exists() && strings.TrimSpace(finishReason.String()) != ""

	// Terminal chunk: keep as-is.
	if terminalReason {
		return rawJSON, false
	}

	if !hasUsageMetadata(jsonBytes) {
		return rawJSON, false
	}

	cleaned := jsonBytes
	var changed bool
	for _, item := range []struct {
		source string
		target string
	}{
		{source: "usageMetadata", target: "cpaUsageMetadata"},
		{source: "usage_metadata", target: "cpaUsageMetadata"},
		{source: "response.usageMetadata", target: "response.cpaUsageMetadata"},
		{source: "response.usage_metadata", target: "response.cpaUsageMetadata"},
	} {
		usageMetadata := gjson.GetBytes(cleaned, item.source)
		if !usageMetadata.Exists() {
			continue
		}
		cleaned, _ = sjson.SetRawBytes(cleaned, item.target, []byte(usageMetadata.Raw))
		cleaned, _ = sjson.DeleteBytes(cleaned, item.source)
		changed = true
	}

	return cleaned, changed
}

func hasUsageMetadata(jsonBytes []byte) bool {
	if len(jsonBytes) == 0 || !gjson.ValidBytes(jsonBytes) {
		return false
	}
	if gjson.GetBytes(jsonBytes, "usageMetadata").Exists() {
		return true
	}
	if gjson.GetBytes(jsonBytes, "response.usageMetadata").Exists() {
		return true
	}
	if gjson.GetBytes(jsonBytes, "usage_metadata").Exists() {
		return true
	}
	if gjson.GetBytes(jsonBytes, "response.usage_metadata").Exists() {
		return true
	}
	return false
}

func isStopChunkWithoutUsage(jsonBytes []byte) bool {
	if len(jsonBytes) == 0 || !gjson.ValidBytes(jsonBytes) {
		return false
	}
	finishReason := gjson.GetBytes(jsonBytes, "candidates.0.finishReason")
	if !finishReason.Exists() {
		finishReason = gjson.GetBytes(jsonBytes, "response.candidates.0.finishReason")
	}
	trimmed := strings.TrimSpace(finishReason.String())
	if !finishReason.Exists() || trimmed == "" {
		return false
	}
	return !hasUsageMetadata(jsonBytes)
}

func JSONPayload(line []byte) []byte {
	return jsonPayload(line)
}

func jsonPayload(line []byte) []byte {
	trimmed := bytes.TrimSpace(line)
	if len(trimmed) == 0 {
		return nil
	}
	if bytes.Equal(trimmed, []byte("[DONE]")) {
		return nil
	}
	if bytes.HasPrefix(trimmed, []byte("event:")) {
		return nil
	}
	if bytes.HasPrefix(trimmed, []byte("data:")) {
		trimmed = bytes.TrimSpace(trimmed[len("data:"):])
	}
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return nil
	}
	return trimmed
}
