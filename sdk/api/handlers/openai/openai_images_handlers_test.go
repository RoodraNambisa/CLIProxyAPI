package openai

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/constant"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/interfaces"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	executorhelps "github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	sdkaccess "github.com/router-for-me/CLIProxyAPI/v6/sdk/access"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/api/handlers"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	coreexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
	"github.com/tidwall/gjson"
)

type imageCaptureExecutor struct {
	provider                        string
	calls                           int
	streamCalls                     int
	model                           string
	requested                       string
	override                        string
	payload                         []byte
	stream                          bool
	sourceFormat                    string
	alt                             string
	maxResults                      int
	ignoreUnsupportedImageParams    bool
	hasIgnoreUnsupportedImageParams bool
	imageConfigSnapshot             coreexecutor.ChatGPTWebImageConfigSnapshot
	imageConfigSnapshots            []coreexecutor.ChatGPTWebImageConfigSnapshot
	hasImageConfigSnapshot          bool
	response                        []byte
	streamChunks                    []coreexecutor.StreamChunk
	beforeExecute                   func()
}

func (e *imageCaptureExecutor) Identifier() string {
	if strings.TrimSpace(e.provider) != "" {
		return e.provider
	}
	return "codex"
}

func (e *imageCaptureExecutor) Execute(ctx context.Context, auth *coreauth.Auth, req coreexecutor.Request, opts coreexecutor.Options) (coreexecutor.Response, error) {
	if e.beforeExecute != nil {
		e.beforeExecute()
	}
	e.calls++
	e.model = req.Model
	e.requested = strings.TrimSpace(stringValue(opts.Metadata[coreexecutor.RequestedModelMetadataKey]))
	e.override = strings.TrimSpace(stringValue(opts.Metadata[coreexecutor.ExecutionModelOverrideMetadataKey]))
	e.payload = append([]byte(nil), req.Payload...)
	e.stream = opts.Stream
	e.sourceFormat = opts.SourceFormat.String()
	e.alt = opts.Alt
	e.maxResults, _ = opts.Metadata[coreexecutor.ImageGenerationMaxResultsMetadataKey].(int)
	e.ignoreUnsupportedImageParams, e.hasIgnoreUnsupportedImageParams = opts.Metadata[coreexecutor.ChatGPTWebIgnoreUnsupportedImageParamsMetadataKey].(bool)
	e.imageConfigSnapshot, e.hasImageConfigSnapshot = opts.Metadata[coreexecutor.ChatGPTWebImageConfigSnapshotMetadataKey].(coreexecutor.ChatGPTWebImageConfigSnapshot)
	if e.hasImageConfigSnapshot {
		e.imageConfigSnapshots = append(e.imageConfigSnapshots, e.imageConfigSnapshot)
	}
	if len(e.response) == 0 {
		e.response = []byte(`{"created_at":1700000000,"output":[{"type":"image_generation_call","result":"aGVsbG8=","revised_prompt":"rev"}],"usage":{"total_tokens":3}}`)
	}
	return coreexecutor.Response{Payload: e.response}, nil
}

func (e *imageCaptureExecutor) ExecuteStream(_ context.Context, _ *coreauth.Auth, req coreexecutor.Request, opts coreexecutor.Options) (*coreexecutor.StreamResult, error) {
	e.streamCalls++
	e.model = req.Model
	e.requested = strings.TrimSpace(stringValue(opts.Metadata[coreexecutor.RequestedModelMetadataKey]))
	e.override = strings.TrimSpace(stringValue(opts.Metadata[coreexecutor.ExecutionModelOverrideMetadataKey]))
	e.payload = append([]byte(nil), req.Payload...)
	e.stream = opts.Stream
	e.sourceFormat = opts.SourceFormat.String()
	e.alt = opts.Alt
	e.maxResults, _ = opts.Metadata[coreexecutor.ImageGenerationMaxResultsMetadataKey].(int)
	e.ignoreUnsupportedImageParams, e.hasIgnoreUnsupportedImageParams = opts.Metadata[coreexecutor.ChatGPTWebIgnoreUnsupportedImageParamsMetadataKey].(bool)
	e.imageConfigSnapshot, e.hasImageConfigSnapshot = opts.Metadata[coreexecutor.ChatGPTWebImageConfigSnapshotMetadataKey].(coreexecutor.ChatGPTWebImageConfigSnapshot)
	if e.hasImageConfigSnapshot {
		e.imageConfigSnapshots = append(e.imageConfigSnapshots, e.imageConfigSnapshot)
	}
	ch := make(chan coreexecutor.StreamChunk, len(e.streamChunks))
	for _, chunk := range e.streamChunks {
		ch <- chunk
	}
	close(ch)
	return &coreexecutor.StreamResult{Chunks: ch}, nil
}

func (e *imageCaptureExecutor) Refresh(ctx context.Context, auth *coreauth.Auth) (*coreauth.Auth, error) {
	return auth, nil
}

func (e *imageCaptureExecutor) CountTokens(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (coreexecutor.Response, error) {
	return coreexecutor.Response{}, errors.New("not implemented")
}

func (e *imageCaptureExecutor) HttpRequest(context.Context, *coreauth.Auth, *http.Request) (*http.Response, error) {
	return nil, errors.New("not implemented")
}

func stringValue(raw any) string {
	switch v := raw.(type) {
	case string:
		return v
	case []byte:
		return string(v)
	default:
		return ""
	}
}

func assertImageToolNAbsent(t *testing.T, payload []byte) {
	t.Helper()
	if n := gjson.GetBytes(payload, "tools.0.n"); n.Exists() {
		t.Fatalf("tool n exists = %s, want absent", n.Raw)
	}
}

func defaultChatGPTWebImageConfigSnapshot() coreexecutor.ChatGPTWebImageConfigSnapshot {
	resolved := (sdkconfig.ChatGPTWebImageConfig{}).Resolved()
	return coreexecutor.ChatGPTWebImageConfigSnapshot{
		AdaptSizeToAspectRatio:     resolved.AdaptSizeToAspectRatio,
		StrictSize:                 resolved.StrictSize,
		AspectRatioMaxErrorPercent: resolved.AspectRatioMaxErrorPercent,
		MaxResizeEdgePixels:        resolved.MaxResizeEdgePixels,
		ResizeToRequestedSize:      resolved.ResizeToRequestedSize,
		ResizeFilter:               resolved.ResizeFilter,
		MaxImageResponseBytes:      resolved.MaxImageResponseMegabytes << 20,
	}
}

func newImagesTestHandler(t *testing.T, executor *imageCaptureExecutor, registeredModels ...string) *OpenAIImagesAPIHandler {
	t.Helper()
	manager := coreauth.NewManager(nil, nil, nil)
	manager.RegisterExecutor(executor)
	auth := &coreauth.Auth{ID: "codex-auth", Provider: executor.Identifier(), Status: coreauth.StatusActive}
	if auth.Provider == "chatgpt-web" {
		auth.Metadata = map[string]any{"access_token": "token", "lifecycle_state": coreauth.LifecycleStateActive}
	}
	if _, err := manager.Register(context.Background(), auth); err != nil {
		t.Fatalf("Register auth: %v", err)
	}
	if len(registeredModels) == 0 {
		registeredModels = []string{"gpt-image-2", "gpt-5.4-mini"}
	}
	models := make([]*registry.ModelInfo, 0, len(registeredModels))
	for _, modelID := range registeredModels {
		modelID = strings.TrimSpace(modelID)
		if modelID == "" {
			continue
		}
		models = append(models, &registry.ModelInfo{ID: modelID})
	}
	registry.GetGlobalRegistry().RegisterClient(auth.ID, auth.Provider, models)
	t.Cleanup(func() {
		registry.GetGlobalRegistry().UnregisterClient(auth.ID)
	})
	base := handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{
		Images: sdkconfig.ImagesConfig{CodexModel: "gpt-5.4-mini"},
	}, manager)
	return NewOpenAIImagesAPIHandler(base)
}

func newMixedImagesTestHandler(t *testing.T, codexExecutor, webExecutor *imageCaptureExecutor) *OpenAIImagesAPIHandler {
	t.Helper()
	manager := coreauth.NewManager(nil, nil, nil)
	manager.RegisterExecutor(codexExecutor)
	manager.RegisterExecutor(webExecutor)
	auths := []*coreauth.Auth{
		{
			ID:         "codex-mixed-auth",
			Provider:   "codex",
			Status:     coreauth.StatusActive,
			Attributes: map[string]string{"priority": "0"},
		},
		{
			ID:         "chatgpt-web-mixed-auth",
			Provider:   "chatgpt-web",
			Status:     coreauth.StatusActive,
			Attributes: map[string]string{"priority": "1"},
			Metadata:   map[string]any{"access_token": "token", "lifecycle_state": coreauth.LifecycleStateActive},
		},
	}
	for _, auth := range auths {
		if _, err := manager.Register(context.Background(), auth); err != nil {
			t.Fatalf("Register auth %s: %v", auth.ID, err)
		}
		registry.GetGlobalRegistry().RegisterClient(auth.ID, auth.Provider, []*registry.ModelInfo{{ID: "gpt-image-2"}})
		t.Cleanup(func() {
			registry.GetGlobalRegistry().UnregisterClient(auth.ID)
		})
	}
	base := handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{
		Images: sdkconfig.ImagesConfig{CodexModel: "gpt-5.4-mini"},
	}, manager)
	return NewOpenAIImagesAPIHandler(base)
}

func TestOpenAIImagesGenerationsNonStreamingUsesCodexImageTool(t *testing.T) {
	gin.SetMode(gin.TestMode)
	executor := &imageCaptureExecutor{}
	h := newImagesTestHandler(t, executor)
	router := gin.New()
	router.POST("/v1/images/generations", h.Generations)

	req := httptest.NewRequest(http.MethodPost, "/v1/images/generations", strings.NewReader(`{"model":"gpt-image-2","prompt":"draw a cat","size":"1024x1024","quality":"high","output_format":"png","n":1}`))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	router.ServeHTTP(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body=%s", resp.Code, http.StatusOK, resp.Body.String())
	}
	if executor.calls != 1 {
		t.Fatalf("executor calls = %d, want 1", executor.calls)
	}
	if executor.maxResults != 1 {
		t.Fatalf("image result limit = %d, want 1", executor.maxResults)
	}
	if executor.model != "gpt-5.4-mini" {
		t.Fatalf("executor model = %q, want gpt-5.4-mini", executor.model)
	}
	if executor.requested != "gpt-image-2" {
		t.Fatalf("requested model = %q, want gpt-image-2", executor.requested)
	}
	if executor.override != "gpt-5.4-mini" {
		t.Fatalf("execution override = %q, want gpt-5.4-mini", executor.override)
	}
	if executor.sourceFormat != "openai-response" {
		t.Fatalf("source format = %q, want openai-response", executor.sourceFormat)
	}
	if got := gjson.GetBytes(executor.payload, "model").String(); got != "gpt-5.4-mini" {
		t.Fatalf("payload model = %q", got)
	}
	if store := gjson.GetBytes(executor.payload, "store"); !store.Exists() || store.Bool() {
		t.Fatalf("payload store = %v, exists=%v, want false", store.Bool(), store.Exists())
	}
	if instructions := gjson.GetBytes(executor.payload, "instructions"); !instructions.Exists() || instructions.String() != "" {
		t.Fatalf("payload instructions = %q, exists=%v, want empty", instructions.String(), instructions.Exists())
	}
	if stream := gjson.GetBytes(executor.payload, "stream"); !stream.Exists() || !stream.Bool() {
		t.Fatalf("payload stream = %v, exists=%v, want true", stream.Bool(), stream.Exists())
	}
	if got := gjson.GetBytes(executor.payload, "tools.0.type").String(); got != "image_generation" {
		t.Fatalf("tool type = %q", got)
	}
	if got := gjson.GetBytes(executor.payload, "tools.0.model").String(); got != "gpt-image-2" {
		t.Fatalf("tool model = %q", got)
	}
	if got := gjson.GetBytes(executor.payload, "tools.0.action").String(); got != "generate" {
		t.Fatalf("tool action = %q", got)
	}
	if got := gjson.GetBytes(executor.payload, "tools.0.size").String(); got != "1024x1024" {
		t.Fatalf("tool size = %q, want 1024x1024", got)
	}
	assertImageToolNAbsent(t, executor.payload)
	if got := gjson.GetBytes(executor.payload, "tool_choice.type").String(); got != "image_generation" {
		t.Fatalf("tool_choice.type = %q, want image_generation", got)
	}
	if got := gjson.Get(resp.Body.String(), "data.0.b64_json").String(); got != "aGVsbG8=" {
		t.Fatalf("b64_json = %q", got)
	}
	if got := gjson.Get(resp.Body.String(), "data.0.revised_prompt").String(); got != "rev" {
		t.Fatalf("revised_prompt = %q", got)
	}
	if got := gjson.Get(resp.Body.String(), "usage.total_tokens").Int(); got != 3 {
		t.Fatalf("usage.total_tokens = %d", got)
	}
}

func TestOpenAIImagesGenerationsCanUseChatGPTWebProvider(t *testing.T) {
	gin.SetMode(gin.TestMode)
	executor := &imageCaptureExecutor{provider: "chatgpt-web"}
	h := newImagesTestHandler(t, executor)
	router := gin.New()
	router.POST("/v1/images/generations", h.Generations)

	request := httptest.NewRequest(http.MethodPost, "/v1/images/generations", strings.NewReader(`{"model":"gpt-image-2","prompt":"draw"}`))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", response.Code, response.Body.String())
	}
	if executor.calls != 1 || executor.requested != "gpt-image-2" {
		t.Fatalf("executor calls=%d requested=%q", executor.calls, executor.requested)
	}
	parsed, err := executorhelps.ParseChatGPTWebRequest(executor.payload)
	if err != nil {
		t.Fatalf("ChatGPT Web rejected handler payload: %v; payload=%s", err, executor.payload)
	}
	if parsed.Image == nil || parsed.Image.OutputFormat != "png" {
		t.Fatalf("ChatGPT Web image request = %#v", parsed.Image)
	}
	if parsed.Image.Size != "" || parsed.Image.Quality != "auto" {
		t.Fatalf("ChatGPT Web image defaults = %#v", parsed.Image)
	}
	if size := gjson.GetBytes(executor.payload, "tools.0.size"); size.Exists() {
		t.Fatalf("tool size exists = %s, want omitted", size.Raw)
	}
	for path, want := range map[string]string{
		"tools.0.background":         "auto",
		"tools.0.moderation":         "auto",
		"tools.0.output_compression": "100",
	} {
		if got := gjson.GetBytes(executor.payload, path).String(); got != want {
			t.Fatalf("%s = %q, want %q", path, got, want)
		}
	}
}

func TestOpenAIImagesEditsApplyOfficialDefaults(t *testing.T) {
	gin.SetMode(gin.TestMode)
	executor := &imageCaptureExecutor{provider: "chatgpt-web"}
	h := newImagesTestHandler(t, executor)
	router := gin.New()
	router.POST("/v1/images/edits", h.Edits)

	request := httptest.NewRequest(http.MethodPost, "/v1/images/edits", strings.NewReader(`{
		"model":"gpt-image-2",
		"prompt":"edit",
		"images":[{"image_url":"data:image/png;base64,aGVsbG8="}]
	}`))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", response.Code, response.Body.String())
	}
	parsed, errParse := executorhelps.ParseChatGPTWebRequest(executor.payload)
	if errParse != nil {
		t.Fatalf("ChatGPT Web rejected handler payload: %v; payload=%s", errParse, executor.payload)
	}
	if parsed.Image == nil || parsed.Image.Size != "" ||
		parsed.Image.Quality != "auto" || parsed.Image.OutputFormat != "png" {
		t.Fatalf("ChatGPT Web edit defaults = %#v", parsed.Image)
	}
	if size := gjson.GetBytes(executor.payload, "tools.0.size"); size.Exists() {
		t.Fatalf("tool size exists = %s, want omitted", size.Raw)
	}
}

func TestBuildNativeImageRequestJSONForwardsSizeOnlyWhenExplicit(t *testing.T) {
	tests := []struct {
		name       string
		size       string
		wantExists bool
		want       string
	}{
		{name: "omitted"},
		{name: "auto", size: "auto", wantExists: true, want: "auto"},
		{name: "exact", size: "1024x1536", wantExists: true, want: "1024x1536"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			payload, err := buildNativeImageRequestJSON(openAIImageRequest{
				Model:  "gpt-image-2",
				Prompt: "draw",
				Size:   test.size,
			})
			if err != nil {
				t.Fatalf("buildNativeImageRequestJSON() error = %v", err)
			}
			got := gjson.GetBytes(payload, "size")
			if got.Exists() != test.wantExists {
				t.Fatalf("size exists = %v, want %v; payload=%s", got.Exists(), test.wantExists, payload)
			}
			if test.wantExists && got.String() != test.want {
				t.Fatalf("size = %q, want %q", got.String(), test.want)
			}
		})
	}
}

func TestImageResponsesProvidersExcludeChatGPTWebForUnsupportedInputs(t *testing.T) {
	dataImage := imageReference{ImageURL: "data:image/png;base64,aGVsbG8="}
	tests := []struct {
		name string
		req  openAIImageRequest
		want []string
	}{
		{name: "generation", req: openAIImageRequest{}, want: []string{constant.Codex, constant.ChatGPTWeb}},
		{name: "data image edit", req: openAIImageRequest{Images: []imageReference{dataImage}}, want: []string{constant.Codex, constant.ChatGPTWeb}},
		{name: "remote image", req: openAIImageRequest{Images: []imageReference{{ImageURL: "https://example.com/image.png"}}}, want: []string{constant.Codex}},
		{name: "file id", req: openAIImageRequest{Images: []imageReference{{FileID: "file-1"}}}, want: []string{constant.Codex}},
		{name: "size", req: openAIImageRequest{Size: "1024x1024"}, want: []string{constant.Codex}},
		{name: "quality", req: openAIImageRequest{Quality: "high"}, want: []string{constant.Codex}},
		{name: "auto quality", req: openAIImageRequest{Quality: "auto"}, want: []string{constant.Codex, constant.ChatGPTWeb}},
		{name: "PNG output format", req: openAIImageRequest{OutputFormat: "png"}, want: []string{constant.Codex, constant.ChatGPTWeb}},
		{name: "WebP output format", req: openAIImageRequest{OutputFormat: "webp"}, want: []string{constant.Codex}},
		{name: "partial images", req: openAIImageRequest{PartialImages: intPointer(1)}, want: []string{constant.Codex}},
		{name: "webp mask", req: openAIImageRequest{
			Images: []imageReference{dataImage},
			Mask:   &imageReference{ImageURL: "data:image/webp;base64,aGVsbG8="},
		}, want: []string{constant.Codex}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := imageResponsesProviders(test.req, false, defaultChatGPTWebImageConfigSnapshot())
			if strings.Join(got, ",") != strings.Join(test.want, ",") {
				t.Fatalf("providers = %v, want %v", got, test.want)
			}
		})
	}
}

func TestImageResponsesProvidersCanIgnoreUnsupportedWebParams(t *testing.T) {
	request := openAIImageRequest{
		Size:              "1024x1024",
		Quality:           "high",
		Background:        "transparent",
		OutputFormat:      "jpeg",
		InputFidelity:     "high",
		Moderation:        "low",
		OutputCompression: intPointer(90),
		PartialImages:     intPointer(1),
	}
	got := imageResponsesProviders(request, true, defaultChatGPTWebImageConfigSnapshot())
	want := []string{constant.Codex, constant.ChatGPTWeb}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("providers = %v, want %v", got, want)
	}

	request.Images = []imageReference{{ImageURL: "https://example.com/image.png"}}
	got = imageResponsesProviders(request, true, defaultChatGPTWebImageConfigSnapshot())
	if strings.Join(got, ",") != constant.Codex {
		t.Fatalf("remote image providers = %v, want Codex only", got)
	}
}

func TestImageResponsesProvidersAdaptSupportedAndIgnoredWebSizes(t *testing.T) {
	imageConfig := defaultChatGPTWebImageConfigSnapshot()
	imageConfig.AdaptSizeToAspectRatio = true
	tests := []struct {
		name string
		size string
		want []string
	}{
		{name: "matched", size: "941x1672", want: []string{constant.Codex, constant.ChatGPTWeb}},
		{name: "unsupported ratio ignored", size: "1024x1536", want: []string{constant.Codex, constant.ChatGPTWeb}},
		{name: "oversize ignored", size: "4000x4000", want: []string{constant.Codex, constant.ChatGPTWeb}},
		{name: "invalid", size: "square", want: []string{constant.Codex}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := imageResponsesProviders(openAIImageRequest{Size: test.size}, false, imageConfig)
			if strings.Join(got, ",") != strings.Join(test.want, ",") {
				t.Fatalf("providers = %v, want %v", got, test.want)
			}
		})
	}
}

func TestImageResponsesProvidersKeepsStrictSizesForRequestFiltering(t *testing.T) {
	imageConfig := defaultChatGPTWebImageConfigSnapshot()
	imageConfig.AdaptSizeToAspectRatio = true
	imageConfig.StrictSize = true
	for _, size := range []string{"1024x1536", "4000x4000", "square"} {
		got := imageResponsesProviders(openAIImageRequest{Size: size}, false, imageConfig)
		want := []string{constant.Codex, constant.ChatGPTWeb}
		if strings.Join(got, ",") != strings.Join(want, ",") {
			t.Fatalf("size %q providers = %v, want %v", size, got, want)
		}
	}
}

func TestOpenAIImagesPinsChatGPTWebImageAspectConfig(t *testing.T) {
	gin.SetMode(gin.TestMode)
	executor := &imageCaptureExecutor{provider: constant.ChatGPTWeb}
	h := newImagesTestHandler(t, executor)
	h.Cfg.Images.ChatGPTWeb.AdaptSizeToAspectRatio = true
	h.Cfg.Images.ChatGPTWeb.StrictSize = true
	router := gin.New()
	router.Use(allowedImageProvidersMiddleware(constant.ChatGPTWeb))
	router.POST("/v1/images/generations", h.Generations)

	request := httptest.NewRequest(http.MethodPost, "/v1/images/generations", strings.NewReader(
		`{"model":"gpt-image-2","prompt":"draw","size":"941x1672"}`,
	))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", response.Code, response.Body.String())
	}
	if !executor.hasImageConfigSnapshot || !executor.imageConfigSnapshot.AdaptSizeToAspectRatio || !executor.imageConfigSnapshot.StrictSize ||
		executor.imageConfigSnapshot.AspectRatioMaxErrorPercent != 1 || executor.imageConfigSnapshot.MaxResizeEdgePixels != 3840 ||
		executor.imageConfigSnapshot.ResizeFilter != sdkconfig.DefaultChatGPTWebResizeFilter ||
		executor.imageConfigSnapshot.MaxImageResponseBytes != sdkconfig.DefaultChatGPTWebMaxImageResponseMegabytes<<20 {
		t.Fatalf("image config snapshot = %#v, present=%v", executor.imageConfigSnapshot, executor.hasImageConfigSnapshot)
	}
}

func TestOpenAIImagesGenerationsCanRouteIgnoredParamsToChatGPTWeb(t *testing.T) {
	gin.SetMode(gin.TestMode)
	executor := &imageCaptureExecutor{provider: "chatgpt-web"}
	h := newImagesTestHandler(t, executor)
	h.Cfg.Images.ChatGPTWeb.IgnoreUnsupportedParams = true
	router := gin.New()
	router.Use(allowedImageProvidersMiddleware(constant.ChatGPTWeb))
	router.POST("/v1/images/generations", h.Generations)

	request := httptest.NewRequest(http.MethodPost, "/v1/images/generations", strings.NewReader(
		`{"model":"gpt-image-2","prompt":"draw","size":"1024x1024","quality":"high"}`,
	))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", response.Code, response.Body.String())
	}
	if executor.calls != 1 {
		t.Fatalf("executor calls = %d, want 1", executor.calls)
	}
	if !executor.hasIgnoreUnsupportedImageParams || !executor.ignoreUnsupportedImageParams {
		t.Fatalf("ignore unsupported image params metadata = (%v, %v), want (true, true)",
			executor.ignoreUnsupportedImageParams, executor.hasIgnoreUnsupportedImageParams)
	}
}

func TestOpenAIImagesWebOnlyKeyRejectsUnsupportedParamsBeforeProviderFiltering(t *testing.T) {
	gin.SetMode(gin.TestMode)
	executor := &imageCaptureExecutor{provider: "chatgpt-web"}
	h := newImagesTestHandler(t, executor)
	h.Cfg.Images.UnsupportedStatusCode = http.StatusUnprocessableEntity
	router := gin.New()
	router.Use(allowedImageProvidersMiddleware(constant.ChatGPTWeb))
	router.POST("/v1/images/generations", h.Generations)

	request := httptest.NewRequest(http.MethodPost, "/v1/images/generations", strings.NewReader(
		`{"model":"gpt-image-2","prompt":"draw","size":"1024x1024"}`,
	))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)

	if response.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422; body=%s", response.Code, response.Body.String())
	}
	if got := gjson.Get(response.Body.String(), "error.code").String(); got != "unsupported_parameter" {
		t.Fatalf("error code = %q, want unsupported_parameter; body=%s", got, response.Body.String())
	}
	if got := gjson.Get(response.Body.String(), "error.param").String(); got != "size" {
		t.Fatalf("error param = %q, want size; body=%s", got, response.Body.String())
	}
	if executor.calls != 0 {
		t.Fatalf("executor calls = %d, want 0", executor.calls)
	}
}

func TestOpenAIImagesMixedKeyRoutesUnsupportedWebParamsToCodex(t *testing.T) {
	gin.SetMode(gin.TestMode)
	codexExecutor := &imageCaptureExecutor{provider: "codex"}
	webExecutor := &imageCaptureExecutor{provider: "chatgpt-web"}
	h := newMixedImagesTestHandler(t, codexExecutor, webExecutor)
	router := gin.New()
	router.Use(allowedImageProvidersMiddleware(constant.ChatGPTWeb, constant.Codex))
	router.POST("/v1/images/generations", h.Generations)

	request := httptest.NewRequest(http.MethodPost, "/v1/images/generations", strings.NewReader(
		`{"model":"gpt-image-2","prompt":"draw","size":"1024x1024"}`,
	))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", response.Code, response.Body.String())
	}
	if codexExecutor.calls != 1 || webExecutor.calls != 0 {
		t.Fatalf("executor calls: codex=%d web=%d, want 1/0", codexExecutor.calls, webExecutor.calls)
	}
}

func TestOpenAIImagesStrictSizeRoutesIncompatibleRequestToCodex(t *testing.T) {
	gin.SetMode(gin.TestMode)
	executor := &imageCaptureExecutor{provider: constant.Codex}
	h := newImagesTestHandler(t, executor)
	h.Cfg.Images.ChatGPTWeb.AdaptSizeToAspectRatio = true
	h.Cfg.Images.ChatGPTWeb.StrictSize = true
	h.Cfg.Images.ChatGPTWeb.IgnoreUnsupportedParams = true
	router := gin.New()
	router.POST("/v1/images/generations", h.Generations)

	request := httptest.NewRequest(http.MethodPost, "/v1/images/generations", strings.NewReader(
		`{"model":"gpt-image-2","prompt":"draw","size":"1024x1536"}`,
	))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", response.Code, response.Body.String())
	}
	if executor.calls != 1 {
		t.Fatalf("executor calls = %d, want 1", executor.calls)
	}
}

func TestOpenAIImagesStrictSizeRejectsWebOnlyBeforeExecution(t *testing.T) {
	gin.SetMode(gin.TestMode)
	tests := []struct {
		name string
		path string
		body string
	}{
		{
			name: "generation",
			path: "/v1/images/generations",
			body: `{"model":"gpt-image-2","prompt":"draw","size":"1024x1536"}`,
		},
		{
			name: "streaming generation",
			path: "/v1/images/generations",
			body: `{"model":"gpt-image-2","prompt":"draw","size":"4000x4000","stream":true}`,
		},
		{
			name: "edit",
			path: "/v1/images/edits",
			body: `{"model":"gpt-image-2","prompt":"edit","size":"square","images":[{"image_url":"data:image/png;base64,aGVsbG8="}]}`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			executor := &imageCaptureExecutor{provider: constant.ChatGPTWeb}
			h := newImagesTestHandler(t, executor)
			h.Cfg.Images.ChatGPTWeb.AdaptSizeToAspectRatio = true
			h.Cfg.Images.ChatGPTWeb.StrictSize = true
			h.Cfg.Images.ChatGPTWeb.IgnoreUnsupportedParams = true
			router := gin.New()
			router.Use(allowedImageProvidersMiddleware(constant.ChatGPTWeb))
			router.POST("/v1/images/generations", h.Generations)
			router.POST("/v1/images/edits", h.Edits)

			request := httptest.NewRequest(http.MethodPost, test.path, strings.NewReader(test.body))
			request.Header.Set("Content-Type", "application/json")
			response := httptest.NewRecorder()
			router.ServeHTTP(response, request)

			if response.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400; body=%s", response.Code, response.Body.String())
			}
			if got := gjson.Get(response.Body.String(), "error.param").String(); got != "size" {
				t.Fatalf("error param = %q; body=%s", got, response.Body.String())
			}
			if got := gjson.Get(response.Body.String(), "error.code").String(); got != "invalid_value" {
				t.Fatalf("error code = %q; body=%s", got, response.Body.String())
			}
			if executor.calls != 0 || executor.streamCalls != 0 {
				t.Fatalf("executor calls = %d stream calls = %d, want none", executor.calls, executor.streamCalls)
			}
		})
	}
}

func TestOpenAIImagesPinsDisabledUnsupportedParamPolicy(t *testing.T) {
	gin.SetMode(gin.TestMode)
	executor := &imageCaptureExecutor{provider: "chatgpt-web"}
	h := newImagesTestHandler(t, executor)
	router := gin.New()
	router.POST("/v1/images/generations", h.Generations)

	request := httptest.NewRequest(http.MethodPost, "/v1/images/generations", strings.NewReader(
		`{"model":"gpt-image-2","prompt":"draw"}`,
	))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", response.Code, response.Body.String())
	}
	if !executor.hasIgnoreUnsupportedImageParams || executor.ignoreUnsupportedImageParams {
		t.Fatalf("ignore unsupported image params metadata = (%v, %v), want (false, true)",
			executor.ignoreUnsupportedImageParams, executor.hasIgnoreUnsupportedImageParams)
	}
}

func allowedImageProvidersMiddleware(providers ...string) gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Set("accessMetadata", map[string]string{
			sdkaccess.MetadataAllowedProviders: strings.Join(providers, ","),
		})
		c.Next()
	}
}

func TestOpenAIImagesNonStreamingCapsProviderMultiImageResult(t *testing.T) {
	gin.SetMode(gin.TestMode)
	executor := &imageCaptureExecutor{
		provider: "chatgpt-web",
		response: []byte(`{"created_at":1700000000,"output":[
			{"type":"image_generation_call","result":"Zmlyc3Q="},
			{"type":"image_generation_call","result":"c2Vjb25k"}
		]}`),
	}
	h := newImagesTestHandler(t, executor)
	router := gin.New()
	router.POST("/v1/images/generations", h.Generations)

	request := httptest.NewRequest(http.MethodPost, "/v1/images/generations", strings.NewReader(`{"model":"gpt-image-2","prompt":"draw","n":1}`))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", response.Code, response.Body.String())
	}
	if got := len(gjson.Get(response.Body.String(), "data").Array()); got != 1 {
		t.Fatalf("image count = %d, body=%s", got, response.Body.String())
	}
}

func TestOpenAIImagesMixedProvidersUseWebOnlyForEquivalentRequests(t *testing.T) {
	gin.SetMode(gin.TestMode)
	codexExecutor := &imageCaptureExecutor{provider: "codex"}
	webExecutor := &imageCaptureExecutor{provider: "chatgpt-web"}
	h := newMixedImagesTestHandler(t, codexExecutor, webExecutor)
	router := gin.New()
	router.POST("/v1/images/generations", h.Generations)

	request := httptest.NewRequest(http.MethodPost, "/v1/images/generations", strings.NewReader(`{"model":"gpt-image-2","prompt":"draw"}`))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("web-capable status = %d, body=%s", response.Code, response.Body.String())
	}
	if webExecutor.calls != 1 || codexExecutor.calls != 0 {
		t.Fatalf("equivalent request calls: web=%d codex=%d", webExecutor.calls, codexExecutor.calls)
	}

	request = httptest.NewRequest(http.MethodPost, "/v1/images/generations", strings.NewReader(`{"model":"gpt-image-2","prompt":"draw","output_format":"webp"}`))
	request.Header.Set("Content-Type", "application/json")
	response = httptest.NewRecorder()
	router.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("Codex-only status = %d, body=%s", response.Code, response.Body.String())
	}
	if webExecutor.calls != 1 || codexExecutor.calls != 1 {
		t.Fatalf("unsupported request calls: web=%d codex=%d", webExecutor.calls, codexExecutor.calls)
	}
}

func TestOpenAIImagesNativeGenerationsDirectProxyAppliesParamRules(t *testing.T) {
	gin.SetMode(gin.TestMode)
	executor := &imageCaptureExecutor{
		response: []byte(`{"created":1700000000,"data":[{"b64_json":"bmF0aXZl"}]}`),
	}
	h := newImagesTestHandler(t, executor, "gpt-image-2")
	h.Cfg.Images.Native.Generations.Enabled = true
	h.Cfg.Images.Native.Generations.Models = []string{"gpt-image-2"}
	h.Cfg.Images.Native.Generations.UnsupportedModelStatusCode = http.StatusBadRequest
	h.Cfg.Images.Native.Generations.UnsupportedModelMessage = "native generation disabled for {model}"
	h.Cfg.Images.Native.Generations.ParamRules = []string{"n", "background=transparent", "partial_images=2"}
	router := gin.New()
	router.POST("/v1/images/generations", h.Generations)

	req := httptest.NewRequest(http.MethodPost, "/v1/images/generations", strings.NewReader(`{"model":"gpt-image-2","prompt":"draw","n":3,"background":"auto"}`))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	router.ServeHTTP(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body=%s", resp.Code, http.StatusOK, resp.Body.String())
	}
	if executor.calls != 1 {
		t.Fatalf("executor calls = %d, want 1", executor.calls)
	}
	if executor.sourceFormat != nativeImagesHandlerType {
		t.Fatalf("source format = %q, want %s", executor.sourceFormat, nativeImagesHandlerType)
	}
	if executor.alt != nativeImagesGenerations {
		t.Fatalf("alt = %q, want %s", executor.alt, nativeImagesGenerations)
	}
	if got := gjson.GetBytes(executor.payload, "model").String(); got != "gpt-image-2" {
		t.Fatalf("payload model = %q", got)
	}
	if got := gjson.GetBytes(executor.payload, "tools"); got.Exists() {
		t.Fatalf("tools exists = %s, want absent", got.Raw)
	}
	if got := gjson.GetBytes(executor.payload, "n"); got.Exists() {
		t.Fatalf("n exists = %s, want absent", got.Raw)
	}
	if got := gjson.GetBytes(executor.payload, "background").String(); got != "transparent" {
		t.Fatalf("background = %q, want transparent", got)
	}
	if got := gjson.GetBytes(executor.payload, "partial_images").Int(); got != 2 {
		t.Fatalf("partial_images = %d, want 2", got)
	}
	if got := gjson.Get(resp.Body.String(), "data.0.b64_json").String(); got != "bmF0aXZl" {
		t.Fatalf("native response not forwarded, b64_json = %q", got)
	}
}

func TestOpenAIImagesNativeRoutesRejectCountAboveMaximum(t *testing.T) {
	gin.SetMode(gin.TestMode)
	executor := &imageCaptureExecutor{}
	h := newImagesTestHandler(t, executor, "gpt-image-2")
	h.Cfg.Images.Native.Generations.Enabled = true
	h.Cfg.Images.Native.Generations.Models = []string{"gpt-image-2"}
	h.Cfg.Images.Native.Edits.Enabled = true
	h.Cfg.Images.Native.Edits.Models = []string{"gpt-image-2"}
	router := gin.New()
	router.POST("/v1/images/generations", h.Generations)
	router.POST("/v1/images/edits", h.Edits)

	for _, test := range []struct {
		name string
		path string
		body string
	}{
		{
			name: "generation",
			path: "/v1/images/generations",
			body: `{"model":"gpt-image-2","prompt":"draw","n":11}`,
		},
		{
			name: "json edit",
			path: "/v1/images/edits",
			body: `{"model":"gpt-image-2","prompt":"edit","images":[{"file_id":"file-1"}],"n":11}`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, test.path, strings.NewReader(test.body))
			req.Header.Set("Content-Type", "application/json")
			resp := httptest.NewRecorder()
			router.ServeHTTP(resp, req)
			if resp.Code != http.StatusBadRequest || !strings.Contains(resp.Body.String(), "n must be at most 10") {
				t.Fatalf("status = %d, body=%s", resp.Code, resp.Body.String())
			}
		})
	}
	if executor.calls != 0 || executor.streamCalls != 0 {
		t.Fatalf("executor calls = %d streamCalls = %d, want none", executor.calls, executor.streamCalls)
	}
}

func TestOpenAIImagesNativeGenerationsRejectsUnsupportedModel(t *testing.T) {
	gin.SetMode(gin.TestMode)
	executor := &imageCaptureExecutor{}
	h := newImagesTestHandler(t, executor, "gpt-image-2", "gpt-image-1.5")
	h.Cfg.Images.Native.Generations.Enabled = true
	h.Cfg.Images.Native.Generations.Models = []string{"gpt-image-2"}
	h.Cfg.Images.Native.Generations.UnsupportedModelStatusCode = http.StatusConflict
	h.Cfg.Images.Native.Generations.UnsupportedModelMessage = "no native generation for {model}"
	router := gin.New()
	router.POST("/v1/images/generations", h.Generations)

	req := httptest.NewRequest(http.MethodPost, "/v1/images/generations", strings.NewReader(`{"model":"gpt-image-1.5","prompt":"draw"}`))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	router.ServeHTTP(resp, req)

	if resp.Code != http.StatusConflict {
		t.Fatalf("status = %d, want %d, body=%s", resp.Code, http.StatusConflict, resp.Body.String())
	}
	if executor.calls != 0 || executor.streamCalls != 0 {
		t.Fatalf("executor calls = %d streamCalls = %d, want none", executor.calls, executor.streamCalls)
	}
	if !strings.Contains(resp.Body.String(), "no native generation for gpt-image-1.5") {
		t.Fatalf("body = %s, want custom unsupported model message", resp.Body.String())
	}
}

func TestOpenAIImagesNativeGenerationsStreamsUpstreamEvents(t *testing.T) {
	gin.SetMode(gin.TestMode)
	executor := &imageCaptureExecutor{
		streamChunks: []coreexecutor.StreamChunk{
			{Payload: []byte("event: image_generation.partial_image\n")},
			{Payload: []byte(`data: {"type":"image_generation.partial_image","b64_json":"cGFydA=="}` + "\n\n")},
		},
	}
	h := newImagesTestHandler(t, executor, "gpt-image-2")
	h.Cfg.Images.Native.Generations.Enabled = true
	h.Cfg.Images.Native.Generations.Models = []string{"gpt-image-2"}
	h.Cfg.Images.Native.Generations.UnsupportedModelStatusCode = http.StatusBadRequest
	h.Cfg.Images.Native.Generations.UnsupportedModelMessage = "native generation disabled for {model}"
	router := gin.New()
	router.POST("/v1/images/generations", h.Generations)

	req := httptest.NewRequest(http.MethodPost, "/v1/images/generations", strings.NewReader(`{"model":"gpt-image-2","prompt":"draw","stream":true}`))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	router.ServeHTTP(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body=%s", resp.Code, http.StatusOK, resp.Body.String())
	}
	if executor.streamCalls != 1 {
		t.Fatalf("streamCalls = %d, want 1", executor.streamCalls)
	}
	if executor.sourceFormat != nativeImagesHandlerType {
		t.Fatalf("source format = %q, want %s", executor.sourceFormat, nativeImagesHandlerType)
	}
	if executor.alt != nativeImagesGenerations {
		t.Fatalf("alt = %q, want %s", executor.alt, nativeImagesGenerations)
	}
	if !strings.Contains(resp.Body.String(), "image_generation.partial_image") {
		t.Fatalf("body = %q, want upstream image event", resp.Body.String())
	}
}

func TestOpenAIImagesGenerationsUsesConfiguredImageModel(t *testing.T) {
	gin.SetMode(gin.TestMode)
	executor := &imageCaptureExecutor{}
	h := newImagesTestHandler(t, executor, "gpt-image-custom", "gpt-5.4-mini")
	h.Cfg.Images.ImageModel = "gpt-image-custom"
	router := gin.New()
	router.POST("/v1/images/generations", h.Generations)

	req := httptest.NewRequest(http.MethodPost, "/v1/images/generations", strings.NewReader(`{"model":"gpt-image-custom","prompt":"draw a cat"}`))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	router.ServeHTTP(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body=%s", resp.Code, http.StatusOK, resp.Body.String())
	}
	if got := gjson.GetBytes(executor.payload, "tools.0.model").String(); got != "gpt-image-custom" {
		t.Fatalf("tool model = %q", got)
	}
	models := h.Models()
	if got := models[0]["id"]; got != "gpt-image-custom" {
		t.Fatalf("model id = %v", got)
	}
}

func TestOpenAIImagesGenerationsSelectsOnImageModelOnly(t *testing.T) {
	gin.SetMode(gin.TestMode)
	executor := &imageCaptureExecutor{}
	h := newImagesTestHandler(t, executor, "gpt-image-2")
	router := gin.New()
	router.POST("/v1/images/generations", h.Generations)

	req := httptest.NewRequest(http.MethodPost, "/v1/images/generations", strings.NewReader(`{"model":"gpt-image-2","prompt":"draw a cat"}`))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	router.ServeHTTP(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body=%s", resp.Code, http.StatusOK, resp.Body.String())
	}
	if executor.calls != 1 {
		t.Fatalf("executor calls = %d, want 1", executor.calls)
	}
	if executor.model != "gpt-5.4-mini" {
		t.Fatalf("executor model = %q, want gpt-5.4-mini", executor.model)
	}
}

func TestOpenAIImagesGenerationsRejectsWhenImageModelIsNotRegistered(t *testing.T) {
	gin.SetMode(gin.TestMode)
	executor := &imageCaptureExecutor{}
	h := newImagesTestHandler(t, executor, "gpt-5.4-mini")
	router := gin.New()
	router.POST("/v1/images/generations", h.Generations)

	req := httptest.NewRequest(http.MethodPost, "/v1/images/generations", strings.NewReader(`{"model":"gpt-image-2","prompt":"draw a cat"}`))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	router.ServeHTTP(resp, req)

	if resp.Code == http.StatusOK {
		t.Fatalf("status = %d, want non-200 failure", resp.Code)
	}
	if executor.calls != 0 && executor.streamCalls != 0 {
		t.Fatalf("executor calls = %d streamCalls = %d, want none", executor.calls, executor.streamCalls)
	}
	if !strings.Contains(resp.Body.String(), "gpt-image-2") {
		t.Fatalf("expected model-unavailable error, body=%s", resp.Body.String())
	}
}

func TestOpenAIImagesGenerationsAggregatesMultipleNonStreamingImages(t *testing.T) {
	gin.SetMode(gin.TestMode)
	executor := &imageCaptureExecutor{
		response: []byte(`{"created_at":1700000000,"output":[{"type":"image_generation_call","result":"aGVsbG8=","revised_prompt":"rev"}],"tool_usage":{"image_gen":{"input_tokens":1,"input_tokens_details":{"text_tokens":1,"image_tokens":0},"output_tokens":2,"output_tokens_details":{"image_tokens":2,"text_tokens":0,"vendor_units":9},"total_tokens":3,"future_count":7,"extension":{"mode":"official"}}},"usage":{"input_tokens":99,"output_tokens":99,"total_tokens":198}}`),
	}
	h := newImagesTestHandler(t, executor)
	enableNAggregation := true
	h.Cfg.Images.EnableNAggregation = &enableNAggregation
	router := gin.New()
	router.POST("/v1/images/generations", h.Generations)

	req := httptest.NewRequest(http.MethodPost, "/v1/images/generations", strings.NewReader(`{"model":"gpt-image-2","prompt":"draw","n":2}`))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	router.ServeHTTP(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body=%s", resp.Code, http.StatusOK, resp.Body.String())
	}
	if executor.calls != 2 {
		t.Fatalf("executor calls = %d, want 2", executor.calls)
	}
	assertImageToolNAbsent(t, executor.payload)
	if count := len(gjson.Get(resp.Body.String(), "data").Array()); count != 2 {
		t.Fatalf("data count = %d, want 2: %s", count, resp.Body.String())
	}
	if got := gjson.Get(resp.Body.String(), "usage.input_tokens").Int(); got != 1 {
		t.Fatalf("usage.input_tokens = %d, want 1", got)
	}
	if got := gjson.Get(resp.Body.String(), "usage.total_tokens").Int(); got != 5 {
		t.Fatalf("usage.total_tokens = %d, want 5", got)
	}
	if got := gjson.Get(resp.Body.String(), "usage.output_tokens").Int(); got != 4 {
		t.Fatalf("usage.output_tokens = %d, want 4", got)
	}
	if got := gjson.Get(resp.Body.String(), "usage.input_tokens_details.text_tokens").Int(); got != 1 {
		t.Fatalf("usage.input_tokens_details.text_tokens = %d, want 1", got)
	}
	if got := gjson.Get(resp.Body.String(), "usage.output_tokens_details.image_tokens").Int(); got != 4 {
		t.Fatalf("usage.output_tokens_details.image_tokens = %d, want 4", got)
	}
	if got := gjson.Get(resp.Body.String(), "usage.output_tokens_details.vendor_units").Int(); got != 9 {
		t.Fatalf("unknown output detail = %d, want first upstream value 9", got)
	}
	if got := gjson.Get(resp.Body.String(), "usage.future_count").Int(); got != 7 {
		t.Fatalf("unknown usage field = %d, want first upstream value 7", got)
	}
	if got := gjson.Get(resp.Body.String(), "usage.extension.mode").String(); got != "official" {
		t.Fatalf("unknown usage object = %q", got)
	}
}

func TestOpenAIImagesGenerationsEnforcesAggregateWebResponseBudget(t *testing.T) {
	gin.SetMode(gin.TestMode)
	encoded := base64.StdEncoding.EncodeToString(make([]byte, 700<<10))
	executor := &imageCaptureExecutor{
		provider: constant.ChatGPTWeb,
		response: []byte(`{"created_at":1700000000,"output":[{"type":"image_generation_call","result":"` + encoded + `"}]}`),
	}
	h := newImagesTestHandler(t, executor)
	enableNAggregation := true
	oneMegabyte := 1
	h.Cfg.Images.EnableNAggregation = &enableNAggregation
	h.Cfg.Images.ChatGPTWeb.MaxImageResponseMegabytes = &oneMegabyte
	router := gin.New()
	router.POST("/v1/images/generations", h.Generations)

	req := httptest.NewRequest(http.MethodPost, "/v1/images/generations", strings.NewReader(`{"model":"gpt-image-2","prompt":"draw","n":2}`))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	router.ServeHTTP(resp, req)

	if resp.Code != http.StatusBadGateway || !strings.Contains(resp.Body.String(), "image response exceeds 1048576 bytes") {
		t.Fatalf("status=%d body=%s", resp.Code, resp.Body.String())
	}
	if executor.calls != 2 || len(executor.imageConfigSnapshots) != 2 {
		t.Fatalf("calls=%d snapshots=%d", executor.calls, len(executor.imageConfigSnapshots))
	}
	if got := executor.imageConfigSnapshots[0].MaxImageResponseBytes; got != 1<<20 {
		t.Fatalf("first response budget = %d", got)
	}
	if got := executor.imageConfigSnapshots[1].MaxImageResponseBytes; got != (1<<20)-(700<<10) {
		t.Fatalf("second response budget = %d", got)
	}
}

func TestOpenAIImagesGenerationsRejectsAggregationAboveMaximum(t *testing.T) {
	gin.SetMode(gin.TestMode)
	executor := &imageCaptureExecutor{}
	h := newImagesTestHandler(t, executor)
	enableNAggregation := true
	h.Cfg.Images.EnableNAggregation = &enableNAggregation
	router := gin.New()
	router.POST("/v1/images/generations", h.Generations)

	req := httptest.NewRequest(http.MethodPost, "/v1/images/generations", strings.NewReader(`{"model":"gpt-image-2","prompt":"draw","n":11}`))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	router.ServeHTTP(resp, req)

	if resp.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d, body=%s", resp.Code, http.StatusBadRequest, resp.Body.String())
	}
	if executor.calls != 0 || executor.streamCalls != 0 {
		t.Fatalf("executor calls = %d streamCalls = %d, want none", executor.calls, executor.streamCalls)
	}
	if !strings.Contains(resp.Body.String(), "n must be at most 10") {
		t.Fatalf("error body = %s", resp.Body.String())
	}
}

func TestOpenAIImagesGenerationsRejectsMultipleImagesByDefault(t *testing.T) {
	gin.SetMode(gin.TestMode)
	executor := &imageCaptureExecutor{}
	h := newImagesTestHandler(t, executor)
	h.Cfg.Images.UnsupportedStatusCode = http.StatusConflict
	router := gin.New()
	router.POST("/v1/images/generations", h.Generations)

	req := httptest.NewRequest(http.MethodPost, "/v1/images/generations", strings.NewReader(`{"model":"gpt-image-2","prompt":"draw","n":2}`))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	router.ServeHTTP(resp, req)

	if resp.Code != http.StatusConflict {
		t.Fatalf("status = %d, want %d, body=%s", resp.Code, http.StatusConflict, resp.Body.String())
	}
	if executor.calls != 0 && executor.streamCalls != 0 {
		t.Fatalf("executor calls = %d streamCalls = %d, want none", executor.calls, executor.streamCalls)
	}
}

func TestOpenAIImagesUnsupportedOptionsUseConfiguredStatus(t *testing.T) {
	gin.SetMode(gin.TestMode)
	executor := &imageCaptureExecutor{}
	h := newImagesTestHandler(t, executor)
	h.Cfg.Images.UnsupportedStatusCode = http.StatusUnprocessableEntity
	router := gin.New()
	router.POST("/v1/images/generations", h.Generations)

	req := httptest.NewRequest(http.MethodPost, "/v1/images/generations", strings.NewReader(`{"model":"gpt-image-2","prompt":"draw","response_format":"url"}`))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	router.ServeHTTP(resp, req)

	if resp.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want %d, body=%s", resp.Code, http.StatusUnprocessableEntity, resp.Body.String())
	}
	if executor.calls != 0 && executor.streamCalls != 0 {
		t.Fatalf("executor calls = %d streamCalls = %d, want none", executor.calls, executor.streamCalls)
	}
}

func TestOpenAIImagesCanOverrideUnsupportedOptions(t *testing.T) {
	gin.SetMode(gin.TestMode)
	executor := &imageCaptureExecutor{}
	h := newImagesTestHandler(t, executor)
	overrideResponseFormatURL := true
	overrideTransparentBackground := true
	h.Cfg.Images.OverrideResponseFormatURL = &overrideResponseFormatURL
	h.Cfg.Images.OverrideTransparentBackground = &overrideTransparentBackground
	router := gin.New()
	router.POST("/v1/images/generations", h.Generations)

	req := httptest.NewRequest(http.MethodPost, "/v1/images/generations", strings.NewReader(`{"model":"gpt-image-2","prompt":"draw","response_format":"url","background":"transparent"}`))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	router.ServeHTTP(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body=%s", resp.Code, http.StatusOK, resp.Body.String())
	}
	if executor.calls != 1 {
		t.Fatalf("executor calls = %d, want 1", executor.calls)
	}
	if got := gjson.GetBytes(executor.payload, "tools.0.background").String(); got != "auto" {
		t.Fatalf("tool background = %q, want auto", got)
	}
	if got := gjson.Get(resp.Body.String(), "data.0.b64_json").String(); got != "aGVsbG8=" {
		t.Fatalf("b64_json = %q", got)
	}
}

func TestOpenAIImagesCanReturnDataURLForURLResponseFormat(t *testing.T) {
	gin.SetMode(gin.TestMode)
	executor := &imageCaptureExecutor{
		response: []byte(`{"created_at":1700000000,"output":[{"type":"image_generation_call","result":"aGVsbG8=","output_format":"png"}]}`),
	}
	h := newImagesTestHandler(t, executor)
	responseFormatURLDataURL := true
	h.Cfg.Images.ResponseFormatURLDataURL = &responseFormatURLDataURL
	router := gin.New()
	router.POST("/v1/images/generations", h.Generations)

	req := httptest.NewRequest(http.MethodPost, "/v1/images/generations", strings.NewReader(`{"model":"gpt-image-2","prompt":"draw","response_format":"url"}`))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	router.ServeHTTP(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body=%s", resp.Code, http.StatusOK, resp.Body.String())
	}
	if got := gjson.Get(resp.Body.String(), "data.0.url").String(); got != "data:image/png;base64,aGVsbG8=" {
		t.Fatalf("url = %q", got)
	}
	if got := gjson.Get(resp.Body.String(), "data.0.b64_json"); got.Exists() {
		t.Fatalf("b64_json exists = %s, want absent", got.Raw)
	}
}

func TestImageDataURLUsesGIFMIME(t *testing.T) {
	if got := imageDataURL("R0lGODlh", "gif"); got != "data:image/gif;base64,R0lGODlh" {
		t.Fatalf("GIF data URL = %q", got)
	}
}

func TestOpenAIImagesOverrideOptionsAreSeparate(t *testing.T) {
	gin.SetMode(gin.TestMode)
	overrideResponseFormatURL := true
	overrideTransparentBackground := false
	executor := &imageCaptureExecutor{}
	h := newImagesTestHandler(t, executor)
	h.Cfg.Images.OverrideResponseFormatURL = &overrideResponseFormatURL
	h.Cfg.Images.OverrideTransparentBackground = &overrideTransparentBackground
	router := gin.New()
	router.POST("/v1/images/generations", h.Generations)

	req := httptest.NewRequest(http.MethodPost, "/v1/images/generations", strings.NewReader(`{"model":"gpt-image-2","prompt":"draw","response_format":"url","background":"transparent"}`))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	router.ServeHTTP(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body=%s", resp.Code, http.StatusOK, resp.Body.String())
	}
	if executor.calls != 1 {
		t.Fatalf("executor calls = %d, want 1", executor.calls)
	}
	if got := gjson.GetBytes(executor.payload, "tools.0.background").String(); got != "transparent" {
		t.Fatalf("tool background = %q, want transparent", got)
	}
}

func TestOpenAIImagesLegacyOverrideEnablesBothOptions(t *testing.T) {
	gin.SetMode(gin.TestMode)
	executor := &imageCaptureExecutor{}
	h := newImagesTestHandler(t, executor)
	h.Cfg.Images.OverrideUnsupportedParams = true
	router := gin.New()
	router.POST("/v1/images/generations", h.Generations)

	req := httptest.NewRequest(http.MethodPost, "/v1/images/generations", strings.NewReader(`{"model":"gpt-image-2","prompt":"draw","response_format":"url","background":"transparent"}`))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	router.ServeHTTP(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body=%s", resp.Code, http.StatusOK, resp.Body.String())
	}
	if executor.calls != 1 {
		t.Fatalf("executor calls = %d, want 1", executor.calls)
	}
}

func TestOpenAIImagesOverrideKeepsUnknownResponseFormatUnsupported(t *testing.T) {
	gin.SetMode(gin.TestMode)
	executor := &imageCaptureExecutor{}
	h := newImagesTestHandler(t, executor)
	overrideResponseFormatURL := true
	h.Cfg.Images.OverrideResponseFormatURL = &overrideResponseFormatURL
	router := gin.New()
	router.POST("/v1/images/generations", h.Generations)

	req := httptest.NewRequest(http.MethodPost, "/v1/images/generations", strings.NewReader(`{"model":"gpt-image-2","prompt":"draw","response_format":"json"}`))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	router.ServeHTTP(resp, req)

	if resp.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d, body=%s", resp.Code, http.StatusBadRequest, resp.Body.String())
	}
	if executor.calls != 0 && executor.streamCalls != 0 {
		t.Fatalf("executor calls = %d streamCalls = %d, want none", executor.calls, executor.streamCalls)
	}
}

func TestOpenAIImagesEditsMultipartBuildsDataURLsAndMask(t *testing.T) {
	gin.SetMode(gin.TestMode)
	releasedBeforeExecute := false
	var req *http.Request
	executor := &imageCaptureExecutor{beforeExecute: func() {
		releasedBeforeExecute = req != nil && req.Body == http.NoBody && req.MultipartForm == nil
	}}
	h := newImagesTestHandler(t, executor)
	router := gin.New()
	router.POST("/v1/images/edits", h.Edits)

	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	if err := writer.WriteField("model", "gpt-image-2"); err != nil {
		t.Fatalf("WriteField model: %v", err)
	}
	if err := writer.WriteField("prompt", "edit this"); err != nil {
		t.Fatalf("WriteField prompt: %v", err)
	}
	if err := writer.WriteField("input_fidelity", "high"); err != nil {
		t.Fatalf("WriteField input_fidelity: %v", err)
	}
	if err := writer.WriteField("n", "1"); err != nil {
		t.Fatalf("WriteField n: %v", err)
	}
	imagePart, err := writer.CreateFormFile("image[]", "image.png")
	if err != nil {
		t.Fatalf("CreateFormFile image: %v", err)
	}
	_, _ = imagePart.Write([]byte("\x89PNG\r\n\x1a\nimage-data"))
	maskPart, err := writer.CreateFormFile("mask", "mask.png")
	if err != nil {
		t.Fatalf("CreateFormFile mask: %v", err)
	}
	_, _ = maskPart.Write([]byte("\x89PNG\r\n\x1a\nmask-data"))
	if err := writer.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}

	req = httptest.NewRequest(http.MethodPost, "/v1/images/edits", &body)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	resp := httptest.NewRecorder()
	router.ServeHTTP(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body=%s", resp.Code, http.StatusOK, resp.Body.String())
	}
	if !releasedBeforeExecute {
		t.Fatal("multipart request body and form were retained until executor invocation")
	}
	if got := gjson.GetBytes(executor.payload, "tools.0.action").String(); got != "edit" {
		t.Fatalf("tool action = %q", got)
	}
	if got := gjson.GetBytes(executor.payload, "tools.0.input_fidelity").String(); got != "high" {
		t.Fatalf("tool input_fidelity = %q", got)
	}
	assertImageToolNAbsent(t, executor.payload)
	imageURL := gjson.GetBytes(executor.payload, "input.0.content.1.image_url").String()
	if !strings.HasPrefix(imageURL, "data:image/png;base64,") {
		t.Fatalf("image_url = %q", imageURL)
	}
	maskURL := gjson.GetBytes(executor.payload, "tools.0.input_image_mask.image_url").String()
	if !strings.HasPrefix(maskURL, "data:image/png;base64,") {
		t.Fatalf("mask image_url = %q", maskURL)
	}
}

func TestDataURLFromFileHeaderStreamsAndEnforcesLimit(t *testing.T) {
	content := []byte("\x89PNG\r\n\x1a\nimage-data")
	fileHeader, form := imageFileHeaderForTest(t, "image.png", content)
	defer func() { _ = form.RemoveAll() }()

	dataURL, err := dataURLFromFileHeaderWithLimit(fileHeader, int64(len(content)))
	if err != nil {
		t.Fatalf("dataURLFromFileHeaderWithLimit() error = %v", err)
	}
	if !strings.HasPrefix(dataURL, "data:image/png;base64,") {
		t.Fatalf("data URL prefix = %q", dataURL)
	}
	encoded := strings.TrimPrefix(dataURL, "data:image/png;base64,")
	decoded, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		t.Fatalf("decode data URL: %v", err)
	}
	if !bytes.Equal(decoded, content) {
		t.Fatalf("decoded content = %q, want %q", decoded, content)
	}

	fileHeader.Size = 0
	if _, err = dataURLFromFileHeaderWithLimit(fileHeader, int64(len(content)-1)); err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("over-limit error = %v, want exceeds", err)
	}
}

func TestDataURLFromFileHeaderRejectsEmptyFile(t *testing.T) {
	fileHeader, form := imageFileHeaderForTest(t, "empty.png", nil)
	defer func() { _ = form.RemoveAll() }()
	if _, err := dataURLFromFileHeaderWithLimit(fileHeader, 1); err == nil || !strings.Contains(err.Error(), "empty") {
		t.Fatalf("empty file error = %v", err)
	}
}

func imageFileHeaderForTest(t *testing.T, name string, content []byte) (*multipart.FileHeader, *multipart.Form) {
	t.Helper()
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	part, err := writer.CreateFormFile("image", name)
	if err != nil {
		t.Fatalf("CreateFormFile() error = %v", err)
	}
	if _, err = part.Write(content); err != nil {
		t.Fatalf("write multipart file: %v", err)
	}
	if err = writer.Close(); err != nil {
		t.Fatalf("close multipart writer: %v", err)
	}
	request := httptest.NewRequest(http.MethodPost, "/", &body)
	request.Header.Set("Content-Type", writer.FormDataContentType())
	if err = request.ParseMultipartForm(1); err != nil {
		t.Fatalf("ParseMultipartForm() error = %v", err)
	}
	files := request.MultipartForm.File["image"]
	if len(files) != 1 {
		t.Fatalf("multipart files = %d, want 1", len(files))
	}
	return files[0], request.MultipartForm
}

func TestOpenAIImagesNativeEditsJSONForwardsFileIDs(t *testing.T) {
	gin.SetMode(gin.TestMode)
	executor := &imageCaptureExecutor{
		response: []byte(`{"created":1700000000,"data":[{"b64_json":"ZWRpdA=="}]}`),
	}
	h := newImagesTestHandler(t, executor, "gpt-image-2")
	h.Cfg.Images.Native.Edits.Enabled = true
	h.Cfg.Images.Native.Edits.Models = []string{"gpt-image-2"}
	h.Cfg.Images.Native.Edits.UnsupportedModelStatusCode = http.StatusBadRequest
	h.Cfg.Images.Native.Edits.UnsupportedModelMessage = "native edit disabled for {model}"
	router := gin.New()
	router.POST("/v1/images/edits", h.Edits)

	raw := `{"model":"gpt-image-2","prompt":"edit this","images":[{"file_id":"file_image"},{"image_url":"https://example.com/image.png"}],"mask":{"file_id":"file_mask"}}`
	req := httptest.NewRequest(http.MethodPost, "/v1/images/edits", strings.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	router.ServeHTTP(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body=%s", resp.Code, http.StatusOK, resp.Body.String())
	}
	if executor.calls != 1 {
		t.Fatalf("executor calls = %d, want 1", executor.calls)
	}
	if executor.sourceFormat != nativeImagesHandlerType {
		t.Fatalf("source format = %q, want %s", executor.sourceFormat, nativeImagesHandlerType)
	}
	if executor.alt != nativeImagesEdits {
		t.Fatalf("alt = %q, want %s", executor.alt, nativeImagesEdits)
	}
	if got := gjson.GetBytes(executor.payload, "images.0.file_id").String(); got != "file_image" {
		t.Fatalf("images.0.file_id = %q", got)
	}
	if got := gjson.GetBytes(executor.payload, "mask.file_id").String(); got != "file_mask" {
		t.Fatalf("mask.file_id = %q", got)
	}
	if got := gjson.GetBytes(executor.payload, "images.1.image_url").String(); got != "https://example.com/image.png" {
		t.Fatalf("images.1.image_url = %q", got)
	}
}

func TestOpenAIImagesNativeEditsMultipartConvertsToDataURLJSON(t *testing.T) {
	gin.SetMode(gin.TestMode)
	releasedBeforeExecute := false
	var req *http.Request
	executor := &imageCaptureExecutor{
		response: []byte(`{"created":1700000000,"data":[{"b64_json":"ZWRpdA=="}]}`),
		beforeExecute: func() {
			releasedBeforeExecute = req != nil && req.Body == http.NoBody && req.MultipartForm == nil
		},
	}
	h := newImagesTestHandler(t, executor, "gpt-image-2")
	h.Cfg.Images.Native.Edits.Enabled = true
	h.Cfg.Images.Native.Edits.Models = []string{"gpt-image-2"}
	h.Cfg.Images.Native.Edits.UnsupportedModelStatusCode = http.StatusBadRequest
	h.Cfg.Images.Native.Edits.UnsupportedModelMessage = "native edit disabled for {model}"
	router := gin.New()
	router.POST("/v1/images/edits", h.Edits)

	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	if err := writer.WriteField("model", "gpt-image-2"); err != nil {
		t.Fatalf("WriteField model: %v", err)
	}
	if err := writer.WriteField("prompt", "edit this"); err != nil {
		t.Fatalf("WriteField prompt: %v", err)
	}
	if err := writer.WriteField("n", "1"); err != nil {
		t.Fatalf("WriteField n: %v", err)
	}
	imagePart, err := writer.CreateFormFile("image", "image.png")
	if err != nil {
		t.Fatalf("CreateFormFile image: %v", err)
	}
	_, _ = imagePart.Write([]byte("\x89PNG\r\n\x1a\nimage-data"))
	maskPart, err := writer.CreateFormFile("mask", "mask.png")
	if err != nil {
		t.Fatalf("CreateFormFile mask: %v", err)
	}
	_, _ = maskPart.Write([]byte("\x89PNG\r\n\x1a\nmask-data"))
	if err := writer.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}

	req = httptest.NewRequest(http.MethodPost, "/v1/images/edits", &body)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	resp := httptest.NewRecorder()
	router.ServeHTTP(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body=%s", resp.Code, http.StatusOK, resp.Body.String())
	}
	if !releasedBeforeExecute {
		t.Fatal("native multipart request body and form were retained until executor invocation")
	}
	if got := gjson.GetBytes(executor.payload, "tools"); got.Exists() {
		t.Fatalf("tools exists = %s, want absent", got.Raw)
	}
	if got := gjson.GetBytes(executor.payload, "images.0.image_url").String(); !strings.HasPrefix(got, "data:image/png;base64,") {
		t.Fatalf("images.0.image_url = %q", got)
	}
	if got := gjson.GetBytes(executor.payload, "mask.image_url").String(); !strings.HasPrefix(got, "data:image/png;base64,") {
		t.Fatalf("mask.image_url = %q", got)
	}
	if got := gjson.GetBytes(executor.payload, "n").Int(); got != 1 {
		t.Fatalf("n = %d, want 1", got)
	}
}

func TestOpenAIImagesEditsCanOverrideInputFidelity(t *testing.T) {
	gin.SetMode(gin.TestMode)
	executor := &imageCaptureExecutor{}
	h := newImagesTestHandler(t, executor, "gpt-image-1.5", "gpt-5.4-mini")
	overrideInputFidelity := true
	h.Cfg.Images.OverrideInputFidelity = &overrideInputFidelity
	h.Cfg.Images.ImageModel = "gpt-image-1.5"
	router := gin.New()
	router.POST("/v1/images/edits", h.Edits)

	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	if err := writer.WriteField("model", "gpt-image-1.5"); err != nil {
		t.Fatalf("WriteField model: %v", err)
	}
	if err := writer.WriteField("prompt", "edit this"); err != nil {
		t.Fatalf("WriteField prompt: %v", err)
	}
	if err := writer.WriteField("input_fidelity", "high"); err != nil {
		t.Fatalf("WriteField input_fidelity: %v", err)
	}
	imagePart, err := writer.CreateFormFile("image", "image.png")
	if err != nil {
		t.Fatalf("CreateFormFile image: %v", err)
	}
	_, _ = imagePart.Write([]byte("\x89PNG\r\n\x1a\nimage-data"))
	if err := writer.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/v1/images/edits", &body)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	resp := httptest.NewRecorder()
	router.ServeHTTP(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body=%s", resp.Code, http.StatusOK, resp.Body.String())
	}
	if got := gjson.GetBytes(executor.payload, "tools.0.input_fidelity"); got.Exists() {
		t.Fatalf("tool input_fidelity exists = %s, want absent", got.Raw)
	}
}

func TestOpenAIImagesEditsJSONDoesNotForwardN(t *testing.T) {
	gin.SetMode(gin.TestMode)
	executor := &imageCaptureExecutor{}
	h := newImagesTestHandler(t, executor)
	router := gin.New()
	router.POST("/v1/images/edits", h.Edits)

	req := httptest.NewRequest(http.MethodPost, "/v1/images/edits", strings.NewReader(`{"model":"gpt-image-2","prompt":"edit this","n":1,"images":[{"image_url":"data:image/png;base64,aGVsbG8="}]}`))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	router.ServeHTTP(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body=%s", resp.Code, http.StatusOK, resp.Body.String())
	}
	if executor.calls != 1 {
		t.Fatalf("executor calls = %d, want 1", executor.calls)
	}
	if got := gjson.GetBytes(executor.payload, "tools.0.action").String(); got != "edit" {
		t.Fatalf("tool action = %q", got)
	}
	assertImageToolNAbsent(t, executor.payload)
}

func TestConvertResponsesToImagesResponse(t *testing.T) {
	raw := []byte(`{"created_at":1700000000,"output":[{"type":"message"},{"type":"image_generation_call","result":"ZmluYWw=","revised_prompt":"better","output_format":"webp","size":"1024x1024","background":"opaque","quality":"high"}],"tool_usage":{"image_gen":{"input_tokens":3,"input_tokens_details":{"text_tokens":3,"image_tokens":0,"cache_tokens":2},"output_tokens":6,"output_tokens_details":{"image_tokens":6,"text_tokens":0},"total_tokens":9,"future_detail":{"source":"official"}}},"usage":{"total_tokens":999}}`)
	out, err := convertResponsesToImagesResponse(raw, 1)
	if err != nil {
		t.Fatalf("convertResponsesToImagesResponse: %v", err)
	}
	if got := gjson.GetBytes(out, "created").Int(); got != 1700000000 {
		t.Fatalf("created = %d", got)
	}
	if got := gjson.GetBytes(out, "data.0.b64_json").String(); got != "ZmluYWw=" {
		t.Fatalf("b64_json = %q", got)
	}
	if got := gjson.GetBytes(out, "data.0.revised_prompt").String(); got != "better" {
		t.Fatalf("revised_prompt = %q", got)
	}
	if got := gjson.GetBytes(out, "output_format").String(); got != "webp" {
		t.Fatalf("output_format = %q", got)
	}
	if got := gjson.GetBytes(out, "size").String(); got != "1024x1024" {
		t.Fatalf("size = %q", got)
	}
	if got := gjson.GetBytes(out, "background").String(); got != "opaque" {
		t.Fatalf("background = %q", got)
	}
	if got := gjson.GetBytes(out, "quality").String(); got != "high" {
		t.Fatalf("quality = %q", got)
	}
	if got := gjson.GetBytes(out, "usage.total_tokens").Int(); got != 9 {
		t.Fatalf("usage.total_tokens = %d", got)
	}
	if got := gjson.GetBytes(out, "usage.input_tokens").Int(); got != 3 {
		t.Fatalf("usage.input_tokens = %d", got)
	}
	if got := gjson.GetBytes(out, "usage.output_tokens").Int(); got != 6 {
		t.Fatalf("usage.output_tokens = %d", got)
	}
	if got := gjson.GetBytes(out, "usage.input_tokens_details.text_tokens").Int(); got != 3 {
		t.Fatalf("usage.input_tokens_details.text_tokens = %d", got)
	}
	if got := gjson.GetBytes(out, "usage.output_tokens_details.image_tokens").Int(); got != 6 {
		t.Fatalf("usage.output_tokens_details.image_tokens = %d", got)
	}
	if got := gjson.GetBytes(out, "usage.input_tokens_details.cache_tokens").Int(); got != 2 {
		t.Fatalf("unknown input detail = %d", got)
	}
	if got := gjson.GetBytes(out, "usage.future_detail.source").String(); got != "official" {
		t.Fatalf("unknown usage object = %q", got)
	}
}

func TestConvertResponsesToImagesResponseClassifiesCompletedTextWithoutImage(t *testing.T) {
	raw := []byte(`{"created_at":1700000000,"status":"completed","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"Any non-empty response"}]}],"usage":{"total_tokens":9}}`)
	_, err := convertResponsesToImagesResponse(raw, 1)
	if err == nil {
		t.Fatal("convertResponsesToImagesResponse succeeded, want error")
	}
	var status interface{ StatusCode() int }
	if !errors.As(err, &status) || status.StatusCode() != http.StatusBadRequest || err.Error() != executorhelps.OpenAIImageModerationErrorBody {
		t.Fatalf("moderation error = %v", err)
	}
}

func TestConvertResponsesToImagesResponseKeepsAmbiguousNoOutputErrors(t *testing.T) {
	for _, raw := range []string{
		`{"status":"completed","output":[{"type":"message","content":[{"type":"output_text","text":"   "}]}]}`,
		`{"status":"failed","output":[{"type":"message","content":[{"type":"output_text","text":"failure detail"}]}]}`,
		`{"status":"completed","output":[]}`,
	} {
		_, err := convertResponsesToImagesResponse([]byte(raw), 1)
		if err == nil || !strings.Contains(err.Error(), "upstream did not return image output") {
			t.Fatalf("ambiguous response error = %v for %s", err, raw)
		}
		var status interface{ StatusCode() int }
		if errors.As(err, &status) {
			t.Fatalf("ambiguous response received status %d", status.StatusCode())
		}
	}
}

func TestConvertResponsesToImagesResponseInfersImageMetadata(t *testing.T) {
	const png1x1 = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII="
	raw := []byte(`{"created_at":1700000000,"output":[{"type":"image_generation_call","result":"` + png1x1 + `","output_format":"auto","size":"auto","quality":"auto","background":"auto"}]}`)
	out, errConvert := convertResponsesToImagesResponse(raw, 1)
	if errConvert != nil {
		t.Fatalf("convertResponsesToImagesResponse() error = %v", errConvert)
	}
	if got := gjson.GetBytes(out, "output_format").String(); got != "png" {
		t.Fatalf("output_format = %q, want png", got)
	}
	if got := gjson.GetBytes(out, "size").String(); got != "1x1" {
		t.Fatalf("size = %q, want 1x1", got)
	}
	if got := gjson.GetBytes(out, "quality"); got.Exists() {
		t.Fatalf("unknown quality should be omitted: %s", got.Raw)
	}
	if got := gjson.GetBytes(out, "background"); got.Exists() {
		t.Fatalf("unknown background should be omitted: %s", got.Raw)
	}
}

func TestApplyRequestedImageMetadataUsesDecodedValuesBeforeExplicitRequest(t *testing.T) {
	const png1x1 = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII="
	raw := []byte(`{"created_at":1700000000,"output":[{"type":"image_generation_call","result":"` + png1x1 + `"}]}`)
	response, errConvert := parseResponsesToImagesResponse(raw, 1)
	if errConvert != nil {
		t.Fatalf("parseResponsesToImagesResponse() error = %v", errConvert)
	}

	applyRequestedImageMetadata(&response, openAIImageRequest{
		OutputFormat: "webp",
		Size:         "1024x1536",
		Quality:      "high",
		Background:   "opaque",
	})

	if response.OutputFormat != "png" || response.Size != "1x1" {
		t.Fatalf("decoded metadata was overwritten: format=%q size=%q", response.OutputFormat, response.Size)
	}
	if response.Quality != "high" || response.Background != "opaque" {
		t.Fatalf("explicit request metadata was not applied: quality=%q background=%q", response.Quality, response.Background)
	}
}

func TestApplyRequestedImageMetadataDoesNotInventFormatOrSize(t *testing.T) {
	response := imagesResponse{Data: []imageResult{{B64JSON: "bm90LWFuLWltYWdl"}}}

	applyRequestedImageMetadata(&response, openAIImageRequest{
		OutputFormat: "webp",
		Size:         "1024x1536",
		Quality:      "high",
		Background:   "opaque",
	})

	if response.OutputFormat != "" || response.Size != "" {
		t.Fatalf("unknown metadata was invented: format=%q size=%q", response.OutputFormat, response.Size)
	}
	if response.Quality != "high" || response.Background != "opaque" {
		t.Fatalf("confirmed request metadata was not applied: quality=%q background=%q", response.Quality, response.Background)
	}
}

func TestOpenAIImagesNonStreamingReturnsModerationForCompletedTextWithoutImage(t *testing.T) {
	gin.SetMode(gin.TestMode)
	executor := &imageCaptureExecutor{
		response: []byte(`{"created_at":1700000000,"status":"completed","output":[{"type":"message","content":[{"type":"output_text","text":"Any non-empty response"}]}],"usage":{"total_tokens":9}}`),
	}
	h := newImagesTestHandler(t, executor)
	router := gin.New()
	router.POST("/v1/images/generations", h.Generations)

	req := httptest.NewRequest(http.MethodPost, "/v1/images/generations", strings.NewReader(`{"model":"gpt-image-2","prompt":"draw"}`))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	router.ServeHTTP(resp, req)

	if resp.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d, body=%s", resp.Code, http.StatusBadRequest, resp.Body.String())
	}
	if !strings.Contains(resp.Body.String(), "moderation_blocked") ||
		strings.Contains(resp.Body.String(), "upstream did not return image output") {
		t.Fatalf("moderation error missing or leaked generic detail: %s", resp.Body.String())
	}
}

func TestOpenAIImagesStreamingMapsPartialAndCompletedEvents(t *testing.T) {
	gin.SetMode(gin.TestMode)
	executor := &imageCaptureExecutor{
		streamChunks: []coreexecutor.StreamChunk{
			{Payload: []byte(`data: {"type":"response.image_generation_call.partial_image","partial_image_b64":"cGFydA==","partial_image_index":0}`)},
			{Payload: []byte(`data: {"type":"response.completed","response":{"created_at":1700000000,"output":[{"type":"image_generation_call","result":"ZmluYWw=","revised_prompt":"rev"}],"usage":{"total_tokens":7}}}`)},
		},
	}
	h := newImagesTestHandler(t, executor)
	router := gin.New()
	router.POST("/v1/images/generations", h.Generations)

	req := httptest.NewRequest(http.MethodPost, "/v1/images/generations", strings.NewReader(`{"model":"gpt-image-2","prompt":"draw","stream":true}`))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	router.ServeHTTP(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body=%s", resp.Code, http.StatusOK, resp.Body.String())
	}
	body := resp.Body.String()
	if !strings.Contains(body, "event: image_generation.partial_image") || !strings.Contains(body, `"b64_json":"cGFydA=="`) {
		t.Fatalf("partial event missing: %s", body)
	}
	if !strings.Contains(body, "event: image_generation.completed") || !strings.Contains(body, `"b64_json":"ZmluYWw="`) {
		t.Fatalf("completed event missing: %s", body)
	}
	if !strings.Contains(body, `"total_tokens":7`) {
		t.Fatalf("usage missing: %s", body)
	}
}

func TestOpenAIImagesStreamingCanReturnDataURLForURLResponseFormat(t *testing.T) {
	gin.SetMode(gin.TestMode)
	executor := &imageCaptureExecutor{
		streamChunks: []coreexecutor.StreamChunk{
			{Payload: []byte(`data: {"type":"response.image_generation_call.partial_image","partial_image_b64":"cGFydA==","partial_image_index":0,"output_format":"png"}`)},
			{Payload: []byte(`data: {"type":"response.completed","response":{"created_at":1700000000,"output":[{"type":"image_generation_call","result":"ZmluYWw=","output_format":"webp"}]}}`)},
		},
	}
	h := newImagesTestHandler(t, executor)
	responseFormatURLDataURL := true
	h.Cfg.Images.ResponseFormatURLDataURL = &responseFormatURLDataURL
	router := gin.New()
	router.POST("/v1/images/generations", h.Generations)

	req := httptest.NewRequest(http.MethodPost, "/v1/images/generations", strings.NewReader(`{"model":"gpt-image-2","prompt":"draw","stream":true,"response_format":"url"}`))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	router.ServeHTTP(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body=%s", resp.Code, http.StatusOK, resp.Body.String())
	}
	body := resp.Body.String()
	if !strings.Contains(body, `"url":"data:image/png;base64,cGFydA=="`) {
		t.Fatalf("partial data URL missing: %s", body)
	}
	if !strings.Contains(body, `"url":"data:image/webp;base64,ZmluYWw="`) {
		t.Fatalf("completed data URL missing: %s", body)
	}
	if strings.Contains(body, "b64_json") {
		t.Fatalf("b64_json should be absent in URL response format: %s", body)
	}
}

func TestOpenAIImagesStreamingReturnsModerationWhenCompletedWithTextWithoutImage(t *testing.T) {
	gin.SetMode(gin.TestMode)
	executor := &imageCaptureExecutor{
		streamChunks: []coreexecutor.StreamChunk{
			{Payload: []byte(`data: {"type":"response.completed","response":{"created_at":1700000000,`)},
			{Payload: []byte(`"status":"completed","output":[{"type":"message","content":[{"type":"output_text","text":"Any non-empty response"}]}],"usage":{"total_tokens":7}}}`)},
		},
	}
	h := newImagesTestHandler(t, executor)
	router := gin.New()
	router.POST("/v1/images/generations", h.Generations)

	req := httptest.NewRequest(http.MethodPost, "/v1/images/generations", strings.NewReader(`{"model":"gpt-image-2","prompt":"draw","stream":true}`))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	router.ServeHTTP(resp, req)

	if resp.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d, body=%s", resp.Code, http.StatusBadRequest, resp.Body.String())
	}
	body := resp.Body.String()
	if strings.Contains(body, "event: error") {
		t.Fatalf("preflight failure should use a JSON error response: %s", body)
	}
	if !strings.Contains(body, "moderation_blocked") || strings.Contains(body, "upstream did not return image output") {
		t.Fatalf("moderation error missing: %s", body)
	}
	if strings.Contains(body, "event: image_generation.completed") {
		t.Fatalf("completed event should not be emitted: %s", body)
	}
}

func TestOpenAIImagesStreamingReturnsModerationAfterPartialImage(t *testing.T) {
	gin.SetMode(gin.TestMode)
	executor := &imageCaptureExecutor{
		streamChunks: []coreexecutor.StreamChunk{
			{Payload: []byte(`data: {"type":"response.image_generation_call.partial_image","partial_image_b64":"cGFydA=="}` + "\n\n")},
			{Payload: []byte(`data: {"type":"response.output_text.delta","delta":"Any response"}` + "\n\n")},
			{Payload: []byte(`data: {"type":"response.completed","response":{"status":"completed","output":[]}}` + "\n\n")},
		},
	}
	h := newImagesTestHandler(t, executor)
	router := gin.New()
	router.POST("/v1/images/generations", h.Generations)

	req := httptest.NewRequest(http.MethodPost, "/v1/images/generations", strings.NewReader(`{"model":"gpt-image-2","prompt":"draw","stream":true}`))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	router.ServeHTTP(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200, body=%s", resp.Code, resp.Body.String())
	}
	body := resp.Body.String()
	if !strings.Contains(body, "image_generation.partial_image") || !strings.Contains(body, "event: error") || !strings.Contains(body, "moderation_blocked") {
		t.Fatalf("stream moderation sequence missing: %s", body)
	}
	if strings.Contains(body, "upstream did not return image output") {
		t.Fatalf("generic no-output error leaked: %s", body)
	}
}

func TestOpenAIImagesStreamingSupportsMultipleCompletedImages(t *testing.T) {
	gin.SetMode(gin.TestMode)
	executor := &imageCaptureExecutor{
		streamChunks: []coreexecutor.StreamChunk{
			{Payload: []byte(`data: {"type":"response.output_item.done","item":{"type":"image_generation_call","result":"Zmlyc3Q="}}`)},
			{Payload: []byte(`data: {"type":"response.completed","response":{"created_at":1700000000,"output":[],"tool_usage":{"image_gen":{"input_tokens":1,"input_tokens_details":{"text_tokens":1,"image_tokens":0},"output_tokens":2,"output_tokens_details":{"image_tokens":2,"text_tokens":0},"total_tokens":3}},"usage":{"input_tokens":99,"output_tokens":99,"total_tokens":198}}}`)},
		},
	}
	h := newImagesTestHandler(t, executor)
	enableNAggregation := true
	h.Cfg.Images.EnableNAggregation = &enableNAggregation
	router := gin.New()
	router.POST("/v1/images/generations", h.Generations)

	req := httptest.NewRequest(http.MethodPost, "/v1/images/generations", strings.NewReader(`{"model":"gpt-image-2","prompt":"draw","stream":true,"n":2}`))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	router.ServeHTTP(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body=%s", resp.Code, http.StatusOK, resp.Body.String())
	}
	if executor.streamCalls != 2 {
		t.Fatalf("stream calls = %d, want 2", executor.streamCalls)
	}
	assertImageToolNAbsent(t, executor.payload)
	body := resp.Body.String()
	if count := strings.Count(body, "event: image_generation.completed"); count != 2 {
		t.Fatalf("completed event count = %d, want 2: %s", count, body)
	}
	if strings.Count(body, `"b64_json":"Zmlyc3Q="`) != 2 {
		t.Fatalf("completed image payloads missing: %s", body)
	}
	if strings.Count(body, `"input_tokens":1`) != 1 {
		t.Fatalf("input tokens should appear once across aggregated stream: %s", body)
	}
	if strings.Count(body, `"output_tokens":2`) != 2 {
		t.Fatalf("output tokens should appear on each completed image: %s", body)
	}
}

func TestOpenAIImagesStreamingEnforcesAggregateWebResponseBudget(t *testing.T) {
	gin.SetMode(gin.TestMode)
	encoded := base64.StdEncoding.EncodeToString(make([]byte, 700<<10))
	executor := &imageCaptureExecutor{
		provider: constant.ChatGPTWeb,
		streamChunks: []coreexecutor.StreamChunk{{Payload: []byte(
			`data: {"type":"response.completed","response":{"output":[{"type":"image_generation_call","result":"` + encoded + `"}]}}` + "\n\n",
		)}},
	}
	h := newImagesTestHandler(t, executor)
	enableNAggregation := true
	oneMegabyte := 1
	h.Cfg.Images.EnableNAggregation = &enableNAggregation
	h.Cfg.Images.ChatGPTWeb.MaxImageResponseMegabytes = &oneMegabyte
	router := gin.New()
	router.POST("/v1/images/generations", h.Generations)

	req := httptest.NewRequest(http.MethodPost, "/v1/images/generations", strings.NewReader(`{"model":"gpt-image-2","prompt":"draw","stream":true,"n":2}`))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	router.ServeHTTP(resp, req)

	body := resp.Body.String()
	if count := strings.Count(body, "event: image_generation.completed"); count != 1 {
		t.Fatalf("completed event count = %d, want 1", count)
	}
	if count := strings.Count(body, "event: error"); count != 1 || !strings.Contains(body, "image response exceeds") {
		t.Fatalf("stream budget error missing: %s", body)
	}
	if executor.streamCalls != 2 || len(executor.imageConfigSnapshots) != 2 {
		t.Fatalf("stream calls=%d snapshots=%d", executor.streamCalls, len(executor.imageConfigSnapshots))
	}
	if got := executor.imageConfigSnapshots[1].MaxImageResponseBytes; got != (1<<20)-(700<<10) {
		t.Fatalf("second stream response budget = %d", got)
	}
}

func TestOpenAIImagesStreamingStopsMultiImageRequestAfterIncompleteStream(t *testing.T) {
	gin.SetMode(gin.TestMode)
	executor := &imageCaptureExecutor{
		streamChunks: []coreexecutor.StreamChunk{
			{Payload: []byte(`data: {"type":"response.image_generation_call.partial_image","partial_image_b64":"cGFydA=="}` + "\n\n")},
		},
	}
	h := newImagesTestHandler(t, executor)
	enableNAggregation := true
	h.Cfg.Images.EnableNAggregation = &enableNAggregation
	router := gin.New()
	router.POST("/v1/images/generations", h.Generations)

	req := httptest.NewRequest(http.MethodPost, "/v1/images/generations", strings.NewReader(`{"model":"gpt-image-2","prompt":"draw","stream":true,"n":2}`))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	router.ServeHTTP(resp, req)

	if executor.streamCalls != 1 {
		t.Fatalf("stream calls = %d, want 1 after terminal error", executor.streamCalls)
	}
	if count := strings.Count(resp.Body.String(), "event: error"); count != 1 {
		t.Fatalf("error event count = %d, want 1: %s", count, resp.Body.String())
	}
}

func TestOpenAIImagesStreamingStopsMultiImageRequestAfterCompletedWithoutImage(t *testing.T) {
	gin.SetMode(gin.TestMode)
	executor := &imageCaptureExecutor{
		streamChunks: []coreexecutor.StreamChunk{
			{Payload: []byte(`data: {"type":"response.completed","response":{"output":[]}}` + "\n\n")},
		},
	}
	h := newImagesTestHandler(t, executor)
	enableNAggregation := true
	h.Cfg.Images.EnableNAggregation = &enableNAggregation
	router := gin.New()
	router.POST("/v1/images/generations", h.Generations)

	req := httptest.NewRequest(http.MethodPost, "/v1/images/generations", strings.NewReader(`{"model":"gpt-image-2","prompt":"draw","stream":true,"n":2}`))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	router.ServeHTTP(resp, req)

	if executor.streamCalls != 1 {
		t.Fatalf("stream calls = %d, want 1 after terminal error", executor.streamCalls)
	}
	if count := strings.Count(resp.Body.String(), "event: error"); count != 1 {
		t.Fatalf("error event count = %d, want 1: %s", count, resp.Body.String())
	}
}

func TestImageStreamMapperPreservesHeartbeatAndCapsResults(t *testing.T) {
	mapper := &imageStreamMapper{operation: imageGenerationOperation, maxResults: 1}
	var output bytes.Buffer
	mapper.writeChunk(&output, []byte(": chatgpt-web upstream pending\n\n"))
	mapper.writeChunk(&output, []byte(`data: {"type":"response.completed","response":{"output":[{"type":"image_generation_call","result":"Zmlyc3Q="},{"type":"image_generation_call","result":"c2Vjb25k"}]}}`+"\n\n"))

	body := output.String()
	if !strings.Contains(body, ": chatgpt-web upstream pending") {
		t.Fatalf("heartbeat missing: %s", body)
	}
	if count := strings.Count(body, "event: image_generation.completed"); count != 1 {
		t.Fatalf("completed count = %d, body=%s", count, body)
	}
	if strings.Contains(body, "c2Vjb25k") {
		t.Fatalf("second image was not capped: %s", body)
	}

	var mixed bytes.Buffer
	mapper = &imageStreamMapper{operation: imageGenerationOperation, maxResults: 1}
	mapper.writeChunk(&mixed, []byte(": upstream pending\n\ndata: {\"type\":\"response.completed\",\"response\":{\"output\":[{\"type\":\"image_generation_call\",\"result\":\"Zmlyc3Q=\"}]}}\n\n"))
	if strings.Contains(mixed.String(), `"type":"response.completed"`) ||
		!strings.Contains(mixed.String(), "event: image_generation.completed") {
		t.Fatalf("mixed comment/data chunk bypassed mapping: %s", mixed.String())
	}

	var split bytes.Buffer
	mapper = &imageStreamMapper{operation: imageGenerationOperation, maxResults: 1}
	mapper.writeChunk(&split, []byte(": upstream pend"))
	mapper.writeChunk(&split, []byte("ing\n\ndata: {\"type\":\"response.completed\",\"response\":{\"output\":[{\"type\":\"image_generation_call\",\"result\":\"Zmlyc3Q=\"}]}}\n\n"))
	if !strings.Contains(split.String(), ": upstream pending\n\n") ||
		strings.Contains(split.String(), ": upstream pendevent:") ||
		!strings.Contains(split.String(), "event: image_generation.completed") {
		t.Fatalf("split comment corrupted mapped event: %s", split.String())
	}
	if !mapper.consumeForceFlush() || mapper.consumeForceFlush() {
		t.Fatal("split comment did not request exactly one immediate flush")
	}
}

func TestImageSSEParserWaitsForValidJSON(t *testing.T) {
	mapper := &imageStreamMapper{operation: imageGenerationOperation, maxResults: 1}
	var output bytes.Buffer
	mapper.writeChunk(&output, []byte(`data: {"type":"response.completed","response":{"output":[{"type":"image_generation_call","result":"Zm`))
	if output.Len() != 0 {
		t.Fatalf("truncated JSON was emitted: %q", output.String())
	}
	mapper.writeChunk(&output, []byte(`9v"}]}}`+"\n\n"))
	if !strings.Contains(output.String(), "event: image_generation.completed") ||
		!strings.Contains(output.String(), `Zm9v`) {
		t.Fatalf("completed split JSON missing: %s", output.String())
	}
}

func TestImageSSEParserJoinsMultilineDataFields(t *testing.T) {
	mapper := &imageStreamMapper{operation: imageGenerationOperation, maxResults: 1}
	var output bytes.Buffer
	mapper.writeChunk(&output, []byte("data: {\"type\":\"response.completed\",\ndata: \"response\":{\"output\":[{\"type\":\"image_generation_call\",\"result\":\"Zm9v\"}]}}\n\n"))
	if !strings.Contains(output.String(), "event: image_generation.completed") ||
		!strings.Contains(output.String(), `"b64_json":"Zm9v"`) {
		t.Fatalf("multiline SSE output = %q", output.String())
	}
}

func TestExtractImageSSEItemsOwnsSingleDataPayload(t *testing.T) {
	frame := []byte("data: {\"type\":\"response.output_item.done\"}\n\n")
	items := extractImageSSEItems(frame)
	if len(items) != 1 || len(items[0].payload) == 0 {
		t.Fatalf("items = %#v", items)
	}
	want := string(items[0].payload)
	for index := range frame {
		frame[index] = 'x'
	}
	if got := string(items[0].payload); got != want {
		t.Fatalf("payload changed after source reuse: got %q want %q", got, want)
	}
}

func TestImageSSEParserRejectsOversizedPendingFrame(t *testing.T) {
	mapper := &imageStreamMapper{
		operation: imageGenerationOperation,
		parser:    imageSSEParser{maxPendingBytes: 8},
	}
	var output bytes.Buffer
	mapper.writeChunk(&output, []byte("12345678"))
	mapper.writeChunk(&output, []byte("9"))
	if output.Len() != 0 {
		t.Fatalf("oversized parser wrote terminal output directly: %q", output.String())
	}
	if err := mapper.fatalError(); err == nil || !strings.Contains(err.Error(), "exceeds 8 bytes") {
		t.Fatalf("oversized parser error = %v", err)
	}
}

func TestImageSSEParserAllowsMultipleBoundedFramesInOneChunk(t *testing.T) {
	parser := imageSSEParser{maxPendingBytes: 40}

	items := parser.Push([]byte("data: {\"type\":\"first\"}\n\ndata: {\"type\":\"second\"}\n\n"))

	if err := parser.Error(); err != nil {
		t.Fatalf("parser error = %v", err)
	}
	if len(items) != 2 {
		t.Fatalf("item count = %d, want 2", len(items))
	}
	if got := gjson.GetBytes(items[0].payload, "type").String(); got != "first" {
		t.Fatalf("first item type = %q", got)
	}
	if got := gjson.GetBytes(items[1].payload, "type").String(); got != "second" {
		t.Fatalf("second item type = %q", got)
	}
}

func TestImageStreamMapperRejectsCompletedImageOverBinaryBudget(t *testing.T) {
	mapper := &imageStreamMapper{operation: imageGenerationOperation, maxBinaryBytes: 4}
	var output bytes.Buffer
	mapper.writeChunk(&output, []byte(`data: {"type":"response.completed","response":{"output":[{"type":"image_generation_call","result":"aGVsbG8="}]}}`+"\n\n"))
	if output.Len() != 0 {
		t.Fatalf("oversized completed image was written: %q", output.String())
	}
	if err := mapper.fatalError(); err == nil || !strings.Contains(err.Error(), "image response exceeds 4 bytes") {
		t.Fatalf("mapper error = %v", err)
	}
}

func TestDefaultImageSSEPendingLimitCoversMaximumEncodedImage(t *testing.T) {
	encodedBytes := base64.StdEncoding.EncodedLen(sdkconfig.DefaultChatGPTWebMaxImageResponseMegabytes << 20)
	if maxImageSSEPendingBytes <= encodedBytes {
		t.Fatalf("SSE pending limit = %d, encoded image bytes = %d", maxImageSSEPendingBytes, encodedBytes)
	}
}

func TestImageSSEPendingLimitUsesRequestSnapshot(t *testing.T) {
	responseBytes := 7 << 20
	want := base64.StdEncoding.EncodedLen(responseBytes) + imageSSEFrameOverhead
	if got := imageSSEPendingByteLimit(coreexecutor.ChatGPTWebImageConfigSnapshot{MaxImageResponseBytes: responseBytes}); got != want {
		t.Fatalf("SSE pending limit = %d, want %d", got, want)
	}
	if got := imageSSEPendingByteLimit(coreexecutor.ChatGPTWebImageConfigSnapshot{}); got != maxImageSSEPendingBytes {
		t.Fatalf("default SSE pending limit = %d, want %d", got, maxImageSSEPendingBytes)
	}
}

func TestImageStreamMapperErrorsOnIncompleteEOF(t *testing.T) {
	mapper := &imageStreamMapper{operation: imageGenerationOperation}
	var output bytes.Buffer
	mapper.writeChunk(&output, []byte(`data: {"type":"response.image_generation_call.partial_image","partial_image_b64":"cGFydA=="}`+"\n\n"))
	mapper.flush(&output)
	if strings.Contains(output.String(), "event: error") {
		t.Fatalf("mapper wrote terminal error directly: %q", output.String())
	}
	if err := mapper.fatalError(); err == nil || !strings.Contains(err.Error(), "without a completed response") {
		t.Fatalf("incomplete stream error = %v", err)
	}
}

func TestImageStreamMapperErrorsOnCompletedResponseWithoutImage(t *testing.T) {
	mapper := &imageStreamMapper{operation: imageGenerationOperation}
	var output bytes.Buffer
	mapper.writeChunk(&output, []byte(`data: {"type":"response.completed","response":{"output":[]}}`+"\n\n"))
	if output.Len() != 0 {
		t.Fatalf("mapper wrote terminal error directly: %q", output.String())
	}
	if err := mapper.fatalError(); err == nil || !strings.Contains(err.Error(), "upstream did not return image output") {
		t.Fatalf("missing image output error = %v", err)
	}
}

func TestImageStreamMapperClassifiesCompletedTextWithoutImage(t *testing.T) {
	for _, events := range [][]string{
		{`data: {"type":"response.completed","response":{"status":"completed","output":[{"type":"message","content":[{"type":"output_text","text":"Any response"}]}]}}` + "\n\n"},
		{
			`data: {"type":"response.output_text.delta","delta":"Any response"}` + "\n\n",
			`data: {"type":"response.completed","response":{"status":"completed","output":[]}}` + "\n\n",
		},
	} {
		mapper := &imageStreamMapper{operation: imageGenerationOperation}
		var output bytes.Buffer
		for _, event := range events {
			mapper.writeChunk(&output, []byte(event))
		}
		err := mapper.fatalError()
		var status interface{ StatusCode() int }
		if !errors.As(err, &status) || status.StatusCode() != http.StatusBadRequest || err.Error() != executorhelps.OpenAIImageModerationErrorBody {
			t.Fatalf("moderation error = %v", err)
		}
		if output.Len() != 0 {
			t.Fatalf("text-only response emitted image events: %q", output.String())
		}
	}
}

func TestImageStreamMapperErrorsOnSplitCompletedResponseWithoutImage(t *testing.T) {
	mapper := &imageStreamMapper{operation: imageGenerationOperation}
	var output bytes.Buffer
	mapper.writeChunk(&output, []byte(`data: {"type":"response.completed","response":{`))
	if mapper.fatalError() != nil {
		t.Fatalf("first chunk error = %v", mapper.fatalError())
	}
	mapper.writeChunk(&output, []byte(`"output":[]}}`))
	if err := mapper.fatalError(); err == nil || !strings.Contains(err.Error(), "upstream did not return image output") {
		t.Fatalf("split completed response error = %v", err)
	}
}

func TestOpenAIImagesStreamingEmitsTerminalErrorAfterPartialImageEOF(t *testing.T) {
	gin.SetMode(gin.TestMode)
	executor := &imageCaptureExecutor{
		streamChunks: []coreexecutor.StreamChunk{
			{Payload: []byte(`data: {"type":"response.image_generation_call.partial_image","partial_image_b64":"cGFydA=="}` + "\n\n")},
		},
	}
	h := newImagesTestHandler(t, executor)
	router := gin.New()
	router.POST("/v1/images/generations", h.Generations)

	req := httptest.NewRequest(http.MethodPost, "/v1/images/generations", strings.NewReader(`{"model":"gpt-image-2","prompt":"draw","stream":true}`))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	router.ServeHTTP(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body=%s", resp.Code, http.StatusOK, resp.Body.String())
	}
	body := resp.Body.String()
	if !strings.Contains(body, "event: image_generation.partial_image") {
		t.Fatalf("partial image event missing: %s", body)
	}
	if count := strings.Count(body, "event: error"); count != 1 {
		t.Fatalf("terminal error count = %d, want 1: %s", count, body)
	}
	if !strings.Contains(body, "without a completed response") {
		t.Fatalf("terminal error message missing: %s", body)
	}
}

func TestForwardImagesStreamFlushesSplitHeartbeat(t *testing.T) {
	cfg := &sdkconfig.SDKConfig{Streaming: sdkconfig.StreamingConfig{
		EnableStreamFlush:   true,
		StreamFlushMinBytes: 1 << 20,
	}}
	h := NewOpenAIImagesAPIHandler(handlers.NewBaseAPIHandlers(cfg, nil))
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	requestContext, cancelContext := context.WithCancel(context.Background())
	defer cancelContext()
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/images/generations", nil).WithContext(requestContext)

	data := make(chan []byte)
	errs := make(chan *interfaces.ErrorMessage)
	flusher := &responsesNotifyingFlusher{flushed: make(chan struct{}, 1)}
	done := make(chan struct{})
	go func() {
		defer close(done)
		h.forwardImagesStream(c, flusher, func(error) {}, data, errs, &imageStreamMapper{operation: imageGenerationOperation})
	}()

	data <- []byte(": chatgpt-web upstream pending\n")
	select {
	case <-flusher.flushed:
		t.Fatal("partial image heartbeat was flushed before its frame completed")
	case <-time.After(20 * time.Millisecond):
	}
	data <- []byte("\n")
	select {
	case <-flusher.flushed:
	case <-time.After(time.Second):
		t.Fatal("split image heartbeat was not flushed")
	}
	if got := recorder.Body.String(); got != ": chatgpt-web upstream pending\n\n" {
		t.Fatalf("heartbeat output = %q", got)
	}
	cancelContext()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("forwardImagesStream did not stop after cancellation")
	}
}

func intPointer(value int) *int {
	return &value
}

func TestImageStreamFlushFallsBackToStreamingConfig(t *testing.T) {
	cfg := &sdkconfig.SDKConfig{
		Streaming: sdkconfig.StreamingConfig{
			EnableStreamFlush:     true,
			StreamFlushIntervalMS: 25,
			StreamFlushMinBytes:   32768,
		},
	}

	interval := imageStreamFlushInterval(cfg)
	if interval == nil || *interval != 25*time.Millisecond {
		t.Fatalf("imageStreamFlushInterval = %v, want 25ms", interval)
	}
	if got := imageStreamFlushMinBytes(cfg); got != 32768 {
		t.Fatalf("imageStreamFlushMinBytes = %d, want 32768", got)
	}
}

func TestImageStreamFlushCanBeDisabled(t *testing.T) {
	disabled := false
	cfg := &sdkconfig.SDKConfig{
		Streaming: sdkconfig.StreamingConfig{
			EnableStreamFlush:     true,
			StreamFlushIntervalMS: 25,
			StreamFlushMinBytes:   32768,
		},
		Images: sdkconfig.ImagesConfig{
			EnableStreamFlush: &disabled,
		},
	}

	if interval := imageStreamFlushInterval(cfg); interval != nil {
		t.Fatalf("imageStreamFlushInterval = %v, want nil", interval)
	}
	if got := imageStreamFlushMinBytes(cfg); got != 0 {
		t.Fatalf("imageStreamFlushMinBytes = %d, want 0", got)
	}
}

func TestResponseStreamFlushRequiresEnableSwitch(t *testing.T) {
	cfg := &sdkconfig.SDKConfig{
		Streaming: sdkconfig.StreamingConfig{
			StreamFlushIntervalMS: 25,
			StreamFlushMinBytes:   32768,
		},
	}
	if interval := responseStreamFlushInterval(cfg); interval != nil {
		t.Fatalf("responseStreamFlushInterval = %v, want nil", interval)
	}
	if got := responseStreamFlushMinBytes(cfg); got != 0 {
		t.Fatalf("responseStreamFlushMinBytes = %d, want 0", got)
	}

	cfg.Streaming.EnableStreamFlush = true
	interval := responseStreamFlushInterval(cfg)
	if interval == nil || *interval != 25*time.Millisecond {
		t.Fatalf("responseStreamFlushInterval = %v, want 25ms", interval)
	}
	if got := responseStreamFlushMinBytes(cfg); got != 32768 {
		t.Fatalf("responseStreamFlushMinBytes = %d, want 32768", got)
	}
}
