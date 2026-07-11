package openai

import (
	"bytes"
	"context"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/api/handlers"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	coreexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
	"github.com/tidwall/gjson"
)

type xaiMediaCaptureExecutor struct {
	mu       sync.Mutex
	provider string
	authIDs  []string
	models   []string
	formats  []string
	paths    []string
	payloads [][]byte
}

func (e *xaiMediaCaptureExecutor) Identifier() string {
	if strings.TrimSpace(e.provider) != "" {
		return e.provider
	}
	return xaiProvider
}

func (e *xaiMediaCaptureExecutor) Execute(_ context.Context, auth *coreauth.Auth, req coreexecutor.Request, opts coreexecutor.Options) (coreexecutor.Response, error) {
	authID := ""
	if auth != nil {
		authID = auth.ID
	}
	path, _ := opts.Metadata[coreexecutor.RequestPathMetadataKey].(string)
	e.mu.Lock()
	e.authIDs = append(e.authIDs, authID)
	e.models = append(e.models, req.Model)
	e.formats = append(e.formats, opts.SourceFormat.String())
	e.paths = append(e.paths, path)
	e.payloads = append(e.payloads, bytes.Clone(req.Payload))
	e.mu.Unlock()

	if opts.SourceFormat.String() == xaiImagesHandlerType {
		return coreexecutor.Response{Payload: []byte(`{"created":1,"data":[{"b64_json":"aW1hZ2U="}]}`)}, nil
	}
	requestID := strings.TrimSpace(gjson.GetBytes(req.Payload, "request_id").String())
	if requestID == "" {
		requestID = "video_xai_bound"
	}
	return coreexecutor.Response{Payload: []byte(`{"request_id":"` + requestID + `","status":"completed","progress":100,"video":{"url":"https://example.test/video.mp4","duration":4}}`)}, nil
}

func (e *xaiMediaCaptureExecutor) ExecuteStream(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (*coreexecutor.StreamResult, error) {
	return nil, &coreauth.Error{Code: "not_implemented", Message: "stream not implemented"}
}

func (e *xaiMediaCaptureExecutor) Refresh(_ context.Context, auth *coreauth.Auth) (*coreauth.Auth, error) {
	return auth, nil
}

func (e *xaiMediaCaptureExecutor) CountTokens(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (coreexecutor.Response, error) {
	return coreexecutor.Response{}, nil
}

func (e *xaiMediaCaptureExecutor) HttpRequest(context.Context, *coreauth.Auth, *http.Request) (*http.Response, error) {
	return nil, nil
}

func newXAIMediaTestHandlers(t *testing.T, capture *xaiMediaCaptureExecutor, models ...string) (*OpenAIImagesAPIHandler, *OpenAIAPIHandler) {
	t.Helper()
	manager := coreauth.NewManager(nil, &coreauth.RoundRobinSelector{}, nil)
	manager.RegisterExecutor(capture)
	for index := 0; index < 2; index++ {
		authID := "xai-media-auth-" + string(rune('a'+index))
		auth := &coreauth.Auth{ID: authID, Provider: "xai", Status: coreauth.StatusActive}
		if _, err := manager.Register(context.Background(), auth); err != nil {
			t.Fatalf("Register(%s) error = %v", authID, err)
		}
		infos := make([]*registry.ModelInfo, 0, len(models))
		for _, model := range models {
			infos = append(infos, &registry.ModelInfo{ID: model})
		}
		registry.GetGlobalRegistry().RegisterClient(authID, "xai", infos)
		manager.RefreshSchedulerEntry(authID)
		t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(authID) })
	}
	base := handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, manager)
	return NewOpenAIImagesAPIHandler(base), NewOpenAIAPIHandler(base)
}

func TestXAIImagesGenerationRoutesThroughExistingImagesHandler(t *testing.T) {
	gin.SetMode(gin.TestMode)
	capture := &xaiMediaCaptureExecutor{}
	images, _ := newXAIMediaTestHandlers(t, capture, defaultXAIImagesModel)
	router := gin.New()
	router.POST("/v1/images/generations", images.Generations)
	req := httptest.NewRequest(http.MethodPost, "/v1/images/generations", strings.NewReader(`{"model":"grok-imagine-image","prompt":"draw","response_format":"b64_json"}`))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	router.ServeHTTP(resp, req)

	if resp.Code != http.StatusOK || gjson.GetBytes(resp.Body.Bytes(), "data.0.b64_json").String() != "aW1hZ2U=" {
		t.Fatalf("response status=%d body=%s", resp.Code, resp.Body.String())
	}
	capture.mu.Lock()
	defer capture.mu.Unlock()
	if len(capture.models) != 1 || capture.models[0] != defaultXAIImagesModel {
		t.Fatalf("models = %v", capture.models)
	}
	if capture.formats[0] != xaiImagesHandlerType || capture.paths[0] != "/v1/images/generations" {
		t.Fatalf("format=%q path=%q", capture.formats[0], capture.paths[0])
	}
}

func TestXAIMediaPinsExecutionToXAIProvider(t *testing.T) {
	gin.SetMode(gin.TestMode)
	xaiCapture := &xaiMediaCaptureExecutor{provider: xaiProvider}
	otherCapture := &xaiMediaCaptureExecutor{provider: "codex"}
	manager := coreauth.NewManager(nil, &coreauth.RoundRobinSelector{}, nil)
	manager.RegisterExecutor(xaiCapture)
	manager.RegisterExecutor(otherCapture)
	for _, provider := range []string{xaiProvider, "codex"} {
		authID := provider + "-overlap-media-auth"
		auth := &coreauth.Auth{ID: authID, Provider: provider, Status: coreauth.StatusActive}
		if _, err := manager.Register(context.Background(), auth); err != nil {
			t.Fatalf("Register(%s) error = %v", authID, err)
		}
		registry.GetGlobalRegistry().RegisterClient(authID, provider, []*registry.ModelInfo{{ID: defaultXAIImagesModel}})
		manager.RefreshSchedulerEntry(authID)
		t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(authID) })
	}

	base := handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, manager)
	images := NewOpenAIImagesAPIHandler(base)
	router := gin.New()
	router.POST("/v1/images/generations", images.Generations)
	for range 2 {
		req := httptest.NewRequest(http.MethodPost, "/v1/images/generations", strings.NewReader(`{"model":"grok-imagine-image","prompt":"draw"}`))
		req.Header.Set("Content-Type", "application/json")
		resp := httptest.NewRecorder()
		router.ServeHTTP(resp, req)
		if resp.Code != http.StatusOK {
			t.Fatalf("response status=%d body=%s", resp.Code, resp.Body.String())
		}
	}

	xaiCapture.mu.Lock()
	xaiCalls := len(xaiCapture.authIDs)
	xaiCapture.mu.Unlock()
	otherCapture.mu.Lock()
	otherCalls := len(otherCapture.authIDs)
	otherCapture.mu.Unlock()
	if xaiCalls != 2 || otherCalls != 0 {
		t.Fatalf("provider calls xai=%d codex=%d", xaiCalls, otherCalls)
	}
}

func TestXAIMediaCustomPrefixRoutesAndCanonicalizes(t *testing.T) {
	gin.SetMode(gin.TestMode)
	routeModel := "team-a/" + defaultXAIImagesModel
	capture := &xaiMediaCaptureExecutor{}
	images, _ := newXAIMediaTestHandlers(t, capture, routeModel)
	router := gin.New()
	router.POST("/v1/images/generations", images.Generations)
	req := httptest.NewRequest(http.MethodPost, "/v1/images/generations", strings.NewReader(`{"model":"`+routeModel+`","prompt":"draw"}`))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	router.ServeHTTP(resp, req)
	if resp.Code != http.StatusOK {
		t.Fatalf("response status=%d body=%s", resp.Code, resp.Body.String())
	}

	capture.mu.Lock()
	defer capture.mu.Unlock()
	if len(capture.models) != 1 || capture.models[0] != routeModel {
		t.Fatalf("route models = %v", capture.models)
	}
	if got := gjson.GetBytes(capture.payloads[0], "model").String(); got != defaultXAIImagesModel {
		t.Fatalf("upstream model = %q; payload=%s", got, capture.payloads[0])
	}

	videoRouteModel := "team-a/" + defaultXAIVideosModel
	if !isXAIVideosModel(videoRouteModel) {
		t.Fatalf("custom video route model %q was rejected", videoRouteModel)
	}
	videoPayload, meta, err := buildXAIVideosCreateRequest([]byte(`{"prompt":"animate"}`), videoRouteModel)
	if err != nil {
		t.Fatalf("buildXAIVideosCreateRequest() error = %v", err)
	}
	if meta.UpstreamModel != videoRouteModel || gjson.GetBytes(videoPayload, "model").String() != defaultXAIVideosModel {
		t.Fatalf("video route=%q payload=%s", meta.UpstreamModel, videoPayload)
	}
}

func TestImagesMaxBytesErrorReturns413(t *testing.T) {
	gin.SetMode(gin.TestMode)
	images, _ := newXAIMediaTestHandlers(t, &xaiMediaCaptureExecutor{}, defaultXAIImagesModel)
	response := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(response)
	images.writeImagesRequestError(c, &http.MaxBytesError{Limit: 1})
	if response.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want %d; body=%s", response.Code, http.StatusRequestEntityTooLarge, response.Body.String())
	}
}

func TestXAIImagesMultipartEditPreservesNativeOptions(t *testing.T) {
	gin.SetMode(gin.TestMode)
	capture := &xaiMediaCaptureExecutor{}
	images, _ := newXAIMediaTestHandlers(t, capture, defaultXAIImagesModel)
	router := gin.New()
	router.POST("/v1/images/edits", images.Edits)

	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	for name, value := range map[string]string{
		"model":           defaultXAIImagesModel,
		"prompt":          "edit",
		"aspect_ratio":    "16:9",
		"resolution":      "2k",
		"response_format": "b64_json",
	} {
		if err := writer.WriteField(name, value); err != nil {
			t.Fatalf("WriteField(%s) error = %v", name, err)
		}
	}
	imagePart, err := writer.CreateFormFile("image", "input.png")
	if err != nil {
		t.Fatalf("CreateFormFile() error = %v", err)
	}
	if _, err = imagePart.Write([]byte("image")); err != nil {
		t.Fatalf("image write error = %v", err)
	}
	if err = writer.Close(); err != nil {
		t.Fatalf("multipart close error = %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/v1/images/edits", &body)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	resp := httptest.NewRecorder()
	router.ServeHTTP(resp, req)
	if resp.Code != http.StatusOK {
		t.Fatalf("response status=%d body=%s", resp.Code, resp.Body.String())
	}

	capture.mu.Lock()
	defer capture.mu.Unlock()
	if len(capture.payloads) != 1 {
		t.Fatalf("payload count = %d, want 1", len(capture.payloads))
	}
	payload := capture.payloads[0]
	if got := gjson.GetBytes(payload, "aspect_ratio").String(); got != "16:9" {
		t.Fatalf("aspect_ratio = %q; payload=%s", got, payload)
	}
	if got := gjson.GetBytes(payload, "resolution").String(); got != "2k" {
		t.Fatalf("resolution = %q; payload=%s", got, payload)
	}
	if gjson.GetBytes(payload, "n").Exists() {
		t.Fatalf("implicit n was sent; payload=%s", payload)
	}
}

func TestXAIImagesMultipartEditRequiresPrompt(t *testing.T) {
	gin.SetMode(gin.TestMode)
	capture := &xaiMediaCaptureExecutor{}
	images, _ := newXAIMediaTestHandlers(t, capture, defaultXAIImagesModel)
	router := gin.New()
	router.POST("/v1/images/edits", images.Edits)

	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	if err := writer.WriteField("model", defaultXAIImagesModel); err != nil {
		t.Fatalf("WriteField() error = %v", err)
	}
	imagePart, err := writer.CreateFormFile("image", "input.png")
	if err != nil {
		t.Fatalf("CreateFormFile() error = %v", err)
	}
	if _, err = imagePart.Write([]byte("image")); err != nil {
		t.Fatalf("image write error = %v", err)
	}
	if err = writer.Close(); err != nil {
		t.Fatalf("multipart close error = %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/v1/images/edits", &body)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	resp := httptest.NewRecorder()
	router.ServeHTTP(resp, req)
	if resp.Code != http.StatusBadRequest || !strings.Contains(resp.Body.String(), "prompt is required") {
		t.Fatalf("response status=%d body=%s", resp.Code, resp.Body.String())
	}
}

func TestXAIVideoCreateAndRetrieveStayBoundToSelectedAuth(t *testing.T) {
	gin.SetMode(gin.TestMode)
	previousBindings := videoAuthBindings
	videoAuthBindings = newVideoAuthBindingStore()
	t.Cleanup(func() { videoAuthBindings = previousBindings })

	routeModel := "team-a/" + defaultXAIVideosModel
	capture := &xaiMediaCaptureExecutor{}
	_, openAI := newXAIMediaTestHandlers(t, capture, routeModel)
	router := gin.New()
	router.POST("/v1/videos/generations", openAI.XAIVideosGenerations)
	router.GET("/v1/videos/:request_id", openAI.XAIVideosRetrieve)

	createReq := httptest.NewRequest(http.MethodPost, "/v1/videos/generations", strings.NewReader(`{"model":"`+routeModel+`","prompt":"animate"}`))
	createReq.Header.Set("Content-Type", "application/json")
	createResp := httptest.NewRecorder()
	router.ServeHTTP(createResp, createReq)
	if createResp.Code != http.StatusOK {
		t.Fatalf("create status=%d body=%s", createResp.Code, createResp.Body.String())
	}
	requestID := gjson.GetBytes(createResp.Body.Bytes(), "request_id").String()
	if requestID != "video_xai_bound" {
		t.Fatalf("request_id = %q", requestID)
	}

	retrieveReq := httptest.NewRequest(http.MethodGet, "/v1/videos/"+requestID, nil)
	retrieveResp := httptest.NewRecorder()
	router.ServeHTTP(retrieveResp, retrieveReq)
	if retrieveResp.Code != http.StatusOK {
		t.Fatalf("retrieve status=%d body=%s", retrieveResp.Code, retrieveResp.Body.String())
	}

	capture.mu.Lock()
	defer capture.mu.Unlock()
	if len(capture.authIDs) != 2 || capture.authIDs[0] == "" || capture.authIDs[1] != capture.authIDs[0] {
		t.Fatalf("auth IDs = %v", capture.authIDs)
	}
	if capture.models[0] != routeModel || capture.models[1] != routeModel {
		t.Fatalf("models = %v", capture.models)
	}
	if capture.paths[0] != "/v1/videos/generations" || capture.paths[1] != "/v1/videos/:request_id" {
		t.Fatalf("paths = %v", capture.paths)
	}
}
