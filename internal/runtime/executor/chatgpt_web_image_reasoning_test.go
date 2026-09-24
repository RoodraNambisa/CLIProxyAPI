package executor

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	"github.com/tidwall/gjson"
)

func TestChatGPTWebImageReasoningPinsRequestPolicy(t *testing.T) {
	auth := chatGPTWebRuntimeAuth()
	auth.ID = t.Name()
	r := registry.GetGlobalRegistry()
	r.RegisterClient(auth.ID, "chatgpt-web", []*registry.ModelInfo{{ID: "alias", UpstreamID: "catalog-instant", ChatGPTWebInstant: true}})
	t.Cleanup(func() { r.UnregisterClient(auth.ID) })
	cfg := &config.Config{}
	cfg.Images.ChatGPTWeb.ReasoningMode = "instant"
	e := NewChatGPTWebExecutor(cfg, nil)
	t.Cleanup(func() { _ = e.Close() })
	for _, stream := range []bool{false, true} {
		e.UpdateConfig(cfg)
		req := core.Request{Model: "gpt-image-2", Payload: []byte(`{"model":"gpt-image-2","input":"draw a cake","tools":[{"type":"image_generation"}]}`)}
		opts := core.Options{SourceFormat: translator.FormatCodex, ResponseFormat: translator.FormatCodex}
		template, err := e.prepareRuntimeRequestTemplate(t.Context(), req, opts, stream)
		if err != nil {
			t.Fatal(err)
		}
		opts = core.WithProviderPreparedRequest(opts, e.Identifier(), template)
		next := &config.Config{}
		next.Images.ChatGPTWeb.UpstreamModel = "custom-next"
		e.UpdateConfig(next)
		for _, alias := range []string{"gpt-image-2", "gpt-image-2.5"} {
			req.Model = alias
			p, errPrepare := e.prepareRuntimeRequest(t.Context(), auth, req, opts, stream)
			if errPrepare != nil {
				t.Fatal(errPrepare)
			}
			if p.imageUpstreamModel != "catalog-instant" || p.request.Image.Prompt != "draw a cake" {
				t.Fatalf("request changed across retry/reload: carrier=%q prompt=%q", p.imageUpstreamModel, p.request.Image.Prompt)
			}
			p.discardUsageProjection()
		}
		p, errPrepare := e.prepareRuntimeRequest(t.Context(), auth, req, core.Options{SourceFormat: translator.FormatCodex}, stream)
		if errPrepare != nil {
			t.Fatal(errPrepare)
		}
		if p.imageUpstreamModel != "custom-next" {
			t.Fatalf("new request carrier=%s", p.imageUpstreamModel)
		}
		p.discardUsageProjection()
	}
}

func TestChatGPTWebImageReasoningDefaultFallbackAndChatIsolation(t *testing.T) {
	for _, mode := range []string{"", "auto", "instant"} {
		t.Run(mode, func(t *testing.T) {
			cfg := &config.Config{}
			cfg.Images.ChatGPTWeb.ReasoningMode = mode
			e := NewChatGPTWebExecutor(cfg, nil)
			t.Cleanup(func() { _ = e.Close() })
			for _, image := range []bool{false, true} {
				body := `{"model":"gpt-5","input":"draw a cat"}`
				if image {
					body = `{"model":"gpt-5","input":"draw a cat","tools":[{"type":"image_generation"}],"tool_choice":{"type":"image_generation"}}`
				}
				p, err := e.prepareRuntimeRequest(t.Context(), nil, core.Request{Model: "gpt-5", Payload: []byte(body)}, core.Options{SourceFormat: translator.FormatCodex}, false)
				if err != nil {
					t.Fatal(err)
				}
				want := ""
				if image {
					want = "auto"
					if mode == "instant" {
						want = config.DefaultChatGPTWebImageInstantModel
					}
				}
				if p.imageUpstreamModel != want {
					t.Fatalf("image=%v carrier=%q want=%q", image, p.imageUpstreamModel, want)
				}
				p.discardUsageProjection()
			}
		})
	}
}

func TestChatGPTWebImageReasoningRejectsConflictingPinnedPolicy(t *testing.T) {
	e := NewChatGPTWebExecutor(nil, nil)
	t.Cleanup(func() { _ = e.Close() })
	_, err := e.prepareRuntimeRequest(t.Context(), nil,
		core.Request{Model: "gpt-image-2", Payload: []byte(`{"input":"draw","tools":[{"type":"image_generation"}]}`)},
		core.Options{SourceFormat: translator.FormatCodex, Metadata: map[string]any{
			core.ChatGPTWebImageConfigSnapshotMetadataKey: core.ChatGPTWebImageConfigSnapshot{ReasoningMode: "instant", UpstreamModel: "custom"},
		}}, false)
	if err == nil || !strings.Contains(err.Error(), "requires upstream-model auto") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestChatGPTWebImageReasoningExplicitLevelsAndUnsupportedCredential(t *testing.T) {
	auth := chatGPTWebRuntimeAuth()
	auth.ID = t.Name()
	r := registry.GetGlobalRegistry()
	r.RegisterClient(auth.ID, "chatgpt-web", []*registry.ModelInfo{{ID: "thinking-model", ChatGPTWebThinkingDefault: true, ChatGPTWebThinkingEfforts: []string{"min", "standard", "extended", "max"}}})
	t.Cleanup(func() { r.UnregisterClient(auth.ID) })
	for _, tc := range []struct{ mode, effort string }{{"low", "min"}, {"medium", "standard"}, {"high", "extended"}, {"xhigh", "max"}} {
		cfg := &config.Config{}
		cfg.Images.ChatGPTWeb.ReasoningMode = tc.mode
		e := NewChatGPTWebExecutor(cfg, nil)
		t.Cleanup(func() { _ = e.Close() })
		req := core.Request{Model: "gpt-image-2", Payload: []byte(`{"input":"draw","tools":[{"type":"image_generation"}]}`)}
		opts := core.Options{SourceFormat: translator.FormatCodex}
		p, err := e.prepareRuntimeRequest(t.Context(), auth, req, opts, true)
		if err != nil {
			t.Fatal(err)
		}
		if p.imageThinkingEffort != tc.effort || p.imageUpstreamModel != "thinking-model" {
			t.Fatal("explicit mode ignored")
		}
		p.discardUsageProjection()
		_, err = e.prepareRuntimeRequest(t.Context(), nil, req, opts, true)
		status, ok := err.(statusErr)
		if !ok || !status.skipAuthResult || !status.retryOtherAuth || !strings.Contains(status.msg, "chatgpt_web_image_reasoning_unsupported") {
			t.Fatalf("unsupported capability error = %v", err)
		}
	}
}

func TestChatGPTWebImageToolIntentMatchesPrepareAndSubmit(t *testing.T) {
	var mu sync.Mutex
	payloads := make(map[string][]byte)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		data, _ := io.ReadAll(r.Body)
		mu.Lock()
		payloads[r.URL.Path] = data
		mu.Unlock()
		if strings.HasSuffix(r.URL.Path, "/prepare") {
			_, _ = io.WriteString(w, `{"conduit_token":"test"}`)
		} else {
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = io.WriteString(w, "data: [DONE]\n\n")
		}
	}))
	defer server.Close()
	e := NewChatGPTWebExecutor(nil, nil)
	t.Cleanup(func() { _ = e.Close() })
	e.runtimeBaseURL = server.URL
	client, credential, err := e.newRuntimeClient(chatGPTWebRuntimeAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	for _, tc := range []struct {
		effort  string
		uploads []chatGPTWebUploadedImage
	}{
		{},
		{effort: "max"},
		{uploads: []chatGPTWebUploadedImage{{FileID: "fixture-file", Width: 16, Height: 16}}},
		{effort: "standard", uploads: []chatGPTWebUploadedImage{{FileID: "fixture-file", Width: 16, Height: 16}}},
	} {
		uploads := tc.uploads
		conduit, errPrepare := e.prepareChatGPTWebImageConversation(t.Context(), client, credential, chatGPTWebRequirements{}, "instant-carrier", tc.effort, "draw a cake")
		if errPrepare != nil {
			t.Fatal(errPrepare)
		}
		response, _, errOpen := e.openChatGPTWebImageConversation(t.Context(), client, credential, chatGPTWebRequirements{}, "instant-carrier", tc.effort, conduit, "draw a cake", uploads)
		if errOpen != nil {
			t.Fatal(errOpen)
		}
		_ = response.Body.Close()
		mu.Lock()
		prepare := gjson.ParseBytes(payloads["/backend-api/f/conversation/prepare"])
		submit := gjson.ParseBytes(payloads["/backend-api/f/conversation"])
		mu.Unlock()
		text, expectedMetadata := helps.ChatGPTWebImageToolInput("draw a cake")
		metadata, _ := json.Marshal(expectedMetadata)
		message := submit.Get("messages.0")
		parts := message.Get("content.parts").Array()
		if prepare.Get("model").String() != "instant-carrier" || submit.Get("model").String() != "instant-carrier" || submit.Get("thinking_effort").String() != tc.effort || prepare.Get("thinking_effort").String() != tc.effort || tc.effort == "" && submit.Get("thinking_effort").Exists() {
			t.Fatal("carrier or effort diverged")
		}
		if prepare.Get("partial_query.content.parts.0").String() != text || parts[len(parts)-1].String() != text {
			t.Fatal("tool prompt differs across stages")
		}
		for _, key := range []string{"system_hints", "serialization_metadata", "submission_mode"} {
			want := gjson.GetBytes(metadata, key).Raw
			if prepare.Get("partial_query.metadata."+key).Raw != want || message.Get("metadata."+key).Raw != want {
				t.Fatalf("tool metadata %s diverged", key)
			}
		}
		if len(uploads) > 0 && (message.Get("content.content_type").String() != "multimodal_text" || message.Get("metadata.attachments.0.id").String() != "fixture-file") {
			t.Fatal("edit attachment lost")
		}
	}
}
