package helps

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	"github.com/tidwall/gjson"
)

func TestXAICompressionNegotiatesExactBaseAndPreservesPlainRequest(t *testing.T) {
	var queries atomic.Int32
	var supports atomic.Bool
	supports.Store(true)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		queries.Add(1)
		if r.URL.Path != "/v1/settings" {
			t.Errorf("wrong settings path: %s", r.URL.Path)
		}
		if supports.Load() {
			_, _ = w.Write([]byte(`{"accept_request_encodings":["zstd"]}`))
		} else {
			_, _ = w.Write([]byte(`{}`))
		}
	}))
	defer server.Close()
	cfg := &config.Config{XAI: config.XAIConfig{RequestCompression: "auto"}}
	auth := &coreauth.Auth{ID: "compress-fixture", Attributes: map[string]string{"base_url": server.URL + "/v1"}}
	source := []byte(`{"model":"grok-4.7","input":"` + strings.Repeat("x", XAICompressionMinimum) + `"}`)
	makeRequest := func() *http.Request {
		r, _ := http.NewRequestWithContext(t.Context(), "POST", server.URL+"/v1/responses", bytes.NewReader(source))
		r.Header.Set("Content-Type", "application/json")
		return r
	}
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			req := makeRequest()
			got := PrepareXAICompressedRequest(t.Context(), server.Client(), req, auth, cfg)
			if got.Header.Get("Content-Encoding") != "zstd" {
				t.Error("missing zstd header")
				return
			}
			compressed, _ := io.ReadAll(got.Body)
			decoder, _ := zstd.NewReader(nil)
			decoded, err := decoder.DecodeAll(compressed, nil)
			decoder.Close()
			if err != nil || !bytes.Equal(decoded, source) {
				t.Error("invalid zstd body")
			}
			original, _ := io.ReadAll(req.Body)
			if !bytes.Equal(original, source) || req.Header.Get("Content-Encoding") != "" {
				t.Error("original request changed")
			}
		})
	}
	wg.Wait()
	if queries.Load() != 1 {
		t.Fatalf("settings queried %d times", queries.Load())
	}
	// A different exact base never inherits another server's advertised support.
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(`{}`)) }))
	defer other.Close()
	authOther := auth.Clone()
	authOther.Attributes["base_url"] = other.URL + "/v1"
	req, _ := http.NewRequestWithContext(t.Context(), "POST", other.URL+"/v1/responses", bytes.NewReader(source))
	req.Header.Set("Content-Type", "application/json")
	if got := PrepareXAICompressedRequest(t.Context(), other.Client(), req, authOther, cfg); got.Header.Get("Content-Encoding") != "" {
		t.Error("support leaked across origins")
	}
	for _, tc := range []struct {
		mode     string
		small    bool
		encoding string
	}{{"none", false, ""}, {"auto", true, ""}, {"auto", false, "gzip"}} {
		r := makeRequest()
		if tc.small {
			r.ContentLength = 1
		}
		r.Header.Set("Content-Encoding", tc.encoding)
		local := *cfg
		local.XAI = cfg.XAI.Clone()
		local.XAI.RequestCompression = tc.mode
		if PrepareXAICompressedRequest(t.Context(), server.Client(), r, auth, &local) != r {
			t.Error("ineligible request compressed")
		}
	}
}

func TestXAIClientSearchAliasAcrossHTTPAndWebsocket(t *testing.T) {
	original := []byte(`{"tools":[{"type":"function","name":"web_search","parameters":{"type":"object"}},{"type":"web_search"},{"type":"function","name":"client_web_search","parameters":{"type":"object"}}],"tool_choice":{"type":"function","name":"web_search"},"input":[{"type":"function_call","name":"web_search","arguments":"{}"}]}`)
	body, alias := AliasXAIClientWebSearch(original, original)
	if alias != "client_web_search_1" || gjson.GetBytes(body, "tools.0.name").String() != alias || gjson.GetBytes(body, "tool_choice.name").String() != alias || gjson.GetBytes(body, "input.0.name").String() != alias || gjson.GetBytes(body, "tools.1.type").String() != "web_search" {
		t.Fatalf("bad alias: %s", body)
	}
	policy := NewXAIResponsesOutputPolicy(original, body, sdktranslator.FormatOpenAIResponse, CodexMultiAgentResponsePolicy{}).WithClientWebSearchAlias(alias)
	event := []byte(`{"type":"response.output_item.done","item":{"type":"function_call","name":"` + alias + `","arguments":"{\"name\":\"client_web_search_1\"}"}}`)
	for _, got := range policy.RewriteWebsocket(event) {
		if gjson.GetBytes(got, "item.name").String() != "web_search" || !strings.Contains(gjson.GetBytes(got, "item.arguments").String(), alias) {
			t.Fatalf("bad restore: %s", got)
		}
	}
	chunks := policy.TranslateStream(t.Context(), sdktranslator.FormatCodex, sdktranslator.FormatOpenAIResponse, "grok-4.7", original, body, append(append([]byte("data: "), event...), []byte("\n\n")...), new(any))
	if len(chunks) == 0 || !bytes.Contains(bytes.Join(chunks, nil), []byte(`"name":"web_search"`)) {
		t.Fatalf("alias leaked through stream: %q", chunks)
	}
}

func TestXAIHeadersAndParametersUpgrade(t *testing.T) {
	cfg := &config.Config{XAI: config.XAIConfig{HeaderDefaults: config.XAIHeaderDefaults{OfficialBuild: true}, ResolvedClientVersion: "1.0.52"}}
	for _, endpoint := range []string{"https://cli-chat-proxy.grok.com/v1/responses", "https://api.x.ai/v1/responses", "https://relay.example/v1/responses"} {
		req, _ := http.NewRequest("POST", endpoint, nil)
		ApplyXAIResourceHeaders(req, nil, cfg)
		if strings.Contains(endpoint, "cli-chat") {
			if !strings.HasPrefix(req.Header.Get("User-Agent"), "grok-shell/1.0.52 (") || req.Header.Get("x-grok-client-mode") != "headless" || req.Header.Get("X-XAI-Token-Auth") != "xai-grok-cli" {
				t.Fatal(req.Header)
			}
		} else if req.Header.Get("x-grok-client-version") != "" || req.Header.Get("User-Agent") != "" {
			t.Fatal("CLI identity leaked to API")
		}
	}
	original := []byte(`{"service_tier":"priority","safety_identifier":"fixture","reasoning":{"summary":"detailed"},"text":{"format":{"type":"json_object"}}}`)
	result := ApplyXAIRequestParameters([]byte(`{}`), original, nil, "openai")
	if gjson.GetBytes(result, "service_tier").String() != "priority" || gjson.GetBytes(result, "safety_identifier").String() != "fixture" || gjson.GetBytes(result, "reasoning.summary").String() != "detailed" || gjson.GetBytes(result, "text.format.type").String() != "json_object" {
		t.Fatal(string(result))
	}
	plan, err := NewXAIRequestPlan(t.Context(), cfg, core.Request{}, core.Options{})
	if err != nil || plan.Config.XAI.ResolvedClientVersion != "1.0.52" {
		t.Fatal("version not frozen")
	}
}

func TestXAIClientSearchAliasPreservesNamespaces(t *testing.T) {
	original := []byte(`{"tools":[{"type":"function","name":"web_search"},{"type":"namespace","name":"search","tools":[{"type":"function","name":"web_search"}]}]}`)
	body, alias := AliasXAIClientWebSearch(original, original)
	if alias == "" || gjson.GetBytes(body, "tools.0.name").String() != alias || gjson.GetBytes(body, "tools.1.tools.0.name").String() != "web_search" {
		t.Fatal(string(body))
	}
}

func TestXAICompressionRedirectFallsBackToPlainJSON(t *testing.T) {
	source := []byte(`{"model":"grok-4.7","input":"` + strings.Repeat("x", XAICompressionMinimum) + `"}`)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		data, _ := io.ReadAll(r.Body)
		if r.Header.Get("Content-Encoding") != "" || !bytes.Equal(data, source) {
			t.Error("redirect inherited compressed body")
		}
		w.WriteHeader(200)
	}))
	defer target.Close()
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/settings" {
			_, _ = w.Write([]byte(`{"accept_request_encodings":["zstd"]}`))
			return
		}
		if r.Header.Get("Content-Encoding") != "zstd" {
			t.Error("initial request was not compressed")
		}
		http.Redirect(w, r, target.URL+"/responses", 307)
	}))
	defer origin.Close()
	cfg := &config.Config{XAI: config.XAIConfig{RequestCompression: "auto"}}
	auth := &coreauth.Auth{Attributes: map[string]string{"base_url": origin.URL + "/v1"}}
	req, _ := http.NewRequestWithContext(t.Context(), "POST", origin.URL+"/v1/responses", bytes.NewReader(source))
	req.Header.Set("Content-Type", "application/json")
	compressed := PrepareXAICompressedRequest(t.Context(), origin.Client(), req, auth, cfg)
	resp, err := XAICompressedHTTPClient(origin.Client(), req).Do(compressed)
	if err != nil {
		t.Fatal(err)
	}
	if err = resp.Body.Close(); err != nil {
		t.Fatal(err)
	}
}
