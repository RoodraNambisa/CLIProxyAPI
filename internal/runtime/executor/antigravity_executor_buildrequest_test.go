package executor

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

type antigravityProjectRoundTripperFunc func(*http.Request) (*http.Response, error)

func (f antigravityProjectRoundTripperFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func TestAntigravityBuildRequest_SanitizesGeminiToolSchema(t *testing.T) {
	body := buildRequestBodyFromPayload(t, "gemini-2.5-pro")

	decl := extractFirstFunctionDeclaration(t, body)
	if _, ok := decl["parametersJsonSchema"]; ok {
		t.Fatalf("parametersJsonSchema should be renamed to parameters")
	}

	params, ok := decl["parameters"].(map[string]any)
	if !ok {
		t.Fatalf("parameters missing or invalid type")
	}
	assertSchemaSanitizedAndPropertyPreserved(t, params)
}

func TestAntigravityBuildRequest_SanitizesAntigravityToolSchema(t *testing.T) {
	body := buildRequestBodyFromPayload(t, "claude-opus-4-6")

	decl := extractFirstFunctionDeclaration(t, body)
	params, ok := decl["parameters"].(map[string]any)
	if !ok {
		t.Fatalf("parameters missing or invalid type")
	}
	assertSchemaSanitizedAndPropertyPreserved(t, params)
}

func TestAntigravityBuildRequest_SkipsSchemaSanitizationWithoutToolsField(t *testing.T) {
	body := buildRequestBodyFromRawPayload(t, "gemini-3.1-flash-image", []byte(`{
		"request": {
			"contents": [
				{
					"role": "user",
					"x-debug": "keep-me",
					"parts": [
						{
							"text": "hello"
						}
					]
				}
			],
			"nonSchema": {
				"nullable": true,
				"x-extra": "keep-me"
			},
			"generationConfig": {
				"maxOutputTokens": 128
			}
		}
	}`))

	assertNonSchemaRequestPreserved(t, body)
}

func TestAntigravityBuildRequest_SkipsSchemaSanitizationWithEmptyToolsArray(t *testing.T) {
	body := buildRequestBodyFromRawPayload(t, "gemini-3.1-flash-image", []byte(`{
		"request": {
			"tools": [],
			"contents": [
				{
					"role": "user",
					"x-debug": "keep-me",
					"parts": [
						{
							"text": "hello"
						}
					]
				}
			],
			"nonSchema": {
				"nullable": true,
				"x-extra": "keep-me"
			},
			"generationConfig": {
				"maxOutputTokens": 128
			}
		}
	}`))

	assertNonSchemaRequestPreserved(t, body)
}

func TestAntigravityPrepareRequestAuthFetchesMissingProjectID(t *testing.T) {
	executor := &AntigravityExecutor{}
	auth := &cliproxyauth.Auth{Metadata: map[string]any{
		"access_token": "token",
		"expired":      time.Now().Add(time.Hour).Format(time.RFC3339),
	}}
	ctx := context.WithValue(t.Context(), "cliproxy.roundtripper", antigravityProjectRoundTripperFunc(func(req *http.Request) (*http.Response, error) {
		if got := req.URL.String(); got != "https://cloudcode-pa.googleapis.com/v1internal:loadCodeAssist" {
			t.Fatalf("project discovery URL = %q", got)
		}
		if got := req.Header.Get("X-Goog-Api-Client"); got != "" {
			t.Fatalf("X-Goog-Api-Client = %q, want empty", got)
		}
		if got := req.Header.Get("Authorization"); got != "Bearer token" {
			t.Fatalf("Authorization = %q, want Bearer token", got)
		}
		raw, errRead := io.ReadAll(req.Body)
		if errRead != nil {
			t.Fatalf("read discovery body: %v", errRead)
		}
		if !strings.Contains(string(raw), `"ideType":"ANTIGRAVITY"`) {
			t.Fatalf("unexpected discovery body: %s", string(raw))
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader(`{"cloudaicompanionProject":"fetched-project"}`)),
		}, nil
	}))

	updated, errPrepare := executor.PrepareRequestAuth(ctx, auth)
	if errPrepare != nil {
		t.Fatalf("PrepareRequestAuth() error: %v", errPrepare)
	}
	if updated == nil {
		t.Fatal("PrepareRequestAuth() returned nil auth")
	}
	if _, ok := auth.Metadata["project_id"]; ok {
		t.Fatal("PrepareRequestAuth() mutated the original auth")
	}
	if got := updated.Metadata["project_id"]; got != "fetched-project" {
		t.Fatalf("prepared project_id = %v, want fetched-project", got)
	}
}

func TestAntigravityPrepareRequestAuthUsesRefreshedTokenForProjectDiscovery(t *testing.T) {
	executor := &AntigravityExecutor{}
	auth := &cliproxyauth.Auth{Metadata: map[string]any{
		"access_token":  "expired-token",
		"refresh_token": "refresh-token",
		"expired":       time.Now().Add(-time.Hour).Format(time.RFC3339),
	}}
	var tokenCalls, projectCalls int
	ctx := context.WithValue(t.Context(), "cliproxy.roundtripper", antigravityProjectRoundTripperFunc(func(req *http.Request) (*http.Response, error) {
		switch req.URL.String() {
		case "https://oauth2.googleapis.com/token":
			tokenCalls++
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     make(http.Header),
				Body:       io.NopCloser(strings.NewReader(`{"access_token":"refreshed-token","expires_in":3600,"token_type":"Bearer"}`)),
			}, nil
		case "https://cloudcode-pa.googleapis.com/v1internal:loadCodeAssist":
			projectCalls++
			if got := req.Header.Get("Authorization"); got != "Bearer refreshed-token" {
				t.Fatalf("project discovery Authorization = %q", got)
			}
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     make(http.Header),
				Body:       io.NopCloser(strings.NewReader(`{"cloudaicompanionProject":"refreshed-project"}`)),
			}, nil
		default:
			t.Fatalf("unexpected request URL: %s", req.URL.String())
			return nil, nil
		}
	}))

	updated, errPrepare := executor.PrepareRequestAuth(ctx, auth)
	if errPrepare != nil {
		t.Fatalf("PrepareRequestAuth() error: %v", errPrepare)
	}
	if tokenCalls != 1 || projectCalls != 1 {
		t.Fatalf("token/project calls = %d/%d, want 1/1", tokenCalls, projectCalls)
	}
	if got := updated.Metadata["access_token"]; got != "refreshed-token" {
		t.Fatalf("updated access_token = %v", got)
	}
	if got := updated.Metadata["project_id"]; got != "refreshed-project" {
		t.Fatalf("updated project_id = %v", got)
	}
	if got := auth.Metadata["access_token"]; got != "expired-token" {
		t.Fatalf("original access_token = %v, want expired-token", got)
	}
	if _, ok := auth.Metadata["project_id"]; ok {
		t.Fatal("PrepareRequestAuth() mutated original project_id")
	}
}

func TestAntigravityPrepareRequestAuthPreservesDiscoveryStatusAndRetryAfter(t *testing.T) {
	executor := &AntigravityExecutor{}
	auth := &cliproxyauth.Auth{Metadata: map[string]any{
		"access_token": "token",
		"expired":      time.Now().Add(time.Hour).Format(time.RFC3339),
	}}
	ctx := context.WithValue(t.Context(), "cliproxy.roundtripper", antigravityProjectRoundTripperFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusTooManyRequests,
			Header:     http.Header{"Retry-After": {"7"}},
			Body:       io.NopCloser(strings.NewReader(`{"error":"rate limited"}`)),
		}, nil
	}))

	_, errPrepare := executor.PrepareRequestAuth(ctx, auth)
	var statusErr interface{ StatusCode() int }
	if !errors.As(errPrepare, &statusErr) || statusErr.StatusCode() != http.StatusTooManyRequests {
		t.Fatalf("PrepareRequestAuth() error = %v, want 429", errPrepare)
	}
	var retryErr interface{ RetryAfter() *time.Duration }
	if !errors.As(errPrepare, &retryErr) || retryErr.RetryAfter() == nil || *retryErr.RetryAfter() != 7*time.Second {
		t.Fatalf("PrepareRequestAuth() RetryAfter = %v, want 7s", errPrepare)
	}
}

func TestAntigravityBuildRequestUsesAuthProjectID(t *testing.T) {
	body := buildRequestBodyFromRawPayload(t, "gemini-3.1-pro", []byte(`{"project":"payload-project","request":{}}`))
	if got := body["project"]; got != "project-1" {
		t.Fatalf("request project = %v, want project-1", got)
	}
}

func TestAntigravityBuildRequestRejectsMissingProjectID(t *testing.T) {
	executor := &AntigravityExecutor{}
	auth := &cliproxyauth.Auth{Metadata: map[string]any{}}

	_, errBuild := executor.buildRequest(t.Context(), auth, "token", "gemini-3.1-pro", []byte(`{"request":{}}`), false, "", "https://example.com")
	if errBuild == nil {
		t.Fatal("buildRequest() should reject missing project_id")
	}
	status, ok := errBuild.(interface{ StatusCode() int })
	if !ok {
		t.Fatalf("buildRequest() error type = %T, want status error", errBuild)
	}
	if got := status.StatusCode(); got != http.StatusBadRequest {
		t.Fatalf("buildRequest() status = %d, want %d", got, http.StatusBadRequest)
	}
}

func assertNonSchemaRequestPreserved(t *testing.T, body map[string]any) {
	t.Helper()

	request, ok := body["request"].(map[string]any)
	if !ok {
		t.Fatalf("request missing or invalid type")
	}

	contents, ok := request["contents"].([]any)
	if !ok || len(contents) == 0 {
		t.Fatalf("contents missing or empty")
	}
	content, ok := contents[0].(map[string]any)
	if !ok {
		t.Fatalf("content missing or invalid type")
	}
	if got, ok := content["x-debug"].(string); !ok || got != "keep-me" {
		t.Fatalf("x-debug should be preserved when no tool schema exists, got=%v", content["x-debug"])
	}

	nonSchema, ok := request["nonSchema"].(map[string]any)
	if !ok {
		t.Fatalf("nonSchema missing or invalid type")
	}
	if _, ok := nonSchema["nullable"]; !ok {
		t.Fatalf("nullable should be preserved outside schema cleanup path")
	}
	if got, ok := nonSchema["x-extra"].(string); !ok || got != "keep-me" {
		t.Fatalf("x-extra should be preserved outside schema cleanup path, got=%v", nonSchema["x-extra"])
	}

	if generationConfig, ok := request["generationConfig"].(map[string]any); ok {
		if _, ok := generationConfig["maxOutputTokens"]; ok {
			t.Fatalf("maxOutputTokens should still be removed for non-Claude requests")
		}
	}
}

func buildRequestBodyFromPayload(t *testing.T, modelName string) map[string]any {
	t.Helper()
	return buildRequestBodyFromRawPayload(t, modelName, []byte(`{
		"request": {
			"tools": [
				{
					"function_declarations": [
						{
							"name": "tool_1",
							"parametersJsonSchema": {
								"$schema": "http://json-schema.org/draft-07/schema#",
								"$id": "root-schema",
								"type": "object",
								"properties": {
									"$id": {"type": "string"},
									"arg": {
										"type": "object",
										"prefill": "hello",
										"properties": {
											"mode": {
												"type": "string",
												"deprecated": true,
												"enum": ["a", "b"],
												"enumTitles": ["A", "B"]
											}
										}
									}
								},
								"patternProperties": {
									"^x-": {"type": "string"}
								}
							}
						}
					]
				}
			]
		}
	}`))
}

func buildRequestBodyFromRawPayload(t *testing.T, modelName string, payload []byte) map[string]any {
	t.Helper()

	executor := &AntigravityExecutor{}
	auth := &cliproxyauth.Auth{Metadata: map[string]any{"project_id": "project-1"}}

	req, err := executor.buildRequest(context.Background(), auth, "token", modelName, payload, false, "", "https://example.com")
	if err != nil {
		t.Fatalf("buildRequest error: %v", err)
	}

	raw, err := io.ReadAll(req.Body)
	if err != nil {
		t.Fatalf("read request body error: %v", err)
	}

	var body map[string]any
	if err := json.Unmarshal(raw, &body); err != nil {
		t.Fatalf("unmarshal request body error: %v, body=%s", err, string(raw))
	}
	return body
}

func extractFirstFunctionDeclaration(t *testing.T, body map[string]any) map[string]any {
	t.Helper()

	request, ok := body["request"].(map[string]any)
	if !ok {
		t.Fatalf("request missing or invalid type")
	}
	tools, ok := request["tools"].([]any)
	if !ok || len(tools) == 0 {
		t.Fatalf("tools missing or empty")
	}
	tool, ok := tools[0].(map[string]any)
	if !ok {
		t.Fatalf("first tool invalid type")
	}
	decls, ok := tool["function_declarations"].([]any)
	if !ok || len(decls) == 0 {
		t.Fatalf("function_declarations missing or empty")
	}
	decl, ok := decls[0].(map[string]any)
	if !ok {
		t.Fatalf("first function declaration invalid type")
	}
	return decl
}

func assertSchemaSanitizedAndPropertyPreserved(t *testing.T, params map[string]any) {
	t.Helper()

	if _, ok := params["$id"]; ok {
		t.Fatalf("root $id should be removed from schema")
	}
	if _, ok := params["patternProperties"]; ok {
		t.Fatalf("patternProperties should be removed from schema")
	}

	props, ok := params["properties"].(map[string]any)
	if !ok {
		t.Fatalf("properties missing or invalid type")
	}
	if _, ok := props["$id"]; !ok {
		t.Fatalf("property named $id should be preserved")
	}

	arg, ok := props["arg"].(map[string]any)
	if !ok {
		t.Fatalf("arg property missing or invalid type")
	}
	if _, ok := arg["prefill"]; ok {
		t.Fatalf("prefill should be removed from nested schema")
	}

	argProps, ok := arg["properties"].(map[string]any)
	if !ok {
		t.Fatalf("arg.properties missing or invalid type")
	}
	mode, ok := argProps["mode"].(map[string]any)
	if !ok {
		t.Fatalf("mode property missing or invalid type")
	}
	if _, ok := mode["enumTitles"]; ok {
		t.Fatalf("enumTitles should be removed from nested schema")
	}
	if _, ok := mode["deprecated"]; ok {
		t.Fatalf("deprecated should be removed from nested schema")
	}
}
