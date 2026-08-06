package config

import (
	"encoding/json"
	"testing"
)

func TestClonePreservesCompleteConfigAndIsolatesMutableValues(t *testing.T) {
	original := &Config{
		Host:    "127.0.0.1",
		Port:    8317,
		AuthDir: "/tmp/auths",
		RemoteManagement: RemoteManagement{
			AllowRemote: true,
			SecretKey:   "management-secret",
			AccessPath:  "hidden",
		},
		SDKConfig: SDKConfig{APIKeys: []string{"key-a"}},
		OAuthExcludedModels: map[string][]string{
			"codex": {"model-a"},
		},
		Payload: PayloadConfig{DefaultRaw: []PayloadRule{{
			Models: []PayloadModelRule{{Name: "gpt-*"}},
			Params: map[string]any{"raw": json.RawMessage(`{"enabled":true}`)},
		}}},
	}

	cloned, errClone := Clone(original)
	if errClone != nil {
		t.Fatalf("Clone() error = %v", errClone)
	}
	if cloned.Host != original.Host || cloned.Port != original.Port || cloned.AuthDir != original.AuthDir {
		t.Fatalf("startup fields were not preserved: %#v", cloned)
	}
	if cloned.RemoteManagement.SecretKey != original.RemoteManagement.SecretKey || cloned.RemoteManagement.AccessPath != original.RemoteManagement.AccessPath {
		t.Fatalf("management fields were not preserved: %#v", cloned.RemoteManagement)
	}

	cloned.APIKeys[0] = "key-b"
	cloned.OAuthExcludedModels["codex"][0] = "model-b"
	cloned.Payload.DefaultRaw[0].Models[0].Name = "claude-*"
	raw := cloned.Payload.DefaultRaw[0].Params["raw"].(json.RawMessage)
	raw[2] = 'X'

	if original.APIKeys[0] != "key-a" {
		t.Fatalf("API key slice shares storage with clone: %#v", original.APIKeys)
	}
	if original.OAuthExcludedModels["codex"][0] != "model-a" {
		t.Fatalf("model exclusion map shares storage with clone: %#v", original.OAuthExcludedModels)
	}
	if original.Payload.DefaultRaw[0].Models[0].Name != "gpt-*" {
		t.Fatalf("payload model slice shares storage with clone: %#v", original.Payload.DefaultRaw)
	}
	if string(original.Payload.DefaultRaw[0].Params["raw"].(json.RawMessage)) != `{"enabled":true}` {
		t.Fatalf("raw payload shares storage with clone: %s", original.Payload.DefaultRaw[0].Params["raw"])
	}
}
