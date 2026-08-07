package cliproxy

import (
	"slices"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

func TestApplyRuntimeConfigPreservesStartupFieldsAndDeduplicates(t *testing.T) {
	service := &Service{cfg: &config.Config{
		Host:         "127.0.0.1",
		Port:         8317,
		AuthDir:      "/runtime/auths",
		RequestRetry: 1,
		RemoteManagement: config.RemoteManagement{
			AccessPath: "runtime-path",
		},
	}}
	requested := &config.Config{
		Host:         "0.0.0.0",
		Port:         9000,
		AuthDir:      "/next/auths",
		RequestRetry: 4,
		RemoteManagement: config.RemoteManagement{
			AccessPath: "next-path",
		},
	}

	result, errApply := service.ApplyRuntimeConfig(t.Context(), requested)
	if errApply != nil {
		t.Fatalf("ApplyRuntimeConfig() error = %v", errApply)
	}
	if !result.Applied || !result.RestartRequired {
		t.Fatalf("ApplyRuntimeConfig() result = %#v", result)
	}
	if got := service.currentConfig(); got.Host != "127.0.0.1" || got.Port != 8317 || got.AuthDir != "/runtime/auths" || got.RequestRetry != 4 || got.RemoteManagement.AccessPath != "runtime-path" {
		t.Fatalf("runtime config = %#v", got)
	}
	if !slices.Contains(result.RestartFields, "remote-management.access-path") {
		t.Fatalf("restart fields = %v, want remote-management.access-path", result.RestartFields)
	}

	second, errSecond := service.ApplyRuntimeConfig(t.Context(), requested)
	if errSecond != nil {
		t.Fatalf("second ApplyRuntimeConfig() error = %v", errSecond)
	}
	if !second.Applied || !second.Deduplicated || !second.RestartRequired {
		t.Fatalf("second ApplyRuntimeConfig() result = %#v", second)
	}

	requested.RemoteManagement.AllowRemote = true
	third, errThird := service.ApplyRuntimeConfig(t.Context(), requested)
	if errThird != nil {
		t.Fatalf("third ApplyRuntimeConfig() error = %v", errThird)
	}
	if !third.Applied || third.Deduplicated {
		t.Fatalf("remote-management update was incorrectly deduplicated: %#v", third)
	}
	if !service.currentConfig().RemoteManagement.AllowRemote {
		t.Fatal("remote-management update was not applied")
	}
}
