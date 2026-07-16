package cliproxy

import (
	"context"
	"path/filepath"
	"testing"

	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
)

func TestBuilderInstallsProxyPoolResolver(t *testing.T) {
	t.Parallel()

	cfg := &config.Config{}
	cfg.ProxyURL = "http://global.example:8080"
	service, errBuild := NewBuilder().
		WithConfig(cfg).
		WithConfigPath(filepath.Join(t.TempDir(), "config.yaml")).
		Build()
	if errBuild != nil {
		t.Fatalf("Build() error = %v", errBuild)
	}
	if service.proxyPoolManager == nil {
		t.Fatal("Build() did not create proxy pool manager")
	}

	auth := &coreauth.Auth{ID: "auth-a", Provider: "codex"}
	resolved, errResolve := service.coreManager.ResolveProxyAuth(context.Background(), auth)
	if errResolve != nil {
		t.Fatalf("ResolveProxyAuth() error = %v", errResolve)
	}
	if resolved == auth {
		t.Fatal("ResolveProxyAuth() returned original auth; resolver was not installed")
	}
	if resolved.RuntimeProxyURL != cfg.ProxyURL || resolved.ProxyURL != "" || auth.RuntimeProxyURL != "" {
		t.Fatalf("resolved auth = %+v; original = %+v", resolved, auth)
	}
}

func TestBuilderProxyResolverPreservesExplicitCredentialProxy(t *testing.T) {
	t.Parallel()

	cfg := &config.Config{}
	cfg.ProxyURL = "http://global.example:8080"
	service, errBuild := NewBuilder().
		WithConfig(cfg).
		WithConfigPath(filepath.Join(t.TempDir(), "config.yaml")).
		Build()
	if errBuild != nil {
		t.Fatalf("Build() error = %v", errBuild)
	}
	auth := &coreauth.Auth{ID: "auth-a", Provider: "codex", ProxyURL: "socks5h://credential.example:1080"}
	resolved, errResolve := service.coreManager.ResolveProxyAuth(context.Background(), auth)
	if errResolve != nil {
		t.Fatalf("ResolveProxyAuth() error = %v", errResolve)
	}
	if resolved.EffectiveProxyURL() != auth.ProxyURL || resolved.RuntimeProxyURL != "" || resolved.RuntimeProxyBindingID != "" {
		t.Fatalf("resolved explicit proxy auth = %+v", resolved)
	}
}

func TestBeforeStartHookUpdatesProxyPoolRuntime(t *testing.T) {
	t.Parallel()

	cfg := &config.Config{}
	cfg.ProxyURL = "http://before.example:8080"
	service, errBuild := NewBuilder().
		WithConfig(cfg).
		WithConfigPath(filepath.Join(t.TempDir(), "config.yaml")).
		WithHooks(Hooks{OnBeforeStart: func(runtimeConfig *config.Config) {
			runtimeConfig.ProxyURL = "http://after.example:8080"
		}}).
		Build()
	if errBuild != nil {
		t.Fatalf("Build() error = %v", errBuild)
	}
	if errApply := service.applyBeforeStartConfig(); errApply != nil {
		t.Fatalf("applyBeforeStartConfig() error = %v", errApply)
	}

	resolved, errResolve := service.coreManager.ResolveProxyAuth(context.Background(), &coreauth.Auth{ID: "auth-a", Provider: "codex"})
	if errResolve != nil {
		t.Fatalf("ResolveProxyAuth() error = %v", errResolve)
	}
	if resolved.RuntimeProxyURL != "http://after.example:8080" {
		t.Fatalf("runtime proxy URL = %q, want hook-updated URL", resolved.RuntimeProxyURL)
	}
}
