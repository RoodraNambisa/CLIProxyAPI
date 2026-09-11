package management

import (
	"net/http"
	"os"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/sentinelcompat"
)

func TestSentinelCompatibilityManagementPreservesLegacyAndRejectsInvalid(t *testing.T) {
	initial := config.ChatGPTWebSentinelConfig{GoVMCompatibility: sentinelcompat.Config{Enabled: true, WritableWindowProperties: []string{"__owner"}}}
	h, path := newPersistedChatGPTWebSentinelHandler(t, initial)
	ctx, rec := newChatGPTWebSentinelRequest(http.MethodPut, `{"sdk-runtime-enabled":true,"sdk-workers":2,"sdk-queue-size":4,"sdk-cache-versions":3}`)
	h.PutChatGPTWebSentinel(ctx)
	if rec.Code != 200 {
		t.Fatal(rec.Body.String())
	}
	loaded, err := config.LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded.ChatGPTWeb.Sentinel.GoVMCompatibility.WritableWindowProperties) != 1 {
		t.Fatal("legacy PUT removed compatibility config")
	}
	ctx, rec = newChatGPTWebSentinelRequest(http.MethodPatch, `{"go-vm-compatibility":{"enabled":false}}`)
	h.PatchChatGPTWebSentinel(ctx)
	if rec.Code != 200 || h.cfg.ChatGPTWeb.Sentinel.GoVMCompatibility.Enabled || len(h.cfg.ChatGPTWeb.Sentinel.GoVMCompatibility.WritableWindowProperties) != 1 {
		t.Fatal("partial PATCH lost fields")
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	ctx, rec = newChatGPTWebSentinelRequest(http.MethodPatch, `{"go-vm-compatibility":{"writable-window-properties":["__proto__"]}}`)
	h.PatchChatGPTWebSentinel(ctx)
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if rec.Code != 400 || string(before) != string(after) || h.cfg.ChatGPTWeb.Sentinel.GoVMCompatibility.WritableWindowProperties[0] != "__owner" {
		t.Fatal("invalid rules changed persisted config")
	}
}
