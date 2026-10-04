package management

import (
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

func TestChatGPTWebUploadCooldownSummaryAndReset(t *testing.T) {
	now := time.Now()
	auth := &core.Auth{Provider: "chatgpt-web", Metadata: map[string]any{"upload_cooldown_until": now.Add(time.Hour).Format(time.RFC3339Nano)}}
	entry := gin.H{}
	applyChatGPTWebMetadataSummary(entry, auth.Metadata, "active", now)
	if entry["upload_cooldown_active"] != true || !entry["upload_cooldown_until"].(time.Time).Equal(now.Add(time.Hour)) {
		t.Fatalf("missing upload cooldown: %v", entry)
	}
	if summarizeAuthCooldown(auth, now).Active {
		t.Fatal("upload cooldown exposed as model/auth cooldown")
	}
	applyChatGPTWebMetadataSummary(entry, auth.Metadata, "active", now.Add(2*time.Hour))
	if entry["upload_cooldown_active"] != false {
		t.Fatal("expired upload limit shown active")
	}
	if clearSelectedModelCooldownState(auth, map[string]struct{}{"gpt-image-2": {}}, now) {
		t.Fatal("model reset cleared upload cooldown")
	}
	if !clearFullAuthCooldownState(auth, now) || !core.ChatGPTWebUploadCooldownUntil(auth).IsZero() {
		t.Fatal("explicit full cooldown reset did not clear upload limit")
	}
}
