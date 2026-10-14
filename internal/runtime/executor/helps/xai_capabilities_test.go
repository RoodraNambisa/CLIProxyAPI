package helps

import (
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

func TestXAICapabilitiesCredentialNodeAndSnapshotIsolation(t *testing.T) {
	api, err := ParseXAIModels([]byte(`{"data":[{"id":"grok-4.7","supports_reasoning_effort":false}]}`), "https://api.x.ai/v1/models")
	if err != nil {
		t.Fatal(err)
	}
	cli, err := ParseXAIModels([]byte(`{"data":[{"id":"grok-4.7","reasoning_efforts":[{"value":"low"}]}]}`), "https://cli-chat-proxy.grok.com/v1/models")
	if err != nil {
		t.Fatal(err)
	}
	auth := &coreauth.Auth{ID: "a", Provider: "xai", Metadata: map[string]any{XAIModelCatalogsKey: []*XAIModelCatalog{api, cli}}}
	req := core.Request{Model: "grok-4.7"}
	bound, caps := BindXAIRequestCapabilities(req, auth, &config.Config{}, "https://api.x.ai/v1")
	if !caps.ThinkingKnown || caps.Thinking != nil || caps.ThinkingSource != "credential-catalog" {
		t.Fatalf("API denial lost: %+v", caps)
	}
	caps = ResolveXAIRequestCapabilities(req, auth, &config.Config{})
	if caps.Thinking == nil || len(caps.Thinking.Levels) != 1 || caps.Thinking.Levels[0] != "low" {
		t.Fatalf("CLI declaration lost: %+v", caps)
	}
	auth.Metadata[XAIModelCatalogsKey] = []*XAIModelCatalog{cli}
	caps = ResolveXAIRequestCapabilities(bound, auth, &config.Config{})
	if caps.Thinking != nil || caps.Node != "https://api.x.ai/v1" {
		t.Fatal("attempt snapshot changed")
	}
	other := ResolveXAIRequestCapabilities(req, &coreauth.Auth{ID: "b", Provider: "xai"}, &config.Config{}, "https://api.x.ai/v1")
	if other.ThinkingSource != "builtin" {
		t.Fatal("another credential borrowed cached capabilities")
	}
	unknown := ResolveXAIRequestCapabilities(core.Request{Model: "grok-unknown"}, auth, &config.Config{})
	if unknown.ThinkingKnown || unknown.MaxEditImages != 5 || unknown.QualityKnown {
		t.Fatal("unknown capabilities were invented")
	}
}

func TestXAIImageCapabilitiesValidateMixedReferences(t *testing.T) {
	caps := ResolveXAIRequestCapabilities(core.Request{Model: "grok-imagine-image-2.0"}, nil, &config.Config{})
	if err := caps.ValidateImages([]byte(`{"images":[{"url":"https://example.test/a"},{"file_id":"f1"},{"url":"data:image/png;base64,AA"},{"file_id":"f2"},{"file_id":"f3"}],"quality":"medium"}`)); err != nil {
		t.Fatal(err)
	}
	for _, body := range []string{`{"images":[{},{},{},{},{},{}]}`, `{"quality":"high"}`} {
		if caps.ValidateImages([]byte(body)) == nil {
			t.Fatal("accepted invalid input")
		}
	}
	catalog, err := ParseXAIModels([]byte(`{"data":[{"id":"custom-image","image_capabilities":{"max_edit_images":2,"quality_values":[],"file_id_inputs":false}}]}`), "https://api.x.ai/v1/models")
	if err != nil {
		t.Fatal(err)
	}
	auth := &coreauth.Auth{ID: "c", Provider: "xai", Attributes: map[string]string{"base_url": "https://api.x.ai/v1"}, Metadata: map[string]any{XAIModelCatalogKey: catalog}}
	caps = ResolveXAIRequestCapabilities(core.Request{Model: "custom-image"}, auth, &config.Config{})
	for _, body := range []string{`{"images":[{},{},{}]}`, `{"image":{"file_id":"f"}}`, `{"quality":"medium"}`} {
		if caps.ValidateImages([]byte(body)) == nil {
			t.Fatal("directory denial lost")
		}
	}
}
