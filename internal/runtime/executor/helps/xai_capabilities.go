package helps

import (
	"fmt"
	"strings"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/thinking"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	"github.com/tidwall/gjson"
)

// XAIDeclaredCapabilities keeps absent fields distinct from explicit denials.
type XAIDeclaredCapabilities struct {
	ThinkingKnown bool                      `json:"thinking_known,omitempty"`
	Thinking      *registry.ThinkingSupport `json:"thinking,omitempty"`
	MaxEditImages *int                      `json:"max_edit_images,omitempty"`
	QualityValues *[]string                 `json:"quality_values,omitempty"`
	FileIDInputs  *bool                     `json:"file_id_inputs,omitempty"`
}

type XAIRequestCapabilities struct {
	Model          string                    `json:"model"`
	Node           string                    `json:"node"`
	ThinkingKnown  bool                      `json:"thinking_known"`
	Thinking       *registry.ThinkingSupport `json:"thinking,omitempty"`
	ThinkingSource string                    `json:"thinking_source"`
	MaxEditImages  int                       `json:"max_edit_images"`
	ImageSource    string                    `json:"image_source"`
	QualityValues  []string                  `json:"quality_values,omitempty"`
	QualityKnown   bool                      `json:"quality_known"`
	QualitySource  string                    `json:"quality_source"`
	FileIDInputs   bool                      `json:"file_id_inputs"`
	FileIDKnown    bool                      `json:"file_id_known"`
	FileIDSource   string                    `json:"file_id_source"`
}

const xaiCapabilitiesKey = "cliproxy.xai.capabilities"

func BindXAIRequestCapabilities(req core.Request, auth *coreauth.Auth, cfg *config.Config, nodes ...string) (core.Request, XAIRequestCapabilities) {
	caps := ResolveXAIRequestCapabilities(req, auth, cfg, nodes...)
	metadata := make(map[string]any, len(req.Metadata)+1)
	for key, value := range req.Metadata {
		metadata[key] = value
	}
	metadata[xaiCapabilitiesKey] = caps
	req.Metadata = metadata
	return req, caps
}
func XAIRequestCapabilitySnapshot(req core.Request) (XAIRequestCapabilities, bool) {
	value, ok := req.Metadata[xaiCapabilitiesKey].(XAIRequestCapabilities)
	return value, ok
}

func ResolveXAIRequestCapabilities(req core.Request, auth *coreauth.Auth, cfg *config.Config, nodes ...string) XAIRequestCapabilities {
	if caps, ok := XAIRequestCapabilitySnapshot(req); ok {
		return caps
	}
	model := thinking.ParseSuffix(req.Model).ModelName
	route, _ := ResolveXAIModelUpstream(auth, cfg, model)
	node := route.BaseURL
	if len(nodes) > 0 && nodes[0] != "" {
		node = strings.TrimRight(nodes[0], "/")
	}
	caps := XAIRequestCapabilities{Model: model, Node: node, MaxEditImages: 5, ThinkingSource: "unverified", ImageSource: "compatibility", QualitySource: "unverified", FileIDSource: "compatibility", FileIDInputs: true}
	if info, ok := coreauth.ResolvedAPIKeyModelInfo(req); ok && info != nil && !info.UserDefined {
		caps.ThinkingKnown = true
		caps.Thinking = info.Thinking
		caps.ThinkingSource = "api-key-declaration"
	}
	catalogs := XAIModelCatalogsForAuth(auth)
	if catalog := catalogs[node+"/models"]; catalog != nil {
		if declared, ok := catalog.DeclaredCapabilities[model]; ok {
			if !caps.ThinkingKnown && declared.ThinkingKnown {
				caps.ThinkingKnown = true
				caps.Thinking = declared.Thinking
				caps.ThinkingSource = "credential-catalog"
			}
			if declared.MaxEditImages != nil {
				caps.MaxEditImages = *declared.MaxEditImages
				caps.ImageSource = "credential-catalog"
			}
			if declared.QualityValues != nil {
				caps.QualityKnown = true
				caps.QualityValues = append([]string(nil), (*declared.QualityValues)...)
				caps.QualitySource = "credential-catalog"
			}
			if declared.FileIDInputs != nil {
				caps.FileIDKnown = true
				caps.FileIDInputs = *declared.FileIDInputs
				caps.FileIDSource = "credential-catalog"
			}
		}
	}
	if !caps.ThinkingKnown {
		for _, info := range registry.GetXAIModels() {
			if info.ID == model {
				caps.ThinkingKnown = true
				caps.Thinking = info.Thinking
				caps.ThinkingSource = "builtin"
				break
			}
		}
	}
	if model == "grok-imagine-image-2.0" {
		if caps.ImageSource == "compatibility" {
			caps.ImageSource = "builtin"
		}
		if !caps.QualityKnown {
			caps.QualityKnown = true
			caps.QualityValues = []string{"low", "medium", "auto"}
			caps.QualitySource = "builtin"
		}
	}
	if model == "grok-imagine-image-2.0" {
		if !caps.FileIDKnown {
			caps.FileIDKnown = true
			caps.FileIDInputs = true
			caps.FileIDSource = "builtin"
		}
	}
	if declared := coreauth.ResolvedXAIImageCapabilities(req); declared != nil {
		if declared.MaxEditImages != nil {
			caps.MaxEditImages = *declared.MaxEditImages
			caps.ImageSource = "api-key-declaration"
		}
		if declared.QualityValues != nil {
			caps.QualityKnown = true
			caps.QualityValues = append([]string(nil), (*declared.QualityValues)...)
			caps.QualitySource = "api-key-declaration"
		}
		if declared.FileIDInputs != nil {
			caps.FileIDKnown = true
			caps.FileIDInputs = *declared.FileIDInputs
			caps.FileIDSource = "api-key-declaration"
		}
	}
	return caps
}

func (caps XAIRequestCapabilities) ApplyThinking(body []byte, req core.Request, opts core.Options, from, to string) ([]byte, error) {
	var info *registry.ModelInfo
	if caps.ThinkingKnown {
		info = &registry.ModelInfo{ID: caps.Model, Thinking: caps.Thinking}
	}
	return thinking.ApplyThinkingWithModelInfoAndSummary(body, nil, req.Model, from, to, "xai", info, RequestSummaryConfig(body, req, opts, from, to))
}

func (caps XAIRequestCapabilities) ValidateImages(body []byte) error {
	refs := gjson.GetBytes(body, "images").Array()
	if image := gjson.GetBytes(body, "image"); image.Exists() {
		refs = append(refs, image)
	}
	if len(refs) > caps.MaxEditImages {
		return fmt.Errorf("Grok model %s accepts at most %d reference images", caps.Model, caps.MaxEditImages)
	}
	for _, ref := range refs {
		if ref.Get("file_id").Exists() && caps.FileIDKnown && !caps.FileIDInputs {
			return fmt.Errorf("Grok model %s does not accept file_id image inputs", caps.Model)
		}
	}
	if quality := gjson.GetBytes(body, "quality"); quality.Exists() && caps.QualityKnown {
		for _, value := range caps.QualityValues {
			if quality.String() == value {
				return nil
			}
		}
		return fmt.Errorf("Grok model %s does not accept quality %q", caps.Model, quality.String())
	}
	return nil
}
