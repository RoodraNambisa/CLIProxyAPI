package openai

import (
	"errors"
	"slices"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/constant"
	coreexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
	"github.com/tidwall/sjson"
)

func (h *OpenAIImagesAPIHandler) imageModelsConfig() sdkconfig.ImagesConfig {
	if h != nil && h.Cfg != nil {
		return h.Cfg.Images
	}
	return sdkconfig.ImagesConfig{}
}

func imageModelInList(models []string, model string) bool {
	for _, candidate := range models {
		if strings.EqualFold(strings.TrimSpace(candidate), strings.TrimSpace(model)) {
			return true
		}
	}
	return false
}

func (h *OpenAIImagesAPIHandler) configuredImageModels() []string {
	cfg := h.imageModelsConfig()
	models := append(cfg.ResolvedImageModels(), cfg.ResolvedChatGPTWebImageModels()...)
	for _, op := range []imageOperation{imageGenerationOperation, imageEditOperation} {
		if endpoint := h.nativeImageEndpointConfig(op); endpoint.Enabled {
			models = append(models, endpoint.Models...)
		}
	}
	var result []string
	for _, model := range models {
		if !imageModelInList(result, model) {
			result = append(result, model)
		}
	}
	return result
}

func (h *OpenAIImagesAPIHandler) codexImageModelSupported(model string, op imageOperation) bool {
	if native := h.nativeImageEndpointConfig(op); native.Enabled {
		return nativeImageModelAllowed(native.Models, model)
	}
	return imageModelInList(h.imageModelsConfig().ResolvedImageModels(), model)
}

func (h *OpenAIImagesAPIHandler) imageModelSupported(model string, op imageOperation) bool {
	return h.codexImageModelSupported(model, op) || imageModelInList(h.imageModelsConfig().ResolvedChatGPTWebImageModels(), model)
}

func (h *OpenAIImagesAPIHandler) configuredImageProviders(req openAIImageRequest, op imageOperation, ignoreUnsupported bool, snapshot coreexecutor.ChatGPTWebImageConfigSnapshot) []string {
	var result []string
	for _, provider := range imageResponsesProviders(req, ignoreUnsupported, snapshot) {
		if provider == constant.Codex && h.codexImageModelSupported(req.Model, op) ||
			provider == constant.ChatGPTWeb && imageModelInList(h.imageModelsConfig().ResolvedChatGPTWebImageModels(), req.Model) {
			result = append(result, provider)
		}
	}
	return result
}

func nativeImageRequest(c *gin.Context) *coreexecutor.CodexNativeImageRequest {
	if c == nil {
		return nil
	}
	value, _ := c.Get(coreexecutor.CodexNativeImageRequestMetadataKey)
	request, _ := value.(*coreexecutor.CodexNativeImageRequest)
	return request
}

// Native mode changes only the Codex request alternative, not Web eligibility.
func (h *OpenAIImagesAPIHandler) handleNativeImagesRequest(c *gin.Context, rawJSON []byte, req openAIImageRequest, op imageOperation) {
	if strings.TrimSpace(req.Model) == "" {
		req.Model = h.imagesImageModel()
	}
	native := h.nativeImageEndpointConfig(op)
	codexAllowed := nativeImageModelAllowed(native.Models, req.Model)
	webAllowed := imageModelInList(h.imageModelsConfig().ResolvedChatGPTWebImageModels(), req.Model)
	webReq := req
	webErr := h.validateImageRequest(&webReq, op)
	if webErr == nil && codexAllowed && h.chatGPTWebImageConfigSnapshot().NormalizeMismatchedImageMIME {
		webReq.Images = slices.Clone(req.Images)
		if req.Mask != nil {
			mask := *req.Mask
			webReq.Mask = &mask
		}
		webErr = normalizeChatGPTWebImageRequestMIME(&webReq)
	}
	if webErr == nil && imageRequestCount(webReq) > 1 && !h.imagesNAggregationEnabled() {
		webErr = unsupportedImageErrorf("n > 1 is not supported")
	}
	if webErr == nil {
		if incompatibility := chatGPTWebImageRequestCompatibilityError(webReq, h.chatGPTWebIgnoreUnsupportedImageParams(), h.chatGPTWebImageConfigSnapshot()); incompatibility != nil && !incompatibility.providerFiltered {
			webErr = chatGPTWebUnsupportedParameterError(incompatibility)
		}
	}
	webAllowed = webAllowed && webErr == nil
	if !webAllowed {
		if codexAllowed {
			h.handleCodexNativeImagesRequest(c, rawJSON, req, op)
		} else if webErr != nil && imageModelInList(h.imageModelsConfig().ResolvedChatGPTWebImageModels(), req.Model) {
			h.PinChatGPTWebImageErrorSanitization(c, true)
			h.writeImagesRequestError(c, webErr)
		} else {
			h.writeImagesError(c, native.UnsupportedModelStatusCode, errors.New(nativeImageUnsupportedModelMessage(native.UnsupportedModelMessage, req.Model)))
		}
		return
	}
	if codexAllowed {
		rawJSON, _ = sjson.SetBytes(rawJSON, "model", req.Model)
		rawJSON = applyNativeImageParamRules(rawJSON, native.ParamRules)
		request := coreexecutor.NewCodexNativeImageRequest(rawJSON, nativeImagesAlt(op))
		defer request.Release()
		c.Set(coreexecutor.CodexNativeImageRequestMetadataKey, request)
	}
	h.handleImagesRequest(c, webReq, op)
}
