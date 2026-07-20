package openai

import (
	"context"
	"encoding/json"
	"fmt"
	"mime"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/interfaces"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/api/handlers"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

const (
	xaiProvider                 = "xai"
	defaultXAIImagesModel       = "grok-imagine-image"
	xaiImagesQualityModel       = "grok-imagine-image-quality"
	xaiImagesHandlerType        = "openai-image"
	xaiImagesDefaultAspectRatio = "1:1"
	xaiImagesDefaultResolution  = "1k"
)

type xaiImageResult struct {
	B64JSON       string
	URL           string
	RevisedPrompt string
	MimeType      string
}

func imagesModelParts(model string) (prefix string, baseModel string) {
	model = strings.TrimSpace(model)
	if index := strings.LastIndex(model, "/"); index >= 0 && index < len(model)-1 {
		return strings.TrimSpace(model[:index]), strings.TrimSpace(model[index+1:])
	}
	return "", model
}

func isXAIImagesModel(model string) bool {
	_, baseModel := imagesModelParts(model)
	baseModel = strings.ToLower(strings.TrimSpace(baseModel))
	return baseModel == defaultXAIImagesModel || baseModel == xaiImagesQualityModel
}

func canonicalXAIImagesModel(model string) string {
	_, baseModel := imagesModelParts(model)
	if strings.EqualFold(strings.TrimSpace(baseModel), xaiImagesQualityModel) {
		return xaiImagesQualityModel
	}
	return defaultXAIImagesModel
}

func normalizeXAIImagesResponseFormat(responseFormat string) string {
	if strings.EqualFold(strings.TrimSpace(responseFormat), "url") {
		return "url"
	}
	return "b64_json"
}

func xaiImagesAspectRatio(raw, fallback string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "1:1", "square":
		return "1:1"
	case "16:9", "landscape":
		return "16:9"
	case "9:16", "portrait":
		return "9:16"
	case "4:3":
		return "4:3"
	case "3:4":
		return "3:4"
	case "3:2":
		return "3:2"
	case "2:3":
		return "2:3"
	default:
		return fallback
	}
}

func xaiImagesAspectRatioFromSize(size, fallback string) string {
	switch strings.ToLower(strings.TrimSpace(size)) {
	case "1024x1024", "2048x2048", "1:1":
		return "1:1"
	case "1792x1024", "16:9":
		return "16:9"
	case "1024x1792", "9:16":
		return "9:16"
	case "1536x1024", "3:2":
		return "3:2"
	case "1024x1536", "2:3":
		return "2:3"
	default:
		return fallback
	}
}

func xaiImagesResolution(raw, size, fallback string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "1k", "2k":
		return strings.ToLower(strings.TrimSpace(raw))
	}
	if strings.Contains(strings.ToLower(strings.TrimSpace(size)), "2048") {
		return "2k"
	}
	return fallback
}

func xaiImagesRef(imageURL string) []byte {
	ref := []byte(`{"type":"image_url","url":""}`)
	ref, _ = sjson.SetBytes(ref, "url", strings.TrimSpace(imageURL))
	return ref
}

func buildXAIImagesBaseRequest(model, prompt, responseFormat, aspectRatio, resolution string, n int64) []byte {
	req := []byte(`{}`)
	req, _ = sjson.SetBytes(req, "model", canonicalXAIImagesModel(model))
	req, _ = sjson.SetBytes(req, "prompt", strings.TrimSpace(prompt))
	req, _ = sjson.SetBytes(req, "response_format", normalizeXAIImagesResponseFormat(responseFormat))
	if aspectRatio != "" {
		req, _ = sjson.SetBytes(req, "aspect_ratio", aspectRatio)
	}
	if resolution != "" {
		req, _ = sjson.SetBytes(req, "resolution", resolution)
	}
	if n > 0 {
		req, _ = sjson.SetBytes(req, "n", n)
	}
	return req
}

func buildXAIImagesGenerationsRequest(rawJSON []byte, model, responseFormat string) []byte {
	size := strings.TrimSpace(gjson.GetBytes(rawJSON, "size").String())
	aspectRatio := xaiImagesAspectRatio(gjson.GetBytes(rawJSON, "aspect_ratio").String(), "")
	aspectRatio = xaiImagesAspectRatioFromSize(size, aspectRatio)
	if aspectRatio == "" {
		aspectRatio = xaiImagesDefaultAspectRatio
	}
	resolution := xaiImagesResolution(gjson.GetBytes(rawJSON, "resolution").String(), size, xaiImagesDefaultResolution)
	n := int64(0)
	if value := gjson.GetBytes(rawJSON, "n"); value.Exists() && value.Type == gjson.Number {
		n = value.Int()
	}
	return buildXAIImagesBaseRequest(model, gjson.GetBytes(rawJSON, "prompt").String(), responseFormat, aspectRatio, resolution, n)
}

func buildXAIImagesEditRequest(model, prompt string, images []string, responseFormat, aspectRatio, resolution string, n int64) []byte {
	req := buildXAIImagesBaseRequest(model, prompt, responseFormat, aspectRatio, resolution, n)
	filtered := make([]string, 0, len(images))
	for _, image := range images {
		if image = strings.TrimSpace(image); image != "" {
			filtered = append(filtered, image)
		}
	}
	if len(filtered) == 1 {
		req, _ = sjson.SetRawBytes(req, "image", xaiImagesRef(filtered[0]))
		return req
	}
	for _, image := range filtered {
		req, _ = sjson.SetRawBytes(req, "images.-1", xaiImagesRef(image))
	}
	return req
}

func collectXAIImagesFromJSON(rawJSON []byte) []string {
	images := make([]string, 0)
	appendImage := func(value string) {
		if value = strings.TrimSpace(value); value != "" {
			images = append(images, value)
		}
	}
	collect := func(value gjson.Result) {
		if value.Type == gjson.String {
			appendImage(value.String())
			return
		}
		appendImage(value.Get("image_url.url").String())
		if imageURL := value.Get("image_url"); imageURL.Type == gjson.String {
			appendImage(imageURL.String())
		}
		appendImage(value.Get("url").String())
	}
	if image := gjson.GetBytes(rawJSON, "image"); image.Exists() {
		collect(image)
	}
	for _, image := range gjson.GetBytes(rawJSON, "images").Array() {
		collect(image)
	}
	return images
}

func xaiImagesEditOptions(rawJSON []byte) (string, string, int64) {
	size := strings.TrimSpace(gjson.GetBytes(rawJSON, "size").String())
	aspectRatio := xaiImagesAspectRatio(gjson.GetBytes(rawJSON, "aspect_ratio").String(), "")
	aspectRatio = xaiImagesAspectRatioFromSize(size, aspectRatio)
	resolution := xaiImagesResolution(gjson.GetBytes(rawJSON, "resolution").String(), size, "")
	n := int64(0)
	if value := gjson.GetBytes(rawJSON, "n"); value.Exists() && value.Type == gjson.Number {
		n = value.Int()
	}
	return aspectRatio, resolution, n
}

func (h *OpenAIImagesAPIHandler) handleXAIGenerationIfRequested(c *gin.Context, rawJSON []byte) bool {
	model := strings.TrimSpace(gjson.GetBytes(rawJSON, "model").String())
	if !isXAIImagesModel(model) {
		return false
	}
	if !json.Valid(rawJSON) {
		h.writeImagesRequestError(c, fmt.Errorf("invalid request: body must be valid JSON"))
		return true
	}
	prompt := strings.TrimSpace(gjson.GetBytes(rawJSON, "prompt").String())
	if prompt == "" {
		h.writeImagesRequestError(c, fmt.Errorf("prompt is required"))
		return true
	}
	responseFormat := normalizeXAIImagesResponseFormat(gjson.GetBytes(rawJSON, "response_format").String())
	h.handleXAIImages(c, model, buildXAIImagesGenerationsRequest(rawJSON, model, responseFormat), responseFormat, "image_generation", gjson.GetBytes(rawJSON, "stream").Bool())
	return true
}

func (h *OpenAIImagesAPIHandler) handleXAIEditIfRequested(c *gin.Context, rawJSON []byte) bool {
	contentType, _, _ := mime.ParseMediaType(c.GetHeader("Content-Type"))
	if strings.EqualFold(contentType, "multipart/form-data") {
		form, err := ensureImageMultipartForm(c)
		if err != nil {
			h.writeImagesRequestError(c, err)
			return true
		}
		model := multipartValue(form, "model")
		if !isXAIImagesModel(model) {
			return false
		}
		req, err := parseMultipartImageEditRequest(c)
		if err != nil {
			h.writeImagesRequestError(c, err)
			return true
		}
		if err = validateImageRequestCount(&req); err != nil {
			h.writeImagesRequestError(c, err)
			return true
		}
		if strings.TrimSpace(req.Prompt) == "" {
			h.writeImagesRequestError(c, fmt.Errorf("prompt is required"))
			return true
		}
		images := make([]string, 0, len(req.Images))
		for _, image := range req.Images {
			imageURL, errURL := imageURLFromReference(image)
			if errURL != nil {
				h.writeImagesRequestError(c, errURL)
				return true
			}
			images = append(images, imageURL)
		}
		if len(images) == 0 {
			h.writeImagesRequestError(c, fmt.Errorf("image is required"))
			return true
		}
		responseFormat := normalizeXAIImagesResponseFormat(req.ResponseFormat)
		aspectRatio := xaiImagesAspectRatio(req.XAIAspectRatio, "")
		aspectRatio = xaiImagesAspectRatioFromSize(req.Size, aspectRatio)
		resolution := xaiImagesResolution(req.XAIResolution, req.Size, "")
		var n int64
		if req.N != nil {
			n = int64(*req.N)
		}
		xaiReq := buildXAIImagesEditRequest(model, req.Prompt, images, responseFormat, aspectRatio, resolution, n)
		h.handleXAIImages(c, model, xaiReq, responseFormat, "image_edit", req.Stream)
		return true
	}

	model := strings.TrimSpace(gjson.GetBytes(rawJSON, "model").String())
	if !isXAIImagesModel(model) {
		return false
	}
	if !json.Valid(rawJSON) {
		h.writeImagesRequestError(c, fmt.Errorf("invalid request: body must be valid JSON"))
		return true
	}
	prompt := strings.TrimSpace(gjson.GetBytes(rawJSON, "prompt").String())
	images := collectXAIImagesFromJSON(rawJSON)
	if prompt == "" || len(images) == 0 {
		h.writeImagesRequestError(c, fmt.Errorf("prompt and image are required"))
		return true
	}
	responseFormat := normalizeXAIImagesResponseFormat(gjson.GetBytes(rawJSON, "response_format").String())
	aspectRatio, resolution, n := xaiImagesEditOptions(rawJSON)
	h.handleXAIImages(c, model, buildXAIImagesEditRequest(model, prompt, images, responseFormat, aspectRatio, resolution, n), responseFormat, "image_edit", gjson.GetBytes(rawJSON, "stream").Bool())
	return true
}

func extractXAIImagesResponse(payload []byte) ([]xaiImageResult, int64, []byte, error) {
	if !json.Valid(payload) {
		return nil, 0, nil, fmt.Errorf("upstream returned invalid image response JSON")
	}
	createdAt := gjson.GetBytes(payload, "created").Int()
	if createdAt <= 0 {
		createdAt = time.Now().Unix()
	}
	results := make([]xaiImageResult, 0)
	for _, item := range gjson.GetBytes(payload, "data").Array() {
		result := xaiImageResult{
			B64JSON:       strings.TrimSpace(item.Get("b64_json").String()),
			URL:           strings.TrimSpace(item.Get("url").String()),
			RevisedPrompt: strings.TrimSpace(item.Get("revised_prompt").String()),
			MimeType:      strings.TrimSpace(item.Get("mime_type").String()),
		}
		if result.MimeType == "" {
			result.MimeType = mimeTypeFromOutputFormat(item.Get("output_format").String())
		}
		if result.B64JSON != "" || result.URL != "" {
			results = append(results, result)
		}
	}
	if len(results) == 0 {
		return nil, 0, nil, fmt.Errorf("upstream did not return image output")
	}
	var usage []byte
	if node := gjson.GetBytes(payload, "usage"); node.Exists() && node.IsObject() {
		usage = []byte(node.Raw)
	}
	return results, createdAt, usage, nil
}

func buildImagesAPIResponseFromXAI(payload []byte, responseFormat string) ([]byte, error) {
	results, createdAt, usage, err := extractXAIImagesResponse(payload)
	if err != nil {
		return nil, err
	}
	out := []byte(`{"created":0,"data":[]}`)
	out, _ = sjson.SetBytes(out, "created", createdAt)
	responseFormat = normalizeXAIImagesResponseFormat(responseFormat)
	for _, image := range results {
		item := []byte(`{}`)
		if responseFormat == "url" {
			if image.URL != "" {
				item, _ = sjson.SetBytes(item, "url", image.URL)
			} else {
				item, _ = sjson.SetBytes(item, "url", "data:"+mimeTypeFromOutputFormat(image.MimeType)+";base64,"+image.B64JSON)
			}
		} else if image.B64JSON != "" {
			item, _ = sjson.SetBytes(item, "b64_json", image.B64JSON)
		} else {
			item, _ = sjson.SetBytes(item, "url", image.URL)
		}
		if image.RevisedPrompt != "" {
			item, _ = sjson.SetBytes(item, "revised_prompt", image.RevisedPrompt)
		}
		out, _ = sjson.SetRawBytes(out, "data.-1", item)
	}
	if len(usage) > 0 {
		out, _ = sjson.SetRawBytes(out, "usage", usage)
	}
	return out, nil
}

func (h *OpenAIImagesAPIHandler) handleXAIImages(c *gin.Context, routeModel string, request []byte, responseFormat, streamPrefix string, stream bool) {
	routeModel = strings.TrimSpace(routeModel)
	cliCtx, cliCancel := h.GetContextWithCancel(h, c, context.Background())
	stopKeepAlive := h.StartNonStreamingKeepAlive(c, cliCtx)
	resp, upstreamHeaders, errMsg := h.ExecuteWithProviders(cliCtx, []string{xaiProvider}, xaiImagesHandlerType, routeModel, request, "")
	stopKeepAlive()
	if errMsg != nil {
		h.WriteErrorResponse(c, errMsg)
		cliCancel(errMsg.Error)
		return
	}
	results, _, usage, err := extractXAIImagesResponse(resp)
	if err != nil {
		h.WriteErrorResponse(c, &interfaces.ErrorMessage{StatusCode: http.StatusBadGateway, Error: err})
		cliCancel(err)
		return
	}
	handlers.WriteUpstreamHeaders(c.Writer.Header(), upstreamHeaders)
	if !stream {
		out, errBuild := buildImagesAPIResponseFromXAI(resp, responseFormat)
		if errBuild != nil {
			h.WriteErrorResponse(c, &interfaces.ErrorMessage{StatusCode: http.StatusBadGateway, Error: errBuild})
			cliCancel(errBuild)
			return
		}
		c.Header("Content-Type", "application/json")
		_, _ = c.Writer.Write(out)
		cliCancel(nil)
		return
	}

	c.Header("Content-Type", "text/event-stream")
	c.Header("Cache-Control", "no-cache")
	eventName := streamPrefix + ".completed"
	responseFormat = normalizeXAIImagesResponseFormat(responseFormat)
	for _, image := range results {
		data := []byte(`{"type":""}`)
		data, _ = sjson.SetBytes(data, "type", eventName)
		if responseFormat == "url" {
			if image.URL != "" {
				data, _ = sjson.SetBytes(data, "url", image.URL)
			} else {
				data, _ = sjson.SetBytes(data, "url", "data:"+mimeTypeFromOutputFormat(image.MimeType)+";base64,"+image.B64JSON)
			}
		} else if image.B64JSON != "" {
			data, _ = sjson.SetBytes(data, "b64_json", image.B64JSON)
		} else {
			data, _ = sjson.SetBytes(data, "url", image.URL)
		}
		if len(usage) > 0 {
			data, _ = sjson.SetRawBytes(data, "usage", usage)
		}
		_, _ = fmt.Fprintf(c.Writer, "event: %s\ndata: %s\n\n", eventName, data)
	}
	if flusher, ok := c.Writer.(http.Flusher); ok {
		flusher.Flush()
	}
	cliCancel(nil)
}
