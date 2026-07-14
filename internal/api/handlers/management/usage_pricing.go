package management

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/usage"
)

// GetUsagePrices returns the shared server-side model price table.
func (h *Handler) GetUsagePrices(c *gin.Context) {
	pricing, ok := h.usagePricingSnapshot()
	if !ok {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "configuration unavailable"})
		return
	}
	c.JSON(http.StatusOK, pricing)
}

// PutUsagePrices replaces the shared server-side model price table.
func (h *Handler) PutUsagePrices(c *gin.Context) {
	patches, ok := bindUsagePricing(c)
	if !ok {
		return
	}
	input := config.UsagePricingConfig{Models: make(map[string]config.UsageModelPrice, len(patches))}
	for model, patch := range patches {
		input.Models[model] = patch.apply(config.UsageModelPrice{})
	}
	normalized, err := config.NormalizeUsagePricing(input)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if h == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "configuration unavailable"})
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.cfg == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "configuration unavailable"})
		return
	}
	previous := h.cfg.UsagePricing
	h.cfg.UsagePricing = normalized
	if !h.persistLocked(c) {
		h.cfg.UsagePricing = previous
	}
}

// PatchUsagePrices merges model prices into the shared table.
func (h *Handler) PatchUsagePrices(c *gin.Context) {
	patches, ok := bindUsagePricing(c)
	if !ok {
		return
	}
	if h == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "configuration unavailable"})
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.cfg == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "configuration unavailable"})
		return
	}
	models := cloneUsagePricingModels(h.cfg.UsagePricing.Models)
	for model, patch := range patches {
		models[model] = patch.apply(models[model])
	}
	merged, err := config.NormalizeUsagePricing(config.UsagePricingConfig{Models: models})
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	previous := h.cfg.UsagePricing
	h.cfg.UsagePricing = merged
	if !h.persistLocked(c) {
		h.cfg.UsagePricing = previous
	}
}

// DeleteUsagePrices removes all shared model prices.
func (h *Handler) DeleteUsagePrices(c *gin.Context) {
	if h == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "configuration unavailable"})
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.cfg == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "configuration unavailable"})
		return
	}
	previous := h.cfg.UsagePricing
	h.cfg.UsagePricing = config.UsagePricingConfig{Models: map[string]config.UsageModelPrice{}}
	if !h.persistLocked(c) {
		h.cfg.UsagePricing = previous
	}
}

// DeleteUsagePrice removes one shared model price.
func (h *Handler) DeleteUsagePrice(c *gin.Context) {
	model := strings.ToLower(strings.TrimSpace(strings.TrimPrefix(c.Param("model"), "/")))
	if model == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "model is required"})
		return
	}
	if h == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "configuration unavailable"})
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.cfg == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "configuration unavailable"})
		return
	}
	models := cloneUsagePricingModels(h.cfg.UsagePricing.Models)
	if _, exists := models[model]; !exists {
		c.JSON(http.StatusNotFound, gin.H{"error": "model price not found"})
		return
	}
	delete(models, model)
	previous := h.cfg.UsagePricing
	h.cfg.UsagePricing = config.UsagePricingConfig{Models: models}
	if !h.persistLocked(c) {
		h.cfg.UsagePricing = previous
	}
}

func (h *Handler) usagePricingSnapshot() (config.UsagePricingConfig, bool) {
	if h == nil {
		return config.UsagePricingConfig{}, false
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.cfg == nil {
		return config.UsagePricingConfig{}, false
	}
	return config.UsagePricingConfig{Models: cloneUsagePricingModels(h.cfg.UsagePricing.Models)}, true
}

func (h *Handler) usageCostPrices() map[string]usage.ModelPrice {
	pricing, ok := h.usagePricingSnapshot()
	if !ok {
		return map[string]usage.ModelPrice{}
	}
	pricing, err := config.NormalizeUsagePricing(pricing)
	if err != nil {
		return map[string]usage.ModelPrice{}
	}
	prices := make(map[string]usage.ModelPrice, len(pricing.Models))
	for model, price := range pricing.Models {
		prices[model] = usage.ModelPrice{
			InputPerMillion:       price.InputPerMillion,
			OutputPerMillion:      price.OutputPerMillion,
			CachedInputPerMillion: price.CachedInputPerMillion,
		}
	}
	return prices
}

func cloneUsagePricingModels(input map[string]config.UsageModelPrice) map[string]config.UsageModelPrice {
	output := make(map[string]config.UsageModelPrice, len(input))
	for model, price := range input {
		output[model] = price
	}
	return output
}

type usageModelPricePatch struct {
	InputPerMillion       *float64
	OutputPerMillion      *float64
	CachedInputPerMillion *float64
}

func (p *usageModelPricePatch) UnmarshalJSON(data []byte) error {
	if p == nil || bytes.Equal(bytes.TrimSpace(data), []byte("null")) {
		return fmt.Errorf("model price must be an object")
	}
	var raw struct {
		InputPerMillion       json.RawMessage `json:"input-per-million"`
		OutputPerMillion      json.RawMessage `json:"output-per-million"`
		CachedInputPerMillion json.RawMessage `json:"cached-input-per-million"`
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&raw); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return fmt.Errorf("invalid model price")
	}
	var err error
	if p.InputPerMillion, err = decodeOptionalPrice(raw.InputPerMillion); err != nil {
		return fmt.Errorf("invalid input-per-million: %w", err)
	}
	if p.OutputPerMillion, err = decodeOptionalPrice(raw.OutputPerMillion); err != nil {
		return fmt.Errorf("invalid output-per-million: %w", err)
	}
	if p.CachedInputPerMillion, err = decodeOptionalPrice(raw.CachedInputPerMillion); err != nil {
		return fmt.Errorf("invalid cached-input-per-million: %w", err)
	}
	return nil
}

func (p usageModelPricePatch) apply(current config.UsageModelPrice) config.UsageModelPrice {
	if p.InputPerMillion != nil {
		current.InputPerMillion = *p.InputPerMillion
	}
	if p.OutputPerMillion != nil {
		current.OutputPerMillion = *p.OutputPerMillion
	}
	if p.CachedInputPerMillion != nil {
		current.CachedInputPerMillion = *p.CachedInputPerMillion
	}
	return current
}

func decodeOptionalPrice(raw json.RawMessage) (*float64, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return nil, fmt.Errorf("value must be a number")
	}
	var value float64
	if err := json.Unmarshal(raw, &value); err != nil {
		return nil, err
	}
	return &value, nil
}

func bindUsagePricing(c *gin.Context) (map[string]usageModelPricePatch, bool) {
	var body struct {
		Models *map[string]*usageModelPricePatch `json:"models"`
	}
	decoder := json.NewDecoder(c.Request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&body); err != nil || body.Models == nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid body"})
		return nil, false
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid body"})
		return nil, false
	}
	patches := make(map[string]usageModelPricePatch, len(*body.Models))
	for rawModel, patch := range *body.Models {
		if patch == nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid body"})
			return nil, false
		}
		candidate := patch.apply(config.UsageModelPrice{})
		normalized, err := config.NormalizeUsagePricing(config.UsagePricingConfig{Models: map[string]config.UsageModelPrice{rawModel: candidate}})
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return nil, false
		}
		for model := range normalized.Models {
			if _, exists := patches[model]; exists {
				c.JSON(http.StatusBadRequest, gin.H{"error": "duplicate normalized model"})
				return nil, false
			}
			patches[model] = *patch
		}
	}
	return patches, true
}
