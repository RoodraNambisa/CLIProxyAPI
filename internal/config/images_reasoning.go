package config

import (
	"fmt"
	"strings"
)

const (
	ChatGPTWebImageReasoningAuto    = "auto"
	ChatGPTWebImageReasoningInstant = "instant"
	// This fallback was verified against the website's Instant catalog category.
	DefaultChatGPTWebImageInstantModel = "gpt-5-6"
)

func (cfg ChatGPTWebImageConfig) ResolvedReasoningMode() string {
	if mode := strings.ToLower(strings.TrimSpace(cfg.ReasoningMode)); mode != "" {
		return mode
	}
	return ChatGPTWebImageReasoningAuto
}

func (cfg ChatGPTWebImageConfig) ValidateReasoningMode() error {
	switch cfg.ResolvedReasoningMode() {
	case ChatGPTWebImageReasoningAuto, "low", "medium", "high", "xhigh":
		return nil
	case ChatGPTWebImageReasoningInstant:
		if cfg.ResolvedUpstreamModel() != DefaultChatGPTWebImageUpstreamModel {
			return fmt.Errorf("images.chatgpt-web.reasoning-mode instant requires upstream-model auto; the custom carrier is not overwritten")
		}
		return nil
	default:
		return fmt.Errorf("images.chatgpt-web.reasoning-mode must be auto, instant, low, medium, high or xhigh")
	}
}
