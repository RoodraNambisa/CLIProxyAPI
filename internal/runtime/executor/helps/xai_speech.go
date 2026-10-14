package helps

import "strings"

func IsXAISpeechModel(model string) bool {
	model = strings.ToLower(strings.TrimSpace(model))
	if slash := strings.LastIndex(model, "/"); slash >= 0 {
		model = model[slash+1:]
	}
	return model == "grok-tts" || model == "grok-voice-tts-1.0"
}

func XAISpeechModelUnavailable(body []byte) bool {
	lower := strings.ToLower(string(body))
	for _, pattern := range []string{"model_not_found", "model_not_supported", "unsupported model", "model is not supported", "model is unsupported", "model not supported", "model is not available", "model not available", "model is unavailable", "model unavailable", "not available for your plan", "not available for your account"} {
		if strings.Contains(lower, pattern) {
			return true
		}
	}
	return false
}
