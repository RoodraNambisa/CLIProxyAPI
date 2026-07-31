package config

import (
	"strings"
	"testing"
)

func TestChatGPTWebLoginProxyDefaults(t *testing.T) {
	resolved := (ChatGPTWebLoginProxyConfig{}).Resolved()
	if resolved.Enabled || resolved.URLTemplate != "" || resolved.PlaceholderCharset != "" ||
		!resolved.RotateOnRetry ||
		resolved.RequestAttempts != DefaultChatGPTWebLoginProxyRequestAttempts ||
		resolved.FlowAttempts != DefaultChatGPTWebLoginProxyFlowAttempts ||
		resolved.RetryDelayMilliseconds != DefaultChatGPTWebLoginProxyRetryDelayMS ||
		resolved.AcquisitionTimeoutSeconds != DefaultChatGPTWebLoginProxyTimeoutSeconds {
		t.Fatalf("resolved defaults = %#v", resolved)
	}
}

func TestChatGPTWebLoginProxyValidation(t *testing.T) {
	zero := 0
	one := 1
	thirty := 30
	disabledRotation := false
	valid := ChatGPTWebLoginProxyConfig{
		Enabled:                   true,
		URLTemplate:               "http://session-{12}:secret@proxy.example:59999",
		RotateOnRetry:             &disabledRotation,
		RequestAttempts:           &one,
		FlowAttempts:              &one,
		RetryDelayMilliseconds:    &zero,
		AcquisitionTimeoutSeconds: &thirty,
	}
	if errValidate := valid.Validate(); errValidate != nil {
		t.Fatalf("Validate() error = %v", errValidate)
	}
	if resolved := valid.Resolved(); resolved.RotateOnRetry || resolved.RetryDelayMilliseconds != 0 {
		t.Fatalf("explicit false/zero values were not preserved: %#v", resolved)
	}

	tests := []struct {
		name   string
		config ChatGPTWebLoginProxyConfig
		want   string
	}{
		{name: "enabled without template", config: ChatGPTWebLoginProxyConfig{Enabled: true}, want: "url-template is required"},
		{name: "unsupported scheme", config: ChatGPTWebLoginProxyConfig{URLTemplate: "ftp://proxy.example:21"}, want: "invalid proxy URL"},
		{name: "placeholder outside credentials", config: ChatGPTWebLoginProxyConfig{URLTemplate: "http://proxy-{4}.example:8080"}, want: "only supported in proxy credentials"},
		{name: "request attempts", config: ChatGPTWebLoginProxyConfig{RequestAttempts: &zero}, want: "request-attempts"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			errValidate := test.config.Validate()
			if errValidate == nil || !strings.Contains(errValidate.Error(), test.want) {
				t.Fatalf("Validate() error = %v, want %q", errValidate, test.want)
			}
		})
	}
}
