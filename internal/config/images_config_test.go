package config

import (
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLoadConfigOptionalImagesStreamFlushSettings(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	data := []byte(`images:
  enable-stream-flush: false
  stream-flush-interval-ms: 20
  stream-flush-min-bytes: 65536
`)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := LoadConfigOptional(path, false)
	if err != nil {
		t.Fatalf("LoadConfigOptional() error = %v", err)
	}
	if cfg.Images.StreamFlushIntervalMS != 20 {
		t.Fatalf("StreamFlushIntervalMS = %d, want 20", cfg.Images.StreamFlushIntervalMS)
	}
	if cfg.Images.StreamFlushMinBytes != 65536 {
		t.Fatalf("StreamFlushMinBytes = %d, want 65536", cfg.Images.StreamFlushMinBytes)
	}
	if cfg.Images.EnableStreamFlush == nil || *cfg.Images.EnableStreamFlush {
		t.Fatalf("EnableStreamFlush = %v, want false", cfg.Images.EnableStreamFlush)
	}
}

func TestLoadConfigOptionalChatGPTWebImageSettings(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	data := []byte(`images:
  chatgpt-web:
    upstream-model: gpt-5-5-custom
    ignore-unsupported-params: true
    remote-image-url-enabled: true
    remote-image-url-download-mode: credential-proxy
    normalize-mismatched-image-mime: true
    normalize-remote-image-mime: false
`)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := LoadConfigOptional(path, false)
	if err != nil {
		t.Fatalf("LoadConfigOptional() error = %v", err)
	}
	if got := cfg.Images.ChatGPTWeb.ResolvedUpstreamModel(); got != "gpt-5-5-custom" {
		t.Fatalf("ChatGPTWeb upstream model = %q, want gpt-5-5-custom", got)
	}
	if !cfg.Images.ChatGPTWeb.IgnoreUnsupportedParams {
		t.Fatal("IgnoreUnsupportedParams = false, want true")
	}
	resolved := cfg.Images.ChatGPTWeb.Resolved()
	if !resolved.RemoteImageURLEnabled {
		t.Fatal("RemoteImageURLEnabled = false, want true")
	}
	if resolved.RemoteImageURLDownloadMode != ChatGPTWebRemoteImageDownloadCredentialProxy {
		t.Fatalf("RemoteImageURLDownloadMode = %q", resolved.RemoteImageURLDownloadMode)
	}
	if !resolved.NormalizeMismatchedImageMIME {
		t.Fatal("NormalizeMismatchedImageMIME = false, want true")
	}
	if resolved.NormalizeRemoteImageMIME {
		t.Fatal("NormalizeRemoteImageMIME = true, want false")
	}
}

func TestLoadConfigOptionalDefaultsChatGPTWebImageUpstreamModel(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("images: {}\n"), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := LoadConfigOptional(path, false)
	if err != nil {
		t.Fatalf("LoadConfigOptional() error = %v", err)
	}
	if got := cfg.Images.ChatGPTWeb.ResolvedUpstreamModel(); got != DefaultChatGPTWebImageUpstreamModel {
		t.Fatalf("ChatGPTWeb upstream model = %q, want %q", got, DefaultChatGPTWebImageUpstreamModel)
	}
	if cfg.Images.ChatGPTWeb.IgnoreUnsupportedParams {
		t.Fatal("IgnoreUnsupportedParams = true, want false")
	}
	resolved := cfg.Images.ChatGPTWeb.Resolved()
	if resolved.RemoteImageURLEnabled {
		t.Fatal("RemoteImageURLEnabled = true, want false")
	}
	if resolved.RemoteImageURLDownloadMode != ChatGPTWebRemoteImageDownloadDirect {
		t.Fatalf("RemoteImageURLDownloadMode = %q", resolved.RemoteImageURLDownloadMode)
	}
	if resolved.NormalizeMismatchedImageMIME {
		t.Fatal("NormalizeMismatchedImageMIME = true, want false")
	}
	if !resolved.NormalizeRemoteImageMIME {
		t.Fatal("NormalizeRemoteImageMIME = false, want true")
	}
}

func TestLoadConfigOptionalChatGPTWebImageAspectSettings(t *testing.T) {
	responseBudget := 64
	path := filepath.Join(t.TempDir(), "config.yaml")
	data := []byte(`images:
  chatgpt-web:
    adapt-size-to-aspect-ratio: true
    strict-size: true
    aspect-ratio-max-error-percent: 0.5
    max-resize-edge-pixels: 2048
    resize-to-requested-size: true
    resize-filter: approx-bilinear
    max-image-response-megabytes: 64
    max-n: 2
    max-in-flight: 48
    admission-queue-size: 24
    admission-wait-milliseconds: 750
    max-finalizers: 6
    completion-reserve-megabytes: 2
    memory-capacity-megabytes: 768
    poll-concurrency: 96
    poll-stall-breaker-enabled: false
    poll-stall-seconds: 300
    memory-finalizer-concurrency: 4
`)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := LoadConfigOptional(path, false)
	if err != nil {
		t.Fatalf("LoadConfigOptional() error = %v", err)
	}
	resolved := cfg.Images.ChatGPTWeb.Resolved()
	if !resolved.AdaptSizeToAspectRatio || !resolved.StrictSize || resolved.AspectRatioMaxErrorPercent != 0.5 || resolved.MaxResizeEdgePixels != 2048 ||
		!resolved.ResizeToRequestedSize || resolved.ResizeFilter != ChatGPTWebResizeFilterApproxBiLinear ||
		resolved.MaxImageResponseMegabytes != responseBudget || resolved.MaxN != 2 ||
		resolved.MaxInFlight != 48 || resolved.AdmissionQueueSize != 24 || resolved.AdmissionWaitMilliseconds != 750 ||
		resolved.MaxFinalizers != 6 || resolved.CompletionReserveMegabytes != 2 || resolved.MemoryCapacityMegabytes != 768 ||
		resolved.PollConcurrency != 96 || resolved.PollStallBreakerEnabled || resolved.PollStallSeconds != 300 ||
		resolved.MemoryFinalizerConcurrency != 4 {
		t.Fatalf("resolved ChatGPT Web image config = %#v", resolved)
	}
}

func TestChatGPTWebImageAspectSettingsDefaultsAndValidation(t *testing.T) {
	resolved := (ChatGPTWebImageConfig{}).Resolved()
	if resolved.RemoteImageURLEnabled ||
		resolved.AdaptSizeToAspectRatio ||
		resolved.StrictSize ||
		resolved.AspectRatioMaxErrorPercent != DefaultChatGPTWebAspectRatioMaxErrorPercent ||
		resolved.MaxResizeEdgePixels != DefaultChatGPTWebMaxResizeEdgePixels ||
		resolved.ResizeToRequestedSize || resolved.ResizeFilter != DefaultChatGPTWebResizeFilter ||
		resolved.MaxImageResponseMegabytes != DefaultChatGPTWebMaxImageResponseMegabytes ||
		resolved.MaxN != DefaultChatGPTWebMaxN ||
		resolved.MaxInFlight != DefaultChatGPTWebImageMaxInFlight ||
		resolved.AdmissionQueueSize != DefaultChatGPTWebImageAdmissionQueueSize ||
		resolved.AdmissionWaitMilliseconds != DefaultChatGPTWebImageAdmissionWaitMS ||
		resolved.MaxFinalizers != DefaultChatGPTWebImageMaxFinalizers ||
		resolved.CompletionReserveMegabytes != DefaultChatGPTWebImageCompletionReserveMB ||
		resolved.MemoryCapacityMegabytes != DefaultChatGPTWebImageMemoryCapacityMB ||
		resolved.PollConcurrency != DefaultChatGPTWebImagePollConcurrency ||
		resolved.PollStallBreakerEnabled != DefaultChatGPTWebImagePollStallBreakerEnabled ||
		resolved.PollStallSeconds != DefaultChatGPTWebImagePollStallSeconds ||
		resolved.MemoryFinalizerConcurrency != DefaultChatGPTWebImageMemoryFinalizerConcurrency {
		t.Fatalf("resolved defaults = %#v", resolved)
	}

	negativeTolerance := -0.1
	tooLargeTolerance := MaxChatGPTWebAspectRatioMaxErrorPercent + 0.1
	nanTolerance := math.NaN()
	zeroEdge := 0
	tooLargeEdge := MaxChatGPTWebMaxResizeEdgePixels + 1
	zeroResponseBudget := 0
	tooLargeResponseBudget := MaxChatGPTWebMaxImageResponseMegabytes + 1
	zeroN := 0
	tooLargeN := MaxChatGPTWebMaxN + 1
	zeroInFlight := 0
	negativeQueue := -1
	negativeAdmissionWait := -1
	zeroFinalizers := 0
	negativeReserve := -1
	zeroMemoryCapacity := 0
	zeroPollConcurrency := 0
	tooShortPollStall := MinChatGPTWebImagePollStallSeconds - 1
	tooLongPollStall := MaxChatGPTWebImagePollStallSeconds + 1
	zeroMemoryFinalizers := 0
	aboveRecommendedInFlight := 5000
	aboveRecommendedQueue := 5000
	aboveRecommendedAdmissionWait := 30001
	aboveRecommendedFinalizers := 65
	aboveRecommendedReserve := 33
	aboveRecommendedMemory := 8193
	aboveRecommendedPoll := 600
	aboveRecommendedMemoryFinalizers := 65
	if err := (ChatGPTWebImageConfig{
		MaxInFlight:                &aboveRecommendedInFlight,
		AdmissionQueueSize:         &aboveRecommendedQueue,
		AdmissionWaitMilliseconds:  &aboveRecommendedAdmissionWait,
		MaxFinalizers:              &aboveRecommendedFinalizers,
		CompletionReserveMegabytes: &aboveRecommendedReserve,
		MemoryCapacityMegabytes:    &aboveRecommendedMemory,
		PollConcurrency:            &aboveRecommendedPoll,
		MemoryFinalizerConcurrency: &aboveRecommendedMemoryFinalizers,
	}).Validate(); err != nil {
		t.Fatalf("values above the operational recommendations should be configurable: %v", err)
	}
	tests := []struct {
		name string
		cfg  ChatGPTWebImageConfig
	}{
		{name: "negative tolerance", cfg: ChatGPTWebImageConfig{AspectRatioMaxErrorPercent: &negativeTolerance}},
		{name: "large tolerance", cfg: ChatGPTWebImageConfig{AspectRatioMaxErrorPercent: &tooLargeTolerance}},
		{name: "NaN tolerance", cfg: ChatGPTWebImageConfig{AspectRatioMaxErrorPercent: &nanTolerance}},
		{name: "zero edge", cfg: ChatGPTWebImageConfig{MaxResizeEdgePixels: &zeroEdge}},
		{name: "large edge", cfg: ChatGPTWebImageConfig{MaxResizeEdgePixels: &tooLargeEdge}},
		{name: "resize without adaptation", cfg: ChatGPTWebImageConfig{ResizeToRequestedSize: true}},
		{name: "strict size without adaptation", cfg: ChatGPTWebImageConfig{StrictSize: true}},
		{name: "unknown filter", cfg: ChatGPTWebImageConfig{ResizeFilter: "nearest"}},
		{name: "unknown remote download mode", cfg: ChatGPTWebImageConfig{RemoteImageURLDownloadMode: "environment-proxy"}},
		{name: "zero response budget", cfg: ChatGPTWebImageConfig{MaxImageResponseMegabytes: &zeroResponseBudget}},
		{name: "large response budget", cfg: ChatGPTWebImageConfig{MaxImageResponseMegabytes: &tooLargeResponseBudget}},
		{name: "zero max n", cfg: ChatGPTWebImageConfig{MaxN: &zeroN}},
		{name: "large max n", cfg: ChatGPTWebImageConfig{MaxN: &tooLargeN}},
		{name: "zero in flight", cfg: ChatGPTWebImageConfig{MaxInFlight: &zeroInFlight}},
		{name: "negative queue", cfg: ChatGPTWebImageConfig{AdmissionQueueSize: &negativeQueue}},
		{name: "negative admission wait", cfg: ChatGPTWebImageConfig{AdmissionWaitMilliseconds: &negativeAdmissionWait}},
		{name: "zero finalizers", cfg: ChatGPTWebImageConfig{MaxFinalizers: &zeroFinalizers}},
		{name: "negative reserve", cfg: ChatGPTWebImageConfig{CompletionReserveMegabytes: &negativeReserve}},
		{name: "zero memory capacity", cfg: ChatGPTWebImageConfig{MemoryCapacityMegabytes: &zeroMemoryCapacity}},
		{name: "zero poll concurrency", cfg: ChatGPTWebImageConfig{PollConcurrency: &zeroPollConcurrency}},
		{name: "short poll stall", cfg: ChatGPTWebImageConfig{PollStallSeconds: &tooShortPollStall}},
		{name: "long poll stall", cfg: ChatGPTWebImageConfig{PollStallSeconds: &tooLongPollStall}},
		{name: "zero memory finalizers", cfg: ChatGPTWebImageConfig{MemoryFinalizerConcurrency: &zeroMemoryFinalizers}},
	}
	overflowInFlight := math.MaxInt/2 + 1
	tests = append(tests, struct {
		name string
		cfg  ChatGPTWebImageConfig
	}{name: "poll queue derivation overflow", cfg: ChatGPTWebImageConfig{MaxInFlight: &overflowInFlight}})
	if overflowWait := int64(math.MaxInt64/int64(time.Millisecond)) + 1; overflowWait <= int64(math.MaxInt) {
		value := int(overflowWait)
		tests = append(tests, struct {
			name string
			cfg  ChatGPTWebImageConfig
		}{name: "admission wait duration overflow", cfg: ChatGPTWebImageConfig{AdmissionWaitMilliseconds: &value}})
	}
	if overflowMegabytes := int64(math.MaxInt64/(1<<20)) + 1; overflowMegabytes <= int64(math.MaxInt) {
		value := int(overflowMegabytes)
		tests = append(tests, struct {
			name string
			cfg  ChatGPTWebImageConfig
		}{name: "memory bytes overflow", cfg: ChatGPTWebImageConfig{MemoryCapacityMegabytes: &value}}, struct {
			name string
			cfg  ChatGPTWebImageConfig
		}{name: "completion reserve bytes overflow", cfg: ChatGPTWebImageConfig{CompletionReserveMegabytes: &value}})
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := test.cfg.Validate(); err == nil {
				t.Fatal("Validate() error = nil, want validation error")
			}
		})
	}
}

func TestChatGPTWebImageResizeSettingsAcceptBothFilters(t *testing.T) {
	for _, filter := range []string{ChatGPTWebResizeFilterCatmullRom, ChatGPTWebResizeFilterApproxBiLinear} {
		t.Run(filter, func(t *testing.T) {
			cfg := ChatGPTWebImageConfig{AdaptSizeToAspectRatio: true, ResizeToRequestedSize: true, ResizeFilter: filter}
			if err := cfg.Validate(); err != nil {
				t.Fatalf("Validate() error = %v", err)
			}
		})
	}
}

func TestLoadConfigOptionalRejectsInvalidChatGPTWebImageAspectSettings(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{name: "ratio tolerance", body: "aspect-ratio-max-error-percent: 11"},
		{name: "resize dependency", body: "resize-to-requested-size: true"},
		{name: "strict size dependency", body: "strict-size: true"},
		{name: "resize filter", body: "resize-filter: nearest"},
		{name: "response budget", body: "max-image-response-megabytes: 257"},
		{name: "max n", body: "max-n: 11"},
		{name: "max in flight", body: "max-in-flight: 0"},
		{name: "admission queue", body: "admission-queue-size: -1"},
		{name: "admission wait", body: "admission-wait-milliseconds: -1"},
		{name: "max finalizers", body: "max-finalizers: 0"},
		{name: "completion reserve", body: "completion-reserve-megabytes: -1"},
		{name: "memory capacity", body: "memory-capacity-megabytes: 0"},
		{name: "poll concurrency", body: "poll-concurrency: 0"},
		{name: "short poll stall", body: "poll-stall-seconds: 29"},
		{name: "long poll stall", body: "poll-stall-seconds: 3601"},
		{name: "memory finalizer concurrency", body: "memory-finalizer-concurrency: 0"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.yaml")
			data := []byte("images:\n  chatgpt-web:\n    " + test.body + "\n")
			if err := os.WriteFile(path, data, 0o600); err != nil {
				t.Fatalf("write config: %v", err)
			}
			if _, err := LoadConfigOptional(path, false); err == nil {
				t.Fatal("LoadConfigOptional() error = nil, want validation error")
			}
		})
	}
}

func TestLoadConfigOptionalDefaultsImagesStreamFlushEnabled(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	data := []byte(`images:
  stream-flush-interval-ms: 20
`)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := LoadConfigOptional(path, false)
	if err != nil {
		t.Fatalf("LoadConfigOptional() error = %v", err)
	}
	if cfg.Images.EnableStreamFlush == nil || !*cfg.Images.EnableStreamFlush {
		t.Fatalf("EnableStreamFlush = %v, want true", cfg.Images.EnableStreamFlush)
	}
}

func TestLoadConfigOptionalDefaultsNativeImagesDisabled(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	data := []byte(`images:
  image-model: gpt-image-2
`)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := LoadConfigOptional(path, false)
	if err != nil {
		t.Fatalf("LoadConfigOptional() error = %v", err)
	}
	if cfg.Images.Native.Generations.Enabled {
		t.Fatal("Native.Generations.Enabled = true, want false")
	}
	if cfg.Images.Native.Edits.Enabled {
		t.Fatal("Native.Edits.Enabled = true, want false")
	}
	if got := cfg.Images.Native.Generations.Models; len(got) != 2 || got[0] != "gpt-image-2" || got[1] != "gpt-image-1.5" {
		t.Fatalf("native generation models = %#v", got)
	}
	if cfg.Images.Native.Generations.UnsupportedModelStatusCode != 400 {
		t.Fatalf("native generation status = %d, want 400", cfg.Images.Native.Generations.UnsupportedModelStatusCode)
	}
	if cfg.Images.Native.Edits.UnsupportedModelMessage != "Native image edit is not enabled for model {model}" {
		t.Fatalf("native edit message = %q", cfg.Images.Native.Edits.UnsupportedModelMessage)
	}
}

func TestLoadConfigOptionalNativeImagesSettings(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	data := []byte(`images:
  native:
    generations:
      enabled: true
      models: ["gpt-image-2", "gpt-image-2", " GPT-IMAGE-2 ", "gpt-image-1.5"]
      param-rules: ["n", " n ", "background=transparent"]
      unsupported-model-status-code: 409
      unsupported-model-message: "no native generation for {model}"
    edits:
      enabled: true
      models: []
      param-rules: ["mask=null"]
      unsupported-model-status-code: 99
      unsupported-model-message: ""
`)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := LoadConfigOptional(path, false)
	if err != nil {
		t.Fatalf("LoadConfigOptional() error = %v", err)
	}
	gen := cfg.Images.Native.Generations
	if !gen.Enabled {
		t.Fatal("generation native enabled = false, want true")
	}
	if got := gen.Models; len(got) != 2 || got[0] != "gpt-image-2" || got[1] != "gpt-image-1.5" {
		t.Fatalf("generation models = %#v", got)
	}
	if got := gen.ParamRules; len(got) != 2 || got[0] != "n" || got[1] != "background=transparent" {
		t.Fatalf("generation param rules = %#v", got)
	}
	if gen.UnsupportedModelStatusCode != 409 {
		t.Fatalf("generation status = %d, want 409", gen.UnsupportedModelStatusCode)
	}
	if gen.UnsupportedModelMessage != "no native generation for {model}" {
		t.Fatalf("generation message = %q", gen.UnsupportedModelMessage)
	}
	edit := cfg.Images.Native.Edits
	if !edit.Enabled {
		t.Fatal("edit native enabled = false, want true")
	}
	if got := edit.Models; len(got) != 2 || got[0] != "gpt-image-2" || got[1] != "gpt-image-1.5" {
		t.Fatalf("edit default models = %#v", got)
	}
	if edit.UnsupportedModelStatusCode != 400 {
		t.Fatalf("edit status = %d, want 400", edit.UnsupportedModelStatusCode)
	}
	if edit.UnsupportedModelMessage != "Native image edit is not enabled for model {model}" {
		t.Fatalf("edit message = %q", edit.UnsupportedModelMessage)
	}
}

func TestLoadConfigOptionalClampsNegativeImagesStreamFlushSettings(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	data := []byte(`images:
  stream-flush-interval-ms: -1
  stream-flush-min-bytes: -1
`)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := LoadConfigOptional(path, false)
	if err != nil {
		t.Fatalf("LoadConfigOptional() error = %v", err)
	}
	if cfg.Images.StreamFlushIntervalMS != 0 {
		t.Fatalf("StreamFlushIntervalMS = %d, want 0", cfg.Images.StreamFlushIntervalMS)
	}
	if cfg.Images.StreamFlushMinBytes != 0 {
		t.Fatalf("StreamFlushMinBytes = %d, want 0", cfg.Images.StreamFlushMinBytes)
	}
}

func TestLoadConfigOptionalStreamingFlushSettings(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	data := []byte(`streaming:
  enable-stream-flush: true
  stream-flush-interval-ms: 25
  stream-flush-min-bytes: 32768
  trust-upstream-sse: true
`)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := LoadConfigOptional(path, false)
	if err != nil {
		t.Fatalf("LoadConfigOptional() error = %v", err)
	}
	if !cfg.Streaming.EnableStreamFlush {
		t.Fatalf("EnableStreamFlush = false, want true")
	}
	if cfg.Streaming.StreamFlushIntervalMS != 25 {
		t.Fatalf("StreamFlushIntervalMS = %d, want 25", cfg.Streaming.StreamFlushIntervalMS)
	}
	if cfg.Streaming.StreamFlushMinBytes != 32768 {
		t.Fatalf("StreamFlushMinBytes = %d, want 32768", cfg.Streaming.StreamFlushMinBytes)
	}
	if !cfg.Streaming.TrustUpstreamSSE {
		t.Fatalf("TrustUpstreamSSE = false, want true")
	}
}

func TestLoadConfigOptionalClampsNegativeStreamingFlushSettings(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	data := []byte(`streaming:
  stream-flush-interval-ms: -1
  stream-flush-min-bytes: -1
`)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := LoadConfigOptional(path, false)
	if err != nil {
		t.Fatalf("LoadConfigOptional() error = %v", err)
	}
	if cfg.Streaming.StreamFlushIntervalMS != 0 {
		t.Fatalf("StreamFlushIntervalMS = %d, want 0", cfg.Streaming.StreamFlushIntervalMS)
	}
	if cfg.Streaming.StreamFlushMinBytes != 0 {
		t.Fatalf("StreamFlushMinBytes = %d, want 0", cfg.Streaming.StreamFlushMinBytes)
	}
}
