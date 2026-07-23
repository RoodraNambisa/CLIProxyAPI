package helps

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestChatGPTWebUsageProjectionPrecomputesSmallInput(t *testing.T) {
	cache := NewChatGPTWebUsageCache()
	t.Cleanup(cache.Close)
	projection, err := cache.NewProjection("gpt-5.4", chatGPTWebUsageTestRequest("describe this"), ChatGPTWebUsageCacheOptions{
		Enabled:            true,
		DiskThresholdBytes: 1 << 20,
		MaxDiskBytes:       8 << 20,
		AutoOutputQuality:  "medium",
	})
	if err != nil {
		t.Fatalf("NewProjection() error = %v", err)
	}
	projection.AddImage(ChatGPTWebUsageImage{Model: "gpt-5.4", Detail: "high", Width: 1024, Height: 1024})
	if estimateErrors := projection.PrecomputeInput(); len(estimateErrors) != 0 {
		t.Fatalf("PrecomputeInput() errors = %v", estimateErrors)
	}
	if !projection.inputPrecomputed || len(projection.records) != 0 || len(projection.images) != 0 {
		t.Fatalf("projection retained input after precompute: %#v", projection)
	}
	if snapshot := cache.Snapshot(); snapshot.ActiveMemoryBytes != 0 || snapshot.ActiveMemoryEntries != 1 {
		t.Fatalf("snapshot after precompute = %#v", snapshot)
	}
	usage, estimateErrors := projection.Estimate("finished", nil)
	if len(estimateErrors) != 0 {
		t.Fatalf("Estimate() errors = %v", estimateErrors)
	}
	if usage["input_tokens"].(int64) <= 0 || usage["output_tokens"].(int64) <= 0 {
		t.Fatalf("usage = %#v", usage)
	}
	projection.Complete()
	if snapshot := cache.Snapshot(); snapshot.SuccessfulCalculations != 1 || snapshot.ActiveMemoryEntries != 0 {
		t.Fatalf("completed snapshot = %#v", snapshot)
	}
}

func TestChatGPTWebUsageProjectionSpillsLargeInputUntilSuccess(t *testing.T) {
	cache := NewChatGPTWebUsageCache()
	t.Cleanup(cache.Close)
	directory := t.TempDir()
	projection, err := cache.NewProjection("gpt-5.4", chatGPTWebUsageTestRequest("large input"), ChatGPTWebUsageCacheOptions{
		Enabled:            true,
		DiskThresholdBytes: 1,
		MaxDiskBytes:       8 << 20,
		Path:               directory,
	})
	if err != nil {
		t.Fatalf("NewProjection() error = %v", err)
	}
	if projection.filePath == "" || projection.inputPrecomputed {
		t.Fatalf("projection = %#v, want lazy disk projection", projection)
	}
	if estimateErrors := projection.PrecomputeInput(); len(estimateErrors) != 0 || projection.inputPrecomputed {
		t.Fatalf("disk PrecomputeInput() errors = %v, projection = %#v", estimateErrors, projection)
	}
	info, errStat := os.Stat(projection.filePath)
	if errStat != nil {
		t.Fatalf("stat projection file: %v", errStat)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("projection mode = %o, want 600", info.Mode().Perm())
	}
	path := projection.filePath
	usage, estimateErrors := projection.Estimate("finished", nil)
	if len(estimateErrors) != 0 || usage["input_tokens"].(int64) <= 0 {
		t.Fatalf("Estimate() usage = %#v, errors = %v", usage, estimateErrors)
	}
	projection.Complete()
	if _, errStat = os.Stat(path); !errors.Is(errStat, os.ErrNotExist) {
		t.Fatalf("projection file still exists: %v", errStat)
	}
	if matches, errGlob := filepath.Glob(filepath.Join(directory, "usage-*.bin")); errGlob != nil || len(matches) != 0 {
		t.Fatalf("remaining projection files = %v, error = %v", matches, errGlob)
	}
}

func TestChatGPTWebUsageProjectionDisabledCacheStaysLazyInMemory(t *testing.T) {
	cache := NewChatGPTWebUsageCache()
	t.Cleanup(cache.Close)
	projection, err := cache.NewProjection("gpt-5.4", chatGPTWebUsageTestRequest("lazy input"), ChatGPTWebUsageCacheOptions{
		Enabled:            false,
		DiskThresholdBytes: 1,
		MaxDiskBytes:       1,
	})
	if err != nil {
		t.Fatalf("NewProjection() error = %v", err)
	}
	if estimateErrors := projection.PrecomputeInput(); len(estimateErrors) != 0 || projection.inputPrecomputed {
		t.Fatalf("PrecomputeInput() errors = %v, projection = %#v", estimateErrors, projection)
	}
	projection.Discard()
	if snapshot := cache.Snapshot(); snapshot.FailedDiscards != 1 || snapshot.ActiveMemoryEntries != 0 {
		t.Fatalf("discard snapshot = %#v", snapshot)
	}
}

func TestChatGPTWebUsageProjectionRejectsDiskCapacityBeforeUpstream(t *testing.T) {
	cache := NewChatGPTWebUsageCache()
	t.Cleanup(cache.Close)
	_, err := cache.NewProjection("gpt-5.4", chatGPTWebUsageTestRequest("too large"), ChatGPTWebUsageCacheOptions{
		Enabled:            true,
		DiskThresholdBytes: 1,
		MaxDiskBytes:       1,
		Path:               t.TempDir(),
	})
	var cacheErr *ChatGPTWebUsageCacheError
	if !errors.As(err, &cacheErr) || cacheErr.Code != "chatgpt_web_usage_cache_full" {
		t.Fatalf("NewProjection() error = %v", err)
	}
}

func TestChatGPTWebUsageProjectionDoesNotRetainImagePayload(t *testing.T) {
	cache := NewChatGPTWebUsageCache()
	t.Cleanup(cache.Close)
	withImage := chatGPTWebUsageTestRequest("describe this")
	withImage.Messages[0].Parts[0].ImageURL = "data:image/png;base64,SECRET_IMAGE_BYTES"
	withoutImage := chatGPTWebUsageTestRequest("describe this")

	first, errFirst := cache.NewProjection("gpt-5.4", withImage, ChatGPTWebUsageCacheOptions{})
	second, errSecond := cache.NewProjection("gpt-5.4", withoutImage, ChatGPTWebUsageCacheOptions{})
	if errFirst != nil || errSecond != nil {
		t.Fatalf("NewProjection() errors = %v, %v", errFirst, errSecond)
	}
	firstUsage, firstErrors := first.Estimate("", nil)
	secondUsage, secondErrors := second.Estimate("", nil)
	if len(firstErrors) != 0 || len(secondErrors) != 0 {
		t.Fatalf("Estimate() errors = %v, %v", firstErrors, secondErrors)
	}
	if firstUsage["input_tokens"] != secondUsage["input_tokens"] {
		t.Fatalf("image payload changed text usage: with=%#v without=%#v", firstUsage, secondUsage)
	}
	first.Discard()
	second.Discard()
}

func TestChatGPTWebImageOutputTokenCount(t *testing.T) {
	for _, test := range []struct {
		quality string
		want    int64
	}{
		{quality: "low", want: 196},
		{quality: "medium", want: 1756},
		{quality: "high", want: 7024},
	} {
		got, err := ChatGPTWebImageOutputTokenCount("gpt-image-2", test.quality, "medium", 1024, 1024)
		if err != nil || got != test.want {
			t.Fatalf("quality %s: got %d, error %v, want %d", test.quality, got, err, test.want)
		}
	}
}

func TestChatGPTWebImageProjectionSeparatesResponseAndToolUsage(t *testing.T) {
	cache := NewChatGPTWebUsageCache()
	t.Cleanup(cache.Close)
	request := chatGPTWebUsageTestRequest("draw a tiger")
	request.Image = &ChatGPTWebImageRequest{Model: "gpt-image-2", Prompt: "draw a tiger"}
	projection, err := cache.NewProjection("gpt-5.4", request, ChatGPTWebUsageCacheOptions{AutoOutputQuality: "high"})
	if err != nil {
		t.Fatalf("NewProjection() error = %v", err)
	}
	usage, estimateErrors := projection.Estimate("", []ChatGPTWebUsageImage{{
		Model: "gpt-image-2", Quality: "auto", Width: 1024, Height: 1024,
	}})
	if len(estimateErrors) != 0 {
		t.Fatalf("Estimate() errors = %v", estimateErrors)
	}
	if usage["output_tokens"].(int64) != 0 {
		t.Fatalf("response usage contains image output tokens: %#v", usage)
	}
	toolUsage := usage["tool_usage"].(map[string]any)["image_gen"].(map[string]any)
	if toolUsage["output_tokens"].(int64) != 7024 ||
		toolUsage["output_tokens_details"].(map[string]any)["image_tokens"].(int64) != 7024 {
		t.Fatalf("tool usage = %#v", toolUsage)
	}
	projection.Complete()
}

func chatGPTWebUsageTestRequest(text string) ChatGPTWebRequest {
	return ChatGPTWebRequest{Messages: []ChatGPTWebMessage{{
		Role:  "user",
		Parts: []ChatGPTWebContentPart{{Text: text}},
	}}}
}
