package config

import "testing"

func TestChatGPTWebImageAdmissionRecommendationsAreReexported(t *testing.T) {
	checks := []struct {
		name                    string
		minimum, value, maximum int
	}{
		{name: "max in flight", minimum: MinChatGPTWebImageMaxInFlight, value: DefaultChatGPTWebImageMaxInFlight, maximum: RecommendedMaxChatGPTWebImageMaxInFlight},
		{name: "admission queue", minimum: MinChatGPTWebImageAdmissionQueueSize, value: DefaultChatGPTWebImageAdmissionQueueSize, maximum: RecommendedMaxChatGPTWebImageAdmissionQueueSize},
		{name: "admission wait", minimum: MinChatGPTWebImageAdmissionWaitMS, value: DefaultChatGPTWebImageAdmissionWaitMS, maximum: RecommendedMaxChatGPTWebImageAdmissionWaitMS},
		{name: "max finalizers", minimum: MinChatGPTWebImageMaxFinalizers, value: DefaultChatGPTWebImageMaxFinalizers, maximum: RecommendedMaxChatGPTWebImageMaxFinalizers},
		{name: "completion reserve", minimum: MinChatGPTWebImageCompletionReserveMB, value: DefaultChatGPTWebImageCompletionReserveMB, maximum: RecommendedMaxChatGPTWebImageCompletionReserveMB},
		{name: "memory capacity", minimum: RecommendedMinChatGPTWebImageMemoryCapacityMB, value: DefaultChatGPTWebImageMemoryCapacityMB, maximum: RecommendedMaxChatGPTWebImageMemoryCapacityMB},
		{name: "poll concurrency", minimum: MinChatGPTWebImagePollConcurrency, value: DefaultChatGPTWebImagePollConcurrency, maximum: RecommendedMaxChatGPTWebImagePollConcurrency},
		{name: "poll stall seconds", minimum: MinChatGPTWebImagePollStallSeconds, value: DefaultChatGPTWebImagePollStallSeconds, maximum: MaxChatGPTWebImagePollStallSeconds},
		{name: "memory finalizer concurrency", minimum: MinChatGPTWebImageMemoryFinalizerConcurrency, value: DefaultChatGPTWebImageMemoryFinalizerConcurrency, maximum: RecommendedMaxChatGPTWebImageMemoryFinalizerConcurrency},
	}
	for _, check := range checks {
		if check.minimum > check.value || check.value > check.maximum {
			t.Fatalf("%s bounds = %d <= %d <= %d", check.name, check.minimum, check.value, check.maximum)
		}
	}
	if MaxChatGPTWebImageMaxInFlight != RecommendedMaxChatGPTWebImageMaxInFlight ||
		MaxChatGPTWebImagePollConcurrency != RecommendedMaxChatGPTWebImagePollConcurrency {
		t.Fatal("legacy maximum aliases no longer match the published recommendations")
	}
}
