package config

import "testing"

func TestChatGPTWebImageAdmissionBoundsAreReexported(t *testing.T) {
	checks := []struct {
		name                    string
		minimum, value, maximum int
	}{
		{name: "max in flight", minimum: MinChatGPTWebImageMaxInFlight, value: DefaultChatGPTWebImageMaxInFlight, maximum: MaxChatGPTWebImageMaxInFlight},
		{name: "admission queue", minimum: MinChatGPTWebImageAdmissionQueueSize, value: DefaultChatGPTWebImageAdmissionQueueSize, maximum: MaxChatGPTWebImageAdmissionQueueSize},
		{name: "admission wait", minimum: MinChatGPTWebImageAdmissionWaitMS, value: DefaultChatGPTWebImageAdmissionWaitMS, maximum: MaxChatGPTWebImageAdmissionWaitMS},
		{name: "max finalizers", minimum: MinChatGPTWebImageMaxFinalizers, value: DefaultChatGPTWebImageMaxFinalizers, maximum: MaxChatGPTWebImageMaxFinalizers},
		{name: "completion reserve", minimum: MinChatGPTWebImageCompletionReserveMB, value: DefaultChatGPTWebImageCompletionReserveMB, maximum: MaxChatGPTWebImageCompletionReserveMB},
		{name: "memory capacity", minimum: MinChatGPTWebImageMemoryCapacityMB, value: DefaultChatGPTWebImageMemoryCapacityMB, maximum: MaxChatGPTWebImageMemoryCapacityMB},
	}
	for _, check := range checks {
		if check.minimum > check.value || check.value > check.maximum {
			t.Fatalf("%s bounds = %d <= %d <= %d", check.name, check.minimum, check.value, check.maximum)
		}
	}
}
