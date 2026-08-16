package codex

import "testing"

func TestEffectiveFingerprintModeRequiresExplicitValidMode(t *testing.T) {
	tests := []struct {
		name     string
		metadata map[string]any
		want     FingerprintMode
	}{
		{name: "missing", metadata: nil, want: FingerprintModeOff},
		{name: "invalid", metadata: map[string]any{FingerprintModeMetadataKey: "aggressive"}, want: FingerprintModeOff},
		{name: "normalized", metadata: map[string]any{FingerprintModeMetadataKey: " Session "}, want: FingerprintModeSession},
		{name: "full", metadata: map[string]any{FingerprintModeMetadataKey: "full"}, want: FingerprintModeFull},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := EffectiveFingerprintMode(tt.metadata); got != tt.want {
				t.Fatalf("EffectiveFingerprintMode() = %q, want %q", got, tt.want)
			}
		})
	}
}
