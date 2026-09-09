package signature

import (
	"bytes"
	"encoding/base64"
	"strings"
	"testing"

	"google.golang.org/protobuf/encoding/protowire"
)

func caisTestVarint(body []byte, field protowire.Number, value uint64) []byte {
	body = protowire.AppendTag(body, field, protowire.VarintType)
	return protowire.AppendVarint(body, value)
}

func caisTestBytes(body []byte, field protowire.Number, value []byte) []byte {
	body = protowire.AppendTag(body, field, protowire.BytesType)
	return protowire.AppendBytes(body, value)
}

func caisTestChannel(channel uint64, signature []byte, model string) []byte {
	body := caisTestVarint(nil, 1, channel)
	body = caisTestBytes(body, 5, signature)
	return caisTestBytes(body, 6, []byte(model))
}

func caisTestWrap(channel []byte, version uint64) string {
	container := caisTestBytes(nil, 1, channel)
	body := caisTestVarint(nil, 1, version)
	body = caisTestBytes(body, 2, container)
	body = caisTestVarint(body, 3, 1)
	return base64.StdEncoding.EncodeToString(body)
}

func TestClaudeCAISRecognizesStructureWithoutHardcodingObservedValues(t *testing.T) {
	for _, version := range []uint64{0, 2, 9} {
		for _, channelID := range []uint64{0, 16, 99} {
			channel := caisTestChannel(channelID, []byte{1}, "claude-future")
			channel = caisTestVarint(channel, 3, 12)
			channel = caisTestVarint(channel, 7, 2)
			channel = caisTestBytes(channel, 8, []byte("future-kind"))
			channel = caisTestBytes(channel, 11, []byte("ABCDEF12-1234-5678-9ABC-DEF012345678"))
			channel = caisTestBytes(channel, 30, []byte("unknown-extension"))
			sig := caisTestWrap(channel, version)
			info, err := InspectClaudeCAISSignature(sig)
			if err != nil || info.FirstByte != 8 || info.EnvelopeVersion != version || info.ChannelID != channelID ||
				info.ModelText != "claude-future" || info.BlockKind != "future-kind" || info.ContextID != "ABCDEF12-1234-5678-9ABC-DEF012345678" || info.SignatureLen != 1 {
				t.Fatal("CAIS structure or optional fields were rejected")
			}
		}
	}
}

func TestClaudeCAISRejectsInvalidWireShapesWithoutEchoingContent(t *testing.T) {
	valid := caisTestChannel(16, bytes.Repeat([]byte{1}, 64), "claude-opus-5")
	for _, channel := range [][]byte{
		{},
		caisTestBytes(caisTestBytes(nil, 5, []byte{1}), 6, []byte("claude-test")),
		caisTestBytes(caisTestVarint(nil, 1, 16), 6, []byte("claude-test")),
		caisTestBytes(caisTestVarint(nil, 1, 16), 5, []byte{1}),
		caisTestChannel(16, nil, "claude-test"),
		caisTestChannel(16, []byte{1}, "private-model-value"),
		caisTestChannel(16, []byte{1}, "claude-\xff"),
		caisTestBytes(bytes.Clone(valid), 11, []byte("private-context-value")),
		caisTestVarint(bytes.Clone(valid), 8, 1),
		caisTestBytes(bytes.Clone(valid), 3, []byte{1}),
		caisTestBytes(bytes.Clone(valid), 7, []byte{1}),
		caisTestBytes(bytes.Clone(valid), 1, []byte{1}),
	} {
		_, err := InspectClaudeCAISSignature(caisTestWrap(channel, 2))
		if err == nil {
			t.Fatal("malformed CAIS channel was accepted")
		}
		if strings.Contains(err.Error(), "private-model-value") || strings.Contains(err.Error(), "private-context-value") {
			t.Fatal("CAIS validation exposed opaque field content")
		}
	}
	for _, body := range [][]byte{
		{},
		{8, 128},
		caisTestVarint(nil, 1, 2),
		caisTestVarint(caisTestVarint(nil, 1, 2), 2, 1),
		caisTestBytes(caisTestVarint(nil, 1, 2), 2, []byte{10, 255}),
		caisTestBytes(caisTestVarint(nil, 1, 2), 2, caisTestBytes(nil, 2, valid)),
	} {
		if IsValidClaudeCAISSignature(base64.StdEncoding.EncodeToString(body)) {
			t.Fatal("malformed CAIS envelope was accepted")
		}
	}
	for _, raw := range []string{"", "C!", "CAIS", "C" + strings.Repeat("A", MaxClaudeThinkingSignatureLen)} {
		if IsValidClaudeCAISSignature(raw) {
			t.Fatal("invalid encoding or oversized CAIS signature was accepted")
		}
	}
}

func TestClaudeCAISProviderCompatibilityPreservesNativeAndRejectsForeign(t *testing.T) {
	sig := caisTestWrap(caisTestChannel(16, bytes.Repeat([]byte{1}, 64), "claude-opus-5"), 2)
	for _, prefix := range []string{"", "claude#", "anthropic#", "cais#", "claude-cais#", "claude_cais#", "ccmax#", "claude-code-max#", "claude_code_max#"} {
		raw := prefix + sig
		if DetectSignatureProvider(raw) != SignatureProviderClaude {
			t.Fatal("recognized CAIS prefix lost its provider")
		}
		value, ok := CompatibleSignatureForProvider(SignatureProviderClaude, raw)
		if !ok || value != sig {
			t.Fatal("CAIS signature was re-encoded or not normalized to its native form")
		}
		if _, ok := CompatibleAntigravityClaudeThinkingSignature(raw); ok {
			t.Fatal("CAIS signature entered the Antigravity R-only replay path")
		}
		for _, target := range []SignatureProvider{SignatureProviderGemini, SignatureProviderGPT} {
			if _, ok := CompatibleSignatureForProvider(target, raw); ok {
				t.Fatal("CAIS signature was replayed to a foreign provider")
			}
		}
	}
	for _, prefix := range []string{"gemini#", "gpt#", "unrecognized#"} {
		if DetectSignatureProvider(prefix+sig) != SignatureProviderUnknown {
			t.Fatal("CAIS bypassed explicit prefix isolation")
		}
	}
	if IsValidGeminiThoughtSignature(sig, GeminiThoughtSignatureValidationOptions{RequireKnownEnvelope: true}) {
		t.Fatal("Gemini envelope validation accepted a CAIS envelope")
	}
	for _, tc := range []struct {
		signature string
		provider  SignatureProvider
	}{
		{testClaudeThinkingSignature(), SignatureProviderClaude},
		{testGPTReasoningSignature(), SignatureProviderGPT},
		{testGemini3ThoughtSignature([]byte{1, 12, 57}), SignatureProviderGemini},
	} {
		if DetectSignatureProvider(tc.signature) != tc.provider || IsValidClaudeCAISSignature(tc.signature) {
			t.Fatal("CAIS recognition changed an existing envelope")
		}
	}
}
