package signature

import (
	"encoding/base64"
	"fmt"
	"strings"
	"unicode/utf8"

	"google.golang.org/protobuf/encoding/protowire"
)

// claudeCAISSignatureMarker is the decoded first byte identifying the CAIS
// envelope (protobuf tag for top-level field 1, varint).
const claudeCAISSignatureMarker = 0x08

// claudeCAISModelTextPrefix is the model_text prefix that distinguishes a CAIS
// channel block from an arbitrary protobuf payload.
const claudeCAISModelTextPrefix = "claude-"

// ClaudeCAISSignatureInfo describes the locally inspected structure of a Claude
// CAIS thinking signature.
type ClaudeCAISSignatureInfo struct {
	FirstByte       byte
	EnvelopeVersion uint64
	ChannelID       uint64
	ModelText       string
	BlockKind       string
	ContextID       string

	SignatureLen int
}

// IsValidClaudeCAISSignature returns whether rawSignature is a valid Claude CAIS
// thinking signature.
func IsValidClaudeCAISSignature(rawSignature string) bool {
	_, err := InspectClaudeCAISSignature(rawSignature)
	return err == nil
}

// InspectClaudeCAISSignature decodes and validates a Claude CAIS thinking
// envelope by structure. The upstream remains responsible for authenticating
// the opaque signature; observed version and channel values are not hardcoded.
func InspectClaudeCAISSignature(rawSignature string) (*ClaudeCAISSignatureInfo, error) {
	sig := stripClaudeSignaturePrefix(rawSignature)
	if sig == "" {
		return nil, fmt.Errorf("empty signature")
	}
	if len(sig) > MaxClaudeThinkingSignatureLen {
		return nil, fmt.Errorf("signature exceeds maximum length (%d bytes)", MaxClaudeThinkingSignatureLen)
	}
	// A payload whose first byte is 0x08 always base64-encodes to a string
	// starting with 'C' (0x08>>2 == 2). Checking that first keeps this validator
	// cheap on the hot paths that probe every signature, since classic Claude
	// (E/R) and Gemini envelopes are rejected without a base64 decode.
	if sig[0] != 'C' {
		return nil, fmt.Errorf("invalid Claude CAIS signature: expected 'C' prefix")
	}

	decoded, err := base64.StdEncoding.DecodeString(sig)
	if err != nil {
		return nil, fmt.Errorf("invalid Claude CAIS signature: base64 decode failed: %w", err)
	}
	if len(decoded) == 0 {
		return nil, fmt.Errorf("invalid Claude CAIS signature: empty after decode")
	}
	if decoded[0] != claudeCAISSignatureMarker {
		return nil, fmt.Errorf("invalid Claude CAIS signature: expected first byte 0x%02x, got 0x%02x", claudeCAISSignatureMarker, decoded[0])
	}

	info := &ClaudeCAISSignatureInfo{FirstByte: decoded[0]}

	var container []byte
	err = walkClaudeProtobufFields(decoded, func(num protowire.Number, typ protowire.Type, raw []byte) error {
		switch num {
		case 1:
			value, errField := decodeClaudeCAISVarint(raw, typ, "CAIS top-level field 1 envelope version")
			if errField != nil {
				return errField
			}
			info.EnvelopeVersion = value
		case 2:
			value, errField := decodeClaudeCAISBytes(raw, typ, "CAIS top-level field 2 container")
			if errField != nil {
				return errField
			}
			container = value
		case 3:
			if _, errField := decodeClaudeCAISVarint(raw, typ, "CAIS top-level field 3 trailer"); errField != nil {
				return errField
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if container == nil {
		return nil, fmt.Errorf("invalid Claude CAIS signature: missing top-level field 2 container")
	}

	var channelBlock []byte
	err = walkClaudeProtobufFields(container, func(num protowire.Number, typ protowire.Type, raw []byte) error {
		if num != 1 {
			return nil
		}
		value, errField := decodeClaudeCAISBytes(raw, typ, "CAIS container field 1 channel block")
		if errField != nil {
			return errField
		}
		channelBlock = value
		return nil
	})
	if err != nil {
		return nil, err
	}
	if channelBlock == nil {
		return nil, fmt.Errorf("invalid Claude CAIS signature: missing container field 1 channel block")
	}

	var haveChannelID, haveSignatureBytes, haveModelText bool
	err = walkClaudeProtobufFields(channelBlock, func(num protowire.Number, typ protowire.Type, raw []byte) error {
		switch num {
		case 1:
			value, errField := decodeClaudeCAISVarint(raw, typ, "CAIS channel field 1 channel_id")
			if errField != nil {
				return errField
			}
			info.ChannelID = value
			haveChannelID = true
		case 3:
			if _, errField := decodeClaudeCAISVarint(raw, typ, "CAIS channel field 3 version"); errField != nil {
				return errField
			}
		case 5:
			value, errField := decodeClaudeCAISBytes(raw, typ, "CAIS channel field 5 signature bytes")
			if errField != nil {
				return errField
			}
			if len(value) == 0 {
				return fmt.Errorf("invalid Claude CAIS signature: channel field 5 signature bytes must not be empty")
			}
			info.SignatureLen = len(value)
			haveSignatureBytes = true
		case 6:
			value, errField := decodeClaudeCAISUTF8(raw, typ, "CAIS channel field 6 model_text")
			if errField != nil {
				return errField
			}
			if !strings.HasPrefix(value, claudeCAISModelTextPrefix) {
				return fmt.Errorf("invalid Claude CAIS signature: channel field 6 model_text must start with %q", claudeCAISModelTextPrefix)
			}
			info.ModelText = value
			haveModelText = true
		case 7:
			if _, errField := decodeClaudeCAISVarint(raw, typ, "CAIS channel field 7"); errField != nil {
				return errField
			}
		case 8:
			value, errField := decodeClaudeCAISUTF8(raw, typ, "CAIS channel field 8 block kind")
			if errField != nil {
				return errField
			}
			info.BlockKind = value
		case 11:
			value, errField := decodeClaudeCAISUTF8(raw, typ, "CAIS channel field 11 context id")
			if errField != nil {
				return errField
			}
			if !isCanonicalUUID(value) {
				return fmt.Errorf("invalid Claude CAIS signature: channel field 11 context id must be a canonical UUID")
			}
			info.ContextID = value
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	switch {
	case !haveChannelID:
		return nil, fmt.Errorf("invalid Claude CAIS signature: missing channel field 1 channel_id")
	case !haveSignatureBytes:
		return nil, fmt.Errorf("invalid Claude CAIS signature: missing channel field 5 signature bytes")
	case !haveModelText:
		return nil, fmt.Errorf("invalid Claude CAIS signature: missing channel field 6 model_text")
	}

	return info, nil
}

func decodeClaudeCAISVarint(raw []byte, typ protowire.Type, label string) (uint64, error) {
	if typ != protowire.VarintType {
		return 0, fmt.Errorf("invalid Claude CAIS signature: %s must be varint", label)
	}
	return decodeClaudeVarintField(raw, label)
}

func decodeClaudeCAISBytes(raw []byte, typ protowire.Type, label string) ([]byte, error) {
	if typ != protowire.BytesType {
		return nil, fmt.Errorf("invalid Claude CAIS signature: %s must be bytes", label)
	}
	return decodeClaudeBytesField(raw, label)
}

func decodeClaudeCAISUTF8(raw []byte, typ protowire.Type, label string) (string, error) {
	value, err := decodeClaudeCAISBytes(raw, typ, label)
	if err != nil {
		return "", err
	}
	if !utf8.Valid(value) {
		return "", fmt.Errorf("invalid Claude CAIS signature: %s must be valid UTF-8", label)
	}
	return string(value), nil
}

func isCanonicalUUID(s string) bool {
	if len(s) != 36 {
		return false
	}
	for i := 0; i < len(s); i++ {
		b := s[i]
		switch i {
		case 8, 13, 18, 23:
			if b != '-' {
				return false
			}
		default:
			if !((b >= '0' && b <= '9') || (b >= 'a' && b <= 'f') || (b >= 'A' && b <= 'F')) {
				return false
			}
		}
	}
	return true
}
