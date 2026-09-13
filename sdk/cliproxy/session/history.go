package session

import (
	"crypto/sha256"
	"strings"

	sdktranslator "github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	"github.com/tidwall/gjson"
)

const historyMaxTurns = 4096

// History is an immutable, bounded sequence of rolling conversation digests.
// The zero value is ineligible for shared-prefix inference.
type History struct {
	prefixes [][sha256.Size]byte
	minimum  int
}

// Usable reports whether a complete bounded history includes actual user input.
func (h History) Usable() bool { return h.minimum > 0 && h.minimum <= len(h.prefixes) }

// InitialUserPrefixDigest returns the already captured prefix through the first
// user input. Provider identity pools can keep this anchor as history grows,
// while routing still matches the longest prefix to distinguish branches.
func (h History) InitialUserPrefixDigest() ([sha256.Size]byte, bool) {
	if !h.Usable() {
		return [sha256.Size]byte{}, false
	}
	return h.prefixes[h.minimum-1], true
}

type historyBuilder struct {
	history History
	valid   bool
}

func (b *historyBuilder) add(role string, values ...gjson.Result) {
	if !b.valid {
		return
	}
	digest := historyDigest(role, values...)
	if !digest.valid {
		b.valid = false
		return
	}
	if digest.parts == 0 {
		return
	}
	if len(b.history.prefixes) >= historyMaxTurns {
		b.valid = false
		return
	}
	var input [sha256.Size * 2]byte
	if count := len(b.history.prefixes); count > 0 {
		copy(input[:sha256.Size], b.history.prefixes[count-1][:])
	}
	copy(input[sha256.Size:], digest.sum[:])
	b.history.prefixes = append(b.history.prefixes, sha256.Sum256(input[:]))
	if b.history.minimum == 0 && digest.userContent {
		b.history.minimum = len(b.history.prefixes)
	}
}

func historyRole(raw string) string {
	switch role := strings.ToLower(strings.TrimSpace(raw)); role {
	case "system", "developer":
		return "system"
	case "assistant", "model", "ai":
		return "assistant"
	case "tool", "function":
		return "tool"
	case "user":
		return "user"
	default:
		return "unknown"
	}
}

// FingerprintHistory captures supported protocol roots without retaining body
// bytes. Incremental response references cannot prove a complete shared prefix.
func FingerprintHistory(format sdktranslator.Format, payload []byte) History {
	if len(payload) == 0 || !gjson.ValidBytes(payload) {
		return History{}
	}
	root := gjson.ParseBytes(payload)
	if !root.IsObject() {
		return History{}
	}
	if previous := root.Get("previous_response_id"); previous.Exists() && previous.Type != gjson.Null && previous.String() != "" {
		return History{}
	}
	format = sdktranslator.Format(strings.ToLower(strings.TrimSpace(string(format))))
	if format == "" {
		switch {
		case root.Get("contents").IsArray() || root.Get("request.contents").IsArray():
			format = sdktranslator.FormatGemini
		case root.Get("input").Exists() && (root.Get("system_instruction").Exists() || root.Get(`input.#(type=="user_input")`).Exists() || root.Get(`input.#(type=="model_output")`).Exists()):
			format = sdktranslator.FormatInteractions
		case root.Get("system").Exists():
			format = sdktranslator.FormatClaude
		case root.Get("messages").IsArray():
			format = sdktranslator.FormatOpenAI
		case root.Get("input").Exists():
			format = sdktranslator.FormatOpenAIResponse
		}
	}
	b := historyBuilder{valid: true}
	// Tool declarations affect the prompt prefix but cannot establish a user anchor.
	b.add("system", root.Get("tools"))
	switch format {
	case sdktranslator.FormatGemini, sdktranslator.FormatAntigravity:
		b.addGeminiHistory(root)
	case sdktranslator.FormatInteractions:
		if previous := root.Get("previous_interaction_id"); previous.Exists() && previous.Type != gjson.Null && previous.String() != "" {
			return History{}
		}
		b.addGoogleSystem(root)
		b.addInteractionHistory(root.Get("input"), "user", 0)
	case sdktranslator.FormatOpenAI, sdktranslator.FormatClaude:
		if format == sdktranslator.FormatClaude {
			b.add("system", root.Get("system"))
		}
		messages := root.Get("messages")
		if !messages.IsArray() {
			return History{}
		}
		messages.ForEach(func(_, message gjson.Result) bool {
			if !message.IsObject() {
				b.valid = false
				return false
			}
			role := historyRole(message.Get("role").String())
			if role == "tool" {
				b.add(role, message)
			} else {
				b.add(role, message.Get("content"), message.Get("tool_calls"), message.Get("tool_call"), message.Get("function_call"), message.Get("tool_use"))
			}
			return b.valid
		})
	case sdktranslator.FormatOpenAIResponse, sdktranslator.FormatCodex:
		b.add("system", root.Get("instructions"))
		input := root.Get("input")
		if input.Type == gjson.String {
			b.add("user", input)
		} else if input.IsArray() {
			input.ForEach(func(_, item gjson.Result) bool {
				if !item.IsObject() {
					b.valid = false
					return false
				}
				typ := strings.ToLower(strings.TrimSpace(item.Get("type").String()))
				switch typ {
				case "reasoning":
					return true
				case "item_reference":
					b.valid = false
					return false
				case "function_call", "custom_tool_call":
					b.add("assistant", item)
				case "function_call_output", "custom_tool_call_output":
					b.add("tool", item)
				case "compaction", "additional_tools":
					b.add("system", item)
				case "", "message":
					b.add(historyRole(item.Get("role").String()), item.Get("content"))
				default:
					b.add(historyRole(item.Get("role").String()), item)
				}
				return b.valid
			})
		} else {
			return History{}
		}
	default:
		return History{}
	}
	if !b.valid || !b.history.Usable() {
		return History{}
	}
	return b.history
}
