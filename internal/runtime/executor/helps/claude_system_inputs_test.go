package helps

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	"github.com/tidwall/gjson"
)

func TestClaudeTranslationKeepsEverySystemSourceInOrder(t *testing.T) {
	for _, source := range []translator.Format{translator.FormatOpenAIResponse, translator.FormatOpenAI} {
		for _, compat := range []bool{false, true} {
			raw := []byte(`{"instructions":" first ","input":[{"role":"system","content":"second"},{"role":"user","content":"question"},{"role":"developer","content":[{"type":"input_text","text":"third","cache_control":{"type":"ephemeral","ttl":"5m"}},{"type":"text","text":" fourth "}]},{"role":"system","content":"fifth"}]}`)
			want := []string{" first ", "second", "third", " fourth ", "fifth"}
			if source == translator.FormatOpenAI {
				raw = []byte(`{"messages":[{"role":"system","content":"second"},{"role":"user","content":"question"},{"role":"developer","content":[{"type":"text","text":"third","cache_control":{"type":"ephemeral","ttl":"5m"}},{"type":"text","text":" fourth "}]},{"role":"system","content":"fifth"}]}`)
				want = want[1:]
			}
			before := bytes.Clone(raw)
			out := TranslateRequestWithAPIKeyModelCompatibility(source, translator.FormatClaude, "claude-fixture", raw, false, compat)
			blocks := gjson.GetBytes(out, "system").Array()
			if len(blocks) != len(want) {
				t.Fatalf("system sources disappeared or were demoted: source=%s compat=%t count=%d", source, compat, len(blocks))
			}
			for index, text := range want {
				if blocks[index].Get("type").String() != "text" || blocks[index].Get("text").String() != text {
					t.Fatal("system block text or order changed")
				}
				if text == "third" && blocks[index].Get("cache_control.ttl").String() != "5m" {
					t.Fatal("system cache metadata disappeared")
				}
			}
			messages := gjson.GetBytes(out, "messages").Array()
			if len(messages) != 1 || messages[0].Get("role").String() != "user" || !bytes.Equal(raw, before) {
				t.Fatal("operator instructions became conversational messages or mutated the source")
			}
		}
	}
}

func TestClaudeSystemValidationPreservesTextShapesAndCancellation(t *testing.T) {
	for index, raw := range []string{`{}`, `{"system":null}`, `{"system":"plain instructions"}`, `{"system":[]}`, `{"system":[{"type":"text","text":"instruction"}]}`} {
		if err := ValidateClaudeSystemInputs(t.Context(), []byte(raw)); err != nil {
			t.Fatalf("valid text shape %d rejected", index)
		}
	}
	raw := []byte(`{"system":[{"type":"text","text":"valid"},{"type":"private-type","data":"private-payload"}]}`)
	err := ValidateClaudeSystemInputs(t.Context(), raw)
	var typed *claudeSystemInputError
	if !errors.As(err, &typed) || typed.StatusCode() != 400 || !typed.SkipAuthResult() || typed.RetryOtherAuth() ||
		!gjson.Valid(err.Error()) || bytes.Contains([]byte(err.Error()), []byte("private")) {
		t.Fatal("unsupported system block lost its local request error or exposed content")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if !errors.Is(ValidateClaudeSystemInputs(ctx, raw), context.Canceled) {
		t.Fatal("system validation replaced cancellation")
	}
}

func TestClaudeSystemOnlyInputsKeepFallbackAndOpaqueMarkers(t *testing.T) {
	for _, source := range []translator.Format{translator.FormatOpenAIResponse, translator.FormatOpenAI} {
		for _, invalid := range []bool{false, true} {
			parts := `[{"type":"text","text":"first","cache_control":{"type":"ephemeral","ttl":"1h"}},{"type":"text","text":"last"}]`
			if invalid {
				parts = `[{"type":"input_image","image_url":"private-fixture"}]`
			}
			rootKey := "input"
			if source == translator.FormatOpenAI {
				rootKey = "messages"
			}
			raw := []byte(`{"` + rootKey + `":[{"role":"developer","content":` + parts + `,"cache_control":{"type":"ephemeral","ttl":"5m"}}]}`)
			out := TranslateRequestWithAPIKeyModelCompatibility(source, translator.FormatClaude, "claude-fixture", raw, false, false)
			if gjson.GetBytes(out, "messages.#").Int() != 1 || gjson.GetBytes(out, "messages.0.role").String() != "user" {
				t.Fatal("system-only input lost its minimal conversational turn")
			}
			if invalid {
				if ValidateClaudeSystemInputs(t.Context(), out) == nil || bytes.Contains(out, []byte("private-fixture")) {
					t.Fatal("unsupported system content was dropped or retained its opaque payload")
				}
			} else if gjson.GetBytes(out, "system.0.cache_control.ttl").String() != "1h" || gjson.GetBytes(out, "system.1.cache_control.ttl").String() != "5m" {
				t.Fatal("item cache metadata replaced an explicit block value or was lost")
			}
		}
	}
}
