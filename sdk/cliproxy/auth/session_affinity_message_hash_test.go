package auth

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

func TestBasicMessageAffinityDigestIncludesEveryChunk(t *testing.T) {
	for _, size := range []int{0, 1, 4095, 4096, 4097, 100000} {
		text := strings.Repeat("x", size) + "tail"
		if got := messageTextDigest(text); got != sha256.Sum256([]byte(text)) {
			t.Fatalf("complete text digest differed at size %d", size)
		}
	}
}

func TestBasicMessageAffinityEscapedFieldsAndTextStayEquivalent(t *testing.T) {
	for _, bodies := range [][2]string{
		{`{"input":"question: \"words\""}`, `{"in\u0070ut":"qu\u0065stion: \"words\""}`},
		{`{"conversation_id":"explicit","input":"question"}`, `{"conversation_\u0069d":"explicit","input":"question"}`},
		{`{"metadata":{"user_id":"explicit"}}`, `{"m\u0065tadata":{"user_id":"explicit"}}`},
		{`{"prompt_cache_key":"explicit"}`, `{"prompt_cache_\u006bey":"explicit"}`},
	} {
		first := ExtractSessionID(nil, []byte(bodies[0]), nil)
		if first == "" || first != ExtractSessionID(nil, []byte(bodies[1]), nil) {
			t.Fatal("escaped protocol fields or text changed the identity")
		}
	}
}

func TestBasicMessageAffinityHashesCompleteInitialText(t *testing.T) {
	for _, role := range []string{"system", "user", "assistant"} {
		t.Run(role, func(t *testing.T) {
			identity := func(suffix string) string {
				messages := []map[string]string{{"role": "system", "content": "template"}, {"role": "user", "content": "question"}, {"role": "assistant", "content": "answer"}}
				for _, message := range messages {
					if message["role"] == role {
						message["content"] = strings.Repeat("共同前缀", 50) + suffix
					}
				}
				body, err := json.Marshal(map[string]any{"messages": messages})
				if err != nil {
					t.Fatal(err)
				}
				return ExtractSessionID(nil, body, nil)
			}
			if first, second := identity("left"), identity("right"); first == "" || first == second {
				t.Fatal("initial messages differing after 100 bytes shared a binding")
			}
		})
	}
}

func TestBasicMessageAffinityReadsResponsesStringInput(t *testing.T) {
	first := ExtractSessionID(nil, []byte(`{"instructions":"same template","input":"question one"}`), nil)
	second := ExtractSessionID(nil, []byte(`{"instructions":"same template","input":"question two"}`), nil)
	array := ExtractSessionID(nil, []byte(`{"instructions":"same template","input":[{"role":"user","content":"question one"}]}`), nil)
	if first == "" || second == "" || first == second || first != array {
		t.Fatal("Responses string input was ignored or differed from equivalent message input")
	}
	for _, body := range []string{`{"instructions":"same template"}`, `{"messages":[{"role":"system","content":"same template"}]}`, `{"input":[{"role":"developer","content":"same template"}]}`, `{"system":"same template","messages":[]}`} {
		if got := ExtractSessionID(nil, []byte(body), nil); got != "" {
			t.Fatal("a shared template without user content established affinity")
		}
	}
}

func TestBasicMessageAffinityKeepsRoleBoundariesAndFirstTurnInheritance(t *testing.T) {
	first := []byte(`{"messages":[{"role":"system","content":"x\nusr:y"},{"role":"user","content":"z"}]}`)
	second := []byte(`{"messages":[{"role":"system","content":"x"},{"role":"user","content":"y\nusr:z"}]}`)
	if ExtractSessionID(nil, first, nil) == ExtractSessionID(nil, second, nil) {
		t.Fatal("message delimiters crossed role boundaries")
	}
	initial := []byte(`{"input":"original question"}`)
	continued := []byte(`{"input":[{"role":"user","content":"original question"},{"role":"assistant","content":"original answer"},{"role":"user","content":"follow-up"}]}`)
	short, _ := extractMessageHashIDs(initial)
	full, fallback := extractMessageHashIDs(continued)
	if short == "" || full == short || fallback != short {
		t.Fatal("full text hashing lost inheritance from the first completed turn")
	}
}

func TestBasicMessageAffinityManagerInheritsCompletedFirstTurn(t *testing.T) {
	selector := NewSessionAffinitySelector(&FillFirstSelector{})
	t.Cleanup(selector.Stop)
	manager := NewManager(nil, selector, nil)
	manager.SetConfig(&config.Config{Routing: config.RoutingConfig{SessionAffinity: true}})
	executor := &requestLimitOperationExecutor{}
	manager.RegisterExecutor(executor)
	for _, id := range []string{"a", "b"} {
		if _, err := manager.Register(WithSkipPersist(t.Context()), &Auth{ID: id, Provider: "test"}); err != nil {
			t.Fatal(err)
		}
	}
	for i, body := range []string{
		`{"input":"original question"}`,
		`{"input":[{"role":"user","content":"original question"},{"role":"assistant","content":"original answer"},{"role":"user","content":"follow-up"}]}`,
		`{"input":"unrelated question"}`,
	} {
		options := core.Options{}
		if i == 0 {
			options.Metadata = map[string]any{core.PinnedAuthMetadataKey: "b"}
		}
		if _, err := manager.Execute(t.Context(), []string{"test"}, core.Request{Payload: []byte(body)}, options); err != nil {
			t.Fatal(err)
		}
	}
	if got := executor.callIDs(); !reflect.DeepEqual(got, []string{"b", "b", "a"}) {
		t.Fatalf("first-turn inheritance and unrelated selection = %v", got)
	}
}

var basicAffinityBenchmarkIdentity string

func BenchmarkBasicAffinityLargeTextCapture(b *testing.B) {
	for _, size := range []int{1 << 20, 10 << 20} {
		b.Run(fmt.Sprintf("%dMiB", size>>20), func(b *testing.B) {
			payload := []byte(`{"instructions":"template","input":"` + strings.Repeat("x", size) + `"}`)
			selector := NewSessionAffinitySelector(nil)
			b.Cleanup(selector.Stop)
			request := core.Request{Payload: payload}
			b.ReportAllocs()
			b.SetBytes(int64(len(payload)))
			b.ResetTimer()
			for b.Loop() {
				options := selector.withBasicAffinityIdentity(b.Context(), request, core.Options{})
				basicAffinityBenchmarkIdentity, _ = selector.sessionIDs(b.Context(), options)
			}
		})
	}
}
