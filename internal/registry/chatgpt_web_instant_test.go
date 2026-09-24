package registry

import (
	"fmt"
	"testing"
)

func TestChatGPTWebInstantModelForClient(t *testing.T) {
	r := newTestModelRegistry()
	r.clientModelInfos = map[string]map[string]*ModelInfo{
		"a": {"alias": {ID: "alias", UpstreamID: "gpt-5-6", ChatGPTWebInstant: true}, "other": {ID: "thinking"}},
		"b": {"native": {ID: "gpt-5-7", ChatGPTWebInstant: true}},
	}
	for client, want := range map[string]string{"a": "gpt-5-6", "b": "gpt-5-7", "missing": ""} {
		if got := r.ChatGPTWebInstantModelForClient(client); got != want {
			t.Fatalf("%s: %q != %q", client, got, want)
		}
	}
	clone := cloneModelInfo(r.clientModelInfos["a"]["alias"])
	if !clone.ChatGPTWebInstant {
		t.Fatal("clone lost category")
	}
}

func BenchmarkChatGPTWebInstantModel100KCredentials(b *testing.B) {
	r := newTestModelRegistry()
	r.clientModelInfos = make(map[string]map[string]*ModelInfo, 100000)
	for i := range 100000 {
		r.clientModelInfos[fmt.Sprint(i)] = map[string]*ModelInfo{"model": {ID: "gpt-5-6", ChatGPTWebInstant: true}}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		_ = r.ChatGPTWebInstantModelForClient("50000")
	}
}
