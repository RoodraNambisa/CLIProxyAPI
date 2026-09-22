package auth

import (
	"bytes"
	"reflect"
	"testing"
)

func TestResponseGuardPublicModelObserverPreservesInput(t *testing.T) {
	var models []string
	r := NewStreamRewriter(StreamRewriteOptions{StrictModelFields: true, OnModel: func(v string) { models = append(models, v) }})
	chunks := [][]byte{[]byte("data: {\"type\":\"response.created\",\n"), []byte("data: \"response\":{\"model\":\"visible\"}}\n\n"), []byte("event: message_start\ndata: {\"message\":{\"model\":\"claude-view\"}}\n\n"), []byte("data: {\"error\":{},\"model\":\"error-only\"}\n\n")}
	for _, chunk := range chunks {
		before := bytes.Clone(chunk)
		_ = r.RewriteChunk(chunk)
		if !bytes.Equal(chunk, before) {
			t.Fatal("observer modified the forwarded payload")
		}
	}
	if !reflect.DeepEqual(models, []string{"visible", "claude-view"}) {
		t.Fatalf("public models=%v", models)
	}
}
