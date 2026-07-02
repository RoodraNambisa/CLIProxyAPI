package auth

import (
	"strings"
	"testing"
)

func TestStreamRewriterRewritesGluedSSEDataFrames(t *testing.T) {
	rewriter := NewStreamRewriter(StreamRewriteOptions{RewriteModel: "client-model"})
	payload := []byte(`data: {"model":"upstream-model"}data: {"response":{"model":"upstream-model"}}`)

	got := string(rewriter.RewriteChunk(payload))

	if strings.Count(got, `"client-model"`) != 2 {
		t.Fatalf("rewritten payload = %s, want both frames rewritten", got)
	}
	if strings.Contains(got, `"upstream-model"`) {
		t.Fatalf("rewritten payload = %s, want upstream model removed", got)
	}
	if !strings.Contains(got, "\n\ndata:") {
		t.Fatalf("rewritten payload = %q, want glued frames split", got)
	}
}

func TestStreamRewriterFlushesPartialFrame(t *testing.T) {
	rewriter := NewStreamRewriter(StreamRewriteOptions{RewriteModel: "client-model"})
	if got := rewriter.RewriteChunk([]byte(`data: {"model"`)); len(got) != 0 {
		t.Fatalf("RewriteChunk() = %q, want buffered partial frame", got)
	}

	got := string(rewriter.Finish())
	if !strings.Contains(got, `data: {"model"`) {
		t.Fatalf("Finish() = %q, want partial frame flushed", got)
	}
}

func TestStreamRewriterJoinsPartialFrameWithoutNewline(t *testing.T) {
	rewriter := NewStreamRewriter(StreamRewriteOptions{RewriteModel: "client-model"})
	if got := rewriter.RewriteChunk([]byte(`data: {"model"`)); len(got) != 0 {
		t.Fatalf("RewriteChunk() = %q, want buffered partial frame", got)
	}

	got := string(rewriter.RewriteChunk([]byte(`:"upstream-model"}`)))

	if strings.Contains(got, "\"model\"\n:") {
		t.Fatalf("RewriteChunk() inserted newline inside JSON: %q", got)
	}
	if !strings.Contains(got, `"client-model"`) {
		t.Fatalf("RewriteChunk() = %q, want model rewritten after reassembly", got)
	}
}

func TestRewriteForceMappedStreamChunkBuffersPartialFrame(t *testing.T) {
	rewriter := NewStreamRewriter(StreamRewriteOptions{RewriteModel: "client-model"})

	got := rewriteForceMappedStreamChunk(rewriter, []byte(`data: {"model"`))

	if len(got) != 0 {
		t.Fatalf("rewriteForceMappedStreamChunk() = %q, want buffered partial frame", got)
	}
	if len(rewriter.pendingBuf) == 0 {
		t.Fatalf("pendingBuf is empty, want partial frame retained")
	}
}
