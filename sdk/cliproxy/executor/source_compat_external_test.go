package executor_test

import (
	"errors"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

func TestStreamChunkV6UnkeyedLiteralRemainsSourceCompatible(t *testing.T) {
	wantErr := errors.New("terminal")
	chunk := executor.StreamChunk{[]byte("payload"), wantErr}
	if string(chunk.Payload) != "payload" || !errors.Is(chunk.Err, wantErr) {
		t.Fatalf("chunk = %#v", chunk)
	}
}
