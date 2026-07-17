package executor

var (
	successfulStreamTerminalMarker = [1]byte{}
	bootstrapCommitStreamMarker    = [1]byte{}
)

// SuccessfulStreamTerminalChunk returns an empty internal marker used to
// distinguish successful completion from cancellation without changing the
// source-compatible StreamChunk layout.
func SuccessfulStreamTerminalChunk() StreamChunk {
	return StreamChunk{Payload: successfulStreamTerminalMarker[:0:1]}
}

// IsSuccessfulStreamTerminalChunk reports whether chunk is the internal
// successful-completion marker.
func IsSuccessfulStreamTerminalChunk(chunk StreamChunk) bool {
	if chunk.Err != nil || len(chunk.Payload) != 0 || cap(chunk.Payload) != 1 {
		return false
	}
	return &chunk.Payload[:1][0] == &successfulStreamTerminalMarker[0]
}

// BootstrapCommitStreamChunk returns an empty internal marker that prevents
// provider fallback after a non-semantic stream heartbeat has been forwarded.
func BootstrapCommitStreamChunk() StreamChunk {
	return StreamChunk{Payload: bootstrapCommitStreamMarker[:0:1]}
}

// IsBootstrapCommitStreamChunk reports whether chunk commits stream bootstrap
// without carrying downstream payload bytes.
func IsBootstrapCommitStreamChunk(chunk StreamChunk) bool {
	if chunk.Err != nil || len(chunk.Payload) != 0 || cap(chunk.Payload) != 1 {
		return false
	}
	return &chunk.Payload[:1][0] == &bootstrapCommitStreamMarker[0]
}
