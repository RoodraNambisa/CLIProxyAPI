package openai

import (
	"context"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/interfaces"
)

// Begin forwarding before credential preparation or bootstrap finishes so the
// existing writer loop can keep the peer alive and observe cancellation.
func (h *OpenAIResponsesAPIHandler) startResponsesWebsocketStream(ctx context.Context, model string, request []byte) (<-chan []byte, <-chan *interfaces.ErrorMessage, <-chan struct{}) {
	data := make(chan []byte)
	errors := make(chan *interfaces.ErrorMessage)
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer close(errors)
		defer close(data)
		upstreamData, _, upstreamErrors := h.ExecuteStreamWithAuthManager(ctx, h.HandlerType(), model, request, "")
		request = nil
		for upstreamData != nil || upstreamErrors != nil {
			select {
			case <-ctx.Done():
				return
			case payload, ok := <-upstreamData:
				if !ok {
					upstreamData = nil
					continue
				}
				select {
				case data <- payload:
				case <-ctx.Done():
					return
				}
			case errMsg, ok := <-upstreamErrors:
				if !ok {
					upstreamErrors = nil
					continue
				}
				select {
				case errors <- errMsg:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return data, errors, done
}
