package openai

import (
	"context"

	"github.com/gorilla/websocket"
)

type responsesWebsocketRequestMessage struct {
	kind    int
	payload []byte
}

// One reader handles peer control frames while a response is streaming. Its
// bounded queue applies backpressure to pipelined requests without another writer.
func readResponsesWebsocketRequests(ctx context.Context, cancel context.CancelCauseFunc, conn *websocket.Conn) (<-chan responsesWebsocketRequestMessage, <-chan struct{}) {
	messages := make(chan responsesWebsocketRequestMessage, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			kind, payload, err := conn.ReadMessage()
			if err != nil {
				cancel(err)
				return
			}
			select {
			case messages <- responsesWebsocketRequestMessage{kind: kind, payload: payload}:
			case <-ctx.Done():
				return
			}
		}
	}()
	return messages, done
}
