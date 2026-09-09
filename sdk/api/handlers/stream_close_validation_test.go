package handlers

import (
	"errors"
	"fmt"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/interfaces"
)

func TestForwardStreamCloseValidationRunsBeforeDone(t *testing.T) {
	for _, pending := range []bool{false, true} {
		for _, invalid := range []bool{false, true} {
			t.Run(fmt.Sprintf("pending=%t/invalid=%t", pending, invalid), func(t *testing.T) {
				h, c, flusher := newForwardStreamTestContext(t)
				data := make(chan []byte)
				close(data)
				errs := make(chan *interfaces.ErrorMessage, 1)
				upstream := &interfaces.ErrorMessage{StatusCode: 429, Error: errors.New("upstream quota")}
				local := &interfaces.ErrorMessage{StatusCode: 502, Error: errors.New("missing terminal")}
				if pending {
					errs <- upstream
				}
				close(errs)
				validated, done := 0, 0
				var delivered *interfaces.ErrorMessage
				var canceled error
				h.ForwardStream(c, flusher, func(err error) { canceled = err }, data, errs, StreamForwardOptions{
					CloseError: func() *interfaces.ErrorMessage {
						validated++
						if invalid {
							return local
						}
						return nil
					},
					WriteDone:          func() { done++ },
					WriteTerminalError: func(err *interfaces.ErrorMessage) { delivered = err },
				})
				if pending {
					if validated != 0 || done != 0 || delivered != upstream || !errors.Is(canceled, upstream.Error) {
						t.Fatal("close validation replaced a pending upstream error")
					}
				} else if invalid {
					if validated != 1 || done != 0 || delivered != local || !errors.Is(canceled, local.Error) {
						t.Fatal("invalid close was completed or lost its original error message")
					}
				} else if validated != 1 || done != 1 || delivered != nil || canceled != nil {
					t.Fatal("valid close did not complete once")
				}
			})
		}
	}
}
