package helps

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"net/http"
	"strings"
	"sync"

	"github.com/tidwall/gjson"
)

const (
	CodexBootstrapMaxEvents = 16
	CodexBootstrapMaxBytes  = 64 * 1024
)

// CodexBootstrapProbe accepts only known pre-output metadata. Unknown events
// commit the stream, even if they happen to contain no visible text.
type CodexBootstrapProbe struct {
	events int
}

func (p *CodexBootstrapProbe) Observe(payload []byte) (hold bool, overloadStatus int) {
	if !gjson.ValidBytes(payload) {
		return false, 0
	}
	if status := CodexBootstrapOverloadStatus(payload); status != 0 {
		return false, status
	}
	switch gjson.GetBytes(payload, "type").String() {
	case "response.created", "response.in_progress", "codex.rate_limits", "codex.response.metadata":
		p.events++
		return p.events < CodexBootstrapMaxEvents, 0
	default:
		return false, 0
	}
}

// CodexBootstrapOverloadStatus never infers retry eligibility from message text.
// Explicit request, policy and authentication failures take precedence over a
// generic capacity type; an actual rate limit retains its 429 classification.
func CodexBootstrapOverloadStatus(payload []byte) int {
	root := gjson.ParseBytes(payload)
	switch root.Get("type").String() {
	case "error", "response.failed", "response.incomplete", "response.completed", "response.done":
	default:
		return 0
	}
	failure := root.Get("response.error")
	if !failure.IsObject() {
		failure = root.Get("error")
	}
	if !failure.IsObject() {
		return 0
	}
	code := strings.ToLower(strings.TrimSpace(failure.Get("code").String()))
	kind := strings.ToLower(strings.TrimSpace(failure.Get("type").String()))
	switch code {
	case "", "server_is_overloaded", "rate_limit_exceeded":
	default:
		return 0
	}
	if kind != "service_unavailable_error" && kind != "rate_limit_error" && code != "server_is_overloaded" && code != "rate_limit_exceeded" {
		return 0
	}
	switch kind {
	case "authentication_error", "permission_error", "invalid_request_error", "misalignment_policy_violation", "cyber_policy", "content_policy_violation":
		return 0
	}
	status := 0
	for _, field := range []gjson.Result{root.Get("status"), root.Get("status_code"), failure.Get("status"), failure.Get("status_code")} {
		if !field.Exists() {
			continue
		}
		value := field.Int()
		if value >= 400 && value <= 599 {
			if value != http.StatusTooManyRequests && value != http.StatusServiceUnavailable {
				return 0
			}
			if status == 0 || value == http.StatusTooManyRequests {
				status = int(value)
			}
		}
	}
	if status == http.StatusTooManyRequests || code == "rate_limit_exceeded" || kind == "rate_limit_error" {
		return http.StatusTooManyRequests
	}
	return http.StatusServiceUnavailable
}

type CodexBootstrapFailure struct {
	Payload []byte
	Status  int
}

// CodexBootstrapErrorBody keeps the public error object, including retry hints.
func CodexBootstrapErrorBody(payload []byte) []byte {
	if failure := gjson.GetBytes(payload, "response.error"); failure.IsObject() {
		return append(append([]byte(`{"error":`), failure.Raw...), '}')
	}
	return payload
}

type codexBootstrapBody struct {
	io.Reader
	io.Closer
}

type codexBootstrapReadError struct{ err error }

func (r codexBootstrapReadError) Read([]byte) (int, error) { return 0, r.err }

// CodexBootstrapDisconnectGate defers transport disconnect notifications while
// the first events are inspected. Only a confirmed replayable overload discards
// the pending notification; cancellation and credential retirement stay external.
type CodexBootstrapDisconnectGate struct {
	mu       sync.Mutex
	notify   func(error)
	finished bool
	discard  bool
	pending  bool
	err      error
}

func NewCodexBootstrapDisconnectGate(notify func(error)) *CodexBootstrapDisconnectGate {
	return &CodexBootstrapDisconnectGate{notify: notify}
}

func (g *CodexBootstrapDisconnectGate) Notify(err error) {
	g.mu.Lock()
	if !g.finished {
		g.pending, g.err = true, err
		g.mu.Unlock()
		return
	}
	discard := g.discard
	g.mu.Unlock()
	if !discard {
		g.notify(err)
	}
}

func (g *CodexBootstrapDisconnectGate) Finish(discard bool) {
	g.mu.Lock()
	if g.finished {
		g.mu.Unlock()
		return
	}
	g.finished, g.discard = true, discard
	pending, err := g.pending, g.err
	g.err = nil
	g.mu.Unlock()
	if pending && !discard {
		g.notify(err)
	}
}

// ProbeCodexSSEBootstrap returns an exact replay of all consumed bytes unless a
// replayable overload is found. It neither translates nor logs/records usage.
// The caller owns closing body on failure and closing the returned body otherwise.
func ProbeCodexSSEBootstrap(ctx context.Context, body io.ReadCloser, replayable func() bool) (io.ReadCloser, *CodexBootstrapFailure, error) {
	reader := bufio.NewReader(body)
	prefix := make([]byte, 0, 4096)
	var probe CodexBootstrapProbe
	var frameData []byte
	stopCancel := func() bool { return true }
	if ctx != nil {
		stopCancel = context.AfterFunc(ctx, func() { _ = body.Close() })
	}
	defer stopCancel()
	replay := func(readErr error) io.ReadCloser {
		var tail io.Reader = reader
		if readErr != nil {
			tail = codexBootstrapReadError{readErr}
		}
		return &codexBootstrapBody{Reader: io.MultiReader(bytes.NewReader(prefix), tail), Closer: body}
	}
	for len(prefix) < CodexBootstrapMaxBytes {
		if ctx != nil && ctx.Err() != nil {
			return nil, nil, ctx.Err()
		}
		if !replayable() {
			return replay(nil), nil, nil
		}
		start := len(prefix)
		var readErr error
		for len(prefix) < CodexBootstrapMaxBytes {
			if _, readErr = reader.Peek(1); readErr != nil {
				break
			}
			available, _ := reader.Peek(reader.Buffered())
			n := min(len(available), CodexBootstrapMaxBytes-len(prefix))
			newline := bytes.IndexByte(available[:n], '\n')
			if newline >= 0 {
				n = newline + 1
			}
			prefix = append(prefix, available[:n]...)
			_, _ = reader.Discard(n)
			if newline >= 0 {
				break
			}
		}
		if ctx != nil && ctx.Err() != nil {
			return nil, nil, ctx.Err()
		}
		if len(prefix) >= CodexBootstrapMaxBytes || !replayable() {
			return replay(readErr), nil, nil
		}
		line := bytes.TrimSuffix(bytes.TrimSuffix(prefix[start:], []byte("\n")), []byte("\r"))
		if bytes.HasPrefix(line, []byte("data:")) {
			data := bytes.TrimSpace(line[len("data:"):])
			if len(frameData) > 0 {
				frameData = append(frameData, '\n')
			}
			frameData = append(frameData, data...)
			if gjson.ValidBytes(frameData) {
				hold, status := probe.Observe(frameData)
				if status != 0 {
					return nil, &CodexBootstrapFailure{Payload: frameData, Status: status}, nil
				}
				if !hold {
					return replay(readErr), nil, nil
				}
				frameData = frameData[:0]
			}
		} else if len(line) == 0 {
			if len(frameData) != 0 {
				return replay(readErr), nil, nil
			}
		} else if !bytes.HasPrefix(line, []byte(":")) && !bytes.HasPrefix(line, []byte("event:")) && !bytes.HasPrefix(line, []byte("id:")) && !bytes.HasPrefix(line, []byte("retry:")) {
			return replay(readErr), nil, nil
		}
		if readErr != nil {
			return replay(readErr), nil, nil
		}
	}
	return replay(nil), nil, nil
}
