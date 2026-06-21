package executor

import (
	"context"
	"io"
	"sync"
	"sync/atomic"
	"time"
)

// BodyReleaseControllerMetadataKey stores the per-request body release controller.
const BodyReleaseControllerMetadataKey = "request_body_release_controller"

// RequestBodyReleaseController coordinates dropping request body copies for one downstream request.
type RequestBodyReleaseController struct {
	originalSize int64
	placeholder  []byte
	logOnly      bool

	released atomic.Bool
	timerRun atomic.Bool

	mu        sync.Mutex
	nextID    uint64
	callbacks map[uint64]func([]byte)
}

// ReleasableBytes holds a request body reference that can be cleared by a release callback.
// Bytes returns a read-only slice view; callers must not mutate it.
type ReleasableBytes struct {
	mu   sync.RWMutex
	body []byte
}

// NewReleasableBytes wraps a byte slice without copying it.
func NewReleasableBytes(body []byte) *ReleasableBytes {
	return &ReleasableBytes{body: body}
}

// Bytes returns the current body reference.
func (b *ReleasableBytes) Bytes() []byte {
	if b == nil {
		return nil
	}
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.body
}

// Release drops the body reference.
func (b *ReleasableBytes) Release() {
	if b == nil {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.body = nil
}

// Replace swaps the current body reference with a smaller retained payload.
func (b *ReleasableBytes) Replace(body []byte) {
	if b == nil {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.body = body
}

// NewRequestBodyReleaseController creates a real release controller for one request body.
func NewRequestBodyReleaseController(originalSize int64, placeholder []byte) *RequestBodyReleaseController {
	return NewRequestBodyReleaseControllerWithMode(originalSize, placeholder, false)
}

// NewRequestBodyReleaseControllerWithMode creates a controller for one request body.
func NewRequestBodyReleaseControllerWithMode(originalSize int64, placeholder []byte, logOnly bool) *RequestBodyReleaseController {
	if originalSize < 0 {
		originalSize = 0
	}
	return &RequestBodyReleaseController{
		originalSize: originalSize,
		placeholder:  append([]byte(nil), placeholder...),
		logOnly:      logOnly,
		callbacks:    make(map[uint64]func([]byte)),
	}
}

// OriginalSize returns the captured request body size.
func (c *RequestBodyReleaseController) OriginalSize() int64 {
	if c == nil {
		return 0
	}
	return c.originalSize
}

// Placeholder returns the body replacement text used after release.
func (c *RequestBodyReleaseController) Placeholder() []byte {
	if c == nil {
		return nil
	}
	return append([]byte(nil), c.placeholder...)
}

// Released reports whether the request body has been released.
func (c *RequestBodyReleaseController) Released() bool {
	return c != nil && c.released.Load()
}

// LogOnly reports whether release only affects log body copies.
func (c *RequestBodyReleaseController) LogOnly() bool {
	return c != nil && c.logOnly
}

// Replayable reports whether the request body may still be used for another attempt.
func (c *RequestBodyReleaseController) Replayable() bool {
	return c == nil || c.logOnly || !c.released.Load()
}

// StartTimer starts the release timer once for this controller.
func (c *RequestBodyReleaseController) StartTimer(after time.Duration, done <-chan struct{}) bool {
	if c == nil || after <= 0 || !c.timerRun.CompareAndSwap(false, true) {
		return false
	}
	go func() {
		timer := time.NewTimer(after)
		defer timer.Stop()
		if done == nil {
			<-timer.C
			c.Release()
			return
		}
		select {
		case <-timer.C:
			c.Release()
		case <-done:
		}
	}()
	return true
}

// Release invokes registered release callbacks and marks real releases as non-replayable.
func (c *RequestBodyReleaseController) Release() bool {
	if c == nil {
		return false
	}
	if !c.released.CompareAndSwap(false, true) {
		return false
	}
	c.mu.Lock()
	callbacks := make([]func([]byte), 0, len(c.callbacks))
	for _, callback := range c.callbacks {
		if callback != nil {
			callbacks = append(callbacks, callback)
		}
	}
	c.callbacks = nil
	placeholder := append([]byte(nil), c.placeholder...)
	c.mu.Unlock()
	for _, callback := range callbacks {
		callback(placeholder)
	}
	return true
}

// RegisterReleaseCallback registers a callback and returns an unregister function.
func (c *RequestBodyReleaseController) RegisterReleaseCallback(callback func([]byte)) func() {
	if c == nil || callback == nil {
		return func() {}
	}
	if c.released.Load() {
		callback(c.Placeholder())
		return func() {}
	}
	c.mu.Lock()
	if c.released.Load() {
		c.mu.Unlock()
		callback(c.Placeholder())
		return func() {}
	}
	c.nextID++
	id := c.nextID
	if c.callbacks == nil {
		c.callbacks = make(map[uint64]func([]byte))
	}
	c.callbacks[id] = callback
	c.mu.Unlock()
	return func() {
		c.mu.Lock()
		if c.callbacks != nil {
			delete(c.callbacks, id)
		}
		c.mu.Unlock()
	}
}

// RequestBodyReleaseControllerFromMetadata returns the controller stored in metadata.
func RequestBodyReleaseControllerFromMetadata(meta map[string]any) *RequestBodyReleaseController {
	if len(meta) == 0 {
		return nil
	}
	ctrl, _ := meta[BodyReleaseControllerMetadataKey].(*RequestBodyReleaseController)
	return ctrl
}

// RequestBodyReleaseControllerFromOptions returns the controller stored in options metadata.
func RequestBodyReleaseControllerFromOptions(opts Options) *RequestBodyReleaseController {
	return RequestBodyReleaseControllerFromMetadata(opts.Metadata)
}

// RequestBodyReleaseControllerFromContext returns the controller stored in context.
func RequestBodyReleaseControllerFromContext(ctx context.Context) *RequestBodyReleaseController {
	if ctx == nil {
		return nil
	}
	ctrl, _ := ctx.Value(BodyReleaseControllerMetadataKey).(*RequestBodyReleaseController)
	return ctrl
}

// WithRequestBodyReleaseController stores the controller in context.
func WithRequestBodyReleaseController(ctx context.Context, ctrl *RequestBodyReleaseController) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if ctrl == nil {
		return ctx
	}
	return context.WithValue(ctx, BodyReleaseControllerMetadataKey, ctrl)
}

// RequestBodyReplayable reports whether options still carry a replayable request body.
func RequestBodyReplayable(opts Options) bool {
	return RequestBodyReleaseControllerFromOptions(opts).Replayable()
}

// RegisterRequestBodyReleaseCallback registers a real-release callback from options or context.
func RegisterRequestBodyReleaseCallback(ctx context.Context, opts Options, callback func([]byte)) func() {
	if callback == nil {
		return func() {}
	}
	if ctrl := RequestBodyReleaseControllerFromOptions(opts); ctrl != nil {
		if ctrl.LogOnly() {
			return func() {}
		}
		return ctrl.RegisterReleaseCallback(callback)
	}
	if ctrl := RequestBodyReleaseControllerFromContext(ctx); ctrl != nil {
		if ctrl.LogOnly() {
			return func() {}
		}
		return ctrl.RegisterReleaseCallback(callback)
	}
	return func() {}
}

// RegisterRequestBodyReleaseCleanup registers cleanup for executor-local request copies.
func RegisterRequestBodyReleaseCleanup(ctx context.Context, req *Request, opts *Options) func() {
	var value Options
	if opts != nil {
		value = *opts
	}
	return RegisterRequestBodyReleaseCallback(ctx, value, func([]byte) {
		if req != nil {
			req.Payload = nil
		}
		if opts != nil {
			opts.OriginalRequest = nil
		}
	})
}

// ReleasableReadCloser releases its backing byte slice after it has been consumed or closed.
type ReleasableReadCloser struct {
	mu        sync.Mutex
	body      []byte
	offset    int
	onRelease func()
	released  bool
}

// NewReleasableReadCloser wraps body in a reader that drops its backing slice on EOF or Close.
func NewReleasableReadCloser(body []byte, onRelease func()) *ReleasableReadCloser {
	return &ReleasableReadCloser{body: body, onRelease: onRelease}
}

// Len returns the initial readable body length.
func (r *ReleasableReadCloser) Len() int {
	if r == nil {
		return 0
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.body)
}

// Read implements io.Reader.
func (r *ReleasableReadCloser) Read(p []byte) (int, error) {
	if r == nil {
		return 0, io.EOF
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.offset >= len(r.body) {
		r.releaseLocked()
		return 0, io.EOF
	}
	n := copy(p, r.body[r.offset:])
	r.offset += n
	if r.offset >= len(r.body) {
		r.releaseLocked()
	}
	return n, nil
}

// Close implements io.Closer.
func (r *ReleasableReadCloser) Close() error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.releaseLocked()
	return nil
}

func (r *ReleasableReadCloser) releaseLocked() {
	if r.released {
		return
	}
	r.released = true
	r.body = nil
	r.offset = 0
	if r.onRelease != nil {
		r.onRelease()
	}
}
