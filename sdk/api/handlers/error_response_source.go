package handlers

import (
	"sync"

	"github.com/gin-gonic/gin"
	coreexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	"golang.org/x/net/context"
)

const errorResponseSourceTrackerGinKey = "error_response_source_tracker"

type errorResponseSourceTrackerContextKey struct{}

type errorResponseSourceTracker struct {
	mu     sync.RWMutex
	source coreexecutor.ErrorResponseSourceSnapshot
	set    bool
}

func (t *errorResponseSourceTracker) store(source coreexecutor.ErrorResponseSourceSnapshot) {
	if t == nil || source.Provider == "" {
		return
	}
	t.mu.Lock()
	t.source = source
	t.set = true
	t.mu.Unlock()
}

func (t *errorResponseSourceTracker) load() (coreexecutor.ErrorResponseSourceSnapshot, bool) {
	if t == nil {
		return coreexecutor.ErrorResponseSourceSnapshot{}, false
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.source, t.set
}

func errorResponseSourceTrackerFromContext(ctx context.Context) *errorResponseSourceTracker {
	if ctx == nil {
		return nil
	}
	tracker, _ := ctx.Value(errorResponseSourceTrackerContextKey{}).(*errorResponseSourceTracker)
	return tracker
}

func errorResponseSourceTrackerFromGin(c *gin.Context) *errorResponseSourceTracker {
	if c == nil {
		return nil
	}
	value, exists := c.Get(errorResponseSourceTrackerGinKey)
	if !exists {
		return nil
	}
	tracker, _ := value.(*errorResponseSourceTracker)
	return tracker
}

func ensureErrorResponseSourceTracker(ctx context.Context, c *gin.Context) (context.Context, *errorResponseSourceTracker) {
	if ctx == nil {
		ctx = context.Background()
	}
	tracker := errorResponseSourceTrackerFromContext(ctx)
	if tracker == nil {
		tracker = &errorResponseSourceTracker{}
	}
	if c != nil {
		c.Set(errorResponseSourceTrackerGinKey, tracker)
	}
	return context.WithValue(ctx, errorResponseSourceTrackerContextKey{}, tracker), tracker
}

func errorResponseSourceForContext(ctx context.Context, err error) coreexecutor.ErrorResponseSourceSnapshot {
	if source, ok := coreexecutor.ErrorResponseSourceOf(err); ok {
		return source
	}
	if source, ok := errorResponseSourceTrackerFromContext(ctx).load(); ok {
		return source
	}
	return coreexecutor.LocalErrorResponseSource()
}

func errorResponseSourceForGin(c *gin.Context, err error) coreexecutor.ErrorResponseSourceSnapshot {
	if source, ok := coreexecutor.ErrorResponseSourceOf(err); ok {
		return source
	}
	if source, ok := errorResponseSourceTrackerFromGin(c).load(); ok {
		return source
	}
	return coreexecutor.LocalErrorResponseSource()
}
