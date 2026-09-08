package auth

import (
	"context"
	"errors"
	"net/http"
)

// PrepareHttpRequest uses the executor captured during selection. It changes
// only the supplied request and performs no network operation or slot commit.
func (l *CodexLiveLease) PrepareHttpRequest(req *http.Request) error {
	if req == nil {
		return &Error{Code: "invalid_request", Message: "http request is nil", HTTPStatus: http.StatusBadRequest}
	}
	if errCtx := context.Cause(l.ctx); errCtx != nil {
		return errCtx
	}
	if errCtx := context.Cause(req.Context()); errCtx != nil {
		return errCtx
	}
	preparer, ok := l.executor.(RequestPreparer)
	if !ok {
		return &Error{Code: "not_supported", Message: "executor does not support realtime request preparation", HTTPStatus: http.StatusNotImplemented}
	}
	ctx, finish := l.httpContext(req.Context())
	defer finish()
	if errCtx := context.Cause(ctx); errCtx != nil {
		return errCtx
	}
	requestCtx := req.Context()
	*req = *req.WithContext(ctx)
	errPrepare := preparer.PrepareRequest(req, l.auth)
	*req = *req.WithContext(requestCtx)
	if errCtx := context.Cause(requestCtx); errCtx != nil {
		return errCtx
	}
	if errCtx := context.Cause(ctx); errCtx != nil {
		return errCtx
	}
	return errPrepare
}

// HttpRequest retains the selected transport and cancels on either the lease
// or request context. The caller commits initial setup capacity before calling
// and must close the returned body. Cleanup does not acquire a new credential.
func (l *CodexLiveLease) HttpRequest(req *http.Request) (*http.Response, error) {
	if req == nil {
		return nil, &Error{Code: "invalid_request", Message: "http request is nil", HTTPStatus: http.StatusBadRequest}
	}
	ctx, finish := l.httpContext(req.Context())
	if errCtx := context.Cause(ctx); errCtx != nil {
		finish()
		return nil, errCtx
	}
	resp, errRequest := l.executor.HttpRequest(ctx, l.auth, req.Clone(ctx))
	if errCtx := context.Cause(req.Context()); errCtx != nil {
		errRequest = errCtx
	}
	if errCtx := context.Cause(ctx); errCtx != nil {
		errRequest = errCtx
	}
	if errRequest != nil {
		finish()
		if resp != nil && resp.Body != nil {
			errRequest = errors.Join(errRequest, resp.Body.Close())
		}
		return resp, errRequest
	}
	if resp == nil || resp.Body == nil {
		finish()
		return resp, nil
	}
	resp.Body = &runtimeExecutionResponseBody{ReadCloser: resp.Body, release: func() bool {
		finish()
		return l.auth.RuntimeInstanceRetired()
	}}
	return resp, nil
}

func (l *CodexLiveLease) httpContext(requestCtx context.Context) (context.Context, func()) {
	ctx, cancel := context.WithCancelCause(l.ctx)
	l.mu.Lock()
	closed := l.closed
	l.mu.Unlock()
	if closed {
		cancel(context.Canceled)
	}
	stop := context.AfterFunc(requestCtx, func() { cancel(context.Cause(requestCtx)) })
	if errCtx := context.Cause(requestCtx); errCtx != nil {
		cancel(errCtx)
	}
	return ctx, func() {
		stop()
		cancel(nil)
	}
}
