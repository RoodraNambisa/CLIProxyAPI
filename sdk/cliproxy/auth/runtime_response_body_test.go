package auth

import (
	"context"
	"errors"
	"io"
	"testing"
)

type retirementReadCloser struct {
	payload []byte
	err     error
	retire  func()
}

func (r *retirementReadCloser) Read(p []byte) (int, error) {
	n := copy(p, r.payload)
	if r.retire != nil {
		r.retire()
	}
	return n, r.err
}

func (*retirementReadCloser) Close() error { return nil }

func TestRuntimeExecutionResponseBodyPreservesEOFDuringRetirement(t *testing.T) {
	tests := []struct {
		name    string
		payload string
	}{
		{name: "empty final read"},
		{name: "data with final read", payload: "done"},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			auth := &Auth{instanceState: &authInstanceState{}}
			_, release, active := auth.BeginRuntimeExecution(context.Background())
			if !active {
				t.Fatal("runtime execution lease was not active")
			}
			body := &runtimeExecutionResponseBody{
				ReadCloser: &retirementReadCloser{
					payload: []byte(testCase.payload),
					err:     io.EOF,
					retire:  auth.retireInstance,
				},
				release: release,
			}

			buffer := make([]byte, len(testCase.payload)+1)
			n, errRead := body.Read(buffer)
			if errRead != io.EOF {
				t.Fatalf("read error = %#v, want io.EOF", errRead)
			}
			if got := string(buffer[:n]); got != testCase.payload {
				t.Fatalf("read payload = %q, want %q", got, testCase.payload)
			}
		})
	}
}

func TestRuntimeExecutionResponseBodyTranslatesRetirementReadError(t *testing.T) {
	auth := &Auth{instanceState: &authInstanceState{}}
	_, release, active := auth.BeginRuntimeExecution(context.Background())
	if !active {
		t.Fatal("runtime execution lease was not active")
	}
	body := &runtimeExecutionResponseBody{
		ReadCloser: &retirementReadCloser{
			payload: []byte("partial"),
			err:     io.ErrUnexpectedEOF,
			retire:  auth.retireInstance,
		},
		release: release,
	}

	buffer := make([]byte, 16)
	n, errRead := body.Read(buffer)
	if got := string(buffer[:n]); got != "partial" {
		t.Fatalf("read payload = %q, want partial", got)
	}
	var authErr *Error
	if !errors.As(errRead, &authErr) || authErr.Code != "auth_instance_retired" {
		t.Fatalf("read error = %#v, want auth_instance_retired", errRead)
	}
}

func TestRuntimeExecutionResponseBodyPreservesUnrelatedReadError(t *testing.T) {
	body := &runtimeExecutionResponseBody{
		ReadCloser: &retirementReadCloser{err: io.ErrUnexpectedEOF},
		release:    func() bool { return false },
	}

	_, errRead := body.Read(make([]byte, 1))
	if errRead != io.ErrUnexpectedEOF {
		t.Fatalf("read error = %#v, want io.ErrUnexpectedEOF", errRead)
	}
}
