package auth

import (
	"reflect"
	"sync"
	"testing"
)

func TestAuthCloneLastErrorIsolation(t *testing.T) {
	for _, clone := range []struct {
		name string
		run  func(*Auth) *Auth
	}{{"clone", (*Auth).Clone}, {"without runtime", (*Auth).CloneWithoutRuntimeInstance}} {
		t.Run(clone.name, func(t *testing.T) {
			originalError := &Error{Code: "upstream_error", Message: "fixture failure", HTTPStatus: 503, Retryable: true, Diagnostic: &ErrorDiagnostic{Stage: "request", Attempts: 2}}
			original := &Auth{LastError: originalError, ModelStates: map[string]*ModelState{"model": {LastError: originalError}}}
			copied := clone.run(original)
			if !reflect.DeepEqual(copied.LastError, originalError) {
				t.Fatal("clone lost error details")
			}
			copied.LastError.Message = "changed"
			copied.LastError.Diagnostic.Stage = "changed"
			copied.ModelStates["model"].LastError.Code = "changed"
			copied.ModelStates["model"].LastError.Diagnostic.Attempts = 99
			if originalError.Message != "fixture failure" || originalError.Code != "upstream_error" ||
				originalError.Diagnostic.Stage != "request" || originalError.Diagnostic.Attempts != 2 {
				t.Fatal("cloned credential errors alias the original")
			}
			if copied.LastError == copied.ModelStates["model"].LastError {
				t.Fatal("auth and model errors share a mutable pointer")
			}
			if clone.run(&Auth{}).LastError != nil || clone.run(nil) != nil {
				t.Fatal("nil clone behavior changed")
			}
		})
	}
}

func TestManagerLastErrorSnapshotsCannotMutateLiveState(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	original := &Auth{ID: "error-snapshot", Provider: "codex", LastError: &Error{
		Code: "upstream_error", Message: "fixture failure", HTTPStatus: 503,
		Diagnostic: &ErrorDiagnostic{Stage: "request"},
	}}
	registered, errRegister := manager.Register(WithSkipPersist(t.Context()), original)
	if errRegister != nil {
		t.Fatal(errRegister)
	}
	snapshots := []*Auth{original, registered}
	byID, ok := manager.GetByID(original.ID)
	if !ok {
		t.Fatal("registered credential missing")
	}
	snapshots = append(snapshots, byID, manager.List()[0])
	for _, snapshot := range snapshots {
		snapshot.LastError.Message = "external mutation"
		snapshot.LastError.Diagnostic.Stage = "external mutation"
	}
	current, _ := manager.GetByID(original.ID)
	if current.LastError.Message != "fixture failure" || current.LastError.Diagnostic.Stage != "request" {
		t.Fatal("external error mutation reached manager state")
	}
	var readers sync.WaitGroup
	for range 8 {
		readers.Go(func() {
			for range 100 {
				snapshot, _ := manager.GetByID(original.ID)
				snapshot.LastError.Message = "reader mutation"
				snapshot.LastError.Diagnostic.Stage = "reader mutation"
			}
		})
	}
	readers.Wait()
	current, _ = manager.GetByID(original.ID)
	if current.LastError.Message != "fixture failure" || current.LastError.Diagnostic.Stage != "request" {
		t.Fatal("concurrent readers mutated manager state")
	}
}
