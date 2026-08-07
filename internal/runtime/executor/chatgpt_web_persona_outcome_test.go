package executor

import (
	"errors"
	"net/http"
	"testing"

	chatgptwebauth "github.com/router-for-me/CLIProxyAPI/v6/internal/auth/chatgptweb"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

func TestChatGPTWebPersonaOutcomesClassifyRequestResults(t *testing.T) {
	executor := &ChatGPTWebExecutor{personaOutcomes: make(map[string]chatgptwebauth.PersonaOutcomeSnapshot)}
	auth := chatGPTWebTestAuth("persona-outcomes")

	executor.recordPersonaOutcome(auth, chatgptwebauth.Persona{}, nil)
	executor.recordPersonaOutcome(auth, chatgptwebauth.Persona{}, chatGPTWebDiagnosticError{
		cause: errors.New("forbidden"),
		diagnostic: &cliproxyauth.ErrorDiagnostic{
			HTTPStatus: http.StatusForbidden,
		},
	})
	executor.recordPersonaOutcome(auth, chatgptwebauth.Persona{}, chatGPTWebDiagnosticError{
		cause: errors.New("cloudflare challenge"),
		diagnostic: &cliproxyauth.ErrorDiagnostic{
			HTTPStatus: http.StatusForbidden,
			Cloudflare: true,
		},
	})
	executor.recordPersonaOutcome(auth, chatgptwebauth.Persona{}, errors.New("unclassified failure"))

	snapshot := executor.SentinelSnapshot()
	if len(snapshot.PersonaOutcomes) != 1 {
		t.Fatalf("persona outcomes = %#v", snapshot.PersonaOutcomes)
	}
	outcome := snapshot.PersonaOutcomes[0]
	if outcome.CatalogVersion != "v2" || outcome.CatalogID == "" || outcome.TLSProfile != "chrome_146" || outcome.UAMajor != "146" {
		t.Fatalf("persona identity = %#v", outcome)
	}
	if outcome.Success200 != 1 || outcome.Forbidden403 != 1 || outcome.Cloudflare403 != 1 || outcome.Other != 1 {
		t.Fatalf("persona counters = %#v", outcome)
	}
}

func TestChatGPTWebPersonaOutcomeDoesNotMutateCredential(t *testing.T) {
	executor := &ChatGPTWebExecutor{personaOutcomes: make(map[string]chatgptwebauth.PersonaOutcomeSnapshot)}
	auth := chatGPTWebTestAuth("persona-outcome-stability")
	before := auth.Clone()

	executor.recordPersonaOutcome(auth, chatgptwebauth.Persona{}, chatGPTWebDiagnosticError{
		cause: errors.New("cloudflare challenge"),
		diagnostic: &cliproxyauth.ErrorDiagnostic{
			HTTPStatus: http.StatusForbidden,
			Cloudflare: true,
		},
	})

	if got, want := auth.Metadata["persona"], before.Metadata["persona"]; !chatGPTWebTestMetadataEqual(got, want) {
		t.Fatalf("persona changed after 403: got %#v want %#v", got, want)
	}
}

func chatGPTWebTestMetadataEqual(left, right any) bool {
	leftCredential, leftOK := left.(chatgptwebauth.Persona)
	rightCredential, rightOK := right.(chatgptwebauth.Persona)
	return leftOK && rightOK && leftCredential == rightCredential
}
