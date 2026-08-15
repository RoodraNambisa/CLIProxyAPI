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
	executor.recordPersonaOutcome(auth, chatgptwebauth.Persona{}, chatGPTWebDiagnosticError{
		cause: errors.New("sentinel rejected"),
		diagnostic: &cliproxyauth.ErrorDiagnostic{
			Stage:      "sentinel_finalize",
			Code:       "invalid_proof_token",
			HTTPStatus: http.StatusBadRequest,
		},
	})
	executor.recordPersonaOutcome(auth, chatgptwebauth.Persona{}, chatGPTWebDiagnosticError{
		cause: errors.New("upstream unavailable"),
		diagnostic: &cliproxyauth.ErrorDiagnostic{
			HTTPStatus: http.StatusServiceUnavailable,
		},
	})
	executor.recordPersonaOutcome(auth, chatgptwebauth.Persona{}, errors.New("unclassified failure"))

	snapshot := executor.SentinelSnapshot()
	if len(snapshot.PersonaOutcomes) != 1 {
		t.Fatalf("persona outcomes = %#v", snapshot.PersonaOutcomes)
	}
	outcome := snapshot.PersonaOutcomes[0]
	if outcome.CatalogVersion != "v3" || outcome.CatalogID == "" || outcome.TransportPersonaID != outcome.CatalogID ||
		outcome.BrowserEnvironmentID == "" || outcome.TLSProfile != "chrome_146" || outcome.UAMajor != "146" {
		t.Fatalf("persona identity = %#v", outcome)
	}
	if outcome.Success200 != 1 || outcome.Forbidden403 != 1 || outcome.Cloudflare403 != 1 ||
		outcome.SentinelReject != 1 || outcome.HTTPError != 1 || outcome.Other != 1 {
		t.Fatalf("persona counters = %#v", outcome)
	}
}

func TestChatGPTWebPersonaOutcomesSeparateBrowserEnvironments(t *testing.T) {
	executor := &ChatGPTWebExecutor{personaOutcomes: make(map[string]chatgptwebauth.PersonaOutcomeSnapshot)}
	first := chatGPTWebTestAuth("persona-environment-one")
	second := first.Clone()
	second.ID = "chatgpt-web-persona-environment-two"
	credential, errCredential := chatgptwebauth.ParseCredential(first.Metadata)
	if errCredential != nil || credential == nil {
		t.Fatalf("ParseCredential() error = %v", errCredential)
	}
	persona := chatgptwebauth.ResolveCredentialPersona(credential, first.ID)
	first.Metadata["persona"] = persona
	second.Metadata["persona"] = persona
	first.Metadata["browser_environment"] = chatgptwebauth.BrowserEnvironmentIdentity{
		CatalogVersion: "v3",
		CatalogID:      persona.CatalogID + "-e00",
	}
	second.Metadata["browser_environment"] = chatgptwebauth.BrowserEnvironmentIdentity{
		CatalogVersion: "v3",
		CatalogID:      persona.CatalogID + "-e01",
	}

	executor.recordPersonaOutcome(first, chatgptwebauth.Persona{}, nil)
	executor.recordPersonaOutcome(second, chatgptwebauth.Persona{}, nil)

	snapshot := executor.SentinelSnapshot()
	if len(snapshot.PersonaOutcomes) != 2 {
		t.Fatalf("persona outcomes = %#v", snapshot.PersonaOutcomes)
	}
	if snapshot.PersonaOutcomes[0].TransportPersonaID != snapshot.PersonaOutcomes[1].TransportPersonaID {
		t.Fatalf("transport personas differ: %#v", snapshot.PersonaOutcomes)
	}
	if snapshot.PersonaOutcomes[0].BrowserEnvironmentID == snapshot.PersonaOutcomes[1].BrowserEnvironmentID {
		t.Fatalf("browser environments were merged: %#v", snapshot.PersonaOutcomes)
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
