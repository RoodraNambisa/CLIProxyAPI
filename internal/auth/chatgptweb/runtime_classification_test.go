package chatgptweb

import (
	"net/http"
	"testing"
)

func TestClassifyPermanentAccountResponseUsesStructuredEvidence(t *testing.T) {
	for _, test := range []struct {
		name     string
		payload  string
		wantCode string
	}{
		{
			name:     "code",
			payload:  `{"error":{"code":"account_deleted","message":"account unavailable"}}`,
			wantCode: "account_deleted",
		},
		{
			name:     "explicit message",
			payload:  `{"error":{"message":"Your account has been deactivated. Contact support for details."}}`,
			wantCode: "account_deactivated",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			authError := ClassifyPermanentAccountResponse(http.StatusForbidden, []byte(test.payload))
			if authError == nil || authError.Code != test.wantCode || authError.State != LifecycleDead ||
				authError.StatusCode != http.StatusForbidden || !authError.Terminal || authError.Retryable {
				t.Fatalf("ClassifyPermanentAccountResponse() = %#v", authError)
			}
		})
	}
}

func TestClassifyPermanentAccountResponseRejectsBroadMatches(t *testing.T) {
	for _, payload := range []string{
		`{"error":{"code":"not_account_deleted","message":"temporary request failure"}}`,
		`{"error":{"code":"account_deactivated_pending","message":"temporary request failure"}}`,
		`{"error":{"code":"deleted","message":"conversation was deleted"}}`,
		`{"error":{"code":"deactivated","message":"resource was deactivated"}}`,
		`{"error":{"message":"Contact support if your account was deleted or deactivated."}}`,
		`{"message":"Your account has been deactivated"}`,
		`<html>This account was deleted or deactivated.</html>`,
	} {
		if authError := ClassifyPermanentAccountResponse(http.StatusForbidden, []byte(payload)); authError != nil {
			t.Fatalf("ClassifyPermanentAccountResponse(%q) = %#v", payload, authError)
		}
	}
}

func TestLoginClassificationKeepsGenericPermanentCodes(t *testing.T) {
	authError := classifyPermanentAccountPayload([]byte(`{"error":{"code":"deleted"}}`))
	if authError == nil || authError.Code != "account_deleted" || authError.State != LifecycleDead {
		t.Fatalf("classifyPermanentAccountPayload() = %#v", authError)
	}
}
