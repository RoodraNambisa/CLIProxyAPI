package chatgptweb

import (
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestParseAccountProfile(t *testing.T) {
	profile, err := ParseAccountProfile([]byte(`{
		"accounts":{"default":{"account":{"account_id":"acct-1","plan_type":"team"}}}
	}`))
	if err != nil {
		t.Fatalf("ParseAccountProfile() error = %v", err)
	}
	if profile.AccountID != "acct-1" || profile.PlanType != "team" {
		t.Fatalf("profile = %+v", profile)
	}
}

func TestParseAccountProfileAcceptsWorkspaceIDAndObjectKey(t *testing.T) {
	tests := []struct {
		name    string
		payload string
		wantID  string
	}{
		{
			name:    "nested account id",
			payload: `{"accounts":{"default":{"account":{"id":"acct-id","plan_type":"plus"}}}}`,
			wantID:  "acct-id",
		},
		{
			name:    "root id",
			payload: `{"accounts":{"default":{"id":"root-id","plan_type":"team"}}}`,
			wantID:  "root-id",
		},
		{
			name:    "non-default object key",
			payload: `{"accounts":{"workspace-key":{"plan_type":"enterprise"}}}`,
			wantID:  "workspace-key",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			profile, err := ParseAccountProfile([]byte(test.payload))
			if err != nil {
				t.Fatalf("ParseAccountProfile() error = %v", err)
			}
			if profile.AccountID != test.wantID {
				t.Fatalf("account ID = %q, want %q", profile.AccountID, test.wantID)
			}
		})
	}
}

func TestParseAccountProfileFallsBackToLegacyEntitlementPlan(t *testing.T) {
	for _, test := range []struct {
		name string
		body string
		want string
	}{
		{
			name: "subscription plan",
			body: `{
				"accounts":{"default":{
					"account":{"account_id":"acct-plus"},
					"entitlement":{"subscription_plan":"ChatGPTPlusPlan"}
				}}
			}`,
			want: "plus",
		},
		{
			name: "last active subscription",
			body: `{
				"accounts":{"default":{
					"account":{"account_id":"acct-team"},
					"last_active_subscription":{"plan_type":"ChatGPTBusinessPlan"}
				}}
			}`,
			want: "team",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			profile, err := ParseAccountProfile([]byte(test.body))
			if err != nil {
				t.Fatalf("ParseAccountProfile() error = %v", err)
			}
			if profile.PlanType != test.want {
				t.Fatalf("plan type = %q, want %q", profile.PlanType, test.want)
			}
		})
	}
}

func TestParseAccountProfileForAccountSelectionOrder(t *testing.T) {
	payload := []byte(`{
		"accounts":{
			"default":{"account":{"account_id":"acct-default","plan_type":"free"}},
			"team":{"account_id":"acct-team","plan_type":"team"},
			"plus":{"account":{"account_id":"acct-plus","plan_type":"plus"}}
		},
		"default_account_id":"acct-plus",
		"account_ordering":["missing","acct-default","acct-team"]
	}`)
	tests := []struct {
		name      string
		preferred string
		wantID    string
		wantPlan  string
	}{
		{name: "preferred", preferred: "acct-team", wantID: "acct-team", wantPlan: "team"},
		{name: "default account ID", wantID: "acct-plus", wantPlan: "plus"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			profile, err := ParseAccountProfileForAccount(payload, test.preferred)
			if err != nil {
				t.Fatalf("ParseAccountProfileForAccount() error = %v", err)
			}
			if profile.AccountID != test.wantID || profile.PlanType != test.wantPlan {
				t.Fatalf("profile = %+v, want account_id %q plan_type %q", profile, test.wantID, test.wantPlan)
			}
		})
	}
}

func TestParseAccountProfileForAccountRejectsMissingPreferredAccount(t *testing.T) {
	_, err := ParseAccountProfileForAccount([]byte(`{
		"accounts":{
			"default":{"account_id":"acct-default","plan_type":"free"},
			"team":{"account_id":"acct-team","plan_type":"team"}
		},
		"default_account_id":"acct-default"
	}`), "acct-missing")
	if !errors.Is(err, ErrAccountProfileIdentityMismatch) {
		t.Fatalf("ParseAccountProfileForAccount() error = %v, want identity mismatch", err)
	}
}

func TestParseAccountProfileForAccountUsesOrderingBeforeDefaultKey(t *testing.T) {
	profile, err := ParseAccountProfileForAccount([]byte(`{
		"accounts":{
			"default":{"account":{"account_id":"acct-default","plan_type":"free"}},
			"team":{"account_id":"acct-team","plan_type":"team"}
		},
		"account_ordering":["missing","acct-team","acct-default"]
	}`), "")
	if err != nil {
		t.Fatalf("ParseAccountProfileForAccount() error = %v", err)
	}
	if profile.AccountID != "acct-team" || profile.PlanType != "team" {
		t.Fatalf("profile = %+v", profile)
	}
}

func TestParseAccountProfileForAccountUsesDefaultObjectKey(t *testing.T) {
	profile, err := ParseAccountProfileForAccount([]byte(`{
		"accounts":{
			"default":{"account_id":"acct-default","plan_type":"free"},
			"team":{"account_id":"acct-team","plan_type":"team"}
		}
	}`), "")
	if err != nil {
		t.Fatalf("ParseAccountProfileForAccount() error = %v", err)
	}
	if profile.AccountID != "acct-default" || profile.PlanType != "free" {
		t.Fatalf("profile = %+v", profile)
	}
}

func TestParseAccountProfileForAccountAcceptsArray(t *testing.T) {
	profile, err := ParseAccountProfileForAccount([]byte(`{
		"accounts":[
			{"account":{"account_id":"acct-team","plan_type":"team"}},
			{"account_id":"acct-plus","plan_type":"plus"}
		],
		"default_account_id":"acct-plus"
	}`), "")
	if err != nil {
		t.Fatalf("ParseAccountProfileForAccount() error = %v", err)
	}
	if profile.AccountID != "acct-plus" || profile.PlanType != "plus" {
		t.Fatalf("profile = %+v", profile)
	}
}

func TestParseAccountProfileForAccountUsesUniqueCandidate(t *testing.T) {
	for _, payload := range []string{
		`{"accounts":{"team":{"account_id":"Account ID/01","plan_type":"team"}}}`,
		`{"accounts":[{"account":{"account_id":"Account ID/01","plan_type":"team"}}]}`,
	} {
		profile, err := ParseAccountProfileForAccount([]byte(payload), "")
		if err != nil {
			t.Fatalf("ParseAccountProfileForAccount() error = %v", err)
		}
		if profile.AccountID != "Account ID/01" || profile.PlanType != "team" {
			t.Fatalf("profile = %+v", profile)
		}
	}
}

func TestParseAccountProfileForAccountRejectsAmbiguousCandidates(t *testing.T) {
	for _, payload := range []string{
		`{"accounts":{"one":{"account_id":"acct-1"},"two":{"account_id":"acct-2"}}}`,
		`{"accounts":[{"account_id":"acct-1"},{"account":{"account_id":"acct-2"}}]}`,
	} {
		if _, err := ParseAccountProfileForAccount([]byte(payload), ""); err == nil ||
			!strings.Contains(err.Error(), "multiple accounts") {
			t.Fatalf("ParseAccountProfileForAccount() error = %v, want multiple accounts", err)
		}
	}
}

func TestParseAccountProfileRequiresAccountID(t *testing.T) {
	if _, err := ParseAccountProfile([]byte(`{
		"accounts":{"default":{"account":{"plan_type":"team"}}}
	}`)); err == nil {
		t.Fatal("ParseAccountProfile() error = nil")
	}
}

func TestParseAccountProfileRejectsWhitespacePaddedAccountIdentity(t *testing.T) {
	for _, accountID := range []string{" acct-1", "acct-1 ", "\tacct-1", "acct-1\n"} {
		payload := []byte(`{"accounts":{"default":{"account":{"account_id":` +
			strconv.Quote(accountID) + `,"plan_type":"team"}}}}`)
		if _, err := ParseAccountProfile(payload); err == nil || !strings.Contains(err.Error(), "invalid account_id") {
			t.Fatalf("ParseAccountProfile(%q) error = %v, want invalid account_id", accountID, err)
		}
	}
}

func TestParseAccountProfileForAccountRejectsWhitespacePaddedSelectors(t *testing.T) {
	valid := []byte(`{"accounts":{"default":{"account_id":"acct-1"}}}`)
	tests := []struct {
		name      string
		payload   []byte
		preferred string
	}{
		{name: "preferred", payload: valid, preferred: " acct-1"},
		{name: "default account ID", payload: []byte(`{
			"accounts":{"default":{"account_id":"acct-1"}},
			"default_account_id":"acct-1 "
		}`)},
		{name: "account ordering", payload: []byte(`{
			"accounts":{"default":{"account_id":"acct-1"}},
			"account_ordering":["acct-1\t"]
		}`)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := ParseAccountProfileForAccount(test.payload, test.preferred); err == nil ||
				!strings.Contains(err.Error(), "invalid") {
				t.Fatalf("ParseAccountProfileForAccount() error = %v, want invalid selector", err)
			}
		})
	}
}

func TestCredentialApplyToMetadataPreservesOpaqueAccountID(t *testing.T) {
	const accountID = "Account ID/01"
	metadata := make(map[string]any)
	(&Credential{AccountID: accountID}).ApplyToMetadata(metadata)
	if got := metadata["account_id"]; got != accountID {
		t.Fatalf("account_id = %#v, want %q", got, accountID)
	}
}

func TestParseAccountProfileRejectsTrailingData(t *testing.T) {
	valid := `{"accounts":{"default":{"account":{"account_id":"acct-1","plan_type":"team"}}}}`
	for _, suffix := range []string{`{}`, `garbage`} {
		if _, err := ParseAccountProfile([]byte(valid + suffix)); err == nil {
			t.Fatalf("ParseAccountProfile() accepted trailing %q", suffix)
		}
	}
	if _, err := ParseAccountProfile([]byte(valid + " \n\t")); err != nil {
		t.Fatalf("ParseAccountProfile() rejected trailing whitespace: %v", err)
	}
}

func TestParseAccountProfileForAccountRejectsTrailingData(t *testing.T) {
	valid := `{"accounts":[{"account_id":"acct-1"}]}`
	if _, err := ParseAccountProfileForAccount([]byte(valid+`{}`), "acct-1"); err == nil {
		t.Fatal("ParseAccountProfileForAccount() accepted trailing JSON")
	}
}

func TestParseImageQuotaPreservesMissingFeatureAsUnknown(t *testing.T) {
	quota, err := ParseImageQuota([]byte(`{"limits_progress":[{"feature_name":"other","remaining":0}]}`))
	if err != nil {
		t.Fatalf("ParseImageQuota() error = %v", err)
	}
	if quota.Present {
		t.Fatalf("quota = %+v, want unknown", quota)
	}
}

func TestParseImageQuotaPreservesExplicitZero(t *testing.T) {
	quota, err := ParseImageQuota([]byte(`{
		"limits_progress":[{"feature_name":"image_gen","remaining":0,"reset_after":"2026-07-28T00:00:00Z"}]
	}`))
	if err != nil {
		t.Fatalf("ParseImageQuota() error = %v", err)
	}
	if !quota.FeaturePresent || !quota.Present || quota.Remaining != 0 ||
		!quota.ResetAt.Equal(time.Date(2026, 7, 28, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("quota = %+v", quota)
	}
}

func TestParseImageQuotaPreservesNullRemainingAsUnknown(t *testing.T) {
	quota, err := ParseImageQuota([]byte(`{
		"limits_progress":[{"feature_name":"image_gen","remaining":null,"reset_after":"2026-07-28T00:00:00Z"}]
	}`))
	if err != nil {
		t.Fatalf("ParseImageQuota() error = %v", err)
	}
	if !quota.FeaturePresent || quota.Present || quota.Remaining != 0 || !quota.ResetAt.IsZero() {
		t.Fatalf("quota = %+v, want observed unknown", quota)
	}
}

func TestParseImageQuotaAcceptsNumericStringAndUnixMilliseconds(t *testing.T) {
	quota, err := ParseImageQuota([]byte(`{
		"limits_progress":[{"feature_name":"image_gen","remaining":"12","reset_after":1785196800000}]
	}`))
	if err != nil {
		t.Fatalf("ParseImageQuota() error = %v", err)
	}
	if !quota.FeaturePresent || !quota.Present || quota.Remaining != 12 ||
		quota.ResetAt.UnixMilli() != 1785196800000 {
		t.Fatalf("quota = %+v", quota)
	}
}

func TestParseImageQuotaRejectsMalformedMatchedFeature(t *testing.T) {
	for _, payload := range []string{
		`{"limits_progress":[{"feature_name":"image_gen"}]}`,
		`{"limits_progress":[{"feature_name":"image_gen","remaining":-1}]}`,
		`{"limits_progress":[{"feature_name":"image_gen","remaining":1,"reset_after":"tomorrow"}]}`,
		`{"limits_progress":[{"feature_name":"image_gen","remaining":1,"reset_after":253402300800}]}`,
		`{"limits_progress":[{"feature_name":"image_gen","remaining":1,"reset_after":253402300800000}]}`,
	} {
		if _, err := ParseImageQuota([]byte(payload)); err == nil {
			t.Fatalf("ParseImageQuota(%s) error = nil", payload)
		}
	}
}

func TestParseImageQuotaRejectsTrailingData(t *testing.T) {
	valid := `{"limits_progress":[{"feature_name":"image_gen","remaining":1}]}`
	for _, suffix := range []string{`[]`, `garbage`} {
		if _, err := ParseImageQuota([]byte(valid + suffix)); err == nil {
			t.Fatalf("ParseImageQuota() accepted trailing %q", suffix)
		}
	}
	if _, err := ParseImageQuota([]byte(valid + " \n\t")); err != nil {
		t.Fatalf("ParseImageQuota() rejected trailing whitespace: %v", err)
	}
}
