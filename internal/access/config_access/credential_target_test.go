package configaccess

import (
	"net/http/httptest"
	"testing"

	sdkaccess "github.com/router-for-me/CLIProxyAPI/v6/sdk/access"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
)

func TestCredentialTargetKeyKeepsIssuerAndRestrictions(t *testing.T) {
	provider := newProvider("", []string{"base", "exact-auth-name", "disabled"}, []sdkconfig.APIKeyGroup{{APIKey: "base", Providers: []string{"xai"}, AllowCredentialTargeting: true}})
	for _, source := range []string{"Authorization", "X-Api-Key", "X-Goog-Api-Key", "key", "auth_token"} {
		t.Run(source, func(t *testing.T) {
			req := httptest.NewRequest("POST", "/v1/responses", nil)
			if source == "key" || source == "auth_token" {
				query := req.URL.Query()
				query.Set(source, "base-auth-Grok-Test")
				req.URL.RawQuery = query.Encode()
			} else if source == "Authorization" {
				req.Header.Set(source, "Bearer base-auth-Grok-Test")
			} else {
				req.Header.Set(source, "base-auth-Grok-Test")
			}
			result, err := provider.Authenticate(t.Context(), req)
			if err != nil || result.Principal != "base" || result.Metadata[sdkaccess.MetadataCredentialTarget] != "grok-test" || result.Metadata[sdkaccess.MetadataAllowedProviders] != "xai" {
				t.Fatalf("targeted authentication lost its issuer or restrictions: %+v %v", result, err)
			}
		})
	}
	for _, tc := range []struct {
		key    string
		status int
	}{
		{"disabled-auth-name", 403}, {"missing-auth-name", 401},
		{"base-auth-", 400}, {"base-auth-../name", 400},
		{"exact-auth-name", 0}, {"base", 0}, {"base-auth-child-auth-one", 0},
	} {
		req := httptest.NewRequest("GET", "/v1/models", nil)
		req.Header.Set("Authorization", "Bearer "+tc.key)
		result, err := provider.Authenticate(t.Context(), req)
		if tc.status != 0 {
			if err == nil || err.HTTPStatusCode() != tc.status {
				t.Fatalf("%s status: %v", tc.key, err)
			}
			continue
		}
		if err != nil || result == nil {
			t.Fatalf("%s failed: %v", tc.key, err)
		}
		if tc.key == "exact-auth-name" && (result.Principal != tc.key || result.Metadata[sdkaccess.MetadataCredentialTarget] != "") {
			t.Fatal("an exact configured key was parsed as a selector")
		}
	}
}

func TestCredentialTargetOptionsComeOnlyFromTheIssuingKey(t *testing.T) {
	groups := []sdkconfig.APIKeyGroup{
		{APIKey: "raw", AllowCredentialTargeting: true},
		{APIKey: "state", AllowCredentialTargeting: true, CredentialTargetRespectStatePolicy: true},
		{APIKey: "limit", AllowCredentialTargeting: true, CredentialTargetRespectRequestLimit: true},
		{APIKey: "rewrite", AllowCredentialTargeting: true, CredentialTargetResponseModelRewrite: true},
	}
	p := newProvider("", []string{"raw", "state", "limit", "rewrite"}, groups)
	for _, key := range []string{"raw", "state", "limit", "rewrite"} {
		for _, targeted := range []bool{false, true} {
			value := key
			if targeted {
				value += "-auth-test"
			}
			req := httptest.NewRequest("POST", "/v1/responses?credential_target_respect_state_policy=true", nil)
			req.Header.Set("Authorization", "Bearer "+value)
			req.Header.Set(sdkaccess.MetadataCredentialTargetRespectStatePolicy, "true")
			req.Header.Set(sdkaccess.MetadataCredentialTargetRespectRequestLimit, "true")
			req.Header.Set(sdkaccess.MetadataCredentialTargetResponseModelRewrite, "true")
			result, err := p.Authenticate(t.Context(), req)
			if err != nil {
				t.Fatal(err)
			}
			if (result.Metadata[sdkaccess.MetadataCredentialTargetRespectStatePolicy] == "true") != (targeted && key == "state") ||
				(result.Metadata[sdkaccess.MetadataCredentialTargetRespectRequestLimit] == "true") != (targeted && key == "limit") ||
				(result.Metadata[sdkaccess.MetadataCredentialTargetResponseModelRewrite] == "true") != (targeted && key == "rewrite") {
				t.Fatalf("key options leaked or accepted untrusted headers: %+v", result.Metadata)
			}
		}
	}
}
