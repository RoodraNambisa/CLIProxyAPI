package chatgptweb

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"regexp"
	"testing"
)

func TestPKCEStateNonceAndDeviceID(t *testing.T) {
	t.Parallel()
	random := bytes.NewReader(bytes.Repeat([]byte{0xa5}, 112))
	pkce, err := GeneratePKCE(random)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256([]byte(pkce.CodeVerifier))
	wantChallenge := base64.RawURLEncoding.EncodeToString(digest[:])
	if pkce.CodeChallenge != wantChallenge {
		t.Fatalf("CodeChallenge = %q, want %q", pkce.CodeChallenge, wantChallenge)
	}
	if len(pkce.CodeVerifier) != 43 {
		t.Fatalf("CodeVerifier length = %d, want 43", len(pkce.CodeVerifier))
	}

	state, err := GenerateState(random)
	if err != nil {
		t.Fatal(err)
	}
	nonce, err := GenerateNonce(random)
	if err != nil {
		t.Fatal(err)
	}
	if len(state) != 43 || len(nonce) != 43 {
		t.Fatalf("state/nonce lengths = %d/%d, want 43/43", len(state), len(nonce))
	}
	deviceID, err := GenerateDeviceID(random)
	if err != nil {
		t.Fatal(err)
	}
	if !regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`).MatchString(deviceID) {
		t.Fatalf("GenerateDeviceID() = %q, want UUIDv4", deviceID)
	}
}
