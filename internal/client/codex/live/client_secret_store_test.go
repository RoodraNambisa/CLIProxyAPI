package live

import (
	"bytes"
	"crypto/rand"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"
)

func preparedSecret(t *testing.T, owner callOwner, now time.Time) (string, ClientSecretAuthorization) {
	t.Helper()
	token, a, errPrepare := prepareClientSecret(rand.Reader, now, []byte(`{"type":"realtime","model":"public","large":90071992547409931234}`), "public", owner, clientSecretDefaultLifetime)
	if errPrepare != nil {
		t.Fatal(errPrepare)
	}
	return token, a
}

func TestLiveClientSecretStoreOwnsCopiesAndExpiration(t *testing.T) {
	s := newClientSecretStore()
	defer s.close()
	now := time.Unix(1800000000, 0)
	s.now = func() time.Time { return now }
	token, a := preparedSecret(t, callOwner{1}, now)
	original := bytes.Clone(a.session)
	if errPut := s.put(token, a); errPut != nil {
		t.Fatal(errPut)
	}
	a.session[0] = 'x'
	first, errAuth := s.authenticate(token)
	if errAuth != nil || !bytes.Equal(first.session, original) || first.owner != (callOwner{1}) || first.model != "public" {
		t.Fatal("stored capability lost scope or owned session data")
	}
	first.session[0] = 'x'
	second, errAuth := s.authenticate(token)
	if errAuth != nil || !bytes.Equal(second.session, original) {
		t.Fatal("returned capability changed stored data")
	}
	now = now.Add(clientSecretDefaultLifetime - time.Nanosecond)
	if _, errAuth := s.authenticate(token); errAuth != nil {
		t.Fatal("secret expired early")
	}
	now = now.Add(time.Nanosecond)
	if _, errAuth := s.authenticate(token); !errors.Is(errAuth, errInvalidClientSecret) {
		t.Fatal("expired secret remained usable")
	}
}

func TestLiveClientSecretStoreCapacityAndExpiredRecovery(t *testing.T) {
	s := newClientSecretStore()
	defer s.close()
	now := time.Unix(1800000000, 0)
	s.now = func() time.Time { return now }
	for i := 0; i < clientSecretMaxEntries; i++ {
		owner := callOwner{byte(i/clientSecretMaxEntriesPerIssuer + 1)}
		token, a := preparedSecret(t, owner, now)
		if errPut := s.put(token, a); errPut != nil {
			t.Fatal(errPut)
		}
		if i == clientSecretMaxEntriesPerIssuer-1 {
			other, b := preparedSecret(t, owner, now)
			if errPut := s.put(other, b); !errors.Is(errPut, errClientSecretCapacity) {
				t.Fatal("issuer capacity was not enforced")
			}
		}
	}
	token, a := preparedSecret(t, callOwner{100}, now)
	if errPut := s.put(token, a); !errors.Is(errPut, errClientSecretCapacity) {
		t.Fatal("total capacity was not enforced")
	}
	now = now.Add(clientSecretDefaultLifetime)
	token, a = preparedSecret(t, callOwner{1}, now)
	if errPut := s.put(token, a); errPut != nil {
		t.Fatal("expired entries did not release capacity")
	}
	if len(s.entries) != 1 {
		t.Fatal("expired records remained in registry")
	}
}

func TestLiveClientSecretStoreCollisionAndDiscardKeepOriginal(t *testing.T) {
	s := newClientSecretStore()
	defer s.close()
	now := time.Now()
	token, a := preparedSecret(t, callOwner{1}, now)
	if errPut := s.put(token, a); errPut != nil {
		t.Fatal(errPut)
	}
	other, b := preparedSecret(t, callOwner{2}, now)
	if errPut := s.put(token, b); !errors.Is(errPut, errClientSecretCollision) {
		t.Fatal("token collision replaced capability")
	}
	b.principal = a.principal
	if errPut := s.put(other, b); !errors.Is(errPut, errClientSecretCollision) {
		t.Fatal("session ID collision was accepted")
	}
	s.remove(token, "another-principal")
	if got, errAuth := s.authenticate(token); errAuth != nil || got.owner != a.owner {
		t.Fatal("stale cleanup deleted original capability")
	}
	s.remove(token, a.principal)
	if _, errAuth := s.authenticate(token); errAuth == nil {
		t.Fatal("discarded secret remained usable")
	}
}

func TestLiveClientSecretStoreConcurrentShutdownPreventsRepublishing(t *testing.T) {
	s := newClientSecretStore()
	token, a := preparedSecret(t, callOwner{1}, time.Now())
	var workers sync.WaitGroup
	for i := 0; i < 50; i++ {
		workers.Go(func() { _ = s.put(token, a); _, _ = s.authenticate(token) })
		workers.Go(s.close)
	}
	workers.Wait()
	if errPut := s.put(token, a); errPut == nil {
		t.Fatal("closed capability store reopened")
	}
	if _, errAuth := s.authenticate(token); errAuth == nil {
		t.Fatal("shutdown capability remained usable")
	}
}

func TestLiveClientSecretPreparationRejectsInvalidScopeAndEntropyFailure(t *testing.T) {
	for _, lifetime := range []time.Duration{0, clientSecretMinimumLifetime - time.Nanosecond, clientSecretMaximumLifetime + time.Nanosecond} {
		if _, _, errPrepare := prepareClientSecret(rand.Reader, time.Now(), []byte(`{}`), "model", callOwner{1}, lifetime); errPrepare == nil {
			t.Fatal("invalid lifetime accepted")
		}
	}
	for _, session := range []string{"null", "[]", strings.Repeat("x", clientSecretMaxBodySize+1)} {
		if _, _, errPrepare := prepareClientSecret(rand.Reader, time.Now(), []byte(session), "model", callOwner{1}, clientSecretDefaultLifetime); errPrepare == nil {
			t.Fatal("invalid session accepted")
		}
	}
	if _, _, errPrepare := prepareClientSecret(strings.NewReader("short"), time.Now(), []byte(`{}`), "model", callOwner{1}, clientSecretDefaultLifetime); !errors.Is(errPrepare, io.ErrUnexpectedEOF) {
		t.Fatal("partial entropy accepted or original failure lost")
	}
	for _, token := range []string{"", "ek_short", "Bearer ek_fixture", "ek_" + strings.Repeat("!", 43)} {
		if validClientSecretToken(token) {
			t.Fatal("invalid opaque token accepted")
		}
	}
}
