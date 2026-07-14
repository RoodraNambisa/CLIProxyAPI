package authfileguard

import (
	"context"
	"errors"
	"testing"
)

func TestExpectedDeleteHash(t *testing.T) {
	ctx := WithExpectedDeleteHash(context.Background(), " hash ")
	if got := ExpectedDeleteHash(ctx); got != "hash" {
		t.Fatalf("ExpectedDeleteHash() = %q, want hash", got)
	}
	if got := ExpectedDeleteHash(context.Background()); got != "" {
		t.Fatalf("ExpectedDeleteHash() without value = %q, want empty", got)
	}
}

func TestDeleteGenerationBindsBackendIdentity(t *testing.T) {
	generation := NewDeleteGeneration("hash")
	if !generation.BindBackendIdentity("object:auth.json", "etag-a") {
		t.Fatal("first backend identity was rejected")
	}
	if !generation.BindBackendIdentity("object:auth.json", "etag-a") {
		t.Fatal("same backend identity was rejected")
	}
	if generation.BindBackendIdentity("object:auth.json", "etag-b") {
		t.Fatal("replacement backend identity was accepted")
	}
}

func TestDeleteGenerationRejectsRetryWithoutStableBackendIdentity(t *testing.T) {
	generation := NewDeleteGeneration("hash")
	if !generation.BindBackendIdentityForRetry("object:auth.json", "etag", false) {
		t.Fatal("first backend attempt was rejected")
	}
	if generation.BindBackendIdentityForRetry("object:auth.json", "etag", false) {
		t.Fatal("retry without stable backend generation was accepted")
	}
}

func TestDeleteGenerationRetryCannotBindMissingFirstIdentity(t *testing.T) {
	generation := NewDeleteGeneration("hash")
	if generation.MatchBackendIdentity("object:auth.json", "version-b", true, false) {
		t.Fatal("retry bound a backend identity that the first attempt never observed")
	}
}

func TestDeleteGenerationPersistsAndRestoresBackendIdentity(t *testing.T) {
	generation := NewDeleteGeneration("hash")
	var persisted DeleteGenerationSnapshot
	generation.SetPersistHook(func(snapshot DeleteGenerationSnapshot) error {
		persisted = snapshot
		return nil
	})
	if got := generation.CheckBackendIdentity("postgres:auth.json", "42", true, true); got != DeleteIdentityMatched {
		t.Fatalf("initial identity result = %v, want matched", got)
	}
	identity := persisted.Identities["postgres:auth.json"]
	if persisted.Generation == "" || persisted.ExpectedHash != "hash" || identity.Value != "42" || !identity.RetrySafe {
		t.Fatalf("persisted snapshot = %#v", persisted)
	}

	restored := NewDeleteGenerationFromSnapshot(persisted)
	if restored.GenerationID() != persisted.Generation {
		t.Fatalf("restored generation = %q, want %q", restored.GenerationID(), persisted.Generation)
	}
	if got := restored.CheckBackendIdentity("postgres:auth.json", "42", true, false); got != DeleteIdentityMatched {
		t.Fatalf("restored identity result = %v, want matched", got)
	}
	if got := restored.CheckBackendIdentity("postgres:auth.json", "43", true, false); got != DeleteIdentityUncertain {
		t.Fatalf("replacement identity result = %v, want uncertain", got)
	}
}

func TestDeleteGenerationRejectsIdentityWhenDurableBindingFails(t *testing.T) {
	generation := NewDeleteGeneration("hash")
	generation.SetPersistHook(func(DeleteGenerationSnapshot) error { return errors.New("disk full") })
	if got := generation.CheckBackendIdentity("git:auth.json", "commit-a", true, true); got != DeleteIdentityUncertain {
		t.Fatalf("identity result = %v, want uncertain", got)
	}
	if len(generation.Snapshot().Identities) != 0 {
		t.Fatal("failed durable binding remained in memory")
	}
}

func TestDeleteIdentityBindingContextIsExplicit(t *testing.T) {
	if DeleteIdentityBindingAllowed(context.Background()) {
		t.Fatal("identity binding is enabled without an explicit live-attempt marker")
	}
	if !DeleteIdentityBindingAllowed(WithDeleteIdentityBinding(context.Background())) {
		t.Fatal("identity binding marker was not preserved")
	}
}
