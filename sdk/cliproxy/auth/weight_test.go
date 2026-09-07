package auth

import (
	"reflect"
	"testing"
	"time"
)

func TestCredentialWeightPrecedenceAndInvalidUpdates(t *testing.T) {
	credential := &Auth{ID: "weight-test", Provider: "codex", Metadata: map[string]any{"weight": 7}}
	if authWeight(credential) != 7 || authWeight(&Auth{}) != 1 {
		t.Fatal("missing weight or metadata default changed")
	}
	credential.Attributes = map[string]string{"weight": "3"}
	if authWeight(credential) != 3 {
		t.Fatal("attributes did not override metadata")
	}
	credential.Attributes["weight"] = ""
	if authWeight(credential) != 7 {
		t.Fatal("empty attribute did not inherit metadata")
	}
	credential.Attributes["weight"] = "-3"
	if authWeight(credential) != 0 {
		t.Fatal("non-positive weight was not normalized")
	}
	manager := NewManager(nil, nil, nil)
	installed, err := manager.Register(t.Context(), credential)
	if err != nil {
		t.Fatal(err)
	}
	for _, source := range []string{"attributes", "metadata"} {
		updated := installed.Clone()
		if source == "attributes" {
			updated.Attributes["weight"] = "invalid"
		} else {
			updated.Metadata["weight"] = true
		}
		if _, err := manager.Update(t.Context(), updated); err == nil {
			t.Fatal("invalid update replaced credential")
		}
		if _, err := manager.Register(t.Context(), updated); err == nil {
			t.Fatal("invalid registration replaced credential")
		}
		current, _ := manager.GetByID(credential.ID)
		if authWeight(current) != 0 || current.RuntimeInstanceID() != installed.RuntimeInstanceID() {
			t.Fatal("rejected weight changed the installed instance")
		}
	}
}

func TestCredentialPreparedRefreshKeepsWeightSources(t *testing.T) {
	for name, credential := range map[string]*Auth{
		"missing":              {ID: "weight", Provider: "codex"},
		"metadata only":        {ID: "weight", Provider: "codex", Metadata: map[string]any{"weight": 7}},
		"attribute precedence": {ID: "weight", Provider: "codex", Attributes: map[string]string{"weight": "3"}, Metadata: map[string]any{"weight": 7}},
		"empty attribute":      {ID: "weight", Provider: "codex", Attributes: map[string]string{"weight": ""}, Metadata: map[string]any{"weight": 7}},
	} {
		t.Run(name, func(t *testing.T) {
			manager := NewManager(nil, nil, nil)
			current, err := manager.Register(WithSkipPersist(t.Context()), credential)
			if err != nil {
				t.Fatal(err)
			}
			refreshed := current.Clone()
			refreshed.Attributes = map[string]string{"weight": "2"}
			refreshed.Metadata = map[string]any{"weight": 2}
			installed, err := manager.installPreparedRequestAuth(WithSkipPersist(t.Context()), current, refreshed, true)
			if err != nil || installed == nil {
				t.Fatalf("prepared refresh: %v", err)
			}
			oldAttribute, oldAttributeSet := current.Attributes[AttributeWeight]
			attribute, attributeSet := installed.Attributes[AttributeWeight]
			oldMetadata, oldMetadataSet := current.Metadata[AttributeWeight]
			metadata, metadataSet := installed.Metadata[AttributeWeight]
			if oldAttribute != attribute || oldAttributeSet != attributeSet || !reflect.DeepEqual(oldMetadata, metadata) || oldMetadataSet != metadataSet {
				t.Fatal("prepared refresh introduced or replaced a local weight source")
			}
		})
	}
}

func TestCredentialRefreshKeepsConfiguredWeightAndUserEditsRemainWritable(t *testing.T) {
	m := NewManager(nil, nil, nil)
	a := &Auth{ID: "refresh-weight", Provider: "codex", Attributes: map[string]string{"weight": "5"}}
	a.Metadata = map[string]any{"weight": 5}
	current, err := m.Register(WithSkipPersist(t.Context()), a)
	if err != nil {
		t.Fatal(err)
	}
	refreshed := current.Clone()
	refreshed.Attributes[AttributeWeight] = "2"
	refreshed.Metadata[AttributeWeight] = 2
	installed, err := m.applyRefreshedAuth(WithSkipPersist(t.Context()), current, current, refreshed, time.Time{})
	if err != nil || installed == nil || authWeight(installed) != 5 {
		t.Fatal("refresh changed a configured routing weight")
	}
	edited := installed.Clone()
	edited.Attributes[AttributeWeight] = "0"
	edited.Metadata[AttributeWeight] = 0
	updated, changed, err := m.UpdateIfCurrent(WithSkipPersist(t.Context()), installed, edited)
	if err != nil || !changed || authWeight(updated) != 0 {
		t.Fatal("refresh protection blocked an intentional weight edit")
	}
	if stale, err := m.applyRefreshedAuth(WithSkipPersist(t.Context()), installed, installed, refreshed, time.Time{}); err != nil || stale != nil {
		t.Fatal("an old refresh replaced the user's newer weight")
	}
	if latest, _ := m.GetByID(a.ID); authWeight(latest) != 0 {
		t.Fatal("stale refresh restored a cleared participation weight")
	}
}
