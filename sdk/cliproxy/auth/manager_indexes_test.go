package auth

import (
	"path/filepath"
	"reflect"
	"sort"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/authfileguard"
	internalconfig "github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

func TestManagerBackingPathIndexTracksSharedPathsAndReplacement(t *testing.T) {
	firstRoot := t.TempDir()
	secondRoot := t.TempDir()
	manager := NewManager(nil, nil, nil)
	manager.SetConfig(&internalconfig.Config{AuthDir: firstRoot})
	register := func(id, path string) {
		t.Helper()
		_, errRegister := manager.Register(WithSkipPersist(t.Context()), &Auth{
			ID:         id,
			Provider:   "claude",
			FileName:   id + ".json",
			Status:     StatusActive,
			Attributes: map[string]string{"path": path},
		})
		if errRegister != nil {
			t.Fatal(errRegister)
		}
	}
	register("first", "shared.json")
	register("second", "shared.json")

	assertAuthIDsForPath(t, manager, filepath.Join(firstRoot, "shared.json"), "first", "second")
	first, _ := manager.GetByID("first")
	first.Attributes["path"] = "moved.json"
	if _, errUpdate := manager.Update(WithSkipPersist(t.Context()), first); errUpdate != nil {
		t.Fatal(errUpdate)
	}
	assertAuthIDsForPath(t, manager, filepath.Join(firstRoot, "shared.json"), "second")
	assertAuthIDsForPath(t, manager, filepath.Join(firstRoot, "moved.json"), "first")

	manager.SetConfig(&internalconfig.Config{AuthDir: secondRoot})
	assertAuthIDsForPath(t, manager, filepath.Join(firstRoot, "moved.json"))
	assertAuthIDsForPath(t, manager, filepath.Join(secondRoot, "moved.json"), "first")
	if errDelete := manager.Delete(WithSkipPersist(t.Context()), "first"); errDelete != nil {
		t.Fatal(errDelete)
	}
	assertAuthIDsForPath(t, manager, filepath.Join(secondRoot, "moved.json"))
	assertManagerAuthIndexesConsistent(t, manager)
}

func TestManagerDependencyIndexIncludesPersistedOnlyAuthsAndDirtyState(t *testing.T) {
	authDir := t.TempDir()
	source := dependencyTestCodexAuth("source", "uid-a")
	source.Attributes = map[string]string{"path": filepath.Join(authDir, source.FileName)}
	web := dependencyTestWebAuth("web", "uid-a")
	web.Attributes = map[string]string{"path": filepath.Join(authDir, web.FileName)}
	store := newChatGPTWebDependencyTestStore(source, web)
	manager := NewManager(store, nil, nil)
	manager.SetConfig(&internalconfig.Config{AuthDir: authDir})

	authfileguard.MarkQuarantined(source.Attributes["path"])
	t.Cleanup(func() { authfileguard.ClearQuarantined(source.Attributes["path"]) })
	if errLoad := manager.Load(t.Context()); errLoad != nil {
		t.Fatal(errLoad)
	}
	if _, loaded := manager.GetByID(source.ID); loaded {
		t.Fatal("quarantined source unexpectedly entered the runtime view")
	}
	graph, complete := manager.ChatGPTWebDependencyIndexSnapshot()
	indexedSource, ambiguous := graph.SourceByUID("uid-a")
	if !complete || ambiguous || indexedSource == nil || indexedSource.ID != source.ID {
		t.Fatalf("dependency index source = %#v, complete = %v, ambiguous = %v", indexedSource, complete, ambiguous)
	}
	dependents, ambiguous := graph.DependentsForSource(source)
	if ambiguous || len(dependents) != 1 || dependents[0].ID != web.ID {
		t.Fatalf("dependency index dependents = %#v, ambiguous = %v", dependents, ambiguous)
	}
	directSource, ambiguous, directComplete := manager.ChatGPTWebSourceByCredentialUID("uid-a")
	if !directComplete || ambiguous || directSource == nil || directSource.ID != source.ID {
		t.Fatalf("direct source = %#v, complete = %v, ambiguous = %v", directSource, directComplete, ambiguous)
	}
	directDependents, ambiguous, directComplete := manager.ChatGPTWebDependentsForSource(source)
	if !directComplete || ambiguous || len(directDependents) != 1 || directDependents[0].ID != web.ID {
		t.Fatalf("direct dependents = %#v, complete = %v, ambiguous = %v", directDependents, directComplete, ambiguous)
	}
	assertAuthIDsForPath(t, manager, source.Attributes["path"])

	manager.MarkChatGPTWebDependencyIndexDirty()
	if _, complete = manager.ChatGPTWebDependencyIndexSnapshot(); complete {
		t.Fatal("dependency index remained complete after being marked dirty")
	}
	if _, errSnapshot := manager.PersistedAuthSnapshot(t.Context()); errSnapshot != nil {
		t.Fatal(errSnapshot)
	}
	if _, complete = manager.ChatGPTWebDependencyIndexSnapshot(); !complete {
		t.Fatal("successful persisted snapshot did not restore completeness")
	}
	manager.SetStore(store)
	if _, complete = manager.ChatGPTWebDependencyIndexSnapshot(); complete {
		t.Fatal("replacing the store did not mark the dependency index dirty")
	}
	assertManagerAuthIndexesConsistent(t, manager)
}

func TestManagerDependencyIndexTracksDuplicateUIDAndRuntimeChanges(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	first := dependencyTestCodexAuth("source-a", "duplicate")
	second := dependencyTestCodexAuth("source-b", "duplicate")
	web := dependencyTestWebAuth("web", "duplicate")
	for _, auth := range []*Auth{first, second, web} {
		if _, errRegister := manager.Register(WithSkipPersist(t.Context()), auth); errRegister != nil {
			t.Fatal(errRegister)
		}
	}
	graph, complete := manager.ChatGPTWebDependencyIndexSnapshot()
	if complete {
		t.Fatal("runtime-only dependency index unexpectedly reported a complete persisted view")
	}
	if source, ambiguous := graph.SourceByUID("duplicate"); source != nil || !ambiguous {
		t.Fatalf("duplicate source = %#v, ambiguous = %v", source, ambiguous)
	}
	if source, ambiguous, _ := manager.ChatGPTWebSourceByCredentialUID("duplicate"); source != nil || !ambiguous {
		t.Fatalf("direct duplicate source = %#v, ambiguous = %v", source, ambiguous)
	}
	if errDelete := manager.Delete(WithSkipPersist(t.Context()), second.ID); errDelete != nil {
		t.Fatal(errDelete)
	}
	graph, _ = manager.ChatGPTWebDependencyIndexSnapshot()
	resolved, ambiguous := graph.SourceByUID("duplicate")
	if ambiguous || resolved == nil || resolved.ID != first.ID {
		t.Fatalf("resolved source = %#v, ambiguous = %v", resolved, ambiguous)
	}
	assertManagerAuthIndexesConsistent(t, manager)
}

func TestManagerSkipPersistDeleteMarksDependencyIndexDirtyAndRestoresPersistedEntry(t *testing.T) {
	source := dependencyTestCodexAuth("source", "persisted-uid")
	store := newChatGPTWebDependencyTestStore(source)
	manager := NewManager(store, nil, nil)
	if errLoad := manager.Load(t.Context()); errLoad != nil {
		t.Fatal(errLoad)
	}
	if _, complete := manager.ChatGPTWebDependencyIndexSnapshot(); !complete {
		t.Fatal("loaded dependency index is incomplete")
	}
	if errDelete := manager.Delete(WithSkipPersist(t.Context()), source.ID); errDelete != nil {
		t.Fatal(errDelete)
	}
	if _, complete := manager.ChatGPTWebDependencyIndexSnapshot(); complete {
		t.Fatal("skip-persist removal left the dependency index complete")
	}
	if _, errSnapshot := manager.PersistedAuthSnapshot(t.Context()); errSnapshot != nil {
		t.Fatal(errSnapshot)
	}
	restored, ambiguous, complete := manager.ChatGPTWebSourceByCredentialUID("persisted-uid")
	if !complete || ambiguous || restored == nil || restored.ID != source.ID {
		t.Fatalf("restored source = %#v, ambiguous=%v, complete=%v", restored, ambiguous, complete)
	}
}

func assertAuthIDsForPath(t *testing.T, manager *Manager, path string, want ...string) {
	t.Helper()
	auths := manager.AuthsForBackingPath(path)
	got := make([]string, 0, len(auths))
	for _, auth := range auths {
		got = append(got, auth.ID)
	}
	sort.Strings(want)
	match := len(got) == len(want)
	for index := range got {
		if !match || got[index] != want[index] {
			match = false
			break
		}
	}
	if !match {
		t.Fatalf("auth IDs for %q = %v, want %v", path, got, want)
	}
}

func assertManagerAuthIndexesConsistent(t *testing.T, manager *Manager) {
	t.Helper()
	manager.mu.RLock()
	defer manager.mu.RUnlock()
	expectedPaths := make(map[string]map[string]struct{})
	expectedByID := make(map[string]string)
	expectedDependencies := make(map[string]*Auth)
	for id, auth := range manager.auths {
		if key := authBackingPathKey(auth, manager.currentConfig()); key != "" {
			ids := expectedPaths[key]
			if ids == nil {
				ids = make(map[string]struct{})
				expectedPaths[key] = ids
			}
			ids[id] = struct{}{}
			expectedByID[id] = key
		}
		if authRelevantToChatGPTWebDependencyIndex(auth) {
			expectedDependencies[id] = auth
		}
	}
	if !reflect.DeepEqual(manager.backingPathAuthIDs, expectedPaths) || !reflect.DeepEqual(manager.backingPathByAuthID, expectedByID) {
		t.Fatalf("backing path index is inconsistent: paths=%v byID=%v", manager.backingPathAuthIDs, manager.backingPathByAuthID)
	}
	for id, auth := range expectedDependencies {
		indexed := manager.dependencyAuthsByID[id]
		if indexed == nil || indexed.Provider != auth.Provider || ChatGPTWebCredentialUID(indexed) != ChatGPTWebCredentialUID(auth) || ChatGPTWebLinkedSourceUID(indexed) != ChatGPTWebLinkedSourceUID(auth) {
			t.Fatalf("dependency index entry %q = %#v, want %#v", id, indexed, auth)
		}
	}
	for id := range manager.dependencyAuthsByID {
		if manager.auths[id] != nil && expectedDependencies[id] == nil {
			t.Fatalf("unexpected runtime dependency index entry %q", id)
		}
	}
}
