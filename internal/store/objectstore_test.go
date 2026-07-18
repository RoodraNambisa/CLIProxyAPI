package store

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/authfileguard"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

func TestSanitizeObjectStoreRequestErrorRemovesPresignedURL(t *testing.T) {
	wantErr := errors.New("transport failed")
	errSanitized := sanitizeObjectStoreRequestError(&url.Error{
		Op:  http.MethodDelete,
		URL: "https://object.example/auth.json?X-Amz-Credential=secret",
		Err: wantErr,
	})
	if !errors.Is(errSanitized, wantErr) {
		t.Fatalf("sanitizeObjectStoreRequestError() error = %v, want %v", errSanitized, wantErr)
	}
	if strings.Contains(errSanitized.Error(), "X-Amz-Credential") {
		t.Fatalf("sanitized error leaked presigned URL: %v", errSanitized)
	}
}

func TestObjectTokenStorePersistenceHoldsPersistentTargetThroughUpload(t *testing.T) {
	for _, testCase := range []struct {
		name    string
		persist func(context.Context, *ObjectTokenStore, string) error
	}{
		{
			name: "save",
			persist: func(ctx context.Context, store *ObjectTokenStore, fileName string) error {
				_, errSave := store.Save(ctx, &cliproxyauth.Auth{
					ID:       fileName,
					FileName: fileName,
					Provider: "codex",
					Metadata: map[string]any{"type": "codex", "access_token": "save-token"},
				})
				return errSave
			},
		},
		{
			name: "watcher_persistence",
			persist: func(ctx context.Context, store *ObjectTokenStore, fileName string) error {
				path := filepath.Join(store.AuthDir(), fileName)
				if errWrite := os.WriteFile(path, []byte(`{"type":"codex","access_token":"watcher-token"}`), 0o600); errWrite != nil {
					return errWrite
				}
				return store.PersistAuthFiles(ctx, "", path)
			},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			remoteStarted := make(chan struct{}, 1)
			releaseRemote := make(chan struct{})
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case r.Method == http.MethodGet && r.URL.RawQuery == "location=":
					w.Header().Set("Content-Type", "application/xml")
					_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></LocationConstraint>`))
				case r.Method == http.MethodHead:
					w.Header().Set("Content-Type", "application/xml")
					w.WriteHeader(http.StatusNotFound)
				case r.Method == http.MethodPut:
					select {
					case remoteStarted <- struct{}{}:
					default:
					}
					<-releaseRemote
					w.Header().Set("ETag", `"saved"`)
					w.WriteHeader(http.StatusOK)
				default:
					w.WriteHeader(http.StatusMethodNotAllowed)
				}
			}))
			defer server.Close()

			store := newObjectTokenStoreForServer(t, server.URL)
			const fileName = "persistent-target.json"
			operationDone := make(chan error, 1)
			ctx := t.Context()
			go func() {
				operationDone <- testCase.persist(ctx, store, fileName)
			}()

			assertPersistentAuthTargetHeldDuringRemoteOperation(
				t,
				store.AuthDir(),
				fileName,
				remoteStarted,
				releaseRemote,
				operationDone,
			)
		})
	}
}

func TestObjectTokenStoreSyncAuthFromBucketRejectsSymlinkedSubdirectory(t *testing.T) {
	const payload = `{"type":"codex","access_token":"remote"}`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query()
		switch {
		case r.Method == http.MethodGet && query.Has("location"):
			w.Header().Set("Content-Type", "application/xml")
			_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></LocationConstraint>`))
		case r.Method == http.MethodGet && query.Get("prefix") != "":
			w.Header().Set("Content-Type", "application/xml")
			_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>test-bucket</Name><Prefix>auths/</Prefix><KeyCount>1</KeyCount><MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated><Contents><Key>auths/nested/auth.json</Key><LastModified>2026-01-01T00:00:00Z</LastModified><ETag>&quot;fixture&quot;</ETag><Size>47</Size><StorageClass>STANDARD</StorageClass></Contents></ListBucketResult>`))
		case strings.HasSuffix(r.URL.Path, "/auths/nested/auth.json") && r.Method == http.MethodHead:
			w.Header().Set("Content-Length", strconv.Itoa(len(payload)))
			w.Header().Set("ETag", `"fixture"`)
			w.WriteHeader(http.StatusOK)
		case strings.HasSuffix(r.URL.Path, "/auths/nested/auth.json") && r.Method == http.MethodGet:
			_, _ = w.Write([]byte(payload))
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()

	store := newObjectTokenStoreForServer(t, server.URL)
	externalDir := t.TempDir()
	if errLink := os.Symlink(externalDir, filepath.Join(store.AuthDir(), "nested")); errLink != nil {
		t.Skipf("symlink is unavailable: %v", errLink)
	}
	if errSync := store.syncAuthFromBucket(t.Context()); errSync == nil {
		t.Fatal("syncAuthFromBucket() error = nil, want symlinked subdirectory rejection")
	}
	if _, errStat := os.Stat(filepath.Join(externalDir, "auth.json")); !errors.Is(errStat, os.ErrNotExist) {
		t.Fatalf("external auth target was modified: %v", errStat)
	}
}

func TestObjectTokenStoreSaveRejectsPathOutsideAuthDirectory(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()
	store := newObjectTokenStoreForServer(t, server.URL)
	outsidePath := filepath.Join(t.TempDir(), "outside.json")
	auth := &cliproxyauth.Auth{
		ID: "outside.json", Provider: "codex",
		Attributes: map[string]string{"path": outsidePath},
		Metadata:   map[string]any{"type": "codex", "access_token": "token"},
	}
	if _, errSave := store.Save(t.Context(), auth); errSave == nil {
		t.Fatal("Save() error = nil, want outside-path rejection")
	}
	if _, errStat := os.Stat(outsidePath); !errors.Is(errStat, os.ErrNotExist) {
		t.Fatalf("outside auth path was modified: %v", errStat)
	}
}

func TestObjectTokenStoreSaveIfAbsentPreservesExistingLocalRecord(t *testing.T) {
	var putCalls atomic.Int32
	var headCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.RawQuery == "location=":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></LocationConstraint>`))
		case r.Method == http.MethodHead:
			headCalls.Add(1)
			w.WriteHeader(http.StatusNotFound)
		case r.Method == http.MethodPut:
			putCalls.Add(1)
			w.WriteHeader(http.StatusNoContent)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()
	store := newObjectTokenStoreForServer(t, server.URL)
	const (
		fileName = "existing.json"
		existing = `{"type":"codex","access_token":"existing-token"}`
	)
	path := filepath.Join(store.AuthDir(), fileName)
	if errWrite := os.WriteFile(path, []byte(existing), 0o600); errWrite != nil {
		t.Fatal(errWrite)
	}
	_, errSave := store.SaveIfAbsent(t.Context(), &cliproxyauth.Auth{
		ID:       fileName,
		FileName: fileName,
		Provider: "chatgpt-web",
		Metadata: map[string]any{"type": "chatgpt-web", "access_token": "replacement-token"},
	})
	if !errors.Is(errSave, cliproxyauth.ErrAuthAlreadyExists) {
		t.Fatalf("SaveIfAbsent() error = %v, want auth already exists", errSave)
	}
	data, errRead := os.ReadFile(path)
	if errRead != nil || string(data) != existing {
		t.Fatalf("existing file changed: content=%q error=%v", data, errRead)
	}
	if putCalls.Load() != 0 {
		t.Fatalf("PUT calls = %d, want 0", putCalls.Load())
	}
	if headCalls.Load() != 0 {
		t.Fatalf("HEAD calls = %d, want 0", headCalls.Load())
	}
}

func TestObjectTokenStoreSaveIfAbsentDoesNotInspectExistingRemoteRecord(t *testing.T) {
	var objectGetCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.RawQuery == "location=":
			w.Header().Set("Content-Type", "application/xml")
			_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></LocationConstraint>`))
		case r.Method == http.MethodHead:
			w.Header().Set("Content-Length", "49")
			w.Header().Set("ETag", `"retired"`)
			w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodGet:
			objectGetCalls.Add(1)
			w.WriteHeader(http.StatusInternalServerError)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()

	store := newObjectTokenStoreForServer(t, server.URL)
	const fileName = "existing-remote.json"
	_, errSave := store.SaveIfAbsent(t.Context(), &cliproxyauth.Auth{
		ID:       fileName,
		FileName: fileName,
		Provider: "chatgpt-web",
		Metadata: map[string]any{"type": "chatgpt-web", "access_token": "new-token"},
	})
	if !errors.Is(errSave, cliproxyauth.ErrAuthAlreadyExists) {
		t.Fatalf("SaveIfAbsent() error = %v, want auth already exists", errSave)
	}
	if objectGetCalls.Load() != 0 {
		t.Fatalf("remote object GET calls = %d, want 0", objectGetCalls.Load())
	}
	if _, errStat := os.Stat(filepath.Join(store.AuthDir(), fileName)); !errors.Is(errStat, os.ErrNotExist) {
		t.Fatalf("local auth exists after remote conflict: %v", errStat)
	}
}

func TestObjectTokenStoreSaveIfAbsentReportsConcurrentRemoteCreate(t *testing.T) {
	var headCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.RawQuery == "location=":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></LocationConstraint>`))
		case r.Method == http.MethodHead:
			if headCalls.Add(1) == 1 {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			w.Header().Set("ETag", `"concurrent"`)
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPut:
			w.WriteHeader(http.StatusPreconditionFailed)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()

	store := newObjectTokenStoreForServer(t, server.URL)
	const fileName = "concurrent.json"
	_, errSave := store.SaveIfAbsent(t.Context(), &cliproxyauth.Auth{
		ID:       fileName,
		FileName: fileName,
		Provider: "chatgpt-web",
		Metadata: map[string]any{"type": "chatgpt-web", "access_token": "new-token"},
	})
	if !errors.Is(errSave, cliproxyauth.ErrAuthAlreadyExists) {
		t.Fatalf("SaveIfAbsent() error = %v, want auth already exists", errSave)
	}
	if _, errStat := os.Stat(filepath.Join(store.AuthDir(), fileName)); !errors.Is(errStat, os.ErrNotExist) {
		t.Fatalf("local auth exists after conflict: %v", errStat)
	}
}

func TestObjectTokenStoreSaveIfAbsentReportsConditionalRequestConflictAsRolledBack(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.RawQuery == "location=":
			w.Header().Set("Content-Type", "application/xml")
			_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></LocationConstraint>`))
		case r.Method == http.MethodHead:
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusNotFound)
		case r.Method == http.MethodPut:
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusConflict)
			_, _ = w.Write([]byte(`<Error><Code>ConditionalRequestConflict</Code><Message>conflicting operation</Message></Error>`))
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()

	store := newObjectTokenStoreForServer(t, server.URL)
	const fileName = "conditional-request-conflict.json"
	_, errSave := store.SaveIfAbsent(t.Context(), &cliproxyauth.Auth{
		ID:       fileName,
		FileName: fileName,
		Provider: "chatgpt-web",
		Metadata: map[string]any{"type": "chatgpt-web", "access_token": "new-token"},
	})
	if !errors.Is(errSave, authfileguard.ErrPersistGenerationStale) {
		t.Fatalf("SaveIfAbsent() error = %v, want stale generation", errSave)
	}
	if outcome, ok := cliproxyauth.SaveOutcomeFromError(errSave); !ok || outcome != cliproxyauth.SaveOutcomeRolledBack {
		t.Fatalf("SaveIfAbsent() outcome = %v, %t; want rolled back", outcome, ok)
	}
	if _, errStat := os.Stat(filepath.Join(store.AuthDir(), fileName)); !errors.Is(errStat, os.ErrNotExist) {
		t.Fatalf("local auth exists after conflict: %v", errStat)
	}
}

func TestObjectTokenStoreSaveReportsConditionalReplacementAsRolledBack(t *testing.T) {
	for _, test := range []struct {
		name       string
		statusCode int
		errorCode  string
	}{
		{name: "precondition failed", statusCode: http.StatusPreconditionFailed, errorCode: "PreconditionFailed"},
		{name: "conditional request conflict", statusCode: http.StatusConflict, errorCode: "ConditionalRequestConflict"},
	} {
		t.Run(test.name, func(t *testing.T) {
			const remoteData = `{"type":"codex","access_token":"existing-token"}`
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case r.Method == http.MethodGet && r.URL.RawQuery == "location=":
					w.Header().Set("Content-Type", "application/xml")
					_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></LocationConstraint>`))
				case r.Method == http.MethodHead:
					w.Header().Set("Content-Length", strconv.Itoa(len(remoteData)))
					w.Header().Set("ETag", `"old"`)
					w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
					w.WriteHeader(http.StatusOK)
				case r.Method == http.MethodGet:
					w.Header().Set("Content-Length", strconv.Itoa(len(remoteData)))
					w.Header().Set("ETag", `"old"`)
					w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
					w.WriteHeader(http.StatusOK)
					_, _ = w.Write([]byte(remoteData))
				case r.Method == http.MethodPut:
					if got := r.Header.Get("If-Match"); got != `"old"` {
						t.Errorf("If-Match = %q, want %q", got, `"old"`)
					}
					w.Header().Set("Content-Type", "application/xml")
					w.WriteHeader(test.statusCode)
					_, _ = fmt.Fprintf(w, `<Error><Code>%s</Code><Message>etag changed</Message></Error>`, test.errorCode)
				default:
					w.WriteHeader(http.StatusMethodNotAllowed)
				}
			}))
			defer server.Close()

			store := newObjectTokenStoreForServer(t, server.URL)
			const fileName = "conditional-update.json"
			_, errSave := store.Save(t.Context(), &cliproxyauth.Auth{
				ID:       fileName,
				FileName: fileName,
				Provider: "codex",
				Metadata: map[string]any{"type": "codex", "access_token": "replacement-token"},
			})
			if !errors.Is(errSave, authfileguard.ErrPersistGenerationStale) {
				t.Fatalf("Save() error = %v, want stale generation", errSave)
			}
			if outcome, ok := cliproxyauth.SaveOutcomeFromError(errSave); !ok || outcome != cliproxyauth.SaveOutcomeRolledBack {
				t.Fatalf("Save() outcome = %v, %t; want rolled back", outcome, ok)
			}
			if _, errStat := os.Stat(filepath.Join(store.AuthDir(), fileName)); !errors.Is(errStat, os.ErrNotExist) {
				t.Fatalf("local auth exists after rolled-back replacement: %v", errStat)
			}
		})
	}
}

func TestObjectTokenStoreSaveReportsConditionalReplacementDeletedAsRolledBack(t *testing.T) {
	const remoteData = `{"type":"codex","access_token":"existing-token"}`
	var deleted atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.RawQuery == "location=":
			w.Header().Set("Content-Type", "application/xml")
			_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></LocationConstraint>`))
		case r.Method == http.MethodHead && deleted.Load():
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`<Error><Code>NoSuchKey</Code><Message>object was deleted</Message></Error>`))
		case r.Method == http.MethodHead:
			w.Header().Set("Content-Length", strconv.Itoa(len(remoteData)))
			w.Header().Set("ETag", `"old"`)
			w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodGet:
			w.Header().Set("Content-Length", strconv.Itoa(len(remoteData)))
			w.Header().Set("ETag", `"old"`)
			w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
			_, _ = w.Write([]byte(remoteData))
		case r.Method == http.MethodPut:
			if got := r.Header.Get("If-Match"); got != `"old"` {
				t.Errorf("If-Match = %q, want %q", got, `"old"`)
			}
			deleted.Store(true)
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`<Error><Code>NoSuchKey</Code><Message>object was deleted</Message></Error>`))
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()

	store := newObjectTokenStoreForServer(t, server.URL)
	const fileName = "conditional-update-deleted.json"
	_, errSave := store.Save(t.Context(), &cliproxyauth.Auth{
		ID:       fileName,
		FileName: fileName,
		Provider: "codex",
		Metadata: map[string]any{"type": "codex", "access_token": "replacement-token"},
	})
	if !errors.Is(errSave, authfileguard.ErrPersistGenerationStale) {
		t.Fatalf("Save() error = %v, want stale generation", errSave)
	}
	if outcome, ok := cliproxyauth.SaveOutcomeFromError(errSave); !ok || outcome != cliproxyauth.SaveOutcomeRolledBack {
		t.Fatalf("Save() outcome = %v, %t; want rolled back", outcome, ok)
	}
	if _, errStat := os.Stat(filepath.Join(store.AuthDir(), fileName)); !errors.Is(errStat, os.ErrNotExist) {
		t.Fatalf("local auth exists after rolled-back replacement: %v", errStat)
	}
}

func TestObjectTokenStoreSaveIfAbsentReportsDifferentWriteAfterLostAcknowledgementAsUncertain(t *testing.T) {
	const concurrentData = `{"type":"chatgpt-web","access_token":"concurrent"}`
	var remoteExists atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.RawQuery == "location=":
			w.Header().Set("Content-Type", "application/xml")
			_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></LocationConstraint>`))
		case r.Method == http.MethodHead:
			if !remoteExists.Load() {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			w.Header().Set("ETag", `"concurrent"`)
			w.Header().Set("Content-Length", strconv.Itoa(len(concurrentData)))
			w.Header().Set("Last-Modified", time.Date(2026, time.July, 17, 0, 0, 0, 0, time.UTC).Format(http.TimeFormat))
			w.Header().Set(objectStoreWriteIDMetadata, "different-write")
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPut:
			if _, errRead := io.Copy(io.Discard, r.Body); errRead != nil {
				http.Error(w, errRead.Error(), http.StatusInternalServerError)
				return
			}
			remoteExists.Store(true)
			hijacker, ok := w.(http.Hijacker)
			if !ok {
				t.Error("test server does not support hijacking")
				return
			}
			connection, _, errHijack := hijacker.Hijack()
			if errHijack != nil {
				t.Errorf("hijack conditional create: %v", errHijack)
				return
			}
			_ = connection.Close()
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()

	store := newObjectTokenStoreForServer(t, server.URL)
	const fileName = "lost-ack-conflict.json"
	_, errSave := store.SaveIfAbsent(t.Context(), &cliproxyauth.Auth{
		ID:       fileName,
		FileName: fileName,
		Provider: "chatgpt-web",
		Metadata: map[string]any{"type": "chatgpt-web", "access_token": "new-token"},
	})
	if errors.Is(errSave, cliproxyauth.ErrAuthAlreadyExists) {
		t.Fatalf("SaveIfAbsent() error = %v, must not claim a proven create conflict", errSave)
	}
	if outcome, ok := cliproxyauth.SaveOutcomeFromError(errSave); !ok || outcome != cliproxyauth.SaveOutcomeUncertain {
		t.Fatalf("SaveIfAbsent() outcome = %v, %t; want uncertain", outcome, ok)
	}
	if _, errStat := os.Stat(filepath.Join(store.AuthDir(), fileName)); !errors.Is(errStat, os.ErrNotExist) {
		t.Fatalf("local auth exists after concurrent remote write: %v", errStat)
	}
}

func TestObjectTokenStoreSaveIfAbsentReportsUncertainLostAcknowledgement(t *testing.T) {
	var headCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.RawQuery == "location=":
			w.Header().Set("Content-Type", "application/xml")
			_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></LocationConstraint>`))
		case r.Method == http.MethodHead:
			if headCalls.Add(1) == 1 {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			w.WriteHeader(http.StatusInternalServerError)
		case r.Method == http.MethodPut:
			if _, errRead := io.Copy(io.Discard, r.Body); errRead != nil {
				http.Error(w, errRead.Error(), http.StatusInternalServerError)
				return
			}
			hijacker, ok := w.(http.Hijacker)
			if !ok {
				t.Error("test server does not support hijacking")
				return
			}
			connection, _, errHijack := hijacker.Hijack()
			if errHijack != nil {
				t.Errorf("hijack conditional create: %v", errHijack)
				return
			}
			_ = connection.Close()
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()

	store := newObjectTokenStoreForServer(t, server.URL)
	const fileName = "lost-ack-uncertain.json"
	_, errSave := store.SaveIfAbsent(t.Context(), &cliproxyauth.Auth{
		ID:       fileName,
		FileName: fileName,
		Provider: "chatgpt-web",
		Metadata: map[string]any{"type": "chatgpt-web", "access_token": "new-token"},
	})
	if outcome, ok := cliproxyauth.SaveOutcomeFromError(errSave); !ok || outcome != cliproxyauth.SaveOutcomeUncertain {
		t.Fatalf("SaveIfAbsent() outcome = %v, %t; want uncertain; error=%v", outcome, ok, errSave)
	}
	if _, errStat := os.Stat(filepath.Join(store.AuthDir(), fileName)); !errors.Is(errStat, os.ErrNotExist) {
		t.Fatalf("local auth exists after uncertain remote write: %v", errStat)
	}
}

func TestObjectTokenStoreSaveRestoresLocalFileWhenUploadFails(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.RawQuery == "location=":
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></LocationConstraint>`))
		case r.Method == http.MethodHead:
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusNotFound)
		case r.Method == http.MethodPut:
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`<Error><Code>InternalError</Code><Message>put failed</Message></Error>`))
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()

	store := newObjectTokenStoreForServer(t, server.URL)
	const fileName = "rollback-save.json"
	oldData := []byte(`{"type":"codex","access_token":"old"}`)
	path := filepath.Join(store.AuthDir(), fileName)
	if errWrite := os.WriteFile(path, oldData, 0o600); errWrite != nil {
		t.Fatalf("write old auth: %v", errWrite)
	}
	auth := &cliproxyauth.Auth{
		ID: fileName, FileName: fileName, Provider: "codex",
		Metadata: map[string]any{"type": "codex", "access_token": "new"},
	}
	if _, errSave := store.Save(t.Context(), auth); errSave == nil {
		t.Fatal("Save() error = nil, want upload failure")
	}
	got, errRead := os.ReadFile(path)
	if errRead != nil || !bytes.Equal(got, oldData) {
		t.Fatalf("local auth after failed upload = %s, %v; want %s", got, errRead, oldData)
	}
}

func TestObjectTokenStoreSaveSemanticallyEqualRollbackPreservesLocalBytes(t *testing.T) {
	var uploaded []byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.RawQuery == "location=":
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></LocationConstraint>`))
		case r.Method == http.MethodHead:
			w.WriteHeader(http.StatusNotFound)
		case r.Method == http.MethodPut:
			uploaded, _ = io.ReadAll(r.Body)
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`<Error><Code>InternalError</Code><Message>put failed</Message></Error>`))
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()

	store := newObjectTokenStoreForServer(t, server.URL)
	const fileName = "semantic.json"
	localData := []byte("{\n  \"type\": \"codex\",\n  \"disabled\": false,\n  \"access_token\": \"token\"\n}\n")
	path := filepath.Join(store.AuthDir(), fileName)
	if errWrite := os.WriteFile(path, localData, 0o600); errWrite != nil {
		t.Fatalf("write local auth: %v", errWrite)
	}
	auth := &cliproxyauth.Auth{
		ID: fileName, FileName: fileName, Provider: "codex",
		Metadata: map[string]any{"type": "codex", "access_token": "token"},
	}

	_, errSave := store.Save(t.Context(), auth)
	if errSave == nil {
		t.Fatal("Save() error = nil, want upload failure")
	}
	if errors.Is(errSave, authfileguard.ErrPersistGenerationStale) {
		t.Fatalf("Save() reported a stale local generation without rewriting it: %v", errSave)
	}
	if !bytes.Equal(uploaded, localData) {
		t.Fatalf("uploaded auth = %s, want original local bytes", uploaded)
	}
	if got, errRead := os.ReadFile(path); errRead != nil || !bytes.Equal(got, localData) {
		t.Fatalf("local auth = %s, %v; want original bytes", got, errRead)
	}
}

func TestObjectTokenStoreReadAuthFileSetsCanonicalSourceHash(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "auth.json")
	data := []byte(`{"type":"claude","email":"reader@example.com"}`)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write auth file: %v", err)
	}

	store := &ObjectTokenStore{authDir: dir}
	auth, err := store.readAuthFile(path, dir)
	if err != nil {
		t.Fatalf("readAuthFile returned error: %v", err)
	}
	if auth == nil {
		t.Fatal("expected auth to be loaded")
	}
	wantRaw, err := cliproxyauth.CanonicalMetadataBytes(auth)
	if err != nil {
		t.Fatalf("CanonicalMetadataBytes() error = %v", err)
	}
	if got, want := auth.Attributes[cliproxyauth.SourceHashAttributeKey], cliproxyauth.SourceHashFromBytes(wantRaw); got != want {
		t.Fatalf("source hash = %q, want %q", got, want)
	}
	if rawHash := cliproxyauth.SourceHashFromBytes(data); rawHash == auth.Attributes[cliproxyauth.SourceHashAttributeKey] {
		t.Fatal("expected canonical source hash to differ from raw file hash")
	}
}

func TestObjectTokenStoreReadAuthFilePreservesDisabledState(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "auth.json")
	if err := os.WriteFile(path, []byte(`{"type":"claude","email":"reader@example.com","disabled":true}`), 0o600); err != nil {
		t.Fatalf("write auth file: %v", err)
	}

	store := &ObjectTokenStore{authDir: dir}
	auth, err := store.readAuthFile(path, dir)
	if err != nil {
		t.Fatalf("readAuthFile returned error: %v", err)
	}
	if auth == nil {
		t.Fatal("expected auth to be loaded")
	}
	if !auth.Disabled {
		t.Fatal("expected auth to remain disabled")
	}
	if auth.Status != cliproxyauth.StatusDisabled {
		t.Fatalf("status = %q, want %q", auth.Status, cliproxyauth.StatusDisabled)
	}
}

func TestObjectTokenStoreSaveStorageBackedAuthSetsCanonicalSourceHash(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.RawQuery == "location=":
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></LocationConstraint>`))
		case r.Method == http.MethodHead:
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusNotFound)
		case r.Method == http.MethodPut:
			if got := r.Header.Get("If-None-Match"); got != "*" {
				t.Errorf("If-None-Match = %q, want *", got)
			}
			w.Header().Set("ETag", `"etag"`)
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()

	store, err := NewObjectTokenStore(ObjectStoreConfig{
		Endpoint:  strings.TrimPrefix(server.URL, "http://"),
		Bucket:    "test-bucket",
		AccessKey: "test-access",
		SecretKey: "test-secret",
		LocalRoot: t.TempDir(),
		PathStyle: true,
	})
	if err != nil {
		t.Fatalf("NewObjectTokenStore() error = %v", err)
	}

	auth := &cliproxyauth.Auth{
		ID:       "auth.json",
		FileName: "auth.json",
		Provider: "codex",
		Storage:  &testTokenStorage{},
		Metadata: map[string]any{
			"type":                 "codex",
			"email":                "writer@example.com",
			"tool_prefix_disabled": true,
		},
	}

	path, err := store.Save(context.Background(), auth)
	if err != nil {
		t.Fatalf("Save() error = %v", err)
	}

	if got, ok := auth.Metadata["access_token"].(string); !ok || got != "tok-storage" {
		t.Fatalf("metadata access_token = %#v, want %q", auth.Metadata["access_token"], "tok-storage")
	}
	if got, ok := auth.Metadata["refresh_token"].(string); !ok || got != "refresh-storage" {
		t.Fatalf("metadata refresh_token = %#v, want %q", auth.Metadata["refresh_token"], "refresh-storage")
	}
	if got, ok := auth.Metadata["tool_prefix_disabled"].(bool); !ok || !got {
		t.Fatalf("metadata tool_prefix_disabled = %#v, want true", auth.Metadata["tool_prefix_disabled"])
	}

	rawFile, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	wantRaw, err := cliproxyauth.CanonicalMetadataBytes(auth)
	if err != nil {
		t.Fatalf("CanonicalMetadataBytes() error = %v", err)
	}
	wantHash := cliproxyauth.SourceHashFromBytes(wantRaw)
	if got := auth.Attributes[cliproxyauth.SourceHashAttributeKey]; got != wantHash {
		t.Fatalf("source hash = %q, want %q", got, wantHash)
	}
	if got, ok := auth.Metadata["disabled"].(bool); !ok || got {
		t.Fatalf("metadata disabled = %#v, want false", auth.Metadata["disabled"])
	}
	if rawHash := cliproxyauth.SourceHashFromBytes(rawFile); rawHash != wantHash {
		t.Fatalf("raw storage file hash = %q, want %q", rawHash, wantHash)
	}
}

func TestObjectTokenStoreSaveRejectsStorageOutputBeforeReplacingLocalAuth(t *testing.T) {
	var putCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.RawQuery == "location=":
			w.Header().Set("Content-Type", "application/xml")
			_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></LocationConstraint>`))
		case r.Method == http.MethodHead:
			w.WriteHeader(http.StatusNotFound)
		case r.Method == http.MethodPut:
			putCalls.Add(1)
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()
	store := newObjectTokenStoreForServer(t, server.URL)
	path := filepath.Join(store.AuthDir(), "auth.json")
	original := []byte(`{"type":"codex","access_token":"old"}`)
	if errWrite := os.WriteFile(path, original, 0o600); errWrite != nil {
		t.Fatalf("write original auth: %v", errWrite)
	}
	auth := &cliproxyauth.Auth{
		ID: "auth.json", FileName: "auth.json", Provider: "codex",
		Storage:  staticTokenStorage{data: []byte(`{"type":"gemini","access_token":"legacy"}`)},
		Metadata: map[string]any{"type": "codex"},
	}
	if _, errSave := store.Save(t.Context(), auth); !errors.Is(errSave, cliproxyauth.ErrRetiredGeminiCLIAuthReadOnly) {
		t.Fatalf("Save() error = %v, want retired read-only", errSave)
	}
	if got, errRead := os.ReadFile(path); errRead != nil || !bytes.Equal(got, original) {
		t.Fatalf("local auth changed: data=%s error=%v", got, errRead)
	}
	if putCalls.Load() != 0 {
		t.Fatalf("PUT calls = %d, want 0", putCalls.Load())
	}
}

func TestObjectTokenStoreSaveRejectsQuarantinedAuthPath(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()
	store := newObjectTokenStoreForServer(t, server.URL)
	path := filepath.Join(store.AuthDir(), "pending.json")
	authfileguard.MarkQuarantined(path)
	t.Cleanup(func() { authfileguard.ClearQuarantined(path) })

	_, errSave := store.Save(t.Context(), &cliproxyauth.Auth{
		ID: "pending.json", FileName: "pending.json", Provider: "codex",
		Metadata: map[string]any{"type": "codex", "access_token": "replacement"},
	})
	if !errors.Is(errSave, authfileguard.ErrDeleteGenerationUncertain) {
		t.Fatalf("Save() error = %v, want pending deletion error", errSave)
	}
	if _, errStat := os.Stat(path); !errors.Is(errStat, os.ErrNotExist) {
		t.Fatalf("quarantined auth file was created: %v", errStat)
	}
}

func TestObjectTokenStoreSaveRejectsRetiredFileCreatedDuringMarshal(t *testing.T) {
	var putCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.RawQuery == "location=":
			_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></LocationConstraint>`))
		case r.Method == http.MethodHead:
			w.WriteHeader(http.StatusNotFound)
		case r.Method == http.MethodPut:
			putCalls.Add(1)
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()
	store := newObjectTokenStoreForServer(t, server.URL)
	path := filepath.Join(store.AuthDir(), "auth.json")
	if errWrite := os.WriteFile(path, []byte(`{"type":"codex","access_token":"old"}`), 0o600); errWrite != nil {
		t.Fatalf("write original auth: %v", errWrite)
	}
	storage := &blockingTokenStorage{started: make(chan struct{}), release: make(chan struct{}), data: []byte(`{"type":"codex","access_token":"new"}`)}
	saveDone := make(chan error, 1)
	go func() {
		_, errSave := store.Save(t.Context(), &cliproxyauth.Auth{ID: "auth.json", FileName: "auth.json", Provider: "codex", Storage: storage})
		saveDone <- errSave
	}()
	<-storage.started
	retired := []byte(`{"type":"gemini","access_token":"legacy"}`)
	if errWrite := os.WriteFile(path, retired, 0o600); errWrite != nil {
		close(storage.release)
		t.Fatalf("write concurrent retired auth: %v", errWrite)
	}
	close(storage.release)
	if errSave := <-saveDone; !errors.Is(errSave, cliproxyauth.ErrRetiredGeminiCLIAuthReadOnly) {
		t.Fatalf("Save() error = %v, want retired read-only", errSave)
	}
	if got, errRead := os.ReadFile(path); errRead != nil || !bytes.Equal(got, retired) {
		t.Fatalf("concurrent retired auth changed: data=%s error=%v", got, errRead)
	}
	if putCalls.Load() != 0 {
		t.Fatalf("PUT calls = %d, want 0", putCalls.Load())
	}
	authfileguard.ClearRetired(path)
}

func TestObjectTokenStoreSaveRetriesRemoteWhenLocalContentMatches(t *testing.T) {
	var mu sync.Mutex
	var remoteData []byte
	putCalls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.RawQuery == "location=":
			w.Header().Set("Content-Type", "application/xml")
			_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></LocationConstraint>`))
		case r.Method == http.MethodHead:
			mu.Lock()
			data := append([]byte(nil), remoteData...)
			mu.Unlock()
			if len(data) == 0 {
				w.Header().Set("Content-Type", "application/xml")
				w.WriteHeader(http.StatusNotFound)
				return
			}
			w.Header().Set("ETag", `"saved"`)
			w.Header().Set("Content-Length", strconv.Itoa(len(data)))
			w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPut:
			data, errRead := io.ReadAll(r.Body)
			if errRead != nil {
				http.Error(w, errRead.Error(), http.StatusInternalServerError)
				return
			}
			mu.Lock()
			putCalls++
			call := putCalls
			if call > 1 {
				remoteData = append([]byte(nil), data...)
			}
			mu.Unlock()
			if call == 1 {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			if got := r.Header.Get("If-None-Match"); got != "*" {
				t.Errorf("If-None-Match = %q, want *", got)
			}
			w.Header().Set("ETag", `"saved"`)
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()

	store := newObjectTokenStoreForServer(t, server.URL)
	auth := &cliproxyauth.Auth{
		ID:       "retry.json",
		FileName: "retry.json",
		Provider: "codex",
		Metadata: map[string]any{"type": "codex", "access_token": "retry"},
	}
	if _, errSave := store.Save(t.Context(), auth); errSave == nil {
		t.Fatal("first Save() error = nil, want remote failure")
	}
	path := filepath.Join(store.AuthDir(), "retry.json")
	if _, errStat := os.Stat(path); !errors.Is(errStat, os.ErrNotExist) {
		t.Fatalf("local auth remained after failed upload: %v", errStat)
	}
	if _, errSave := store.Save(t.Context(), auth); errSave != nil {
		t.Fatalf("retry Save() error = %v", errSave)
	}
	wantData, errRead := os.ReadFile(path)
	if errRead != nil {
		t.Fatalf("read local auth after retry: %v", errRead)
	}
	mu.Lock()
	gotRemote := append([]byte(nil), remoteData...)
	gotPutCalls := putCalls
	mu.Unlock()
	if gotPutCalls != 2 {
		t.Fatalf("remote put calls = %d, want 2", gotPutCalls)
	}
	if !bytes.Equal(gotRemote, wantData) {
		t.Fatalf("remote auth = %s, want %s", gotRemote, wantData)
	}
}

func TestObjectTokenStoreSaveAcceptsCommittedWriteAfterLostAcknowledgement(t *testing.T) {
	for _, testCase := range []struct {
		name            string
		initiallyExists bool
		cancelCaller    bool
	}{
		{name: "create", initiallyExists: false},
		{name: "replace", initiallyExists: true},
		{name: "create_after_caller_cancellation", initiallyExists: false, cancelCaller: true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			committedData, errCommittedData := json.Marshal(map[string]any{
				"type": "codex", "access_token": "new", "disabled": false,
			})
			if errCommittedData != nil {
				t.Fatalf("marshal committed auth: %v", errCommittedData)
			}
			var mu sync.Mutex
			remoteExists := testCase.initiallyExists
			remoteData := []byte(`{"type":"codex","access_token":"old"}`)
			remoteETag := "old"
			remoteWriteID := ""
			putCalls := 0
			modified := time.Date(2026, time.July, 13, 0, 0, 0, 0, time.UTC)
			saveCtx := t.Context()
			cancelSave := func() {}
			if testCase.cancelCaller {
				var cancel context.CancelFunc
				saveCtx, cancel = context.WithCancel(t.Context())
				cancelSave = cancel
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case r.Method == http.MethodGet && r.URL.RawQuery == "location=":
					w.Header().Set("Content-Type", "application/xml")
					_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></LocationConstraint>`))
				case r.Method == http.MethodHead:
					mu.Lock()
					exists := remoteExists
					data := bytes.Clone(remoteData)
					etag := remoteETag
					writeID := remoteWriteID
					mu.Unlock()
					if !exists {
						w.Header().Set("Content-Type", "application/xml")
						w.WriteHeader(http.StatusNotFound)
						return
					}
					w.Header().Set("ETag", `"`+etag+`"`)
					w.Header().Set("Content-Length", strconv.Itoa(len(data)))
					w.Header().Set("Last-Modified", modified.Format(http.TimeFormat))
					if writeID != "" {
						w.Header().Set(objectStoreWriteIDMetadata, writeID)
					}
					w.WriteHeader(http.StatusOK)
				case r.Method == http.MethodGet:
					mu.Lock()
					exists := remoteExists
					data := bytes.Clone(remoteData)
					etag := remoteETag
					writeID := remoteWriteID
					mu.Unlock()
					if !exists {
						w.WriteHeader(http.StatusNotFound)
						return
					}
					w.Header().Set("ETag", `"`+etag+`"`)
					w.Header().Set("Content-Length", strconv.Itoa(len(data)))
					w.Header().Set("Last-Modified", modified.Format(http.TimeFormat))
					if writeID != "" {
						w.Header().Set(objectStoreWriteIDMetadata, writeID)
					}
					w.WriteHeader(http.StatusOK)
					_, _ = w.Write(data)
				case r.Method == http.MethodPut:
					if _, errRead := io.Copy(io.Discard, r.Body); errRead != nil {
						http.Error(w, errRead.Error(), http.StatusInternalServerError)
						return
					}
					writeID := r.Header.Get(objectStoreWriteIDMetadata)
					mu.Lock()
					putCalls++
					call := putCalls
					if call == 1 {
						remoteExists = true
						remoteData = bytes.Clone(committedData)
						remoteETag = "saved"
						remoteWriteID = writeID
					}
					mu.Unlock()
					if call > 1 {
						w.WriteHeader(http.StatusPreconditionFailed)
						return
					}
					cancelSave()
					if writeID == "" {
						t.Error("auth PUT omitted write ID metadata")
					}
					hijacker, ok := w.(http.Hijacker)
					if !ok {
						t.Error("test server does not support hijacking")
						return
					}
					connection, _, errHijack := hijacker.Hijack()
					if errHijack != nil {
						t.Errorf("hijack committed PUT: %v", errHijack)
						return
					}
					_ = connection.Close()
				default:
					w.WriteHeader(http.StatusMethodNotAllowed)
				}
			}))
			defer server.Close()

			store := newObjectTokenStoreForServer(t, server.URL)
			auth := &cliproxyauth.Auth{
				ID: "lost-ack.json", FileName: "lost-ack.json", Provider: "codex",
				Metadata: map[string]any{"type": "codex", "access_token": "new"},
			}
			path, errSave := store.Save(saveCtx, auth)
			if errSave != nil {
				t.Fatalf("Save() error = %v", errSave)
			}
			localData, errRead := os.ReadFile(path)
			if errRead != nil {
				t.Fatalf("read saved auth: %v", errRead)
			}
			mu.Lock()
			gotRemoteData := bytes.Clone(remoteData)
			gotWriteID := remoteWriteID
			mu.Unlock()
			if !bytes.Equal(gotRemoteData, localData) {
				t.Fatalf("remote auth = %s, want %s", gotRemoteData, localData)
			}
			if gotWriteID == "" {
				t.Fatal("remote auth has no write ID")
			}
			if testCase.cancelCaller && !errors.Is(saveCtx.Err(), context.Canceled) {
				t.Fatalf("save context error = %v, want canceled", saveCtx.Err())
			}
		})
	}
}

func TestObjectDeleteAuthFileFailsClosedWhenRemoteResultIsUnknown(t *testing.T) {
	var deleteCalls atomic.Int32
	lastModified := time.Date(2026, time.July, 12, 0, 0, 0, 0, time.UTC)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.RawQuery == "location=":
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></LocationConstraint>`))
		case r.Method == http.MethodGet && r.URL.Query().Has("versions"):
			writeObjectVersionsResponse(w, "auths/rollback.json", objectVersionFixture{VersionID: "version-a", Latest: true})
		case r.Method == http.MethodGet:
			data := []byte(`{"type":"codex","access_token":"token"}`)
			w.Header().Set("ETag", `"existing"`)
			w.Header().Set("x-amz-version-id", "version-a")
			w.Header().Set("Content-Length", strconv.Itoa(len(data)))
			w.Header().Set("Last-Modified", lastModified.Format(http.TimeFormat))
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(data)
		case r.Method == http.MethodDelete:
			deleteCalls.Add(1)
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`<Error><Code>InternalError</Code><Message>delete failed</Message></Error>`))
		case r.Method == http.MethodHead:
			w.Header().Set("ETag", `"existing"`)
			w.Header().Set("x-amz-version-id", "version-a")
			w.Header().Set("Content-Length", strconv.Itoa(len([]byte(`{"type":"codex","access_token":"token"}`))))
			w.Header().Set("Last-Modified", lastModified.Format(http.TimeFormat))
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()
	store, errStore := NewObjectTokenStore(ObjectStoreConfig{
		Endpoint:  strings.TrimPrefix(server.URL, "http://"),
		Bucket:    "test-bucket",
		AccessKey: "test-access",
		SecretKey: "test-secret",
		LocalRoot: t.TempDir(),
		PathStyle: true,
	})
	if errStore != nil {
		t.Fatalf("NewObjectTokenStore() error = %v", errStore)
	}
	const fileName = "rollback.json"
	wantData := []byte(`{"type":"codex","access_token":"token"}`)
	path := filepath.Join(store.AuthDir(), fileName)
	if errWrite := os.WriteFile(path, wantData, 0o600); errWrite != nil {
		t.Fatalf("write auth file: %v", errWrite)
	}
	root, errRoot := os.OpenRoot(store.AuthDir())
	if errRoot != nil {
		t.Fatalf("open auth root: %v", errRoot)
	}
	defer root.Close()
	errDelete := store.DeleteAuthFileAtRoot(t.Context(), root, fileName)
	if errDelete == nil {
		t.Fatal("DeleteAuthFileAtRoot() error = nil, want remote delete failure")
	}
	if outcome, ok := cliproxyauth.DeleteOutcomeFromError(errDelete); !ok || outcome != cliproxyauth.DeleteOutcomeUncertain {
		t.Fatalf("delete outcome = %v, %t; want uncertain", outcome, ok)
	}
	if _, errRead := os.ReadFile(path); !errors.Is(errRead, os.ErrNotExist) {
		t.Fatalf("local auth restored after unknown delete result: %v", errRead)
	}
	if deleteCalls.Load() == 0 {
		t.Fatalf("remote delete was not attempted: %v", errDelete)
	}

	errDelete = store.Delete(t.Context(), fileName)
	if errDelete == nil {
		t.Fatal("Delete() error = nil, want remote delete failure")
	}
	if outcome, ok := cliproxyauth.DeleteOutcomeFromError(errDelete); !ok || outcome != cliproxyauth.DeleteOutcomeUncertain {
		t.Fatalf("plain delete outcome = %v, %t; want uncertain", outcome, ok)
	}
	if _, errRead := os.ReadFile(path); !errors.Is(errRead, os.ErrNotExist) {
		t.Fatalf("plain Delete() restored local auth: %v", errRead)
	}
	if deleteCalls.Load() < 2 {
		t.Fatalf("plain Delete() did not attempt remote deletion: %v", errDelete)
	}
}

func TestObjectPersistAuthFilesPreservesReplacementFromOlderDeleteGeneration(t *testing.T) {
	original := []byte(`{"type":"codex","access_token":"original"}`)
	replacement := []byte(`{"type":"codex","access_token":"replacement"}`)
	var deleteCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.RawQuery == "location=":
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></LocationConstraint>`))
		case r.Method == http.MethodHead:
			w.Header().Set("ETag", `"replacement"`)
			w.Header().Set("Content-Length", strconv.Itoa(len(replacement)))
			w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodGet:
			w.Header().Set("ETag", `"replacement"`)
			w.Header().Set("Content-Length", strconv.Itoa(len(replacement)))
			w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(replacement)
		case r.Method == http.MethodDelete:
			deleteCalls.Add(1)
			w.WriteHeader(http.StatusNoContent)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()
	store, errStore := NewObjectTokenStore(ObjectStoreConfig{
		Endpoint:  strings.TrimPrefix(server.URL, "http://"),
		Bucket:    "test-bucket",
		AccessKey: "test-access",
		SecretKey: "test-secret",
		LocalRoot: t.TempDir(),
		PathStyle: true,
	})
	if errStore != nil {
		t.Fatalf("NewObjectTokenStore() error = %v", errStore)
	}
	path := filepath.Join(store.AuthDir(), "auth.json")
	ctx := authfileguard.WithExpectedDeleteHash(t.Context(), cliproxyauth.SourceHashFromBytes(original))
	if errPersist := store.PersistAuthFiles(ctx, "retry old auth deletion", path); errPersist != nil {
		t.Fatalf("PersistAuthFiles() error = %v", errPersist)
	}
	if deleteCalls.Load() != 0 {
		t.Fatalf("replacement delete calls = %d, want 0", deleteCalls.Load())
	}
	gotLocal, errRead := os.ReadFile(path)
	if errRead != nil || !bytes.Equal(gotLocal, replacement) {
		t.Fatalf("restored local replacement = %s, %v; want %s", gotLocal, errRead, replacement)
	}
}

func TestObjectPersistAuthFilesRejectsChangedExpectedSnapshot(t *testing.T) {
	var writeCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.RawQuery == "location=":
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></LocationConstraint>`))
		case r.Method == http.MethodHead:
			w.WriteHeader(http.StatusNotFound)
		case r.Method == http.MethodPut || r.Method == http.MethodDelete:
			writeCalls.Add(1)
			w.WriteHeader(http.StatusNoContent)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	store, errStore := NewObjectTokenStore(ObjectStoreConfig{
		Endpoint:  strings.TrimPrefix(server.URL, "http://"),
		Bucket:    "test-bucket",
		AccessKey: "test-access",
		SecretKey: "test-secret",
		LocalRoot: t.TempDir(),
		PathStyle: true,
	})
	if errStore != nil {
		t.Fatalf("NewObjectTokenStore() error = %v", errStore)
	}
	path := filepath.Join(store.AuthDir(), "auth.json")
	if errWrite := os.WriteFile(path, []byte(`{"type":"codex","access_token":"original"}`), 0o600); errWrite != nil {
		t.Fatalf("write auth: %v", errWrite)
	}
	expected := cliproxyauth.SourceHashFromBytes([]byte(`{"type":"codex","access_token":"replacement"}`))
	ctx := authfileguard.WithExpectedPersistHash(t.Context(), expected)
	if errPersist := store.PersistAuthFiles(ctx, "persist replacement", path); !errors.Is(errPersist, authfileguard.ErrPersistGenerationStale) {
		t.Fatalf("PersistAuthFiles() error = %v, want ErrPersistGenerationStale", errPersist)
	}
	if writeCalls.Load() != 0 {
		t.Fatalf("remote mutation calls = %d, want 0", writeCalls.Load())
	}
}

func TestObjectPersistAuthFilesPreservesSameContentNewVersion(t *testing.T) {
	data := []byte(`{"type":"codex","access_token":"same"}`)
	modifiedA := time.Unix(1_700_000_000, 0).UTC()
	modifiedB := modifiedA.Add(time.Second)
	var deleteCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.RawQuery == "location=":
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></LocationConstraint>`))
		case r.Method == http.MethodHead:
			w.Header().Set("ETag", `"same-etag"`)
			w.Header().Set("x-amz-version-id", "version-b")
			w.Header().Set("Content-Length", strconv.Itoa(len(data)))
			w.Header().Set("Last-Modified", modifiedB.Format(http.TimeFormat))
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodGet:
			w.Header().Set("ETag", `"same-etag"`)
			w.Header().Set("x-amz-version-id", "version-b")
			w.Header().Set("Content-Length", strconv.Itoa(len(data)))
			w.Header().Set("Last-Modified", modifiedB.Format(http.TimeFormat))
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(data)
		case r.Method == http.MethodDelete:
			deleteCalls.Add(1)
			w.WriteHeader(http.StatusNoContent)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()
	store, errStore := NewObjectTokenStore(ObjectStoreConfig{
		Endpoint: strings.TrimPrefix(server.URL, "http://"), Bucket: "test-bucket",
		AccessKey: "test-access", SecretKey: "test-secret", LocalRoot: t.TempDir(), PathStyle: true,
	})
	if errStore != nil {
		t.Fatalf("NewObjectTokenStore() error = %v", errStore)
	}
	generation := authfileguard.NewDeleteGeneration(cliproxyauth.SourceHashFromBytes(data))
	originalState := objectAuthWritePrecondition{etag: "same-etag", versionID: "version-a", lastModified: modifiedA, exists: true}
	if !generation.BindBackendIdentity("object:auth.json", objectAuthWriteIdentity(originalState)) {
		t.Fatal("failed to bind original object generation")
	}
	path := filepath.Join(store.AuthDir(), "auth.json")
	ctx := authfileguard.WithDeleteGeneration(t.Context(), generation)
	if errPersist := store.PersistAuthFiles(ctx, "retry old auth deletion", path); !errors.Is(errPersist, authfileguard.ErrDeleteGenerationUncertain) {
		t.Fatalf("PersistAuthFiles() error = %v, want uncertain generation", errPersist)
	}
	if deleteCalls.Load() != 0 {
		t.Fatalf("same-content replacement delete calls = %d, want 0", deleteCalls.Load())
	}
}

func TestObjectPersistAuthFilesStopsRetryWithoutVersionID(t *testing.T) {
	data := []byte(`{"type":"codex","access_token":"same"}`)
	modified := time.Unix(1_700_000_000, 0).UTC()
	var deleteCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.RawQuery == "location=":
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></LocationConstraint>`))
		case r.Method == http.MethodHead:
			w.Header().Set("ETag", `"same-etag"`)
			w.Header().Set("Content-Length", strconv.Itoa(len(data)))
			w.Header().Set("Last-Modified", modified.Format(http.TimeFormat))
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodGet:
			w.Header().Set("ETag", `"same-etag"`)
			w.Header().Set("Content-Length", strconv.Itoa(len(data)))
			w.Header().Set("Last-Modified", modified.Format(http.TimeFormat))
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(data)
		case r.Method == http.MethodDelete:
			deleteCalls.Add(1)
			w.WriteHeader(http.StatusNoContent)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()
	store, errStore := NewObjectTokenStore(ObjectStoreConfig{
		Endpoint: strings.TrimPrefix(server.URL, "http://"), Bucket: "test-bucket",
		AccessKey: "test-access", SecretKey: "test-secret", LocalRoot: t.TempDir(), PathStyle: true,
	})
	if errStore != nil {
		t.Fatalf("NewObjectTokenStore() error = %v", errStore)
	}
	generation := authfileguard.NewDeleteGeneration(cliproxyauth.SourceHashFromBytes(data))
	path := filepath.Join(store.AuthDir(), "auth.json")
	ctx := authfileguard.WithDeleteGeneration(t.Context(), generation)
	ctx = authfileguard.WithDeleteAttempt(ctx, 1)
	if errPersist := store.PersistAuthFiles(ctx, "retry old auth deletion", path); !errors.Is(errPersist, authfileguard.ErrDeleteGenerationUncertain) {
		t.Fatalf("PersistAuthFiles() error = %v, want uncertain generation", errPersist)
	}
	if deleteCalls.Load() != 0 {
		t.Fatalf("unversioned retry delete calls = %d, want 0", deleteCalls.Load())
	}
}

func TestObjectPersistAuthFilesCompletesResumedDeleteWhenObjectIsAbsent(t *testing.T) {
	var deleteCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.RawQuery == "location=":
			w.Header().Set("Content-Type", "application/xml")
			_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></LocationConstraint>`))
		case r.Method == http.MethodHead:
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusNotFound)
		case r.Method == http.MethodDelete:
			deleteCalls.Add(1)
			w.WriteHeader(http.StatusNoContent)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()
	store, errStore := NewObjectTokenStore(ObjectStoreConfig{
		Endpoint: strings.TrimPrefix(server.URL, "http://"), Bucket: "test-bucket",
		AccessKey: "test-access", SecretKey: "test-secret", LocalRoot: t.TempDir(), PathStyle: true,
	})
	if errStore != nil {
		t.Fatalf("NewObjectTokenStore() error = %v", errStore)
	}
	data := []byte(`{"type":"codex","access_token":"deleted"}`)
	generation := authfileguard.NewDeleteGeneration(cliproxyauth.SourceHashFromBytes(data))
	if !generation.BindBackendIdentity("object:auth.json", "old-etag\x00old-version\x001") {
		t.Fatal("bind old object identity")
	}
	ctx := authfileguard.WithDeleteGeneration(t.Context(), generation)
	if errPersist := store.PersistAuthFiles(ctx, "resume completed deletion", filepath.Join(store.AuthDir(), "auth.json")); errPersist != nil {
		t.Fatalf("PersistAuthFiles() error = %v", errPersist)
	}
	if deleteCalls.Load() != 0 {
		t.Fatalf("delete calls = %d, want 0", deleteCalls.Load())
	}
}

func TestStableObjectVersionIDRejectsNullSentinel(t *testing.T) {
	for _, versionID := range []string{"", "null", " NULL "} {
		if stableObjectVersionID(versionID) {
			t.Fatalf("stableObjectVersionID(%q) = true, want false", versionID)
		}
	}
	if !stableObjectVersionID("version-v2") {
		t.Fatal("stableObjectVersionID(version-v2) = false, want true")
	}
}

func TestStableObjectDeleteIdentityRequiresVersion(t *testing.T) {
	legacy := objectAuthWritePrecondition{etag: "same-etag", exists: true}
	if stableObjectDeleteIdentity(legacy) {
		t.Fatal("legacy unversioned ETag should not be retry-safe")
	}
	marked := legacy
	marked.writeID = "write-id"
	if stableObjectDeleteIdentity(marked) {
		t.Fatal("write ID cannot make an unversioned object retry-safe without a matching delete precondition")
	}
	versioned := legacy
	versioned.versionID = "version-a"
	if !stableObjectDeleteIdentity(versioned) {
		t.Fatal("version ID should make an object retry-safe")
	}
	markedAgain := marked
	markedAgain.writeID = "write-id-b"
	if objectAuthWriteIdentity(marked) == objectAuthWriteIdentity(markedAgain) {
		t.Fatal("write ID must participate in object generation identity")
	}
}

func TestObjectDeleteUsesETagConditionWithoutVersionID(t *testing.T) {
	data := []byte(`{"type":"codex","access_token":"token"}`)
	modified := time.Unix(1_700_000_000, 0).UTC()
	var deleteCalls atomic.Int32
	var remoteExists atomic.Bool
	remoteExists.Store(true)
	var deleteIfMatch atomic.Value
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.RawQuery == "location=":
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></LocationConstraint>`))
		case r.Method == http.MethodHead:
			if !remoteExists.Load() {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			w.Header().Set("ETag", `"unversioned"`)
			w.Header().Set("Content-Length", strconv.Itoa(len(data)))
			w.Header().Set("Last-Modified", modified.Format(http.TimeFormat))
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodGet:
			if !remoteExists.Load() {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			w.Header().Set("ETag", `"unversioned"`)
			w.Header().Set("Content-Length", strconv.Itoa(len(data)))
			w.Header().Set("Last-Modified", modified.Format(http.TimeFormat))
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(data)
		case r.Method == http.MethodDelete:
			deleteCalls.Add(1)
			deleteIfMatch.Store(r.Header.Get("If-Match"))
			if r.Header.Get("If-Match") != `"unversioned"` {
				w.WriteHeader(http.StatusPreconditionFailed)
				return
			}
			remoteExists.Store(false)
			w.WriteHeader(http.StatusNoContent)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()

	store := newObjectTokenStoreForServer(t, server.URL)
	const fileName = "unversioned.json"
	path := filepath.Join(store.AuthDir(), fileName)
	if errWrite := os.WriteFile(path, data, 0o600); errWrite != nil {
		t.Fatalf("write local auth: %v", errWrite)
	}
	root, errRoot := os.OpenRoot(store.AuthDir())
	if errRoot != nil {
		t.Fatalf("OpenRoot() error = %v", errRoot)
	}
	defer root.Close()
	errDelete := store.DeleteAuthFileAtRoot(t.Context(), root, fileName)
	if errDelete != nil {
		t.Fatalf("DeleteAuthFileAtRoot() error = %v", errDelete)
	}
	if deleteCalls.Load() != 1 {
		t.Fatalf("unversioned object delete calls = %d, want 1", deleteCalls.Load())
	}
	if got, _ := deleteIfMatch.Load().(string); got != `"unversioned"` {
		t.Fatalf("DELETE If-Match = %q, want quoted ETag", got)
	}
	if _, errStat := os.Stat(path); !errors.Is(errStat, os.ErrNotExist) {
		t.Fatalf("local auth still exists: %v", errStat)
	}
}

func TestObjectConditionalDeleteTargetsInspectedVersion(t *testing.T) {
	var deleteQuery url.Values
	deleted := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.RawQuery == "location=":
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></LocationConstraint>`))
		case r.Method == http.MethodGet && r.URL.Query().Has("versions"):
			writeObjectVersionsResponse(w, "auths/versioned.json", objectVersionFixture{VersionID: "version-v2", Latest: true})
		case r.Method == http.MethodDelete:
			deleteQuery = r.URL.Query()
			deleted = true
			w.WriteHeader(http.StatusNoContent)
		case r.Method == http.MethodHead && deleted:
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusNotFound)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()

	store := newObjectTokenStoreForServer(t, server.URL)
	path := filepath.Join(store.AuthDir(), "versioned.json")
	state := objectAuthWritePrecondition{etag: "versioned-etag", versionID: "version-v2", exists: true}
	if errDelete := store.deleteAuthObjectConditionally(t.Context(), path, state); errDelete != nil {
		t.Fatalf("deleteAuthObjectConditionally() error = %v", errDelete)
	}
	if got := deleteQuery.Get("versionId"); got != "version-v2" {
		t.Fatalf("DELETE versionId = %q, want version-v2", got)
	}
}

func TestObjectConditionalDeleteRemovesPreexistingHistoricalVersions(t *testing.T) {
	deletedVersions := make([]string, 0, 2)
	deleted := make(map[string]bool)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.RawQuery == "location=":
			w.Header().Set("Content-Type", "application/xml")
			_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></LocationConstraint>`))
		case r.Method == http.MethodGet && r.URL.Query().Has("versions"):
			writeObjectVersionsResponse(w, "auths/history.json",
				objectVersionFixture{VersionID: "version-v2", Latest: true},
				objectVersionFixture{VersionID: "version-v1"},
			)
		case r.Method == http.MethodDelete:
			versionID := r.URL.Query().Get("versionId")
			deletedVersions = append(deletedVersions, versionID)
			deleted[versionID] = true
			w.WriteHeader(http.StatusNoContent)
		case r.Method == http.MethodHead:
			if deleted["version-v2"] && !deleted["version-v1"] {
				w.Header().Set("ETag", `"historical"`)
				w.Header().Set("x-amz-version-id", "version-v1")
				w.Header().Set("Content-Length", "1")
				w.Header().Set("Last-Modified", time.Unix(1_699_999_999, 0).UTC().Format(http.TimeFormat))
				w.WriteHeader(http.StatusOK)
				return
			}
			w.WriteHeader(http.StatusNotFound)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()

	store := newObjectTokenStoreForServer(t, server.URL)
	path := filepath.Join(store.AuthDir(), "history.json")
	state := objectAuthWritePrecondition{etag: "versioned-etag", versionID: "version-v2", exists: true}
	if errDelete := store.deleteAuthObjectConditionally(t.Context(), path, state); errDelete != nil {
		t.Fatalf("deleteAuthObjectConditionally() error = %v", errDelete)
	}
	if len(deletedVersions) != 2 || deletedVersions[0] != "version-v2" || deletedVersions[1] != "version-v1" {
		t.Fatalf("deleted versions = %v, want [version-v2 version-v1]", deletedVersions)
	}
}

func TestObjectConditionalDeletePreservesSameETagReplacementVersion(t *testing.T) {
	modified := time.Unix(1_700_000_000, 0).UTC()
	var deleteQuery url.Values
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.RawQuery == "location=":
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></LocationConstraint>`))
		case r.Method == http.MethodGet && r.URL.Query().Has("versions"):
			writeObjectVersionsResponse(w, "auths/versioned.json", objectVersionFixture{VersionID: "version-a", Latest: true})
		case r.Method == http.MethodDelete:
			deleteQuery = r.URL.Query()
			w.WriteHeader(http.StatusNoContent)
		case r.Method == http.MethodHead:
			w.Header().Set("ETag", `"same-etag"`)
			w.Header().Set("x-amz-version-id", "version-b")
			w.Header().Set("Content-Length", "1")
			w.Header().Set("Last-Modified", modified.Format(http.TimeFormat))
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()

	store := newObjectTokenStoreForServer(t, server.URL)
	path := filepath.Join(store.AuthDir(), "versioned.json")
	original := objectAuthWritePrecondition{
		etag:         "same-etag",
		versionID:    "version-a",
		lastModified: modified.Add(-time.Second),
		exists:       true,
	}
	if errDelete := store.deleteAuthObjectConditionally(t.Context(), path, original); errDelete == nil {
		t.Fatal("deleteAuthObjectConditionally() error = nil, want replacement preservation error")
	}
	if got := deleteQuery.Get("versionId"); got != "version-a" {
		t.Fatalf("DELETE versionId = %q, want version-a", got)
	}
}

func TestObjectTokenStoreDeleteReportsCommittedAfterFailedResponse(t *testing.T) {
	wantData := []byte(`{"type":"codex","access_token":"token"}`)
	lastModified := time.Date(2026, time.July, 12, 0, 0, 0, 0, time.UTC)
	var mu sync.Mutex
	remoteExists := true
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.RawQuery == "location=":
			w.Header().Set("Content-Type", "application/xml")
			_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></LocationConstraint>`))
		case r.Method == http.MethodGet && r.URL.Query().Has("versions"):
			writeObjectVersionsResponse(w, "auths/committed.json", objectVersionFixture{VersionID: "version-a", Latest: true})
		case r.Method == http.MethodHead:
			mu.Lock()
			exists := remoteExists
			mu.Unlock()
			if !exists {
				w.Header().Set("Content-Type", "application/xml")
				w.WriteHeader(http.StatusNotFound)
				return
			}
			w.Header().Set("ETag", `"existing"`)
			w.Header().Set("x-amz-version-id", "version-a")
			w.Header().Set("Content-Length", strconv.Itoa(len(wantData)))
			w.Header().Set("Last-Modified", lastModified.Format(http.TimeFormat))
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodGet:
			w.Header().Set("ETag", `"existing"`)
			w.Header().Set("x-amz-version-id", "version-a")
			w.Header().Set("Content-Length", strconv.Itoa(len(wantData)))
			w.Header().Set("Last-Modified", lastModified.Format(http.TimeFormat))
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(wantData)
		case r.Method == http.MethodDelete:
			mu.Lock()
			remoteExists = false
			mu.Unlock()
			w.WriteHeader(http.StatusInternalServerError)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()

	store := newObjectTokenStoreForServer(t, server.URL)
	const fileName = "committed.json"
	path := filepath.Join(store.AuthDir(), fileName)
	if errWrite := os.WriteFile(path, wantData, 0o600); errWrite != nil {
		t.Fatalf("write local auth: %v", errWrite)
	}
	errDelete := store.Delete(t.Context(), fileName)
	if errDelete == nil {
		t.Fatal("Delete() error = nil, want failed response")
	}
	if outcome, ok := cliproxyauth.DeleteOutcomeFromError(errDelete); !ok || outcome != cliproxyauth.DeleteOutcomeCommitted {
		t.Fatalf("delete outcome = %v, %t; want committed", outcome, ok)
	}
	if _, errStat := os.Stat(path); !errors.Is(errStat, os.ErrNotExist) {
		t.Fatalf("local auth still exists: %v", errStat)
	}
}

func TestObjectTokenStoreDeleteDoesNotRestoreOverRemoteReplacement(t *testing.T) {
	original := []byte(`{"type":"codex","access_token":"original"}`)
	replacement := []byte(`{"type":"codex","access_token":"replacement"}`)
	lastModified := time.Date(2026, time.July, 12, 0, 0, 0, 0, time.UTC)
	var replaced atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.RawQuery == "location=":
			w.Header().Set("Content-Type", "application/xml")
			_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></LocationConstraint>`))
		case r.Method == http.MethodGet && r.URL.Query().Has("versions"):
			writeObjectVersionsResponse(w, "auths/replaced.json", objectVersionFixture{VersionID: "version-a", Latest: true})
		case r.Method == http.MethodHead:
			if replaced.Load() {
				w.Header().Set("ETag", `"replacement"`)
				w.Header().Set("x-amz-version-id", "version-b")
				w.Header().Set("Content-Length", strconv.Itoa(len(replacement)))
			} else {
				w.Header().Set("ETag", `"original"`)
				w.Header().Set("x-amz-version-id", "version-a")
				w.Header().Set("Content-Length", strconv.Itoa(len(original)))
			}
			w.Header().Set("Last-Modified", lastModified.Format(http.TimeFormat))
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodGet:
			w.Header().Set("ETag", `"original"`)
			w.Header().Set("x-amz-version-id", "version-a")
			w.Header().Set("Content-Length", strconv.Itoa(len(original)))
			w.Header().Set("Last-Modified", lastModified.Format(http.TimeFormat))
			_, _ = w.Write(original)
		case r.Method == http.MethodDelete:
			replaced.Store(true)
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`<Error><Code>InternalError</Code><Message>result lost</Message></Error>`))
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()

	store := newObjectTokenStoreForServer(t, server.URL)
	const fileName = "replaced.json"
	path := filepath.Join(store.AuthDir(), fileName)
	if errWrite := os.WriteFile(path, original, 0o600); errWrite != nil {
		t.Fatalf("write local auth: %v", errWrite)
	}
	errDelete := store.Delete(t.Context(), fileName)
	if outcome, ok := cliproxyauth.DeleteOutcomeFromError(errDelete); !ok || outcome != cliproxyauth.DeleteOutcomeUncertain {
		t.Fatalf("delete outcome = %v, %t; want uncertain replacement; error=%v", outcome, ok, errDelete)
	}
	if _, errStat := os.Stat(path); !errors.Is(errStat, os.ErrNotExist) {
		t.Fatalf("stale local auth was restored over replacement: %v", errStat)
	}
}

func TestObjectFinalizeRetiredDeletionPreservesRemoteReplacement(t *testing.T) {
	replacement := []byte(`{"type":"codex","access_token":"replacement"}`)
	var deleteCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.RawQuery == "location=":
			w.Header().Set("Content-Type", "application/xml")
			_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></LocationConstraint>`))
		case r.Method == http.MethodHead:
			w.Header().Set("ETag", `"replacement"`)
			w.Header().Set("Content-Length", strconv.Itoa(len(replacement)))
			w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodGet:
			w.Header().Set("ETag", `"replacement"`)
			w.Header().Set("Content-Length", strconv.Itoa(len(replacement)))
			w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(replacement)
		case r.Method == http.MethodDelete:
			deleteCalls.Add(1)
			w.WriteHeader(http.StatusNoContent)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()

	store := newObjectTokenStoreForServer(t, server.URL)
	path := filepath.Join(store.AuthDir(), "legacy.json")
	if errWrite := os.WriteFile(path, []byte(`{"type":"gemini","access_token":"stale"}`), 0o600); errWrite != nil {
		t.Fatalf("write stale local retired auth: %v", errWrite)
	}
	authfileguard.MarkRetired(path)
	t.Cleanup(func() { authfileguard.ClearRetired(path) })
	if errPersist := store.PersistAuthFiles(t.Context(), "ignore stale retired mirror", path); errPersist != nil {
		t.Fatalf("PersistAuthFiles() error = %v", errPersist)
	}
	if _, errStat := os.Stat(path); !errors.Is(errStat, os.ErrNotExist) {
		t.Fatalf("stale local retired auth still exists: %v", errStat)
	}
	if !authfileguard.IsRetired(path) {
		t.Fatal("stale mirror removal cleared quarantine before remote finalization")
	}
	if errFinalize := store.FinalizeAuthFileDeletion(t.Context(), "legacy.json"); errFinalize != nil {
		t.Fatalf("FinalizeAuthFileDeletion() error = %v", errFinalize)
	}
	if deleteCalls.Load() != 0 {
		t.Fatalf("delete calls = %d, want 0", deleteCalls.Load())
	}
	if authfileguard.IsRetired(path) {
		t.Fatal("replacement path remains retired")
	}
}

func TestObjectFinalizeRetiredDeletionAcceptsLostAcknowledgement(t *testing.T) {
	retired := []byte(`{"type":"gemini","access_token":"retired"}`)
	modified := time.Date(2026, time.July, 13, 0, 0, 0, 0, time.UTC)
	var remoteExists atomic.Bool
	remoteExists.Store(true)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.RawQuery == "location=":
			w.Header().Set("Content-Type", "application/xml")
			_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></LocationConstraint>`))
		case r.Method == http.MethodHead:
			if !remoteExists.Load() {
				w.Header().Set("Content-Type", "application/xml")
				w.WriteHeader(http.StatusNotFound)
				return
			}
			w.Header().Set("ETag", `"retired"`)
			w.Header().Set("Content-Length", strconv.Itoa(len(retired)))
			w.Header().Set("Last-Modified", modified.Format(http.TimeFormat))
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodGet:
			if !remoteExists.Load() {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			w.Header().Set("ETag", `"retired"`)
			w.Header().Set("Content-Length", strconv.Itoa(len(retired)))
			w.Header().Set("Last-Modified", modified.Format(http.TimeFormat))
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(retired)
		case r.Method == http.MethodDelete:
			remoteExists.Store(false)
			hijacker, ok := w.(http.Hijacker)
			if !ok {
				t.Error("test server does not support hijacking")
				return
			}
			connection, _, errHijack := hijacker.Hijack()
			if errHijack != nil {
				t.Errorf("hijack committed DELETE: %v", errHijack)
				return
			}
			_ = connection.Close()
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()

	store := newObjectTokenStoreForServer(t, server.URL)
	path := filepath.Join(store.AuthDir(), "legacy.json")
	authfileguard.MarkRetired(path)
	t.Cleanup(func() { authfileguard.ClearRetired(path) })
	if errFinalize := store.FinalizeAuthFileDeletion(t.Context(), "legacy.json"); errFinalize != nil {
		t.Fatalf("FinalizeAuthFileDeletion() error = %v", errFinalize)
	}
	if remoteExists.Load() {
		t.Fatal("remote retired auth still exists")
	}
	if authfileguard.IsRetired(path) {
		t.Fatal("retired marker remained after committed deletion")
	}
}

func TestObjectFinalizeRetiredDeletionPreservesDifferentRetiredGeneration(t *testing.T) {
	original := []byte(`{"type":"gemini","access_token":"original"}`)
	replacement := []byte(`{"type":"gemini","access_token":"replacement"}`)
	modified := time.Unix(1_700_000_000, 0).UTC()
	var deleteCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.RawQuery == "location=":
			w.Header().Set("Content-Type", "application/xml")
			_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></LocationConstraint>`))
		case r.Method == http.MethodHead:
			w.Header().Set("ETag", `"replacement"`)
			w.Header().Set("x-amz-version-id", "version-b")
			w.Header().Set("Content-Length", strconv.Itoa(len(replacement)))
			w.Header().Set("Last-Modified", modified.Format(http.TimeFormat))
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodGet:
			w.Header().Set("ETag", `"replacement"`)
			w.Header().Set("x-amz-version-id", "version-b")
			w.Header().Set("Content-Length", strconv.Itoa(len(replacement)))
			w.Header().Set("Last-Modified", modified.Format(http.TimeFormat))
			_, _ = w.Write(replacement)
		case r.Method == http.MethodDelete:
			deleteCalls.Add(1)
			w.WriteHeader(http.StatusNoContent)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()
	store := newObjectTokenStoreForServer(t, server.URL)
	generation := authfileguard.NewDeleteGeneration(cliproxyauth.SourceHashFromBytes(original))
	originalState := objectAuthWritePrecondition{etag: "original", versionID: "version-a", lastModified: modified, exists: true}
	if !generation.BindBackendIdentity("object:legacy.json", objectAuthWriteIdentity(originalState)) {
		t.Fatal("bind original object generation")
	}
	ctx := authfileguard.WithDeleteGeneration(t.Context(), generation)
	if errFinalize := store.FinalizeAuthFileDeletion(ctx, "legacy.json"); !errors.Is(errFinalize, authfileguard.ErrDeleteGenerationUncertain) {
		t.Fatalf("FinalizeAuthFileDeletion() error = %v, want uncertain", errFinalize)
	}
	if deleteCalls.Load() != 0 {
		t.Fatalf("delete calls = %d, want 0", deleteCalls.Load())
	}
}

func TestObjectPersistAuthFilesChecksRemoteBeforeLocalRetiredContent(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()
	store, errStore := NewObjectTokenStore(ObjectStoreConfig{
		Endpoint:  strings.TrimPrefix(server.URL, "http://"),
		Bucket:    "test-bucket",
		AccessKey: "test-access",
		SecretKey: "test-secret",
		LocalRoot: t.TempDir(),
		PathStyle: true,
	})
	if errStore != nil {
		t.Fatalf("NewObjectTokenStore() error = %v", errStore)
	}
	path := filepath.Join(store.AuthDir(), "legacy.json")
	if errWrite := os.WriteFile(path, []byte(`{"type":"gemini","access_token":"legacy"}`), 0o600); errWrite != nil {
		t.Fatalf("write retired auth file: %v", errWrite)
	}
	errPersist := store.PersistAuthFiles(t.Context(), "sync", path)
	if errPersist == nil || !strings.Contains(errPersist.Error(), http.StatusText(http.StatusInternalServerError)) {
		t.Fatalf("PersistAuthFiles() error = %v, want remote inspection failure", errPersist)
	}
}

func TestObjectPersistAuthFilesRejectsRewritingRetiredObject(t *testing.T) {
	var putCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.RawQuery == "location=":
			w.Header().Set("Content-Type", "application/xml")
			_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></LocationConstraint>`))
		case r.Method == http.MethodHead:
			data := []byte(`{"type":"gemini","access_token":"legacy"}`)
			w.Header().Set("Content-Length", strconv.Itoa(len(data)))
			w.Header().Set("ETag", `"legacy"`)
			w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodGet:
			data := []byte(`{"type":"gemini","access_token":"legacy"}`)
			if got := r.Header.Get("If-Match"); got != `"legacy"` {
				t.Errorf("GET If-Match = %q, want %q", got, `"legacy"`)
			}
			w.Header().Set("Content-Length", strconv.Itoa(len(data)))
			w.Header().Set("ETag", `"legacy"`)
			w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(data)
		case r.Method == http.MethodPut:
			putCalls.Add(1)
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()
	store, errStore := NewObjectTokenStore(ObjectStoreConfig{
		Endpoint: strings.TrimPrefix(server.URL, "http://"), Bucket: "test-bucket", AccessKey: "test-access", SecretKey: "test-secret", LocalRoot: t.TempDir(), PathStyle: true,
	})
	if errStore != nil {
		t.Fatalf("NewObjectTokenStore() error = %v", errStore)
	}
	path := filepath.Join(store.AuthDir(), "legacy.json")
	if errWrite := os.WriteFile(path, []byte(`{"type":"codex","access_token":"rewritten"}`), 0o600); errWrite != nil {
		t.Fatalf("write rewritten auth file: %v", errWrite)
	}
	errPersist := store.PersistAuthFiles(t.Context(), "sync", path)
	if !errors.Is(errPersist, cliproxyauth.ErrRetiredGeminiCLIAuthReadOnly) {
		t.Fatalf("PersistAuthFiles() error = %v, want retired read-only", errPersist)
	}
	if putCalls.Load() != 0 {
		t.Fatalf("put calls = %d, want 0", putCalls.Load())
	}
}

func TestObjectTokenStoreSaveRejectsRewritingRetiredObject(t *testing.T) {
	var putCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.RawQuery == "location=":
			w.Header().Set("Content-Type", "application/xml")
			_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></LocationConstraint>`))
		case r.Method == http.MethodHead:
			data := []byte(`{"type":"gemini","access_token":"legacy"}`)
			w.Header().Set("Content-Length", strconv.Itoa(len(data)))
			w.Header().Set("ETag", `"legacy"`)
			w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodGet:
			data := []byte(`{"type":"gemini","access_token":"legacy"}`)
			if got := r.Header.Get("If-Match"); got != `"legacy"` {
				t.Errorf("GET If-Match = %q, want %q", got, `"legacy"`)
			}
			w.Header().Set("Content-Length", strconv.Itoa(len(data)))
			w.Header().Set("ETag", `"legacy"`)
			w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(data)
		case r.Method == http.MethodPut:
			putCalls.Add(1)
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()
	store, errStore := NewObjectTokenStore(ObjectStoreConfig{
		Endpoint: strings.TrimPrefix(server.URL, "http://"), Bucket: "test-bucket", AccessKey: "test-access", SecretKey: "test-secret", LocalRoot: t.TempDir(), PathStyle: true,
	})
	if errStore != nil {
		t.Fatalf("NewObjectTokenStore() error = %v", errStore)
	}
	path := filepath.Join(store.AuthDir(), "legacy.json")
	if errWrite := os.WriteFile(path, []byte(`{"type":"codex","access_token":"external"}`), 0o600); errWrite != nil {
		t.Fatalf("write rewritten auth file: %v", errWrite)
	}
	_, errSave := store.Save(t.Context(), &cliproxyauth.Auth{
		ID: "legacy.json", FileName: "legacy.json", Provider: "codex",
		Metadata: map[string]any{"type": "codex", "access_token": "replacement"},
	})
	if !errors.Is(errSave, cliproxyauth.ErrRetiredGeminiCLIAuthReadOnly) {
		t.Fatalf("Save() error = %v, want retired read-only", errSave)
	}
	if putCalls.Load() != 0 {
		t.Fatalf("put calls = %d, want 0", putCalls.Load())
	}
}

func TestObjectPersistAuthFilesRejectsConcurrentCreateAfterMissingCheck(t *testing.T) {
	retired := []byte(`{"type":"gemini","access_token":"concurrent"}`)
	var mu sync.Mutex
	var remoteData []byte
	var putCalls int
	var putCondition string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.RawQuery == "location=":
			w.Header().Set("Content-Type", "application/xml")
			_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></LocationConstraint>`))
		case r.Method == http.MethodHead:
			mu.Lock()
			remoteData = append([]byte(nil), retired...)
			mu.Unlock()
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusNotFound)
		case r.Method == http.MethodPut:
			mu.Lock()
			putCalls++
			putCondition = r.Header.Get("If-None-Match")
			exists := len(remoteData) > 0
			mu.Unlock()
			if putCondition == "*" && exists {
				w.Header().Set("Content-Type", "application/xml")
				w.WriteHeader(http.StatusPreconditionFailed)
				_, _ = w.Write([]byte(`<Error><Code>PreconditionFailed</Code><Message>object exists</Message></Error>`))
				return
			}
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()

	store := newObjectTokenStoreForServer(t, server.URL)
	path := filepath.Join(store.AuthDir(), "concurrent-create.json")
	if errWrite := os.WriteFile(path, []byte(`{"type":"codex","access_token":"replacement"}`), 0o600); errWrite != nil {
		t.Fatalf("write replacement auth: %v", errWrite)
	}
	if errPersist := store.PersistAuthFiles(t.Context(), "sync", path); errPersist == nil {
		t.Fatal("PersistAuthFiles() error = nil, want conditional create conflict")
	}
	mu.Lock()
	defer mu.Unlock()
	if putCalls != 1 {
		t.Fatalf("put calls = %d, want 1", putCalls)
	}
	if putCondition != "*" {
		t.Fatalf("If-None-Match = %q, want *", putCondition)
	}
	if !bytes.Equal(remoteData, retired) {
		t.Fatalf("remote data = %s, want concurrent retired auth", remoteData)
	}
}

func TestObjectPersistAuthFilesPreservesExistingEmptyFile(t *testing.T) {
	var putCalls atomic.Int32
	var putBody []byte
	var putCondition string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.RawQuery == "location=":
			w.Header().Set("Content-Type", "application/xml")
			_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></LocationConstraint>`))
		case r.Method == http.MethodHead:
			w.WriteHeader(http.StatusNotFound)
		case r.Method == http.MethodPut:
			putCalls.Add(1)
			putCondition = r.Header.Get("If-None-Match")
			putBody, _ = io.ReadAll(r.Body)
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()

	store := newObjectTokenStoreForServer(t, server.URL)
	path := filepath.Join(store.AuthDir(), "empty.json")
	if errWrite := os.WriteFile(path, nil, 0o600); errWrite != nil {
		t.Fatalf("write empty auth: %v", errWrite)
	}
	if errPersist := store.PersistAuthFiles(t.Context(), "persist empty auth", path); errPersist != nil {
		t.Fatalf("PersistAuthFiles() error = %v", errPersist)
	}
	if putCalls.Load() != 1 || putCondition != "*" || len(putBody) != 0 {
		t.Fatalf("conditional empty PUT = calls:%d condition:%q body:%q", putCalls.Load(), putCondition, putBody)
	}
}

func TestObjectPersistAuthFilesRejectsConcurrentETagChange(t *testing.T) {
	initial := []byte(`{"type":"codex","access_token":"initial"}`)
	retired := []byte(`{"type":"gemini","access_token":"concurrent"}`)
	var mu sync.Mutex
	remoteData := append([]byte(nil), initial...)
	remoteETag := "old"
	var putCalls int
	var putCondition string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.RawQuery == "location=":
			w.Header().Set("Content-Type", "application/xml")
			_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></LocationConstraint>`))
		case r.Method == http.MethodHead:
			mu.Lock()
			size := len(remoteData)
			etag := remoteETag
			mu.Unlock()
			w.Header().Set("Content-Length", strconv.Itoa(size))
			w.Header().Set("ETag", `"`+etag+`"`)
			w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodGet:
			mu.Lock()
			data := append([]byte(nil), remoteData...)
			etag := remoteETag
			mu.Unlock()
			w.Header().Set("Content-Length", strconv.Itoa(len(data)))
			w.Header().Set("ETag", `"`+etag+`"`)
			w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(data)
			mu.Lock()
			remoteData = append([]byte(nil), retired...)
			remoteETag = "retired"
			mu.Unlock()
		case r.Method == http.MethodPut:
			mu.Lock()
			putCalls++
			putCondition = r.Header.Get("If-Match")
			etag := remoteETag
			mu.Unlock()
			if putCondition != `"`+etag+`"` {
				w.Header().Set("Content-Type", "application/xml")
				w.WriteHeader(http.StatusPreconditionFailed)
				_, _ = w.Write([]byte(`<Error><Code>PreconditionFailed</Code><Message>etag changed</Message></Error>`))
				return
			}
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()

	store := newObjectTokenStoreForServer(t, server.URL)
	path := filepath.Join(store.AuthDir(), "concurrent-update.json")
	if errWrite := os.WriteFile(path, []byte(`{"type":"codex","access_token":"replacement"}`), 0o600); errWrite != nil {
		t.Fatalf("write replacement auth: %v", errWrite)
	}
	if errPersist := store.PersistAuthFiles(t.Context(), "sync", path); errPersist == nil {
		t.Fatal("PersistAuthFiles() error = nil, want ETag conflict")
	}
	mu.Lock()
	defer mu.Unlock()
	if putCalls != 1 {
		t.Fatalf("put calls = %d, want 1", putCalls)
	}
	if putCondition != `"old"` {
		t.Fatalf("If-Match = %q, want %q", putCondition, `"old"`)
	}
	if !bytes.Equal(remoteData, retired) {
		t.Fatalf("remote data = %s, want concurrent retired auth", remoteData)
	}
}

func TestObjectPersistAuthFilesFailsClosedWhenInspectionUnsupported(t *testing.T) {
	for _, status := range []int{http.StatusMethodNotAllowed, http.StatusNotImplemented} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			var putCalls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case r.Method == http.MethodGet && r.URL.RawQuery == "location=":
					w.Header().Set("Content-Type", "application/xml")
					_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></LocationConstraint>`))
				case r.Method == http.MethodHead:
					w.Header().Set("Content-Type", "application/xml")
					w.WriteHeader(status)
				case r.Method == http.MethodPut:
					putCalls.Add(1)
					w.WriteHeader(http.StatusOK)
				default:
					w.WriteHeader(http.StatusMethodNotAllowed)
				}
			}))
			defer server.Close()

			store := newObjectTokenStoreForServer(t, server.URL)
			path := filepath.Join(store.AuthDir(), "unsupported.json")
			if errWrite := os.WriteFile(path, []byte(`{"type":"codex","access_token":"replacement"}`), 0o600); errWrite != nil {
				t.Fatalf("write replacement auth: %v", errWrite)
			}
			if errPersist := store.PersistAuthFiles(t.Context(), "sync", path); errPersist == nil {
				t.Fatalf("PersistAuthFiles() error = nil for status %d", status)
			}
			if putCalls.Load() != 0 {
				t.Fatalf("put calls = %d, want 0", putCalls.Load())
			}
		})
	}
}

func TestObjectPersistAuthFilesFailsClosedWithoutETag(t *testing.T) {
	var putCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.RawQuery == "location=":
			w.Header().Set("Content-Type", "application/xml")
			_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></LocationConstraint>`))
		case r.Method == http.MethodHead:
			data := []byte(`{"type":"codex","access_token":"initial"}`)
			w.Header().Set("Content-Length", strconv.Itoa(len(data)))
			w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPut:
			putCalls.Add(1)
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()

	store := newObjectTokenStoreForServer(t, server.URL)
	path := filepath.Join(store.AuthDir(), "missing-etag.json")
	if errWrite := os.WriteFile(path, []byte(`{"type":"codex","access_token":"replacement"}`), 0o600); errWrite != nil {
		t.Fatalf("write replacement auth: %v", errWrite)
	}
	if errPersist := store.PersistAuthFiles(t.Context(), "sync", path); errPersist == nil {
		t.Fatal("PersistAuthFiles() error = nil, want missing ETag error")
	}
	if putCalls.Load() != 0 {
		t.Fatalf("put calls = %d, want 0", putCalls.Load())
	}
}

func TestObjectPersistAuthFilesAllowsDeletingRetiredObject(t *testing.T) {
	var deleteCalls atomic.Int32
	var remoteExists atomic.Bool
	remoteExists.Store(true)
	lastModified := time.Date(2026, time.July, 12, 0, 0, 0, 0, time.UTC)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.RawQuery == "location=":
			w.Header().Set("Content-Type", "application/xml")
			_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></LocationConstraint>`))
		case r.Method == http.MethodGet && r.URL.Query().Has("versions"):
			writeObjectVersionsResponse(w, "auths/legacy.json", objectVersionFixture{VersionID: "version-a", Latest: true})
		case r.Method == http.MethodHead:
			if !remoteExists.Load() {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			data := []byte(`{"type":"gemini","access_token":"legacy"}`)
			w.Header().Set("Content-Length", strconv.Itoa(len(data)))
			w.Header().Set("ETag", `"legacy"`)
			w.Header().Set("x-amz-version-id", "version-a")
			w.Header().Set("Last-Modified", lastModified.Format(http.TimeFormat))
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodGet:
			data := []byte(`{"type":"gemini","access_token":"legacy"}`)
			w.Header().Set("Content-Length", strconv.Itoa(len(data)))
			w.Header().Set("ETag", `"legacy"`)
			w.Header().Set("x-amz-version-id", "version-a")
			w.Header().Set("Last-Modified", lastModified.Format(http.TimeFormat))
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(data)
		case r.Method == http.MethodDelete:
			deleteCalls.Add(1)
			remoteExists.Store(false)
			w.WriteHeader(http.StatusNoContent)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()
	store, errStore := NewObjectTokenStore(ObjectStoreConfig{
		Endpoint: strings.TrimPrefix(server.URL, "http://"), Bucket: "test-bucket", AccessKey: "test-access", SecretKey: "test-secret", LocalRoot: t.TempDir(), PathStyle: true,
	})
	if errStore != nil {
		t.Fatalf("NewObjectTokenStore() error = %v", errStore)
	}
	path := filepath.Join(store.AuthDir(), "legacy.json")
	if errDelete := store.PersistAuthFiles(t.Context(), "delete", path); errDelete != nil {
		t.Fatalf("PersistAuthFiles() error = %v", errDelete)
	}
	if deleteCalls.Load() != 1 {
		t.Fatalf("delete calls = %d, want 1", deleteCalls.Load())
	}
}

func newObjectTokenStoreForServer(t *testing.T, serverURL string) *ObjectTokenStore {
	t.Helper()
	store, errStore := NewObjectTokenStore(ObjectStoreConfig{
		Endpoint:  strings.TrimPrefix(serverURL, "http://"),
		Bucket:    "test-bucket",
		AccessKey: "test-access",
		SecretKey: "test-secret",
		LocalRoot: t.TempDir(),
		PathStyle: true,
	})
	if errStore != nil {
		t.Fatalf("NewObjectTokenStore() error = %v", errStore)
	}
	return store
}

type objectVersionFixture struct {
	VersionID string
	Latest    bool
}

type objectVersionListXML struct {
	Key          string `xml:"Key"`
	VersionID    string `xml:"VersionId"`
	IsLatest     bool   `xml:"IsLatest"`
	LastModified string `xml:"LastModified"`
	ETag         string `xml:"ETag"`
	Size         int64  `xml:"Size"`
	StorageClass string `xml:"StorageClass"`
}

type objectVersionsResponseXML struct {
	XMLName     xml.Name               `xml:"ListVersionsResult"`
	XMLNS       string                 `xml:"xmlns,attr"`
	Name        string                 `xml:"Name"`
	Prefix      string                 `xml:"Prefix"`
	IsTruncated bool                   `xml:"IsTruncated"`
	Versions    []objectVersionListXML `xml:"Version"`
}

func writeObjectVersionsResponse(w http.ResponseWriter, key string, versions ...objectVersionFixture) {
	entries := make([]objectVersionListXML, 0, len(versions))
	for index, version := range versions {
		entries = append(entries, objectVersionListXML{
			Key:          key,
			VersionID:    version.VersionID,
			IsLatest:     version.Latest,
			LastModified: time.Unix(1_700_000_000-int64(index), 0).UTC().Format(time.RFC3339Nano),
			ETag:         `"fixture"`,
			Size:         1,
			StorageClass: "STANDARD",
		})
	}
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	_ = xml.NewEncoder(w).Encode(objectVersionsResponseXML{
		XMLNS:       "http://s3.amazonaws.com/doc/2006-03-01/",
		Name:        "test-bucket",
		Prefix:      key,
		IsTruncated: false,
		Versions:    entries,
	})
}

func TestObjectTokenStoreRejectsReservedAuthLockPath(t *testing.T) {
	authDir := t.TempDir()
	store := &ObjectTokenStore{authDir: authDir}
	path := filepath.Join(authDir, ".auth-root-lock")
	if _, errRelative := store.relativeAuthPath(path); !errors.Is(errRelative, errReservedAuthLockPath) {
		t.Fatalf("relativeAuthPath() error = %v, want reserved lock path", errRelative)
	}
}
