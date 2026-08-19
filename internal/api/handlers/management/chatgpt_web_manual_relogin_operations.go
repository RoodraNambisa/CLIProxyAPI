package management

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	log "github.com/sirupsen/logrus"
)

const (
	chatGPTWebManualReloginOperationQueued    = "queued"
	chatGPTWebManualReloginOperationRunning   = "running"
	chatGPTWebManualReloginOperationCompleted = "completed"
	chatGPTWebManualReloginOperationFailed    = "failed"

	chatGPTWebManualReloginOperationRetention  = 30 * 24 * time.Hour
	chatGPTWebManualReloginOperationMaxRecords = 100_000
	chatGPTWebManualReloginOperationMaxLine    = 1 << 20
)

var (
	errChatGPTWebManualReloginJournalUnavailable  = errors.New("chatgpt web manual re-login operation journal is unavailable")
	errChatGPTWebManualReloginOperationCapacity   = errors.New("too many retained chatgpt web manual re-login operations")
	errChatGPTWebManualReloginIdempotencyConflict = errors.New("manual re-login idempotency key belongs to another credential generation")
)

type chatGPTWebManualReloginOperationSnapshot struct {
	OperationID         string     `json:"operation_id"`
	Status              string     `json:"status"`
	FileName            string     `json:"file_name"`
	IdentityFingerprint string     `json:"identity_fingerprint"`
	CreatedAt           time.Time  `json:"created_at"`
	StartedAt           *time.Time `json:"started_at,omitempty"`
	CompletedAt         *time.Time `json:"completed_at,omitempty"`
	Outcome             string     `json:"outcome,omitempty"`
	ErrorCategory       string     `json:"error_category,omitempty"`
	Error               string     `json:"error,omitempty"`
	HTTPStatus          int        `json:"http_status,omitempty"`
	FailureStage        string     `json:"failure_stage,omitempty"`
	Attempts            int        `json:"attempts,omitempty"`
	LifecycleState      string     `json:"lifecycle_state,omitempty"`
	ResultDurable       bool       `json:"result_durable"`
}

type chatGPTWebManualReloginOperationRecord struct {
	Snapshot        chatGPTWebManualReloginOperationSnapshot `json:"snapshot"`
	AuthID          string                                   `json:"auth_id"`
	IdempotencyHash string                                   `json:"idempotency_hash,omitempty"`
}

func chatGPTWebManualReloginJournalPath(configFilePath string) string {
	if path := strings.TrimSpace(configFilePath); path != "" {
		return path + ".manual-relogin-operations.ndjson"
	}
	return ""
}

func chatGPTWebManualReloginIdentityFingerprint(auth *coreauth.Auth, fileName string) string {
	if auth == nil {
		return ""
	}
	sourceHash := ""
	if auth.Attributes != nil {
		sourceHash = strings.TrimSpace(auth.Attributes[coreauth.SourceHashAttributeKey])
	}
	accountID := ""
	if auth.Metadata != nil {
		accountID = strings.TrimSpace(fmt.Sprint(auth.Metadata["account_id"]))
		if accountID == "<nil>" {
			accountID = ""
		}
	}
	material := strings.TrimSpace(fileName) + "\x00" + strings.TrimSpace(auth.ID) + "\x00" + sourceHash + "\x00" + accountID
	sum := sha256.Sum256([]byte(material))
	return hex.EncodeToString(sum[:])
}

func chatGPTWebManualReloginIdempotencyHash(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func chatGPTWebManualReloginLatestKey(fileName, fingerprint string) string {
	return strings.TrimSpace(fileName) + "\x00" + strings.TrimSpace(fingerprint)
}

func chatGPTWebManualReloginOperationLocation(c *gin.Context, operationID string) string {
	const managementMarker = "/v0/management"
	requestPath := ""
	if c != nil && c.Request != nil && c.Request.URL != nil {
		requestPath = c.Request.URL.Path
	}
	if markerIndex := strings.LastIndex(requestPath, managementMarker); markerIndex >= 0 {
		return requestPath[:markerIndex] + managementMarker + "/chatgpt-web/relogin-operations/" + operationID
	}
	return "/chatgpt-web/relogin-operations/" + operationID
}

func chatGPTWebManualReloginOperationActive(status string) bool {
	return status == chatGPTWebManualReloginOperationQueued || status == chatGPTWebManualReloginOperationRunning
}

func (m *chatGPTWebLoginTaskManager) configureManualReloginJournal(path string) error {
	if m == nil {
		return errChatGPTWebManualReloginJournalUnavailable
	}
	path = strings.TrimSpace(path)
	m.manualPersistMu.Lock()
	defer m.manualPersistMu.Unlock()
	m.mu.Lock()
	m.manualJournalPath = path
	m.mu.Unlock()
	if path == "" {
		return nil
	}
	if _, errStat := os.Stat(path); errors.Is(errStat, os.ErrNotExist) {
		return nil
	} else if errStat != nil {
		m.mu.Lock()
		m.manualJournalPath = ""
		m.mu.Unlock()
		return fmt.Errorf("inspect manual re-login operation journal: %w", errStat)
	}
	if errLoad := m.loadManualReloginJournalLocked(path); errLoad != nil {
		m.mu.Lock()
		m.manualJournalPath = ""
		m.mu.Unlock()
		return errLoad
	}
	if errCompact := m.rewriteManualReloginJournalLocked(); errCompact != nil {
		m.mu.Lock()
		m.manualJournalPath = ""
		m.mu.Unlock()
		return errCompact
	}
	return nil
}

func (m *chatGPTWebLoginTaskManager) loadManualReloginJournalLocked(path string) error {
	file, errOpen := os.Open(path)
	if errors.Is(errOpen, os.ErrNotExist) {
		return nil
	}
	if errOpen != nil {
		return fmt.Errorf("open manual re-login operation journal: %w", errOpen)
	}
	defer func() { _ = file.Close() }()

	loaded := make(map[string]*chatGPTWebManualReloginOperationRecord)
	reader := bufio.NewReaderSize(file, 64*1024)
	for {
		line, errRead := reader.ReadBytes('\n')
		if len(line) > chatGPTWebManualReloginOperationMaxLine {
			return errors.New("manual re-login operation journal record is too large")
		}
		if len(strings.TrimSpace(string(line))) == 0 {
			if errors.Is(errRead, io.EOF) {
				break
			}
			if errRead != nil {
				return fmt.Errorf("read manual re-login operation journal: %w", errRead)
			}
			continue
		}
		var record chatGPTWebManualReloginOperationRecord
		if errDecode := json.Unmarshal(line, &record); errDecode != nil {
			if errors.Is(errRead, io.EOF) {
				break
			}
			return fmt.Errorf("decode manual re-login operation journal: %w", errDecode)
		}
		if strings.TrimSpace(record.Snapshot.OperationID) == "" {
			continue
		}
		copyRecord := record
		loaded[record.Snapshot.OperationID] = &copyRecord
		if errors.Is(errRead, io.EOF) {
			break
		}
		if errRead != nil {
			return fmt.Errorf("read manual re-login operation journal: %w", errRead)
		}
	}

	now := time.Now().UTC()
	for id, record := range loaded {
		if record.Snapshot.CompletedAt != nil && now.Sub(*record.Snapshot.CompletedAt) > chatGPTWebManualReloginOperationRetention {
			delete(loaded, id)
		}
	}
	if len(loaded) > chatGPTWebManualReloginOperationMaxRecords {
		ordered := make([]*chatGPTWebManualReloginOperationRecord, 0, len(loaded))
		for _, record := range loaded {
			ordered = append(ordered, record)
		}
		sort.Slice(ordered, func(i, j int) bool {
			return ordered[i].Snapshot.CreatedAt.After(ordered[j].Snapshot.CreatedAt)
		})
		loaded = make(map[string]*chatGPTWebManualReloginOperationRecord, chatGPTWebManualReloginOperationMaxRecords)
		for _, record := range ordered[:chatGPTWebManualReloginOperationMaxRecords] {
			loaded[record.Snapshot.OperationID] = record
		}
	}

	m.mu.Lock()
	m.manualOperations = loaded
	m.rebuildManualReloginIndexesLocked()
	m.mu.Unlock()
	return nil
}

func (m *chatGPTWebLoginTaskManager) reconcileManualReloginOperationsLocked(authManager *coreauth.Manager) error {
	if authManager == nil {
		return nil
	}
	m.mu.Lock()
	unfinished := make([]string, 0)
	for id, record := range m.manualOperations {
		if chatGPTWebManualReloginOperationActive(record.Snapshot.Status) {
			unfinished = append(unfinished, id)
		}
	}
	m.mu.Unlock()
	sort.Strings(unfinished)
	for _, id := range unfinished {
		m.mu.Lock()
		currentRecord := m.manualOperations[id]
		var record *chatGPTWebManualReloginOperationRecord
		if currentRecord != nil {
			copyRecord := *currentRecord
			record = &copyRecord
		}
		m.mu.Unlock()
		if record == nil {
			continue
		}
		current, exists := authManager.GetByID(record.AuthID)
		outcome := "interrupted"
		category := "manual_relogin_interrupted"
		message := "manual re-login was interrupted before its outcome was recorded"
		status := http.StatusServiceUnavailable
		operationStatus := chatGPTWebManualReloginOperationFailed
		if !exists || current == nil {
			outcome = "credential_missing_after_interruption"
			category = "remote_missing_reconciled"
			message = "credential is no longer present after an interrupted re-login"
			status = http.StatusGone
			operationStatus = chatGPTWebManualReloginOperationCompleted
		} else if chatGPTWebManualReloginIdentityFingerprint(current, record.Snapshot.FileName) != record.Snapshot.IdentityFingerprint {
			outcome = "credential_changed_after_interruption"
			category = "credential_changed"
			message = "credential changed after an interrupted re-login"
			status = http.StatusConflict
		}
		if errComplete := m.completeManualReloginOperationLocked(id, chatGPTWebManualReloginCompletion{
			Status:        operationStatus,
			Outcome:       outcome,
			ErrorCategory: category,
			Error:         message,
			HTTPStatus:    status,
			FailureStage:  "relogin",
		}); errComplete != nil {
			return errComplete
		}
	}
	return nil
}

// ReconcileChatGPTWebManualReloginOperations finalizes journal entries left by a prior process.
func (h *Handler) ReconcileChatGPTWebManualReloginOperations() error {
	if h == nil {
		return nil
	}
	h.mu.Lock()
	taskManager := h.chatGPTWebTasks
	authManager := h.authManager
	h.mu.Unlock()
	if taskManager == nil || authManager == nil {
		return nil
	}
	taskManager.manualPersistMu.Lock()
	defer taskManager.manualPersistMu.Unlock()
	return taskManager.reconcileManualReloginOperationsLocked(authManager)
}

func (m *chatGPTWebLoginTaskManager) rebuildManualReloginIndexesLocked() {
	m.manualLatest = make(map[string]string)
	m.manualIdempotency = make(map[string]string)
	for id, record := range m.manualOperations {
		if record == nil {
			continue
		}
		key := chatGPTWebManualReloginLatestKey(record.Snapshot.FileName, record.Snapshot.IdentityFingerprint)
		if currentID := m.manualLatest[key]; currentID == "" || m.manualOperations[currentID].Snapshot.CreatedAt.Before(record.Snapshot.CreatedAt) {
			m.manualLatest[key] = id
		}
		if record.IdempotencyHash != "" {
			currentID := m.manualIdempotency[record.IdempotencyHash]
			if current := m.manualOperations[currentID]; current == nil || record.Snapshot.CreatedAt.Before(current.Snapshot.CreatedAt) {
				m.manualIdempotency[record.IdempotencyHash] = id
			}
		}
	}
}

func (m *chatGPTWebLoginTaskManager) findManualReloginOperation(idempotencyHash, fileName, fingerprint string, activeOnly bool) (chatGPTWebManualReloginOperationSnapshot, bool) {
	if m == nil {
		return chatGPTWebManualReloginOperationSnapshot{}, false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	id := ""
	if idempotencyHash != "" {
		id = m.manualIdempotency[idempotencyHash]
	}
	if id == "" && strings.TrimSpace(fingerprint) != "" {
		id = m.manualLatest[chatGPTWebManualReloginLatestKey(fileName, fingerprint)]
	}
	if id == "" && strings.TrimSpace(fileName) != "" {
		for candidateID, candidate := range m.manualOperations {
			if candidate == nil || candidate.Snapshot.FileName != strings.TrimSpace(fileName) {
				continue
			}
			if current := m.manualOperations[id]; current == nil || current.Snapshot.CreatedAt.Before(candidate.Snapshot.CreatedAt) {
				id = candidateID
			}
		}
	}
	record := m.manualOperations[id]
	if record == nil || (activeOnly && !chatGPTWebManualReloginOperationActive(record.Snapshot.Status)) {
		return chatGPTWebManualReloginOperationSnapshot{}, false
	}
	return record.Snapshot, true
}

func (m *chatGPTWebLoginTaskManager) getManualReloginOperation(operationID string) (chatGPTWebManualReloginOperationSnapshot, bool) {
	if m == nil {
		return chatGPTWebManualReloginOperationSnapshot{}, false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	record := m.manualOperations[strings.TrimSpace(operationID)]
	if record == nil {
		return chatGPTWebManualReloginOperationSnapshot{}, false
	}
	return record.Snapshot, true
}

func (m *chatGPTWebLoginTaskManager) createManualReloginOperation(auth *coreauth.Auth, fileName, fingerprint, idempotencyHash string) (chatGPTWebManualReloginOperationSnapshot, bool, error) {
	if m == nil || auth == nil {
		return chatGPTWebManualReloginOperationSnapshot{}, false, errChatGPTWebManualReloginJournalUnavailable
	}
	m.manualPersistMu.Lock()
	defer m.manualPersistMu.Unlock()
	m.mu.Lock()
	if m.closed || strings.TrimSpace(m.manualJournalPath) == "" {
		m.mu.Unlock()
		return chatGPTWebManualReloginOperationSnapshot{}, false, errChatGPTWebManualReloginJournalUnavailable
	}
	if existing, ok := m.findManualReloginOperationLocked(idempotencyHash, fileName, fingerprint, true); ok {
		if existing.FileName != strings.TrimSpace(fileName) || existing.IdentityFingerprint != strings.TrimSpace(fingerprint) {
			m.mu.Unlock()
			return chatGPTWebManualReloginOperationSnapshot{}, false, errChatGPTWebManualReloginIdempotencyConflict
		}
		m.mu.Unlock()
		return existing, true, nil
	}
	if idempotencyHash != "" {
		if existing, ok := m.findManualReloginOperationLocked(idempotencyHash, "", "", false); ok {
			if existing.FileName != strings.TrimSpace(fileName) || existing.IdentityFingerprint != strings.TrimSpace(fingerprint) {
				m.mu.Unlock()
				return chatGPTWebManualReloginOperationSnapshot{}, false, errChatGPTWebManualReloginIdempotencyConflict
			}
			m.mu.Unlock()
			return existing, true, nil
		}
	}
	m.pruneManualReloginOperationsLocked(time.Now().UTC())
	if len(m.manualOperations) >= chatGPTWebManualReloginOperationMaxRecords {
		m.mu.Unlock()
		return chatGPTWebManualReloginOperationSnapshot{}, false, errChatGPTWebManualReloginOperationCapacity
	}
	now := time.Now().UTC()
	record := &chatGPTWebManualReloginOperationRecord{
		Snapshot: chatGPTWebManualReloginOperationSnapshot{
			OperationID:         uuid.NewString(),
			Status:              chatGPTWebManualReloginOperationQueued,
			FileName:            strings.TrimSpace(fileName),
			IdentityFingerprint: strings.TrimSpace(fingerprint),
			CreatedAt:           now,
			ResultDurable:       true,
		},
		AuthID:          strings.TrimSpace(auth.ID),
		IdempotencyHash: idempotencyHash,
	}
	m.manualOperations[record.Snapshot.OperationID] = record
	m.manualLatest[chatGPTWebManualReloginLatestKey(fileName, fingerprint)] = record.Snapshot.OperationID
	if idempotencyHash != "" {
		m.manualIdempotency[idempotencyHash] = record.Snapshot.OperationID
	}
	snapshot := record.Snapshot
	persisted := *record
	m.mu.Unlock()
	if errAppend := m.appendManualReloginOperationLocked(persisted); errAppend != nil {
		m.mu.Lock()
		delete(m.manualOperations, snapshot.OperationID)
		m.rebuildManualReloginIndexesLocked()
		m.mu.Unlock()
		return chatGPTWebManualReloginOperationSnapshot{}, false, errAppend
	}
	return snapshot, false, nil
}

func (m *chatGPTWebLoginTaskManager) findManualReloginOperationLocked(idempotencyHash, fileName, fingerprint string, activeOnly bool) (chatGPTWebManualReloginOperationSnapshot, bool) {
	id := ""
	if idempotencyHash != "" {
		id = m.manualIdempotency[idempotencyHash]
	}
	if id == "" && fileName != "" && fingerprint != "" {
		id = m.manualLatest[chatGPTWebManualReloginLatestKey(fileName, fingerprint)]
	}
	record := m.manualOperations[id]
	if record == nil || (activeOnly && !chatGPTWebManualReloginOperationActive(record.Snapshot.Status)) {
		return chatGPTWebManualReloginOperationSnapshot{}, false
	}
	return record.Snapshot, true
}

func (m *chatGPTWebLoginTaskManager) markManualReloginOperationRunning(operationID string) error {
	m.manualPersistMu.Lock()
	defer m.manualPersistMu.Unlock()
	m.mu.Lock()
	record := m.manualOperations[operationID]
	if record == nil {
		m.mu.Unlock()
		return errors.New("manual re-login operation not found")
	}
	now := time.Now().UTC()
	record.Snapshot.Status = chatGPTWebManualReloginOperationRunning
	record.Snapshot.StartedAt = &now
	record.Snapshot.ResultDurable = false
	persisted := *record
	persisted.Snapshot.ResultDurable = true
	m.mu.Unlock()
	if errAppend := m.appendManualReloginOperationLocked(persisted); errAppend != nil {
		return errAppend
	}
	m.mu.Lock()
	if current := m.manualOperations[operationID]; current != nil && current.Snapshot.StartedAt != nil && current.Snapshot.StartedAt.Equal(now) {
		current.Snapshot.ResultDurable = true
	}
	m.mu.Unlock()
	return nil
}

type chatGPTWebManualReloginCompletion struct {
	Status         string
	Outcome        string
	ErrorCategory  string
	Error          string
	HTTPStatus     int
	FailureStage   string
	Attempts       int
	LifecycleState string
}

func (m *chatGPTWebLoginTaskManager) completeManualReloginOperation(operationID string, completion chatGPTWebManualReloginCompletion) error {
	m.manualPersistMu.Lock()
	defer m.manualPersistMu.Unlock()
	return m.completeManualReloginOperationLocked(operationID, completion)
}

func (m *chatGPTWebLoginTaskManager) completeManualReloginOperationLocked(operationID string, completion chatGPTWebManualReloginCompletion) error {
	m.mu.Lock()
	record := m.manualOperations[operationID]
	if record == nil {
		m.mu.Unlock()
		return errors.New("manual re-login operation not found")
	}
	now := time.Now().UTC()
	record.Snapshot.Status = completion.Status
	record.Snapshot.CompletedAt = &now
	record.Snapshot.Outcome = completion.Outcome
	record.Snapshot.ErrorCategory = completion.ErrorCategory
	record.Snapshot.Error = completion.Error
	record.Snapshot.HTTPStatus = completion.HTTPStatus
	record.Snapshot.FailureStage = completion.FailureStage
	record.Snapshot.Attempts = completion.Attempts
	record.Snapshot.LifecycleState = completion.LifecycleState
	record.Snapshot.ResultDurable = false
	persisted := *record
	persisted.Snapshot.ResultDurable = true
	m.mu.Unlock()
	if errAppend := m.appendManualReloginOperationLocked(persisted); errAppend != nil {
		return errAppend
	}
	m.mu.Lock()
	if current := m.manualOperations[operationID]; current != nil && current.Snapshot.CompletedAt != nil && current.Snapshot.CompletedAt.Equal(now) {
		current.Snapshot.ResultDurable = true
	}
	m.mu.Unlock()
	return nil
}

func (m *chatGPTWebLoginTaskManager) appendManualReloginOperationLocked(record chatGPTWebManualReloginOperationRecord) error {
	m.mu.Lock()
	path := strings.TrimSpace(m.manualJournalPath)
	m.mu.Unlock()
	if path == "" {
		return errChatGPTWebManualReloginJournalUnavailable
	}
	if errMkdir := os.MkdirAll(filepath.Dir(path), 0o700); errMkdir != nil {
		return fmt.Errorf("create manual re-login operation journal directory: %w", errMkdir)
	}
	payload, errMarshal := json.Marshal(record)
	if errMarshal != nil {
		return fmt.Errorf("encode manual re-login operation: %w", errMarshal)
	}
	payload = append(payload, '\n')
	_, errStat := os.Stat(path)
	created := errors.Is(errStat, os.ErrNotExist)
	if errStat != nil && !created {
		return fmt.Errorf("inspect manual re-login operation journal: %w", errStat)
	}
	file, errOpen := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if errOpen != nil {
		return fmt.Errorf("open manual re-login operation journal: %w", errOpen)
	}
	if errChmod := file.Chmod(0o600); errChmod != nil {
		_ = file.Close()
		return fmt.Errorf("protect manual re-login operation journal: %w", errChmod)
	}
	if written, errWrite := file.Write(payload); errWrite != nil {
		_ = file.Close()
		return fmt.Errorf("append manual re-login operation journal: %w", errWrite)
	} else if written != len(payload) {
		_ = file.Close()
		return fmt.Errorf("append manual re-login operation journal: %w", io.ErrShortWrite)
	}
	if errSync := file.Sync(); errSync != nil {
		_ = file.Close()
		return fmt.Errorf("sync manual re-login operation journal: %w", errSync)
	}
	if errClose := file.Close(); errClose != nil {
		return fmt.Errorf("close manual re-login operation journal: %w", errClose)
	}
	if created {
		if errSyncDirectory := syncChatGPTWebManualReloginJournalDirectory(filepath.Dir(path)); errSyncDirectory != nil {
			return errSyncDirectory
		}
	}
	return nil
}

func (m *chatGPTWebLoginTaskManager) rewriteManualReloginJournalLocked() error {
	m.mu.Lock()
	path := strings.TrimSpace(m.manualJournalPath)
	records := make([]chatGPTWebManualReloginOperationRecord, 0, len(m.manualOperations))
	for _, record := range m.manualOperations {
		if record != nil {
			records = append(records, *record)
		}
	}
	m.mu.Unlock()
	if path == "" {
		return nil
	}
	sort.Slice(records, func(i, j int) bool {
		return records[i].Snapshot.CreatedAt.Before(records[j].Snapshot.CreatedAt)
	})
	if errMkdir := os.MkdirAll(filepath.Dir(path), 0o700); errMkdir != nil {
		return fmt.Errorf("create manual re-login operation journal directory: %w", errMkdir)
	}
	temporary, errCreate := os.CreateTemp(filepath.Dir(path), ".manual-relogin-operations-*.tmp")
	if errCreate != nil {
		return fmt.Errorf("create compacted manual re-login operation journal: %w", errCreate)
	}
	temporaryPath := temporary.Name()
	removeTemporary := true
	defer func() {
		_ = temporary.Close()
		if removeTemporary {
			_ = os.Remove(temporaryPath)
		}
	}()
	if errChmod := temporary.Chmod(0o600); errChmod != nil {
		return fmt.Errorf("protect compacted manual re-login operation journal: %w", errChmod)
	}
	encoder := json.NewEncoder(temporary)
	for _, record := range records {
		if errEncode := encoder.Encode(record); errEncode != nil {
			return fmt.Errorf("encode compacted manual re-login operation journal: %w", errEncode)
		}
	}
	if errSync := temporary.Sync(); errSync != nil {
		return fmt.Errorf("sync compacted manual re-login operation journal: %w", errSync)
	}
	if errClose := temporary.Close(); errClose != nil {
		return fmt.Errorf("close compacted manual re-login operation journal: %w", errClose)
	}
	if errRename := replaceChatGPTWebManualReloginJournal(temporaryPath, path); errRename != nil {
		return fmt.Errorf("replace manual re-login operation journal: %w", errRename)
	}
	removeTemporary = false
	if errSyncDirectory := syncChatGPTWebManualReloginJournalDirectory(filepath.Dir(path)); errSyncDirectory != nil {
		return errSyncDirectory
	}
	return nil
}

func syncChatGPTWebManualReloginJournalDirectory(directory string) error {
	root, errOpen := os.OpenRoot(directory)
	if errOpen != nil {
		return fmt.Errorf("open manual re-login operation journal directory: %w", errOpen)
	}
	defer func() { _ = root.Close() }()
	if errSync := syncManagedAuthDirectory(root, "."); errSync != nil {
		return fmt.Errorf("sync manual re-login operation journal directory: %w", errSync)
	}
	return nil
}

func (m *chatGPTWebLoginTaskManager) pruneManualReloginOperationsLocked(now time.Time) {
	for id, record := range m.manualOperations {
		if record == nil || record.Snapshot.CompletedAt == nil {
			continue
		}
		if now.Sub(*record.Snapshot.CompletedAt) > chatGPTWebManualReloginOperationRetention {
			delete(m.manualOperations, id)
		}
	}
	m.rebuildManualReloginIndexesLocked()
}

// StartChatGPTWebReloginOperation starts a durable asynchronous manual re-login.
func (h *Handler) StartChatGPTWebReloginOperation(c *gin.Context) {
	name := strings.TrimSpace(c.Param("name"))
	if name == "" || isUnsafeAuthFileName(name) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "valid auth file name is required"})
		return
	}
	idempotencyKey := strings.TrimSpace(c.GetHeader("Idempotency-Key"))
	if len(idempotencyKey) > 256 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "idempotency key is too long", "error_category": "invalid_idempotency_key"})
		return
	}
	idempotencyHash := chatGPTWebManualReloginIdempotencyHash(idempotencyKey)
	taskManager := h.chatGPTWebTaskManager()
	if idempotencyHash != "" {
		if existing, ok := taskManager.findManualReloginOperation(idempotencyHash, "", "", false); ok {
			if existing.FileName != name {
				c.JSON(http.StatusConflict, gin.H{"error": "idempotency key belongs to another credential", "error_category": "idempotency_conflict"})
				return
			}
			c.Header("Location", chatGPTWebManualReloginOperationLocation(c, existing.OperationID))
			c.JSON(http.StatusAccepted, gin.H{"operation": existing, "reused": true})
			return
		}
	}
	auth := h.findManagedAuth(name)
	if auth == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "auth file not found"})
		return
	}
	if !strings.EqualFold(strings.TrimSpace(auth.Provider), "chatgpt-web") {
		c.JSON(http.StatusBadRequest, gin.H{"error": "auth file is not a chatgpt web credential"})
		return
	}
	executor, manager, errExecutor := h.chatGPTWebManagementExecutor()
	if errExecutor != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "chatgpt web management login is unavailable", "error_category": "manual_relogin_unavailable"})
		return
	}
	fingerprint := chatGPTWebManualReloginIdentityFingerprint(auth, name)
	if existing, ok := taskManager.findManualReloginOperation("", name, fingerprint, true); ok {
		c.Header("Location", chatGPTWebManualReloginOperationLocation(c, existing.OperationID))
		c.JSON(http.StatusAccepted, gin.H{"operation": existing, "reused": true})
		return
	}

	email := authEmail(auth)
	if email == "" {
		email = auth.ID
	}
	owner := "manual-operation:" + uuid.NewString()
	if errReserve := taskManager.reserveEmail(email, owner); errReserve != nil {
		if existing, ok := taskManager.findManualReloginOperation("", name, fingerprint, true); ok {
			c.Header("Location", chatGPTWebManualReloginOperationLocation(c, existing.OperationID))
			c.JSON(http.StatusAccepted, gin.H{"operation": existing, "reused": true})
			return
		}
		c.JSON(http.StatusConflict, gin.H{"error": errReserve.Error(), "error_category": "relogin_inflight"})
		return
	}
	manualReloginConcurrency := config.DefaultChatGPTWebManualReloginConcurrency
	if cfg := h.currentConfig(); cfg != nil {
		manualReloginConcurrency = cfg.ChatGPTWeb.ResolvedManualReloginConcurrency()
	}
	operationCtx, releaseOperation, errAdmission := taskManager.beginManualOperation(context.Background(), manualReloginConcurrency)
	if errAdmission != nil {
		taskManager.releaseEmail(email, owner)
		writeChatGPTWebManualReloginAdmissionError(c, errAdmission)
		return
	}
	operation, reused, errCreate := taskManager.createManualReloginOperation(auth, name, fingerprint, idempotencyHash)
	if errCreate != nil {
		releaseOperation()
		taskManager.releaseEmail(email, owner)
		status := http.StatusServiceUnavailable
		category := "manual_relogin_journal_unavailable"
		response := gin.H{
			"status":        "failed",
			"error":         "manual re-login operation was not accepted",
			"failure_stage": "relogin",
		}
		if errors.Is(errCreate, errChatGPTWebManualReloginOperationCapacity) {
			status = http.StatusTooManyRequests
			category = "manual_relogin_history_capacity"
			response["retry_after"] = chatGPTWebManualReloginRetrySec
			c.Header("Retry-After", fmt.Sprintf("%d", chatGPTWebManualReloginRetrySec))
		}
		if errors.Is(errCreate, errChatGPTWebManualReloginIdempotencyConflict) {
			status = http.StatusConflict
			category = "idempotency_conflict"
		}
		response["error_category"] = category
		c.JSON(status, response)
		return
	}
	if reused {
		releaseOperation()
		taskManager.releaseEmail(email, owner)
		c.Header("Location", chatGPTWebManualReloginOperationLocation(c, operation.OperationID))
		c.JSON(http.StatusAccepted, gin.H{"operation": operation, "reused": true})
		return
	}
	go h.runChatGPTWebManualReloginOperation(operationCtx, releaseOperation, taskManager, operation, auth, executor, manager, email, owner)
	c.Header("Location", chatGPTWebManualReloginOperationLocation(c, operation.OperationID))
	c.JSON(http.StatusAccepted, gin.H{"operation": operation, "reused": false})
}

func (h *Handler) runChatGPTWebManualReloginOperation(
	ctx context.Context,
	releaseOperation func(),
	taskManager *chatGPTWebLoginTaskManager,
	operation chatGPTWebManualReloginOperationSnapshot,
	auth *coreauth.Auth,
	executor chatGPTWebManagementExecutor,
	manager *coreauth.Manager,
	email string,
	owner string,
) {
	defer releaseOperation()
	defer taskManager.releaseEmail(email, owner)
	if errRunning := taskManager.markManualReloginOperationRunning(operation.OperationID); errRunning != nil {
		log.WithError(errRunning).Warn("failed to persist running manual ChatGPT Web re-login operation")
		_ = taskManager.completeManualReloginOperation(operation.OperationID, chatGPTWebManualReloginCompletion{
			Status:        chatGPTWebManualReloginOperationFailed,
			Outcome:       "journal_failed",
			ErrorCategory: "manual_relogin_journal_unavailable",
			Error:         "manual re-login operation journal is unavailable",
			HTTPStatus:    http.StatusServiceUnavailable,
			FailureStage:  "relogin",
		})
		return
	}
	status, response := h.executeChatGPTWebManualRelogin(ctx, auth, executor, manager)
	completion := chatGPTWebManualReloginCompletionFromResponse(status, response, auth, manager)
	if errComplete := taskManager.completeManualReloginOperation(operation.OperationID, completion); errComplete != nil {
		log.WithError(errComplete).Warn("failed to persist final manual ChatGPT Web re-login operation")
	}
}

func chatGPTWebManualReloginCompletionFromResponse(status int, response gin.H, auth *coreauth.Auth, manager *coreauth.Manager) chatGPTWebManualReloginCompletion {
	category := strings.TrimSpace(fmt.Sprint(response["error_category"]))
	if category == "<nil>" {
		category = ""
	}
	message := strings.TrimSpace(fmt.Sprint(response["error"]))
	if message == "<nil>" {
		message = ""
	}
	failureStage := strings.TrimSpace(fmt.Sprint(response["failure_stage"]))
	if failureStage == "<nil>" {
		failureStage = ""
	}
	attempts := 0
	switch value := response["attempts"].(type) {
	case int:
		attempts = value
	case float64:
		attempts = int(value)
	}
	lifecycleState := ""
	if entry, ok := response["auth"].(gin.H); ok {
		lifecycleState = strings.TrimSpace(fmt.Sprint(entry["lifecycle_state"]))
	} else if entry, ok := response["auth"].(map[string]any); ok {
		lifecycleState = strings.TrimSpace(fmt.Sprint(entry["lifecycle_state"]))
	}
	if lifecycleState == "<nil>" {
		lifecycleState = ""
	}
	completion := chatGPTWebManualReloginCompletion{
		Status:         chatGPTWebManualReloginOperationFailed,
		Outcome:        "failed",
		ErrorCategory:  category,
		Error:          message,
		HTTPStatus:     status,
		FailureStage:   failureStage,
		Attempts:       attempts,
		LifecycleState: lifecycleState,
	}
	if status >= 200 && status < 300 && strings.TrimSpace(fmt.Sprint(response["status"])) == "ok" {
		completion.Status = chatGPTWebManualReloginOperationCompleted
		completion.Outcome = "succeeded"
		return completion
	}
	if category == "account_deleted" || category == "account_deactivated" {
		completion.Status = chatGPTWebManualReloginOperationCompleted
		completion.Outcome = "official_dead_confirmed"
		if auth != nil && manager != nil {
			if _, exists := manager.GetByID(auth.ID); !exists {
				completion.Outcome = "official_dead_removed"
			}
		}
	}
	return completion
}

// GetChatGPTWebReloginOperation returns one durable manual re-login result.
func (h *Handler) GetChatGPTWebReloginOperation(c *gin.Context) {
	taskManager := h.chatGPTWebTaskManager()
	operation, ok := taskManager.getManualReloginOperation(c.Param("id"))
	if !ok {
		c.JSON(http.StatusNotFound, gin.H{"error": "manual re-login operation not found", "error_category": "manual_relogin_operation_not_found"})
		return
	}
	c.Header("Cache-Control", "no-store")
	c.JSON(http.StatusOK, operation)
}

// FindChatGPTWebReloginOperation finds the latest result for a frozen credential identity.
func (h *Handler) FindChatGPTWebReloginOperation(c *gin.Context) {
	name := strings.TrimSpace(c.Param("name"))
	if name == "" || isUnsafeAuthFileName(name) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "valid auth file name is required"})
		return
	}
	fingerprint := strings.TrimSpace(c.Query("identity_fingerprint"))
	idempotencyKey := strings.TrimSpace(c.GetHeader("Idempotency-Key"))
	if len(idempotencyKey) > 256 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "idempotency key is too long", "error_category": "invalid_idempotency_key"})
		return
	}
	if fingerprint == "" && idempotencyKey == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"error":          "identity_fingerprint or Idempotency-Key is required",
			"error_category": "manual_relogin_identity_required",
		})
		return
	}
	operation, ok := h.chatGPTWebTaskManager().findManualReloginOperation(
		chatGPTWebManualReloginIdempotencyHash(idempotencyKey), name, fingerprint, false,
	)
	if ok && (operation.FileName != name || (fingerprint != "" && operation.IdentityFingerprint != fingerprint)) {
		c.JSON(http.StatusConflict, gin.H{
			"error":          "operation belongs to another credential generation",
			"error_category": "idempotency_conflict",
		})
		return
	}
	if !ok {
		c.JSON(http.StatusNotFound, gin.H{"error": "manual re-login operation not found", "error_category": "manual_relogin_operation_not_found"})
		return
	}
	c.Header("Cache-Control", "no-store")
	c.JSON(http.StatusOK, operation)
}
