package auth

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/managementdiag"
	"github.com/tidwall/gjson"
)

const authErrorHistoryLimit = 20
const authErrorModelLimit = 64

type AuthErrorModelCount struct {
	Model  string    `json:"model"`
	Count  uint64    `json:"count"`
	LastAt time.Time `json:"last_at"`
}

type AuthErrorRecord struct {
	ID          string                `json:"id"`
	HTTPStatus  int                   `json:"http_status,omitempty"`
	Code        string                `json:"code,omitempty"`
	Type        string                `json:"type,omitempty"`
	Message     string                `json:"message"`
	Details     string                `json:"details,omitempty"`
	Truncated   bool                  `json:"truncated,omitempty"`
	Count       uint64                `json:"count"`
	FirstAt     time.Time             `json:"first_at"`
	LastAt      time.Time             `json:"last_at"`
	LastModel   string                `json:"last_model,omitempty"`
	Models      []AuthErrorModelCount `json:"models"`
	OtherModels uint64                `json:"other_models,omitempty"`
}

type AuthErrorHistorySummary struct {
	Total         uint64                `json:"total"`
	RetainedTotal uint64                `json:"retained_total"`
	Distinct      int                   `json:"distinct"`
	Limit         int                   `json:"limit"`
	Since         *time.Time            `json:"since,omitempty"`
	CurrentModel  string                `json:"current_model,omitempty"`
	Recent        []AuthErrorRecord     `json:"recent,omitempty"`
	Models        []AuthErrorModelCount `json:"models,omitempty"`
	OtherModels   uint64                `json:"other_models,omitempty"`
}

// All history state is protected by Manager.mu and never enters Auth metadata.
type authErrorHistory struct {
	total  uint64
	since  time.Time
	recent []AuthErrorRecord
}

func boundedAuthErrorText(text string, limit int) string {
	value, _ := managementdiag.ProcessText(text, managementdiag.DetailLevelSafe, limit)
	return value
}

func describeAuthError(failure *Error) AuthErrorRecord {
	if failure == nil {
		return AuthErrorRecord{}
	}
	raw := failure.Message
	inputTruncated := len(raw) > 64*1024
	if inputTruncated {
		raw = raw[:64*1024]
	}
	record := AuthErrorRecord{HTTPStatus: failure.HTTPStatus, Code: failure.Code}
	message := raw
	if gjson.Valid(raw) {
		root := gjson.Parse(raw)
		item := root
		if nested := root.Get("error"); nested.IsObject() {
			item = nested
		}
		if value := item.Get("message"); value.Type == gjson.String {
			message = value.String()
		} else if value := root.Get("detail"); value.Type == gjson.String {
			message = value.String()
		} else if value := root.Get("error"); value.Type == gjson.String {
			message = value.String()
		}
		if value := item.Get("code"); value.Type == gjson.String {
			record.Code = value.String()
		}
		record.Type = item.Get("type").String()
	}
	record.Code = boundedAuthErrorText(record.Code, 128)
	record.Type = boundedAuthErrorText(record.Type, 128)
	message, messageTruncated := managementdiag.ProcessResponseBody(message, managementdiag.DetailLevelSafe, 4096)
	record.Message = strings.TrimSpace(message)
	record.Details, record.Truncated = managementdiag.ProcessResponseBody(raw, managementdiag.DetailLevelSafe, 4096)
	record.Truncated = record.Truncated || messageTruncated || inputTruncated
	identity, _ := json.Marshal([]any{record.HTTPStatus, record.Code, record.Type, record.Message})
	digest := sha256.Sum256(identity)
	record.ID = hex.EncodeToString(digest[:])
	return record
}

// recordAuthErrorLocked observes an accepted execution failure without changing
// scheduling, quota, persistence, or the current credential error.
func (m *Manager) recordAuthErrorLocked(authID string, record AuthErrorRecord, now time.Time) {
	if record.ID == "" {
		return
	}
	if m.errorHistory == nil {
		m.errorHistory = make(map[string]*authErrorHistory)
	}
	history := m.errorHistory[authID]
	if history == nil {
		history = &authErrorHistory{since: now.UTC()}
		m.errorHistory[authID] = history
	}
	history.total++
	index := slices.IndexFunc(history.recent, func(entry AuthErrorRecord) bool { return entry.ID == record.ID })
	if index >= 0 {
		previous := history.recent[index]
		record.Count, record.FirstAt, record.Models, record.OtherModels = previous.Count, previous.FirstAt, previous.Models, previous.OtherModels
		history.recent = slices.Delete(history.recent, index, index+1)
	} else {
		record.FirstAt = now.UTC()
	}
	record.Count++
	record.LastAt = now.UTC()
	modelIndex := slices.IndexFunc(record.Models, func(model AuthErrorModelCount) bool { return model.Model == record.LastModel })
	if modelIndex >= 0 {
		record.Models[modelIndex].Count++
		record.Models[modelIndex].LastAt = record.LastAt
	} else if len(record.Models) < authErrorModelLimit {
		record.Models = append(record.Models, AuthErrorModelCount{Model: record.LastModel, Count: 1, LastAt: record.LastAt})
	} else {
		record.OtherModels++
	}
	history.recent = slices.Insert(history.recent, 0, record)
	if len(history.recent) > authErrorHistoryLimit {
		history.recent = slices.Delete(history.recent, authErrorHistoryLimit, len(history.recent))
	}
}

func sortAuthErrorModels(models []AuthErrorModelCount) {
	sort.Slice(models, func(i, j int) bool {
		if models[i].Count != models[j].Count {
			return models[i].Count > models[j].Count
		}
		return models[i].Model < models[j].Model
	})
}

// AuthErrorHistory returns detached, bounded process-local error aggregates.
func (m *Manager) AuthErrorHistory(authID string, details bool) AuthErrorHistorySummary {
	result := AuthErrorHistorySummary{Limit: authErrorHistoryLimit}
	if m == nil {
		return result
	}
	m.mu.RLock()
	auth := m.auths[authID]
	history := m.errorHistory[authID]
	if auth == nil || history == nil {
		m.mu.RUnlock()
		return result
	}
	result.Total, result.Distinct = history.total, len(history.recent)
	since := history.since
	result.Since = &since
	var currentError *Error
	if auth.LastError != nil {
		copy := *auth.LastError
		currentError = &copy
	}
	recent := slices.Clone(history.recent)
	for i := range recent {
		if details {
			recent[i].Models = slices.Clone(recent[i].Models)
		} else {
			recent[i].Models = nil
		}
	}
	m.mu.RUnlock()
	currentID := describeAuthError(currentError).ID
	models := make(map[string]AuthErrorModelCount)
	for _, record := range recent {
		result.RetainedTotal += record.Count
		if record.ID == currentID {
			result.CurrentModel = record.LastModel
		}
		if !details {
			continue
		}
		sortAuthErrorModels(record.Models)
		result.Recent = append(result.Recent, record)
		result.OtherModels += record.OtherModels
		for _, item := range record.Models {
			combined := models[item.Model]
			combined.Model, combined.Count = item.Model, combined.Count+item.Count
			if item.LastAt.After(combined.LastAt) {
				combined.LastAt = item.LastAt
			}
			models[item.Model] = combined
		}
	}
	for _, item := range models {
		result.Models = append(result.Models, item)
	}
	sortAuthErrorModels(result.Models)
	return result
}
