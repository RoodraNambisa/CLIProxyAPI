package auth

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
)

func historyFixture(t *testing.T) (*Manager, *Auth) {
	t.Helper()
	m := NewManager(nil, nil, nil)
	a, err := m.Register(WithSkipPersist(t.Context()), &Auth{ID: t.Name(), Provider: "codex"})
	if err != nil {
		t.Fatal(err)
	}
	return m, a
}

func TestAuthErrorHistoryGroupsErrorsAndSeparatesModels(t *testing.T) {
	m, a := historyFixture(t)
	for index, model := range []string{"gpt-a(high)", "gpt-b", "gpt-a(low)"} {
		failure := &Error{Code: "http_error", HTTPStatus: 503, Message: fmt.Sprintf(`{"type":"error","error":{"type":"service_unavailable_error","code":"server_overloaded","message":"Our servers are overloaded","headers":{"Authorization":"Bearer secret-fixture"}},"sequence_number":%d}`, index)}
		m.MarkResult(t.Context(), Result{AuthID: a.ID, Provider: "codex", Model: model, Error: failure})
	}
	summary := m.AuthErrorHistory(a.ID, true)
	if summary.Total != 3 || summary.RetainedTotal != 3 || summary.Distinct != 1 || len(summary.Recent) != 1 || summary.CurrentModel != "gpt-a" {
		t.Fatalf("unexpected summary: %+v", summary)
	}
	record := summary.Recent[0]
	if record.Count != 3 || record.Code != "server_overloaded" || record.Type != "service_unavailable_error" || record.Message != "Our servers are overloaded" || len(record.Models) != 2 || record.Models[0].Model != "gpt-a" || record.Models[0].Count != 2 || record.Models[1].Count != 1 {
		t.Fatalf("incorrect grouping: %+v", record)
	}
	encoded, _ := json.Marshal(summary)
	if strings.Contains(string(encoded), "secret-fixture") {
		t.Fatal("error history exposed a credential")
	}
	summary.Recent[0].Models[0].Count = 999
	if m.AuthErrorHistory(a.ID, true).Recent[0].Models[0].Count != 2 {
		t.Fatal("caller changed counters")
	}
	if light := m.AuthErrorHistory(a.ID, false); len(light.Recent) != 0 || len(light.Models) != 0 || light.Total != 3 {
		t.Fatal("card summary includes full history")
	}
	m.MarkResult(t.Context(), Result{AuthID: a.ID, Success: true})
	if after := m.AuthErrorHistory(a.ID, true); after.Total != 3 || after.CurrentModel != "" {
		t.Fatal("success erased history or retained current error model")
	}
}

func TestAuthErrorHistoryBoundsEvictionAndModelCounts(t *testing.T) {
	m, a := historyFixture(t)
	mark := func(message, model string) {
		m.MarkResult(t.Context(), Result{AuthID: a.ID, Model: model, Error: &Error{HTTPStatus: 500, Message: message}})
	}
	for i := range authErrorHistoryLimit {
		mark(fmt.Sprint(i), "model")
	}
	mark("0", "model")
	mark("new", "model")
	summary := m.AuthErrorHistory(a.ID, true)
	if summary.Total != 22 || summary.RetainedTotal != 21 || len(summary.Recent) != 20 || summary.Recent[0].Message != "new" || summary.Recent[1].Message != "0" || summary.Recent[1].Count != 2 {
		t.Fatalf("incorrect recent eviction: %+v", summary)
	}
	for i := range authErrorModelLimit + 3 {
		mark("bounded models", fmt.Sprintf("model-%d", i))
	}
	group := m.AuthErrorHistory(a.ID, true).Recent[0]
	if len(group.Models) != authErrorModelLimit || group.OtherModels != 3 || group.Count != authErrorModelLimit+3 {
		t.Fatal("model cardinality is not bounded")
	}
	mark(strings.Repeat("long", 40000), strings.Repeat("model", 500))
	group = m.AuthErrorHistory(a.ID, true).Recent[0]
	if !group.Truncated || len(group.Details) > 4096 || len(group.Message) > 4096 || len(group.LastModel) > 256 {
		t.Fatal("diagnostic text is not bounded")
	}
}

func TestAuthErrorHistoryIgnoresRetiredResultsAndCleansRemoval(t *testing.T) {
	m, a := historyFixture(t)
	result := resultForAuth(a, "codex", "model", false)
	result.Error = &Error{HTTPStatus: 503, Message: "overloaded"}
	result.authInstanceID = "retired-instance"
	m.markExecutionResult(t.Context(), result)
	if m.AuthErrorHistory(a.ID, true).Total != 0 {
		t.Fatal("stale execution counted")
	}
	result.authInstanceID = a.instanceID
	m.markExecutionResult(t.Context(), result)
	if m.AuthErrorHistory(a.ID, true).Total != 1 {
		t.Fatal("accepted failure not counted")
	}
	m.mu.Lock()
	m.removeAuthLocked(a.ID)
	_, retained := m.errorHistory[a.ID]
	m.mu.Unlock()
	if retained || m.AuthErrorHistory(a.ID, true).Total != 0 {
		t.Fatal("deleted credential retained errors")
	}
	if NewManager(nil, nil, nil).AuthErrorHistory(a.ID, true).Total != 0 {
		t.Fatal("history persisted across managers")
	}
}

func TestAuthErrorHistoryConcurrentSnapshots(t *testing.T) {
	m, a := historyFixture(t)
	var workers sync.WaitGroup
	for range 4 {
		workers.Go(func() {
			for range 25 {
				m.MarkResult(t.Context(), Result{AuthID: a.ID, Model: "model", Error: &Error{HTTPStatus: 503, Message: "overloaded"}})
				summary := m.AuthErrorHistory(a.ID, true)
				if summary.Total != summary.RetainedTotal || len(summary.Models) != 1 || summary.Models[0].Count != summary.Total {
					t.Error("inconsistent history snapshot")
				}
			}
		})
	}
	workers.Wait()
	if m.AuthErrorHistory(a.ID, true).Total != 100 {
		t.Fatal("concurrent updates lost failures")
	}
}

func TestAuthErrorHistoryDoesNotMergeDifferentTruncatedMessages(t *testing.T) {
	prefix := strings.Repeat("same prefix ", 500)
	first := describeAuthError(&Error{HTTPStatus: 500, Message: prefix + "first reason"})
	second := describeAuthError(&Error{HTTPStatus: 500, Message: prefix + "second reason"})
	if first.Message != second.Message || !first.Truncated || !second.Truncated {
		t.Fatal("fixture did not reach the display limit")
	}
	if first.ID == second.ID {
		t.Fatal("different errors merged after display truncation")
	}
}
