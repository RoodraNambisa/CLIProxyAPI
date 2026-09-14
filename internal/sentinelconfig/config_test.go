package sentinelconfig

import (
	"encoding/json"
	"testing"
)

func TestRejectedPartialJSONDoesNotMutatePreviousSnapshot(t *testing.T) {
	scopes := []string{"images"}
	budget := 30
	remote := Remote{Scopes: &scopes, BudgetSeconds: &budget, Nodes: []Node{{Name: "original", URL: "https://example.com", APIKey: "key"}}}
	before, _ := json.Marshal(remote)
	if err := json.Unmarshal([]byte(`{"budget-seconds":12,"nodes":[{"name":"changed"}],"unknown":true}`), &remote); err == nil {
		t.Fatal("unknown field accepted")
	}
	after, _ := json.Marshal(remote)
	if string(before) != string(after) || budget != 30 {
		t.Fatal("rejected update mutated snapshot")
	}
	server := Server{MemoryBudgetMiB: &budget}
	if err := json.Unmarshal([]byte(`{"memory-budget-mib":1024,"unknown":true}`), &server); err == nil {
		t.Fatal("unknown field accepted")
	}
	if budget != 30 {
		t.Fatal("rejected server update mutated snapshot")
	}
}
