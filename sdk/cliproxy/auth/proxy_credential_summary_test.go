package auth

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

func TestProxyCredentialSummariesBoundSubsetAndSecretExclusion(t *testing.T) {
	m := NewManager(nil, nil, nil)
	ids := make([]string, 0, 5000)
	for i := 0; i < 100000; i++ {
		id := fmt.Sprintf("auth-%06d", i)
		m.auths[id] = &Auth{ID: id, Index: id, Provider: "chatgpt-web", FileName: id + ".json", ProxyURL: "http://user:secret-proxy@host:80", Metadata: map[string]any{"token": "secret-token"}}
		if i < 5000 {
			ids = append(ids, id)
		}
	}
	summaries := m.ProxyCredentialSummaries(ids)
	if len(summaries) != 5000 {
		t.Fatal(len(summaries))
	}
	data, _ := json.Marshal(summaries)
	if strings.Contains(string(data), "secret-") || strings.Contains(string(data), "ProxyURL") {
		t.Fatal("credential secrets leaked")
	}
	if len(m.ProxyCredentialSummaries([]string{})) != 0 {
		t.Fatal("empty binding set scanned all credentials")
	}
	if len(m.ProxyCredentialSummaries(nil)) != 100000 {
		t.Fatal("all scope incomplete")
	}
}

func TestProxyCredentialSummariesIncludeLinkedBindingConsumers(t *testing.T) {
	m := NewManager(nil, nil, nil)
	source := &Auth{ID: "source", Provider: "codex", Metadata: map[string]any{"credential_uid": "uid"}}
	web := &Auth{ID: "web", Provider: "chatgpt-web", ProxyURL: "http://ignored.invalid:80", Metadata: map[string]any{
		"refresh_strategy": "codex_source", "source_auth_id": source.ID, "source_credential_uid": "uid",
	}}
	m.auths[source.ID], m.auths[web.ID] = source, web
	m.dependencyDependentIDs["uid"] = map[string]struct{}{web.ID: {}}
	items := m.ProxyCredentialSummaries([]string{source.ID})
	if len(items) != 2 {
		t.Fatal(items)
	}
	for _, item := range items {
		if item.BindingOwnerID != source.ID || item.HasCustomProxy {
			t.Fatalf("linked route did not follow source: %+v", item)
		}
	}
	web.Metadata["source_credential_uid"] = "replacement"
	if len(m.ProxyCredentialSummaries([]string{source.ID})) != 1 {
		t.Fatal("replaced source identity remained in bound candidates")
	}
}
