package chatgptweb

import "testing"

func TestDecodeCatalogFiltersInvalidAndDuplicateModels(t *testing.T) {
	models, err := DecodeCatalog([]byte(`{"models":[
		{"slug":"gpt-5","title":"GPT-5","created":10},
		"malformed",
		42,
		{"slug":"GPT-5"},
		{"slug":"auto"},
		{"title":"missing"},
		{"slug":"gpt-image-2","owned_by":"chatgpt"}
	]}`))
	if err != nil {
		t.Fatalf("DecodeCatalog() error = %v", err)
	}
	if len(models) != 2 || models[0].Slug != "gpt-5" || models[1].Slug != "gpt-image-2" {
		t.Fatalf("models = %#v", models)
	}
}

func TestDecodeCatalogAcceptsValidEmptyCatalog(t *testing.T) {
	for _, payload := range []string{
		`{"models":[]}`,
		`{"models":[{"slug":"auto"}]}`,
		`{"models":[{"slug":"auto"},{"unexpected":true}]}`,
	} {
		models, err := DecodeCatalog([]byte(payload))
		if err != nil {
			t.Fatalf("DecodeCatalog(%s) error = %v", payload, err)
		}
		if len(models) != 0 {
			t.Fatalf("DecodeCatalog(%s) models = %#v", payload, models)
		}
	}
}

func TestDecodeCatalogRejectsMissingModels(t *testing.T) {
	if _, err := DecodeCatalog([]byte(`{}`)); err == nil {
		t.Fatal("expected missing models error")
	}
}

func TestDecodeCatalogRejectsNonEmptyUnrecognizedEntries(t *testing.T) {
	for _, payload := range []string{
		`{"models":[{"title":"field drift"}]}`,
		`{"models":["gpt-5",42]}`,
	} {
		if _, err := DecodeCatalog([]byte(payload)); err == nil {
			t.Fatalf("DecodeCatalog(%s) accepted an unrecognized non-empty catalog", payload)
		}
	}
}
