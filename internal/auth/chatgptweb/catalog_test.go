package chatgptweb

import "testing"

func TestDecodeCatalogInstantCategory(t *testing.T) {
	models, err := DecodeCatalog([]byte(`{"models":[{"slug":"gpt-5-6"},{"slug":"thinking"}],"categories":["bad",{"model_lane":42},{"model_lane":"auto","default_model":"gpt-5-6"},{"model_lane":"thinking","default_model":"thinking"}]}`))
	if err != nil || len(models) != 2 || !models[0].Instant || models[1].Instant {
		t.Fatalf("models = %+v, error = %v", models, err)
	}
	for _, categories := range []string{`null`, `{}`, `"bad"`, `[{"category":"auto_no_tools","default_model":"gpt-5-6"}]`} {
		models, err = DecodeCatalog([]byte(`{"models":[{"slug":"gpt-5-6"}],"categories":` + categories + `}`))
		if err != nil || len(models) != 1 || models[0].Instant {
			t.Fatalf("optional categories %s: models = %+v, error = %v", categories, models, err)
		}
	}
}

func TestDecodeCatalogThinkingEfforts(t *testing.T) {
	models, err := DecodeCatalog([]byte(`{"models":[{"slug":"thinking","thinking_efforts":[null,"bad",{"thinking_effort":"min"},{"thinking_effort":"standard"},{"thinking_effort":"extended"},{"thinking_effort":"max"}]}],"categories":[{"model_lane":"thinking","default_model":"thinking"}]}`))
	if err != nil || len(models) != 1 || !models[0].ThinkingDefault || len(models[0].ThinkingEfforts) != 4 {
		t.Fatalf("models=%+v err=%v", models, err)
	}
}

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
