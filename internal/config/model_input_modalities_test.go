package config

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestModelInputModalitiesValidationAndDefaults(t *testing.T) {
	for _, tc := range []struct {
		value string
		valid bool
	}{
		{"null", true}, {"[]", true}, {"[text]", true}, {"[' TEXT ', image, image, audio, video]", true},
		{"text", false}, {"1", false}, {"{}", false}, {"[1]", false}, {"[true]", false}, {"[null]", false}, {"['']", false}, {"[text, unknown]", false},
	} {
		t.Run(tc.value, func(t *testing.T) {
			body := "openai-compatibility: [{name: fixture, base-url: https://example.test, models: [{name: upstream, input-modalities: " + tc.value + "}]}]\n"
			path := filepath.Join(t.TempDir(), "config.yaml")
			if err := os.WriteFile(path, []byte(body), 0600); err != nil {
				t.Fatal(err)
			}
			_, err := LoadConfigOptional(path, true)
			if (err == nil) != tc.valid {
				t.Fatalf("validation success = %t, want %t", err == nil, tc.valid)
			}
		})
	}
	model := OpenAICompatibilityModel{InputModalities: []string{" TEXT ", "image", "IMAGE"}}
	got := model.GetInputModalities()
	if !reflect.DeepEqual(got, []string{"text", "image"}) {
		t.Fatal("modalities were not normalized in declared order")
	}
	got[0] = "changed"
	if model.InputModalities[0] != " TEXT " || (OpenAICompatibilityModel{}).GetInputModalities() != nil {
		t.Fatal("normalization mutated config or changed the inheritance default")
	}
	for _, raw := range []string{`{"input-modalities":"text"}`, `{"input-modalities":[true]}`, `{"input-modalities":[1]}`} {
		if err := json.Unmarshal([]byte(raw), &OpenAICompatibilityModel{}); err == nil {
			t.Fatal("invalid JSON type accepted")
		}
	}
}

func TestModelInputModalitiesSaveReloadClearAndReject(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	original := []byte("openai-compatibility: [{name: fixture, base-url: https://example.test, models: [{name: upstream, alias: local, input-modalities: [text], future-field: kept}]}]\n")
	if err := os.WriteFile(path, original, 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadConfigOptional(path, true)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(cfg.OpenAICompatibility[0].Models[0].GetInputModalities(), []string{"text"}) {
		t.Fatal("loading discarded an explicit text-only model setting")
	}
	for _, want := range [][]string{{"text", "image"}, nil} {
		cfg.OpenAICompatibility[0].Models[0].InputModalities = want
		if err := SaveConfigPreserveComments(path, cfg); err != nil {
			t.Fatal(err)
		}
		loaded, err := LoadConfigOptional(path, true)
		if err != nil || !reflect.DeepEqual(loaded.OpenAICompatibility[0].Models[0].GetInputModalities(), want) {
			t.Fatal("saved model modalities changed on reload")
		}
	}
	before, err := os.ReadFile(path)
	if err != nil || !bytes.Contains(before, []byte("future-field: kept")) || bytes.Contains(before, []byte("input-modalities")) {
		t.Fatal("clearing modalities changed unknown fields or left an override")
	}
	cfg.OpenAICompatibility[0].Models[0].InputModalities = []string{"text", "invalid"}
	if err := SaveConfigPreserveComments(path, cfg); err == nil {
		t.Fatal("invalid modalities saved")
	}
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("rejected save changed the previous file")
	}
}

func TestModelInputModalitiesYAMLInheritance(t *testing.T) {
	for _, body := range []string{
		"modalities: &modalities [text, 1]\nopenai-compatibility: [{models: [{input-modalities: *modalities}]}]\n",
		"model: &model {input-modalities: [text, false]}\nopenai-compatibility: [{models: [{<<: *model}]}]\n",
	} {
		if err := validateModelCatalogFieldsYAML([]byte(body)); err == nil {
			t.Fatal("invalid inherited modalities accepted")
		}
	}
	valid := "text: &text text\nmodel: &model {input-modalities: invalid}\nopenai-compatibility: [{models: [{<<: *model, input-modalities: [*text]}]}]\n"
	if err := validateModelCatalogFieldsYAML([]byte(valid)); err != nil {
		t.Fatal(err)
	}
}
