package management

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

func TestPutAndPatchUsagePrices(t *testing.T) {
	handler := &Handler{cfg: &config.Config{}, configFilePath: writeTestConfigFile(t)}

	ctx, recorder := newUsagePricingRequest(http.MethodPut, `{"models":{" GPT-5.4 ":{"input-per-million":1.25,"output-per-million":10,"cached-input-per-million":0.125}}}`)
	handler.PutUsagePrices(ctx)
	if recorder.Code != http.StatusOK {
		t.Fatalf("PUT status = %d, want 200: %s", recorder.Code, recorder.Body.String())
	}
	if _, ok := handler.cfg.UsagePricing.Models["gpt-5.4"]; !ok {
		t.Fatalf("PUT models = %#v, want normalized gpt-5.4", handler.cfg.UsagePricing.Models)
	}

	ctx, recorder = newUsagePricingRequest(http.MethodPatch, `{"models":{"gpt-5.5":{"input-per-million":2}}}`)
	handler.PatchUsagePrices(ctx)
	if recorder.Code != http.StatusOK {
		t.Fatalf("PATCH status = %d, want 200: %s", recorder.Code, recorder.Body.String())
	}
	if len(handler.cfg.UsagePricing.Models) != 2 {
		t.Fatalf("PATCH models = %#v, want merged table", handler.cfg.UsagePricing.Models)
	}

	loaded, err := config.LoadConfig(handler.configFilePath)
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}
	if len(loaded.UsagePricing.Models) != 2 || loaded.UsagePricing.Models["gpt-5.4"].OutputPerMillion != 10 {
		t.Fatalf("persisted models = %#v", loaded.UsagePricing.Models)
	}
}

func TestPutUsagePricesRejectsInvalidPriceWithoutMutation(t *testing.T) {
	handler := &Handler{
		cfg: &config.Config{UsagePricing: config.UsagePricingConfig{Models: map[string]config.UsageModelPrice{
			"model-a": {InputPerMillion: 1},
		}}},
		configFilePath: writeTestConfigFile(t),
	}
	ctx, recorder := newUsagePricingRequest(http.MethodPut, `{"models":{"model-b":{"output-per-million":-1}}}`)
	handler.PutUsagePrices(ctx)
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400: %s", recorder.Code, recorder.Body.String())
	}
	if len(handler.cfg.UsagePricing.Models) != 1 || handler.cfg.UsagePricing.Models["model-a"].InputPerMillion != 1 {
		t.Fatalf("models mutated after invalid PUT: %#v", handler.cfg.UsagePricing.Models)
	}
}

func TestPutUsagePricesRequiresExplicitModelsObject(t *testing.T) {
	tests := []string{
		`null`,
		`{}`,
		`{"model":{}}`,
		`{"models":null}`,
		`{"models":{"model-a":null}}`,
		`{"models":{"model-a":{"input-per-million":null}}}`,
		`{"models":{"model-a":{"unknown-price":1}}}`,
		`{"models":{},"unexpected":true}`,
		`{"models":{}} {}`,
	}
	for _, body := range tests {
		t.Run(body, func(t *testing.T) {
			handler := &Handler{
				cfg: &config.Config{UsagePricing: config.UsagePricingConfig{Models: map[string]config.UsageModelPrice{
					"model-a": {InputPerMillion: 1},
				}}},
				configFilePath: writeTestConfigFile(t),
			}
			ctx, recorder := newUsagePricingRequest(http.MethodPut, body)
			handler.PutUsagePrices(ctx)
			if recorder.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400: %s", recorder.Code, recorder.Body.String())
			}
			if len(handler.cfg.UsagePricing.Models) != 1 {
				t.Fatalf("models mutated after invalid body: %#v", handler.cfg.UsagePricing.Models)
			}
		})
	}
}

func TestPatchUsagePricesUpdatesOnlyProvidedFields(t *testing.T) {
	handler := &Handler{
		cfg: &config.Config{UsagePricing: config.UsagePricingConfig{Models: map[string]config.UsageModelPrice{
			"model-a": {
				InputPerMillion:       1,
				OutputPerMillion:      10,
				CachedInputPerMillion: 0.5,
			},
		}}},
		configFilePath: writeTestConfigFile(t),
	}
	ctx, recorder := newUsagePricingRequest(http.MethodPatch, `{"models":{"model-a":{"input-per-million":2}}}`)
	handler.PatchUsagePrices(ctx)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", recorder.Code, recorder.Body.String())
	}
	price := handler.cfg.UsagePricing.Models["model-a"]
	if price.InputPerMillion != 2 || price.OutputPerMillion != 10 || price.CachedInputPerMillion != 0.5 {
		t.Fatalf("patched price = %#v, want untouched output/cache rates", price)
	}
}

func TestPutUsagePricesAllowsExplicitEmptyModels(t *testing.T) {
	handler := &Handler{
		cfg: &config.Config{UsagePricing: config.UsagePricingConfig{Models: map[string]config.UsageModelPrice{
			"model-a": {InputPerMillion: 1},
		}}},
		configFilePath: writeTestConfigFile(t),
	}
	ctx, recorder := newUsagePricingRequest(http.MethodPut, `{"models":{}}`)
	handler.PutUsagePrices(ctx)
	if recorder.Code != http.StatusOK || len(handler.cfg.UsagePricing.Models) != 0 {
		t.Fatalf("status=%d models=%#v body=%s", recorder.Code, handler.cfg.UsagePricing.Models, recorder.Body.String())
	}
}

func TestPutUsagePricesReplacesPersistedModels(t *testing.T) {
	handler := &Handler{cfg: &config.Config{}, configFilePath: writeTestConfigFile(t)}
	ctx, recorder := newUsagePricingRequest(http.MethodPut, `{"models":{"model-a":{"input-per-million":1},"model-b":{"output-per-million":2}}}`)
	handler.PutUsagePrices(ctx)
	if recorder.Code != http.StatusOK {
		t.Fatalf("first PUT status = %d, want 200: %s", recorder.Code, recorder.Body.String())
	}

	ctx, recorder = newUsagePricingRequest(http.MethodPut, `{"models":{"model-c":{"cached-input-per-million":3}}}`)
	handler.PutUsagePrices(ctx)
	if recorder.Code != http.StatusOK {
		t.Fatalf("replacement PUT status = %d, want 200: %s", recorder.Code, recorder.Body.String())
	}
	loaded, err := config.LoadConfig(handler.configFilePath)
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}
	if len(loaded.UsagePricing.Models) != 1 || loaded.UsagePricing.Models["model-c"].CachedInputPerMillion != 3 {
		t.Fatalf("persisted replacement models = %#v", loaded.UsagePricing.Models)
	}
}

func TestPutUsagePricesRollsBackOnPersistenceFailure(t *testing.T) {
	previous := config.UsagePricingConfig{Models: map[string]config.UsageModelPrice{
		"model-a": {InputPerMillion: 1},
	}}
	handler := &Handler{
		cfg:            &config.Config{UsagePricing: previous},
		configFilePath: filepath.Join(t.TempDir(), "missing", "config.yaml"),
	}
	ctx, recorder := newUsagePricingRequest(http.MethodPut, `{"models":{"model-b":{"input-per-million":2}}}`)
	handler.PutUsagePrices(ctx)
	if recorder.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500: %s", recorder.Code, recorder.Body.String())
	}
	if len(handler.cfg.UsagePricing.Models) != 1 || handler.cfg.UsagePricing.Models["model-a"].InputPerMillion != 1 {
		t.Fatalf("models after persistence failure = %#v, want previous table", handler.cfg.UsagePricing.Models)
	}
}

func TestUsagePriceMutationsRollBackOnPersistenceFailure(t *testing.T) {
	newHandler := func(t *testing.T) *Handler {
		t.Helper()
		return &Handler{
			cfg: &config.Config{UsagePricing: config.UsagePricingConfig{Models: map[string]config.UsageModelPrice{
				"model-a": {InputPerMillion: 1},
				"model-b": {OutputPerMillion: 2},
			}}},
			configFilePath: filepath.Join(t.TempDir(), "missing", "config.yaml"),
		}
	}
	assertUnchanged := func(t *testing.T, handler *Handler, recorder *httptest.ResponseRecorder) {
		t.Helper()
		if recorder.Code != http.StatusInternalServerError {
			t.Fatalf("status = %d, want 500: %s", recorder.Code, recorder.Body.String())
		}
		if len(handler.cfg.UsagePricing.Models) != 2 ||
			handler.cfg.UsagePricing.Models["model-a"].InputPerMillion != 1 ||
			handler.cfg.UsagePricing.Models["model-b"].OutputPerMillion != 2 {
			t.Fatalf("models after persistence failure = %#v, want previous table", handler.cfg.UsagePricing.Models)
		}
	}

	t.Run("patch", func(t *testing.T) {
		handler := newHandler(t)
		ctx, recorder := newUsagePricingRequest(http.MethodPatch, `{"models":{"model-a":{"input-per-million":3}}}`)
		handler.PatchUsagePrices(ctx)
		assertUnchanged(t, handler, recorder)
	})

	t.Run("delete one", func(t *testing.T) {
		handler := newHandler(t)
		ctx, recorder := newUsagePricingRequest(http.MethodDelete, "")
		ctx.Params = gin.Params{{Key: "model", Value: "/model-a"}}
		handler.DeleteUsagePrice(ctx)
		assertUnchanged(t, handler, recorder)
	})

	t.Run("delete all", func(t *testing.T) {
		handler := newHandler(t)
		ctx, recorder := newUsagePricingRequest(http.MethodDelete, "")
		handler.DeleteUsagePrices(ctx)
		assertUnchanged(t, handler, recorder)
	})
}

func TestDeleteUsagePrices(t *testing.T) {
	handler := &Handler{
		cfg: &config.Config{UsagePricing: config.UsagePricingConfig{Models: map[string]config.UsageModelPrice{
			"provider/model-a": {InputPerMillion: 1},
			"model-b":          {OutputPerMillion: 2},
		}}},
		configFilePath: writeTestConfigFile(t),
	}

	ctx, recorder := newUsagePricingRequest(http.MethodDelete, "")
	ctx.Params = gin.Params{{Key: "model", Value: "/provider/model-a"}}
	handler.DeleteUsagePrice(ctx)
	if recorder.Code != http.StatusOK {
		t.Fatalf("DELETE model status = %d, want 200: %s", recorder.Code, recorder.Body.String())
	}
	if len(handler.cfg.UsagePricing.Models) != 1 {
		t.Fatalf("models after single delete = %#v", handler.cfg.UsagePricing.Models)
	}
	loaded, err := config.LoadConfig(handler.configFilePath)
	if err != nil {
		t.Fatalf("LoadConfig() after single delete error = %v", err)
	}
	if len(loaded.UsagePricing.Models) != 1 || loaded.UsagePricing.Models["model-b"].OutputPerMillion != 2 {
		t.Fatalf("persisted models after single delete = %#v", loaded.UsagePricing.Models)
	}

	ctx, recorder = newUsagePricingRequest(http.MethodDelete, "")
	handler.DeleteUsagePrices(ctx)
	if recorder.Code != http.StatusOK || len(handler.cfg.UsagePricing.Models) != 0 {
		t.Fatalf("DELETE all status=%d models=%#v body=%s", recorder.Code, handler.cfg.UsagePricing.Models, recorder.Body.String())
	}
	loaded, err = config.LoadConfig(handler.configFilePath)
	if err != nil {
		t.Fatalf("LoadConfig() after delete all error = %v", err)
	}
	if len(loaded.UsagePricing.Models) != 0 {
		t.Fatalf("persisted models after delete all = %#v", loaded.UsagePricing.Models)
	}
}

func TestDeleteUsagePriceRejectsEmptyModel(t *testing.T) {
	handler := &Handler{cfg: &config.Config{}}
	ctx, recorder := newUsagePricingRequest(http.MethodDelete, "")
	handler.DeleteUsagePrice(ctx)
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400: %s", recorder.Code, recorder.Body.String())
	}
}

func TestGetUsagePricesReturnsStableEmptyMap(t *testing.T) {
	handler := &Handler{cfg: &config.Config{}}
	ctx, recorder := newUsageRequestContext("/v0/management/usage/prices")
	handler.GetUsagePrices(ctx)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", recorder.Code, recorder.Body.String())
	}
	var response config.UsagePricingConfig
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}
	if response.Models == nil || len(response.Models) != 0 {
		t.Fatalf("models = %#v, want empty object", response.Models)
	}
}

func newUsagePricingRequest(method, body string) (*gin.Context, *httptest.ResponseRecorder) {
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(method, "/v0/management/usage/prices", bytes.NewBufferString(body))
	ctx.Request.Header.Set("Content-Type", "application/json")
	return ctx, recorder
}
