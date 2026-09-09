package usage

import (
	"context"
	"encoding/json"
	"testing"
)

func TestUsageStreamModePreservesExplicitFalseAndLegacyDefaults(t *testing.T) {
	if StreamFromContext(nil) || StreamFromContext(context.Background()) {
		t.Fatal("legacy context unexpectedly declares streaming")
	}
	for _, mode := range []bool{false, true} {
		ctx := WithStream(nil, mode)
		if got := WithStreamDefault(ctx, !mode); got != ctx || StreamFromContext(got) != mode {
			t.Fatal("upstream fallback replaced the client's explicit mode")
		}
		if StreamFromContext(WithStreamDefault(nil, mode)) != mode || StreamFromContext(WithStream(ctx, !mode)) == mode || StreamFromContext(ctx) != mode {
			t.Fatal("new request mode mutated a previous request snapshot")
		}
	}
	var record Record
	if err := json.Unmarshal([]byte(`{"Provider":"codex","Model":"fixture"}`), &record); err != nil || record.Stream {
		t.Fatal("legacy records gained an implicit streaming mode")
	}
}
