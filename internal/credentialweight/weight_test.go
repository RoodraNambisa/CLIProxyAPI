package credentialweight

import (
	"encoding/json"
	"math"
	"strings"
	"testing"
)

func TestCredentialWeightsAcrossMetadataRepresentations(t *testing.T) {
	for _, tc := range []struct {
		value any
		want  int64
	}{
		{"", 1}, {"  ", 1}, {" 12 ", 12}, {int8(-2), 0}, {int16(0), 0}, {int32(3), 3},
		{int64(math.MinInt64), 0}, {uint64(Max), Max}, {uint32(0), 0}, {uint16(3), 3},
		{float64(0), 0}, {float64(-5), 0}, {float32(1), 1}, {json.Number("12"), 12}, {json.Number("-4"), 0},
	} {
		got, err := ParseValue(tc.value)
		if err != nil || got != tc.want {
			t.Fatalf("weight normalization=%d, want %d", got, tc.want)
		}
	}
	for _, value := range []any{nil, true, []any{}, map[string]any{}, "invalid-sensitive-value", "1.5", "9223372036854775808", uint64(math.MaxUint64), Max + 1, float64(Max + 1), -0.5, -1e300, math.NaN(), math.Inf(1), json.Number("1.5")} {
		_, err := ParseValue(value)
		if err == nil {
			t.Fatal("invalid weight accepted")
		}
		if strings.Contains(err.Error(), "invalid-sensitive-value") {
			t.Fatal("validation echoed an invalid field value")
		}
	}
}

func TestCredentialWeightJSONPreservesIntegerPrecision(t *testing.T) {
	if err := ValidateMetadataJSON([]byte(`{"weight":1,"weight":-9223372036854775809}`)); err == nil {
		t.Fatal("duplicate weight bypassed precision validation")
	}
	for _, value := range []string{"-9223372036854775809", "9223372036854775808", "1.5", "1e2", "null", "true", `"invalid"`} {
		if err := ValidateMetadataJSON([]byte(`{"weight":` + value + `}`)); err == nil {
			t.Fatal("raw metadata accepted invalid weight before number coercion")
		}
	}
	for _, value := range []string{"-9223372036854775808", "0", "1000000", `"25"`} {
		if err := ValidateMetadataJSON([]byte(`{"weight":` + value + `}`)); err != nil {
			t.Fatal(err)
		}
	}
}
