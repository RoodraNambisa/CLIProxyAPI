package jsonsemantic

import (
	"strings"
	"testing"
)

func TestNumbersEqual(t *testing.T) {
	tests := []struct {
		name   string
		first  string
		second string
		want   bool
	}{
		{name: "integer and decimal", first: "1", second: "1.0", want: true},
		{name: "decimal and exponent", first: "0.01", second: "1e-2", want: true},
		{name: "trailing zeros and exponent", first: "1000e-3", second: "1", want: true},
		{name: "negative zero", first: "-0.0e999", second: "0", want: true},
		{name: "different signs", first: "-1", second: "1", want: false},
		{name: "different values", first: "9007199254740992", second: "9007199254740993", want: false},
		{name: "invalid exponent", first: "1e+", second: "1", want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := NumbersEqual(test.first, test.second); got != test.want {
				t.Fatalf("NumbersEqual(%q, %q) = %v, want %v", test.first, test.second, got, test.want)
			}
		})
	}
}

func TestNumbersEqualHandlesLargeExponentWithoutExpansion(t *testing.T) {
	exponent := "1" + strings.Repeat("0", 2048)
	previousExponent := strings.Repeat("9", 2048)
	if !NumbersEqual("1e"+exponent, "10e"+previousExponent) {
		t.Fatal("equivalent large exponents compared unequal")
	}

	large := "1e1" + strings.Repeat("0", 4096)
	if !NumbersEqual(large, "10e"+strings.Repeat("9", 4096)) {
		t.Fatal("equivalent oversized numbers compared unequal")
	}
	if !NumbersEqual(large, large) {
		t.Fatal("identical oversized numbers compared unequal")
	}
	if NumbersEqual(large, "9e"+strings.Repeat("9", 4096)) {
		t.Fatal("distinct oversized numbers compared equal")
	}
}
