// Package credentialweight validates optional routing weights shared by config,
// credential storage, management updates and the weighted selector.
package credentialweight

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/tidwall/gjson"
)

const Default int64 = 1
const Max int64 = 1_000_000

// ValidateMetadataJSON preserves integer precision for the weight alone. Other
// credential fields keep the existing decoder and its compatibility semantics.
func ValidateMetadataJSON(data []byte) error {
	if !gjson.ValidBytes(data) {
		return nil
	}
	var weight gjson.Result
	seen, duplicate := false, false
	gjson.ParseBytes(data).ForEach(func(key, value gjson.Result) bool {
		if key.Str != "weight" {
			return true
		}
		if seen {
			duplicate = true
			return false
		}
		seen, weight = true, value
		return true
	})
	if duplicate {
		return fmt.Errorf("duplicate weight field")
	}
	if !seen {
		return nil
	}
	decoder := json.NewDecoder(bytes.NewBufferString(weight.Raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return fmt.Errorf("weight must be an integer")
	}
	_, err := ParseValue(value)
	return err
}

// Normalize follows the upstream capacity range: non-positive integers mean
// zero participation in weighted routing; other routing strategies ignore it.
func Normalize(weight int64) (int64, error) {
	if weight <= 0 {
		return 0, nil
	}
	if weight > Max {
		return 0, fmt.Errorf("weight must not exceed %d", Max)
	}
	return weight, nil
}

func ParseString(value string) (int64, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return Default, nil
	}
	weight, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("weight must be an integer in the signed 64-bit range")
	}
	return Normalize(weight)
}

func ParseValue(value any) (int64, error) {
	switch typed := value.(type) {
	case int:
		return Normalize(int64(typed))
	case int8:
		return Normalize(int64(typed))
	case int16:
		return Normalize(int64(typed))
	case int32:
		return Normalize(int64(typed))
	case int64:
		return Normalize(typed)
	case uint:
		return parseUnsigned(uint64(typed))
	case uint8:
		return parseUnsigned(uint64(typed))
	case uint16:
		return parseUnsigned(uint64(typed))
	case uint32:
		return parseUnsigned(uint64(typed))
	case uint64:
		return parseUnsigned(typed)
	case float32:
		return ParseValue(float64(typed))
	case float64:
		if math.IsNaN(typed) || math.IsInf(typed, 0) || math.Trunc(typed) != typed || typed < math.MinInt64 {
			return 0, fmt.Errorf("weight must be an integer in the signed 64-bit range")
		}
		if typed > float64(Max) {
			return 0, fmt.Errorf("weight must not exceed %d", Max)
		}
		return Normalize(int64(typed))
	case json.Number:
		weight, err := typed.Int64()
		if err != nil {
			return 0, fmt.Errorf("weight must be an integer in the signed 64-bit range")
		}
		return Normalize(weight)
	case string:
		return ParseString(typed)
	default:
		return 0, fmt.Errorf("weight must be an integer")
	}
}

func parseUnsigned(weight uint64) (int64, error) {
	if weight > uint64(Max) {
		return 0, fmt.Errorf("weight must not exceed %d", Max)
	}
	return int64(weight), nil
}
