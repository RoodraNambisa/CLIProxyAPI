// Package jsonsemantic contains helpers for comparing decoded JSON values.
package jsonsemantic

import (
	"strconv"
	"strings"
)

type canonicalNumber struct {
	negative bool
	digits   string
	scale    string
}

// NumbersEqual compares two validated JSON number strings exactly without
// expanding their decimal exponents into large rational integers.
func NumbersEqual(first, second string) bool {
	if first == second {
		return true
	}
	firstCanonical, firstOK := canonicalizeNumber(first)
	secondCanonical, secondOK := canonicalizeNumber(second)
	return firstOK && secondOK && firstCanonical == secondCanonical
}

func canonicalizeNumber(value string) (canonicalNumber, bool) {
	if value == "" {
		return canonicalNumber{}, false
	}
	negative := false
	if value[0] == '-' {
		negative = true
		value = value[1:]
	}
	if value == "" {
		return canonicalNumber{}, false
	}

	mantissa := value
	exponentText := "0"
	if exponentIndex := strings.IndexAny(value, "eE"); exponentIndex >= 0 {
		mantissa = value[:exponentIndex]
		exponentText = value[exponentIndex+1:]
		if exponentText == "" {
			return canonicalNumber{}, false
		}
	}
	integerPart := mantissa
	fractionPart := ""
	if decimalIndex := strings.IndexByte(mantissa, '.'); decimalIndex >= 0 {
		integerPart = mantissa[:decimalIndex]
		fractionPart = mantissa[decimalIndex+1:]
	}
	if integerPart == "" || !decimalDigits(integerPart) || !decimalDigits(fractionPart) {
		return canonicalNumber{}, false
	}
	digits := strings.TrimLeft(integerPart+fractionPart, "0")
	if digits == "" {
		return canonicalNumber{scale: "0"}, true
	}
	trailingZeros := len(digits) - len(strings.TrimRight(digits, "0"))
	digits = digits[:len(digits)-trailingZeros]

	scale, ok := addJSONNumberScale(exponentText, trailingZeros-len(fractionPart))
	if !ok {
		return canonicalNumber{}, false
	}
	return canonicalNumber{
		negative: negative,
		digits:   digits,
		scale:    scale,
	}, true
}

func addJSONNumberScale(exponent string, delta int) (string, bool) {
	negative, digits, ok := parseSignedDecimal(exponent)
	if !ok {
		return "", false
	}
	deltaText := strconv.Itoa(delta)
	deltaNegative := strings.HasPrefix(deltaText, "-")
	deltaDigits := strings.TrimPrefix(deltaText, "-")
	deltaDigits = strings.TrimLeft(deltaDigits, "0")
	if deltaDigits == "" {
		deltaDigits = "0"
		deltaNegative = false
	}
	if digits == "0" {
		return formatSignedDecimal(deltaNegative, deltaDigits), true
	}
	if deltaDigits == "0" {
		return formatSignedDecimal(negative, digits), true
	}
	if negative == deltaNegative {
		return formatSignedDecimal(negative, addDecimalMagnitudes(digits, deltaDigits)), true
	}
	switch compareDecimalMagnitudes(digits, deltaDigits) {
	case 0:
		return "0", true
	case 1:
		return formatSignedDecimal(negative, subtractDecimalMagnitudes(digits, deltaDigits)), true
	default:
		return formatSignedDecimal(deltaNegative, subtractDecimalMagnitudes(deltaDigits, digits)), true
	}
}

func parseSignedDecimal(value string) (negative bool, digits string, ok bool) {
	if value == "" {
		return false, "", false
	}
	switch value[0] {
	case '-':
		negative = true
		value = value[1:]
	case '+':
		value = value[1:]
	}
	if value == "" || !decimalDigits(value) {
		return false, "", false
	}
	digits = strings.TrimLeft(value, "0")
	if digits == "" {
		return false, "0", true
	}
	return negative, digits, true
}

func formatSignedDecimal(negative bool, digits string) string {
	if digits == "0" || !negative {
		return digits
	}
	return "-" + digits
}

func compareDecimalMagnitudes(left, right string) int {
	if len(left) < len(right) {
		return -1
	}
	if len(left) > len(right) {
		return 1
	}
	return strings.Compare(left, right)
}

func addDecimalMagnitudes(left, right string) string {
	length := len(left)
	if len(right) > length {
		length = len(right)
	}
	result := make([]byte, length+1)
	leftIndex := len(left) - 1
	rightIndex := len(right) - 1
	carry := 0
	for resultIndex := length; resultIndex >= 1; resultIndex-- {
		sum := carry
		if leftIndex >= 0 {
			sum += int(left[leftIndex] - '0')
			leftIndex--
		}
		if rightIndex >= 0 {
			sum += int(right[rightIndex] - '0')
			rightIndex--
		}
		result[resultIndex] = byte(sum%10) + '0'
		carry = sum / 10
	}
	if carry == 0 {
		return string(result[1:])
	}
	result[0] = byte(carry) + '0'
	return string(result)
}

func subtractDecimalMagnitudes(left, right string) string {
	result := make([]byte, len(left))
	rightIndex := len(right) - 1
	borrow := 0
	for index := len(left) - 1; index >= 0; index-- {
		digit := int(left[index]-'0') - borrow
		if rightIndex >= 0 {
			digit -= int(right[rightIndex] - '0')
			rightIndex--
		}
		if digit < 0 {
			digit += 10
			borrow = 1
		} else {
			borrow = 0
		}
		result[index] = byte(digit) + '0'
	}
	trimmed := strings.TrimLeft(string(result), "0")
	if trimmed == "" {
		return "0"
	}
	return trimmed
}

func decimalDigits(value string) bool {
	for _, char := range value {
		if char < '0' || char > '9' {
			return false
		}
	}
	return true
}
