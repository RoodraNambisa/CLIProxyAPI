package chatgptweb

import (
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base32"
	"encoding/binary"
	"fmt"
	"strings"
	"time"
)

func GenerateTOTP(secret string, at time.Time) (string, error) {
	return generateTOTP(secret, at, 6)
}

func generateTOTP(secret string, at time.Time, digits int) (string, error) {
	if digits <= 0 || digits > 10 {
		return "", fmt.Errorf("invalid TOTP digit count %d", digits)
	}
	normalized := strings.ToUpper(strings.ReplaceAll(strings.TrimSpace(secret), " ", ""))
	normalized = strings.TrimRight(normalized, "=")
	key, err := base32.StdEncoding.WithPadding(base32.NoPadding).DecodeString(normalized)
	if err != nil {
		return "", fmt.Errorf("decode TOTP secret: %w", err)
	}
	if len(key) == 0 {
		return "", fmt.Errorf("TOTP secret is empty")
	}

	counter := uint64(at.Unix() / 30)
	message := make([]byte, 8)
	binary.BigEndian.PutUint64(message, counter)
	mac := hmac.New(sha1.New, key)
	_, _ = mac.Write(message)
	digest := mac.Sum(nil)
	offset := digest[len(digest)-1] & 0x0f
	value := binary.BigEndian.Uint32(digest[offset:offset+4]) & 0x7fffffff

	modulus := uint64(1)
	for range digits {
		modulus *= 10
	}
	return fmt.Sprintf("%0*d", digits, uint64(value)%modulus), nil
}
