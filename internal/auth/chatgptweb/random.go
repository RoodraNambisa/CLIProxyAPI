package chatgptweb

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"io"
)

type PKCE struct {
	CodeVerifier  string `json:"code_verifier"`
	CodeChallenge string `json:"code_challenge"`
}

func randomReader(reader io.Reader) io.Reader {
	if reader == nil {
		return rand.Reader
	}
	return reader
}

func randomBytes(reader io.Reader, length int) ([]byte, error) {
	value := make([]byte, length)
	if _, err := io.ReadFull(randomReader(reader), value); err != nil {
		return nil, fmt.Errorf("read random bytes: %w", err)
	}
	return value, nil
}

func randomToken(reader io.Reader, length int) (string, error) {
	value, err := randomBytes(reader, length)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(value), nil
}

func GeneratePKCE(reader io.Reader) (PKCE, error) {
	verifier, err := randomToken(reader, 32)
	if err != nil {
		return PKCE{}, err
	}
	digest := sha256.Sum256([]byte(verifier))
	return PKCE{
		CodeVerifier:  verifier,
		CodeChallenge: base64.RawURLEncoding.EncodeToString(digest[:]),
	}, nil
}

func GenerateState(reader io.Reader) (string, error) {
	return randomToken(reader, 32)
}

func GenerateNonce(reader io.Reader) (string, error) {
	return randomToken(reader, 32)
}

func GenerateDeviceID(reader io.Reader) (string, error) {
	value, err := randomBytes(reader, 16)
	if err != nil {
		return "", err
	}
	value[6] = value[6]&0x0f | 0x40
	value[8] = value[8]&0x3f | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		value[0:4], value[4:6], value[6:8], value[8:10], value[10:16]), nil
}
