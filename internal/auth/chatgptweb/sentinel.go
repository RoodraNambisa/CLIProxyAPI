package chatgptweb

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"strings"
	"time"
)

const (
	defaultSentinelBaseURL = "https://sentinel.openai.com"
	sentinelSDKURL         = "https://sentinel.openai.com/sentinel/20260219f9f6/sdk.js"
	sentinelErrorPrefix    = "wQ8Lk5FbGpA2NcR9dShT6gYjU7VxZ4D"
	defaultPoWMaxAttempts  = 500_000
)

type SentinelGenerator struct {
	deviceID    string
	persona     Persona
	sid         string
	random      io.Reader
	now         func() time.Time
	maxAttempts int
}

func NewSentinelGenerator(deviceID string, persona Persona, reader io.Reader, now func() time.Time) (*SentinelGenerator, error) {
	if strings.TrimSpace(deviceID) == "" {
		return nil, fmt.Errorf("sentinel device ID is empty")
	}
	if now == nil {
		now = time.Now
	}
	sid, err := GenerateDeviceID(reader)
	if err != nil {
		return nil, fmt.Errorf("generate sentinel session ID: %w", err)
	}
	return &SentinelGenerator{
		deviceID:    strings.TrimSpace(deviceID),
		persona:     normalizePersona(persona),
		sid:         sid,
		random:      randomReader(reader),
		now:         now,
		maxAttempts: defaultPoWMaxAttempts,
	}, nil
}

func (generator *SentinelGenerator) GenerateRequirementsToken() (string, error) {
	configuration, err := generator.configuration()
	if err != nil {
		return "", err
	}
	configuration[3] = 1
	randomValue, err := generator.randomFloat64()
	if err != nil {
		return "", err
	}
	configuration[9] = math.RoundToEven(5 + randomValue*45)
	payload, err := base64JSON(configuration)
	if err != nil {
		return "", err
	}
	return "gAAAAAC" + payload, nil
}

func (generator *SentinelGenerator) GenerateProof(seed, difficulty string) (string, error) {
	startedAt := generator.now()
	configuration, err := generator.configuration()
	if err != nil {
		return "", err
	}
	difficulty = strings.TrimSpace(difficulty)
	if difficulty == "" {
		difficulty = "0"
	}
	for attempt := 0; attempt < generator.maxAttempts; attempt++ {
		configuration[3] = attempt
		configuration[9] = math.RoundToEven(float64(generator.now().Sub(startedAt).Microseconds()) / 1000)
		payload, err := base64JSON(configuration)
		if err != nil {
			return "", err
		}
		hash := fnv1a32(seed + payload)
		prefixLength := len(difficulty)
		if prefixLength > len(hash) {
			prefixLength = len(hash)
		}
		if hash[:prefixLength] <= difficulty {
			return "gAAAAAB" + payload + "~S", nil
		}
	}
	failure, err := base64JSON("None")
	if err != nil {
		return "", err
	}
	return "gAAAAAB" + sentinelErrorPrefix + failure, nil
}

func (generator *SentinelGenerator) configuration() ([]any, error) {
	perfRandom, err := generator.randomFloat64()
	if err != nil {
		return nil, err
	}
	perfNow := 1000 + perfRandom*49_000
	firstRandom, err := generator.randomFloat64()
	if err != nil {
		return nil, err
	}
	secondRandom, err := generator.randomFloat64()
	if err != nil {
		return nil, err
	}
	prototypeProbe, err := randomChoice(generator.random, []string{
		"vendorSub-undefined", "plugins-undefined", "mimeTypes-undefined", "hardwareConcurrency-undefined",
	})
	if err != nil {
		return nil, err
	}
	documentProbe, err := randomChoice(generator.random, []string{
		"location", "implementation", "URL", "documentURI", "compatMode",
	})
	if err != nil {
		return nil, err
	}
	windowProbe, err := randomChoice(generator.random, []string{
		"Object", "Function", "Array", "Number", "parseFloat", "undefined",
	})
	if err != nil {
		return nil, err
	}
	randomHardware, err := randomChoice(generator.random, []int{4, 8, 12, 16})
	if err != nil {
		return nil, err
	}
	hardware := generator.persona.HardwareConcurrency
	if hardware <= 0 {
		hardware = randomHardware
	}
	now := generator.now().UTC()
	return []any{
		fmt.Sprintf("%dx%d", generator.persona.ScreenWidth, generator.persona.ScreenHeight),
		now.Format("Mon Jan 02 2006 15:04:05") + " GMT+0000 (Coordinated Universal Time)",
		uint64(4_294_705_152),
		firstRandom,
		generator.persona.UserAgent,
		sentinelSDKURL,
		nil,
		nil,
		generator.persona.Language,
		secondRandom,
		prototypeProbe,
		documentProbe,
		windowProbe,
		perfNow,
		generator.sid,
		"",
		hardware,
		float64(now.UnixMilli()) - perfNow,
	}, nil
}

func (generator *SentinelGenerator) randomFloat64() (float64, error) {
	var value [8]byte
	if _, err := io.ReadFull(generator.random, value[:]); err != nil {
		return 0, fmt.Errorf("read sentinel random value: %w", err)
	}
	return float64(binary.BigEndian.Uint64(value[:])>>11) / float64(uint64(1)<<53), nil
}

func randomChoice[T any](reader io.Reader, values []T) (T, error) {
	var zero T
	if len(values) == 0 {
		return zero, fmt.Errorf("sentinel random choice has no values")
	}
	var value [8]byte
	if _, err := io.ReadFull(reader, value[:]); err != nil {
		return zero, fmt.Errorf("read sentinel random choice: %w", err)
	}
	return values[binary.BigEndian.Uint64(value[:])%uint64(len(values))], nil
}

func base64JSON(value any) (string, error) {
	payload, err := json.Marshal(value)
	if err != nil {
		return "", fmt.Errorf("encode sentinel payload: %w", err)
	}
	return base64.StdEncoding.EncodeToString(payload), nil
}

func fnv1a32(value string) string {
	hash := uint32(2_166_136_261)
	for _, character := range value {
		hash ^= uint32(character)
		hash *= 16_777_619
	}
	hash ^= hash >> 16
	hash *= 2_246_822_507
	hash ^= hash >> 13
	hash *= 3_266_489_909
	hash ^= hash >> 16
	return fmt.Sprintf("%08x", hash)
}

type Sentinel struct {
	client    *Client
	baseURL   string
	authURL   string
	deviceID  string
	generator *SentinelGenerator
}

func NewSentinel(client *Client, baseURL, authURL, deviceID string, reader io.Reader, now func() time.Time) (*Sentinel, error) {
	if client == nil {
		return nil, fmt.Errorf("sentinel browser client is nil")
	}
	generator, err := NewSentinelGenerator(deviceID, client.Persona(), reader, now)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(baseURL) == "" {
		baseURL = defaultSentinelBaseURL
	}
	return &Sentinel{
		client:    client,
		baseURL:   strings.TrimRight(strings.TrimSpace(baseURL), "/"),
		authURL:   strings.TrimRight(strings.TrimSpace(authURL), "/"),
		deviceID:  strings.TrimSpace(deviceID),
		generator: generator,
	}, nil
}

func (sentinel *Sentinel) Token(ctx context.Context, flow string) (string, error) {
	requirementsToken, err := sentinel.generator.GenerateRequirementsToken()
	if err != nil {
		return "", newAuthError("sentinel_generation_failed", LifecycleLoginPending, 0, false, true, err.Error(), err)
	}
	requestBody := map[string]any{
		"p":    requirementsToken,
		"id":   sentinel.deviceID,
		"flow": flow,
	}
	response, payload, err := sentinel.client.DoJSON(ctx, true, http.MethodPost,
		sentinel.baseURL+"/backend-api/sentinel/req",
		map[string]string{
			"accept":         "application/json",
			"content-type":   "text/plain;charset=UTF-8",
			"origin":         sentinel.baseURL,
			"referer":        sentinel.baseURL + "/backend-api/sentinel/frame.html",
			"sec-fetch-dest": "empty",
			"sec-fetch-mode": "cors",
			"sec-fetch-site": "same-origin",
		}, requestBody)
	if err != nil {
		return "", newAuthError("sentinel_network_error", LifecycleLoginPending, 0, true, false, "sentinel request failed", err)
	}
	if response.StatusCode != http.StatusOK {
		if response.StatusCode == http.StatusTooManyRequests || response.StatusCode >= http.StatusInternalServerError {
			return "", newAuthError("sentinel_transient_error", LifecycleLoginPending, response.StatusCode, true, false, "sentinel request was not accepted", nil)
		}
		return "", newAuthError("sentinel_rejected", LifecycleInteractionRequired, response.StatusCode, false, true, "sentinel interaction is required", nil)
	}

	var challenge map[string]any
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.UseNumber()
	if err := decoder.Decode(&challenge); err != nil {
		return "", newAuthError("sentinel_response_invalid", LifecycleLoginPending, response.StatusCode, true, false, "sentinel returned invalid JSON", err)
	}
	if interaction := sentinelInteraction(challenge); interaction != "" {
		return "", newAuthError(interaction, LifecycleInteractionRequired, response.StatusCode, false, true, "interactive challenge is required", nil)
	}
	challengeToken := stringValue(challenge["token"])
	if challengeToken == "" {
		return "", newAuthError("sentinel_token_missing", LifecycleLoginPending, response.StatusCode, true, false, "sentinel response did not include a challenge token", nil)
	}

	proof := ""
	proofOfWork, _ := challenge["proofofwork"].(map[string]any)
	if boolValue(proofOfWork["required"]) {
		seed := stringValue(proofOfWork["seed"])
		if seed == "" {
			return "", newAuthError("sentinel_pow_invalid", LifecycleLoginPending, response.StatusCode, true, false, "sentinel proof-of-work seed is missing", nil)
		}
		proof, err = sentinel.generator.GenerateProof(seed, stringValue(proofOfWork["difficulty"]))
	} else {
		proof, err = sentinel.generator.GenerateRequirementsToken()
	}
	if err != nil {
		return "", newAuthError("sentinel_generation_failed", LifecycleLoginPending, response.StatusCode, false, true, err.Error(), err)
	}
	headerValue, err := json.Marshal(map[string]any{
		"p":    proof,
		"t":    "",
		"c":    challengeToken,
		"id":   sentinel.deviceID,
		"flow": flow,
	})
	if err != nil {
		return "", newAuthError("sentinel_generation_failed", LifecycleLoginPending, response.StatusCode, false, true, "encode sentinel token", err)
	}
	if sentinel.authURL != "" {
		if err := sentinel.client.SetCookie(sentinel.authURL, "oai-sc", "0"+challengeToken); err != nil {
			return "", newAuthError("sentinel_cookie_failed", LifecycleLoginPending, 0, false, true, "persist sentinel cookie", err)
		}
	}
	return string(headerValue), nil
}

func sentinelInteraction(challenge map[string]any) string {
	for _, candidate := range []struct {
		key  string
		code string
	}{
		{key: "turnstile", code: "turnstile_required"},
		{key: "arkose", code: "arkose_required"},
		{key: "arkose_labs", code: "arkose_required"},
	} {
		value, ok := challenge[candidate.key]
		if !ok || value == nil {
			continue
		}
		switch typed := value.(type) {
		case bool:
			if typed {
				return candidate.code
			}
		case map[string]any:
			if required, exists := typed["required"]; exists {
				if !boolValue(required) {
					continue
				}
				return candidate.code
			}
			if len(typed) > 0 {
				return candidate.code
			}
		case string:
			if strings.TrimSpace(typed) != "" {
				return candidate.code
			}
		}
	}
	return ""
}

func stringValue(value any) string {
	text, _ := value.(string)
	return strings.TrimSpace(text)
}

func boolValue(value any) bool {
	result, _ := value.(bool)
	return result
}
