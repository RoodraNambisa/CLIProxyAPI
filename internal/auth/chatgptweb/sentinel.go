package chatgptweb

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"strings"
	"time"
)

const (
	defaultSentinelBaseURL = "https://sentinel.openai.com"
	sentinelSDKVersion     = "20260219f9f6"
	sentinelSDKURL         = "https://sentinel.openai.com/sentinel/" + sentinelSDKVersion + "/sdk.js"
	sentinelSDKSHA256      = "4f8ef8d5870894fd0101fc40ff45ea13c0f8e25c71c2ba28e5df5baf98babbb5"
	sentinelErrorPrefix    = "wQ8Lk5FbGpA2NcR9dShT6gYjU7VxZ4D"
	defaultPoWMaxAttempts  = 500_000
)

type SentinelGenerator struct {
	deviceID           string
	persona            Persona
	browserEnvironment BrowserEnvironmentIdentity
	sid                string
	random             io.Reader
	now                func() time.Time
	maxAttempts        int
}

func NewSentinelGenerator(deviceID string, persona Persona, reader io.Reader, now func() time.Time) (*SentinelGenerator, error) {
	return NewSentinelGeneratorWithEnvironment(deviceID, persona, BrowserEnvironmentIdentity{}, reader, now)
}

// NewSentinelGeneratorWithEnvironment creates a proof generator bound to one
// immutable transport Persona and one stable browser environment.
func NewSentinelGeneratorWithEnvironment(deviceID string, persona Persona, browserEnvironment BrowserEnvironmentIdentity, reader io.Reader, now func() time.Time) (*SentinelGenerator, error) {
	if strings.TrimSpace(deviceID) == "" {
		return nil, fmt.Errorf("sentinel device ID is empty")
	}
	deviceID = strings.TrimSpace(deviceID)
	persona = canonicalPersona(persona)
	if _, ok := browserEnvironmentSlot(persona, browserEnvironment); !ok {
		browserEnvironment = browserEnvironmentIdentityForSeed(persona, deviceID)
	}
	if now == nil {
		now = time.Now
	}
	sid, err := GenerateDeviceID(reader)
	if err != nil {
		return nil, fmt.Errorf("generate sentinel session ID: %w", err)
	}
	return &SentinelGenerator{
		deviceID:           deviceID,
		persona:            persona,
		browserEnvironment: browserEnvironment,
		sid:                sid,
		random:             randomReader(reader),
		now:                now,
		maxAttempts:        defaultPoWMaxAttempts,
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
	return generator.GenerateProofContext(context.Background(), seed, difficulty)
}

func (generator *SentinelGenerator) GenerateProofContext(ctx context.Context, seed, difficulty string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
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
		if err := ctx.Err(); err != nil {
			return "", err
		}
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
	profile := resolveSentinelBrowserProfile(ConversationTurnstileEnvironment{
		Persona:            generator.persona,
		BrowserEnvironment: generator.browserEnvironment,
		DeviceID:           generator.deviceID,
	})
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
	hardware := profile.hardwareConcurrency
	if hardware <= 0 {
		hardware = randomHardware
	}
	now := generator.now().UTC()
	return []any{
		fmt.Sprintf("%dx%d", profile.screenWidth, profile.screenHeight),
		now.Format("Mon Jan 02 2006 15:04:05") + " GMT+0000 (Coordinated Universal Time)",
		uint64(profile.jsHeapSizeLimit),
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
	client          *Client
	baseURL         string
	authURL         string
	deviceID        string
	generator       *SentinelGenerator
	turnstileSolver func(context.Context, string, string, ConversationTurnstileEnvironment, io.Reader, func() time.Time) (string, error)
}

// SentinelHeaders contains the authentication headers derived from one
// Sentinel challenge. SOToken is present only when the challenge requires a
// Session Observer.
type SentinelHeaders struct {
	Token   string
	SOToken string
}

func NewSentinel(client *Client, baseURL, authURL, deviceID string, reader io.Reader, now func() time.Time) (*Sentinel, error) {
	return NewSentinelWithEnvironment(client, baseURL, authURL, deviceID, BrowserEnvironmentIdentity{}, reader, now)
}

// NewSentinelWithEnvironment creates a Sentinel client that reuses the
// credential's stable browser environment for proof and Turnstile work.
func NewSentinelWithEnvironment(client *Client, baseURL, authURL, deviceID string, browserEnvironment BrowserEnvironmentIdentity, reader io.Reader, now func() time.Time) (*Sentinel, error) {
	if client == nil {
		return nil, fmt.Errorf("sentinel browser client is nil")
	}
	generator, err := NewSentinelGeneratorWithEnvironment(deviceID, client.Persona(), browserEnvironment, reader, now)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(baseURL) == "" {
		baseURL = defaultSentinelBaseURL
	}
	return &Sentinel{
		client:          client,
		baseURL:         strings.TrimRight(strings.TrimSpace(baseURL), "/"),
		authURL:         strings.TrimRight(strings.TrimSpace(authURL), "/"),
		deviceID:        strings.TrimSpace(deviceID),
		generator:       generator,
		turnstileSolver: BuildConversationTurnstileTokenWithEnvironment,
	}, nil
}

func (sentinel *Sentinel) Token(ctx context.Context, flow string) (string, error) {
	headers, err := sentinel.generateHeaders(ctx, flow, nil, false)
	if err != nil {
		return "", err
	}
	return headers.Token, nil
}

// Headers generates both Sentinel headers for authentication endpoints that
// require a Session Observer. The observer runtime remains executor-owned.
func (sentinel *Sentinel) Headers(ctx context.Context, flow string, beginObserver SentinelObserverStarter) (SentinelHeaders, error) {
	return sentinel.generateHeaders(ctx, flow, beginObserver, true)
}

func (sentinel *Sentinel) generateHeaders(
	ctx context.Context,
	flow string,
	beginObserver SentinelObserverStarter,
	requireObserver bool,
) (SentinelHeaders, error) {
	requirementsToken, err := sentinel.generator.GenerateRequirementsToken()
	if err != nil {
		return SentinelHeaders{}, newAuthError("sentinel_generation_failed", LifecycleLoginPending, 0, false, true, err.Error(), err)
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
		return SentinelHeaders{}, networkAuthError("sentinel_network_error", LifecycleLoginPending, err)
	}
	if response.StatusCode != http.StatusOK {
		if response.StatusCode == http.StatusTooManyRequests || response.StatusCode >= http.StatusInternalServerError {
			return SentinelHeaders{}, newAuthError("sentinel_transient_error", LifecycleLoginPending, response.StatusCode, true, false, "sentinel request was not accepted", nil)
		}
		return SentinelHeaders{}, newAuthError("sentinel_rejected", LifecycleInteractionRequired, response.StatusCode, false, true, "sentinel interaction is required", nil)
	}

	var challenge map[string]any
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.UseNumber()
	if err := decoder.Decode(&challenge); err != nil {
		return SentinelHeaders{}, newAuthError("sentinel_response_invalid", LifecycleLoginPending, response.StatusCode, true, false, "sentinel returned invalid JSON", err)
	}
	if interaction := sentinelUnsupportedInteraction(challenge); interaction != "" {
		return SentinelHeaders{}, newAuthError(interaction, LifecycleInteractionRequired, response.StatusCode, false, true, "interactive challenge is required", nil)
	}
	challengeToken := stringValue(challenge["token"])
	if challengeToken == "" {
		return SentinelHeaders{}, newAuthError("sentinel_token_missing", LifecycleLoginPending, response.StatusCode, true, false, "sentinel response did not include a challenge token", nil)
	}
	proofOfWork, _ := challenge["proofofwork"].(map[string]any)
	proofRequired := boolValue(proofOfWork["required"])
	proofSeed := stringValue(proofOfWork["seed"])
	if proofRequired && proofSeed == "" {
		return SentinelHeaders{}, newAuthError("sentinel_pow_invalid", LifecycleLoginPending, response.StatusCode, true, false, "sentinel proof-of-work seed is missing", nil)
	}
	environment := sentinel.environment()
	var observer SentinelObserverHandle
	if requireObserver && sentinelObserverChallengeRequired(challenge) {
		if !sentinelObserverRequired(challenge) {
			return SentinelHeaders{}, sentinelObserverAuthError(nil)
		}
		if beginObserver == nil {
			return SentinelHeaders{}, sentinelObserverAuthError(nil)
		}
		observer, err = beginObserver(ctx, SentinelSDKRequest{
			BaseURL:           sentinel.baseURL,
			SDKURL:            sentinelSDKURL,
			ScriptSources:     []string{sentinelSDKURL},
			ExpectedSHA256:    sentinelSDKSHA256,
			IntegrityRequired: true,
			TransportKey:      sentinel.transportKey(),
			Challenge:         challenge,
			RequirementsToken: requirementsToken,
			Environment:       environment,
			DeviceID:          sentinel.deviceID,
			Flow:              flow,
			Fetcher:           sentinel.sdkFetcher(),
		})
		if err != nil {
			return SentinelHeaders{}, sentinelObserverAuthError(err)
		}
		if observer == nil {
			return SentinelHeaders{}, sentinelObserverAuthError(nil)
		}
		defer observer.Close()
	}
	turnstileToken, err := sentinel.solveTurnstile(ctx, challenge, requirementsToken, response.StatusCode, environment)
	if err != nil {
		return SentinelHeaders{}, err
	}

	proof := ""
	if proofRequired {
		proof, err = sentinel.generator.GenerateProofContext(ctx, proofSeed, stringValue(proofOfWork["difficulty"]))
	} else {
		proof, err = sentinel.generator.GenerateRequirementsToken()
	}
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return SentinelHeaders{}, ctxErr
		}
		return SentinelHeaders{}, newAuthError("sentinel_generation_failed", LifecycleLoginPending, response.StatusCode, false, true, err.Error(), err)
	}
	headerValue, err := json.Marshal(map[string]any{
		"p":    proof,
		"t":    turnstileToken,
		"c":    challengeToken,
		"id":   sentinel.deviceID,
		"flow": flow,
	})
	if err != nil {
		return SentinelHeaders{}, newAuthError("sentinel_generation_failed", LifecycleLoginPending, response.StatusCode, false, true, "encode sentinel token", err)
	}
	if sentinel.authURL != "" {
		if err := sentinel.client.SetCookie(sentinel.authURL, "oai-sc", "0"+challengeToken); err != nil {
			return SentinelHeaders{}, newAuthError("sentinel_cookie_failed", LifecycleLoginPending, 0, false, true, "persist sentinel cookie", err)
		}
	}
	result := SentinelHeaders{Token: string(headerValue)}
	if observer != nil {
		result.SOToken, err = observer.Snapshot(ctx)
		if err != nil || strings.TrimSpace(result.SOToken) == "" {
			return SentinelHeaders{}, sentinelObserverAuthError(err)
		}
	}
	return result, nil
}

func (sentinel *Sentinel) environment() ConversationTurnstileEnvironment {
	return ConversationTurnstileEnvironment{
		Persona:            sentinel.generator.persona,
		BrowserEnvironment: sentinel.generator.browserEnvironment,
		DeviceID:           sentinel.deviceID,
		PageStartedAt:      sentinel.generator.now(),
		ScriptSources:      []string{sentinelSDKURL},
		Location:           sentinel.baseURL + "/backend-api/sentinel/frame.html?sv=" + sentinelSDKVersion,
	}
}

func (sentinel *Sentinel) transportKey() string {
	persona, _ := json.Marshal(sentinel.client.Persona())
	digest := sha256.Sum256([]byte(sentinel.client.ProxyURL() + "\x00" + string(persona)))
	return fmt.Sprintf("%x", digest[:])
}

func (sentinel *Sentinel) sdkFetcher() SentinelSDKFetcher {
	return func(ctx context.Context, targetURL string, maxBytes int64) ([]byte, string, string, error) {
		if maxBytes < 1 {
			return nil, "", "", fmt.Errorf("Sentinel SDK response limit is invalid")
		}
		client, errClient := NewAcquisitionClient(
			sentinel.client.Persona(), sentinel.client.ProxyURL(), nil, sentinel.client.acquisitionTimeout,
		)
		if errClient != nil {
			return nil, "", "", fmt.Errorf("create cookie-free Sentinel SDK client: %w", errClient)
		}
		defer client.CloseIdleConnections()
		defer client.CloseActiveAcquisitionConnections()
		response, errRequest := client.DoSameOriginRedirectStream(ctx, http.MethodGet, targetURL, map[string]string{
			"accept":         "text/javascript,application/javascript;q=0.9,*/*;q=0.1",
			"referer":        sentinel.baseURL + "/",
			"sec-fetch-dest": "script",
			"sec-fetch-mode": "no-cors",
			"sec-fetch-site": "same-origin",
		}, 3)
		if errRequest != nil {
			return nil, "", "", errRequest
		}
		defer func() { _ = response.Body.Close() }()
		if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
			return nil, "", "", fmt.Errorf("Sentinel SDK request returned HTTP %d", response.StatusCode)
		}
		payload, errRead := io.ReadAll(io.LimitReader(response.Body, maxBytes+1))
		if errRead != nil {
			return nil, "", "", fmt.Errorf("read Sentinel SDK response: %w", errRead)
		}
		if int64(len(payload)) > maxBytes {
			return nil, "", "", fmt.Errorf("Sentinel SDK response exceeds limit")
		}
		finalURL := targetURL
		if response.Request != nil && response.Request.URL != nil {
			finalURL = response.Request.URL.String()
		}
		return payload, response.Header.Get("Content-Type"), finalURL, nil
	}
}

func sentinelObserverChallengeRequired(challenge map[string]any) bool {
	observer, _ := challenge["so"].(map[string]any)
	return boolValue(observer["required"])
}

func sentinelObserverAuthError(err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return newAuthError(
		"sentinel_session_observer_unavailable",
		LifecycleLoginPending,
		0,
		true,
		false,
		"Sentinel Session Observer is unavailable",
		err,
	)
}

func (sentinel *Sentinel) solveTurnstile(
	ctx context.Context,
	challenge map[string]any,
	requirementsToken string,
	status int,
	environment ConversationTurnstileEnvironment,
) (string, error) {
	required, dx := sentinelTurnstileChallenge(challenge["turnstile"])
	if !required {
		return "", nil
	}
	if dx == "" {
		return "", newAuthError("turnstile_required", LifecycleInteractionRequired, status, false, true, "interactive challenge is required", nil)
	}
	solver := sentinel.turnstileSolver
	if solver == nil {
		solver = BuildConversationTurnstileTokenWithEnvironment
	}
	token, err := solver(
		ctx,
		dx,
		requirementsToken,
		environment,
		sentinel.generator.random,
		sentinel.generator.now,
	)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return "", ctxErr
		}
		return "", newAuthError("turnstile_required", LifecycleInteractionRequired, status, false, true, "sentinel Turnstile challenge could not be solved", err)
	}
	return token, nil
}

func sentinelTurnstileChallenge(value any) (bool, string) {
	switch typed := value.(type) {
	case bool:
		return typed, ""
	case map[string]any:
		required := len(typed) > 0
		if configured, exists := typed["required"]; exists {
			required = boolValue(configured)
		}
		return required, stringValue(typed["dx"])
	case string:
		return strings.TrimSpace(typed) != "", ""
	default:
		return false, ""
	}
}

func sentinelUnsupportedInteraction(challenge map[string]any) string {
	for _, candidate := range []struct {
		key  string
		code string
	}{
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
