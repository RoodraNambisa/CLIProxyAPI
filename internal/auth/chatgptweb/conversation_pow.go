package chatgptweb

import (
	"bytes"
	"context"
	"crypto/sha3"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"time"

	"golang.org/x/net/html"
)

const (
	defaultConversationPoWScript      = "https://chatgpt.com/backend-api/sentinel/sdk.js"
	defaultConversationPoWMaxAttempts = 500_000
	conversationPoWDigestHexLength    = 128
)

var (
	chatGPTWebScriptSourcePattern = regexp.MustCompile(`(?i)<script[^>]+src=["']([^"']+)["']`)
	chatGPTWebDataBuildPattern    = regexp.MustCompile(`(?i)<html[^>]*data-build=["']([^"']*)["']`)
	chatGPTWebScriptBuildPattern  = regexp.MustCompile(`c/[^/]*/_`)
	conversationPoWSlots          = make(chan struct{}, conversationPoWConcurrency())
)

// ParseConversationPoWResources extracts the current script sources and build
// marker from the ChatGPT bootstrap document.
func ParseConversationPoWResources(document []byte) ([]string, string) {
	matches := chatGPTWebScriptSourcePattern.FindAllSubmatch(document, -1)
	sources := make([]string, 0, len(matches))
	dataBuild := ""
	for _, match := range matches {
		if len(match) < 2 {
			continue
		}
		source := strings.TrimSpace(string(match[1]))
		if source == "" {
			continue
		}
		sources = append(sources, source)
		if dataBuild == "" {
			dataBuild = chatGPTWebScriptBuildPattern.FindString(source)
		}
	}
	if len(sources) == 0 {
		sources = []string{defaultConversationPoWScript}
	}
	if dataBuild == "" {
		if match := chatGPTWebDataBuildPattern.FindSubmatch(document); len(match) > 1 {
			dataBuild = strings.TrimSpace(string(match[1]))
		}
	}
	return sources, dataBuild
}

// ConversationSentinelSDKResource identifies the versioned SDK requested by
// the bootstrap document and its optional SHA-256 subresource integrity value.
type ConversationSentinelSDKResource struct {
	URL               string
	SHA256            string
	IntegrityRequired bool
}

// ParseConversationSentinelSDKResource finds the exact Sentinel SDK script
// selected by the bootstrap document without evaluating document JavaScript.
func ParseConversationSentinelSDKResource(document []byte) ConversationSentinelSDKResource {
	tokenizer := html.NewTokenizer(bytes.NewReader(document))
	for {
		tokenType := tokenizer.Next()
		if tokenType == html.ErrorToken {
			return ConversationSentinelSDKResource{}
		}
		if tokenType != html.StartTagToken && tokenType != html.SelfClosingTagToken {
			continue
		}
		token := tokenizer.Token()
		if !strings.EqualFold(token.Data, "script") {
			continue
		}
		var source, integrity string
		for _, attribute := range token.Attr {
			switch strings.ToLower(attribute.Key) {
			case "src":
				source = strings.TrimSpace(attribute.Val)
			case "integrity":
				integrity = strings.TrimSpace(attribute.Val)
			}
		}
		parsed, err := url.Parse(source)
		if source == "" || err != nil || parsed == nil || parsed.ForceQuery || parsed.RawQuery != "" || parsed.Fragment != "" || parsed.RawFragment != "" {
			continue
		}
		candidate := parsed
		if !candidate.IsAbs() {
			base, _ := url.Parse("https://chatgpt.com/")
			candidate = base.ResolveReference(candidate)
		}
		if validateSentinelSDKURL(candidate) != nil {
			continue
		}
		resource := ConversationSentinelSDKResource{URL: source, IntegrityRequired: integrity != ""}
		seenHashes := make(map[string]struct{})
		hashes := make([]string, 0, 2)
		for _, value := range strings.Fields(integrity) {
			if strings.HasPrefix(strings.ToLower(value), "sha256-") {
				key := "sha256-" + value[len("sha256-"):]
				if _, exists := seenHashes[key]; exists {
					continue
				}
				seenHashes[key] = struct{}{}
				hashes = append(hashes, value)
			}
		}
		resource.SHA256 = strings.Join(hashes, " ")
		return resource
	}
}

func conversationSentinelSDKVersionFromPath(path string) (version string, backendAPI bool) {
	if len(path) < 2 || path[0] != '/' || path[len(path)-1] == '/' || strings.Contains(path[1:], "//") {
		return "", false
	}
	parts := strings.Split(path[1:], "/")
	var escapedVersion string
	switch {
	case len(parts) == 3 && parts[0] == "sentinel" && parts[2] == "sdk.js":
		escapedVersion = parts[1]
	case len(parts) == 4 && parts[0] == "backend-api" && parts[1] == "sentinel" && parts[3] == "sdk.js":
		escapedVersion = parts[2]
		backendAPI = true
	default:
		return "", false
	}
	version, err := url.PathUnescape(escapedVersion)
	if err != nil || len(version) < 8 {
		return "", false
	}
	for index, char := range version {
		if index < 8 {
			if char < '0' || char > '9' {
				return "", false
			}
			continue
		}
		if (char < '0' || char > '9') && (char < 'a' || char > 'z') && (char < 'A' || char > 'Z') && char != '-' && char != '_' && char != '.' {
			return "", false
		}
	}
	return version, backendAPI
}

// BuildConversationRequirementsToken creates the browser proof payload used by
// the chat-requirements prepare endpoint.
func BuildConversationRequirementsToken(persona Persona, scriptSources []string, dataBuild string, reader io.Reader, now func() time.Time) (string, error) {
	config, err := buildConversationPoWConfig(persona, scriptSources, dataBuild, reader, now)
	if err != nil {
		return "", err
	}
	payload, err := compactJSON(config)
	if err != nil {
		return "", err
	}
	return "gAAAAAC" + base64.StdEncoding.EncodeToString(payload), nil
}

// BuildConversationProofToken solves the SHA3-512 browser proof challenge.
func BuildConversationProofToken(ctx context.Context, seed, difficulty string, persona Persona, scriptSources []string, dataBuild string, reader io.Reader, now func() time.Time) (string, error) {
	return buildConversationProofToken(ctx, seed, difficulty, persona, scriptSources, dataBuild, reader, now, time.Now)
}

func buildConversationProofToken(ctx context.Context, seed, difficulty string, persona Persona, scriptSources []string, dataBuild string, reader io.Reader, now, monotonicNow func() time.Time) (string, error) {
	seed = strings.TrimSpace(seed)
	difficulty = strings.ToLower(strings.TrimSpace(difficulty))
	if seed == "" || difficulty == "" {
		return "", fmt.Errorf("invalid conversation proof challenge")
	}
	if len(difficulty) > conversationPoWDigestHexLength {
		return "", fmt.Errorf("conversation proof difficulty exceeds SHA3-512 digest length")
	}
	if _, err := hex.DecodeString(difficulty + strings.Repeat("0", len(difficulty)%2)); err != nil {
		return "", fmt.Errorf("decode conversation proof difficulty: %w", err)
	}
	config, err := buildConversationPoWConfig(persona, scriptSources, dataBuild, reader, now)
	if err != nil {
		return "", err
	}
	static1, err := compactJSON(config[:3])
	if err != nil {
		return "", err
	}
	static2, err := compactJSON(config[4:9])
	if err != nil {
		return "", err
	}
	static3, err := compactJSON(config[10:])
	if err != nil {
		return "", err
	}
	static1 = append(bytes.TrimSuffix(static1, []byte{']'}), ',')
	static2 = bytes.TrimPrefix(bytes.TrimSuffix(static2, []byte{']'}), []byte{'['})
	static3 = bytes.TrimPrefix(static3, []byte{'['})
	if ctx == nil {
		ctx = context.Background()
	}
	if monotonicNow == nil {
		monotonicNow = time.Now
	}
	select {
	case conversationPoWSlots <- struct{}{}:
		defer func() { <-conversationPoWSlots }()
	case <-ctx.Done():
		return "", ctx.Err()
	}
	startedAt := monotonicNow()
	targetHex := []byte(difficulty)
	seedBytes := []byte(seed)
	raw := make([]byte, 0, len(static1)+len(static2)+len(static3)+48)
	encoded := make([]byte, 0, base64.StdEncoding.EncodedLen(cap(raw)))
	hasher := sha3.New512()
	digest := make([]byte, 0, hasher.Size())
	digestHex := make([]byte, hasher.Size()*2)
	for attempt := 0; attempt < defaultConversationPoWMaxAttempts; attempt++ {
		if attempt&1023 == 0 {
			select {
			case <-ctx.Done():
				return "", ctx.Err()
			default:
			}
		}
		raw = raw[:0]
		raw = append(raw, static1...)
		raw = strconv.AppendInt(raw, int64(attempt), 10)
		raw = append(raw, ',')
		raw = append(raw, static2...)
		raw = append(raw, ',')
		elapsed := monotonicNow().Sub(startedAt)
		if elapsed < 0 {
			elapsed = 0
		}
		raw = strconv.AppendInt(raw, elapsed.Round(time.Millisecond).Milliseconds(), 10)
		raw = append(raw, ',')
		raw = append(raw, static3...)
		encodedLength := base64.StdEncoding.EncodedLen(len(raw))
		if cap(encoded) < encodedLength {
			encoded = make([]byte, encodedLength)
		} else {
			encoded = encoded[:encodedLength]
		}
		base64.StdEncoding.Encode(encoded, raw)
		hasher.Reset()
		_, _ = hasher.Write(seedBytes)
		_, _ = hasher.Write(encoded)
		digest = hasher.Sum(digest[:0])
		hex.Encode(digestHex, digest)
		if bytes.Compare(digestHex[:len(targetHex)], targetHex) <= 0 {
			return "gAAAAAB" + string(encoded), nil
		}
	}
	return "", fmt.Errorf("failed to solve conversation proof challenge")
}

func buildConversationPoWConfig(persona Persona, scriptSources []string, dataBuild string, reader io.Reader, now func() time.Time) ([]any, error) {
	persona = normalizePersona(persona)
	reader = randomReader(reader)
	if now == nil {
		now = time.Now
	}
	if len(scriptSources) == 0 {
		scriptSources = []string{defaultConversationPoWScript}
	}
	scriptSource, err := randomChoice(reader, scriptSources)
	if err != nil {
		return nil, err
	}
	navigatorKey, err := randomChoice(reader, []string{
		"vendor−Google Inc.",
		"webdriver−false",
		"cookieEnabled−true",
		"hardwareConcurrency−8",
		"language−en-US",
		"product−Gecko",
	})
	if err != nil {
		return nil, err
	}
	documentKey, err := randomChoice(reader, []string{
		"__reactContainer$fzelfjyxej8",
		"_reactListening5dehydibo78",
		"location",
	})
	if err != nil {
		return nil, err
	}
	windowKey, err := randomChoice(reader, []string{
		"window", "document", "location", "history", "navigator", "performance", "crypto",
	})
	if err != nil {
		return nil, err
	}
	randomValue, err := randomFloat64(reader)
	if err != nil {
		return nil, err
	}
	sessionID, err := GenerateDeviceID(reader)
	if err != nil {
		return nil, err
	}
	current := now()
	eastern := current.In(time.FixedZone("EST", -5*60*60))
	performanceNow := float64(current.UnixNano()%50_000_000) / float64(time.Microsecond)
	timeOrigin := float64(current.UnixNano())/float64(time.Millisecond) - performanceNow
	hardware := persona.HardwareConcurrency
	if hardware <= 0 {
		hardware = 8
	}
	return []any{
		persona.ScreenWidth + persona.ScreenHeight,
		eastern.Format("Mon Jan 02 2006 15:04:05") + " GMT-0500 (Eastern Standard Time)",
		uint64(4_294_705_152),
		1,
		persona.UserAgent,
		scriptSource,
		strings.TrimSpace(dataBuild),
		persona.Language,
		persona.AcceptLanguage,
		randomValue,
		navigatorKey,
		documentKey,
		windowKey,
		performanceNow,
		sessionID,
		"",
		hardware,
		timeOrigin,
		0, 0, 0, 0, 0, 0,
		0,
	}, nil
}

func conversationPoWConcurrency() int {
	concurrency := runtime.GOMAXPROCS(0) / 2
	if concurrency < 1 {
		return 1
	}
	return concurrency
}

func compactJSON(value any) ([]byte, error) {
	payload, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("encode conversation proof payload: %w", err)
	}
	return payload, nil
}

func randomFloat64(reader io.Reader) (float64, error) {
	var value [8]byte
	if _, err := io.ReadFull(randomReader(reader), value[:]); err != nil {
		return 0, fmt.Errorf("read conversation proof random value: %w", err)
	}
	var integer uint64
	for _, item := range value {
		integer = integer<<8 | uint64(item)
	}
	return float64(integer>>11) / float64(uint64(1)<<53), nil
}
