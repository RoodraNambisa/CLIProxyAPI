package chatgptweb

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"math"
	"strings"
	"testing"
	"time"
)

func TestParseConversationPoWResources(t *testing.T) {
	sources, build := ParseConversationPoWResources([]byte(`<html data-build="fallback"><script src="/c/abc/_next/static/a.js"></script></html>`))
	if len(sources) != 1 || sources[0] != "/c/abc/_next/static/a.js" {
		t.Fatalf("sources = %v", sources)
	}
	if build != "c/abc/_" {
		t.Fatalf("build = %q", build)
	}
}

func TestBuildConversationRequirementsTokenUsesTwentyFiveItems(t *testing.T) {
	token, err := BuildConversationRequirementsToken(DefaultPersona(), []string{"/sdk.js"}, "build", zeroReader{}, func() time.Time {
		return time.Unix(1_700_000_000, 0)
	})
	if err != nil {
		t.Fatalf("BuildConversationRequirementsToken() error = %v", err)
	}
	if !strings.HasPrefix(token, "gAAAAAC") {
		t.Fatalf("token = %q", token)
	}
	payload, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(token, "gAAAAAC"))
	if err != nil {
		t.Fatalf("decode token: %v", err)
	}
	var config []any
	if err := json.Unmarshal(payload, &config); err != nil {
		t.Fatalf("decode config: %v", err)
	}
	if len(config) != 25 {
		t.Fatalf("config items = %d, want 25", len(config))
	}
}

func TestConversationPoWBuildersAcceptNilRandomReader(t *testing.T) {
	if _, err := BuildConversationRequirementsToken(DefaultPersona(), nil, "", nil, time.Now); err != nil {
		t.Fatalf("BuildConversationRequirementsToken() error = %v", err)
	}
	if _, err := BuildConversationProofToken(context.Background(), "seed", "ff", DefaultPersona(), nil, "", nil, time.Now); err != nil {
		t.Fatalf("BuildConversationProofToken() error = %v", err)
	}
}

func TestBuildConversationProofTokenSolvesEasyChallenge(t *testing.T) {
	token, err := BuildConversationProofToken(context.Background(), "seed", "ff", DefaultPersona(), []string{"/sdk.js"}, "", zeroReader{}, func() time.Time {
		return time.Unix(1_700_000_000, 0)
	})
	if err != nil {
		t.Fatalf("BuildConversationProofToken() error = %v", err)
	}
	if !strings.HasPrefix(token, "gAAAAAB") {
		t.Fatalf("token = %q", token)
	}
	payload, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(token, "gAAAAAB"))
	if err != nil {
		t.Fatalf("decode proof token: %v", err)
	}
	var config []any
	if err := json.Unmarshal(payload, &config); err != nil {
		t.Fatalf("decode proof config: %v; payload=%s", err, payload)
	}
	if len(config) != 25 {
		t.Fatalf("proof config items = %d, want 25", len(config))
	}
}

func TestBuildConversationProofTokenSupportsOddNibbleDifficulty(t *testing.T) {
	token, err := BuildConversationProofToken(context.Background(), "seed", "f", DefaultPersona(), nil, "", zeroReader{}, time.Now)
	if err != nil {
		t.Fatalf("BuildConversationProofToken() error = %v", err)
	}
	if !strings.HasPrefix(token, "gAAAAAB") {
		t.Fatalf("token = %q", token)
	}
}

func TestBuildConversationProofTokenRecordsElapsedSolveTime(t *testing.T) {
	base := time.Unix(1_700_000_000, 0)
	times := []time.Time{base, base.Add(7 * time.Millisecond)}
	index := 0
	monotonicNow := func() time.Time {
		if index >= len(times) {
			return times[len(times)-1]
		}
		current := times[index]
		index++
		return current
	}
	token, err := buildConversationProofToken(
		context.Background(), "seed", "f", DefaultPersona(), []string{"/sdk.js"}, "",
		zeroReader{}, func() time.Time { return base }, monotonicNow,
	)
	if err != nil {
		t.Fatalf("buildConversationProofToken() error = %v", err)
	}
	payload, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(token, "gAAAAAB"))
	if err != nil {
		t.Fatalf("decode proof token: %v", err)
	}
	var config []any
	if err := json.Unmarshal(payload, &config); err != nil {
		t.Fatalf("decode proof config: %v", err)
	}
	if got := config[3]; got != float64(0) {
		t.Fatalf("attempt = %#v, want 0", got)
	}
	if got := config[9]; got != float64(7) {
		t.Fatalf("elapsed milliseconds = %#v, want 7", got)
	}
}

func TestBuildConversationProofTokenRejectsOversizedDifficulty(t *testing.T) {
	_, err := BuildConversationProofToken(context.Background(), "seed", strings.Repeat("ff", 65), DefaultPersona(), nil, "", zeroReader{}, time.Now)
	if err == nil || !strings.Contains(err.Error(), "digest length") {
		t.Fatalf("BuildConversationProofToken() error = %v", err)
	}
}

func TestBuildConversationProofTokenObservesCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := BuildConversationProofToken(ctx, "seed", strings.Repeat("00", 64), DefaultPersona(), nil, "", zeroReader{}, time.Now)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("BuildConversationProofToken() error = %v, want context.Canceled", err)
	}
}

func TestConversationPoWPerformanceClockIsConsistent(t *testing.T) {
	now := time.Unix(1_700_000_000, 42_000_000)
	token, err := BuildConversationRequirementsToken(DefaultPersona(), []string{"/sdk.js"}, "build", zeroReader{}, func() time.Time {
		return now
	})
	if err != nil {
		t.Fatalf("BuildConversationRequirementsToken() error = %v", err)
	}
	payload, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(token, "gAAAAAC"))
	if err != nil {
		t.Fatalf("decode token: %v", err)
	}
	var config []any
	if err := json.Unmarshal(payload, &config); err != nil {
		t.Fatalf("decode config: %v", err)
	}
	performanceNow, _ := config[13].(float64)
	timeOrigin, _ := config[17].(float64)
	currentMilliseconds := float64(now.UnixNano()) / float64(time.Millisecond)
	if delta := math.Abs(timeOrigin + performanceNow - currentMilliseconds); delta > 0.001 {
		t.Fatalf("timeOrigin + performance.now delta = %fms", delta)
	}
}

func BenchmarkBuildConversationProofToken(b *testing.B) {
	for range b.N {
		if _, err := BuildConversationProofToken(context.Background(), "seed", "f", DefaultPersona(), nil, "", zeroReader{}, time.Now); err != nil {
			b.Fatal(err)
		}
	}
}
