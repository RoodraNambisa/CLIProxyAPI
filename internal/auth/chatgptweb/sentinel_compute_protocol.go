package chatgptweb

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strings"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/sentinelcompat"
)

const SentinelComputeProtocol = 1
const sentinelComputeMaxBody = 32 << 20

// SentinelComputeEnvironment is a data-only snapshot; no transport or credentials cross RPC.
type SentinelComputeEnvironment struct {
	Persona            Persona                    `json:"persona"`
	BrowserEnvironment BrowserEnvironmentIdentity `json:"browser_environment"`
	DeviceID           string                     `json:"device_id"`
	PageStartedAt      time.Time                  `json:"page_started_at"`
	ScriptSources      []string                   `json:"script_sources"`
	Location           string                     `json:"location"`
	LocalStorageKeys   []string                   `json:"local_storage_keys,omitempty"`
}

func ComputeEnvironment(env ConversationTurnstileEnvironment) SentinelComputeEnvironment {
	return SentinelComputeEnvironment{env.Persona, env.BrowserEnvironment, env.DeviceID, env.PageStartedAt, append([]string{}, env.ScriptSources...), env.Location, append([]string{}, env.LocalStorageKeys...)}
}

func (env SentinelComputeEnvironment) native(policy *sentinelcompat.Policy) ConversationTurnstileEnvironment {
	return ConversationTurnstileEnvironment{Persona: env.Persona, BrowserEnvironment: env.BrowserEnvironment, DeviceID: env.DeviceID, PageStartedAt: env.PageStartedAt, ScriptSources: env.ScriptSources, Location: env.Location, LocalStorageKeys: env.LocalStorageKeys, Compatibility: policy}
}

type SentinelComputeInput struct {
	Format               string                     `json:"format"`
	Environment          SentinelComputeEnvironment `json:"environment"`
	DataBuild            string                     `json:"data_build,omitempty"`
	GeneratorSID         string                     `json:"generator_sid,omitempty"`
	Flow                 string                     `json:"flow"`
	Clock                time.Time                  `json:"clock"`
	SDKURL               string                     `json:"sdk_url"`
	SDKSHA256            string                     `json:"sdk_sha256"`
	SDKIntegrityRequired bool                       `json:"sdk_integrity_required"`
}

func (in SentinelComputeInput) normalized() (SentinelComputeInput, error) {
	if in.SDKURL == "" {
		resource := DefaultConversationSentinelSDKResource()
		in.SDKURL = resource.URL
		in.SDKSHA256 = resource.SHA256
		in.SDKIntegrityRequired = true
	}
	u, err := url.Parse(in.SDKURL)
	if err != nil {
		return in, computeError("invalid_input", false, err)
	}
	if !u.IsAbs() {
		base, _ := url.Parse("https://chatgpt.com/")
		u = base.ResolveReference(u)
	}
	if validateSentinelSDKURL(u) != nil {
		return in, computeError("invalid_input", false, nil)
	}
	in.SDKURL = u.String()
	hashes, err := normalizeSentinelHashes(in.SDKSHA256)
	if err != nil {
		return in, computeError("invalid_input", false, err)
	}
	in.SDKSHA256 = strings.Join(hashes, " ")
	return in, nil
}

func (in SentinelComputeInput) validate() error {
	if in.Format != "conversation" && in.Format != "auth" {
		return computeError("invalid_input", false, nil)
	}
	if len(in.Environment.DeviceID) == 0 || len(in.Environment.DeviceID) > 256 || len(in.GeneratorSID) > 256 || len(in.Flow) > 128 || len(in.DataBuild) > 4096 || in.Clock.IsZero() {
		return computeError("invalid_input", false, nil)
	}
	entry, ok := personaCatalogEntryForPersona(in.Environment.Persona)
	if !ok || entry.persona != in.Environment.Persona {
		return computeError("unsupported_profile", true, nil)
	}
	if _, ok = browserEnvironmentSlot(in.Environment.Persona, in.Environment.BrowserEnvironment); !ok {
		return computeError("unsupported_profile", true, nil)
	}
	if len(in.Environment.ScriptSources) > 256 || len(in.Environment.LocalStorageKeys) > 256 || len(in.Environment.Location) > 4096 {
		return computeError("invalid_input", false, nil)
	}
	for _, values := range [][]string{in.Environment.ScriptSources, in.Environment.LocalStorageKeys} {
		for _, v := range values {
			if len(v) > 4096 {
				return computeError("invalid_input", false, nil)
			}
		}
	}
	if in.SDKURL != "" {
		u, err := url.Parse(in.SDKURL)
		if err != nil || validateSentinelSDKURL(u) != nil {
			return computeError("invalid_input", false, nil)
		}
	}
	if _, err := normalizeSentinelHashes(in.SDKSHA256); err != nil {
		return computeError("invalid_input", false, nil)
	}
	return nil
}

type SentinelComputeChallenge struct {
	Token             string `json:"token,omitempty"`
	ProofRequired     bool   `json:"proof_required"`
	ProofSeed         string `json:"proof_seed,omitempty"`
	ProofDifficulty   string `json:"proof_difficulty,omitempty"`
	TurnstileRequired bool   `json:"turnstile_required"`
	TurnstileDX       string `json:"turnstile_dx,omitempty"`
	ObserverRequired  bool   `json:"observer_required"`
	CollectorDX       string `json:"collector_dx,omitempty"`
	SnapshotDX        string `json:"snapshot_dx,omitempty"`
}

// ComputeChallenge whitelists protocol inputs instead of forwarding upstream response bodies.
func ComputeChallenge(raw map[string]any, observer bool) SentinelComputeChallenge {
	pow, _ := raw["proofofwork"].(map[string]any)
	so, _ := raw["so"].(map[string]any)
	tsRequired, dx := sentinelTurnstileChallenge(raw["turnstile"])
	return SentinelComputeChallenge{stringValue(raw["token"]), boolValue(pow["required"]), stringValue(pow["seed"]), stringValue(pow["difficulty"]), tsRequired, dx, observer && boolValue(so["required"]), stringValue(so["collector_dx"]), stringValue(so["snapshot_dx"])}
}

func (c SentinelComputeChallenge) native() map[string]any {
	return map[string]any{"token": c.Token, "proofofwork": map[string]any{"required": c.ProofRequired, "seed": c.ProofSeed, "difficulty": c.ProofDifficulty}, "turnstile": map[string]any{"required": c.TurnstileRequired, "dx": c.TurnstileDX}, "so": map[string]any{"required": c.ObserverRequired, "collector_dx": c.CollectorDX, "snapshot_dx": c.SnapshotDX}}
}

func (c SentinelComputeChallenge) validate() error {
	if len(c.Token) > 1<<20 || len(c.ProofSeed) > 4096 || len(c.ProofDifficulty) > 128 {
		return computeError("invalid_input", false, nil)
	}
	if c.ProofRequired && (c.ProofSeed == "" || c.ProofDifficulty == "") || c.TurnstileRequired && c.TurnstileDX == "" || c.ObserverRequired && (c.CollectorDX == "" || c.SnapshotDX == "") {
		return computeError("invalid_input", false, nil)
	}
	for _, dx := range []string{c.TurnstileDX, c.CollectorDX, c.SnapshotDX} {
		if len(dx) > ((conversationTurnstileMaxBytes+2)/3)*4 {
			return computeError("invalid_input", false, nil)
		}
	}
	return nil
}

type SentinelComputeResult struct {
	RequirementsToken string `json:"requirements_token,omitempty"`
	ProofToken        string `json:"proof_token,omitempty"`
	TurnstileToken    string `json:"turnstile_token,omitempty"`
	SnapshotToken     string `json:"snapshot_token,omitempty"`
	Engine            string `json:"engine"`
}

type sentinelComputeRequest struct {
	Version      int                       `json:"version"`
	SessionID    string                    `json:"session_id"`
	OperationID  string                    `json:"operation_id"`
	BudgetMillis int64                     `json:"budget_ms"`
	Input        *SentinelComputeInput     `json:"input,omitempty"`
	Challenge    *SentinelComputeChallenge `json:"challenge,omitempty"`
}

type sentinelComputeResponse struct {
	Version   int                   `json:"version"`
	Instance  string                `json:"instance"`
	SessionID string                `json:"session_id,omitempty"`
	Result    SentinelComputeResult `json:"result"`
	Rules     sentinelcompat.Config `json:"rules"`
	RulesHash string                `json:"rules_hash"`
	SDKURL    string                `json:"sdk_url,omitempty"`
	SDKSHA256 string                `json:"sdk_sha256,omitempty"`
	Error     *SentinelComputeError `json:"error,omitempty"`
}

// SentinelComputeError is infrastructure-local, never an upstream credential status.
type SentinelComputeError struct {
	Code      string `json:"code"`
	Retryable bool   `json:"retryable"`
	Err       error  `json:"-"`
}

func (e *SentinelComputeError) Error() string        { return "Sentinel computation failed: " + e.Code }
func (e *SentinelComputeError) Unwrap() error        { return e.Err }
func (e *SentinelComputeError) SkipAuthResult() bool { return true }
func (e *SentinelComputeError) RetryOtherAuth() bool { return false }
func (e *SentinelComputeError) StatusCode() int      { return 503 }
func computeError(code string, retryable bool, err error) *SentinelComputeError {
	return &SentinelComputeError{code, retryable, err}
}
func classifyComputeError(err error) *SentinelComputeError {
	var known *SentinelComputeError
	if errors.As(err, &known) {
		return known
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return computeError("budget_exhausted", true, err)
	}
	var compat *SentinelCompatibilityError
	if errors.As(err, &compat) {
		return computeError("compatibility", true, err)
	}
	var runtime *SentinelRuntimeError
	if errors.As(err, &runtime) {
		return computeError("sdk_unavailable", true, err)
	}
	var fatal conversationTurnstileFatalError
	if errors.As(err, &fatal) {
		return computeError("safety_limit", false, err)
	}
	return computeError("invalid_input", false, err)
}

// SentinelComputeHooks stay local and are not part of the portable wire contract.
type SentinelComputeHooks struct {
	Reader  io.Reader
	Now     func() time.Time
	Fetcher SentinelSDKFetcher
}

func (in SentinelComputeInput) String() string {
	return fmt.Sprintf("SentinelComputeInput(format=%s)", in.Format)
}
