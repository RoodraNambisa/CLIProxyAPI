package helps

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/session"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
	"golang.org/x/net/http/httpguts"
)

const XAIIdentitySeedKey = "xai_identity_seed"

// XAIRequestPlan owns configuration and client identity before translation.
// Only the attempt counter changes; no message history or credential is retained.
type XAIRequestPlan struct {
	Config                                                *config.Config
	Session, Conversation, CacheKey, Agent, Request, Turn string
	Basis, Source, Nonce                                  string
	IncomingRetry                                         uint64
	attempts                                              atomic.Uint64
}

func NewXAIRequestPlan(ctx context.Context, cfg *config.Config, req core.Request, opts core.Options) (*XAIRequestPlan, error) {
	owned := &config.Config{}
	if cfg != nil {
		*owned = *cfg
		owned.XAI = cfg.XAI.Clone()
	}
	p := &XAIRequestPlan{Config: owned}
	if !owned.XAI.IdentityEnabled() {
		return p, nil
	}
	if opts.Headers == nil && ctx != nil {
		if c, ok := ctx.Value("gin").(*gin.Context); ok && c != nil && c.Request != nil {
			opts.Headers = c.Request.Header
		}
	}
	if owned.XAI.PoolSize() < 1 || owned.XAI.PoolSize() > 64 {
		return nil, fmt.Errorf("xai.session-identity-pool-size must be between 1 and 64")
	}
	payload := opts.OriginalRequest
	if len(payload) == 0 {
		payload = req.Payload
	}
	root := gjson.ParseBytes(payload)
	p.Session = firstXAIValue(opts.Headers.Get("X-Grok-Session-Id"), opts.Headers.Get("Session-Id"), opts.Headers.Get("Session_id"), opts.Headers.Get("X-Session-ID"), opts.Headers.Get("X-Claude-Code-Session-Id"), xaiIdentityString(root.Get("session_id")), xaiIdentityString(root.Get("metadata.session_id")))
	p.Conversation = firstXAIValue(opts.Headers.Get("X-Grok-Conv-Id"), xaiIdentityString(root.Get("conversation_id")))
	p.CacheKey = xaiIdentityString(root.Get("prompt_cache_key"))
	explicit, _ := session.ExtractGrokExplicitIdentity(opts.Headers, payload, "")
	if p.Session == "" {
		if explicit.SessionID != "" {
			namespace, raw, _ := strings.Cut(explicit.SessionID, ":")
			switch namespace {
			case "claude", "codex", "session", "header", "agy", "affinity", "slot", "thread", "conv":
				p.Session = raw
			}
		}
	}
	p.Agent = opts.Headers.Get("X-Grok-Agent-Id")
	p.Request = opts.Headers.Get("X-Grok-Req-Id")
	p.Turn = opts.Headers.Get("X-Grok-Turn-Idx")
	p.IncomingRetry, _ = strconv.ParseUint(opts.Headers.Get("X-Grok-Transient-Retry"), 10, 32)
	if p.Turn != "" {
		if _, err := strconv.ParseUint(p.Turn, 10, 32); err != nil {
			return nil, fmt.Errorf("invalid x-grok-turn-idx")
		}
	}
	for _, value := range []string{p.Session, p.Conversation, p.CacheKey, p.Agent, p.Request} {
		if len(value) > 8192 || !httpguts.ValidHeaderFieldValue(value) || strings.TrimSpace(value) != value {
			return nil, fmt.Errorf("invalid Grok identity field")
		}
	}
	useHistory := (owned.XAI.SpoofSessionIdentity || owned.XAI.SessionIdentityConvergence) && (owned.Routing.SessionAffinityUseHistory == nil || *owned.Routing.SessionAffinityUseHistory)
	p.Basis, p.Source = coreauth.ProviderSessionFingerprint(ctx, req, opts, useHistory, explicit)
	if p.Basis != "" {
		switch {
		case opts.Headers.Get("X-Grok-Session-Id") != "":
			p.Source = "grok-session"
		case p.Session != "":
			p.Source = "client-session"
		case p.CacheKey != "":
			p.Source = "prompt-cache-key"
		case p.Conversation != "":
			p.Source = "grok-conversation"
		}
	}
	nonce, err := uuid.NewRandom()
	if err != nil {
		return nil, err
	}
	p.Nonce = nonce.String()
	if p.Basis == "" {
		p.Basis = p.Nonce
		p.Source = "request"
	}
	return p, nil
}

func XAIPlanFromOptions(opts core.Options) *XAIRequestPlan {
	value, _ := core.ProviderPreparedRequest(opts, "xai")
	plan, _ := value.(*XAIRequestPlan)
	return plan
}

func XAIIdentitySeed(auth *coreauth.Auth) []byte {
	if auth == nil || auth.Metadata == nil {
		return nil
	}
	raw, _ := auth.Metadata[XAIIdentitySeedKey].(string)
	seed, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil || len(seed) != 32 {
		return nil
	}
	return seed
}

func (p *XAIRequestPlan) ShouldPrepareRequestAuth(auth *coreauth.Auth) bool {
	return p != nil && p.Config.XAI.NeedsIdentitySeed() && len(XAIIdentitySeed(auth)) == 0
}

func (p *XAIRequestPlan) PrepareRequestAuth(_ context.Context, auth *coreauth.Auth) (*coreauth.Auth, error) {
	if auth == nil {
		return nil, fmt.Errorf("xai credential is nil")
	}
	if !p.ShouldPrepareRequestAuth(auth) {
		return auth, nil
	}
	seed := make([]byte, 32)
	if _, err := rand.Read(seed); err != nil {
		return nil, err
	}
	updated := auth.Clone()
	if updated.Metadata == nil {
		updated.Metadata = make(map[string]any)
	}
	updated.Metadata[XAIIdentitySeedKey] = base64.RawURLEncoding.EncodeToString(seed)
	return updated, nil
}

type XAIIdentityProjection struct {
	Session, Conversation, CacheKey, Agent, Request, Turn string
	Slot                                                  int
	Source                                                string
}

// Project changes the envelope only; transport/stateful response identifiers
// remain bound to the original execution session, never to the converged slot.
func (p *XAIRequestPlan) Project(auth *coreauth.Auth, body []byte, headers http.Header, legacySession string) ([]byte, XAIIdentityProjection, error) {
	result := XAIIdentityProjection{Slot: -1}
	if p == nil || !p.Config.XAI.IdentityEnabled() {
		return body, result, nil
	}
	c := p.Config.XAI
	seed := XAIIdentitySeed(auth)
	if c.NeedsIdentitySeed() && len(seed) == 0 {
		return nil, result, fmt.Errorf("Grok credential identity is not prepared")
	}
	result.Session = firstXAIValue(p.Session, headers.Get("x-grok-session-id"), legacySession)
	result.Conversation = firstXAIValue(p.Conversation, headers.Get("x-grok-conv-id"), legacySession)
	result.CacheKey = firstXAIValue(p.CacheKey, xaiIdentityString(gjson.GetBytes(body, "prompt_cache_key")))
	if c.SpoofSessionIdentity {
		// A composer fallback may have just generated a new ID. Missing fields
		// must instead derive from the captured session, not from that fallback.
		result.Session = firstXAIValue(p.Session, headers.Get("x-grok-session-id"))
		result.Conversation = firstXAIValue(p.Conversation, headers.Get("x-grok-conv-id"))
		if p.CacheKey == "" && result.CacheKey == legacySession {
			result.CacheKey = ""
		}
	}
	result.Agent = firstXAIValue(p.Agent, headers.Get("x-grok-agent-id"))
	result.Request = firstXAIValue(p.Request, headers.Get("x-grok-req-id"))
	result.Turn, result.Source = p.Turn, p.Source
	if c.SessionIdentityConvergence {
		digest := xaiIdentityMAC(seed, "slot", p.Basis)
		if p.Source == "request" {
			// With no session evidence, choose the slot once for the logical
			// request, including retries which switch to another credential.
			random := sha256.Sum256([]byte(p.Nonce))
			digest = random[:]
		}
		result.Slot = int(binary.BigEndian.Uint64(digest[:8]) % uint64(c.PoolSize()))
		id := xaiIdentityUUID(seed, "session-slot", strconv.Itoa(result.Slot))
		result.Session, result.Conversation, result.CacheKey = id, id, id
		result.Agent = xaiIdentityUUID(seed, "agent", "credential")
	}
	if c.SpoofSessionIdentity || c.SessionIdentityConvergence {
		base := firstXAIValue(result.Session, result.CacheKey, result.Conversation)
		if base == "" {
			base = xaiIdentityUUID(seed, "session", p.Basis)
		}
		result.Session = firstXAIValue(result.Session, base)
		result.Conversation = firstXAIValue(result.Conversation, result.Session)
		result.CacheKey = firstXAIValue(result.CacheKey, result.Conversation)
		result.Agent = firstXAIValue(result.Agent, xaiIdentityUUID(seed, "agent", "credential"))
		result.Request = firstXAIValue(result.Request, p.Nonce)
	}
	if c.IdentityConfuse {
		for _, target := range []*string{&result.Session, &result.Conversation, &result.CacheKey} {
			if *target != "" {
				*target = xaiIdentityUUID(seed, "conversation", *target)
			}
		}
		if result.Agent != "" {
			result.Agent = xaiIdentityUUID(seed, "agent-obfuscated", result.Agent)
		}
		if result.Request != "" {
			result.Request = p.Nonce
		}
	}
	if c.PassthroughClientIdentity {
		if p.Session != "" {
			result.Session = p.Session
		}
		if p.Conversation != "" {
			result.Conversation = p.Conversation
		}
		if p.CacheKey != "" {
			result.CacheKey = p.CacheKey
		}
		if p.Session != "" && p.Conversation != "" && p.CacheKey != "" {
			result.Slot = -1
		}
	}
	if result.CacheKey != "" {
		var err error
		body, err = sjson.SetBytes(body, "prompt_cache_key", result.CacheKey)
		if err != nil {
			return nil, result, err
		}
	}
	return body, result, nil
}

func (projection XAIIdentityProjection) ApplyHeaders(headers http.Header) {
	for key, value := range map[string]string{
		"x-grok-session-id": projection.Session, "x-grok-conv-id": projection.Conversation,
		"x-grok-agent-id": projection.Agent, "x-grok-req-id": projection.Request, "x-grok-turn-idx": projection.Turn,
	} {
		if value != "" {
			headers.Set(key, value)
		}
	}
}

func (p *XAIRequestPlan) ApplyAttemptHeader(headers http.Header) uint64 {
	if p == nil || !p.Config.XAI.IdentityEnabled() {
		return 0
	}
	attempt := p.attempts.Add(1) - 1 + p.IncomingRetry
	if attempt > 0 {
		headers.Set("x-grok-transient-retry", strconv.FormatUint(attempt, 10))
	} else {
		headers.Del("x-grok-transient-retry")
	}
	return attempt
}

func XAIIdentityDigest(value string) string {
	if value == "" {
		return ""
	}
	digest := sha256.Sum256([]byte(value))
	return hex.EncodeToString(digest[:8])
}

func xaiIdentityMAC(seed []byte, namespace, value string) []byte {
	mac := hmac.New(sha256.New, seed)
	_, _ = mac.Write([]byte("cliproxy:xai:v1\x00" + namespace + "\x00" + value))
	return mac.Sum(nil)
}

func xaiIdentityUUID(seed []byte, namespace, value string) string {
	digest := xaiIdentityMAC(seed, namespace, value)
	var id uuid.UUID
	copy(id[:], digest[:16])
	id[6] = (id[6] & 0x0f) | 0x50
	id[8] = (id[8] & 0x3f) | 0x80
	return id.String()
}

func firstXAIValue(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func xaiIdentityString(value gjson.Result) string {
	if value.Type == gjson.String {
		return value.Str
	}
	return ""
}
