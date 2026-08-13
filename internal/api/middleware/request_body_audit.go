package middleware

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

// RequestBodyAuditConfigProvider returns the current request body audit configuration.
type RequestBodyAuditConfigProvider func() config.RequestBodyAuditConfig

// RequestBodyAuditRuntimeSnapshot contains process-local matching counters.
type RequestBodyAuditRuntimeSnapshot struct {
	ScannedBytes  uint64 `json:"scanned_bytes"`
	MatchNanos    uint64 `json:"match_nanos"`
	MatchedBodies uint64 `json:"matched_bodies"`
}

var requestBodyAuditCounters struct {
	scannedBytes  atomic.Uint64
	matchNanos    atomic.Uint64
	matchedBodies atomic.Uint64
}

// RequestBodyAuditSnapshot returns low-overhead process-local matching counters.
func RequestBodyAuditSnapshot() RequestBodyAuditRuntimeSnapshot {
	return RequestBodyAuditRuntimeSnapshot{
		ScannedBytes:  requestBodyAuditCounters.scannedBytes.Load(),
		MatchNanos:    requestBodyAuditCounters.matchNanos.Load(),
		MatchedBodies: requestBodyAuditCounters.matchedBodies.Load(),
	}
}

// RequestBodyAuditMiddleware blocks model API requests whose raw body contains configured byte keywords.
func RequestBodyAuditMiddleware(provider RequestBodyAuditConfigProvider) gin.HandlerFunc {
	runtime := &requestBodyAuditRuntime{}
	return func(c *gin.Context) {
		if c == nil || c.Request == nil || !shouldAuditRequestBody(c.Request) {
			c.Next()
			return
		}
		if provider == nil {
			c.Next()
			return
		}

		policy := runtime.resolve(provider())
		cfg := policy.config
		if !cfg.Enable {
			c.Next()
			return
		}
		if policy.matcher == nil {
			c.Next()
			return
		}
		if c.Request.Body == nil || c.Request.Body == http.NoBody {
			c.Next()
			return
		}

		body, tooLarge, errRead := readAuditRequestBody(c.Request, cfg.MaxBodyBytes)
		if errRead != nil {
			writeRequestBodyAuditError(c, cfg.Error)
			return
		}
		if tooLarge && cfg.RejectOversize {
			writeRequestBodyAuditError(c, cfg.Error)
			return
		}

		restoreAuditRequestBody(c.Request, body, tooLarge)
		startedAt := time.Now()
		matched := policy.matcher.match(body)
		requestBodyAuditCounters.scannedBytes.Add(uint64(len(body)))
		requestBodyAuditCounters.matchNanos.Add(uint64(time.Since(startedAt)))
		if matched {
			requestBodyAuditCounters.matchedBodies.Add(1)
			writeRequestBodyAuditError(c, cfg.Error)
			return
		}

		c.Next()
	}
}

type requestBodyAuditPolicy struct {
	source  config.RequestBodyAuditConfig
	config  config.RequestBodyAuditConfig
	matcher *requestBodyAuditMatcher
}

type requestBodyAuditRuntime struct {
	mu     sync.Mutex
	policy atomic.Pointer[requestBodyAuditPolicy]
}

func (runtime *requestBodyAuditRuntime) resolve(source config.RequestBodyAuditConfig) *requestBodyAuditPolicy {
	if current := runtime.policy.Load(); current != nil && requestBodyAuditConfigEqual(current.source, source) {
		return current
	}
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if current := runtime.policy.Load(); current != nil && requestBodyAuditConfigEqual(current.source, source) {
		return current
	}
	normalized := config.NormalizeRequestBodyAudit(source)
	keywords := config.CompiledRequestBodyAuditKeywords(normalized)
	policy := &requestBodyAuditPolicy{
		source: requestBodyAuditConfigClone(source),
		config: normalized,
	}
	if len(keywords) > 0 {
		policy.matcher = newRequestBodyAuditMatcher(keywords, normalized.CaseSensitive)
	}
	runtime.policy.Store(policy)
	return policy
}

func requestBodyAuditConfigClone(source config.RequestBodyAuditConfig) config.RequestBodyAuditConfig {
	clone := source
	clone.Keywords = append([]string(nil), source.Keywords...)
	clone.KeywordsBase64 = append([]string(nil), source.KeywordsBase64...)
	return clone
}

func requestBodyAuditConfigEqual(left, right config.RequestBodyAuditConfig) bool {
	if left.Enable != right.Enable || left.CaseSensitive != right.CaseSensitive ||
		left.MaxBodyBytes != right.MaxBodyBytes || left.RejectOversize != right.RejectOversize ||
		left.Error != right.Error || len(left.Keywords) != len(right.Keywords) ||
		len(left.KeywordsBase64) != len(right.KeywordsBase64) {
		return false
	}
	for index := range left.Keywords {
		if left.Keywords[index] != right.Keywords[index] {
			return false
		}
	}
	for index := range left.KeywordsBase64 {
		if left.KeywordsBase64[index] != right.KeywordsBase64[index] {
			return false
		}
	}
	return true
}

func shouldAuditRequestBody(req *http.Request) bool {
	if req == nil || req.URL == nil {
		return false
	}
	switch req.Method {
	case http.MethodGet, http.MethodHead, http.MethodOptions:
		return false
	}
	path := req.URL.Path
	return strings.HasPrefix(path, "/v1/") ||
		strings.HasPrefix(path, "/v1beta/")
}

func readAuditRequestBody(req *http.Request, maxBodyBytes int64) ([]byte, bool, error) {
	if req == nil || req.Body == nil {
		return nil, false, nil
	}
	if maxBodyBytes <= 0 {
		body, err := io.ReadAll(req.Body)
		return body, false, err
	}
	body, err := io.ReadAll(io.LimitReader(req.Body, maxBodyBytes+1))
	if err != nil {
		return nil, false, err
	}
	return body, int64(len(body)) > maxBodyBytes, nil
}

func restoreAuditRequestBody(req *http.Request, body []byte, hasRemainder bool) {
	if req == nil {
		return
	}
	if hasRemainder && req.Body != nil {
		req.Body = io.NopCloser(io.MultiReader(bytes.NewReader(body), req.Body))
		return
	}
	req.Body = io.NopCloser(bytes.NewReader(body))
}

func requestBodyAuditMatched(body []byte, keywords [][]byte, caseSensitive bool) bool {
	return newRequestBodyAuditMatcher(keywords, caseSensitive).match(body)
}

func asciiBytes(value []byte) bool {
	for _, b := range value {
		if b >= 0x80 {
			return false
		}
	}
	return true
}

func asciiLower(value byte) byte {
	if value >= 'A' && value <= 'Z' {
		return value + ('a' - 'A')
	}
	return value
}

type requestBodyAuditMatcher struct {
	caseSensitive bool
	ascii         requestBodyAuditAutomaton
	nonASCII      [][]byte
}

func newRequestBodyAuditMatcher(keywords [][]byte, caseSensitive bool) *requestBodyAuditMatcher {
	matcher := &requestBodyAuditMatcher{caseSensitive: caseSensitive}
	asciiKeywords := make([][]byte, 0, len(keywords))
	for _, keyword := range keywords {
		if len(keyword) == 0 {
			continue
		}
		cloned := append([]byte(nil), keyword...)
		if asciiBytes(cloned) {
			if !caseSensitive {
				for index := range cloned {
					cloned[index] = asciiLower(cloned[index])
				}
			}
			asciiKeywords = append(asciiKeywords, cloned)
			continue
		}
		if !caseSensitive {
			cloned = bytes.ToLower(cloned)
		}
		matcher.nonASCII = append(matcher.nonASCII, cloned)
	}
	matcher.ascii = newRequestBodyAuditAutomaton(asciiKeywords)
	return matcher
}

func (matcher *requestBodyAuditMatcher) match(body []byte) bool {
	if matcher == nil || len(body) == 0 {
		return false
	}
	if matcher.ascii.match(body, matcher.caseSensitive) {
		return true
	}
	if len(matcher.nonASCII) == 0 {
		return false
	}
	if matcher.caseSensitive {
		for _, keyword := range matcher.nonASCII {
			if bytes.Contains(body, keyword) {
				return true
			}
		}
		return false
	}
	foldedBody := bytes.ToLower(body)
	for _, keyword := range matcher.nonASCII {
		if bytes.Contains(foldedBody, keyword) {
			return true
		}
	}
	return false
}

type requestBodyAuditAutomatonNode struct {
	next   [128]int
	fail   int
	output bool
}

type requestBodyAuditAutomaton struct {
	nodes []requestBodyAuditAutomatonNode
}

func newRequestBodyAuditAutomaton(keywords [][]byte) requestBodyAuditAutomaton {
	if len(keywords) == 0 {
		return requestBodyAuditAutomaton{}
	}
	automaton := requestBodyAuditAutomaton{nodes: make([]requestBodyAuditAutomatonNode, 1)}
	for _, keyword := range keywords {
		state := 0
		for _, value := range keyword {
			next := automaton.nodes[state].next[value]
			if next == 0 {
				automaton.nodes = append(automaton.nodes, requestBodyAuditAutomatonNode{})
				next = len(automaton.nodes) - 1
				automaton.nodes[state].next[value] = next
			}
			state = next
		}
		automaton.nodes[state].output = true
	}
	queue := make([]int, 0, len(automaton.nodes))
	for value := range 128 {
		if next := automaton.nodes[0].next[value]; next != 0 {
			queue = append(queue, next)
		}
	}
	for head := 0; head < len(queue); head++ {
		state := queue[head]
		for value := range 128 {
			next := automaton.nodes[state].next[value]
			if next == 0 {
				continue
			}
			failure := automaton.nodes[state].fail
			for failure != 0 && automaton.nodes[failure].next[value] == 0 {
				failure = automaton.nodes[failure].fail
			}
			if candidate := automaton.nodes[failure].next[value]; candidate != 0 && candidate != next {
				failure = candidate
			}
			automaton.nodes[next].fail = failure
			automaton.nodes[next].output = automaton.nodes[next].output || automaton.nodes[failure].output
			queue = append(queue, next)
		}
	}
	return automaton
}

func (automaton requestBodyAuditAutomaton) match(body []byte, caseSensitive bool) bool {
	if len(automaton.nodes) == 0 {
		return false
	}
	state := 0
	for index := 0; index < len(body); {
		var value byte
		if body[index] < utf8.RuneSelf {
			value = body[index]
			index++
			if !caseSensitive {
				value = asciiLower(value)
			}
		} else if caseSensitive {
			state = 0
			index++
			continue
		} else {
			r, size := utf8.DecodeRune(body[index:])
			index += size
			r = unicode.ToLower(r)
			if r >= utf8.RuneSelf {
				state = 0
				continue
			}
			value = byte(r)
		}
		for state != 0 && automaton.nodes[state].next[value] == 0 {
			state = automaton.nodes[state].fail
		}
		if next := automaton.nodes[state].next[value]; next != 0 {
			state = next
		}
		if automaton.nodes[state].output {
			return true
		}
	}
	return false
}

func writeRequestBodyAuditError(c *gin.Context, errCfg config.RequestBodyAuditErrorConfig) {
	errCfg = config.NormalizeRequestBodyAuditError(errCfg)
	body, err := json.Marshal(struct {
		Error struct {
			Message string `json:"message"`
			Type    string `json:"type"`
			Code    string `json:"code,omitempty"`
		} `json:"error"`
	}{Error: struct {
		Message string `json:"message"`
		Type    string `json:"type"`
		Code    string `json:"code,omitempty"`
	}{
		Message: errCfg.Message,
		Type:    errCfg.Type,
		Code:    errCfg.Code,
	}})
	if err != nil {
		body = []byte(`{"error":{"message":"Request body was rejected by policy.","type":"invalid_request_error","code":"request_body_blocked"}}`)
	}
	c.Data(errCfg.StatusCode, "application/json; charset=utf-8", body)
	c.Abort()
}
