package util

import (
	"bytes"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"sync"
	"unicode/utf16"

	"github.com/tidwall/gjson"
)

const PromptCacheLogMarker = "[REDACTED_CACHE_KEY]"
const promptCacheLogPatternLimit = 8 * 1024

// PromptCacheLogRedactor is immutable and applies only to diagnostic copies.
// Short keys match complete JSON strings, avoiding replacement of every letter
// in JSON syntax, model names or binary image data. Header/scalar values are
// checked separately. Extremely large keys hide diagnostic content instead of
// building an unbounded matcher; the request's accepted key is never limited.
type PromptCacheLogRedactor struct {
	once       sync.Once
	key        string
	matcher    *regexp.Regexp
	maxPattern int
	hide       bool
}

func PromptCacheLogRedactorForRequest(payload []byte) *PromptCacheLogRedactor {
	key := gjson.GetBytes(payload, "prompt_cache_key")
	if key.Type != gjson.String || key.Str == "" {
		return nil
	}
	return NewPromptCacheLogRedactor(key.Str)
}

func NewPromptCacheLogRedactor(key string) *PromptCacheLogRedactor {
	if key == "" {
		return nil
	}
	redactor := &PromptCacheLogRedactor{}
	if len(key) > promptCacheLogPatternLimit {
		redactor.hide = true
		return redactor
	}
	redactor.key = strings.Clone(key)
	return redactor
}

func (redactor *PromptCacheLogRedactor) prepare() {
	if redactor != nil {
		redactor.once.Do(redactor.compile)
	}
}

func (redactor *PromptCacheLogRedactor) compile() {
	if redactor.hide {
		return
	}
	key := redactor.key
	patterns := make(map[string]struct{})
	add := func(pattern string) {
		patterns[pattern] = struct{}{}
		redactor.maxPattern = max(redactor.maxPattern, len(pattern))
	}
	quoted, _ := json.Marshal(key)
	value := string(quoted)
	for range 3 {
		add(value)
		next, _ := json.Marshal(value)
		value = string(next[1 : len(next)-1])
	}
	if len(key) >= 8 {
		value = key
		for range 3 {
			add(value)
			next, _ := json.Marshal(value)
			value = string(next[1 : len(next)-1])
		}
	}
	if redactor.maxPattern > promptCacheLogPatternLimit {
		redactor.hide = true
		return
	}
	ordered := make([]string, 0, len(patterns))
	for pattern := range patterns {
		ordered = append(ordered, regexp.QuoteMeta(pattern))
	}
	for depth := range 3 {
		pattern, bound := promptCacheEscapedKeyPattern(key, depth)
		redactor.maxPattern = max(redactor.maxPattern, bound)
		if redactor.maxPattern > promptCacheLogPatternLimit {
			redactor.hide = true
			return
		}
		ordered = append(ordered, pattern)
	}
	sort.Strings(ordered)
	matcher, err := regexp.Compile(strings.Join(ordered, "|"))
	if err != nil {
		redactor.hide = true
		return
	}
	matcher.Longest()
	redactor.matcher = matcher
}

func (r *PromptCacheLogRedactor) ProtectsKey(key string) bool {
	return r != nil && r.key != "" && r.key == key
}

// Accept mixed literal and Unicode-escaped characters without decoding or
// retaining an entire JSON string (which may hold a large image or prompt).
func promptCacheEscapedKeyPattern(key string, depth int) (string, int) {
	escapeTo := func(text string, levels int) string {
		for range levels {
			text = strings.NewReplacer(`\`, `\\`, `"`, `\"`).Replace(text)
		}
		return text
	}
	escape := func(text string) string { return escapeTo(text, depth) }
	quote := escape(`"`)
	quotePattern := regexp.QuoteMeta(quote)
	quoteBound := len(quote)
	if depth > 0 {
		parts := []string{quotePattern}
		for level := 0; level < depth; level++ {
			encoded := escapeTo(`\u0022`, level)
			parts = append(parts, "(?i:"+regexp.QuoteMeta(encoded)+")")
			quoteBound = max(quoteBound, len(encoded))
		}
		quotePattern = "(?:" + strings.Join(parts, "|") + ")"
	}
	var pattern strings.Builder
	pattern.WriteString(quotePattern)
	bound := 2 * quoteBound
	for _, value := range key {
		canonical, _ := json.Marshal(string(value))
		variants := []string{string(canonical[1 : len(canonical)-1])}
		if value >= 0x20 && value != '"' && value != '\\' {
			variants = append(variants, string(value))
		}
		if value == '/' {
			variants = append(variants, `\/`)
		}
		unicodeEscape := fmt.Sprintf(`\u%04x`, value)
		if value > 0xffff {
			hi, lo := utf16.EncodeRune(value)
			unicodeEscape = fmt.Sprintf(`\u%04x\u%04x`, hi, lo)
		}
		maxLength := len(escape(unicodeEscape))
		pattern.WriteString("(?:")
		for index, variant := range variants {
			if index > 0 {
				pattern.WriteByte('|')
			}
			encoded := escape(variant)
			maxLength = max(maxLength, len(encoded))
			pattern.WriteString(regexp.QuoteMeta(encoded))
		}
		// Case insensitivity is limited to hexadecimal escapes, not literal keys.
		for level := 0; level <= depth; level++ {
			pattern.WriteString("|(?i:")
			pattern.WriteString(regexp.QuoteMeta(escapeTo(unicodeEscape, level)))
			pattern.WriteByte(')')
		}
		pattern.WriteByte(')')
		bound += maxLength
	}
	pattern.WriteString(quotePattern)
	return pattern.String(), bound
}

func (r *PromptCacheLogRedactor) Redact(text string) string {
	if r == nil || text == "" {
		return text
	}
	r.prepare()
	if r.hide || text == r.key {
		return PromptCacheLogMarker
	}
	return r.matcher.ReplaceAllStringFunc(text, func(match string) string {
		return r.replacement(match)
	})
}

func (r *PromptCacheLogRedactor) replacement(match string) string {
	// Retain surrounding JSON quotes, including mirrors encoded as JSON text.
	quotes := [...]string{`\\u0022`, `\u0022`, `\\\"`, `\"`, `"`}
	for _, open := range quotes {
		if len(match) < len(open) || !strings.EqualFold(match[:len(open)], open) {
			continue
		}
		for _, close := range quotes {
			if len(match) >= len(open)+len(close) && strings.EqualFold(match[len(match)-len(close):], close) {
				return match[:len(open)] + PromptCacheLogMarker + match[len(match)-len(close):]
			}
		}
	}
	return PromptCacheLogMarker
}

func (r *PromptCacheLogRedactor) Headers(headers map[string][]string) map[string][]string {
	if r == nil {
		return headers
	}
	out := make(map[string][]string, len(headers))
	for name, values := range headers {
		out[name] = make([]string, len(values))
		for i, value := range values {
			out[name][i] = r.Redact(value)
		}
	}
	return out
}

// PromptCacheLogStream handles matches spanning arbitrary logging chunks while
// retaining at most one bounded pattern's trailing bytes between writes.
type PromptCacheLogStream struct {
	redactor *PromptCacheLogRedactor
	pending  []byte
	hidden   bool
	emitted  bool
}

func (r *PromptCacheLogRedactor) Stream() *PromptCacheLogStream {
	r.prepare()
	return &PromptCacheLogStream{redactor: r}
}

func (s *PromptCacheLogStream) Write(chunk []byte, final bool) []byte {
	r := s.redactor
	if r == nil {
		return chunk
	}
	if r.hide {
		if s.hidden || len(chunk) == 0 {
			return nil
		}
		s.hidden = true
		return []byte(PromptCacheLogMarker)
	}
	s.pending = append(s.pending, chunk...)
	if final && !s.emitted && string(s.pending) == r.key {
		s.pending = nil
		s.emitted = true
		return []byte(PromptCacheLogMarker)
	}
	cut := len(s.pending)
	if !final {
		cut = max(0, cut-r.maxPattern+1)
	}
	if cut == 0 {
		return nil
	}
	var out bytes.Buffer
	consumed := 0
	for _, span := range r.matcher.FindAllIndex(s.pending, -1) {
		if span[0] >= cut {
			break
		}
		out.Write(s.pending[consumed:span[0]])
		out.WriteString(r.replacement(string(s.pending[span[0]:span[1]])))
		consumed = span[1]
	}
	// A match may extend past cut; consume it as a whole and retain the remainder.
	end := max(consumed, cut)
	out.Write(s.pending[consumed:end])
	consumed = end
	if cap(s.pending) > 64*1024 {
		// Do not retain a large upstream chunk merely for the small boundary tail.
		s.pending = bytes.Clone(s.pending[consumed:])
	} else {
		copy(s.pending, s.pending[consumed:])
		s.pending = s.pending[:len(s.pending)-consumed]
	}
	s.emitted = s.emitted || out.Len() > 0
	return out.Bytes()
}
