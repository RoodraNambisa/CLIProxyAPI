package proxyutil

import (
	"crypto/rand"
	"fmt"
	"io"
	"net"
	"net/url"
	"sort"
	"strconv"
	"strings"
)

const DefaultPlaceholderCharset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

type portRange struct {
	start int
	end   int
}

type zeroReader struct{}

func (zeroReader) Read(buffer []byte) (int, error) {
	clear(buffer)
	return len(buffer), nil
}

// PortSet stores normalized, non-overlapping port ranges without expanding them.
type PortSet struct {
	ranges []portRange
	count  int
}

// ParsePortSet parses comma-separated ports and inclusive ranges.
func ParsePortSet(raw string) (PortSet, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return PortSet{}, nil
	}

	ranges := make([]portRange, 0)
	for _, part := range strings.Split(trimmed, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			return PortSet{}, fmt.Errorf("empty port item")
		}
		pieces := strings.Split(part, "-")
		if len(pieces) > 2 {
			return PortSet{}, fmt.Errorf("invalid port range %q", part)
		}
		start, errStart := parsePort(pieces[0])
		if errStart != nil {
			return PortSet{}, fmt.Errorf("invalid port item %q: %w", part, errStart)
		}
		end := start
		if len(pieces) == 2 {
			var errEnd error
			end, errEnd = parsePort(pieces[1])
			if errEnd != nil {
				return PortSet{}, fmt.Errorf("invalid port item %q: %w", part, errEnd)
			}
			if end < start {
				return PortSet{}, fmt.Errorf("invalid descending port range %q", part)
			}
		}
		ranges = append(ranges, portRange{start: start, end: end})
	}

	sort.Slice(ranges, func(i, j int) bool {
		if ranges[i].start == ranges[j].start {
			return ranges[i].end < ranges[j].end
		}
		return ranges[i].start < ranges[j].start
	})
	merged := make([]portRange, 0, len(ranges))
	for _, candidate := range ranges {
		if len(merged) == 0 || candidate.start > merged[len(merged)-1].end+1 {
			merged = append(merged, candidate)
			continue
		}
		if candidate.end > merged[len(merged)-1].end {
			merged[len(merged)-1].end = candidate.end
		}
	}
	count := 0
	for _, item := range merged {
		count += item.end - item.start + 1
	}
	return PortSet{ranges: merged, count: count}, nil
}

func parsePort(raw string) (int, error) {
	value, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil || value < 1 || value > 65535 {
		return 0, fmt.Errorf("port must be between 1 and 65535")
	}
	return value, nil
}

// Count returns the number of represented ports.
func (s PortSet) Count() int { return s.count }

// PortAt resolves a zero-based ordinal without expanding the ranges.
func (s PortSet) PortAt(index int) (int, bool) {
	if index < 0 || index >= s.count {
		return 0, false
	}
	for _, item := range s.ranges {
		width := item.end - item.start + 1
		if index < width {
			return item.start + index, true
		}
		index -= width
	}
	return 0, false
}

// String returns the canonical range expression.
func (s PortSet) String() string {
	parts := make([]string, 0, len(s.ranges))
	for _, item := range s.ranges {
		if item.start == item.end {
			parts = append(parts, strconv.Itoa(item.start))
			continue
		}
		parts = append(parts, fmt.Sprintf("%d-%d", item.start, item.end))
	}
	return strings.Join(parts, ",")
}

// NormalizePlaceholderCharset validates a URL-safe placeholder character set.
func NormalizePlaceholderCharset(raw string) (string, error) {
	charset := strings.TrimSpace(raw)
	if charset == "" {
		charset = DefaultPlaceholderCharset
	}
	seen := make(map[byte]struct{}, len(charset))
	normalized := make([]byte, 0, len(charset))
	for i := 0; i < len(charset); i++ {
		ch := charset[i]
		if !isURLUnreserved(ch) {
			return "", fmt.Errorf("placeholder charset contains unsupported character %q", ch)
		}
		if _, ok := seen[ch]; ok {
			continue
		}
		seen[ch] = struct{}{}
		normalized = append(normalized, ch)
	}
	if len(seen) == 0 {
		return "", fmt.Errorf("placeholder charset cannot be empty")
	}
	return string(normalized), nil
}

func isURLUnreserved(ch byte) bool {
	return ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' || ch >= '0' && ch <= '9' || strings.ContainsRune("-._~", rune(ch))
}

// ExpandURLTemplate replaces each {N} placeholder independently.
func ExpandURLTemplate(template, charset string, source io.Reader) (string, []string, error) {
	charset, errCharset := NormalizePlaceholderCharset(charset)
	if errCharset != nil {
		return "", nil, errCharset
	}
	if source == nil {
		source = rand.Reader
	}

	var output strings.Builder
	values := make([]string, 0)
	for index := 0; index < len(template); {
		if template[index] != '{' {
			if template[index] == '}' {
				return "", nil, fmt.Errorf("unmatched placeholder delimiter")
			}
			output.WriteByte(template[index])
			index++
			continue
		}
		endOffset := strings.IndexByte(template[index+1:], '}')
		if endOffset < 0 {
			return "", nil, fmt.Errorf("unmatched placeholder delimiter")
		}
		end := index + 1 + endOffset
		length, errLength := strconv.Atoi(template[index+1 : end])
		if errLength != nil || length < 1 || length > 128 {
			return "", nil, fmt.Errorf("placeholder length must be between 1 and 128")
		}
		value, errRandom := randomPlaceholderValue(length, charset, source)
		if errRandom != nil {
			return "", nil, fmt.Errorf("read placeholder randomness: %w", errRandom)
		}
		values = append(values, value)
		output.WriteString(value)
		index = end + 1
	}
	return output.String(), values, nil
}

func randomPlaceholderValue(length int, charset string, source io.Reader) (string, error) {
	value := make([]byte, length)
	limit := 256 - 256%len(charset)
	buffer := []byte{0}
	for index := range value {
		for {
			if _, errRead := io.ReadFull(source, buffer); errRead != nil {
				return "", errRead
			}
			if int(buffer[0]) >= limit {
				continue
			}
			value[index] = charset[int(buffer[0])%len(charset)]
			break
		}
	}
	return string(value), nil
}

// ValidateURLTemplate verifies that a template expands to a supported proxy URL.
func ValidateURLTemplate(template, ports, charset string) (string, string, error) {
	template = strings.TrimSpace(template)
	if template == "" {
		return "", "", fmt.Errorf("url-template is required")
	}
	if errLocation := validatePlaceholderLocations(template); errLocation != nil {
		return "", "", errLocation
	}
	portSet, errPorts := ParsePortSet(ports)
	if errPorts != nil {
		return "", "", errPorts
	}
	expanded, _, errExpand := ExpandURLTemplate(template, charset, zeroReader{})
	if errExpand != nil {
		return "", "", errExpand
	}
	setting, errParse := Parse(expanded)
	if errParse != nil {
		return "", "", fmt.Errorf("invalid proxy URL template")
	}
	if setting.Mode != ModeProxy || setting.URL == nil || setting.URL.Hostname() == "" {
		return "", "", fmt.Errorf("url-template must contain a concrete proxy URL")
	}
	if setting.URL.Path != "" || setting.URL.RawQuery != "" || setting.URL.Fragment != "" {
		return "", "", fmt.Errorf("url-template must contain only a proxy authority")
	}
	if inlinePort := setting.URL.Port(); inlinePort != "" {
		if _, errPort := parsePort(inlinePort); errPort != nil {
			return "", "", fmt.Errorf("url-template contains an invalid port")
		}
	}
	if portSet.Count() == 0 && setting.URL.Port() == "" {
		return "", "", fmt.Errorf("url-template requires a port when ports is empty")
	}
	return template, portSet.String(), nil
}

func validatePlaceholderLocations(template string) error {
	schemeEnd := strings.Index(template, "://")
	if schemeEnd < 0 {
		return nil
	}
	authorityStart := schemeEnd + 3
	authorityEnd := len(template)
	if offset := strings.IndexAny(template[authorityStart:], "/?#"); offset >= 0 {
		authorityEnd = authorityStart + offset
	}
	authority := template[authorityStart:authorityEnd]
	at := strings.LastIndexByte(authority, '@')
	userinfoEnd := authorityStart + at
	for index := 0; index < len(template); index++ {
		if template[index] != '{' {
			continue
		}
		if at < 0 || index < authorityStart || index >= userinfoEnd {
			return fmt.Errorf("placeholders are only supported in proxy credentials")
		}
		endOffset := strings.IndexByte(template[index+1:], '}')
		if endOffset < 0 {
			return nil
		}
		index += endOffset + 1
	}
	return nil
}

// WithPort replaces the URL port while preserving user info and host names.
func WithPort(raw string, port int) (string, error) {
	if port < 1 || port > 65535 {
		return "", fmt.Errorf("port must be between 1 and 65535")
	}
	parsed, errParse := url.Parse(strings.TrimSpace(raw))
	if errParse != nil || parsed.Scheme == "" || parsed.Hostname() == "" {
		return "", fmt.Errorf("invalid proxy URL")
	}
	if strings.Count(parsed.Host, ":") > 1 && !strings.HasPrefix(parsed.Host, "[") {
		return "", fmt.Errorf("IPv6 proxy host must be enclosed in brackets")
	}
	parsed.Host = net.JoinHostPort(parsed.Hostname(), strconv.Itoa(port))
	return parsed.String(), nil
}

// MaskProxyURL hides proxy passwords for management responses and logs.
func MaskProxyURL(raw string) string {
	trimmed := strings.TrimSpace(raw)
	if hasAmbiguousProxyAuthority(trimmed) {
		return maskRawProxyPassword(trimmed)
	}
	parsed, errParse := url.Parse(trimmed)
	if errParse != nil || parsed.Scheme == "" || parsed.Host == "" {
		return maskRawProxyPassword(trimmed)
	}
	if parsed.User == nil {
		return trimmed
	}
	if _, hasPassword := parsed.User.Password(); !hasPassword {
		return trimmed
	}
	username := parsed.User.Username()
	clone := *parsed
	clone.User = nil
	prefix := clone.Scheme + "://"
	rest := strings.TrimPrefix(clone.String(), prefix)
	return prefix + url.User(username).String() + ":********@" + rest
}

func hasAmbiguousProxyAuthority(raw string) bool {
	schemeEnd := strings.Index(raw, "://")
	if schemeEnd < 0 {
		return false
	}
	authorityStart := schemeEnd + 3
	authorityEnd := len(raw)
	if offset := strings.IndexAny(raw[authorityStart:], "/?#"); offset >= 0 {
		authorityEnd = authorityStart + offset
	}
	authority := raw[authorityStart:authorityEnd]
	if strings.Contains(authority, "@") || strings.Count(authority, ":") < 2 {
		return false
	}
	if strings.HasPrefix(authority, "[") {
		closeBracket := strings.IndexByte(authority, ']')
		if closeBracket > 1 && net.ParseIP(authority[1:closeBracket]) != nil {
			return false
		}
	}
	return true
}

func maskRawProxyPassword(raw string) string {
	if at := strings.LastIndexByte(raw, '@'); at >= 0 {
		userinfoStart := 0
		if schemeEnd := strings.Index(raw[:at], "://"); schemeEnd >= 0 {
			userinfoStart = schemeEnd + 3
		} else if delimiter := strings.LastIndexAny(raw[:at], "/?#"); delimiter >= 0 {
			userinfoStart = delimiter + 1
		}
		if colonOffset := strings.IndexByte(raw[userinfoStart:at], ':'); colonOffset >= 0 {
			colon := userinfoStart + colonOffset
			return raw[:colon+1] + "********" + raw[at:]
		}
	}

	schemeEnd := strings.Index(raw, "://")
	authorityStart := 0
	if schemeEnd >= 0 {
		authorityStart = schemeEnd + 3
	}
	authorityEnd := len(raw)
	if offset := strings.IndexAny(raw[authorityStart:], "/?#"); offset >= 0 {
		authorityEnd = authorityStart + offset
	}
	authority := raw[authorityStart:authorityEnd]
	firstColon := strings.IndexByte(authority, ':')
	if firstColon < 0 {
		return raw
	}
	lastColon := strings.LastIndexByte(authority, ':')
	if firstColon == lastColon {
		if port, errPort := strconv.Atoi(authority[lastColon+1:]); errPort == nil && port >= 1 && port <= 65535 {
			return raw
		}
		maskedAuthority := authority[:firstColon] + ":********"
		return raw[:authorityStart] + maskedAuthority + raw[authorityEnd:]
	}
	if _, errPort := strconv.Atoi(authority[lastColon+1:]); errPort == nil {
		maskedAuthority := authority[:firstColon] + ":********" + authority[lastColon:]
		return raw[:authorityStart] + maskedAuthority + raw[authorityEnd:]
	}
	maskedAuthority := authority[:firstColon] + ":********"
	return raw[:authorityStart] + maskedAuthority + raw[authorityEnd:]
}
