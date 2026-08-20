package codex

import (
	"regexp"
	"strconv"
	"strings"
)

const (
	DefaultUserAgent                  = "codex_cli_rs/0.146.0 (Mac OS 26.5.0; arm64) iTerm.app/3.6.10"
	DefaultOriginator                 = "codex_cli_rs"
	MinimumCompatibleUserAgentVersion = "0.144.0"
)

var codexSoftwareIdentityPattern = regexp.MustCompile(`^([A-Za-z0-9][A-Za-z0-9._-]*)/([0-9]+\.[0-9]+\.[0-9]+(?:[-+][A-Za-z0-9.-]+)?)((?:\s.*)?)$`)

// SoftwareIdentity is the normalized Codex client identity sent upstream.
type SoftwareIdentity struct {
	UserAgent  string
	Originator string
	Version    string
}

type codexSoftwareVersion struct {
	major      int
	minor      int
	patch      int
	preRelease bool
}

// ResolveSoftwareIdentity validates a Codex User-Agent and upgrades only its
// version segment when it is older than the supported baseline.
func ResolveSoftwareIdentity(candidate string) SoftwareIdentity {
	candidate = strings.TrimSpace(candidate)
	if candidate == "" {
		candidate = DefaultUserAgent
	}
	parsed, ok := parseCodexSoftwareIdentity(candidate)
	if !ok {
		parsed, _ = parseCodexSoftwareIdentity(DefaultUserAgent)
		return parsed
	}
	minimum, _ := parseCodexSoftwareVersion(MinimumCompatibleUserAgentVersion)
	current, _ := parseCodexSoftwareVersion(parsed.Version)
	if current.compare(minimum) >= 0 {
		return parsed
	}
	builtin, _ := parseCodexSoftwareIdentity(DefaultUserAgent)
	parsed.Version = builtin.Version
	parsed.UserAgent = parsed.Originator + "/" + parsed.Version + codexSoftwareIdentitySuffix(candidate)
	return parsed
}

func parseCodexSoftwareIdentity(userAgent string) (SoftwareIdentity, bool) {
	match := codexSoftwareIdentityPattern.FindStringSubmatch(strings.TrimSpace(userAgent))
	if len(match) != 4 {
		return SoftwareIdentity{}, false
	}
	if _, ok := parseCodexSoftwareVersion(match[2]); !ok {
		return SoftwareIdentity{}, false
	}
	return SoftwareIdentity{
		UserAgent:  strings.TrimSpace(userAgent),
		Originator: match[1],
		Version:    match[2],
	}, true
}

func codexSoftwareIdentitySuffix(userAgent string) string {
	match := codexSoftwareIdentityPattern.FindStringSubmatch(strings.TrimSpace(userAgent))
	if len(match) != 4 {
		return ""
	}
	return match[3]
}

func parseCodexSoftwareVersion(value string) (codexSoftwareVersion, bool) {
	core := strings.TrimSpace(value)
	preRelease := false
	if index := strings.IndexAny(core, "-+"); index >= 0 {
		preRelease = core[index] == '-'
		core = core[:index]
	}
	parts := strings.Split(core, ".")
	if len(parts) != 3 {
		return codexSoftwareVersion{}, false
	}
	values := [3]int{}
	for index, part := range parts {
		parsed, err := strconv.Atoi(part)
		if err != nil || parsed < 0 {
			return codexSoftwareVersion{}, false
		}
		values[index] = parsed
	}
	return codexSoftwareVersion{major: values[0], minor: values[1], patch: values[2], preRelease: preRelease}, true
}

func (version codexSoftwareVersion) compare(other codexSoftwareVersion) int {
	values := [][2]int{
		{version.major, other.major},
		{version.minor, other.minor},
		{version.patch, other.patch},
	}
	for _, pair := range values {
		if pair[0] < pair[1] {
			return -1
		}
		if pair[0] > pair[1] {
			return 1
		}
	}
	if version.preRelease == other.preRelease {
		return 0
	}
	if version.preRelease {
		return -1
	}
	return 1
}
