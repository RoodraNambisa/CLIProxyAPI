// Package buildinfo exposes compile-time metadata shared across the server.
package buildinfo

import (
	"runtime/debug"
	"strings"
)

// The following variables are overridden via ldflags during release builds.
// Defaults cover local development builds.
var (
	// Version is the semantic version or git describe output of the binary.
	Version = "dev"

	// Commit is the git commit SHA baked into the binary.
	Commit = "none"

	// BuildDate records when the binary was built in UTC.
	BuildDate = "unknown"
)

type vcsMetadata struct {
	revision string
	modified bool
}

// Configure applies release metadata and falls back to Go's embedded VCS
// revision for source builds that were not given explicit linker flags.
func Configure(version, commit, buildDate string) {
	resolvedVersion, resolvedCommit, resolvedBuildDate := resolve(
		version,
		commit,
		buildDate,
		readVCSMetadata(),
	)
	Version = resolvedVersion
	Commit = resolvedCommit
	BuildDate = resolvedBuildDate
}

func readVCSMetadata() vcsMetadata {
	info, ok := debug.ReadBuildInfo()
	if !ok || info == nil {
		return vcsMetadata{}
	}
	var metadata vcsMetadata
	for _, setting := range info.Settings {
		switch setting.Key {
		case "vcs.revision":
			metadata.revision = strings.TrimSpace(setting.Value)
		case "vcs.modified":
			metadata.modified = strings.EqualFold(strings.TrimSpace(setting.Value), "true")
		}
	}
	return metadata
}

func resolve(version, commit, buildDate string, vcs vcsMetadata) (string, string, string) {
	version = strings.TrimSpace(version)
	commit = strings.TrimSpace(commit)
	buildDate = strings.TrimSpace(buildDate)

	if placeholderCommit(commit) && vcs.revision != "" {
		commit = shortRevision(vcs.revision)
	}
	if placeholderVersion(version) && !placeholderCommit(commit) {
		version = "dev-" + commit
		if vcs.modified {
			version += "-dirty"
		}
	}

	if version == "" {
		version = "dev"
	}
	if commit == "" {
		commit = "none"
	}
	if buildDate == "" {
		buildDate = "unknown"
	}
	return version, commit, buildDate
}

func placeholderVersion(version string) bool {
	return version == "" || strings.EqualFold(version, "dev")
}

func placeholderCommit(commit string) bool {
	return commit == "" || strings.EqualFold(commit, "none") || strings.EqualFold(commit, "unknown")
}

func shortRevision(revision string) string {
	revision = strings.TrimSpace(revision)
	const shortLength = 12
	if len(revision) <= shortLength {
		return revision
	}
	return revision[:shortLength]
}
