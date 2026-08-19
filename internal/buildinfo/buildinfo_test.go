package buildinfo

import "testing"

func TestResolveUsesExplicitReleaseMetadata(t *testing.T) {
	version, commit, buildDate := resolve(
		"v7.2.136-fork.1",
		"abcdef123456",
		"2026-08-19T09:00:00Z",
		vcsMetadata{revision: "ffffffffffffffff", modified: true},
	)

	if version != "v7.2.136-fork.1" {
		t.Fatalf("version = %q", version)
	}
	if commit != "abcdef123456" {
		t.Fatalf("commit = %q", commit)
	}
	if buildDate != "2026-08-19T09:00:00Z" {
		t.Fatalf("build date = %q", buildDate)
	}
}

func TestResolveUsesEmbeddedVCSMetadataForSourceBuild(t *testing.T) {
	version, commit, buildDate := resolve(
		"dev",
		"none",
		"unknown",
		vcsMetadata{revision: "abcdef1234567890", modified: false},
	)

	if version != "dev-abcdef123456" {
		t.Fatalf("version = %q", version)
	}
	if commit != "abcdef123456" {
		t.Fatalf("commit = %q", commit)
	}
	if buildDate != "unknown" {
		t.Fatalf("build date = %q", buildDate)
	}
}

func TestResolveMarksModifiedSourceBuild(t *testing.T) {
	version, commit, _ := resolve(
		"dev",
		"none",
		"unknown",
		vcsMetadata{revision: "abcdef1234567890", modified: true},
	)

	if version != "dev-abcdef123456-dirty" {
		t.Fatalf("version = %q", version)
	}
	if commit != "abcdef123456" {
		t.Fatalf("commit = %q", commit)
	}
}

func TestResolveKeepsDevelopmentPlaceholdersWithoutVCSMetadata(t *testing.T) {
	version, commit, buildDate := resolve("dev", "none", "unknown", vcsMetadata{})

	if version != "dev" || commit != "none" || buildDate != "unknown" {
		t.Fatalf("resolved metadata = %q, %q, %q", version, commit, buildDate)
	}
}
