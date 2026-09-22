package codexstate

import (
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

type cookieBackup struct {
	bundle  *CookieBundle
	version uint64
}

// Route identity ignores auxiliary updates and expiry declarations. Repeated
// routing values do not create independent backups or restart their local age.
func (b *CookieBundle) routeIdentity() string {
	if b == nil {
		return ""
	}
	var values []string
	for _, member := range b.Members {
		if RouteCookieName(member.Cookie.Name) {
			values = append(values, member.key()+"\x00"+member.Cookie.Value)
		}
	}
	sort.Strings(values)
	return cookieDigest(strings.Join(values, "\x00"))
}

func (g *cookieGroup) duplicate(b *CookieBundle, now time.Time) bool {
	id := b.routeIdentity()
	if g.main != nil && g.main.Select(g.main.Origin, now, g.work.policy).Header != "" && g.main.routeIdentity() == id {
		return true
	}
	for _, spare := range g.backups {
		if spare.bundle.routeIdentity() == id {
			return true
		}
	}
	return false
}

func (g *cookieGroup) pruneBackups(now time.Time) {
	g.backups = slices.DeleteFunc(g.backups, func(b cookieBackup) bool {
		return b.bundle.Select(b.bundle.Origin, now, g.work.policy).Header == ""
	})
	slices.SortStableFunc(g.backups, func(a, b cookieBackup) int { return a.bundle.ReceivedAt.Compare(b.bundle.ReceivedAt) })
	// Retain the newest samples when the configured pool shrinks.
	if len(g.backups) > g.backupTarget {
		g.backups = slices.Clone(g.backups[len(g.backups)-g.backupTarget:])
	}
}

func (g *cookieGroup) readyBackups(now time.Time, p config.CodexStateOverrideConfig) int {
	n := 0
	for _, b := range g.backups {
		end := b.bundle.refreshDeadline(p, now)
		if b.bundle.Select(b.bundle.Origin, now, p).Header != "" && (end.IsZero() || now.Before(end)) {
			n++
		}
	}
	return n
}

func (m *Manager) activateCookie(g *cookieGroup, b *CookieBundle) {
	m.nextValueVersion++
	g.main, g.version = b, m.nextValueVersion
	g.work.CurrentUses = 0
	g.work.ExpiresAt = b.expiry(g.work.policy)
}

// Promotion assigns a new version even if a former primary is reused, so an
// old in-flight response cannot invalidate the newly activated selection.
func (m *Manager) promoteCookie(g *cookieGroup, target string, now time.Time, p config.CodexStateOverrideConfig) bool {
	if g.work.paused {
		return false
	}
	g.pruneBackups(now)
	for i, spare := range g.backups {
		if spare.bundle.Select(target, now, p).Header == "" {
			continue
		}
		g.backups = slices.Delete(g.backups, i, i+1)
		old := g.main
		oldVersion := g.version
		m.activateCookie(g, spare.bundle)
		g.promotions++
		if old != nil && old.Select(old.Origin, now, p).Header != "" {
			g.backups = append(g.backups, cookieBackup{old, oldVersion})
		}
		g.pruneBackups(now)
		return true
	}
	return false
}

func (g *cookieGroup) availabilityExpiry(p config.CodexStateOverrideConfig) time.Time {
	var latest time.Time
	include := func(b *CookieBundle) {
		if b == nil {
			return
		}
		end := b.expiry(p)
		if end.IsZero() {
			end = time.Date(9999, 1, 1, 0, 0, 0, 0, time.UTC)
		}
		if end.After(latest) {
			latest = end
		}
	}
	include(g.main)
	for _, b := range g.backups {
		include(b.bundle)
	}
	return latest
}
