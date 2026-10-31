package cache

import (
	"container/list"
	"crypto/sha256"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const codexAliasShards = 64
const CodexIdentityAliasLimit = 100000
const CodexIdentityAliasBytes = 64 << 20
const CodexIdentityAliasMaxValue = 4 << 10
const CodexIdentityAliasTTL = time.Hour

// CodexIdentityAlias stores only protocol identifiers, never credential objects or bodies.
type CodexIdentityAlias struct {
	Original, Mapped, Scope, Generation, Role string
	SessionSlot                               string
	Converged                                 bool
}

type codexAliasEntry struct {
	key   [32]byte
	value CodexIdentityAlias
	used  time.Time
	bytes int
}
type codexAliasShard struct {
	count *atomic.Int64
	sync.Mutex
	entries map[[32]byte]*list.Element
	order   list.List
	bytes   int
}
type CodexIdentityAliases struct {
	count  atomic.Int64
	shards [codexAliasShards]codexAliasShard
	now    func() time.Time
}

func NewCodexIdentityAliases() *CodexIdentityAliases {
	c := &CodexIdentityAliases{now: time.Now}
	for i := range c.shards {
		c.shards[i].count = &c.count
	}
	return c
}

var codexAliases = NewCodexIdentityAliases()
var codexAliasMaintenance sync.Once

func GlobalCodexIdentityAliases() *CodexIdentityAliases { return codexAliases }
func codexAliasKey(role, value string) [32]byte         { return sha256.Sum256([]byte(role + "\x00" + value)) }
func (s *codexAliasShard) remove(e *list.Element) {
	item := e.Value.(*codexAliasEntry)
	delete(s.entries, item.key)
	s.count.Add(-1)
	s.bytes -= item.bytes
	s.order.Remove(e)
	if len(s.entries) == 0 {
		s.entries = nil
	}
}

func (c *CodexIdentityAliases) Put(v CodexIdentityAlias) {
	size := len(v.Original) + len(v.Mapped)
	if v.Mapped == "" || size > CodexIdentityAliasMaxValue || v.Scope == "" {
		return
	}
	if v.Converged {
		v.Original = ""
	}
	key := codexAliasKey(v.Role, v.Mapped)
	index := int(key[0]) % codexAliasShards
	s := &c.shards[index]
	s.Lock()
	defer s.Unlock()
	now := c.now()
	if s.entries == nil {
		s.entries = make(map[[32]byte]*list.Element)
	}
	if e := s.entries[key]; e != nil {
		old := e.Value.(*codexAliasEntry)
		if old.value.Scope == v.Scope && old.value.Generation == v.Generation && (old.value.Converged || old.value.Original != v.Original) {
			v.Converged = true
			v.Original = ""
		}
		if old.value == v {
			old.used = now
			s.order.MoveToFront(e)
			return
		}
		s.remove(e)
	}
	if v.Converged {
		v.Original = ""
	}
	if len(v.Original)+len(v.Mapped)+len(v.Scope)+len(v.Generation)+len(v.Role)+len(v.SessionSlot) > CodexIdentityAliasBytes/codexAliasShards {
		return
	}
	v.Original = strings.Clone(v.Original)
	v.Mapped = strings.Clone(v.Mapped)
	v.Scope = strings.Clone(v.Scope)
	v.Generation = strings.Clone(v.Generation)
	v.SessionSlot = strings.Clone(v.SessionSlot)
	size = len(v.Original) + len(v.Mapped) + len(v.Scope) + len(v.Generation) + len(v.Role) + len(v.SessionSlot)
	countLimit := CodexIdentityAliasLimit / codexAliasShards
	if index < CodexIdentityAliasLimit%codexAliasShards {
		countLimit++
	}
	for s.order.Len() > 0 && (s.order.Len() >= countLimit || s.bytes+size > CodexIdentityAliasBytes/codexAliasShards) {
		s.remove(s.order.Back())
	}
	if s.entries == nil {
		s.entries = make(map[[32]byte]*list.Element)
	}
	e := s.order.PushFront(&codexAliasEntry{key: key, value: v, used: now, bytes: size})
	s.entries[key] = e
	s.count.Add(1)
	s.bytes += size
	if c == codexAliases {
		codexAliasMaintenance.Do(func() {
			go func() {
				timer := time.NewTicker(time.Minute)
				defer timer.Stop()
				for range timer.C {
					c.Prune()
				}
			}()
		})
	}
}

func (c *CodexIdentityAliases) Get(role, value string) (CodexIdentityAlias, bool) {
	if value == "" || c.count.Load() == 0 {
		return CodexIdentityAlias{}, false
	}
	key := codexAliasKey(role, value)
	s := &c.shards[int(key[0])%codexAliasShards]
	s.Lock()
	defer s.Unlock()
	e := s.entries[key]
	if e == nil {
		return CodexIdentityAlias{}, false
	}
	v := e.Value.(*codexAliasEntry)
	now := c.now()
	if now.Sub(v.used) >= CodexIdentityAliasTTL {
		s.remove(e)
		return CodexIdentityAlias{}, false
	}
	v.used = now
	s.order.MoveToFront(e)
	return v.value, true
}

func (c *CodexIdentityAliases) Prune() {
	now := c.now()
	for i := range c.shards {
		s := &c.shards[i]
		s.Lock()
		for e := s.order.Back(); e != nil; e = s.order.Back() {
			if now.Sub(e.Value.(*codexAliasEntry).used) < CodexIdentityAliasTTL {
				break
			}
			s.remove(e)
		}
		s.Unlock()
	}
}

func (c *CodexIdentityAliases) RemoveGeneration(scope, generation string) {
	for i := range c.shards {
		s := &c.shards[i]
		s.Lock()
		for _, e := range s.entries {
			v := e.Value.(*codexAliasEntry).value
			if v.Scope == scope && v.Generation == generation {
				s.remove(e)
			}
		}
		s.Unlock()
	}
}

// CanonicalCodexSessionAlias normalizes only known one-to-one session aliases.
func CanonicalCodexSessionAlias(value string) string {
	for _, role := range []string{"conversation", "cache"} {
		if v, ok := codexAliases.Get(role, value); ok && !v.Converged {
			return v.Original
		}
	}
	return value
}
