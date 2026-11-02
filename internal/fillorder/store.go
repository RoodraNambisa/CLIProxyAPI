// Package fillorder owns durable credential ordering independently of runtime auth IDs.
package fillorder

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

var ErrConflict = errors.New("fill order changed; refresh and retry")
var ErrNotFound = errors.New("fill order record not found")
var ErrInvalidMove = errors.New("invalid fill order move")

type Input struct {
	Key      string `json:"key"`
	Identity string `json:"identity,omitempty"`
	Scope    string `json:"scope,omitempty"`
	Priority int    `json:"priority"`
}
type Record struct {
	Input
	ID       string `json:"id"`
	Sequence uint64 `json:"sequence,string"`
	Rank     uint64 `json:"rank,string"`
	Position uint64 `json:"-"`
}
type Move struct {
	RecordID  string `json:"record_id"`
	Revision  string `json:"revision"`
	Direction string `json:"direction,omitempty"`
	Steps     int    `json:"steps,omitempty"`
	BeforeID  string `json:"before_id,omitempty"`
	AfterID   string `json:"after_id,omitempty"`
}
type Preview struct {
	Record   Record  `json:"record"`
	Before   *Record `json:"before,omitempty"`
	After    *Record `json:"after,omitempty"`
	Revision string  `json:"revision"`
}
type state struct {
	Format    int      `json:"format"`
	Revision  uint64   `json:"revision,string"`
	HighWater uint64   `json:"high_water,string"`
	Records   []Record `json:"records"`
}
type change struct {
	Revision  uint64   `json:"revision,string"`
	HighWater uint64   `json:"high_water,string"`
	Upserts   []Record `json:"upserts,omitempty"`
	Deletes   []string `json:"deletes,omitempty"`
	MoveID    string   `json:"move_id,omitempty"`
	BeforeID  string   `json:"before_id,omitempty"`
}
type envelope struct {
	Data     json.RawMessage `json:"data"`
	Checksum string          `json:"sha256"`
}

type Store struct {
	stop                chan struct{}
	done                chan struct{}
	closeOnce           sync.Once
	writeFailure        error
	writeMu             sync.Mutex
	mu                  sync.RWMutex
	records             []Record
	byKey               map[string]int
	byID                map[string]int
	bySequence          map[uint64]string
	revision, highWater uint64
	path                string
	journal             *os.File
	lock                *os.File
	closed              bool
	events              int
}

// Open uses an in-memory registry when path is empty. A nonempty path is exclusively locked.
func Open(path string) (*Store, error) {
	s := &Store{path: path, byKey: make(map[string]int), byID: make(map[string]int), bySequence: make(map[uint64]string)}
	if path == "" {
		return s, nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path+".lock", os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err = lockFile(f); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("lock fill order: %w", err)
	}
	s.lock = f
	if err = s.restore(); err != nil {
		_ = s.Close()
		return nil, err
	}
	s.journal, err = os.OpenFile(path+".wal", os.O_CREATE|os.O_RDWR|os.O_APPEND, 0600)
	if err != nil {
		_ = s.Close()
		return nil, err
	}
	s.stop = make(chan struct{})
	s.done = make(chan struct{})
	go s.maintenance()
	return s, nil
}
func (s *Store) restore() error {
	data, err := os.ReadFile(s.path)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err == nil {
		var st state
		if err = decode(data, &st); err != nil {
			return fmt.Errorf("read fill order snapshot: %w", err)
		}
		if st.Format != 1 {
			return errors.New("unsupported fill order format")
		}
		s.records, s.revision, s.highWater = st.Records, st.Revision, st.HighWater
		if err = s.index(); err != nil {
			return err
		}
	}
	f, err := os.OpenFile(s.path+".wal", os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	reader := bufio.NewReader(f)
	var offset int64
	for {
		line, readErr := reader.ReadBytes('\n')
		if readErr == io.EOF {
			if len(line) > 0 {
				if err = f.Truncate(offset); err != nil {
					return err
				}
				return f.Sync()
			}
			break
		}
		if readErr != nil {
			return readErr
		}
		var op change
		if err = decode(line, &op); err != nil {
			return fmt.Errorf("read fill order journal: %w", err)
		}
		if op.Revision > s.revision {
			if op.Revision != s.revision+1 || op.HighWater < s.highWater {
				return errors.New("invalid fill order journal sequence")
			}
			if err = s.validate(op); err != nil {
				return err
			}
			if err = s.apply(op); err != nil {
				return err
			}
		}
		offset += int64(len(line))
	}
	return nil
}
func encode(v any) ([]byte, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	sum := sha256.Sum256(raw)
	return json.Marshal(envelope{Data: raw, Checksum: hex.EncodeToString(sum[:])})
}
func decode(data []byte, v any) error {
	var e envelope
	if err := json.Unmarshal(data, &e); err != nil {
		return err
	}
	sum := sha256.Sum256(e.Data)
	if e.Checksum != hex.EncodeToString(sum[:]) {
		return errors.New("fill order checksum mismatch")
	}
	return json.Unmarshal(e.Data, v)
}
func (s *Store) index() error {
	clear(s.byID)
	clear(s.byKey)
	clear(s.bySequence)
	for i := range s.records {
		r := &s.records[i]
		if _, err := uuid.Parse(r.ID); err != nil {
			return errors.New("invalid fill order record UUID")
		}
		if r.ID == "" || r.Key == "" || r.Sequence == 0 || r.Sequence > s.highWater || r.Rank == 0 || (i > 0 && r.Rank <= s.records[i-1].Rank) {
			return errors.New("invalid fill order record")
		}
		if _, ok := s.byID[r.ID]; ok {
			return errors.New("duplicate fill order record ID")
		}
		if _, ok := s.byKey[r.Key]; ok {
			return errors.New("duplicate fill order source")
		}
		if _, ok := s.bySequence[r.Sequence]; ok {
			return errors.New("duplicate fill order sequence")
		}
		s.bySequence[r.Sequence] = r.ID
		r.Position = uint64(i + 1)
		s.byID[r.ID] = i
		s.byKey[r.Key] = i
	}
	return nil
}
func (s *Store) apply(op change) error {
	if len(op.Deletes) > 0 {
		removed := make(map[string]bool, len(op.Deletes))
		for _, key := range op.Deletes {
			removed[key] = true
		}
		s.records = slices.DeleteFunc(s.records, func(r Record) bool { return removed[r.Key] })
		if err := s.index(); err != nil {
			return err
		}
	}
	for _, r := range op.Upserts {
		if i, ok := s.byID[r.ID]; ok {
			delete(s.byKey, s.records[i].Key)
			s.records[i] = r
			s.byKey[r.Key] = i
			s.records[i].Position = uint64(i + 1)
		} else {
			if _, ok := s.byKey[r.Key]; ok {
				return errors.New("fill order source collision")
			}
			s.byKey[r.Key] = len(s.records)
			s.byID[r.ID] = len(s.records)
			s.bySequence[r.Sequence] = r.ID
			r.Rank = 1
			if len(s.records) > 0 {
				r.Rank = s.records[len(s.records)-1].Rank + 1
			}
			r.Position = uint64(len(s.records) + 1)
			s.records = append(s.records, r)
		}
	}
	if op.MoveID != "" {
		i, ok := s.byID[op.MoveID]
		if !ok {
			return ErrNotFound
		}
		r := s.records[i]
		s.records = slices.Delete(s.records, i, i+1)
		target := len(s.records)
		if op.BeforeID != "" {
			target = slices.IndexFunc(s.records, func(v Record) bool { return v.ID == op.BeforeID })
			if target < 0 {
				return ErrNotFound
			}
		}
		s.records = slices.Insert(s.records, target, r)
		for i := range s.records {
			s.records[i].Rank = uint64(i + 1)
		}
	}
	s.revision, s.highWater = op.Revision, op.HighWater
	if len(op.Deletes) > 0 || op.MoveID != "" {
		return s.index()
	}
	return nil
}
func (s *Store) commit(op change) error {
	if s.writeFailure != nil {
		return s.writeFailure
	}
	if s.closed {
		return errors.New("fill order closed")
	}
	if s.revision == math.MaxUint64 {
		return errors.New("fill order revision exhausted")
	}
	op.Revision = s.revision + 1
	if op.HighWater < s.highWater {
		op.HighWater = s.highWater
	}
	if err := s.validate(op); err != nil {
		return err
	}
	if s.journal != nil {
		raw, err := encode(op)
		if err != nil {
			return err
		}
		raw = append(raw, '\n')
		offset, err := s.journal.Seek(0, io.SeekEnd)
		if err != nil {
			return err
		}
		n, err := s.journal.Write(raw)
		if err == nil && n != len(raw) {
			err = io.ErrShortWrite
		}
		if err == nil {
			err = s.journal.Sync()
		}
		if err != nil {
			rollbackErr := errors.Join(s.journal.Truncate(offset), s.journal.Sync())
			if rollbackErr != nil {
				s.writeFailure = fmt.Errorf("fill order journal requires recovery: %w", rollbackErr)
			}
			return fmt.Errorf("persist fill order: %w", errors.Join(err, rollbackErr))
		}
	}
	s.mu.Lock()
	err := s.apply(op)
	s.mu.Unlock()
	s.events++
	if err != nil {
		s.writeFailure = fmt.Errorf("fill order requires recovery: %w", err)
	}
	return err
}
func (s *Store) Lookup(key string) (Record, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	i, ok := s.byKey[key]
	if !ok {
		return Record{}, false
	}
	return s.records[i], true
}
func (s *Store) Revision() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return strconv.FormatUint(s.revision, 10)
}
func (s *Store) Records() []Record {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return slices.Clone(s.records)
}
func (s *Store) Upsert(in Input, trustedRefresh ...bool) (Record, error) {
	if in.Key == "" {
		return Record{}, errors.New("empty fill order source")
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if r, ok := s.Lookup(in.Key); ok {
		if r.Scope == in.Scope && (r.Identity == in.Identity || in.Identity == "" || len(trustedRefresh) > 0 && trustedRefresh[0]) {
			if r.Priority == in.Priority && (r.Identity == in.Identity || in.Identity == "") {
				return r, nil
			}
			r.Priority = in.Priority
			if in.Identity != "" {
				r.Identity = in.Identity
			}
			if err := s.commit(change{Upserts: []Record{r}}); err != nil {
				return Record{}, err
			}
			return r, nil
		}
	}
	if s.highWater == math.MaxUint64 || len(s.records) > 0 && s.records[len(s.records)-1].Rank == math.MaxUint64 {
		return Record{}, errors.New("fill order sequence exhausted")
	}
	r := Record{Input: in, ID: uuid.NewString(), Sequence: s.highWater + 1}
	for {
		if _, ok := s.byID[r.ID]; !ok {
			break
		}
		r.ID = uuid.NewString()
	}
	op := change{HighWater: r.Sequence, Upserts: []Record{r}}
	if _, exists := s.Lookup(in.Key); exists {
		op.Deletes = []string{in.Key}
	}
	if err := s.commit(op); err != nil {
		return Record{}, err
	}
	r, _ = s.Lookup(in.Key)
	return r, nil
}
func (s *Store) Delete(key string) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if _, ok := s.Lookup(key); !ok {
		return nil
	}
	return s.commit(change{Deletes: []string{key}})
}

// Reconcile bootstraps in the former ID order and recognizes an unambiguous offline rename.
func (s *Store) Reconcile(inputs []Input) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	inputs = slices.Clone(inputs)
	sort.Slice(inputs, func(i, j int) bool { return inputs[i].Key < inputs[j].Key })
	present := make(map[string]bool, len(inputs))
	identityCount := make(map[string]int)
	for _, in := range inputs {
		if in.Key == "" {
			return errors.New("empty fill order source")
		}
		if present[in.Key] {
			return errors.New("duplicate fill order input")
		}
		present[in.Key] = true
		if in.Identity != "" {
			identityCount[in.Identity]++
		}
	}
	absent := make(map[string][]Record)
	for _, r := range s.records {
		if !present[r.Key] && r.Identity != "" {
			absent[r.Identity] = append(absent[r.Identity], r)
		}
	}
	op := change{HighWater: s.highWater}
	migrated := make(map[string]bool)
	allocated := make(map[string]bool)
	for _, in := range inputs {
		if r, ok := s.Lookup(in.Key); ok && r.Scope == in.Scope && (r.Identity == in.Identity || in.Identity == "") {
			if r.Priority != in.Priority {
				r.Priority = in.Priority
				op.Upserts = append(op.Upserts, r)
			}
			continue
		}
		if matches := absent[in.Identity]; in.Identity != "" && len(matches) == 1 && identityCount[in.Identity] == 1 {
			r := matches[0]
			if _, occupied := s.Lookup(in.Key); occupied {
				op.Deletes = append(op.Deletes, in.Key)
			}
			migrated[r.Key] = true
			r.Input = in
			op.Upserts = append(op.Upserts, r)
			continue
		}
		if op.HighWater == math.MaxUint64 {
			return errors.New("fill order sequence exhausted")
		}
		op.HighWater++
		if _, ok := s.Lookup(in.Key); ok {
			op.Deletes = append(op.Deletes, in.Key)
		}
		id := uuid.NewString()
		for {
			_, exists := s.byID[id]
			if !exists && !allocated[id] {
				break
			}
			id = uuid.NewString()
		}
		allocated[id] = true
		op.Upserts = append(op.Upserts, Record{Input: in, ID: id, Sequence: op.HighWater})
	}
	for _, r := range s.records {
		if !present[r.Key] && !migrated[r.Key] {
			op.Deletes = append(op.Deletes, r.Key)
		}
	}
	if len(op.Upserts) == 0 && len(op.Deletes) == 0 {
		return nil
	}
	return s.commit(op)
}
func (s *Store) preview(m Move) (Preview, change, error) {
	if m.Revision != strconv.FormatUint(s.revision, 10) {
		return Preview{}, change{}, ErrConflict
	}
	i, ok := s.byID[m.RecordID]
	if !ok {
		return Preview{}, change{}, ErrNotFound
	}
	r := s.records[i]
	modeCount := 0
	for _, v := range []string{m.Direction, m.BeforeID, m.AfterID} {
		if v != "" {
			modeCount++
		}
	}
	if modeCount != 1 {
		return Preview{}, change{}, ErrInvalidMove
	}
	var peers []Record
	for _, v := range s.records {
		if v.Priority == r.Priority {
			peers = append(peers, v)
		}
	}
	current := slices.IndexFunc(peers, func(v Record) bool { return v.ID == r.ID })
	target := current
	if m.Direction != "" {
		if m.Steps < 1 {
			return Preview{}, change{}, ErrInvalidMove
		}
		switch m.Direction {
		case "up":
			target = max(0, current-min(m.Steps, len(peers)))
		case "down":
			target = min(len(peers)-1, current+min(m.Steps, len(peers)))
		default:
			return Preview{}, change{}, ErrInvalidMove
		}
	} else {
		id := m.BeforeID
		if id == "" {
			id = m.AfterID
		}
		anchor, ok := s.byID[id]
		if !ok {
			return Preview{}, change{}, ErrNotFound
		}
		if id == r.ID || s.records[anchor].Priority != r.Priority {
			return Preview{}, change{}, ErrInvalidMove
		}
		target = slices.IndexFunc(peers, func(v Record) bool { return v.ID == id })
		if target > current {
			target--
		}
		if m.AfterID != "" {
			target++
		}
	}
	op := change{}
	if target != current {
		peers = slices.Delete(peers, current, current+1)
		peers = slices.Insert(peers, target, r)
		op.MoveID = r.ID
		if target+1 < len(peers) {
			op.BeforeID = peers[target+1].ID
		} else {
			// Insert immediately after the last peer, preserving other priorities' relative order.
			last := peers[target-1]
			at := s.byID[last.ID] + 1
			for at < len(s.records) && s.records[at].ID == r.ID {
				at++
			}
			if at < len(s.records) {
				op.BeforeID = s.records[at].ID
			}
		}
	}
	p := Preview{Record: r, Revision: m.Revision}
	if target > 0 {
		v := peers[target-1]
		p.Before = &v
	}
	if target+1 < len(peers) {
		v := peers[target+1]
		p.After = &v
	}
	return p, op, nil
}
func (s *Store) Preview(m Move) (Preview, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	p, _, err := s.preview(m)
	return p, err
}
func (s *Store) Move(m Move) (Preview, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	s.mu.RLock()
	p, op, err := s.preview(m)
	s.mu.RUnlock()
	if err != nil {
		return p, err
	}
	if op.MoveID != "" {
		if err = s.commit(op); err != nil {
			return Preview{}, err
		}
	}
	p.Revision = s.Revision()
	p.Record, _ = s.Lookup(p.Record.Key)
	return p, nil
}

// Compact bounds journal growth. Callers invoke this outside request scheduling locks.
func (s *Store) Compact() error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if s.writeFailure != nil {
		return s.writeFailure
	}
	if s.path == "" || s.closed {
		return nil
	}
	s.mu.RLock()
	st := state{Format: 1, Revision: s.revision, HighWater: s.highWater, Records: slices.Clone(s.records)}
	s.mu.RUnlock()
	raw, err := encode(st)
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(s.path), ".fill-order-*")
	if err != nil {
		return err
	}
	name := tmp.Name()
	defer func() { _ = os.Remove(name) }()
	if err = tmp.Chmod(0600); err == nil {
		_, err = tmp.Write(raw)
	}
	if err == nil {
		err = tmp.Sync()
	}
	closeErr := tmp.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	if err = os.Rename(name, s.path); err != nil {
		return err
	}
	if err = syncDirectory(filepath.Dir(s.path)); err != nil {
		return err
	}
	if err = s.journal.Truncate(0); err != nil {
		return err
	}
	if err = s.journal.Sync(); err != nil {
		return err
	}
	s.events = 0
	return nil
}
func (s *Store) Close() error {
	if s.stop != nil {
		s.closeOnce.Do(func() { close(s.stop) })
		<-s.done
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	var err error
	if s.journal != nil {
		err = s.journal.Close()
	}
	if s.lock != nil {
		err = errors.Join(err, unlockFile(s.lock), s.lock.Close())
	}
	return err
}

// maintenance has one bounded lifetime per registry, independent of credential count.
func (s *Store) maintenance() {
	defer close(s.done)
	timer := time.NewTicker(30 * time.Second)
	defer timer.Stop()
	for {
		select {
		case <-s.stop:
			return
		case <-timer.C:
			s.writeMu.Lock()
			needed := s.events >= 4096
			if s.journal != nil {
				if st, err := s.journal.Stat(); err == nil && st.Size() >= 8<<20 {
					needed = true
				}
			}
			s.writeMu.Unlock()
			if needed {
				if err := s.Compact(); err != nil {
					log.WithError(err).Warn("fill order journal compaction failed")
				}
			}
		}
	}
}

// validate checks each changed record without copying or scanning unaffected records.
func (s *Store) validate(op change) error {
	if op.Revision != s.revision+1 || op.HighWater < s.highWater {
		return errors.New("invalid fill order revision")
	}
	deleted := make(map[string]bool, len(op.Deletes))
	for _, key := range op.Deletes {
		deleted[key] = true
	}
	ids := make(map[string]bool, len(op.Upserts))
	keys := make(map[string]bool, len(op.Upserts))
	seqs := make(map[uint64]bool, len(op.Upserts))
	appended := uint64(0)
	for _, r := range op.Upserts {
		if r.ID == "" || r.Key == "" || r.Sequence == 0 || r.Sequence > op.HighWater || ids[r.ID] || keys[r.Key] || seqs[r.Sequence] {
			return errors.New("invalid or duplicate fill order record")
		}
		if _, err := uuid.Parse(r.ID); err != nil {
			return errors.New("invalid fill order record UUID")
		}
		ids[r.ID] = true
		keys[r.Key] = true
		seqs[r.Sequence] = true
		if i, exists := s.byKey[r.Key]; exists && s.records[i].ID != r.ID && !deleted[r.Key] {
			return errors.New("fill order source collision")
		}
		if i, exists := s.byID[r.ID]; exists {
			old := s.records[i]
			if deleted[old.Key] || r.Sequence != old.Sequence || r.Rank != old.Rank {
				return errors.New("fill order immutable identity changed")
			}
		} else {
			appended++
			if r.Sequence <= s.highWater {
				return errors.New("fill order sequence reused")
			}
		}
		if id, exists := s.bySequence[r.Sequence]; exists && id != r.ID {
			return errors.New("duplicate fill order sequence")
		}
	}
	if len(s.records) > 0 && appended > math.MaxUint64-s.records[len(s.records)-1].Rank {
		return errors.New("fill order rank exhausted")
	}
	if op.MoveID != "" {
		i, ok := s.byID[op.MoveID]
		if !ok || deleted[s.records[i].Key] || len(op.Upserts) > 0 || len(op.Deletes) > 0 {
			return ErrInvalidMove
		}
		if op.BeforeID != "" {
			if _, ok := s.byID[op.BeforeID]; !ok || op.BeforeID == op.MoveID {
				return ErrInvalidMove
			}
		}
	}
	return nil
}
