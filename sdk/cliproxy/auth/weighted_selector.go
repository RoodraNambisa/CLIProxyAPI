package auth

import (
	"context"
	"math"
	"strings"
	"sync"
	"time"

	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

// WeightedRoundRobinSelector distributes eligible attempts using smooth weights.
// Existing selectors ignore weights, including an explicitly configured zero.
type WeightedRoundRobinSelector struct {
	mu      sync.Mutex
	states  map[string]*smoothWeightedState
	maxKeys int
}

type smoothWeightedState struct {
	current map[string]int64
	weights map[string]int64
}

const maxSmoothWeightedStateEntries = 1024

func positiveWeightAuths(auths []*Auth) []*Auth {
	positive := make([]*Auth, 0, len(auths))
	for _, credential := range auths {
		if credential != nil && authWeight(credential) > 0 {
			positive = append(positive, credential)
		}
	}
	return positive
}

func (s *WeightedRoundRobinSelector) Pick(ctx context.Context, provider, model string, opts core.Options, auths []*Auth) (*Auth, error) {
	return s.pickAccepted(ctx, provider, model, opts, auths, nil)
}

// pickAccepted advances credits only after the caller acquires its request slot.
// Rejected candidates keep their previous credit and consume no reservation.
func (s *WeightedRoundRobinSelector) pickAccepted(ctx context.Context, provider, model string, opts core.Options, auths []*Auth, accept func(*Auth) bool) (*Auth, error) {
	if ctx != nil && ctx.Err() != nil {
		return nil, ctx.Err()
	}
	available, err := getAvailableAuthsForContext(ctx, positiveWeightAuths(auths), provider, model, time.Now(), selectionAttemptFromMetadata(opts.Metadata))
	if err != nil {
		return nil, err
	}
	stateModel := model
	if requested, ok := opts.Metadata[core.RequestedModelMetadataKey].(string); ok && strings.TrimSpace(requested) != "" {
		stateModel = requested
	}
	key := provider + ":" + canonicalModelKey(stateModel)
	s.mu.Lock()
	defer s.mu.Unlock()
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
	}
	if s.states == nil {
		s.states = make(map[string]*smoothWeightedState)
	}
	limit := s.maxKeys
	if limit <= 0 {
		limit = 4096
	}
	if s.states[key] == nil {
		if len(s.states) >= limit {
			clear(s.states)
		}
		s.states[key] = &smoothWeightedState{}
	}
	state := s.states[key]
	picked := state.pickAccepted(available, accept)
	if picked != nil {
		return picked, nil
	}
	return nil, &Error{Code: "auth_unavailable", Message: "no auth available with positive weight"}
}

// The candidate slice is local to a pick and may be compacted after rejection.
func (s *smoothWeightedState) pickAccepted(available []*Auth, accept func(*Auth) bool) *Auth {
	s.prepare(authWeightVector(available))
	for len(available) > 0 {
		picked := s.preview(available)
		if picked == nil {
			return nil
		}
		if accept == nil || accept(picked) {
			s.commit(available, picked.ID)
			return picked
		}
		for index, candidate := range available {
			if candidate.ID == picked.ID {
				available = append(available[:index], available[index+1:]...)
				break
			}
		}
	}
	return nil
}

func authWeightVector(auths []*Auth) map[string]int64 {
	weights := make(map[string]int64, len(auths))
	for _, credential := range auths {
		if credential != nil {
			if weight := authWeight(credential); weight > 0 {
				weights[credential.ID] = weight
			}
		}
	}
	return weights
}

// A temporary candidate subset is not a configuration change. Keep its absent
// credits until the bounded stale-entry cleanup is needed; reset on real edits.
func (s *smoothWeightedState) prepare(weights map[string]int64) {
	if s.current == nil || weightsConfigChanged(s.weights, weights) {
		s.current = make(map[string]int64, len(weights))
	}
	if s.weights == nil {
		s.weights = make(map[string]int64, len(weights))
	}
	for id, weight := range weights {
		s.weights[id] = weight
	}
	if len(s.current) > maxSmoothWeightedStateEntries || len(s.weights) > maxSmoothWeightedStateEntries {
		for id := range s.current {
			if _, exists := weights[id]; !exists {
				delete(s.current, id)
			}
		}
		for id := range s.weights {
			if _, exists := weights[id]; !exists {
				delete(s.weights, id)
			}
		}
	}
}

func weightsConfigChanged(previous, current map[string]int64) bool {
	for id, weight := range current {
		if before, exists := previous[id]; exists && before != weight {
			return true
		}
	}
	return false
}

func (s *smoothWeightedState) preview(auths []*Auth) *Auth {
	var picked *Auth
	var best int64
	for _, candidate := range auths {
		if candidate == nil {
			continue
		}
		weight := authWeight(candidate)
		if weight <= 0 {
			continue
		}
		credit := saturatingAddInt64(s.current[candidate.ID], weight)
		if picked == nil || credit > best {
			picked, best = candidate, credit
		}
	}
	return picked
}

func (s *smoothWeightedState) commit(auths []*Auth, pickedID string) {
	var total int64
	for _, candidate := range auths {
		if candidate == nil {
			continue
		}
		weight := authWeight(candidate)
		if weight <= 0 {
			continue
		}
		s.current[candidate.ID] = saturatingAddInt64(s.current[candidate.ID], weight)
		total = saturatingAddInt64(total, weight)
	}
	s.current[pickedID] = saturatingAddInt64(s.current[pickedID], -total)
}

func saturatingAddInt64(value, delta int64) int64 {
	if delta > 0 && value > math.MaxInt64-delta {
		return math.MaxInt64
	}
	if delta < 0 && value < math.MinInt64-delta {
		return math.MinInt64
	}
	return value + delta
}

func (s smoothWeightedState) clone() smoothWeightedState {
	copyState := smoothWeightedState{}
	if s.current != nil {
		copyState.current = make(map[string]int64, len(s.current))
		for id, value := range s.current {
			copyState.current[id] = value
		}
	}
	if s.weights != nil {
		copyState.weights = make(map[string]int64, len(s.weights))
		for id, value := range s.weights {
			copyState.weights[id] = value
		}
	}
	return copyState
}
