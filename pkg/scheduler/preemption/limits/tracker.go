/*
Copyright The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package limits

import (
	"slices"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/types"

	kueuealpha "sigs.k8s.io/kueue/apis/kueue/v1alpha1"
	"sigs.k8s.io/kueue/pkg/scheduler/preemption/policy"
)

// pruneGracePeriod delays pruning past the window, so that readers whose
// "now" lags slightly behind still find every event they count.
const pruneGracePeriod = time.Minute

// preemptionCounts holds a number of preemptions per PreemptionLimit and scope value.
// A missing entry counts as 0.
type preemptionCounts map[policy.PreemptionLimitReference]map[policy.PreemptionLimitScopeValue]int32

// add adds n preemptions for limit and scopeValue.
func (c preemptionCounts) add(limit policy.PreemptionLimitReference, scopeValue policy.PreemptionLimitScopeValue, n int32) {
	if c[limit] == nil {
		c[limit] = make(map[policy.PreemptionLimitScopeValue]int32)
	}
	c[limit][scopeValue] += n
}

// events holds preemption timestamps in ascending order.
type events []time.Time

// insert adds ts while keeping the events sorted. Timestamps usually arrive in
// order, so the insertion point is searched from the tail.
func (e events) insert(ts time.Time) events {
	i := len(e)
	for i > 0 && e[i-1].After(ts) {
		i--
	}
	return slices.Insert(e, i, ts)
}

// countSince returns the number of events at or after cutoff.
func (e events) countSince(cutoff time.Time) int {
	i, _ := slices.BinarySearchFunc(e, cutoff, time.Time.Compare) // first event >= cutoff
	return len(e) - i
}

// pruneBefore reslices e to drop events before cutoff without copying. Dropped
// elements stay in memory until a later insert reallocates the slice.
func (e events) pruneBefore(cutoff time.Time) events {
	i, _ := slices.BinarySearchFunc(e, cutoff, time.Time.Compare) // first event >= cutoff
	return e[i:]
}

// limitState holds the preemption events of a single PreemptionLimit, by
// scope value, along with the UID and scope of the PreemptionLimit they count
// for.
type limitState struct {
	uid    types.UID
	scope  kueuealpha.PreemptionLimitScope
	events map[policy.PreemptionLimitScopeValue]events
}

// PreemptionLimitTracker records the timestamps of confirmed configurable preemptions for
// each PreemptionLimit and scope value. It is safe for concurrent use.
type PreemptionLimitTracker struct {
	mu     sync.RWMutex
	limits map[policy.PreemptionLimitReference]*limitState
}

// NewPreemptionLimitTracker creates an empty PreemptionLimitTracker.
func NewPreemptionLimitTracker() *PreemptionLimitTracker {
	return &PreemptionLimitTracker{
		limits: make(map[policy.PreemptionLimitReference]*limitState),
	}
}

// Record records a preemption at ts for every PreemptionLimit and scope value
// pair in consumedLimits. Pairs resolved against a PreemptionLimit not tracked
// with the same UID and scope (see Sync) are skipped, so that late records
// neither resurrect a deleted PreemptionLimit nor count against a recreated or
// rescoped one. As a tradeoff, preemptions before the first Sync of a new
// PreemptionLimit are lost.
func (t *PreemptionLimitTracker) Record(ts time.Time, consumedLimits policy.ConsumedLimits) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for limit, consumed := range consumedLimits {
		state, found := t.limits[limit]
		if !found || state.uid != consumed.UID || state.scope != consumed.Scope {
			continue
		}
		state.events[consumed.ScopeValue] = state.events[consumed.ScopeValue].insert(ts)
	}
}

// windowStart returns the start of the window of limit ending at now.
func windowStart(now time.Time, limit *kueuealpha.PreemptionLimit) time.Time {
	return now.Add(-time.Duration(limit.Spec.LimitWindowSeconds) * time.Second)
}

// counts returns a consistent snapshot of the preemptions recorded for limits
// within their windows [windowStart, now]. Scope values with 0 events are omitted,
// as are limits not tracked with the same UID and scope (see Sync).
func (t *PreemptionLimitTracker) counts(now time.Time, limits []*kueuealpha.PreemptionLimit) preemptionCounts {
	counts := make(preemptionCounts, len(limits))
	t.mu.RLock()
	defer t.mu.RUnlock()
	for _, limit := range limits {
		ref := policy.PreemptionLimitReference(limit.Name)
		state, found := t.limits[ref]
		if !found || state.uid != limit.UID || state.scope != limit.Spec.Scope {
			continue
		}
		cutoff := windowStart(now, limit)
		for scopeValue, e := range state.events {
			if count := e.countSince(cutoff); count > 0 {
				counts.add(ref, scopeValue, int32(count))
			}
		}
	}
	return counts
}

// Sync starts tracking limit, or resets its events if it was recreated or its
// scope changed. Otherwise, it prunes the events older than the window plus
// pruneGracePeriod; events pruned under a shorter window are not restored.
func (t *PreemptionLimitTracker) Sync(now time.Time, limit *kueuealpha.PreemptionLimit) {
	ref := policy.PreemptionLimitReference(limit.Name)
	t.mu.Lock()
	defer t.mu.Unlock()
	state, found := t.limits[ref]
	if !found || state.uid != limit.UID || state.scope != limit.Spec.Scope {
		t.limits[ref] = &limitState{
			uid:    limit.UID,
			scope:  limit.Spec.Scope,
			events: make(map[policy.PreemptionLimitScopeValue]events),
		}
		return
	}
	cutoff := windowStart(now, limit).Add(-pruneGracePeriod)
	for scopeValue, e := range state.events {
		if e = e.pruneBefore(cutoff); len(e) == 0 {
			delete(state.events, scopeValue)
		} else {
			state.events[scopeValue] = e
		}
	}
}

// Forget stops tracking limit, dropping all its events.
func (t *PreemptionLimitTracker) Forget(limit policy.PreemptionLimitReference) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.limits, limit)
}
