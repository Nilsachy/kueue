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

	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
// scope value, along with the UID, scope and selectors of the PreemptionLimit
// they count for, and its generation when they were last reset.
type limitState struct {
	uid                           types.UID
	resetGeneration               int64
	scope                         kueuealpha.PreemptionLimitScope
	configSelector                *metav1.LabelSelector
	preemptorClusterQueueSelector *metav1.LabelSelector
	preemptedClusterQueueSelector *metav1.LabelSelector
	events                        map[policy.PreemptionLimitScopeValue]events
}

// newLimitState returns the state of limit, without events.
func newLimitState(limit *kueuealpha.PreemptionLimit) *limitState {
	return &limitState{
		uid:                           limit.UID,
		resetGeneration:               limit.Generation,
		scope:                         limit.Spec.Scope,
		configSelector:                limit.Spec.ConfigSelector.DeepCopy(),
		preemptorClusterQueueSelector: limit.Spec.PreemptorClusterQueueSelector.DeepCopy(),
		preemptedClusterQueueSelector: limit.Spec.PreemptedClusterQueueSelector.DeepCopy(),
		events:                        make(map[policy.PreemptionLimitScopeValue]events),
	}
}

// countsFor reports whether the events of s count for limit, i.e. whether
// limit has the same UID, scope and selectors as the PreemptionLimit s was
// tracked with.
func (s *limitState) countsFor(limit *kueuealpha.PreemptionLimit) bool {
	return s.uid == limit.UID &&
		s.scope == limit.Spec.Scope &&
		equality.Semantic.DeepEqual(s.configSelector, limit.Spec.ConfigSelector) &&
		equality.Semantic.DeepEqual(s.preemptorClusterQueueSelector, limit.Spec.PreemptorClusterQueueSelector) &&
		equality.Semantic.DeepEqual(s.preemptedClusterQueueSelector, limit.Spec.PreemptedClusterQueueSelector)
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
// pair in consumedLimits. Pairs resolved against another UID, or a generation
// older than the last reset (see Sync), are skipped, so that late records
// neither resurrect a deleted PreemptionLimit nor count against a recreated one
// or one whose scope or selectors changed. As a tradeoff, preemptions before
// the first Sync of a new PreemptionLimit are lost.
func (t *PreemptionLimitTracker) Record(ts time.Time, consumedLimits policy.ConsumedLimits) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for limit, consumed := range consumedLimits {
		state, found := t.limits[limit]
		if !found || state.uid != consumed.UID || consumed.Generation < state.resetGeneration {
			continue
		}
		state.events[consumed.ScopeValue] = state.events[consumed.ScopeValue].insert(ts)
	}
}

// windowStart returns the start of the window of limit ending at now.
func windowStart(now time.Time, limit *kueuealpha.PreemptionLimit) time.Time {
	return now.Add(-time.Duration(limit.Spec.LimitWindowSeconds) * time.Second)
}

// Snapshot returns a snapshot of preemptionLimits with the preemptions recorded
// within their windows ending at now. The caller must not modify preemptionLimits.
func (t *PreemptionLimitTracker) Snapshot(now time.Time, preemptionLimits []kueuealpha.PreemptionLimit) *PreemptionLimitsSnapshot {
	limits := make([]*kueuealpha.PreemptionLimit, len(preemptionLimits))
	for i := range preemptionLimits {
		limits[i] = &preemptionLimits[i]
	}
	return &PreemptionLimitsSnapshot{limits: limits, recorded: t.counts(now, limits)}
}

// counts returns a consistent snapshot of the preemptions recorded for limits
// within their windows [windowStart, now]. Scope values with 0 events are omitted,
// as are limits not tracked with the same UID, scope and selectors (see Sync).
func (t *PreemptionLimitTracker) counts(now time.Time, limits []*kueuealpha.PreemptionLimit) preemptionCounts {
	counts := make(preemptionCounts, len(limits))
	t.mu.RLock()
	defer t.mu.RUnlock()
	for _, limit := range limits {
		ref := policy.PreemptionLimitReference(limit.Name)
		state, found := t.limits[ref]
		if !found || !state.countsFor(limit) {
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

// Counts returns the number of preemptions recorded for limit within its
// window [windowStart, now], by scope value. Scope values with 0 events are
// omitted, and a limit not tracked with the same UID, scope and selectors
// (see Sync) has no counts.
func (t *PreemptionLimitTracker) Counts(now time.Time, limit *kueuealpha.PreemptionLimit) map[policy.PreemptionLimitScopeValue]int32 {
	return t.counts(now, []*kueuealpha.PreemptionLimit{limit})[policy.PreemptionLimitReference(limit.Name)]
}

// Sync starts tracking limit, or resets its events if it was recreated or its
// scope or selectors changed, saving its generation as the last reset.
// Otherwise, it prunes the events older than the window plus pruneGracePeriod;
// events pruned under a shorter window are not restored.
func (t *PreemptionLimitTracker) Sync(now time.Time, limit *kueuealpha.PreemptionLimit) {
	ref := policy.PreemptionLimitReference(limit.Name)
	t.mu.Lock()
	defer t.mu.Unlock()
	state, found := t.limits[ref]
	if !found || !state.countsFor(limit) {
		t.limits[ref] = newLimitState(limit)
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
