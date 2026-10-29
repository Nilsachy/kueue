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
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	kueuealpha "sigs.k8s.io/kueue/apis/kueue/v1alpha1"
	"sigs.k8s.io/kueue/pkg/scheduler/preemption/policy"
	utiltestingalpha "sigs.k8s.io/kueue/pkg/util/testing/v1alpha1"
)

func TestPreemptionLimitTrackerCounts(t *testing.T) {
	now := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	perCQLimit := utiltestingalpha.MakePreemptionLimit("per-cq", perCQ).LimitWindowSeconds(60).Obj()
	cutoff := now.Add(-60 * time.Second)

	cases := map[string]struct {
		limits trackedLimits
		asked  []*kueuealpha.PreemptionLimit
		want   preemptionCounts
	}{
		"events before the window are not counted": {
			limits: trackedLimits{
				"per-cq": trackedLimit("per-cq-uid", perCQ, scopeEvents{"cq-a": {cutoff.Add(-time.Hour), cutoff.Add(-time.Nanosecond), cutoff.Add(time.Nanosecond), now}}),
			},
			asked: []*kueuealpha.PreemptionLimit{perCQLimit},
			want: preemptionCounts{
				"per-cq": {"cq-a": 2},
			},
		},
		"event at the start of the window is counted": {
			limits: trackedLimits{
				"per-cq": trackedLimit("per-cq-uid", perCQ, scopeEvents{"cq-a": {cutoff}}),
			},
			asked: []*kueuealpha.PreemptionLimit{perCQLimit},
			want: preemptionCounts{
				"per-cq": {"cq-a": 1},
			},
		},
		"each limit counts within its own window": {
			limits: trackedLimits{
				"per-cq": trackedLimit("per-cq-uid", perCQ, scopeEvents{"cq-a": {now.Add(-30 * time.Minute)}}),
				"global": trackedLimit("global-uid", global, scopeEvents{"Global": {now.Add(-2 * time.Hour), now.Add(-30 * time.Minute)}}),
			},
			asked: []*kueuealpha.PreemptionLimit{
				perCQLimit,
				utiltestingalpha.MakePreemptionLimit("global", global).LimitWindowSeconds(3600).Obj(),
			},
			want: preemptionCounts{
				"global": {"Global": 1},
			},
		},
		"every scope value of a limit is counted": {
			limits: trackedLimits{
				"per-cq": trackedLimit("per-cq-uid", perCQ, scopeEvents{"cq-a": {now}, "cq-b": {now, now}}),
			},
			asked: []*kueuealpha.PreemptionLimit{perCQLimit},
			want: preemptionCounts{
				"per-cq": {"cq-a": 1, "cq-b": 2},
			},
		},
		"limits that are not asked for are not counted": {
			limits: trackedLimits{
				"other": trackedLimit("other-uid", perCQ, scopeEvents{"cq-a": {now}}),
			},
			asked: []*kueuealpha.PreemptionLimit{perCQLimit},
		},
		"limit tracked with another UID is not counted": {
			limits: trackedLimits{
				"per-cq": trackedLimit("old-uid", perCQ, scopeEvents{"cq-a": {now}}),
			},
			asked: []*kueuealpha.PreemptionLimit{perCQLimit},
		},
		"limit tracked with another scope is not counted": {
			limits: trackedLimits{
				"per-cq": trackedLimit("per-cq-uid", kueuealpha.PreemptedCQLimitScope, scopeEvents{"cq-a": {now}}),
			},
			asked: []*kueuealpha.PreemptionLimit{perCQLimit},
		},
		"limit tracked with other selectors is not counted": {
			limits: trackedLimits{
				"per-cq": withSelectors(trackedLimit("per-cq-uid", perCQ, scopeEvents{"cq-a": {now}}), selector("tier", "gold"), nil, nil),
			},
			asked: []*kueuealpha.PreemptionLimit{perCQLimit},
		},
		"limit tracked with the same selectors is counted": {
			limits: trackedLimits{
				"per-cq": withSelectors(trackedLimit("per-cq-uid", perCQ, scopeEvents{"cq-a": {now}}),
					selector("tier", "gold"), selector("side", "preemptor"), selector("side", "preempted")),
			},
			asked: []*kueuealpha.PreemptionLimit{
				utiltestingalpha.MakePreemptionLimit("per-cq", perCQ).LimitWindowSeconds(60).
					ConfigSelector(selector("tier", "gold")).
					PreemptorClusterQueueSelector(selector("side", "preemptor")).
					PreemptedClusterQueueSelector(selector("side", "preempted")).
					Obj(),
			},
			want: preemptionCounts{
				"per-cq": {"cq-a": 1},
			},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			tracker := NewPreemptionLimitTracker()
			if tc.limits != nil {
				tracker.limits = tc.limits
			}
			got := tracker.counts(now, tc.asked)
			if diff := cmp.Diff(tc.want, got, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("Unexpected counts (-want,+got):\n%s", diff)
			}
		})
	}
}

// Scopes of the PreemptionLimits used across the tracker tests.
const (
	global = kueuealpha.GlobalPreemptionLimitScope
	perCQ  = kueuealpha.PreemptorCQLimitScope
)

type scopeEvents = map[policy.PreemptionLimitScopeValue]events

type trackedLimits = map[policy.PreemptionLimitReference]*limitState

var cmpTrackedLimits = []cmp.Option{
	cmp.AllowUnexported(limitState{}),
	cmpopts.EquateEmpty(),
}

func trackedLimit(uid types.UID, scope kueuealpha.PreemptionLimitScope, events scopeEvents) *limitState {
	if events == nil {
		// As allocated by Sync.
		events = scopeEvents{}
	}
	return &limitState{uid: uid, scope: scope, events: events}
}

// withSelectors sets the selectors s was tracked with.
func withSelectors(s *limitState, config, preemptorClusterQueue, preemptedClusterQueue *metav1.LabelSelector) *limitState {
	s.configSelector = config
	s.preemptorClusterQueueSelector = preemptorClusterQueue
	s.preemptedClusterQueueSelector = preemptedClusterQueue
	return s
}

// selector returns a LabelSelector matching the label key=value.
func selector(key, value string) *metav1.LabelSelector {
	return &metav1.LabelSelector{MatchLabels: map[string]string{key: value}}
}

// withResetGeneration sets the generation of the PreemptionLimit when the
// events of s were last reset.
func withResetGeneration(s *limitState, generation int64) *limitState {
	s.resetGeneration = generation
	return s
}

func consumedLimit(uid types.UID, scopeValue policy.PreemptionLimitScopeValue) policy.ConsumedLimit {
	return policy.ConsumedLimit{UID: uid, ScopeValue: scopeValue}
}

func TestPreemptionLimitTrackerRecord(t *testing.T) {
	t1 := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	t2 := t1.Add(time.Second)
	t3 := t2.Add(time.Second)

	cases := map[string]struct {
		limits   trackedLimits
		ts       time.Time
		consumed policy.ConsumedLimits
		want     trackedLimits
	}{
		"nil consumed limits is a no-op": {
			limits: trackedLimits{
				"global": trackedLimit("global-uid", global, scopeEvents{"Global": {t1}}),
			},
			ts: t2,
			want: trackedLimits{
				"global": trackedLimit("global-uid", global, scopeEvents{"Global": {t1}}),
			},
		},
		"every consumed limit is recorded": {
			limits: trackedLimits{
				"global": trackedLimit("global-uid", global, nil),
				"per-cq": trackedLimit("per-cq-uid", perCQ, nil),
			},
			ts: t1,
			consumed: policy.ConsumedLimits{
				"global": consumedLimit("global-uid", "Global"),
				"per-cq": consumedLimit("per-cq-uid", "cq-a"),
			},
			want: trackedLimits{
				"global": trackedLimit("global-uid", global, scopeEvents{"Global": {t1}}),
				"per-cq": trackedLimit("per-cq-uid", perCQ, scopeEvents{"cq-a": {t1}}),
			},
		},
		"untracked limit is skipped": {
			limits: trackedLimits{
				"global": trackedLimit("global-uid", global, nil),
			},
			ts: t1,
			consumed: policy.ConsumedLimits{
				"global": consumedLimit("global-uid", "Global"),
				"per-cq": consumedLimit("per-cq-uid", "cq-a"),
			},
			want: trackedLimits{
				"global": trackedLimit("global-uid", global, scopeEvents{"Global": {t1}}),
			},
		},
		"limit resolved before it was recreated is skipped": {
			limits: trackedLimits{
				"global": trackedLimit("global-uid", global, nil),
				"per-cq": trackedLimit("new-uid", perCQ, scopeEvents{"cq-a": {t1}}),
			},
			ts: t2,
			consumed: policy.ConsumedLimits{
				"global": consumedLimit("global-uid", "Global"),
				"per-cq": consumedLimit("old-uid", "cq-a"),
			},
			want: trackedLimits{
				"global": trackedLimit("global-uid", global, scopeEvents{"Global": {t2}}),
				"per-cq": trackedLimit("new-uid", perCQ, scopeEvents{"cq-a": {t1}}),
			},
		},
		"limit resolved before its last reset is skipped": {
			limits: trackedLimits{
				"global": trackedLimit("global-uid", global, nil),
				"per-cq": withResetGeneration(trackedLimit("per-cq-uid", perCQ, scopeEvents{"cq-a": {t1}}), 2),
			},
			ts: t2,
			consumed: policy.ConsumedLimits{
				"global": consumedLimit("global-uid", "Global"),
				"per-cq": {UID: "per-cq-uid", Generation: 1, ScopeValue: "cq-a"},
			},
			want: trackedLimits{
				"global": trackedLimit("global-uid", global, scopeEvents{"Global": {t2}}),
				"per-cq": withResetGeneration(trackedLimit("per-cq-uid", perCQ, scopeEvents{"cq-a": {t1}}), 2),
			},
		},
		"limit resolved at or after its last reset is recorded": {
			limits: trackedLimits{
				"global": withResetGeneration(trackedLimit("global-uid", global, nil), 2),
				"per-cq": withResetGeneration(trackedLimit("per-cq-uid", perCQ, nil), 2),
			},
			ts: t1,
			consumed: policy.ConsumedLimits{
				"global": {UID: "global-uid", Generation: 2, ScopeValue: "Global"},
				"per-cq": {UID: "per-cq-uid", Generation: 3, ScopeValue: "cq-a"},
			},
			want: trackedLimits{
				"global": withResetGeneration(trackedLimit("global-uid", global, scopeEvents{"Global": {t1}}), 2),
				"per-cq": withResetGeneration(trackedLimit("per-cq-uid", perCQ, scopeEvents{"cq-a": {t1}}), 2),
			},
		},
		"new scope value of a recorded limit": {
			limits: trackedLimits{
				"per-cq": trackedLimit("per-cq-uid", perCQ, scopeEvents{"cq-a": {t1}}),
			},
			ts:       t2,
			consumed: policy.ConsumedLimits{"per-cq": consumedLimit("per-cq-uid", "cq-b")},
			want: trackedLimits{
				"per-cq": trackedLimit("per-cq-uid", perCQ, scopeEvents{"cq-a": {t1}, "cq-b": {t2}}),
			},
		},
		"later event is appended": {
			limits: trackedLimits{
				"global": trackedLimit("global-uid", global, scopeEvents{"Global": {t1, t2}}),
			},
			ts:       t3,
			consumed: policy.ConsumedLimits{"global": consumedLimit("global-uid", "Global")},
			want: trackedLimits{
				"global": trackedLimit("global-uid", global, scopeEvents{"Global": {t1, t2, t3}}),
			},
		},
		"earlier event is inserted in order": {
			limits: trackedLimits{
				"global": trackedLimit("global-uid", global, scopeEvents{"Global": {t1, t3}}),
			},
			ts:       t2,
			consumed: policy.ConsumedLimits{"global": consumedLimit("global-uid", "Global")},
			want: trackedLimits{
				"global": trackedLimit("global-uid", global, scopeEvents{"Global": {t1, t2, t3}}),
			},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			tracker := NewPreemptionLimitTracker()
			tracker.limits = tc.limits
			tracker.Record(tc.ts, tc.consumed)
			if diff := cmp.Diff(tc.want, tracker.limits, cmpTrackedLimits...); diff != "" {
				t.Errorf("Unexpected tracked limits (-want,+got):\n%s", diff)
			}
		})
	}
}

func TestPreemptionLimitTrackerRecordConcurrent(t *testing.T) {
	const goroutines, recordsPerGoroutine = 10, 100
	start := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	consumed := policy.ConsumedLimits{"global": consumedLimit("global-uid", "Global")}

	tracker := NewPreemptionLimitTracker()
	tracker.Sync(start, utiltestingalpha.MakePreemptionLimit("global", global).Obj())
	var wg sync.WaitGroup
	for g := range goroutines {
		wg.Go(func() {
			for i := range recordsPerGoroutine {
				tracker.Record(start.Add(time.Duration(g*recordsPerGoroutine+i)*time.Second), consumed)
			}
		})
	}
	wg.Wait()

	got := tracker.limits["global"].events["Global"]
	if len(got) != goroutines*recordsPerGoroutine {
		t.Errorf("Recorded %d events, want %d", len(got), goroutines*recordsPerGoroutine)
	}
	if !slices.IsSortedFunc(got, time.Time.Compare) {
		t.Errorf("Events are not sorted: %v", got)
	}
}

func TestPreemptionLimitTrackerSync(t *testing.T) {
	const windowSeconds = 60
	const syncedLimit, otherLimit = "per-cq", "global"
	now := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	window := windowSeconds * time.Second
	cutoff := now.Add(-window - pruneGracePeriod)
	expired := cutoff.Add(-time.Nanosecond)
	withinGracePeriod := now.Add(-window - pruneGracePeriod/2)
	// prunedOnlyWithHalfWindow is kept with a window of windowSeconds, but pruned
	// once the window is halved, as it is 1s older than the halved window's cutoff.
	prunedOnlyWithHalfWindow := now.Add(-window/2 - pruneGracePeriod - time.Second)
	// limit is the PreemptionLimit passed to Sync, unless updatedLimit is set.
	limit := utiltestingalpha.MakePreemptionLimit(syncedLimit, perCQ).LimitWindowSeconds(windowSeconds).Obj()

	cases := map[string]struct {
		// limits is the state of the tracker before Sync.
		limits trackedLimits
		// updatedLimit, if set, is passed to Sync instead of limit. It is an
		// updated version of the same PreemptionLimit, not a new one.
		updatedLimit *kueuealpha.PreemptionLimit
		// want is the state of the tracker after Sync.
		want trackedLimits
	}{
		"events before the cutoff are dropped, events at or after it are kept": {
			limits: trackedLimits{
				syncedLimit: trackedLimit("per-cq-uid", perCQ, scopeEvents{"cq-a": {cutoff.Add(-time.Hour), expired, cutoff, now}}),
			},
			want: trackedLimits{
				syncedLimit: trackedLimit("per-cq-uid", perCQ, scopeEvents{"cq-a": {cutoff, now}}),
			},
		},
		"events within the grace period are kept": {
			limits: trackedLimits{
				syncedLimit: trackedLimit("per-cq-uid", perCQ, scopeEvents{"cq-a": {withinGracePeriod}}),
			},
			want: trackedLimits{
				syncedLimit: trackedLimit("per-cq-uid", perCQ, scopeEvents{"cq-a": {withinGracePeriod}}),
			},
		},
		"scope value left without events is forgotten": {
			limits: trackedLimits{
				syncedLimit: trackedLimit("per-cq-uid", perCQ, scopeEvents{"cq-a": {expired}, "cq-b": {now}}),
			},
			want: trackedLimits{
				syncedLimit: trackedLimit("per-cq-uid", perCQ, scopeEvents{"cq-b": {now}}),
			},
		},
		"limit left without events is still tracked": {
			limits: trackedLimits{
				syncedLimit: trackedLimit("per-cq-uid", perCQ, scopeEvents{"cq-a": {expired}, "cq-b": {expired}}),
			},
			want: trackedLimits{
				syncedLimit: trackedLimit("per-cq-uid", perCQ, nil),
			},
		},
		"untracked limit starts being tracked": {
			want: trackedLimits{
				syncedLimit: trackedLimit("per-cq-uid", perCQ, nil),
			},
		},
		"other limits are untouched": {
			limits: trackedLimits{
				syncedLimit: trackedLimit("per-cq-uid", perCQ, scopeEvents{"cq-a": {expired, now}}),
				otherLimit:  trackedLimit("global-uid", global, scopeEvents{"Global": {expired}}),
			},
			want: trackedLimits{
				syncedLimit: trackedLimit("per-cq-uid", perCQ, scopeEvents{"cq-a": {now}}),
				otherLimit:  trackedLimit("global-uid", global, scopeEvents{"Global": {expired}}),
			},
		},
		"limit and window increase keep the events and the reset generation": {
			limits: trackedLimits{
				syncedLimit: withResetGeneration(trackedLimit("per-cq-uid", perCQ, scopeEvents{"cq-a": {expired, now}}), 1),
			},
			updatedLimit: utiltestingalpha.MakePreemptionLimit(syncedLimit, perCQ).Generation(2).Limit(10).LimitWindowSeconds(2 * windowSeconds).Obj(),
			want: trackedLimits{
				syncedLimit: withResetGeneration(trackedLimit("per-cq-uid", perCQ, scopeEvents{"cq-a": {expired, now}}), 1),
			},
		},
		"window decrease keeps the events within the new window": {
			limits: trackedLimits{
				syncedLimit: trackedLimit("per-cq-uid", perCQ, scopeEvents{"cq-a": {prunedOnlyWithHalfWindow, now}}),
			},
			updatedLimit: utiltestingalpha.MakePreemptionLimit(syncedLimit, perCQ).LimitWindowSeconds(windowSeconds / 2).Obj(),
			want: trackedLimits{
				syncedLimit: trackedLimit("per-cq-uid", perCQ, scopeEvents{"cq-a": {now}}),
			},
		},
		"scope change resets the events and the reset generation": {
			limits: trackedLimits{
				syncedLimit: withResetGeneration(trackedLimit("per-cq-uid", kueuealpha.PreemptedCQLimitScope, scopeEvents{"cq-a": {now}}), 1),
			},
			updatedLimit: utiltestingalpha.MakePreemptionLimit(syncedLimit, perCQ).Generation(2).LimitWindowSeconds(windowSeconds).Obj(),
			want: trackedLimits{
				syncedLimit: withResetGeneration(trackedLimit("per-cq-uid", perCQ, nil), 2),
			},
		},
		"limit recreated with the same name resets the events": {
			limits: trackedLimits{
				syncedLimit: trackedLimit("old-uid", perCQ, scopeEvents{"cq-a": {now}}),
			},
			want: trackedLimits{
				syncedLimit: trackedLimit("per-cq-uid", perCQ, nil),
			},
		},
		"untracked limit starts being tracked with its selectors": {
			updatedLimit: utiltestingalpha.MakePreemptionLimit(syncedLimit, perCQ).LimitWindowSeconds(windowSeconds).
				ConfigSelector(selector("tier", "gold")).
				PreemptorClusterQueueSelector(selector("side", "preemptor")).
				PreemptedClusterQueueSelector(selector("side", "preempted")).
				Obj(),
			want: trackedLimits{
				syncedLimit: withSelectors(trackedLimit("per-cq-uid", perCQ, nil),
					selector("tier", "gold"), selector("side", "preemptor"), selector("side", "preempted")),
			},
		},
		"unchanged selectors keep the events": {
			limits: trackedLimits{
				syncedLimit: withSelectors(trackedLimit("per-cq-uid", perCQ, scopeEvents{"cq-a": {now}}),
					selector("tier", "gold"), selector("side", "preemptor"), selector("side", "preempted")),
			},
			updatedLimit: utiltestingalpha.MakePreemptionLimit(syncedLimit, perCQ).LimitWindowSeconds(windowSeconds).
				ConfigSelector(selector("tier", "gold")).
				PreemptorClusterQueueSelector(selector("side", "preemptor")).
				PreemptedClusterQueueSelector(selector("side", "preempted")).
				Obj(),
			want: trackedLimits{
				syncedLimit: withSelectors(trackedLimit("per-cq-uid", perCQ, scopeEvents{"cq-a": {now}}),
					selector("tier", "gold"), selector("side", "preemptor"), selector("side", "preempted")),
			},
		},
		"config selector change resets the events": {
			limits: trackedLimits{
				syncedLimit: trackedLimit("per-cq-uid", perCQ, scopeEvents{"cq-a": {now}}),
			},
			updatedLimit: utiltestingalpha.MakePreemptionLimit(syncedLimit, perCQ).LimitWindowSeconds(windowSeconds).
				ConfigSelector(selector("tier", "gold")).
				Obj(),
			want: trackedLimits{
				syncedLimit: withSelectors(trackedLimit("per-cq-uid", perCQ, nil), selector("tier", "gold"), nil, nil),
			},
		},
		"preemptor ClusterQueue selector change resets the events": {
			limits: trackedLimits{
				syncedLimit: withSelectors(trackedLimit("per-cq-uid", perCQ, scopeEvents{"cq-a": {now}}), nil, selector("side", "preemptor"), nil),
			},
			updatedLimit: utiltestingalpha.MakePreemptionLimit(syncedLimit, perCQ).LimitWindowSeconds(windowSeconds).
				PreemptorClusterQueueSelector(selector("side", "other")).
				Obj(),
			want: trackedLimits{
				syncedLimit: withSelectors(trackedLimit("per-cq-uid", perCQ, nil), nil, selector("side", "other"), nil),
			},
		},
		"preempted ClusterQueue selector removal resets the events": {
			limits: trackedLimits{
				syncedLimit: withSelectors(trackedLimit("per-cq-uid", perCQ, scopeEvents{"cq-a": {now}}), nil, nil, selector("side", "preempted")),
			},
			want: trackedLimits{
				syncedLimit: trackedLimit("per-cq-uid", perCQ, nil),
			},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			tracker := NewPreemptionLimitTracker()
			if tc.limits != nil {
				tracker.limits = tc.limits
			}
			limitToSync := limit
			if tc.updatedLimit != nil {
				limitToSync = tc.updatedLimit
			}
			tracker.Sync(now, limitToSync)
			if diff := cmp.Diff(tc.want, tracker.limits, cmpTrackedLimits...); diff != "" {
				t.Errorf("Unexpected tracked limits (-want,+got):\n%s", diff)
			}
		})
	}
}

func TestPreemptionLimitTrackerSyncConcurrent(t *testing.T) {
	const goroutines, recordsPerGoroutine = 10, 100
	const records = goroutines * recordsPerGoroutine
	const windowSeconds = 60
	const syncedLimit, otherLimit = "per-cq", "global"
	start := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	window := windowSeconds * time.Second
	limit := utiltestingalpha.MakePreemptionLimit(syncedLimit, perCQ).LimitWindowSeconds(windowSeconds).Obj()
	consumed := policy.ConsumedLimits{
		syncedLimit: consumedLimit("per-cq-uid", "cq-a"),
		otherLimit:  consumedLimit("global-uid", "Global"),
	}
	// Every recorded event is already expired, so the syncs keep pruning the
	// synced limit while the records keep adding to it.
	now := start.Add(records*time.Second + window + pruneGracePeriod)

	tracker := NewPreemptionLimitTracker()
	tracker.Sync(now, utiltestingalpha.MakePreemptionLimit(otherLimit, global).Obj())
	var wg sync.WaitGroup
	for g := range goroutines {
		wg.Go(func() {
			for i := range recordsPerGoroutine {
				tracker.Sync(now, limit)
				tracker.Record(start.Add(time.Duration(g*recordsPerGoroutine+i)*time.Second), consumed)
			}
		})
	}
	wg.Wait()

	got := tracker.limits[otherLimit].events["Global"]
	if len(got) != records {
		t.Errorf("Recorded %d events of the other limit, want %d", len(got), records)
	}
	if !slices.IsSortedFunc(got, time.Time.Compare) {
		t.Errorf("Events of the other limit are not sorted: %v", got)
	}
}

func TestPreemptionLimitTrackerForget(t *testing.T) {
	const forgottenLimit, otherLimit = "per-cq", "global"
	now := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)

	cases := map[string]struct {
		limits trackedLimits
		want   trackedLimits
	}{
		"only the given limit is forgotten": {
			limits: trackedLimits{
				forgottenLimit: trackedLimit("per-cq-uid", perCQ, scopeEvents{"cq-a": {now}, "cq-b": {now}}),
				otherLimit:     trackedLimit("global-uid", global, scopeEvents{"Global": {now}}),
			},
			want: trackedLimits{
				otherLimit: trackedLimit("global-uid", global, scopeEvents{"Global": {now}}),
			},
		},
		"unknown limit is a no-op": {
			limits: trackedLimits{
				otherLimit: trackedLimit("global-uid", global, scopeEvents{"Global": {now}}),
			},
			want: trackedLimits{
				otherLimit: trackedLimit("global-uid", global, scopeEvents{"Global": {now}}),
			},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			tracker := NewPreemptionLimitTracker()
			tracker.limits = tc.limits
			tracker.Forget(forgottenLimit)
			if diff := cmp.Diff(tc.want, tracker.limits, cmpTrackedLimits...); diff != "" {
				t.Errorf("Unexpected tracked limits (-want,+got):\n%s", diff)
			}
		})
	}
}
