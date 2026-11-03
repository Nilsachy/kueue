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
	"cmp"
	"testing"
	"time"

	kueuealpha "sigs.k8s.io/kueue/apis/kueue/v1alpha1"
	"sigs.k8s.io/kueue/pkg/scheduler/preemption/policy"
	utiltestingalpha "sigs.k8s.io/kueue/pkg/util/testing/v1alpha1"
)

func TestPreemptionLimitsSnapshotAllowance(t *testing.T) {
	now := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	perCQLimit := utiltestingalpha.MakePreemptionLimit("per-cq", kueuealpha.PreemptedCQLimitScope).
		Limit(3).LimitWindowSeconds(60).Obj()
	perCQRef := policy.PreemptionLimitReference(perCQLimit.Name)

	cases := map[string]struct {
		limit  policy.PreemptionLimitReference
		limits trackedLimits
		// changeTracker, when set, changes the tracker after the snapshot is taken.
		changeTracker func(*PreemptionLimitTracker)
		want          int32
	}{
		"allowance is the limit minus the preemptions within the window": {
			limits: tracked(trackedLimit(perCQLimit, scopeEvents{"cq-a": {now.Add(-2 * time.Minute), now.Add(-30 * time.Second)}})),
			want:   2,
		},
		"allowance returns the current limit for the proper scope value": {
			limits: tracked(trackedLimit(perCQLimit, scopeEvents{"cq-b": {now, now}})),
			want:   3,
		},
		"limit missing from the snapshot has 0 allowance": {
			limits: tracked(trackedLimit(perCQLimit, nil)),
			limit:  "missing",
			want:   0,
		},
		"allowance doesn't change when the tracker forgets the limit after the snapshot is taken": {
			limits:        tracked(trackedLimit(perCQLimit, scopeEvents{"cq-a": {now, now}})),
			changeTracker: func(tracker *PreemptionLimitTracker) { tracker.Forget(perCQRef) },
			want:          1,
		},
		"allowance doesn't change when the tracker prunes the limit after the snapshot is taken": {
			limits: tracked(trackedLimit(perCQLimit, scopeEvents{"cq-a": {now.Add(-50 * time.Second), now}})),
			changeTracker: func(tracker *PreemptionLimitTracker) {
				// Drops all the events, as a prune with a shrunk window would.
				shrunk := perCQLimit.DeepCopy()
				shrunk.Spec.LimitWindowSeconds = 1
				tracker.Sync(now.Add(time.Hour), shrunk)
			},
			want: 1,
		},
		"allowance doesn't change when the tracker records preemptions after the snapshot is taken": {
			limits: tracked(trackedLimit(perCQLimit, nil)),
			changeTracker: func(tracker *PreemptionLimitTracker) {
				tracker.Record(now, policy.ConsumedLimits{perCQRef: policy.NewConsumedLimit(perCQLimit, "cq-a")})
			},
			want: 3,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			tracker := NewPreemptionLimitTracker()
			if tc.limits != nil {
				tracker.limits = tc.limits
			}
			snapshot := tracker.Snapshot(now)
			if tc.changeTracker != nil {
				tc.changeTracker(tracker)
			}
			limit := cmp.Or(tc.limit, perCQRef)
			if got := snapshot.allowance(limit, "cq-a"); got != tc.want {
				t.Errorf("allowance() = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestPreemptionLimitsSnapshotRecord(t *testing.T) {
	now := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	globalLimit := utiltestingalpha.MakePreemptionLimit("global", global).
		Limit(5).LimitWindowSeconds(60).Obj()
	perCQLimit := utiltestingalpha.MakePreemptionLimit("per-cq", kueuealpha.PreemptedCQLimitScope).
		Limit(3).LimitWindowSeconds(60).Obj()
	globalConsumed := policy.NewConsumedLimit(globalLimit, "Global")
	perCQConsumed := func(cq policy.PreemptionLimitScopeValue) policy.ConsumedLimit {
		return policy.NewConsumedLimit(perCQLimit, cq)
	}

	cases := map[string]struct {
		limits trackedLimits
		// recorded are the preemptions recorded in the snapshot after it was taken.
		recorded      []policy.ConsumedLimits
		wantAllowance map[policy.PreemptionLimitReference]map[policy.PreemptionLimitScopeValue]int32
	}{
		"preemptions recorded in the snapshot are counted in allowance along with tracker events": {
			limits: tracked(
				trackedLimit(globalLimit, nil),
				trackedLimit(perCQLimit, scopeEvents{"cq-a": {now.Add(-30 * time.Second)}}),
			),
			recorded: []policy.ConsumedLimits{{"per-cq": perCQConsumed("cq-a")}},
			wantAllowance: map[policy.PreemptionLimitReference]map[policy.PreemptionLimitScopeValue]int32{
				"per-cq": {"cq-a": 1},
			},
		},
		"preemptions recorded in the snapshot are counted only for their scope value": {
			limits:   tracked(trackedLimit(globalLimit, nil), trackedLimit(perCQLimit, nil)),
			recorded: []policy.ConsumedLimits{{"per-cq": perCQConsumed("cq-a")}},
			wantAllowance: map[policy.PreemptionLimitReference]map[policy.PreemptionLimitScopeValue]int32{
				"per-cq": {"cq-b": 3},
			},
		},
		"multiple consumed limits are all counted in allowance": {
			limits: tracked(
				trackedLimit(globalLimit, scopeEvents{"Global": {now.Add(-30 * time.Second)}}),
				trackedLimit(perCQLimit, nil),
			),
			recorded: []policy.ConsumedLimits{
				{"global": globalConsumed, "per-cq": perCQConsumed("cq-a")},
				{"global": globalConsumed, "per-cq": perCQConsumed("cq-b")},
			},
			wantAllowance: map[policy.PreemptionLimitReference]map[policy.PreemptionLimitScopeValue]int32{
				"global": {"Global": 2},
				"per-cq": {"cq-a": 2, "cq-b": 2},
			},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			snapshot := snapshotOf(now, tc.limits)
			for _, consumed := range tc.recorded {
				snapshot.Record(consumed)
			}
			for limit, scopes := range tc.wantAllowance {
				for scope, want := range scopes {
					if got := snapshot.allowance(limit, scope); got != want {
						t.Errorf("allowance(%q, %q) = %d, want %d", limit, scope, got, want)
					}
				}
			}
		})
	}
}

func TestPreemptionLimitsSnapshotWithinLimits(t *testing.T) {
	now := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	perCQLimit := utiltestingalpha.MakePreemptionLimit("per-cq", kueuealpha.PreemptedCQLimitScope).
		Limit(2).LimitWindowSeconds(60).Obj()
	globalLimit := utiltestingalpha.MakePreemptionLimit("global", global).
		Limit(2).LimitWindowSeconds(60).Obj()
	missingLimit := utiltestingalpha.MakePreemptionLimit("missing", global).
		Limit(2).LimitWindowSeconds(60).Obj()
	globalConsumed := policy.NewConsumedLimit(globalLimit, "Global")
	perCQConsumed := func(cq policy.PreemptionLimitScopeValue) policy.ConsumedLimit {
		return policy.NewConsumedLimit(perCQLimit, cq)
	}

	cases := map[string]struct {
		limits trackedLimits
		// recorded are the preemptions recorded in the snapshot after it was taken.
		recorded         []policy.ConsumedLimits
		nilSnapshot      bool
		preemptions      []policy.ConsumedLimits
		wantWithinLimits bool
	}{
		"preemptions within the remaining limit are allowed": {
			limits: tracked(
				trackedLimit(perCQLimit, scopeEvents{"cq-a": {now.Add(-30 * time.Second)}}),
				trackedLimit(globalLimit, nil),
			),
			preemptions:      []policy.ConsumedLimits{{"per-cq": perCQConsumed("cq-a")}},
			wantWithinLimits: true,
		},
		"preemptions together exceeding the remaining limit are blocked": {
			limits: tracked(
				trackedLimit(perCQLimit, scopeEvents{"cq-a": {now.Add(-30 * time.Second)}}),
				trackedLimit(globalLimit, nil),
			),
			preemptions:      []policy.ConsumedLimits{{"per-cq": perCQConsumed("cq-a")}, {"per-cq": perCQConsumed("cq-a")}},
			wantWithinLimits: false,
		},
		"preemptions recorded in the snapshot are counted against the limit": {
			recorded:         []policy.ConsumedLimits{{"per-cq": perCQConsumed("cq-a")}, {"per-cq": perCQConsumed("cq-a")}},
			preemptions:      []policy.ConsumedLimits{{"per-cq": perCQConsumed("cq-a")}},
			wantWithinLimits: false,
		},
		"different scope values are counted independently": {
			recorded:         []policy.ConsumedLimits{{"per-cq": perCQConsumed("cq-a")}},
			preemptions:      []policy.ConsumedLimits{{"per-cq": perCQConsumed("cq-b")}, {"per-cq": perCQConsumed("cq-b")}, {"per-cq": perCQConsumed("cq-a")}},
			wantWithinLimits: true,
		},
		"preemption is blocked when any of its consumed limits is exhausted": {
			recorded:         []policy.ConsumedLimits{{"global": globalConsumed}, {"global": globalConsumed}},
			preemptions:      []policy.ConsumedLimits{{"per-cq": perCQConsumed("cq-a"), "global": globalConsumed}},
			wantWithinLimits: false,
		},
		"preemption consuming a limit missing from the snapshot is blocked": {
			preemptions:      []policy.ConsumedLimits{{"missing": policy.NewConsumedLimit(missingLimit, "Global")}},
			wantWithinLimits: false,
		},
		"preemption is blocked when the snapshot is nil": {
			nilSnapshot:      true,
			preemptions:      []policy.ConsumedLimits{{"per-cq": perCQConsumed("cq-a")}},
			wantWithinLimits: false,
		},
		"preemption consuming no limits is blocked when the snapshot is nil": {
			nilSnapshot:      true,
			preemptions:      []policy.ConsumedLimits{{}},
			wantWithinLimits: false,
		},
		// True when there are no preemptions to block.
		"zero preemptions are allowed when the snapshot is nil": {
			nilSnapshot:      true,
			wantWithinLimits: true,
		},
		"zero preemptions are allowed": {
			wantWithinLimits: true,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			var snapshot *PreemptionLimitsSnapshot
			if !tc.nilSnapshot {
				limits := tc.limits
				if limits == nil {
					limits = tracked(trackedLimit(perCQLimit, nil), trackedLimit(globalLimit, nil))
				}
				snapshot = snapshotOf(now, limits)
				for _, consumed := range tc.recorded {
					snapshot.Record(consumed)
				}
			}
			if got := snapshot.WithinLimits(tc.preemptions); got != tc.wantWithinLimits {
				t.Errorf("WithinLimits(%v) = %v, want %v", tc.preemptions, got, tc.wantWithinLimits)
			}
		})
	}
}
