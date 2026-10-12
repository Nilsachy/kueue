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

	kueuealpha "sigs.k8s.io/kueue/apis/kueue/v1alpha1"
	"sigs.k8s.io/kueue/pkg/scheduler/preemption/policy"
	utiltestingalpha "sigs.k8s.io/kueue/pkg/util/testing/v1alpha1"
	utiltestingapi "sigs.k8s.io/kueue/pkg/util/testing/v1beta2"
	"sigs.k8s.io/kueue/pkg/workload"
)

func TestResolve(t *testing.T) {
	preemptor := &workload.Info{
		Obj:          utiltestingapi.MakeWorkload("preemptor", "ns").Obj(),
		ClusterQueue: "preempting-cq",
	}
	target := &workload.Info{
		Obj:          utiltestingapi.MakeWorkload("target", "ns").Obj(),
		ClusterQueue: "preempted-cq",
	}

	cases := map[string]struct {
		preemptionLimits []kueuealpha.PreemptionLimit
		want             policy.ConsumedLimits
	}{
		"no limits": {},
		"single limit resolves its scope value": {
			preemptionLimits: []kueuealpha.PreemptionLimit{
				*utiltestingalpha.MakePreemptionLimit("preempted-workload", kueuealpha.PreemptedWorkloadLimitScope).Obj(),
			},
			want: policy.ConsumedLimits{"preempted-workload": "ns/target"},
		},
		"each scope resolves its scope value": {
			preemptionLimits: []kueuealpha.PreemptionLimit{
				*utiltestingalpha.MakePreemptionLimit("global", kueuealpha.GlobalPreemptionLimitScope).Obj(),
				*utiltestingalpha.MakePreemptionLimit("preempting-cq", kueuealpha.PreemptingCQLimitScope).Obj(),
				*utiltestingalpha.MakePreemptionLimit("preempted-cq", kueuealpha.PreemptedCQLimitScope).Obj(),
				*utiltestingalpha.MakePreemptionLimit("preempted-workload", kueuealpha.PreemptedWorkloadLimitScope).Obj(),
			},
			want: policy.ConsumedLimits{
				"global":             "Global",
				"preempting-cq":      "preempting-cq",
				"preempted-cq":       "preempted-cq",
				"preempted-workload": "ns/target",
			},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got := Resolve(tc.preemptionLimits, preemptor, target)
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("Unexpected consumed limits (-want,+got):\n%s", diff)
			}
		})
	}
}

func TestPreemptionLimitTrackerRecord(t *testing.T) {
	t1 := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	t2 := t1.Add(time.Second)
	t3 := t2.Add(time.Second)

	cases := map[string]struct {
		events   map[policy.PreemptionLimitReference]map[policy.PreemptionLimitScopeValue]events
		ts       time.Time
		consumed policy.ConsumedLimits
		want     map[policy.PreemptionLimitReference]map[policy.PreemptionLimitScopeValue]events
	}{
		"nil consumed limits is a no-op": {
			events: map[policy.PreemptionLimitReference]map[policy.PreemptionLimitScopeValue]events{
				"global": {"Global": {t1}},
			},
			ts: t2,
			want: map[policy.PreemptionLimitReference]map[policy.PreemptionLimitScopeValue]events{
				"global": {"Global": {t1}},
			},
		},
		"every consumed limit is recorded": {
			ts:       t1,
			consumed: policy.ConsumedLimits{"global": "Global", "per-cq": "cq-a"},
			want: map[policy.PreemptionLimitReference]map[policy.PreemptionLimitScopeValue]events{
				"global": {"Global": {t1}},
				"per-cq": {"cq-a": {t1}},
			},
		},
		"new scope value of a recorded limit": {
			events: map[policy.PreemptionLimitReference]map[policy.PreemptionLimitScopeValue]events{
				"per-cq": {"cq-a": {t1}},
			},
			ts:       t2,
			consumed: policy.ConsumedLimits{"per-cq": "cq-b"},
			want: map[policy.PreemptionLimitReference]map[policy.PreemptionLimitScopeValue]events{
				"per-cq": {"cq-a": {t1}, "cq-b": {t2}},
			},
		},
		"later event is appended": {
			events: map[policy.PreemptionLimitReference]map[policy.PreemptionLimitScopeValue]events{
				"global": {"Global": {t1, t2}},
			},
			ts:       t3,
			consumed: policy.ConsumedLimits{"global": "Global"},
			want: map[policy.PreemptionLimitReference]map[policy.PreemptionLimitScopeValue]events{
				"global": {"Global": {t1, t2, t3}},
			},
		},
		"earlier event is inserted in order": {
			events: map[policy.PreemptionLimitReference]map[policy.PreemptionLimitScopeValue]events{
				"global": {"Global": {t1, t3}},
			},
			ts:       t2,
			consumed: policy.ConsumedLimits{"global": "Global"},
			want: map[policy.PreemptionLimitReference]map[policy.PreemptionLimitScopeValue]events{
				"global": {"Global": {t1, t2, t3}},
			},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			tracker := NewPreemptionLimitTracker()
			if tc.events != nil {
				tracker.events = tc.events
			}
			tracker.Record(tc.ts, tc.consumed)
			if diff := cmp.Diff(tc.want, tracker.events, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("Unexpected events (-want,+got):\n%s", diff)
			}
		})
	}
}

func TestPreemptionLimitTrackerRecordConcurrent(t *testing.T) {
	const goroutines, recordsPerGoroutine = 10, 100
	start := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	consumed := policy.ConsumedLimits{"global": "Global"}

	tracker := NewPreemptionLimitTracker()
	var wg sync.WaitGroup
	for g := range goroutines {
		wg.Go(func() {
			for i := range recordsPerGoroutine {
				tracker.Record(start.Add(time.Duration(g*recordsPerGoroutine+i)*time.Second), consumed)
			}
		})
	}
	wg.Wait()

	got := tracker.events["global"]["Global"]
	if len(got) != goroutines*recordsPerGoroutine {
		t.Errorf("Recorded %d events, want %d", len(got), goroutines*recordsPerGoroutine)
	}
	if !slices.IsSortedFunc(got, time.Time.Compare) {
		t.Errorf("Events are not sorted: %v", got)
	}
}
