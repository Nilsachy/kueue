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

package core

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	testingclock "k8s.io/utils/clock/testing"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	kueuealpha "sigs.k8s.io/kueue/apis/kueue/v1alpha1"
	"sigs.k8s.io/kueue/pkg/scheduler/preemption/limits"
	"sigs.k8s.io/kueue/pkg/scheduler/preemption/policy"
	utiltesting "sigs.k8s.io/kueue/pkg/util/testing"
	utiltestingalpha "sigs.k8s.io/kueue/pkg/util/testing/v1alpha1"
)

func TestPreemptionLimitReconcile(t *testing.T) {
	now := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	errTest := errors.New("test error")
	limit := utiltestingalpha.MakePreemptionLimit("limit", kueuealpha.PreemptorCQLimitScope).Generation(1).LimitWindowSeconds(60).Obj()
	otherLimit := utiltestingalpha.MakePreemptionLimit("other-limit", kueuealpha.GlobalPreemptionLimitScope).Generation(1).LimitWindowSeconds(60).Obj()
	consumed := func(l *kueuealpha.PreemptionLimit, v policy.PreemptionLimitScopeValue) policy.ConsumedLimit {
		return policy.ConsumedLimit{UID: l.UID, Generation: l.Generation, ScopeValue: v}
	}
	// recorded holds the preemptions recorded before "limit" is reconciled, by timestamp.
	recorded := map[time.Time]policy.ConsumedLimits{
		now.Add(-time.Hour):        {"limit": consumed(limit, "cq-a"), "other-limit": consumed(otherLimit, "Global")},
		now.Add(-30 * time.Second): {"limit": consumed(limit, "cq-a")},
		now:                        {"limit": consumed(limit, "cq-b"), "other-limit": consumed(otherLimit, "Global")},
	}

	cases := map[string]struct {
		// limit is "limit" when it is reconciled, or nil once deleted.
		limit       *kueuealpha.PreemptionLimit
		clientFuncs interceptor.Funcs
		wantResult  reconcile.Result
		wantErr     error
		// wantCounts are the preemptions within the window of "limit".
		wantCounts map[policy.PreemptionLimitScopeValue]int32
	}{
		"unchanged limit keeps its events": {
			limit:      limit,
			wantResult: reconcile.Result{RequeueAfter: time.Minute},
			wantCounts: map[policy.PreemptionLimitScopeValue]int32{"cq-a": 1, "cq-b": 1},
		},
		"limit and window change keep the events": {
			limit:      utiltestingalpha.MakePreemptionLimit("limit", kueuealpha.PreemptorCQLimitScope).Generation(2).Limit(10).LimitWindowSeconds(2 * 3600).Obj(),
			wantResult: reconcile.Result{RequeueAfter: time.Minute},
			wantCounts: map[policy.PreemptionLimitScopeValue]int32{"cq-a": 2, "cq-b": 1},
		},
		"deleted limit is forgotten": {},
		"get error other than NotFound is returned and the events are kept": {
			limit: limit,
			clientFuncs: interceptor.Funcs{
				Get: func(context.Context, client.WithWatch, client.ObjectKey, client.Object, ...client.GetOption) error {
					return errTest
				},
			},
			wantErr:    errTest,
			wantCounts: map[policy.PreemptionLimitScopeValue]int32{"cq-a": 1, "cq-b": 1},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			ctx, _ := utiltesting.ContextWithLog(t)
			tracker := limits.NewPreemptionLimitTracker()
			reconciler := NewPreemptionLimitReconciler(utiltesting.NewClientBuilder().WithObjects(limit.DeepCopy(), otherLimit.DeepCopy()).Build(), tracker, nil)
			reconciler.clock = testingclock.NewFakeClock(now)
			// Preemptions are only recorded for the reconciled PreemptionLimits.
			for _, l := range []*kueuealpha.PreemptionLimit{limit, otherLimit} {
				if _, err := reconciler.Reconcile(ctx, reconcile.Request{Name: l.Name}); err != nil {
					t.Fatalf("Reconcile() of PreemptionLimit %q failed: %v", l.Name, err)
				}
			}
			for ts, consumedLimits := range recorded {
				tracker.Record(ts, consumedLimits)
			}

			builder := utiltesting.NewClientBuilder().WithInterceptorFuncs(tc.clientFuncs)
			if tc.limit != nil {
				builder = builder.WithObjects(tc.limit.DeepCopy())
			}
			reconciler.client = builder.Build()
			gotResult, gotErr := reconciler.Reconcile(ctx, reconcile.Request{Name: "limit"})
			if diff := cmp.Diff(tc.wantErr, gotErr, cmpopts.EquateErrors()); diff != "" {
				t.Errorf("Unexpected Reconcile() error (-want,+got):\n%s", diff)
			}
			if diff := cmp.Diff(tc.wantResult, gotResult); diff != "" {
				t.Errorf("Unexpected Reconcile() result (-want,+got):\n%s", diff)
			}
			// A deleted limit is counted as last seen, to check that its events are gone.
			countedLimit := limit
			if tc.limit != nil {
				countedLimit = tc.limit
			}
			if diff := cmp.Diff(tc.wantCounts, tracker.Counts(now, countedLimit), cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("Unexpected counts of the reconciled limit (-want,+got):\n%s", diff)
			}
			wantOtherCounts := map[policy.PreemptionLimitScopeValue]int32{"Global": 1}
			if diff := cmp.Diff(wantOtherCounts, tracker.Counts(now, otherLimit)); diff != "" {
				t.Errorf("Unexpected counts of the other limit (-want,+got):\n%s", diff)
			}
		})
	}
}
