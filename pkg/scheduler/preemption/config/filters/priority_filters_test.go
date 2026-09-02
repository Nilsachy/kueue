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

package filters

import (
	"testing"

	"github.com/go-logr/logr"
	schedulingv1 "k8s.io/api/scheduling/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/component-base/featuregate"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	controllerconstants "sigs.k8s.io/kueue/pkg/controller/constants"
	"sigs.k8s.io/kueue/pkg/features"
	utiltesting "sigs.k8s.io/kueue/pkg/util/testing"
	utiltestingapi "sigs.k8s.io/kueue/pkg/util/testing/v1beta2"
	"sigs.k8s.io/kueue/pkg/workload"
)

func TestCheckPreemptingWorkloadPriority(t *testing.T) {
	ctx := t.Context()
	defaultClient := utiltesting.NewFakeClient(
		utiltestingapi.MakeWorkloadPriorityClass("wpc-critical").Label("tier", "critical-training").Label("preemptible", "false").Obj(),
		utiltestingapi.MakeWorkloadPriorityClass("wpc-batch").Label("tier", "batch").Label("preemptible", "true").Obj(),
		utiltestingapi.MakeWorkloadPriorityClass("wpc-no-labels").Obj(),
	)

	cases := map[string]struct {
		selector     *metav1.LabelSelector
		preemptor    *workload.Info
		customClient client.Reader
		want         bool
	}{
		"nil selector returns true": {
			selector:  nil,
			preemptor: workload.NewInfo(utiltestingapi.MakeWorkload("p", "ns").WorkloadPriorityClassRef("wpc-critical").Obj()),
			want:      true,
		},
		"empty selector returns true": {
			selector:  &metav1.LabelSelector{},
			preemptor: workload.NewInfo(utiltestingapi.MakeWorkload("p", "ns").Obj()),
			want:      true,
		},
		"matching preemptor priority class returns true": {
			selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"tier": "critical-training"},
			},
			preemptor: workload.NewInfo(utiltestingapi.MakeWorkload("p", "ns").WorkloadPriorityClassRef("wpc-critical").Obj()),
			want:      true,
		},
		"non-matching preemptor priority class returns false": {
			selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"tier": "critical-training"},
			},
			preemptor: workload.NewInfo(utiltestingapi.MakeWorkload("p", "ns").WorkloadPriorityClassRef("wpc-batch").Obj()),
			want:      false,
		},
		"zero-label preemptor priority class matches negative/inverted selector": {
			selector: &metav1.LabelSelector{
				MatchExpressions: []metav1.LabelSelectorRequirement{
					{Key: "tier", Operator: metav1.LabelSelectorOpDoesNotExist},
				},
			},
			preemptor: workload.NewInfo(utiltestingapi.MakeWorkload("p", "ns").WorkloadPriorityClassRef("wpc-no-labels").Obj()),
			want:      true,
		},
		"labeled preemptor priority class does not match negative/inverted selector": {
			selector: &metav1.LabelSelector{
				MatchExpressions: []metav1.LabelSelectorRequirement{
					{Key: "tier", Operator: metav1.LabelSelectorOpDoesNotExist},
				},
			},
			preemptor: workload.NewInfo(utiltestingapi.MakeWorkload("p", "ns").WorkloadPriorityClassRef("wpc-critical").Obj()),
			want:      false,
		},
		"zero-label preemptor priority class does not match positive key selector": {
			selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"tier": "critical-training"},
			},
			preemptor: workload.NewInfo(utiltestingapi.MakeWorkload("p", "ns").WorkloadPriorityClassRef("wpc-no-labels").Obj()),
			want:      false,
		},
		"missing preemptor priority class in client returns false": {
			selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"tier": "critical-training"},
			},
			preemptor: workload.NewInfo(utiltestingapi.MakeWorkload("p", "ns").WorkloadPriorityClassRef("non-existent").Obj()),
			want:      false,
		},
		"preemptor with nil PriorityClassRef returns false": {
			selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"tier": "critical-training"},
			},
			preemptor: workload.NewInfo(utiltestingapi.MakeWorkload("p", "ns").Obj()),
			want:      false,
		},
		"nil client returns false for non-empty selector": {
			selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"tier": "critical-training"},
			},
			preemptor:    workload.NewInfo(utiltestingapi.MakeWorkload("p", "ns").WorkloadPriorityClassRef("wpc-critical").Obj()),
			customClient: nil,
			want:         false,
		},
		"invalid label selector returns false": {
			selector: &metav1.LabelSelector{
				MatchExpressions: []metav1.LabelSelectorRequirement{
					{Key: "invalid!", Operator: "InvalidOp"},
				},
			},
			preemptor: workload.NewInfo(utiltestingapi.MakeWorkload("p", "ns").WorkloadPriorityClassRef("wpc-critical").Obj()),
			want:      false,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			var cl client.Reader = defaultClient
			if tc.customClient != nil || name == "nil client returns false for non-empty selector" {
				cl = tc.customClient
			}
			got := CheckPreemptingWorkloadPriority(ctx, logr.Discard(), tc.selector, tc.preemptor, cl)
			if got != tc.want {
				t.Errorf("CheckPreemptingWorkloadPriority() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestCandidateWorkloadPriorityFilter_Matches(t *testing.T) {
	ctx := t.Context()
	defaultClient := utiltesting.NewFakeClient(
		utiltestingapi.MakeWorkloadPriorityClass("wpc-batch").Label("tier", "batch").Label("preemptible", "true").Obj(),
		utiltestingapi.MakeWorkloadPriorityClass("wpc-critical").Label("tier", "critical").Label("preemptible", "false").Obj(),
		utiltestingapi.MakeWorkloadPriorityClass("wpc-no-labels").Obj(),
		&schedulingv1.PriorityClass{ObjectMeta: metav1.ObjectMeta{Name: "pc-low", Labels: map[string]string{"priority-tier": "low"}}},
	)

	parseSelector := func(ls *metav1.LabelSelector) labels.Selector {
		if ls == nil {
			return nil
		}
		sel, err := metav1.LabelSelectorAsSelector(ls)
		if err != nil {
			t.Fatalf("failed to parse selector: %v", err)
		}
		return sel
	}

	cases := map[string]struct {
		candidate    *workload.Info
		selector     labels.Selector
		customClient client.Reader
		wantMatch    bool
	}{
		"matching WorkloadPriorityClass returns true": {
			candidate: workload.NewInfo(utiltestingapi.MakeWorkload("wl", "ns").WorkloadPriorityClassRef("wpc-batch").Obj()),
			selector:  parseSelector(&metav1.LabelSelector{MatchLabels: map[string]string{"preemptible": "true"}}),
			wantMatch: true,
		},
		"non-matching WorkloadPriorityClass returns false": {
			candidate: workload.NewInfo(utiltestingapi.MakeWorkload("wl", "ns").WorkloadPriorityClassRef("wpc-critical").Obj()),
			selector:  parseSelector(&metav1.LabelSelector{MatchLabels: map[string]string{"preemptible": "true"}}),
			wantMatch: false,
		},
		"matching standard PriorityClass returns true": {
			candidate: workload.NewInfo(utiltestingapi.MakeWorkload("wl", "ns").PodPriorityClassRef("pc-low").Obj()),
			selector:  parseSelector(&metav1.LabelSelector{MatchLabels: map[string]string{"priority-tier": "low"}}),
			wantMatch: true,
		},
		"zero-label candidate priority class matches negative/inverted selector": {
			candidate: workload.NewInfo(utiltestingapi.MakeWorkload("wl", "ns").WorkloadPriorityClassRef("wpc-no-labels").Obj()),
			selector: parseSelector(&metav1.LabelSelector{
				MatchExpressions: []metav1.LabelSelectorRequirement{
					{Key: "preemptible", Operator: metav1.LabelSelectorOpDoesNotExist},
				},
			}),
			wantMatch: true,
		},
		"zero-label candidate priority class does not match positive key selector": {
			candidate: workload.NewInfo(utiltestingapi.MakeWorkload("wl", "ns").WorkloadPriorityClassRef("wpc-no-labels").Obj()),
			selector:  parseSelector(&metav1.LabelSelector{MatchLabels: map[string]string{"preemptible": "true"}}),
			wantMatch: false,
		},
		"missing WorkloadPriorityClass in client returns false": {
			candidate: workload.NewInfo(utiltestingapi.MakeWorkload("wl", "ns").WorkloadPriorityClassRef("wpc-nonexistent").Obj()),
			selector:  parseSelector(&metav1.LabelSelector{MatchLabels: map[string]string{"preemptible": "true"}}),
			wantMatch: false,
		},
		"candidate with nil PriorityClassRef returns false": {
			candidate: workload.NewInfo(utiltestingapi.MakeWorkload("wl", "ns").Obj()),
			selector:  parseSelector(&metav1.LabelSelector{MatchLabels: map[string]string{"preemptible": "true"}}),
			wantMatch: false,
		},
		"nil selector matches candidate without PC": {
			candidate: workload.NewInfo(utiltestingapi.MakeWorkload("wl", "ns").Obj()),
			selector:  nil,
			wantMatch: true,
		},
		"empty selector matches candidate without PC": {
			candidate: workload.NewInfo(utiltestingapi.MakeWorkload("wl", "ns").Obj()),
			selector:  labels.Everything(),
			wantMatch: true,
		},
		"nil client returns false for non-empty selector": {
			candidate:    workload.NewInfo(utiltestingapi.MakeWorkload("wl", "ns").WorkloadPriorityClassRef("wpc-batch").Obj()),
			selector:     parseSelector(&metav1.LabelSelector{MatchLabels: map[string]string{"preemptible": "true"}}),
			customClient: nil,
			wantMatch:    false,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			var cl client.Reader = defaultClient
			if tc.customClient != nil || name == "nil client returns false for non-empty selector" {
				cl = tc.customClient
			}
			filter := NewCandidateWorkloadPriorityFilter(ctx, logr.Discard(), tc.selector, cl)
			if got := filter.Matches(tc.candidate); got != tc.wantMatch {
				t.Errorf("Matches(candidate) = %v, want %v", got, tc.wantMatch)
			}
		})
	}
}

func TestRelativeWorkloadPriorityFilter_Matches(t *testing.T) {
	cases := map[string]struct {
		relation          kueue.RelativeConstraint
		preemptorPriority *int32
		candidatePriority *int32
		wantMatch         bool
	}{
		"Lower: candidate strictly lower matches": {
			relation:          kueue.Lower,
			preemptorPriority: ptr.To[int32](100),
			candidatePriority: ptr.To[int32](50),
			wantMatch:         true,
		},
		"Lower: candidate equal rejected": {
			relation:          kueue.Lower,
			preemptorPriority: ptr.To[int32](100),
			candidatePriority: ptr.To[int32](100),
			wantMatch:         false,
		},
		"LowerOrEqual: candidate strictly lower matches": {
			relation:          kueue.LowerOrEqual,
			preemptorPriority: ptr.To[int32](100),
			candidatePriority: ptr.To[int32](50),
			wantMatch:         true,
		},
		"LowerOrEqual: candidate equal matches": {
			relation:          kueue.LowerOrEqual,
			preemptorPriority: ptr.To[int32](100),
			candidatePriority: ptr.To[int32](100),
			wantMatch:         true,
		},
		"LowerOrEqual: candidate strictly greater rejected": {
			relation:          kueue.LowerOrEqual,
			preemptorPriority: ptr.To[int32](100),
			candidatePriority: ptr.To[int32](150),
			wantMatch:         false,
		},
		"Greater: candidate strictly greater matches": {
			relation:          kueue.Greater,
			preemptorPriority: ptr.To[int32](100),
			candidatePriority: ptr.To[int32](150),
			wantMatch:         true,
		},
		"Greater: candidate equal rejected": {
			relation:          kueue.Greater,
			preemptorPriority: ptr.To[int32](100),
			candidatePriority: ptr.To[int32](100),
			wantMatch:         false,
		},
		"GreaterOrEqual: candidate strictly greater matches": {
			relation:          kueue.GreaterOrEqual,
			preemptorPriority: ptr.To[int32](100),
			candidatePriority: ptr.To[int32](150),
			wantMatch:         true,
		},
		"GreaterOrEqual: candidate equal matches": {
			relation:          kueue.GreaterOrEqual,
			preemptorPriority: ptr.To[int32](100),
			candidatePriority: ptr.To[int32](100),
			wantMatch:         true,
		},
		"GreaterOrEqual: candidate strictly lower rejected": {
			relation:          kueue.GreaterOrEqual,
			preemptorPriority: ptr.To[int32](100),
			candidatePriority: ptr.To[int32](50),
			wantMatch:         false,
		},
		"Default priority handling: nil preemptor priority defaults to 0 and matches strictly lower candidate": {
			relation:          kueue.Lower,
			preemptorPriority: nil,
			candidatePriority: ptr.To[int32](-10),
			wantMatch:         true,
		},
		"Default priority handling: nil candidate priority defaults to 0 and matches when equal": {
			relation:          kueue.LowerOrEqual,
			preemptorPriority: ptr.To[int32](0),
			candidatePriority: nil,
			wantMatch:         true,
		},
		"Default priority handling: both nil priorities compare as equal (0 vs 0)": {
			relation:          kueue.LowerOrEqual,
			preemptorPriority: nil,
			candidatePriority: nil,
			wantMatch:         true,
		},
		"Negative priorities: candidate -100 is Lower than preemptor -50": {
			relation:          kueue.Lower,
			preemptorPriority: ptr.To[int32](-50),
			candidatePriority: ptr.To[int32](-100),
			wantMatch:         true,
		},
		"Negative priorities: candidate -50 is Greater than preemptor -100": {
			relation:          kueue.Greater,
			preemptorPriority: ptr.To[int32](-100),
			candidatePriority: ptr.To[int32](-50),
			wantMatch:         true,
		},
		"Unknown/unsupported relation constraint rejects all candidates": {
			relation:          kueue.RelativeConstraint("InvalidRelation"),
			preemptorPriority: ptr.To[int32](100),
			candidatePriority: ptr.To[int32](50),
			wantMatch:         false,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			preemptorBuilder := utiltestingapi.MakeWorkload("preemptor", "ns")
			if tc.preemptorPriority != nil {
				preemptorBuilder = preemptorBuilder.Priority(*tc.preemptorPriority)
			}
			preemptor := workload.NewInfo(preemptorBuilder.Obj())

			candBuilder := utiltestingapi.MakeWorkload("candidate", "ns")
			if tc.candidatePriority != nil {
				candBuilder = candBuilder.Priority(*tc.candidatePriority)
			}
			candidate := workload.NewInfo(candBuilder.Obj())

			filter := NewRelativeWorkloadPriorityFilter(logr.Discard(), tc.relation, preemptor)
			if got := filter.Matches(candidate); got != tc.wantMatch {
				t.Errorf("Matches(candidate) = %v, want %v", got, tc.wantMatch)
			}
		})
	}
}

func TestRelativeWorkloadPriorityFilter_PriorityBoost(t *testing.T) {
	cases := map[string]struct {
		featureGates      map[featuregate.Feature]bool
		relation          kueue.RelativeConstraint
		preemptorPriority int32
		preemptorBoost    string
		candidatePriority int32
		candidateBoost    string
		wantMatch         bool
	}{
		"PriorityBoost enabled: candidate boost raises effective priority above preemptor": {
			featureGates:      map[featuregate.Feature]bool{features.PriorityBoost: true},
			relation:          kueue.Greater,
			preemptorPriority: 50,
			candidatePriority: 10,
			candidateBoost:    "100", // effective priority: 10 + 100 = 110 > 50
			wantMatch:         true,
		},
		"PriorityBoost enabled: preemptor boost raises effective priority above candidate": {
			featureGates:      map[featuregate.Feature]bool{features.PriorityBoost: true},
			relation:          kueue.Lower,
			preemptorPriority: 50,
			preemptorBoost:    "100", // effective priority: 50 + 100 = 150 > 120
			candidatePriority: 120,
			wantMatch:         true,
		},
		"PriorityBoost enabled: both workloads boosted with boundary equality": {
			featureGates:      map[featuregate.Feature]bool{features.PriorityBoost: true},
			relation:          kueue.LowerOrEqual,
			preemptorPriority: 60,
			preemptorBoost:    "10", // effective priority: 60 + 10 = 70
			candidatePriority: 50,
			candidateBoost:    "20", // effective priority: 50 + 20 = 70 <= 70
			wantMatch:         true,
		},
		"PriorityBoost disabled: boost annotation is ignored and base priority is used": {
			featureGates:      map[featuregate.Feature]bool{features.PriorityBoost: false},
			relation:          kueue.Greater,
			preemptorPriority: 50,
			candidatePriority: 10,
			candidateBoost:    "100", // ignored -> base priority is 10 (not > 50)
			wantMatch:         false,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			features.SetFeatureGatesDuringTest(t, tc.featureGates)

			preemptorBuilder := utiltestingapi.MakeWorkload("preemptor", "ns").Priority(tc.preemptorPriority)
			if tc.preemptorBoost != "" {
				preemptorBuilder = preemptorBuilder.Annotation(controllerconstants.PriorityBoostAnnotationKey, tc.preemptorBoost)
			}
			preemptor := workload.NewInfo(preemptorBuilder.Obj())

			candBuilder := utiltestingapi.MakeWorkload("candidate", "ns").Priority(tc.candidatePriority)
			if tc.candidateBoost != "" {
				candBuilder = candBuilder.Annotation(controllerconstants.PriorityBoostAnnotationKey, tc.candidateBoost)
			}
			candidate := workload.NewInfo(candBuilder.Obj())

			filter := NewRelativeWorkloadPriorityFilter(logr.Discard(), tc.relation, preemptor)
			if got := filter.Matches(candidate); got != tc.wantMatch {
				t.Errorf("Matches(candidate) = %v, want %v", got, tc.wantMatch)
			}
		})
	}
}
