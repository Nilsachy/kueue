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
	"k8s.io/component-base/featuregate"
	"k8s.io/utils/ptr"

	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	controllerconstants "sigs.k8s.io/kueue/pkg/controller/constants"
	"sigs.k8s.io/kueue/pkg/features"
	utiltestingapi "sigs.k8s.io/kueue/pkg/util/testing/v1beta2"
	"sigs.k8s.io/kueue/pkg/workload"
)

func TestRelativeWorkloadPriorityFilter_Matches(t *testing.T) {
	cases := map[string]struct {
		comparison        kueue.NumericComparison
		preemptorPriority *int32
		candidatePriority *int32
		wantMatch         bool
	}{
		"LessThan: candidate strictly lower matches": {
			comparison:        kueue.LessThan,
			preemptorPriority: ptr.To[int32](100),
			candidatePriority: ptr.To[int32](50),
			wantMatch:         true,
		},
		"LessThan: candidate equal rejected": {
			comparison:        kueue.LessThan,
			preemptorPriority: ptr.To[int32](100),
			candidatePriority: ptr.To[int32](100),
			wantMatch:         false,
		},
		"LessThanOrEqual: candidate strictly lower matches": {
			comparison:        kueue.LessThanOrEqual,
			preemptorPriority: ptr.To[int32](100),
			candidatePriority: ptr.To[int32](50),
			wantMatch:         true,
		},
		"LessThanOrEqual: candidate equal matches": {
			comparison:        kueue.LessThanOrEqual,
			preemptorPriority: ptr.To[int32](100),
			candidatePriority: ptr.To[int32](100),
			wantMatch:         true,
		},
		"LessThanOrEqual: candidate strictly greater rejected": {
			comparison:        kueue.LessThanOrEqual,
			preemptorPriority: ptr.To[int32](100),
			candidatePriority: ptr.To[int32](150),
			wantMatch:         false,
		},
		"GreaterThan: candidate strictly greater matches": {
			comparison:        kueue.GreaterThan,
			preemptorPriority: ptr.To[int32](100),
			candidatePriority: ptr.To[int32](150),
			wantMatch:         true,
		},
		"GreaterThan: candidate equal rejected": {
			comparison:        kueue.GreaterThan,
			preemptorPriority: ptr.To[int32](100),
			candidatePriority: ptr.To[int32](100),
			wantMatch:         false,
		},
		"GreaterThanOrEqual: candidate strictly greater matches": {
			comparison:        kueue.GreaterThanOrEqual,
			preemptorPriority: ptr.To[int32](100),
			candidatePriority: ptr.To[int32](150),
			wantMatch:         true,
		},
		"GreaterThanOrEqual: candidate equal matches": {
			comparison:        kueue.GreaterThanOrEqual,
			preemptorPriority: ptr.To[int32](100),
			candidatePriority: ptr.To[int32](100),
			wantMatch:         true,
		},
		"GreaterThanOrEqual: candidate strictly lower rejected": {
			comparison:        kueue.GreaterThanOrEqual,
			preemptorPriority: ptr.To[int32](100),
			candidatePriority: ptr.To[int32](50),
			wantMatch:         false,
		},
		"Default priority handling: nil preemptor priority defaults to 0 and matches strictly lower candidate": {
			comparison:        kueue.LessThan,
			preemptorPriority: nil,
			candidatePriority: ptr.To[int32](-10),
			wantMatch:         true,
		},
		"Default priority handling: nil candidate priority defaults to 0 and matches when equal": {
			comparison:        kueue.LessThanOrEqual,
			preemptorPriority: ptr.To[int32](0),
			candidatePriority: nil,
			wantMatch:         true,
		},
		"Default priority handling: both nil priorities compare as equal (0 vs 0)": {
			comparison:        kueue.LessThanOrEqual,
			preemptorPriority: nil,
			candidatePriority: nil,
			wantMatch:         true,
		},
		"Negative priorities: candidate -100 is less than preemptor -50": {
			comparison:        kueue.LessThan,
			preemptorPriority: ptr.To[int32](-50),
			candidatePriority: ptr.To[int32](-100),
			wantMatch:         true,
		},
		"Negative priorities: candidate -50 is greater than preemptor -100": {
			comparison:        kueue.GreaterThan,
			preemptorPriority: ptr.To[int32](-100),
			candidatePriority: ptr.To[int32](-50),
			wantMatch:         true,
		},
		"Unknown/unsupported comparison constraint rejects all candidates": {
			comparison:        kueue.NumericComparison("InvalidComparison"),
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

			filter := NewRelativeWorkloadPriorityFilter(logr.Discard(), tc.comparison, preemptor)
			if got := filter.Matches(candidate); got != tc.wantMatch {
				t.Errorf("Matches(candidate) = %v, want %v", got, tc.wantMatch)
			}
		})
	}
}

func TestRelativeWorkloadPriorityFilter_PriorityBoost(t *testing.T) {
	cases := map[string]struct {
		featureGates      map[featuregate.Feature]bool
		comparison        kueue.NumericComparison
		preemptorPriority int32
		preemptorBoost    string
		candidatePriority int32
		candidateBoost    string
		wantMatch         bool
	}{
		"PriorityBoost enabled: candidate boost raises effective priority above preemptor": {
			featureGates:      map[featuregate.Feature]bool{features.PriorityBoost: true},
			comparison:        kueue.GreaterThan,
			preemptorPriority: 50,
			candidatePriority: 10,
			candidateBoost:    "100", // effective priority: 10 + 100 = 110 > 50
			wantMatch:         true,
		},
		"PriorityBoost enabled: preemptor boost raises effective priority above candidate": {
			featureGates:      map[featuregate.Feature]bool{features.PriorityBoost: true},
			comparison:        kueue.LessThan,
			preemptorPriority: 50,
			preemptorBoost:    "100", // effective priority: 50 + 100 = 150 > 120
			candidatePriority: 120,
			wantMatch:         true,
		},
		"PriorityBoost enabled: both workloads boosted with boundary equality": {
			featureGates:      map[featuregate.Feature]bool{features.PriorityBoost: true},
			comparison:        kueue.LessThanOrEqual,
			preemptorPriority: 60,
			preemptorBoost:    "10", // effective priority: 60 + 10 = 70
			candidatePriority: 50,
			candidateBoost:    "20", // effective priority: 50 + 20 = 70 <= 70
			wantMatch:         true,
		},
		"PriorityBoost disabled: boost annotation is ignored and base priority is used": {
			featureGates:      map[featuregate.Feature]bool{features.PriorityBoost: false},
			comparison:        kueue.GreaterThan,
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

			filter := NewRelativeWorkloadPriorityFilter(logr.Discard(), tc.comparison, preemptor)
			if got := filter.Matches(candidate); got != tc.wantMatch {
				t.Errorf("Matches(candidate) = %v, want %v", got, tc.wantMatch)
			}
		})
	}
}
