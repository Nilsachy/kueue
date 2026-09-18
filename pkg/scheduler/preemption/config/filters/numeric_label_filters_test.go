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
	"k8s.io/utils/ptr"

	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	utiltestingapi "sigs.k8s.io/kueue/pkg/util/testing/v1beta2"
	"sigs.k8s.io/kueue/pkg/workload"
)

func wlWithLabels(labels map[string]string) *workload.Info {
	w := utiltestingapi.MakeWorkload("wl", "")
	if len(labels) > 0 {
		w.Labels(labels)
	}
	return workload.NewInfo(w.Obj())
}

func TestNumericLabelFilterMatches(t *testing.T) {
	cases := map[string]struct {
		constraint kueue.PreemptionConfigNumericLabelConstraint
		preemptor  *workload.Info
		candidate  *workload.Info
		wantMatch  bool
	}{
		// 1. Numeric comparisons (LessThanOrEqual, LessThan, GreaterThan, GreaterThanOrEqual)
		"LessThanOrEqual: candidate strictly smaller than preemptor matches": {
			constraint: kueue.PreemptionConfigNumericLabelConstraint{
				Key:        "size",
				Comparison: ptr.To(kueue.LessThanOrEqual),
			},
			preemptor: wlWithLabels(map[string]string{"size": "8"}),
			candidate: wlWithLabels(map[string]string{"size": "4"}),
			wantMatch: true,
		},
		"LessThanOrEqual: candidate equal to preemptor matches": {
			constraint: kueue.PreemptionConfigNumericLabelConstraint{
				Key:        "size",
				Comparison: ptr.To(kueue.LessThanOrEqual),
			},
			preemptor: wlWithLabels(map[string]string{"size": "8"}),
			candidate: wlWithLabels(map[string]string{"size": "8"}),
			wantMatch: true,
		},
		"LessThanOrEqual: candidate greater than preemptor rejected": {
			constraint: kueue.PreemptionConfigNumericLabelConstraint{
				Key:        "size",
				Comparison: ptr.To(kueue.LessThanOrEqual),
			},
			preemptor: wlWithLabels(map[string]string{"size": "8"}),
			candidate: wlWithLabels(map[string]string{"size": "16"}),
			wantMatch: false,
		},
		"LessThan: candidate strictly smaller matches": {
			constraint: kueue.PreemptionConfigNumericLabelConstraint{
				Key:        "size",
				Comparison: ptr.To(kueue.LessThan),
			},
			preemptor: wlWithLabels(map[string]string{"size": "8"}),
			candidate: wlWithLabels(map[string]string{"size": "4"}),
			wantMatch: true,
		},
		"LessThan: candidate equal to preemptor rejected": {
			constraint: kueue.PreemptionConfigNumericLabelConstraint{
				Key:        "size",
				Comparison: ptr.To(kueue.LessThan),
			},
			preemptor: wlWithLabels(map[string]string{"size": "8"}),
			candidate: wlWithLabels(map[string]string{"size": "8"}),
			wantMatch: false,
		},
		"LessThan: candidate strictly greater rejected": {
			constraint: kueue.PreemptionConfigNumericLabelConstraint{
				Key:        "size",
				Comparison: ptr.To(kueue.LessThan),
			},
			preemptor: wlWithLabels(map[string]string{"size": "8"}),
			candidate: wlWithLabels(map[string]string{"size": "16"}),
			wantMatch: false,
		},
		"GreaterThan: candidate strictly greater matches": {
			constraint: kueue.PreemptionConfigNumericLabelConstraint{
				Key:        "size",
				Comparison: ptr.To(kueue.GreaterThan),
			},
			preemptor: wlWithLabels(map[string]string{"size": "8"}),
			candidate: wlWithLabels(map[string]string{"size": "16"}),
			wantMatch: true,
		},
		"GreaterThan: candidate equal to preemptor rejected": {
			constraint: kueue.PreemptionConfigNumericLabelConstraint{
				Key:        "size",
				Comparison: ptr.To(kueue.GreaterThan),
			},
			preemptor: wlWithLabels(map[string]string{"size": "8"}),
			candidate: wlWithLabels(map[string]string{"size": "8"}),
			wantMatch: false,
		},
		"GreaterThan: candidate strictly smaller rejected": {
			constraint: kueue.PreemptionConfigNumericLabelConstraint{
				Key:        "size",
				Comparison: ptr.To(kueue.GreaterThan),
			},
			preemptor: wlWithLabels(map[string]string{"size": "8"}),
			candidate: wlWithLabels(map[string]string{"size": "4"}),
			wantMatch: false,
		},
		"GreaterThanOrEqual: candidate strictly greater matches": {
			constraint: kueue.PreemptionConfigNumericLabelConstraint{
				Key:        "size",
				Comparison: ptr.To(kueue.GreaterThanOrEqual),
			},
			preemptor: wlWithLabels(map[string]string{"size": "8"}),
			candidate: wlWithLabels(map[string]string{"size": "16"}),
			wantMatch: true,
		},
		"GreaterThanOrEqual: candidate equal matches": {
			constraint: kueue.PreemptionConfigNumericLabelConstraint{
				Key:        "size",
				Comparison: ptr.To(kueue.GreaterThanOrEqual),
			},
			preemptor: wlWithLabels(map[string]string{"size": "8"}),
			candidate: wlWithLabels(map[string]string{"size": "8"}),
			wantMatch: true,
		},
		"GreaterThanOrEqual: candidate strictly smaller rejected": {
			constraint: kueue.PreemptionConfigNumericLabelConstraint{
				Key:        "size",
				Comparison: ptr.To(kueue.GreaterThanOrEqual),
			},
			preemptor: wlWithLabels(map[string]string{"size": "8"}),
			candidate: wlWithLabels(map[string]string{"size": "4"}),
			wantMatch: false,
		},

		// 2. Candidate Label Resolution & Defaults
		"Fallback value: candidate missing label uses the fallback value satisfying the comparison": {
			constraint: kueue.PreemptionConfigNumericLabelConstraint{
				Key:           "size",
				FallbackValue: ptr.To[int32](4),
				Comparison:    ptr.To(kueue.LessThanOrEqual),
			},
			preemptor: wlWithLabels(map[string]string{"size": "8"}),
			candidate: wlWithLabels(map[string]string{"other": "123"}),
			wantMatch: true,
		},
		"Fallback value: candidate missing label uses the fallback value failing the comparison": {
			constraint: kueue.PreemptionConfigNumericLabelConstraint{
				Key:           "size",
				FallbackValue: ptr.To[int32](16),
				Comparison:    ptr.To(kueue.LessThanOrEqual),
			},
			preemptor: wlWithLabels(map[string]string{"size": "8"}),
			candidate: wlWithLabels(map[string]string{"other": "123"}),
			wantMatch: false,
		},
		"Candidate missing label with nil default is excluded": {
			constraint: kueue.PreemptionConfigNumericLabelConstraint{
				Key:        "size",
				Comparison: ptr.To(kueue.LessThanOrEqual),
			},
			preemptor: wlWithLabels(map[string]string{"size": "8"}),
			candidate: wlWithLabels(map[string]string{"other": "123"}),
			wantMatch: false,
		},
		"Candidate valid label takes precedence over fallback value": {
			constraint: kueue.PreemptionConfigNumericLabelConstraint{
				Key:           "size",
				FallbackValue: ptr.To[int32](2),
				Comparison:    ptr.To(kueue.GreaterThan),
			},
			preemptor: wlWithLabels(map[string]string{"size": "5"}),
			candidate: wlWithLabels(map[string]string{"size": "10"}),
			wantMatch: true,
		},
		"Malformed candidate label falls back to the fallback value": {
			constraint: kueue.PreemptionConfigNumericLabelConstraint{
				Key:           "size",
				FallbackValue: ptr.To[int32](4),
				Comparison:    ptr.To(kueue.LessThanOrEqual),
			},
			preemptor: wlWithLabels(map[string]string{"size": "8"}),
			candidate: wlWithLabels(map[string]string{"size": "invalid-int"}),
			wantMatch: true,
		},
		"Malformed candidate label with nil default is excluded": {
			constraint: kueue.PreemptionConfigNumericLabelConstraint{
				Key:        "size",
				Comparison: ptr.To(kueue.LessThanOrEqual),
			},
			preemptor: wlWithLabels(map[string]string{"size": "8"}),
			candidate: wlWithLabels(map[string]string{"size": "invalid-int"}),
			wantMatch: false,
		},
		"Candidate with nil labels map falls back to the fallback value": {
			constraint: kueue.PreemptionConfigNumericLabelConstraint{
				Key:           "size",
				FallbackValue: ptr.To[int32](4),
				Comparison:    ptr.To(kueue.LessThanOrEqual),
			},
			preemptor: wlWithLabels(map[string]string{"size": "8"}),
			candidate: wlWithLabels(nil),
			wantMatch: true,
		},
		"Candidate with nil labels map and nil default is excluded": {
			constraint: kueue.PreemptionConfigNumericLabelConstraint{
				Key:        "size",
				Comparison: ptr.To(kueue.LessThanOrEqual),
			},
			preemptor: wlWithLabels(map[string]string{"size": "8"}),
			candidate: wlWithLabels(nil),
			wantMatch: false,
		},

		// 3. Preemptor Label Resolution & Fallbacks
		"Preemptor missing label with nil fallback rejects preemption when a comparison is required": {
			constraint: kueue.PreemptionConfigNumericLabelConstraint{
				Key:        "size",
				Comparison: ptr.To(kueue.LessThanOrEqual),
			},
			preemptor: wlWithLabels(map[string]string{"other": "123"}),
			candidate: wlWithLabels(map[string]string{"size": "4"}),
			wantMatch: false,
		},
		"Preemptor missing label falls back to the fallback value satisfying the comparison": {
			constraint: kueue.PreemptionConfigNumericLabelConstraint{
				Key:           "size",
				FallbackValue: ptr.To[int32](8),
				Comparison:    ptr.To(kueue.LessThanOrEqual),
			},
			preemptor: wlWithLabels(map[string]string{"other-key": "123"}),
			candidate: wlWithLabels(map[string]string{"size": "4"}),
			wantMatch: true,
		},
		"Preemptor missing label falls back to the fallback value failing the comparison": {
			constraint: kueue.PreemptionConfigNumericLabelConstraint{
				Key:           "size",
				FallbackValue: ptr.To[int32](4),
				Comparison:    ptr.To(kueue.LessThanOrEqual),
			},
			preemptor: wlWithLabels(map[string]string{"other-key": "123"}),
			candidate: wlWithLabels(map[string]string{"size": "8"}),
			wantMatch: false,
		},
		"Preemptor malformed label with nil fallback rejects preemption when a comparison is required": {
			constraint: kueue.PreemptionConfigNumericLabelConstraint{
				Key:        "size",
				Comparison: ptr.To(kueue.LessThanOrEqual),
			},
			preemptor: wlWithLabels(map[string]string{"size": "invalid-int"}),
			candidate: wlWithLabels(map[string]string{"size": "4"}),
			wantMatch: false,
		},
		"Preemptor malformed label falls back to the fallback value satisfying the comparison": {
			constraint: kueue.PreemptionConfigNumericLabelConstraint{
				Key:           "size",
				FallbackValue: ptr.To[int32](8),
				Comparison:    ptr.To(kueue.LessThanOrEqual),
			},
			preemptor: wlWithLabels(map[string]string{"size": "invalid-int"}),
			candidate: wlWithLabels(map[string]string{"size": "4"}),
			wantMatch: true,
		},
		"Preemptor with nil labels map falls back to the fallback value": {
			constraint: kueue.PreemptionConfigNumericLabelConstraint{
				Key:           "size",
				FallbackValue: ptr.To[int32](8),
				Comparison:    ptr.To(kueue.LessThanOrEqual),
			},
			preemptor: wlWithLabels(nil),
			candidate: wlWithLabels(map[string]string{"size": "4"}),
			wantMatch: true,
		},
		"Both preemptor and candidate missing label use fallback value": {
			constraint: kueue.PreemptionConfigNumericLabelConstraint{
				Key:           "size",
				FallbackValue: ptr.To[int32](4),
				Comparison:    ptr.To(kueue.LessThanOrEqual),
			},
			preemptor: wlWithLabels(nil),
			candidate: wlWithLabels(nil),
			wantMatch: true,
		},
		"Both preemptor and candidate missing label use the fallback value failing the strict comparison": {
			constraint: kueue.PreemptionConfigNumericLabelConstraint{
				Key:           "size",
				FallbackValue: ptr.To[int32](4),
				Comparison:    ptr.To(kueue.LessThan),
			},
			preemptor: wlWithLabels(nil),
			candidate: wlWithLabels(nil),
			wantMatch: false,
		},

		// 4. Absolute Bounds (MinValue & MaxValue)
		"MinValue bound: candidate below MinValue rejected": {
			constraint: kueue.PreemptionConfigNumericLabelConstraint{
				Key:      "priority-boost",
				MinValue: ptr.To[int32](10),
			},
			preemptor: wlWithLabels(nil),
			candidate: wlWithLabels(map[string]string{"priority-boost": "5"}),
			wantMatch: false,
		},
		"MinValue bound: candidate exactly equal to MinValue permitted": {
			constraint: kueue.PreemptionConfigNumericLabelConstraint{
				Key:      "priority-boost",
				MinValue: ptr.To[int32](10),
			},
			preemptor: wlWithLabels(nil),
			candidate: wlWithLabels(map[string]string{"priority-boost": "10"}),
			wantMatch: true,
		},
		"MinValue bound: candidate strictly greater than MinValue permitted": {
			constraint: kueue.PreemptionConfigNumericLabelConstraint{
				Key:      "priority-boost",
				MinValue: ptr.To[int32](10),
			},
			preemptor: wlWithLabels(nil),
			candidate: wlWithLabels(map[string]string{"priority-boost": "15"}),
			wantMatch: true,
		},
		"MaxValue bound: candidate above MaxValue rejected": {
			constraint: kueue.PreemptionConfigNumericLabelConstraint{
				Key:      "priority-boost",
				MaxValue: ptr.To[int32](10),
			},
			preemptor: wlWithLabels(nil),
			candidate: wlWithLabels(map[string]string{"priority-boost": "15"}),
			wantMatch: false,
		},
		"MaxValue bound: candidate exactly equal to MaxValue permitted": {
			constraint: kueue.PreemptionConfigNumericLabelConstraint{
				Key:      "priority-boost",
				MaxValue: ptr.To[int32](10),
			},
			preemptor: wlWithLabels(nil),
			candidate: wlWithLabels(map[string]string{"priority-boost": "10"}),
			wantMatch: true,
		},
		"MaxValue bound: candidate strictly smaller than MaxValue permitted": {
			constraint: kueue.PreemptionConfigNumericLabelConstraint{
				Key:      "priority-boost",
				MaxValue: ptr.To[int32](10),
			},
			preemptor: wlWithLabels(nil),
			candidate: wlWithLabels(map[string]string{"priority-boost": "5"}),
			wantMatch: true,
		},
		"Range bounds (MinValue and MaxValue): candidate strictly inside range permitted": {
			constraint: kueue.PreemptionConfigNumericLabelConstraint{
				Key:      "size",
				MinValue: ptr.To[int32](4),
				MaxValue: ptr.To[int32](16),
			},
			preemptor: wlWithLabels(nil),
			candidate: wlWithLabels(map[string]string{"size": "8"}),
			wantMatch: true,
		},
		"Range bounds (MinValue and MaxValue): candidate strictly below MinValue rejected": {
			constraint: kueue.PreemptionConfigNumericLabelConstraint{
				Key:      "size",
				MinValue: ptr.To[int32](4),
				MaxValue: ptr.To[int32](16),
			},
			preemptor: wlWithLabels(nil),
			candidate: wlWithLabels(map[string]string{"size": "2"}),
			wantMatch: false,
		},
		"Range bounds (MinValue and MaxValue): candidate strictly above MaxValue rejected": {
			constraint: kueue.PreemptionConfigNumericLabelConstraint{
				Key:      "size",
				MinValue: ptr.To[int32](4),
				MaxValue: ptr.To[int32](16),
			},
			preemptor: wlWithLabels(nil),
			candidate: wlWithLabels(map[string]string{"size": "32"}),
			wantMatch: false,
		},
		// 5. Unconstrained Label Key Checks (No comparison, no bounds - verifies integer label presence)
		"Unconstrained label: candidate with valid integer label matches": {
			constraint: kueue.PreemptionConfigNumericLabelConstraint{
				Key: "size",
			},
			preemptor: wlWithLabels(nil),
			candidate: wlWithLabels(map[string]string{"size": "8"}),
			wantMatch: true,
		},
		"Unconstrained label: candidate missing label without default rejected": {
			constraint: kueue.PreemptionConfigNumericLabelConstraint{
				Key: "size",
			},
			preemptor: wlWithLabels(nil),
			candidate: wlWithLabels(map[string]string{"other": "123"}),
			wantMatch: false,
		},
		"Unconstrained label: candidate missing label with default matches": {
			constraint: kueue.PreemptionConfigNumericLabelConstraint{
				Key:           "size",
				FallbackValue: ptr.To[int32](8),
			},
			preemptor: wlWithLabels(nil),
			candidate: wlWithLabels(map[string]string{"other": "123"}),
			wantMatch: true,
		},

		// 6. Non-standard & Edge Numbers (Negative numbers, parsing)
		"Negative numeric values: candidate strictly lower matches": {
			constraint: kueue.PreemptionConfigNumericLabelConstraint{
				Key:        "prio",
				Comparison: ptr.To(kueue.LessThan),
			},
			preemptor: wlWithLabels(map[string]string{"prio": "-5"}),
			candidate: wlWithLabels(map[string]string{"prio": "-10"}),
			wantMatch: true,
		},
		"Negative numeric values: candidate violating negative MinValue rejected": {
			constraint: kueue.PreemptionConfigNumericLabelConstraint{
				Key:      "prio",
				MinValue: ptr.To[int32](-5),
			},
			preemptor: wlWithLabels(nil),
			candidate: wlWithLabels(map[string]string{"prio": "-10"}),
			wantMatch: false,
		},
		"Malformed label: float string fails integer parsing and falls back to default": {
			constraint: kueue.PreemptionConfigNumericLabelConstraint{
				Key:           "size",
				FallbackValue: ptr.To[int32](4),
				Comparison:    ptr.To(kueue.LessThanOrEqual),
			},
			preemptor: wlWithLabels(map[string]string{"size": "8"}),
			candidate: wlWithLabels(map[string]string{"size": "3.14"}),
			wantMatch: true,
		},
		"Malformed label: integer overflow string fails parsing and falls back to default": {
			constraint: kueue.PreemptionConfigNumericLabelConstraint{
				Key:           "size",
				FallbackValue: ptr.To[int32](4),
				Comparison:    ptr.To(kueue.LessThanOrEqual),
			},
			preemptor: wlWithLabels(map[string]string{"size": "8"}),
			candidate: wlWithLabels(map[string]string{"size": "999999999999999999"}),
			wantMatch: true,
		},

		// 7. Composite Constraints & Error Handling
		"Unsupported comparison constraint rejects": {
			constraint: kueue.PreemptionConfigNumericLabelConstraint{
				Key:        "size",
				Comparison: ptr.To[kueue.NumericComparison]("UnknownScope"),
			},
			preemptor: wlWithLabels(map[string]string{"size": "4"}),
			candidate: wlWithLabels(map[string]string{"size": "2"}),
			wantMatch: false,
		},
		"Composite constraint: candidate passes all bounds, comparison, and fallback value": {
			constraint: kueue.PreemptionConfigNumericLabelConstraint{
				Key:           "size",
				FallbackValue: ptr.To[int32](4),
				Comparison:    ptr.To(kueue.LessThanOrEqual),
				MinValue:      ptr.To[int32](2),
				MaxValue:      ptr.To[int32](8),
			},
			preemptor: wlWithLabels(map[string]string{"size": "6"}),
			candidate: wlWithLabels(map[string]string{"distraction": "100"}),
			wantMatch: true,
		},
		"Composite constraint: candidate rejected by the comparison despite passing bounds": {
			constraint: kueue.PreemptionConfigNumericLabelConstraint{
				Key:           "size",
				FallbackValue: ptr.To[int32](4),
				Comparison:    ptr.To(kueue.LessThanOrEqual),
				MinValue:      ptr.To[int32](2),
				MaxValue:      ptr.To[int32](8),
			},
			preemptor: wlWithLabels(map[string]string{"size": "6"}),
			candidate: wlWithLabels(map[string]string{"size": "8"}),
			wantMatch: false,
		},
		"Composite constraint: candidate rejected by MinValue despite passing the comparison": {
			constraint: kueue.PreemptionConfigNumericLabelConstraint{
				Key:        "size",
				Comparison: ptr.To(kueue.LessThan),
				MinValue:   ptr.To[int32](4),
			},
			preemptor: wlWithLabels(map[string]string{"size": "8"}),
			candidate: wlWithLabels(map[string]string{"size": "2"}),
			wantMatch: false,
		},
		"Composite constraint: candidate rejected by MaxValue despite passing the comparison": {
			constraint: kueue.PreemptionConfigNumericLabelConstraint{
				Key:        "size",
				Comparison: ptr.To(kueue.GreaterThan),
				MaxValue:   ptr.To[int32](16),
			},
			preemptor: wlWithLabels(map[string]string{"size": "8"}),
			candidate: wlWithLabels(map[string]string{"size": "32"}),
			wantMatch: false,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			filter := NewNumericLabelFilter(logr.Discard(), tc.constraint, tc.preemptor)
			gotMatch := filter.Matches(tc.candidate)
			if gotMatch != tc.wantMatch {
				t.Errorf("Matches() = %v, want %v", gotMatch, tc.wantMatch)
			}
		})
	}
}
