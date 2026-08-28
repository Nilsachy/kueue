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

	kueuev1beta2 "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	utiltesting "sigs.k8s.io/kueue/pkg/util/testing/v1beta2"
	"sigs.k8s.io/kueue/pkg/workload"
)

func TestNumericLabelFilterMatches(t *testing.T) {
	cases := map[string]struct {
		constraint kueuev1beta2.NumericLabelConstraint
		preemptor  *workload.Info
		candidate  *workload.Info
		wantMatch  bool
	}{
		// 1. Relational Operators (LowerOrEqual, Lower, Greater, GreaterOrEquals)
		"LowerOrEqual: candidate strictly smaller than preemptor matches": {
			constraint: kueuev1beta2.NumericLabelConstraint{
				Key:      "size",
				Relation: ptr.To(kueuev1beta2.LowerOrEqual),
			},
			preemptor: workload.NewInfo(utiltesting.MakeWorkload("p", "").Labels(map[string]string{"size": "8"}).Obj()),
			candidate: workload.NewInfo(utiltesting.MakeWorkload("c", "").Labels(map[string]string{"size": "4"}).Obj()),
			wantMatch: true,
		},
		"LowerOrEqual: candidate equal to preemptor matches": {
			constraint: kueuev1beta2.NumericLabelConstraint{
				Key:      "size",
				Relation: ptr.To(kueuev1beta2.LowerOrEqual),
			},
			preemptor: workload.NewInfo(utiltesting.MakeWorkload("p", "").Labels(map[string]string{"size": "8"}).Obj()),
			candidate: workload.NewInfo(utiltesting.MakeWorkload("c", "").Labels(map[string]string{"size": "8"}).Obj()),
			wantMatch: true,
		},
		"LowerOrEqual: candidate greater than preemptor rejected": {
			constraint: kueuev1beta2.NumericLabelConstraint{
				Key:      "size",
				Relation: ptr.To(kueuev1beta2.LowerOrEqual),
			},
			preemptor: workload.NewInfo(utiltesting.MakeWorkload("p", "").Labels(map[string]string{"size": "8"}).Obj()),
			candidate: workload.NewInfo(utiltesting.MakeWorkload("c", "").Labels(map[string]string{"size": "16"}).Obj()),
			wantMatch: false,
		},
		"Lower: candidate strictly smaller matches": {
			constraint: kueuev1beta2.NumericLabelConstraint{
				Key:      "size",
				Relation: ptr.To(kueuev1beta2.Lower),
			},
			preemptor: workload.NewInfo(utiltesting.MakeWorkload("p", "").Labels(map[string]string{"size": "8"}).Obj()),
			candidate: workload.NewInfo(utiltesting.MakeWorkload("c", "").Labels(map[string]string{"size": "4"}).Obj()),
			wantMatch: true,
		},
		"Lower: candidate equal to preemptor rejected": {
			constraint: kueuev1beta2.NumericLabelConstraint{
				Key:      "size",
				Relation: ptr.To(kueuev1beta2.Lower),
			},
			preemptor: workload.NewInfo(utiltesting.MakeWorkload("p", "").Labels(map[string]string{"size": "8"}).Obj()),
			candidate: workload.NewInfo(utiltesting.MakeWorkload("c", "").Labels(map[string]string{"size": "8"}).Obj()),
			wantMatch: false,
		},
		"Lower: candidate strictly greater rejected": {
			constraint: kueuev1beta2.NumericLabelConstraint{
				Key:      "size",
				Relation: ptr.To(kueuev1beta2.Lower),
			},
			preemptor: workload.NewInfo(utiltesting.MakeWorkload("p", "").Labels(map[string]string{"size": "8"}).Obj()),
			candidate: workload.NewInfo(utiltesting.MakeWorkload("c", "").Labels(map[string]string{"size": "16"}).Obj()),
			wantMatch: false,
		},
		"Greater: candidate strictly greater matches": {
			constraint: kueuev1beta2.NumericLabelConstraint{
				Key:      "size",
				Relation: ptr.To(kueuev1beta2.Greater),
			},
			preemptor: workload.NewInfo(utiltesting.MakeWorkload("p", "").Labels(map[string]string{"size": "8"}).Obj()),
			candidate: workload.NewInfo(utiltesting.MakeWorkload("c", "").Labels(map[string]string{"size": "16"}).Obj()),
			wantMatch: true,
		},
		"Greater: candidate equal to preemptor rejected": {
			constraint: kueuev1beta2.NumericLabelConstraint{
				Key:      "size",
				Relation: ptr.To(kueuev1beta2.Greater),
			},
			preemptor: workload.NewInfo(utiltesting.MakeWorkload("p", "").Labels(map[string]string{"size": "8"}).Obj()),
			candidate: workload.NewInfo(utiltesting.MakeWorkload("c", "").Labels(map[string]string{"size": "8"}).Obj()),
			wantMatch: false,
		},
		"Greater: candidate strictly smaller rejected": {
			constraint: kueuev1beta2.NumericLabelConstraint{
				Key:      "size",
				Relation: ptr.To(kueuev1beta2.Greater),
			},
			preemptor: workload.NewInfo(utiltesting.MakeWorkload("p", "").Labels(map[string]string{"size": "8"}).Obj()),
			candidate: workload.NewInfo(utiltesting.MakeWorkload("c", "").Labels(map[string]string{"size": "4"}).Obj()),
			wantMatch: false,
		},
		"GreaterOrEquals: candidate strictly greater matches": {
			constraint: kueuev1beta2.NumericLabelConstraint{
				Key:      "size",
				Relation: ptr.To(kueuev1beta2.GreaterOrEquals),
			},
			preemptor: workload.NewInfo(utiltesting.MakeWorkload("p", "").Labels(map[string]string{"size": "8"}).Obj()),
			candidate: workload.NewInfo(utiltesting.MakeWorkload("c", "").Labels(map[string]string{"size": "16"}).Obj()),
			wantMatch: true,
		},
		"GreaterOrEquals: candidate equal matches": {
			constraint: kueuev1beta2.NumericLabelConstraint{
				Key:      "size",
				Relation: ptr.To(kueuev1beta2.GreaterOrEquals),
			},
			preemptor: workload.NewInfo(utiltesting.MakeWorkload("p", "").Labels(map[string]string{"size": "8"}).Obj()),
			candidate: workload.NewInfo(utiltesting.MakeWorkload("c", "").Labels(map[string]string{"size": "8"}).Obj()),
			wantMatch: true,
		},
		"GreaterOrEquals: candidate strictly smaller rejected": {
			constraint: kueuev1beta2.NumericLabelConstraint{
				Key:      "size",
				Relation: ptr.To(kueuev1beta2.GreaterOrEquals),
			},
			preemptor: workload.NewInfo(utiltesting.MakeWorkload("p", "").Labels(map[string]string{"size": "8"}).Obj()),
			candidate: workload.NewInfo(utiltesting.MakeWorkload("c", "").Labels(map[string]string{"size": "4"}).Obj()),
			wantMatch: false,
		},

		// 2. Candidate Label Resolution & Defaults
		"Default value fallback: candidate missing label uses default value satisfying relation": {
			constraint: kueuev1beta2.NumericLabelConstraint{
				Key:          "size",
				DefaultValue: ptr.To[int32](4),
				Relation:     ptr.To(kueuev1beta2.LowerOrEqual),
			},
			preemptor: workload.NewInfo(utiltesting.MakeWorkload("p", "").Labels(map[string]string{"size": "8"}).Obj()),
			candidate: workload.NewInfo(utiltesting.MakeWorkload("c", "").Labels(map[string]string{"other": "123"}).Obj()),
			wantMatch: true,
		},
		"Default value fallback: candidate missing label uses default value failing relation": {
			constraint: kueuev1beta2.NumericLabelConstraint{
				Key:          "size",
				DefaultValue: ptr.To[int32](16),
				Relation:     ptr.To(kueuev1beta2.LowerOrEqual),
			},
			preemptor: workload.NewInfo(utiltesting.MakeWorkload("p", "").Labels(map[string]string{"size": "8"}).Obj()),
			candidate: workload.NewInfo(utiltesting.MakeWorkload("c", "").Labels(map[string]string{"other": "123"}).Obj()),
			wantMatch: false,
		},
		"Candidate missing label with nil default is excluded": {
			constraint: kueuev1beta2.NumericLabelConstraint{
				Key:      "size",
				Relation: ptr.To(kueuev1beta2.LowerOrEqual),
			},
			preemptor: workload.NewInfo(utiltesting.MakeWorkload("p", "").Labels(map[string]string{"size": "8"}).Obj()),
			candidate: workload.NewInfo(utiltesting.MakeWorkload("c", "").Labels(map[string]string{"other": "123"}).Obj()),
			wantMatch: false,
		},
		"Candidate valid label takes precedence over default value": {
			constraint: kueuev1beta2.NumericLabelConstraint{
				Key:          "size",
				DefaultValue: ptr.To[int32](2),
				Relation:     ptr.To(kueuev1beta2.Greater),
			},
			preemptor: workload.NewInfo(utiltesting.MakeWorkload("p", "").Labels(map[string]string{"size": "5"}).Obj()),
			candidate: workload.NewInfo(utiltesting.MakeWorkload("c", "").Labels(map[string]string{"size": "10"}).Obj()),
			wantMatch: true,
		},
		"Malformed candidate label falls back to default value": {
			constraint: kueuev1beta2.NumericLabelConstraint{
				Key:          "size",
				DefaultValue: ptr.To[int32](4),
				Relation:     ptr.To(kueuev1beta2.LowerOrEqual),
			},
			preemptor: workload.NewInfo(utiltesting.MakeWorkload("p", "").Labels(map[string]string{"size": "8"}).Obj()),
			candidate: workload.NewInfo(utiltesting.MakeWorkload("c", "").Labels(map[string]string{"size": "invalid-int"}).Obj()),
			wantMatch: true,
		},
		"Malformed candidate label with nil default is excluded": {
			constraint: kueuev1beta2.NumericLabelConstraint{
				Key:      "size",
				Relation: ptr.To(kueuev1beta2.LowerOrEqual),
			},
			preemptor: workload.NewInfo(utiltesting.MakeWorkload("p", "").Labels(map[string]string{"size": "8"}).Obj()),
			candidate: workload.NewInfo(utiltesting.MakeWorkload("c", "").Labels(map[string]string{"size": "invalid-int"}).Obj()),
			wantMatch: false,
		},
		"Candidate with nil labels map falls back to default value": {
			constraint: kueuev1beta2.NumericLabelConstraint{
				Key:          "size",
				DefaultValue: ptr.To[int32](4),
				Relation:     ptr.To(kueuev1beta2.LowerOrEqual),
			},
			preemptor: workload.NewInfo(utiltesting.MakeWorkload("p", "").Labels(map[string]string{"size": "8"}).Obj()),
			candidate: workload.NewInfo(utiltesting.MakeWorkload("c", "").Obj()),
			wantMatch: true,
		},
		"Candidate with nil labels map and nil default is excluded": {
			constraint: kueuev1beta2.NumericLabelConstraint{
				Key:      "size",
				Relation: ptr.To(kueuev1beta2.LowerOrEqual),
			},
			preemptor: workload.NewInfo(utiltesting.MakeWorkload("p", "").Labels(map[string]string{"size": "8"}).Obj()),
			candidate: workload.NewInfo(utiltesting.MakeWorkload("c", "").Obj()),
			wantMatch: false,
		},

		// 3. Preemptor Label Resolution & Fallbacks
		"Preemptor missing label with nil default rejects preemption when relation is required": {
			constraint: kueuev1beta2.NumericLabelConstraint{
				Key:      "size",
				Relation: ptr.To(kueuev1beta2.LowerOrEqual),
			},
			preemptor: workload.NewInfo(utiltesting.MakeWorkload("p", "").Labels(map[string]string{"other": "123"}).Obj()),
			candidate: workload.NewInfo(utiltesting.MakeWorkload("c", "").Labels(map[string]string{"size": "4"}).Obj()),
			wantMatch: false,
		},
		"Preemptor missing label falls back to default value satisfying relation": {
			constraint: kueuev1beta2.NumericLabelConstraint{
				Key:          "size",
				DefaultValue: ptr.To[int32](8),
				Relation:     ptr.To(kueuev1beta2.LowerOrEqual),
			},
			preemptor: workload.NewInfo(utiltesting.MakeWorkload("p", "").Labels(map[string]string{"other-key": "123"}).Obj()),
			candidate: workload.NewInfo(utiltesting.MakeWorkload("c", "").Labels(map[string]string{"size": "4"}).Obj()),
			wantMatch: true,
		},
		"Preemptor missing label falls back to default value failing relation": {
			constraint: kueuev1beta2.NumericLabelConstraint{
				Key:          "size",
				DefaultValue: ptr.To[int32](4),
				Relation:     ptr.To(kueuev1beta2.LowerOrEqual),
			},
			preemptor: workload.NewInfo(utiltesting.MakeWorkload("p", "").Labels(map[string]string{"other-key": "123"}).Obj()),
			candidate: workload.NewInfo(utiltesting.MakeWorkload("c", "").Labels(map[string]string{"size": "8"}).Obj()),
			wantMatch: false,
		},
		"Preemptor malformed label with nil default rejects preemption when relation is required": {
			constraint: kueuev1beta2.NumericLabelConstraint{
				Key:      "size",
				Relation: ptr.To(kueuev1beta2.LowerOrEqual),
			},
			preemptor: workload.NewInfo(utiltesting.MakeWorkload("p", "").Labels(map[string]string{"size": "invalid-int"}).Obj()),
			candidate: workload.NewInfo(utiltesting.MakeWorkload("c", "").Labels(map[string]string{"size": "4"}).Obj()),
			wantMatch: false,
		},
		"Preemptor malformed label falls back to default value satisfying relation": {
			constraint: kueuev1beta2.NumericLabelConstraint{
				Key:          "size",
				DefaultValue: ptr.To[int32](8),
				Relation:     ptr.To(kueuev1beta2.LowerOrEqual),
			},
			preemptor: workload.NewInfo(utiltesting.MakeWorkload("p", "").Labels(map[string]string{"size": "invalid-int"}).Obj()),
			candidate: workload.NewInfo(utiltesting.MakeWorkload("c", "").Labels(map[string]string{"size": "4"}).Obj()),
			wantMatch: true,
		},
		"Preemptor with nil labels map falls back to default value": {
			constraint: kueuev1beta2.NumericLabelConstraint{
				Key:          "size",
				DefaultValue: ptr.To[int32](8),
				Relation:     ptr.To(kueuev1beta2.LowerOrEqual),
			},
			preemptor: workload.NewInfo(utiltesting.MakeWorkload("p", "").Obj()),
			candidate: workload.NewInfo(utiltesting.MakeWorkload("c", "").Labels(map[string]string{"size": "4"}).Obj()),
			wantMatch: true,
		},
		"Both preemptor and candidate missing label use default value": {
			constraint: kueuev1beta2.NumericLabelConstraint{
				Key:          "size",
				DefaultValue: ptr.To[int32](4),
				Relation:     ptr.To(kueuev1beta2.LowerOrEqual),
			},
			preemptor: workload.NewInfo(utiltesting.MakeWorkload("p", "").Obj()),
			candidate: workload.NewInfo(utiltesting.MakeWorkload("c", "").Obj()),
			wantMatch: true,
		},
		"Both preemptor and candidate missing label use default value failing strict relation": {
			constraint: kueuev1beta2.NumericLabelConstraint{
				Key:          "size",
				DefaultValue: ptr.To[int32](4),
				Relation:     ptr.To(kueuev1beta2.Lower),
			},
			preemptor: workload.NewInfo(utiltesting.MakeWorkload("p", "").Obj()),
			candidate: workload.NewInfo(utiltesting.MakeWorkload("c", "").Obj()),
			wantMatch: false,
		},

		// 4. Absolute Bounds (MinValue & MaxValue)
		"MinValue bound: candidate below MinValue rejected": {
			constraint: kueuev1beta2.NumericLabelConstraint{
				Key:      "priority-boost",
				MinValue: ptr.To[int32](10),
			},
			preemptor: workload.NewInfo(utiltesting.MakeWorkload("p", "").Obj()),
			candidate: workload.NewInfo(utiltesting.MakeWorkload("c", "").Labels(map[string]string{"priority-boost": "5"}).Obj()),
			wantMatch: false,
		},
		"MinValue bound: candidate exactly equal to MinValue permitted": {
			constraint: kueuev1beta2.NumericLabelConstraint{
				Key:      "priority-boost",
				MinValue: ptr.To[int32](10),
			},
			preemptor: workload.NewInfo(utiltesting.MakeWorkload("p", "").Obj()),
			candidate: workload.NewInfo(utiltesting.MakeWorkload("c", "").Labels(map[string]string{"priority-boost": "10"}).Obj()),
			wantMatch: true,
		},
		"MinValue bound: candidate strictly greater than MinValue permitted": {
			constraint: kueuev1beta2.NumericLabelConstraint{
				Key:      "priority-boost",
				MinValue: ptr.To[int32](10),
			},
			preemptor: workload.NewInfo(utiltesting.MakeWorkload("p", "").Obj()),
			candidate: workload.NewInfo(utiltesting.MakeWorkload("c", "").Labels(map[string]string{"priority-boost": "15"}).Obj()),
			wantMatch: true,
		},
		"MaxValue bound: candidate above MaxValue rejected": {
			constraint: kueuev1beta2.NumericLabelConstraint{
				Key:      "priority-boost",
				MaxValue: ptr.To[int32](10),
			},
			preemptor: workload.NewInfo(utiltesting.MakeWorkload("p", "").Obj()),
			candidate: workload.NewInfo(utiltesting.MakeWorkload("c", "").Labels(map[string]string{"priority-boost": "15"}).Obj()),
			wantMatch: false,
		},
		"MaxValue bound: candidate exactly equal to MaxValue permitted": {
			constraint: kueuev1beta2.NumericLabelConstraint{
				Key:      "priority-boost",
				MaxValue: ptr.To[int32](10),
			},
			preemptor: workload.NewInfo(utiltesting.MakeWorkload("p", "").Obj()),
			candidate: workload.NewInfo(utiltesting.MakeWorkload("c", "").Labels(map[string]string{"priority-boost": "10"}).Obj()),
			wantMatch: true,
		},
		"MaxValue bound: candidate strictly smaller than MaxValue permitted": {
			constraint: kueuev1beta2.NumericLabelConstraint{
				Key:      "priority-boost",
				MaxValue: ptr.To[int32](10),
			},
			preemptor: workload.NewInfo(utiltesting.MakeWorkload("p", "").Obj()),
			candidate: workload.NewInfo(utiltesting.MakeWorkload("c", "").Labels(map[string]string{"priority-boost": "5"}).Obj()),
			wantMatch: true,
		},
		"Range bounds (MinValue and MaxValue): candidate strictly inside range permitted": {
			constraint: kueuev1beta2.NumericLabelConstraint{
				Key:      "size",
				MinValue: ptr.To[int32](4),
				MaxValue: ptr.To[int32](16),
			},
			preemptor: workload.NewInfo(utiltesting.MakeWorkload("p", "").Obj()),
			candidate: workload.NewInfo(utiltesting.MakeWorkload("c", "").Labels(map[string]string{"size": "8"}).Obj()),
			wantMatch: true,
		},
		"Range bounds (MinValue and MaxValue): candidate strictly below MinValue rejected": {
			constraint: kueuev1beta2.NumericLabelConstraint{
				Key:      "size",
				MinValue: ptr.To[int32](4),
				MaxValue: ptr.To[int32](16),
			},
			preemptor: workload.NewInfo(utiltesting.MakeWorkload("p", "").Obj()),
			candidate: workload.NewInfo(utiltesting.MakeWorkload("c", "").Labels(map[string]string{"size": "2"}).Obj()),
			wantMatch: false,
		},
		"Range bounds (MinValue and MaxValue): candidate strictly above MaxValue rejected": {
			constraint: kueuev1beta2.NumericLabelConstraint{
				Key:      "size",
				MinValue: ptr.To[int32](4),
				MaxValue: ptr.To[int32](16),
			},
			preemptor: workload.NewInfo(utiltesting.MakeWorkload("p", "").Obj()),
			candidate: workload.NewInfo(utiltesting.MakeWorkload("c", "").Labels(map[string]string{"size": "32"}).Obj()),
			wantMatch: false,
		},
		// 5. Unconstrained Label Key Checks (No relation, no bounds - verifies integer label presence)
		"Unconstrained label: candidate with valid integer label matches": {
			constraint: kueuev1beta2.NumericLabelConstraint{
				Key: "size",
			},
			preemptor: workload.NewInfo(utiltesting.MakeWorkload("p", "").Obj()),
			candidate: workload.NewInfo(utiltesting.MakeWorkload("c", "").Labels(map[string]string{"size": "8"}).Obj()),
			wantMatch: true,
		},
		"Unconstrained label: candidate missing label without default rejected": {
			constraint: kueuev1beta2.NumericLabelConstraint{
				Key: "size",
			},
			preemptor: workload.NewInfo(utiltesting.MakeWorkload("p", "").Obj()),
			candidate: workload.NewInfo(utiltesting.MakeWorkload("c", "").Labels(map[string]string{"other": "123"}).Obj()),
			wantMatch: false,
		},
		"Unconstrained label: candidate missing label with default matches": {
			constraint: kueuev1beta2.NumericLabelConstraint{
				Key:          "size",
				DefaultValue: ptr.To[int32](8),
			},
			preemptor: workload.NewInfo(utiltesting.MakeWorkload("p", "").Obj()),
			candidate: workload.NewInfo(utiltesting.MakeWorkload("c", "").Labels(map[string]string{"other": "123"}).Obj()),
			wantMatch: true,
		},

		// 6. Non-standard & Edge Numbers (Negative numbers, parsing)
		"Negative numeric values: candidate strictly lower matches": {
			constraint: kueuev1beta2.NumericLabelConstraint{
				Key:      "prio",
				Relation: ptr.To(kueuev1beta2.Lower),
			},
			preemptor: workload.NewInfo(utiltesting.MakeWorkload("p", "").Labels(map[string]string{"prio": "-5"}).Obj()),
			candidate: workload.NewInfo(utiltesting.MakeWorkload("c", "").Labels(map[string]string{"prio": "-10"}).Obj()),
			wantMatch: true,
		},
		"Negative numeric values: candidate violating negative MinValue rejected": {
			constraint: kueuev1beta2.NumericLabelConstraint{
				Key:      "prio",
				MinValue: ptr.To[int32](-5),
			},
			preemptor: workload.NewInfo(utiltesting.MakeWorkload("p", "").Obj()),
			candidate: workload.NewInfo(utiltesting.MakeWorkload("c", "").Labels(map[string]string{"prio": "-10"}).Obj()),
			wantMatch: false,
		},
		"Malformed label: float string fails integer parsing and falls back to default": {
			constraint: kueuev1beta2.NumericLabelConstraint{
				Key:          "size",
				DefaultValue: ptr.To[int32](4),
				Relation:     ptr.To(kueuev1beta2.LowerOrEqual),
			},
			preemptor: workload.NewInfo(utiltesting.MakeWorkload("p", "").Labels(map[string]string{"size": "8"}).Obj()),
			candidate: workload.NewInfo(utiltesting.MakeWorkload("c", "").Labels(map[string]string{"size": "3.14"}).Obj()),
			wantMatch: true,
		},
		"Malformed label: integer overflow string fails parsing and falls back to default": {
			constraint: kueuev1beta2.NumericLabelConstraint{
				Key:          "size",
				DefaultValue: ptr.To[int32](4),
				Relation:     ptr.To(kueuev1beta2.LowerOrEqual),
			},
			preemptor: workload.NewInfo(utiltesting.MakeWorkload("p", "").Labels(map[string]string{"size": "8"}).Obj()),
			candidate: workload.NewInfo(utiltesting.MakeWorkload("c", "").Labels(map[string]string{"size": "999999999999999999"}).Obj()),
			wantMatch: true,
		},

		// 7. Composite Constraints & Error Handling
		"Unsupported relation constraint rejects": {
			constraint: kueuev1beta2.NumericLabelConstraint{
				Key:      "size",
				Relation: ptr.To[kueuev1beta2.RelativeConstraint]("UnknownRelation"),
			},
			preemptor: workload.NewInfo(utiltesting.MakeWorkload("p", "").Labels(map[string]string{"size": "4"}).Obj()),
			candidate: workload.NewInfo(utiltesting.MakeWorkload("c", "").Labels(map[string]string{"size": "2"}).Obj()),
			wantMatch: false,
		},
		"Composite constraint: candidate passes all bounds, relation, and default value": {
			constraint: kueuev1beta2.NumericLabelConstraint{
				Key:          "size",
				DefaultValue: ptr.To[int32](4),
				Relation:     ptr.To(kueuev1beta2.LowerOrEqual),
				MinValue:     ptr.To[int32](2),
				MaxValue:     ptr.To[int32](8),
			},
			preemptor: workload.NewInfo(utiltesting.MakeWorkload("p", "").Labels(map[string]string{"size": "6"}).Obj()),
			candidate: workload.NewInfo(utiltesting.MakeWorkload("c", "").Labels(map[string]string{"distraction": "100"}).Obj()),
			wantMatch: true,
		},
		"Composite constraint: candidate rejected by relation despite passing bounds": {
			constraint: kueuev1beta2.NumericLabelConstraint{
				Key:          "size",
				DefaultValue: ptr.To[int32](4),
				Relation:     ptr.To(kueuev1beta2.LowerOrEqual),
				MinValue:     ptr.To[int32](2),
				MaxValue:     ptr.To[int32](8),
			},
			preemptor: workload.NewInfo(utiltesting.MakeWorkload("p", "").Labels(map[string]string{"size": "6"}).Obj()),
			candidate: workload.NewInfo(utiltesting.MakeWorkload("c", "").Labels(map[string]string{"size": "8"}).Obj()),
			wantMatch: false,
		},
		"Composite constraint: candidate rejected by MinValue despite passing relation": {
			constraint: kueuev1beta2.NumericLabelConstraint{
				Key:      "size",
				Relation: ptr.To(kueuev1beta2.Lower),
				MinValue: ptr.To[int32](4),
			},
			preemptor: workload.NewInfo(utiltesting.MakeWorkload("p", "").Labels(map[string]string{"size": "8"}).Obj()),
			candidate: workload.NewInfo(utiltesting.MakeWorkload("c", "").Labels(map[string]string{"size": "2"}).Obj()),
			wantMatch: false,
		},
		"Composite constraint: candidate rejected by MaxValue despite passing relation": {
			constraint: kueuev1beta2.NumericLabelConstraint{
				Key:      "size",
				Relation: ptr.To(kueuev1beta2.Greater),
				MaxValue: ptr.To[int32](16),
			},
			preemptor: workload.NewInfo(utiltesting.MakeWorkload("p", "").Labels(map[string]string{"size": "8"}).Obj()),
			candidate: workload.NewInfo(utiltesting.MakeWorkload("c", "").Labels(map[string]string{"size": "32"}).Obj()),
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
