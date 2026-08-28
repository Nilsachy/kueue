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
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"k8s.io/utils/ptr"

	kueuev1beta2 "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	"sigs.k8s.io/kueue/pkg/workload"
)

func TestNewCandidateFilters(t *testing.T) {
	// Minimal snapshot required by constructor for resolving preemptor's cohort ancestors:
	// rootA -> subA1 -> cq1
	snapshot := newSnapshotBuilder().
		Cohort("rootA", "").
		Cohort("subA1", "rootA").
		ClusterQueue("cq1", "subA1").
		Build()

	preemptor := makeWorkloadInfo("preemptor", "ns1", "lq1", "cq1")
	preemptor.Obj.Labels = map[string]string{"tpu-size": "8"}

	cases := map[string]struct {
		selector    *kueuev1beta2.PreemptionCandidateSelector
		preemptor   *workload.Info
		wantFilters CandidateFilters
	}{
		"nil selector returns empty CandidateFilters": {
			selector:    nil,
			preemptor:   preemptor,
			wantFilters: CandidateFilters{},
		},
		"SameLocalQueue instantiates sameClusterQueueFilter and sameLocalQueueFilter": {
			selector: &kueuev1beta2.PreemptionCandidateSelector{
				RelationRequirement: kueuev1beta2.SameLocalQueue,
			},
			preemptor: preemptor,
			wantFilters: CandidateFilters{
				CQFilters: []ClusterQueueFilter{
					&sameClusterQueueFilter{preemptorCQ: "cq1"},
				},
				WLFilters: []WorkloadFilter{
					&sameLocalQueueFilter{namespace: "ns1", queueName: "lq1"},
				},
			},
		},
		"SameClusterQueue instantiates sameClusterQueueFilter": {
			selector: &kueuev1beta2.PreemptionCandidateSelector{
				RelationRequirement: kueuev1beta2.SameClusterQueue,
			},
			preemptor: preemptor,
			wantFilters: CandidateFilters{
				CQFilters: []ClusterQueueFilter{
					&sameClusterQueueFilter{preemptorCQ: "cq1"},
				},
			},
		},
		"SameCohort resolves immediate parent cohort from snapshot": {
			selector: &kueuev1beta2.PreemptionCandidateSelector{
				RelationRequirement: kueuev1beta2.SameCohort,
			},
			preemptor: preemptor,
			wantFilters: CandidateFilters{
				CQFilters: []ClusterQueueFilter{
					&sameCohortFilter{
						preemptorCQ:     "cq1",
						preemptorCohort: "subA1",
						hasCohort:       true,
					},
				},
			},
		},
		"SameCohortTree resolves root ancestor cohort from snapshot": {
			selector: &kueuev1beta2.PreemptionCandidateSelector{
				RelationRequirement: kueuev1beta2.SameCohortTree,
			},
			preemptor: preemptor,
			wantFilters: CandidateFilters{
				CQFilters: []ClusterQueueFilter{
					&sameCohortTreeFilter{
						preemptorCQ:         "cq1",
						preemptorRootCohort: "rootA",
						hasCohort:           true,
					},
				},
			},
		},
		"AnyClusterQueue results in empty filters": {
			selector: &kueuev1beta2.PreemptionCandidateSelector{
				RelationRequirement: kueuev1beta2.AnyClusterQueue,
			},
			preemptor:   preemptor,
			wantFilters: CandidateFilters{},
		},
		"unrecognized relation requirement returns rejectAllCQFilter": {
			selector: &kueuev1beta2.PreemptionCandidateSelector{
				RelationRequirement: kueuev1beta2.PreemptionRelationConstraint("UnknownRelation"),
			},
			preemptor: preemptor,
			wantFilters: CandidateFilters{
				CQFilters: []ClusterQueueFilter{
					&rejectAllCQFilter{},
				},
			},
		},
		"SameClusterQueue with empty NumericLabels produces no WorkloadFilters": {
			selector: &kueuev1beta2.PreemptionCandidateSelector{
				RelationRequirement: kueuev1beta2.SameClusterQueue,
				NumericLabels:       []kueuev1beta2.NumericLabelConstraint{},
			},
			preemptor: preemptor,
			wantFilters: CandidateFilters{
				CQFilters: []ClusterQueueFilter{
					&sameClusterQueueFilter{preemptorCQ: "cq1"},
				},
				WLFilters: nil,
			},
		},
		"Combined SameLocalQueue and NumericLabelConstraints appends both relation and numeric WorkloadFilters": {
			selector: &kueuev1beta2.PreemptionCandidateSelector{
				RelationRequirement: kueuev1beta2.SameLocalQueue,
				NumericLabels: []kueuev1beta2.NumericLabelConstraint{
					{
						Key:          "tpu-size",
						DefaultValue: ptr.To[int32](1),
						Relation:     ptr.To(kueuev1beta2.LowerOrEqual),
					},
					{
						Key:      "priority-boost",
						MinValue: ptr.To[int32](10),
					},
				},
			},
			preemptor: preemptor,
			wantFilters: CandidateFilters{
				CQFilters: []ClusterQueueFilter{
					&sameClusterQueueFilter{preemptorCQ: "cq1"},
				},
				WLFilters: []WorkloadFilter{
					&sameLocalQueueFilter{
						namespace: "ns1",
						queueName: "lq1",
					},
					&numericLabelFilter{
						constraint: kueuev1beta2.NumericLabelConstraint{
							Key:          "tpu-size",
							DefaultValue: ptr.To[int32](1),
							Relation:     ptr.To(kueuev1beta2.LowerOrEqual),
						},
						preemptorVal: ptr.To[int32](8), // extracted from preemptor's "tpu-size": "8" label
					},
					&numericLabelFilter{
						constraint: kueuev1beta2.NumericLabelConstraint{
							Key:      "priority-boost",
							MinValue: ptr.To[int32](10),
						},
						preemptorVal: nil, // no relation requested, so preemptor value is not parsed
					},
				},
			},
		},
	}

	cmpOptions := []cmp.Option{
		cmp.AllowUnexported(
			sameClusterQueueFilter{},
			sameCohortFilter{},
			sameCohortTreeFilter{},
			sameLocalQueueFilter{},
			rejectAllCQFilter{},
			numericLabelFilter{},
		),
		cmpopts.IgnoreFields(numericLabelFilter{}, "log"),
		cmpopts.EquateEmpty(),
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got := NewCandidateFilters(logr.Discard(), tc.selector, tc.preemptor, snapshot)
			if diff := cmp.Diff(tc.wantFilters, got, cmpOptions...); diff != "" {
				t.Errorf("NewCandidateFilters() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
