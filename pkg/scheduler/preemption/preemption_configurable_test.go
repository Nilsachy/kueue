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

package preemption

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	corev1 "k8s.io/api/core/v1"
	schedulingv1 "k8s.io/api/scheduling/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	clocktesting "k8s.io/utils/clock/testing"
	"k8s.io/utils/ptr"

	config "sigs.k8s.io/kueue/apis/config/v1beta2"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	schdcache "sigs.k8s.io/kueue/pkg/cache/scheduler"
	"sigs.k8s.io/kueue/pkg/features"
	"sigs.k8s.io/kueue/pkg/scheduler/flavorassigner"
	preemptexpectations "sigs.k8s.io/kueue/pkg/scheduler/preemption/expectations"
	utilslices "sigs.k8s.io/kueue/pkg/util/slices"
	utiltesting "sigs.k8s.io/kueue/pkg/util/testing"
	utiltestingapi "sigs.k8s.io/kueue/pkg/util/testing/v1beta2"
	"sigs.k8s.io/kueue/pkg/workload"
)

func TestConfigurablePreemptions(t *testing.T) {
	now := time.Now()
	defaultConfigName := "default-config"
	baseCQs := []*kueue.ClusterQueue{
		utiltestingapi.MakeClusterQueue("a").
			Cohort("all").
			ResourceGroup(*utiltestingapi.MakeFlavorQuotas("default").
				Resource(corev1.ResourceCPU, "2").Obj()).
			Annotation(kueue.PreemptionConfigAnnotation, defaultConfigName).
			Obj(),
	}

	baseConfig := kueue.PreemptionConfig{
		ObjectMeta: metav1.ObjectMeta{
			Name: defaultConfigName,
		},
		Spec: kueue.PreemptionConfigSpec{
			Rules: []kueue.PreemptionRule{
				{
					Name:    "test-rule-one",
					Trigger: kueue.InsufficientQuota,
					Candidates: []kueue.PreemptionCandidateSelector{
						{
							RelationRequirement: kueue.SameClusterQueue,
						},
					},
				},
			},
		},
	}

	// configWithSelector returns baseConfig with a single rule using the given selector.
	configWithSelector := func(selector kueue.PreemptionCandidateSelector) kueue.PreemptionConfig {
		return kueue.PreemptionConfig{
			ObjectMeta: metav1.ObjectMeta{
				Name: defaultConfigName,
			},
			Spec: kueue.PreemptionConfigSpec{
				Rules: []kueue.PreemptionRule{
					{
						Name:       "test-rule-one",
						Trigger:    kueue.InsufficientQuota,
						Candidates: []kueue.PreemptionCandidateSelector{selector},
					},
				},
			},
		}
	}

	lowerTierConstraint := []kueue.NumericLabelConstraint{
		{
			Key:      "preemption-tier",
			Relation: ptr.To(kueue.Lower),
		},
	}
	sameCohortConfig := configWithSelector(kueue.PreemptionCandidateSelector{
		RelationRequirement: kueue.SameCohort,
	})
	sameCohortTierConfig := configWithSelector(kueue.PreemptionCandidateSelector{
		RelationRequirement: kueue.SameCohort,
		NumericLabels:       lowerTierConstraint,
	})
	anyClusterQueueConfig := configWithSelector(kueue.PreemptionCandidateSelector{
		RelationRequirement: kueue.AnyClusterQueue,
	})
	tierConfig := configWithSelector(kueue.PreemptionCandidateSelector{
		RelationRequirement: kueue.SameClusterQueue,
		NumericLabels:       lowerTierConstraint,
	})

	insufficientQuotaCond := metav1.Condition{
		Type:               string(kueue.InsufficientQuota),
		Status:             metav1.ConditionTrue,
		LastTransitionTime: metav1.NewTime(now),
	}

	unitWl := *utiltestingapi.MakeWorkload("unit", "").Request(corev1.ResourceCPU, "1")
	cases := map[string]struct {
		clusterQueues           []*kueue.ClusterQueue
		cohorts                 []*kueue.Cohort
		config                  kueue.PreemptionConfig
		workloadPriorityClasses []kueue.WorkloadPriorityClass
		priorityClasses         []schedulingv1.PriorityClass
		admitted                []kueue.Workload
		incoming                *kueue.Workload
		targetCQ                kueue.ClusterQueueReference
		// fairSharing enables the Fair Sharing algorithm, instead of the classical one.
		fairSharing   *config.FairSharing
		wantPreempted sets.Set[string]
		wantReasons   map[string]string
	}{
		"no candidates for CQ without config": {
			clusterQueues: []*kueue.ClusterQueue{
				utiltestingapi.MakeClusterQueue("a").
					ResourceGroup(*utiltestingapi.MakeFlavorQuotas("default").
						Resource(corev1.ResourceCPU, "2").Obj()).
					Obj(),
			},
			config: baseConfig,
			admitted: []kueue.Workload{
				*unitWl.Clone().Name("a1").SimpleReserveQuota("a", "default", now).Obj(),
				*unitWl.Clone().Name("a2").SimpleReserveQuota("a", "default", now).Obj(),
			},
			incoming:      unitWl.Clone().Name("a_incoming").Condition(insufficientQuotaCond).Obj(),
			targetCQ:      "a",
			wantPreempted: sets.New[string](),
		},
		"one workload should be preempted to fit incoming workload": {
			clusterQueues: baseCQs,
			config:        baseConfig,
			admitted: []kueue.Workload{
				*unitWl.Clone().Name("a1").SimpleReserveQuota("a", "default", now).Obj(),
				*unitWl.Clone().Name("a2").SimpleReserveQuota("a", "default", now).Obj(),
			},
			incoming:      unitWl.Clone().Name("a_incoming").Condition(insufficientQuotaCond).Obj(),
			targetCQ:      "a",
			wantPreempted: sets.New("/a1"),
		},
		"multiple workloads should be preempted to fit incoming workload": {
			clusterQueues: baseCQs,
			config:        baseConfig,
			admitted: []kueue.Workload{
				*unitWl.Clone().Name("a1").SimpleReserveQuota("a", "default", now).Obj(),
				*unitWl.Clone().Name("a2").SimpleReserveQuota("a", "default", now).Obj(),
			},
			incoming:      unitWl.Clone().Name("a_incoming").Request(corev1.ResourceCPU, "2").Condition(insufficientQuotaCond).Obj(),
			targetCQ:      "a",
			wantPreempted: sets.New("/a1", "/a2"),
		},
		"incoming workload cannot fit because no matching triggers": {
			clusterQueues: baseCQs,
			config:        baseConfig,
			admitted: []kueue.Workload{
				*unitWl.Clone().Name("a1").SimpleReserveQuota("a", "default", now).Obj(),
				*unitWl.Clone().Name("a2").SimpleReserveQuota("a", "default", now).Obj(),
			},
			incoming: unitWl.Clone().Name("a_incoming").Request(corev1.ResourceCPU, "1").
				Condition(metav1.Condition{
					Type:               string(kueue.InsufficientTopology),
					Status:             metav1.ConditionTrue,
					LastTransitionTime: metav1.NewTime(now),
				}).
				Obj(),
			targetCQ:      "a",
			wantPreempted: sets.New[string](),
		},
		"incoming workload cannot fit because configuration doesn't provide enough candidates": {
			clusterQueues: baseCQs,
			config: kueue.PreemptionConfig{
				ObjectMeta: metav1.ObjectMeta{
					Name: defaultConfigName,
				},
				Spec: kueue.PreemptionConfigSpec{
					Rules: []kueue.PreemptionRule{
						{
							Name:    "test-rule-one",
							Trigger: kueue.InsufficientQuota,
							Candidates: []kueue.PreemptionCandidateSelector{
								{
									RelationRequirement: kueue.SameClusterQueue,
									NumericLabels: []kueue.NumericLabelConstraint{
										{
											Key:      "test-label",
											Relation: ptr.To(kueue.Lower),
										},
									},
								},
							},
						},
					},
				},
			},
			admitted: []kueue.Workload{
				*unitWl.Clone().Name("a1").SimpleReserveQuota("a", "default", now).Label("test-label", "9").Obj(),
				*unitWl.Clone().Name("a2").SimpleReserveQuota("a", "default", now).Label("test-label", "1").Obj(),
			},
			incoming:      unitWl.Clone().Name("a_incoming").Request(corev1.ResourceCPU, "2").Label("test-label", "5").Condition(insufficientQuotaCond).Obj(),
			targetCQ:      "a",
			wantPreempted: sets.New[string](),
		},
		"returns no candidates when requested config not found by name": {
			clusterQueues: []*kueue.ClusterQueue{
				utiltestingapi.MakeClusterQueue("a").
					Cohort("all").
					ResourceGroup(*utiltestingapi.MakeFlavorQuotas("default").
						Resource(corev1.ResourceCPU, "2").Obj()).
					Annotation(kueue.PreemptionConfigAnnotation, "unknown-name").
					Obj(),
			},
			config: baseConfig,
			admitted: []kueue.Workload{
				*unitWl.Clone().Name("a1").SimpleReserveQuota("a", "default", now).Obj(),
				*unitWl.Clone().Name("a2").SimpleReserveQuota("a", "default", now).Obj(),
			},
			incoming:      unitWl.Clone().Name("a_incoming").Condition(insufficientQuotaCond).Obj(),
			targetCQ:      "a",
			wantPreempted: sets.New[string](),
		},
		"returns no candidates when requested config has incorrect parameters": {
			clusterQueues: baseCQs,
			config: kueue.PreemptionConfig{
				ObjectMeta: metav1.ObjectMeta{
					Name: defaultConfigName,
				},
				Spec: kueue.PreemptionConfigSpec{
					Rules: []kueue.PreemptionRule{
						{
							Name:    "test-rule-one",
							Trigger: kueue.InsufficientQuota,
							MatchingPreemptorWorkloads: metav1.LabelSelector{
								MatchExpressions: []metav1.LabelSelectorRequirement{
									{
										Key:      "test",
										Operator: "invalid",
									},
								},
							},
							Candidates: []kueue.PreemptionCandidateSelector{
								{
									RelationRequirement: kueue.SameClusterQueue,
								},
							},
						},
					},
				},
			},
			admitted: []kueue.Workload{
				*unitWl.Clone().Name("a1").SimpleReserveQuota("a", "default", now).Obj(),
				*unitWl.Clone().Name("a2").SimpleReserveQuota("a", "default", now).Obj(),
			},
			incoming:      unitWl.Clone().Name("a_incoming").Condition(insufficientQuotaCond).Obj(),
			targetCQ:      "a",
			wantPreempted: sets.New[string](),
		},
		"RelativeWorkloadPriority: only candidates with lower priority are preempted": {
			clusterQueues: baseCQs,
			config: kueue.PreemptionConfig{
				ObjectMeta: metav1.ObjectMeta{
					Name: defaultConfigName,
				},
				Spec: kueue.PreemptionConfigSpec{
					Rules: []kueue.PreemptionRule{
						{
							Name:    "relative-priority-rule",
							Trigger: kueue.InsufficientQuota,
							Candidates: []kueue.PreemptionCandidateSelector{
								{
									RelationRequirement:      kueue.SameClusterQueue,
									RelativeWorkloadPriority: ptr.To(kueue.Lower),
								},
							},
						},
					},
				},
			},
			admitted: []kueue.Workload{
				*unitWl.Clone().Name("a1").
					Priority(20).
					SimpleReserveQuota("a", "default", now).Obj(),
				*unitWl.Clone().Name("a2").
					Priority(120).
					SimpleReserveQuota("a", "default", now).Obj(),
			},
			incoming: unitWl.Clone().Name("a_incoming").
				Priority(100).
				Condition(insufficientQuotaCond).Obj(),
			targetCQ:      "a",
			wantPreempted: sets.New("/a1"),
		},
		"RelativeWorkloadPriority with priority boost annotation modifies preemption ordering": {
			clusterQueues: baseCQs,
			config: kueue.PreemptionConfig{
				ObjectMeta: metav1.ObjectMeta{
					Name: defaultConfigName,
				},
				Spec: kueue.PreemptionConfigSpec{
					Rules: []kueue.PreemptionRule{
						{
							Name:    "boost-priority-rule",
							Trigger: kueue.InsufficientQuota,
							Candidates: []kueue.PreemptionCandidateSelector{
								{
									RelationRequirement:      kueue.SameClusterQueue,
									RelativeWorkloadPriority: ptr.To(kueue.Lower),
								},
							},
						},
					},
				},
			},
			admitted: []kueue.Workload{
				*unitWl.Clone().Name("a1").
					Priority(100).
					Annotation("kueue.x-k8s.io/priority-boost", "-60").
					SimpleReserveQuota("a", "default", now).Obj(),
				*unitWl.Clone().Name("a2").
					Priority(60).
					SimpleReserveQuota("a", "default", now).Obj(),
			},
			incoming: unitWl.Clone().Name("a_incoming").
				Priority(50).
				Condition(insufficientQuotaCond).Obj(),
			targetCQ:      "a",
			wantPreempted: sets.New("/a1"),
		},
		"candidates from both classical and configurable preemption algorithms are merged": {
			clusterQueues: []*kueue.ClusterQueue{
				utiltestingapi.MakeClusterQueue("a").
					Cohort("all").
					ResourceGroup(*utiltestingapi.MakeFlavorQuotas("default").
						Resource(corev1.ResourceCPU, "3").Obj()).
					Preemption(kueue.ClusterQueuePreemption{
						WithinClusterQueue: kueue.PreemptionPolicyLowerPriority,
					}).
					Annotation(kueue.PreemptionConfigAnnotation, defaultConfigName).
					Obj(),
			},
			config: kueue.PreemptionConfig{
				ObjectMeta: metav1.ObjectMeta{
					Name: defaultConfigName,
				},
				Spec: kueue.PreemptionConfigSpec{
					Rules: []kueue.PreemptionRule{
						{
							Name:    "candidate-tier-rule",
							Trigger: kueue.InsufficientQuota,
							Candidates: []kueue.PreemptionCandidateSelector{
								{
									RelationRequirement: kueue.SameClusterQueue,
									NumericLabels: []kueue.NumericLabelConstraint{
										{
											Key:      "preemption-tier",
											Relation: ptr.To(kueue.Lower),
										},
									},
								},
							},
						},
					},
				},
			},
			admitted: []kueue.Workload{
				// a1 has no tier label, so it is a candidate for the classical algorithm only.
				*unitWl.Clone().Name("a1").
					Priority(10).
					SimpleReserveQuota("a", "default", now).Obj(),
				// a2 is a candidate for both algorithms.
				*unitWl.Clone().Name("a2").
					Priority(20).
					Label("preemption-tier", "1").
					SimpleReserveQuota("a", "default", now).Obj(),
				// a3 has a higher priority than the incoming workload, so it is a
				// candidate for the configurable algorithm only.
				*unitWl.Clone().Name("a3").
					Priority(200).
					Label("preemption-tier", "2").
					SimpleReserveQuota("a", "default", now).Obj(),
			},
			incoming: unitWl.Clone().Name("a_incoming").
				Priority(100).
				Label("preemption-tier", "5").
				Request(corev1.ResourceCPU, "2").
				Condition(insufficientQuotaCond).Obj(),
			targetCQ: "a",
			// Candidates selected by the configurable rules are considered before the
			// in-ClusterQueue ones, and only as many targets as needed are preempted.
			wantPreempted: sets.New("/a1", "/a3"),
			wantReasons: map[string]string{
				"/a1": kueue.InClusterQueueReason,
				"/a3": "ConfigurablePreemption",
			},
		},
		"configurable candidate in a ClusterQueue within nominal quota is preempted": {
			clusterQueues: []*kueue.ClusterQueue{
				utiltestingapi.MakeClusterQueue("a").
					Cohort("all").
					ResourceGroup(*utiltestingapi.MakeFlavorQuotas("default").
						Resource(corev1.ResourceCPU, "1").Obj()).
					Annotation(kueue.PreemptionConfigAnnotation, defaultConfigName).
					Obj(),
				utiltestingapi.MakeClusterQueue("b").
					Cohort("all").
					ResourceGroup(*utiltestingapi.MakeFlavorQuotas("default").
						Resource(corev1.ResourceCPU, "1").Obj()).
					Obj(),
			},
			config: sameCohortConfig,
			admitted: []kueue.Workload{
				*unitWl.Clone().Name("a1").SimpleReserveQuota("a", "default", now).Obj(),
				// b is not borrowing, so b1 would be rejected by the classical
				// reclamation rules.
				*unitWl.Clone().Name("b1").SimpleReserveQuota("b", "default", now).Obj(),
			},
			incoming:      unitWl.Clone().Name("a_incoming").Condition(insufficientQuotaCond).Obj(),
			targetCQ:      "a",
			wantPreempted: sets.New("/b1"),
			wantReasons: map[string]string{
				"/b1": "ConfigurablePreemption",
			},
		},
		"candidate selected by both algorithms is preempted once, with the classical reason": {
			clusterQueues: []*kueue.ClusterQueue{
				utiltestingapi.MakeClusterQueue("a").
					Cohort("all").
					ResourceGroup(*utiltestingapi.MakeFlavorQuotas("default").
						Resource(corev1.ResourceCPU, "1").Obj()).
					Preemption(kueue.ClusterQueuePreemption{
						WithinClusterQueue: kueue.PreemptionPolicyLowerPriority,
					}).
					Annotation(kueue.PreemptionConfigAnnotation, defaultConfigName).
					Obj(),
			},
			config: baseConfig,
			admitted: []kueue.Workload{
				*unitWl.Clone().Name("a1").Priority(10).SimpleReserveQuota("a", "default", now).Obj(),
			},
			incoming:      unitWl.Clone().Name("a_incoming").Priority(100).Condition(insufficientQuotaCond).Obj(),
			targetCQ:      "a",
			wantPreempted: sets.New("/a1"),
			wantReasons: map[string]string{
				"/a1": kueue.InClusterQueueReason,
			},
		},
		"configurable target not needed anymore is given back": {
			clusterQueues: []*kueue.ClusterQueue{
				utiltestingapi.MakeClusterQueue("a").
					Cohort("all").
					ResourceGroup(*utiltestingapi.MakeFlavorQuotas("default").
						Resource(corev1.ResourceCPU, "3").Obj()).
					Preemption(kueue.ClusterQueuePreemption{
						WithinClusterQueue: kueue.PreemptionPolicyLowerPriority,
					}).
					Annotation(kueue.PreemptionConfigAnnotation, defaultConfigName).
					Obj(),
			},
			config: tierConfig,
			admitted: []kueue.Workload{
				// c1 is considered first, as a configurable candidate, but freeing 1 CPU
				// is not enough, and it is given back once s1 is preempted.
				*unitWl.Clone().Name("c1").
					Priority(500).
					Label("preemption-tier", "1").
					SimpleReserveQuota("a", "default", now).Obj(),
				*utiltestingapi.MakeWorkload("s1", "").Request(corev1.ResourceCPU, "2").
					Priority(10).
					SimpleReserveQuota("a", "default", now).Obj(),
			},
			incoming: utiltestingapi.MakeWorkload("a_incoming", "").Request(corev1.ResourceCPU, "2").
				Priority(100).
				Label("preemption-tier", "5").
				Condition(insufficientQuotaCond).Obj(),
			targetCQ:      "a",
			wantPreempted: sets.New("/s1"),
			wantReasons: map[string]string{
				"/s1": kueue.InClusterQueueReason,
			},
		},
		"candidate from another Cohort is given back when it doesn't help": {
			clusterQueues: []*kueue.ClusterQueue{
				utiltestingapi.MakeClusterQueue("a").
					Cohort("one").
					ResourceGroup(*utiltestingapi.MakeFlavorQuotas("default").
						Resource(corev1.ResourceCPU, "1").Obj()).
					Annotation(kueue.PreemptionConfigAnnotation, defaultConfigName).
					Obj(),
				utiltestingapi.MakeClusterQueue("b").
					Cohort("two").
					ResourceGroup(*utiltestingapi.MakeFlavorQuotas("default").
						Resource(corev1.ResourceCPU, "1").Obj()).
					Obj(),
			},
			config: anyClusterQueueConfig,
			admitted: []kueue.Workload{
				*unitWl.Clone().Name("a1").SimpleReserveQuota("a", "default", now).Obj(),
				// b belongs to another Cohort, so preempting b1 doesn't free any quota
				// for the incoming workload.
				*unitWl.Clone().Name("b1").SimpleReserveQuota("b", "default", now).Obj(),
			},
			incoming:      unitWl.Clone().Name("a_incoming").Condition(insufficientQuotaCond).Obj(),
			targetCQ:      "a",
			wantPreempted: sets.New("/a1"),
			wantReasons: map[string]string{
				"/a1": "ConfigurablePreemption",
			},
		},
		"already evicted configurable candidate is preempted first": {
			clusterQueues: baseCQs,
			config:        baseConfig,
			admitted: []kueue.Workload{
				*unitWl.Clone().Name("a1").SimpleReserveQuota("a", "default", now).Obj(),
				// Despite sorting after a1 by UID, z1 comes first as it is already evicted.
				*unitWl.Clone().Name("z1").SimpleReserveQuota("a", "default", now).
					Condition(metav1.Condition{
						Type:               kueue.WorkloadEvicted,
						Status:             metav1.ConditionTrue,
						Reason:             kueue.WorkloadEvictedByPreemption,
						LastTransitionTime: metav1.NewTime(now),
					}).Obj(),
			},
			incoming:      unitWl.Clone().Name("a_incoming").Condition(insufficientQuotaCond).Obj(),
			targetCQ:      "a",
			wantPreempted: sets.New("/z1"),
		},
		"fair sharing: configurable candidate in a ClusterQueue within nominal quota is preempted": {
			clusterQueues: []*kueue.ClusterQueue{
				utiltestingapi.MakeClusterQueue("a").
					Cohort("all").
					ResourceGroup(*utiltestingapi.MakeFlavorQuotas("default").
						Resource(corev1.ResourceCPU, "1").Obj()).
					Annotation(kueue.PreemptionConfigAnnotation, defaultConfigName).
					Obj(),
				utiltestingapi.MakeClusterQueue("b").
					Cohort("all").
					ResourceGroup(*utiltestingapi.MakeFlavorQuotas("default").
						Resource(corev1.ResourceCPU, "1").Obj()).
					Obj(),
			},
			config:      sameCohortConfig,
			fairSharing: &config.FairSharing{},
			admitted: []kueue.Workload{
				*unitWl.Clone().Name("a1").SimpleReserveQuota("a", "default", now).Obj(),
				// b is not borrowing, so the Fair Sharing ordering prunes it.
				*unitWl.Clone().Name("b1").SimpleReserveQuota("b", "default", now).Obj(),
			},
			incoming:      unitWl.Clone().Name("a_incoming").Condition(insufficientQuotaCond).Obj(),
			targetCQ:      "a",
			wantPreempted: sets.New("/b1"),
			wantReasons: map[string]string{
				"/b1": "ConfigurablePreemption",
			},
		},
		"fair sharing: configurable candidate keeps the reason of the Fair Sharing algorithm": {
			clusterQueues: []*kueue.ClusterQueue{
				utiltestingapi.MakeClusterQueue("a").
					Cohort("all").
					ResourceGroup(*utiltestingapi.MakeFlavorQuotas("default").
						Resource(corev1.ResourceCPU, "1").Obj()).
					Preemption(kueue.ClusterQueuePreemption{
						ReclaimWithinCohort: kueue.PreemptionPolicyAny,
					}).
					Annotation(kueue.PreemptionConfigAnnotation, defaultConfigName).
					Obj(),
				utiltestingapi.MakeClusterQueue("b").
					Cohort("all").
					ResourceGroup(*utiltestingapi.MakeFlavorQuotas("default").
						Resource(corev1.ResourceCPU, "1").Obj()).
					Obj(),
			},
			config:      sameCohortTierConfig,
			fairSharing: &config.FairSharing{},
			admitted: []kueue.Workload{
				// b is borrowing, so both b1 and b2 are Fair Sharing candidates, but only
				// b2 is selected by the configurable rules.
				*unitWl.Clone().Name("b1").SimpleReserveQuota("b", "default", now).Obj(),
				*unitWl.Clone().Name("b2").
					Label("preemption-tier", "1").
					SimpleReserveQuota("b", "default", now).Obj(),
			},
			incoming: unitWl.Clone().Name("a_incoming").
				Label("preemption-tier", "5").
				Condition(insufficientQuotaCond).Obj(),
			targetCQ: "a",
			// Fair Sharing alone would have preempted b1; b2 keeps the reason Fair
			// Sharing would have reported for it.
			wantPreempted: sets.New("/b2"),
			wantReasons: map[string]string{
				"/b2": kueue.InCohortReclamationReason,
			},
		},
		"fair sharing: strategies preempt the remaining targets": {
			clusterQueues: []*kueue.ClusterQueue{
				utiltestingapi.MakeClusterQueue("a").
					Cohort("all").
					ResourceGroup(*utiltestingapi.MakeFlavorQuotas("default").
						Resource(corev1.ResourceCPU, "2").Obj()).
					Preemption(kueue.ClusterQueuePreemption{
						WithinClusterQueue: kueue.PreemptionPolicyLowerPriority,
					}).
					Annotation(kueue.PreemptionConfigAnnotation, defaultConfigName).
					Obj(),
			},
			config:      tierConfig,
			fairSharing: &config.FairSharing{},
			admitted: []kueue.Workload{
				// x1 has a higher priority than the incoming workload, so it is only
				// preemptible through the configurable rules.
				*unitWl.Clone().Name("x1").
					Priority(500).
					Label("preemption-tier", "1").
					SimpleReserveQuota("a", "default", now).Obj(),
				*unitWl.Clone().Name("x2").
					Priority(10).
					SimpleReserveQuota("a", "default", now).Obj(),
			},
			incoming: utiltestingapi.MakeWorkload("a_incoming", "").Request(corev1.ResourceCPU, "2").
				Priority(100).
				Label("preemption-tier", "5").
				Condition(insufficientQuotaCond).Obj(),
			targetCQ:      "a",
			wantPreempted: sets.New("/x1", "/x2"),
			wantReasons: map[string]string{
				"/x1": "ConfigurablePreemption",
				"/x2": kueue.InClusterQueueReason,
			},
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			features.SetFeatureGateDuringTest(t, features.ConfigurablePreemption, true)
			features.SetFeatureGateDuringTest(t, features.PriorityBoost, true)
			ctx, log := utiltesting.ContextWithLog(t)
			// Set name as UID so that candidates sorting is predictable.
			for i := range tc.admitted {
				tc.admitted[i].UID = types.UID(tc.admitted[i].Name)
			}
			cl := utiltesting.NewClientBuilder().
				WithLists(&kueue.WorkloadList{Items: tc.admitted}).
				WithLists(&kueue.PreemptionConfigList{Items: []kueue.PreemptionConfig{tc.config}}).
				WithLists(&kueue.WorkloadPriorityClassList{Items: tc.workloadPriorityClasses}).
				WithLists(&schedulingv1.PriorityClassList{Items: tc.priorityClasses}).
				Build()

			cqCache := schdcache.New(cl)
			cqCache.AddOrUpdateResourceFlavor(log, utiltestingapi.MakeResourceFlavor("default").Obj())
			for _, cq := range tc.clusterQueues {
				if err := cqCache.AddClusterQueue(ctx, cq); err != nil {
					t.Fatalf("Couldn't add ClusterQueue to cache: %v", err)
				}
			}
			for _, cohort := range tc.cohorts {
				if err := cqCache.AddOrUpdateCohort(cohort); err != nil {
					t.Fatalf("Couldn't add Cohort to cache: %v", err)
				}
			}

			recorder := &utiltesting.EventRecorder{}
			preemptor := New(cl, workload.Ordering{}, recorder, tc.fairSharing, false, clocktesting.NewFakeClock(now), nil, preemptexpectations.New(), nil)

			beforeSnapshot, err := cqCache.Snapshot(ctx)
			if err != nil {
				t.Fatalf("unexpected error while building snapshot: %v", err)
			}
			snapshotWorkingCopy, err := cqCache.Snapshot(ctx)
			if err != nil {
				t.Fatalf("unexpected error while building snapshot: %v", err)
			}
			flavorName := kueue.ResourceFlavorReference("default")
			wlInfo := workload.NewInfo(tc.incoming)
			wlInfo.ClusterQueue = tc.targetCQ
			targets := preemptor.GetTargets(ctx, *wlInfo, singlePodSetAssignment(
				flavorassigner.ResourceAssignment{
					corev1.ResourceCPU: &flavorassigner.FlavorAssignment{
						Name: flavorName, Mode: flavorassigner.Preempt,
					},
				},
			), snapshotWorkingCopy)
			gotTargetsList := utilslices.Map(targets, func(t **Target) string {
				return string(workload.Key((*t).WorkloadInfo.Obj))
			})
			gotTargets := sets.New(gotTargetsList...)
			if len(targets) != len(gotTargets) {
				t.Errorf("Targets contain duplicates: %v", gotTargetsList)
			}
			if diff := cmp.Diff(tc.wantPreempted, gotTargets, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("Issued preemptions (-want,+got):\n%s", diff)
			}
			if tc.wantReasons != nil {
				gotReasons := make(map[string]string, len(targets))
				for _, target := range targets {
					gotReasons[string(workload.Key(target.WorkloadInfo.Obj))] = target.Reason
				}
				if diff := cmp.Diff(tc.wantReasons, gotReasons); diff != "" {
					t.Errorf("Preemption reasons (-want,+got):\n%s", diff)
				}
			}

			if diff := cmp.Diff(beforeSnapshot, snapshotWorkingCopy, snapCmpOpts); diff != "" {
				t.Errorf("Snapshot was modified (-initial,+end):\n%s", diff)
			}
		})
	}
}
