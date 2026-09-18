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
	"slices"
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
			Rules: []kueue.PreemptionConfigPreemptionRule{
				{
					Name:             "test-rule-one",
					ActivationPolicy: kueue.PreemptionConfigActivationPolicy{Trigger: kueue.InsufficientQuota},
					CandidateSelectors: []kueue.PreemptionConfigPreemptionCandidateSelector{
						{
							Scope: kueue.WithinClusterQueue,
						},
					},
				},
			},
		},
	}

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
		wantPreempted           sets.Set[string]
		// wantPreemptedAnyOf lists the acceptable outcomes for cases where more
		// candidates are eligible than are needed to fit the incoming workload.
		// Candidates are collected in map iteration order and preemption stops at
		// the first fit, so any of the eligible candidates may be picked.
		// Remove once configurable preemption candidates are merged with the
		// classical and fair preemption strategies.
		wantPreemptedAnyOf []sets.Set[string]
		wantReasons        map[string]string
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
			incoming: unitWl.Clone().Name("a_incoming").Condition(insufficientQuotaCond).Obj(),
			targetCQ: "a",
			// a1 and a2 are equally eligible, and only one of them is needed.
			wantPreemptedAnyOf: []sets.Set[string]{sets.New("/a1"), sets.New("/a2")},
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
					Type:               string(kueue.QuotaFeasibleAndInsufficientTopology),
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
					Rules: []kueue.PreemptionConfigPreemptionRule{
						{
							Name:             "test-rule-one",
							ActivationPolicy: kueue.PreemptionConfigActivationPolicy{Trigger: kueue.InsufficientQuota},
							CandidateSelectors: []kueue.PreemptionConfigPreemptionCandidateSelector{
								{
									Scope: kueue.WithinClusterQueue,
									NumericLabels: []kueue.PreemptionConfigNumericLabelConstraint{
										{
											Key:        "test-label",
											Comparison: ptr.To(kueue.LessThan),
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
					Rules: []kueue.PreemptionConfigPreemptionRule{
						{
							Name:             "test-rule-one",
							ActivationPolicy: kueue.PreemptionConfigActivationPolicy{Trigger: kueue.InsufficientQuota},
							PreemptorSelector: &metav1.LabelSelector{
								MatchExpressions: []metav1.LabelSelectorRequirement{
									{
										Key:      "test",
										Operator: "invalid",
									},
								},
							},
							CandidateSelectors: []kueue.PreemptionConfigPreemptionCandidateSelector{
								{
									Scope: kueue.WithinClusterQueue,
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
					Rules: []kueue.PreemptionConfigPreemptionRule{
						{
							Name:             "relative-priority-rule",
							ActivationPolicy: kueue.PreemptionConfigActivationPolicy{Trigger: kueue.InsufficientQuota},
							CandidateSelectors: []kueue.PreemptionConfigPreemptionCandidateSelector{
								{
									Scope:                    kueue.WithinClusterQueue,
									RelativeWorkloadPriority: ptr.To(kueue.LessThan),
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
					Rules: []kueue.PreemptionConfigPreemptionRule{
						{
							Name:             "boost-priority-rule",
							ActivationPolicy: kueue.PreemptionConfigActivationPolicy{Trigger: kueue.InsufficientQuota},
							CandidateSelectors: []kueue.PreemptionConfigPreemptionCandidateSelector{
								{
									Scope:                    kueue.WithinClusterQueue,
									RelativeWorkloadPriority: ptr.To(kueue.LessThan),
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
		"results from both classical and configurable preemption algorithms are added together without duplicates": {
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
					Rules: []kueue.PreemptionConfigPreemptionRule{
						{
							Name:             "candidate-tier-rule",
							ActivationPolicy: kueue.PreemptionConfigActivationPolicy{Trigger: kueue.InsufficientQuota},
							CandidateSelectors: []kueue.PreemptionConfigPreemptionCandidateSelector{
								{
									Scope: kueue.WithinClusterQueue,
									NumericLabels: []kueue.PreemptionConfigNumericLabelConstraint{
										{
											Key:        "preemption-tier",
											Comparison: ptr.To(kueue.LessThan),
										},
									},
								},
							},
						},
					},
				},
			},
			admitted: []kueue.Workload{
				// a1 has no tier label, so it is not a candidate for the configurable rule.
				*unitWl.Clone().Name("a1").
					Priority(10).
					SimpleReserveQuota("a", "default", now).Obj(),
				*unitWl.Clone().Name("a2").
					Priority(20).
					Label("preemption-tier", "1").
					SimpleReserveQuota("a", "default", now).Obj(),
				*unitWl.Clone().Name("a3").
					Priority(30).
					Label("preemption-tier", "2").
					SimpleReserveQuota("a", "default", now).Obj(),
			},
			incoming: unitWl.Clone().Name("a_incoming").
				Priority(100).
				Label("preemption-tier", "5").
				Request(corev1.ResourceCPU, "2").
				Condition(insufficientQuotaCond).Obj(),
			targetCQ:      "a",
			wantPreempted: sets.New("/a1", "/a2", "/a3"),
			wantReasons: map[string]string{
				"/a1": kueue.InClusterQueueReason,
				"/a2": kueue.InClusterQueueReason,
				"/a3": "ConfigurablePreemption",
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
			preemptor := New(cl, workload.Ordering{}, recorder, nil, false, clocktesting.NewFakeClock(now), nil, preemptexpectations.New(), nil)

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
			if len(tc.wantPreemptedAnyOf) > 0 {
				if !slices.ContainsFunc(tc.wantPreemptedAnyOf, gotTargets.Equal) {
					t.Errorf("Issued preemptions %v, want one of %v", sets.List(gotTargets), tc.wantPreemptedAnyOf)
				}
			} else if diff := cmp.Diff(tc.wantPreempted, gotTargets, cmpopts.EquateEmpty()); diff != "" {
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
