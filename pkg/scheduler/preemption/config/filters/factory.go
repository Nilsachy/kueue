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
	"github.com/go-logr/logr"

	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	schdcache "sigs.k8s.io/kueue/pkg/cache/scheduler"
	"sigs.k8s.io/kueue/pkg/workload"
)

// NewCandidateFilters compiles PreemptionConfigPreemptionCandidateSelector rules into CandidateFilters & RejectAll boolean (if preemptor doesn't pass).
// It returns (CandidateFilters{}, true) if the selector fails to compile and all the candidates should be rejected.
func NewCandidateFilters(
	log logr.Logger,
	selector *kueue.PreemptionConfigPreemptionCandidateSelector,
	preemptor *workload.Info,
	snapshot *schdcache.Snapshot,
) (CandidateFilters, bool) {
	if selector == nil {
		return CandidateFilters{}, false
	}

	cqScopeFilters, wlScopeFilters, ok := buildScopeFilters(log, selector.Scope, preemptor, snapshot)
	if !ok {
		return CandidateFilters{}, true
	}
	wlNumericFilters := buildNumericLabelFilters(log, selector.NumericLabels, preemptor)
	wlPriorityFilters := buildPriorityFilters(log, selector, preemptor)

	var wlFilters []WorkloadFilter
	wlFilters = append(wlFilters, wlScopeFilters...)
	wlFilters = append(wlFilters, wlNumericFilters...)
	wlFilters = append(wlFilters, wlPriorityFilters...)

	return CandidateFilters{
		CQFilters: cqScopeFilters,
		WLFilters: wlFilters,
	}, false
}

func buildScopeFilters(
	log logr.Logger,
	scope kueue.PreemptionConfigPreemptionQueueScope,
	preemptor *workload.Info,
	snapshot *schdcache.Snapshot,
) ([]ClusterQueueFilter, []WorkloadFilter, bool) {
	switch scope {
	case kueue.WithinLocalQueue:
		// CQ Level: Prune all other ClusterQueues
		// WL Level: Narrow down workloads to those matching exactly same LocalQueue
		return []ClusterQueueFilter{NewWithinClusterQueueFilter(preemptor.ClusterQueue)},
			[]WorkloadFilter{NewWithinLocalQueueFilter(preemptor.Obj.Namespace, preemptor.Obj.Spec.QueueName)}, true

	case kueue.WithinClusterQueue:
		return []ClusterQueueFilter{NewWithinClusterQueueFilter(preemptor.ClusterQueue)}, nil, true

	case kueue.WithinParentCohort:
		return []ClusterQueueFilter{NewWithinParentCohortFilter(preemptor.ClusterQueue, snapshot)}, nil, true

	case kueue.WithinCohortTree:
		return []ClusterQueueFilter{NewWithinCohortTreeFilter(preemptor.ClusterQueue, snapshot)}, nil, true

	case kueue.AnyClusterQueue:
		return nil, nil, true

	default:
		log.V(3).Info("Unsupported or unhandled candidate scope evaluated; 0 candidates permitted", "scope", scope)
		return nil, nil, false
	}
}

func buildNumericLabelFilters(
	log logr.Logger,
	labels []kueue.PreemptionConfigNumericLabelConstraint,
	preemptor *workload.Info,
) []WorkloadFilter {
	if len(labels) == 0 {
		return nil
	}
	filters := make([]WorkloadFilter, 0, len(labels))
	for _, numConstraint := range labels {
		filters = append(filters, NewNumericLabelFilter(log, numConstraint, preemptor))
	}
	return filters
}

func buildPriorityFilters(
	log logr.Logger,
	selector *kueue.PreemptionConfigPreemptionCandidateSelector,
	preemptor *workload.Info,
) []WorkloadFilter {
	if selector == nil {
		return nil
	}
	var filters []WorkloadFilter
	if selector.RelativeWorkloadPriority != nil {
		filters = append(filters, NewRelativeWorkloadPriorityFilter(log, *selector.RelativeWorkloadPriority, preemptor))
	}
	return filters
}
