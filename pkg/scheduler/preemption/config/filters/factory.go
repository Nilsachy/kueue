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

	kueuev1beta2 "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	schdcache "sigs.k8s.io/kueue/pkg/cache/scheduler"
	"sigs.k8s.io/kueue/pkg/workload"
)

// NewCandidateFilters compiles PreemptionCandidateSelector rules into CandidateFilters.
func NewCandidateFilters(
	log logr.Logger,
	selector *kueuev1beta2.PreemptionCandidateSelector,
	preemptor *workload.Info,
	snapshot *schdcache.Snapshot,
) CandidateFilters {
	if selector == nil {
		return CandidateFilters{}
	}

	cqRelationFilters, wlRelationFilters := buildRelationFilters(log, selector.RelationRequirement, preemptor, snapshot)
	wlNumericFilters := buildNumericLabelFilters(log, selector.NumericLabels, preemptor)
	wlPriorityFilters := buildPriorityFilters(log, selector.RelativeWorkloadPriority, preemptor)

	var wlFilters []WorkloadFilter
	wlFilters = append(wlFilters, wlRelationFilters...)
	wlFilters = append(wlFilters, wlNumericFilters...)
	wlFilters = append(wlFilters, wlPriorityFilters...)

	return CandidateFilters{
		CQFilters: cqRelationFilters,
		WLFilters: wlFilters,
	}
}

func buildRelationFilters(
	log logr.Logger,
	relation kueuev1beta2.PreemptionRelationConstraint,
	preemptor *workload.Info,
	snapshot *schdcache.Snapshot,
) ([]ClusterQueueFilter, []WorkloadFilter) {
	switch relation {
	case kueuev1beta2.SameLocalQueue:
		// CQ Level: Prune all other ClusterQueues
		// WL Level: Narrow down workloads to those matching exactly same LocalQueue
		return []ClusterQueueFilter{NewSameClusterQueueFilter(preemptor.ClusterQueue)},
			[]WorkloadFilter{NewSameLocalQueueFilter(preemptor.Obj.Namespace, preemptor.Obj.Spec.QueueName)}

	case kueuev1beta2.SameClusterQueue:
		return []ClusterQueueFilter{NewSameClusterQueueFilter(preemptor.ClusterQueue)}, nil

	case kueuev1beta2.SameCohort:
		return []ClusterQueueFilter{NewSameCohortFilter(preemptor.ClusterQueue, snapshot)}, nil

	case kueuev1beta2.SameCohortTree:
		return []ClusterQueueFilter{NewSameCohortTreeFilter(preemptor.ClusterQueue, snapshot)}, nil

	case kueuev1beta2.AnyClusterQueue:
		return nil, nil

	default:
		log.V(3).Info("Unsupported or unhandled relation constraint evaluated; 0 candidates permitted", "relation", relation)
		return []ClusterQueueFilter{NewRejectAllCQFilter()}, nil
	}
}

func buildNumericLabelFilters(
	log logr.Logger,
	labels []kueuev1beta2.NumericLabelConstraint,
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
	relativePriority *kueuev1beta2.RelativeConstraint,
	preemptor *workload.Info,
) []WorkloadFilter {
	if relativePriority == nil {
		return nil
	}
	return []WorkloadFilter{NewRelativeWorkloadPriorityFilter(log, *relativePriority, preemptor)}
}

