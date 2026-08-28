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
	kueuev1beta2 "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	schdcache "sigs.k8s.io/kueue/pkg/cache/scheduler"
	"sigs.k8s.io/kueue/pkg/workload"
)

// sameClusterQueueFilter permits only candidate workloads residing in the exact same ClusterQueue as the preemptor.
type sameClusterQueueFilter struct {
	preemptorCQ kueuev1beta2.ClusterQueueReference
}

// NewSameClusterQueueFilter creates a ClusterQueueFilter permitting only the specified ClusterQueue.
func NewSameClusterQueueFilter(preemptorCQ kueuev1beta2.ClusterQueueReference) ClusterQueueFilter {
	return &sameClusterQueueFilter{preemptorCQ: preemptorCQ}
}

func (f *sameClusterQueueFilter) Matches(cq *schdcache.ClusterQueueSnapshot) bool {
	return cq.Name == f.preemptorCQ
}

// sameCohortFilter permits ClusterQueues sharing the immediate parent Cohort, or the preemptor's own ClusterQueue.
type sameCohortFilter struct {
	preemptorCQ     kueuev1beta2.ClusterQueueReference
	preemptorCohort kueuev1beta2.CohortReference
	hasCohort       bool
}

// NewSameCohortFilter encapsulates preemptor cohort resolution and caches the match target.
func NewSameCohortFilter(preemptorCQ kueuev1beta2.ClusterQueueReference, snapshot *schdcache.Snapshot) ClusterQueueFilter {
	f := &sameCohortFilter{preemptorCQ: preemptorCQ}
	if snapshotCQ := snapshot.ClusterQueue(preemptorCQ); snapshotCQ != nil && snapshotCQ.HasParent() {
		f.preemptorCohort = snapshotCQ.Parent().GetName()
		f.hasCohort = true
	}
	return f
}

func (f *sameCohortFilter) Matches(cq *schdcache.ClusterQueueSnapshot) bool {
	// The preemptor's own ClusterQueue is always within the same cohort boundary.
	if cq.Name == f.preemptorCQ {
		return true
	}
	if !f.hasCohort || !cq.HasParent() {
		return false
	}
	return cq.Parent().GetName() == f.preemptorCohort
}

// sameCohortTreeFilter permits ClusterQueues in the same Cohort Tree (sharing the root Cohort ancestor),
// or the preemptor's own ClusterQueue.
type sameCohortTreeFilter struct {
	preemptorCQ         kueuev1beta2.ClusterQueueReference
	preemptorRootCohort kueuev1beta2.CohortReference
	hasCohort           bool
}

// NewSameCohortTreeFilter encapsulates preemptor root cohort resolution and caches the match target.
func NewSameCohortTreeFilter(preemptorCQ kueuev1beta2.ClusterQueueReference, snapshot *schdcache.Snapshot) ClusterQueueFilter {
	f := &sameCohortTreeFilter{preemptorCQ: preemptorCQ}
	if snapshotCQ := snapshot.ClusterQueue(preemptorCQ); snapshotCQ != nil && snapshotCQ.HasParent() {
		if root := snapshotCQ.Parent().Root(); root != nil {
			f.preemptorRootCohort = root.GetName()
			f.hasCohort = true
		}
	}
	return f
}

func (f *sameCohortTreeFilter) Matches(cq *schdcache.ClusterQueueSnapshot) bool {
	// The preemptor's own ClusterQueue is always within the same cohort tree boundary.
	if cq.Name == f.preemptorCQ {
		return true
	}
	if !f.hasCohort || !cq.HasParent() {
		return false
	}
	root := cq.Parent().Root()
	return root != nil && root.GetName() == f.preemptorRootCohort
}

// sameLocalQueueFilter is a WorkloadFilter matching workloads in the exact same Namespace and LocalQueue.
type sameLocalQueueFilter struct {
	namespace string
	queueName kueuev1beta2.LocalQueueName
}

// NewSameLocalQueueFilter creates a WorkloadFilter matching the given Namespace and LocalQueue.
func NewSameLocalQueueFilter(namespace string, queueName kueuev1beta2.LocalQueueName) WorkloadFilter {
	return &sameLocalQueueFilter{
		namespace: namespace,
		queueName: queueName,
	}
}

func (f *sameLocalQueueFilter) Matches(wl *workload.Info) bool {
	return wl.Obj.Namespace == f.namespace && wl.Obj.Spec.QueueName == f.queueName
}

// rejectAllCQFilter unconditionally rejects all ClusterQueues (used when an unknown relation is specified).
type rejectAllCQFilter struct{}

// NewRejectAllCQFilter creates a ClusterQueueFilter that rejects all ClusterQueues.
func NewRejectAllCQFilter() ClusterQueueFilter {
	return &rejectAllCQFilter{}
}

func (f *rejectAllCQFilter) Matches(_ *schdcache.ClusterQueueSnapshot) bool {
	return false
}
