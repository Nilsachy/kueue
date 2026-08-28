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
	"sigs.k8s.io/kueue/pkg/cache/hierarchy"
	schdcache "sigs.k8s.io/kueue/pkg/cache/scheduler"
	utiltestingapi "sigs.k8s.io/kueue/pkg/util/testing/v1beta2"
	"sigs.k8s.io/kueue/pkg/workload"
)

type snapshotBuilder struct {
	mgr hierarchy.Manager[*schdcache.ClusterQueueSnapshot, *schdcache.CohortSnapshot]
}

func newSnapshotBuilder() *snapshotBuilder {
	return &snapshotBuilder{
		mgr: hierarchy.NewManager(func(name kueuev1beta2.CohortReference) *schdcache.CohortSnapshot {
			return &schdcache.CohortSnapshot{
				Name:   name,
				Cohort: hierarchy.NewCohort[*schdcache.ClusterQueueSnapshot, *schdcache.CohortSnapshot](),
			}
		}),
	}
}

func (b *snapshotBuilder) Cohort(name, parent kueuev1beta2.CohortReference) *snapshotBuilder {
	b.mgr.AddCohort(name)
	if parent != "" {
		b.mgr.UpdateCohortEdge(name, parent)
	}
	return b
}

func (b *snapshotBuilder) ClusterQueue(name kueuev1beta2.ClusterQueueReference, parent kueuev1beta2.CohortReference) *snapshotBuilder {
	b.mgr.AddClusterQueue(&schdcache.ClusterQueueSnapshot{Name: name})
	if parent != "" {
		b.mgr.UpdateClusterQueueEdge(name, parent)
	}
	return b
}

func (b *snapshotBuilder) Build() *schdcache.Snapshot {
	return &schdcache.Snapshot{Manager: b.mgr}
}

func makeWorkloadInfo(name, namespace string, localQueue kueuev1beta2.LocalQueueName, clusterQueue kueuev1beta2.ClusterQueueReference) *workload.Info {
	wl := utiltestingapi.MakeWorkload(name, namespace).Queue(localQueue).Obj()
	info := workload.NewInfo(wl)
	info.ClusterQueue = clusterQueue
	return info
}
