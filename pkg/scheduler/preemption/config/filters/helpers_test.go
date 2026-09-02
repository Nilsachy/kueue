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
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	"sigs.k8s.io/kueue/pkg/cache/hierarchy"
	schdcache "sigs.k8s.io/kueue/pkg/cache/scheduler"
)

type snapshotBuilder struct {
	mgr hierarchy.Manager[*schdcache.ClusterQueueSnapshot, *schdcache.CohortSnapshot]
}

func newSnapshotBuilder() *snapshotBuilder {
	return &snapshotBuilder{
		mgr: hierarchy.NewManager(func(name kueue.CohortReference) *schdcache.CohortSnapshot {
			return &schdcache.CohortSnapshot{
				Name:   name,
				Cohort: hierarchy.NewCohort[*schdcache.ClusterQueueSnapshot, *schdcache.CohortSnapshot](),
			}
		}),
	}
}

func (b *snapshotBuilder) Cohort(name, parent kueue.CohortReference) *snapshotBuilder {
	b.mgr.AddCohort(name)
	if parent != "" {
		b.mgr.UpdateCohortEdge(name, parent)
	}
	return b
}

func (b *snapshotBuilder) ClusterQueue(name kueue.ClusterQueueReference, parent kueue.CohortReference) *snapshotBuilder {
	b.mgr.AddClusterQueue(&schdcache.ClusterQueueSnapshot{Name: name})
	if parent != "" {
		b.mgr.UpdateClusterQueueEdge(name, parent)
	}
	return b
}

func (b *snapshotBuilder) Build() *schdcache.Snapshot {
	return &schdcache.Snapshot{
		Manager: b.mgr,
	}
}
