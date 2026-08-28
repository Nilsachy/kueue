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

package scheduler

import (
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	"sigs.k8s.io/kueue/pkg/cache/hierarchy"
	"sigs.k8s.io/kueue/pkg/resources"
	utiltestingapi "sigs.k8s.io/kueue/pkg/util/testing/v1beta2"
	"sigs.k8s.io/kueue/pkg/workload"
)

func MakeWorkloadInfo(name, namespace string, localQueue kueue.LocalQueueName, clusterQueue kueue.ClusterQueueReference) *workload.Info {
	wl := utiltestingapi.MakeWorkload(name, namespace).Queue(localQueue).Obj()
	info := workload.NewInfo(wl)
	info.ClusterQueue = clusterQueue
	return info
}

type SnapshotBuilder struct {
	mgr hierarchy.Manager[*ClusterQueueSnapshot, *CohortSnapshot]
}

func NewSnapshotBuilder() *SnapshotBuilder {
	return &SnapshotBuilder{
		mgr: hierarchy.NewManager(func(name kueue.CohortReference) *CohortSnapshot {
			return &CohortSnapshot{
				Name:   name,
				Cohort: hierarchy.NewCohort[*ClusterQueueSnapshot](),
			}
		}),
	}
}

func (b *SnapshotBuilder) Cohort(name, parent kueue.CohortReference) *SnapshotBuilder {
	b.mgr.AddCohort(name)
	if parent != "" {
		b.mgr.UpdateCohortEdge(name, parent)
	}
	return b
}

func (b *SnapshotBuilder) CohortWithQuotaAndUsage(name, parent kueue.CohortReference, quota, usage resources.FlavorResourceQuantities) *SnapshotBuilder {
	b.mgr.AddCohort(name)
	if parent != "" {
		b.mgr.UpdateCohortEdge(name, parent)
	}
	if cohort := b.mgr.Cohort(name); cohort != nil {
		cohort.ResourceNode.SubtreeQuota = quota
		cohort.ResourceNode.Usage = usage
	}
	return b
}

func (b *SnapshotBuilder) ClusterQueue(name kueue.ClusterQueueReference, parent kueue.CohortReference) *SnapshotBuilder {
	b.mgr.AddClusterQueue(&ClusterQueueSnapshot{Name: name})
	if parent != "" {
		b.mgr.UpdateClusterQueueEdge(name, parent)
	}
	return b
}

func (b *SnapshotBuilder) ClusterQueueWithQuotas(name kueue.ClusterQueueReference, parent kueue.CohortReference, quotas map[resources.FlavorResource]ResourceQuota, subtreeQuota, usage resources.FlavorResourceQuantities) *SnapshotBuilder {
	b.mgr.AddClusterQueue(&ClusterQueueSnapshot{
		Name: name,
		ResourceNode: resourceNode{
			Quotas:       quotas,
			SubtreeQuota: subtreeQuota,
			Usage:        usage,
		},
	})
	if parent != "" {
		b.mgr.UpdateClusterQueueEdge(name, parent)
	}
	return b
}

func (b *SnapshotBuilder) Build() *Snapshot {
	return &Snapshot{Manager: b.mgr}
}
