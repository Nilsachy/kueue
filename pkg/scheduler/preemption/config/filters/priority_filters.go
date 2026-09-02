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
	"context"

	"github.com/go-logr/logr"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"

	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	"sigs.k8s.io/kueue/pkg/util/priority"
	"sigs.k8s.io/kueue/pkg/workload"
)

// CheckPreemptingWorkloadPriority evaluates whether a preemptor workload satisfies the PreemptingWorkloadPrioritySelector constraint.
// If selector is nil or empty, returns true (all preemptors allowed).
// If preemptor has no priorityClassRef or priority class is not found, returns false.
func CheckPreemptingWorkloadPriority(
	ctx context.Context,
	log logr.Logger,
	selector *metav1.LabelSelector,
	preemptor *workload.Info,
	reader client.Reader,
) bool {
	if selector == nil {
		return true
	}
	ls, err := metav1.LabelSelectorAsSelector(selector)
	if err != nil {
		log.Error(err, "Invalid PreemptingWorkloadPrioritySelector", "selector", selector)
		return false
	}
	if ls.Empty() {
		return true
	}
	if preemptor.Obj.Spec.PriorityClassRef == nil {
		return false
	}
	return matchPriorityClassLabels(ctx, log, ls, preemptor.Obj.Spec.PriorityClassRef, reader)
}

// candidateWorkloadPriorityFilter matches candidates whose PriorityClass satisfies a label selector.
type candidateWorkloadPriorityFilter struct {
	ctx      context.Context
	log      logr.Logger
	selector labels.Selector
	reader   client.Reader
}

// NewCandidateWorkloadPriorityFilter creates a WorkloadFilter to evaluate candidate workloads
// based on their PriorityClass / WorkloadPriorityClass labels.
func NewCandidateWorkloadPriorityFilter(
	ctx context.Context,
	log logr.Logger,
	selector labels.Selector,
	reader client.Reader,
) WorkloadFilter {
	return &candidateWorkloadPriorityFilter{
		ctx:      ctx,
		log:      log.WithValues("filter", "CandidateWorkloadPriority"),
		selector: selector,
		reader:   reader,
	}
}

// Matches evaluates a candidate workload's priority class labels against the selector.
func (f *candidateWorkloadPriorityFilter) Matches(wl *workload.Info) bool {
	if f.selector == nil || f.selector.Empty() {
		return true
	}
	if wl.Obj.Spec.PriorityClassRef == nil {
		return false
	}
	return matchPriorityClassLabels(f.ctx, f.log, f.selector, wl.Obj.Spec.PriorityClassRef, f.reader)
}

func matchPriorityClassLabels(
	ctx context.Context,
	log logr.Logger,
	sel labels.Selector,
	ref *kueue.PriorityClassRef,
	reader client.Reader,
) bool {
	if sel == nil || sel.Empty() {
		return true
	}
	if ref == nil || reader == nil {
		return false
	}
	pcLabels, err := priority.GetPriorityClassLabels(ctx, reader, ref)
	if err != nil {
		if apierrors.IsNotFound(err) {
			log.V(3).Info("Priority class not found", "ref", ref)
		} else {
			log.Error(err, "Failed to get PriorityClass labels", "ref", ref)
		}
		return false
	}
	return sel.Matches(labels.Set(pcLabels))
}

type relativeWorkloadPriorityFilter struct {
	log               logr.Logger
	relation          kueue.RelativeConstraint
	preemptorPriority int64
}

// NewRelativeWorkloadPriorityFilter creates a WorkloadFilter to evaluate candidate workloads
// based on relative workload priority compared against the preemptor workload.
// The effective priority (accounting for priority boost if configured) is used for comparison.
func NewRelativeWorkloadPriorityFilter(log logr.Logger, relation kueue.RelativeConstraint, preemptor *workload.Info) WorkloadFilter {
	filterLog := log.WithValues("filter", "RelativeWorkloadPriority", "relation", relation)
	preemptorLog := filterLog.WithValues("preemptor", klog.KObj(preemptor.Obj))
	preemptorPriority := priority.EffectivePriority(preemptorLog, preemptor.Obj)

	return &relativeWorkloadPriorityFilter{
		log:               filterLog,
		relation:          relation,
		preemptorPriority: preemptorPriority,
	}
}

// Matches evaluates a candidate workload's effective priority against the preemptor's priority.
func (f *relativeWorkloadPriorityFilter) Matches(wl *workload.Info) bool {
	candLog := f.log.WithValues("candidate", klog.KObj(wl.Obj))
	candPriority := priority.EffectivePriority(candLog, wl.Obj)
	return matchesRelation(candLog, &f.relation, candPriority, f.preemptorPriority)
}
