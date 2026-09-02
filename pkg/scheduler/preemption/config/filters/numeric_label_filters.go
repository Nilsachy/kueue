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
	"strconv"

	"github.com/go-logr/logr"
	"k8s.io/klog/v2"

	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	"sigs.k8s.io/kueue/pkg/workload"
)

type numericLabelFilter struct {
	log          logr.Logger
	constraint   kueue.NumericLabelConstraint
	preemptorVal *int32
}

// NewNumericLabelFilter creates a WorkloadFilter to evaluate candidate workloads
// based on customized integer labels and relationship boundaries with the preemptor workload.
func NewNumericLabelFilter(log logr.Logger, constraint kueue.NumericLabelConstraint, preemptor *workload.Info) WorkloadFilter {
	filterLog := log.WithValues("filter", "NumericLabels", "key", constraint.Key)
	if constraint.DefaultValue != nil {
		filterLog = filterLog.WithValues("default", *constraint.DefaultValue)
	}

	f := &numericLabelFilter{
		log:        filterLog,
		constraint: constraint,
	}

	if constraint.Relation != nil {
		preemptorLog := filterLog.WithValues("preemptor", klog.KObj(preemptor.Obj))
		if val, ok := tryGetLabelValue(preemptorLog, preemptor, constraint.Key, constraint.DefaultValue); ok {
			f.preemptorVal = new(val)
		} else {
			preemptorLog.V(2).Info("Preemptor missing required numeric label without defaultValue; relational comparison will not match any candidates")
		}
	}

	return f
}

// Matches evaluates a candidate workload against absolute bounds and relationship boundaries with the preemptor.
func (f *numericLabelFilter) Matches(wl *workload.Info) bool {
	candLog := f.log.WithValues("candidate", klog.KObj(wl.Obj))
	candVal, ok := tryGetLabelValue(candLog, wl, f.constraint.Key, f.constraint.DefaultValue)
	if !ok {
		// Exclude the candidate from preemption since it lacks both the label and default
		return false
	}

	// 1. Check absolute bounds (MinValue, MaxValue)
	if f.constraint.MinValue != nil && candVal < *f.constraint.MinValue {
		return false
	}
	if f.constraint.MaxValue != nil && candVal > *f.constraint.MaxValue {
		return false
	}

	// 2. Check relation constraint compared to preemptor
	if f.constraint.Relation != nil {
		if f.preemptorVal == nil {
			// If preemptor has no valid label and no default is set, relation restrictions cannot be applied
			return false
		}
		return matchesRelation(candLog, f.constraint.Relation, int64(candVal), int64(*f.preemptorVal))
	}

	return true
}

// tryGetLabelValue safely extracts a numeric int32 label from a workload.
// If the label is incorrectly formatted or missing, it evaluates the optionally configured default.
func tryGetLabelValue(log logr.Logger, wl *workload.Info, key string, def *int32) (int32, bool) {
	if wl.Obj.Labels == nil {
		if def != nil {
			return *def, true
		}
		return 0, false
	}

	valStr, exists := wl.Obj.Labels[key]
	if !exists {
		if def != nil {
			return *def, true
		}
		return 0, false
	}

	val, err := strconv.ParseInt(valStr, 10, 32)
	if err != nil {
		log.V(3).Info("Failed to parse label into integer as expected; falling back to default", "value", valStr, "error", err)
		if def != nil {
			return *def, true
		}
		return 0, false
	}

	return int32(val), true
}
