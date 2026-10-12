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

package limits

import (
	"slices"
	"sync"
	"time"

	kueuealpha "sigs.k8s.io/kueue/apis/kueue/v1alpha1"
	"sigs.k8s.io/kueue/pkg/scheduler/preemption/policy"
	"sigs.k8s.io/kueue/pkg/workload"
)

// Resolve returns, for each PreemptionLimit, the scope value consumed by
// preempting target to admit preemptor.
func Resolve(preemptionLimits []kueuealpha.PreemptionLimit, preemptor, target *workload.Info) policy.ConsumedLimits {
	if len(preemptionLimits) == 0 {
		return nil
	}
	consumed := make(policy.ConsumedLimits, len(preemptionLimits))
	for i := range preemptionLimits {
		limit := &preemptionLimits[i]
		consumed[policy.PreemptionLimitReference(limit.Name)] = scopeValue(limit.Spec.Scope, preemptor, target)
	}
	return consumed
}

func scopeValue(scope kueuealpha.PreemptionLimitScope, preemptor, target *workload.Info) policy.PreemptionLimitScopeValue {
	switch scope {
	case kueuealpha.PreemptingCQLimitScope:
		return policy.PreemptionLimitScopeValue(preemptor.ClusterQueue)
	case kueuealpha.PreemptedCQLimitScope:
		return policy.PreemptionLimitScopeValue(target.ClusterQueue)
	case kueuealpha.PreemptedWorkloadLimitScope:
		return policy.PreemptionLimitScopeValue(workload.Key(target.Obj))
	default:
		// GlobalPreemptionLimitScope, as the API admits no other scope.
		return policy.PreemptionLimitScopeValue(kueuealpha.GlobalPreemptionLimitScope)
	}
}

// events holds preemption timestamps in ascending order.
type events []time.Time

// insert adds ts while keeping the events sorted. Timestamps usually arrive in
// order, so the insertion point is searched from the tail.
func (e events) insert(ts time.Time) events {
	i := len(e)
	for i > 0 && e[i-1].After(ts) {
		i--
	}
	return slices.Insert(e, i, ts)
}

// PreemptionLimitTracker records the timestamps of confirmed configurable preemptions for
// each PreemptionLimit and scope value. It is safe for concurrent use.
type PreemptionLimitTracker struct {
	mu     sync.RWMutex
	events map[policy.PreemptionLimitReference]map[policy.PreemptionLimitScopeValue]events
}

// NewPreemptionLimitTracker creates an empty PreemptionLimitTracker.
func NewPreemptionLimitTracker() *PreemptionLimitTracker {
	return &PreemptionLimitTracker{
		events: make(map[policy.PreemptionLimitReference]map[policy.PreemptionLimitScopeValue]events),
	}
}

// Record records a preemption at ts for every PreemptionLimit and scope value
// pair in consumedLimits.
func (t *PreemptionLimitTracker) Record(ts time.Time, consumedLimits policy.ConsumedLimits) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for limit, scopeValue := range consumedLimits {
		scopes, found := t.events[limit]
		if !found {
			scopes = make(map[policy.PreemptionLimitScopeValue]events)
			t.events[limit] = scopes
		}
		scopes[scopeValue] = scopes[scopeValue].insert(ts)
	}
}
