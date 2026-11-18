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
	"maps"
	"slices"

	"k8s.io/klog/v2"

	schdcache "sigs.k8s.io/kueue/pkg/cache/scheduler"
	"sigs.k8s.io/kueue/pkg/scheduler/preemption/policy"
	"sigs.k8s.io/kueue/pkg/workload"
)

// PreemptionLimitBudget counts the preemptions taken from the PreemptionLimits
// during one pass of candidate selection.
type PreemptionLimitBudget struct {
	limiter *PreemptionLimiter
	// taken holds the preemptions taken from this budget.
	taken preemptionCounts
}

// NewBudget returns an empty budget for one pass of candidate selection.
// A budget from a nil PreemptionLimiter imposes no limits.
func (l *PreemptionLimiter) NewBudget() *PreemptionLimitBudget {
	return &PreemptionLimitBudget{
		limiter: l,
		taken:   preemptionCounts{},
	}
}

// TryConsume takes one preemption from every limit that preempting target
// consumes, and returns those limits.
//
// If any limit has no budget left, it takes nothing and returns false along
// with the strictest BlockingPreemptionLimit across the exhausted limits on target.
func (b *PreemptionLimitBudget) TryConsume(
	preemptor, target *workload.Info,
	ruleReference policy.PreemptionConfigRuleReference,
	preemptorCQ, targetCQ *schdcache.ClusterQueueSnapshot,
) (policy.ConsumedLimits, *policy.BlockingPreemptionLimit, bool) {
	consumedLimits := b.limiter.consumedLimits(preemptor, target, ruleReference, preemptorCQ, targetCQ)
	if len(consumedLimits) == 0 {
		return nil, nil, true
	}
	var blockingLimit *policy.BlockingPreemptionLimit
	for _, limit := range slices.Sorted(maps.Keys(consumedLimits)) {
		consumed := consumedLimits[limit]
		if b.remaining(limit, consumed.ScopeValue) > 0 {
			continue
		}
		slotTime := b.limiter.snapshot.nextSlotTime(limit, consumed.ScopeValue)
		if slotTime == nil {
			blockingLimit = &policy.BlockingPreemptionLimit{
				Limit: limit,
				Scope: b.limiter.snapshot.scope(limit),
			}
			break
		}
		if blockingLimit == nil || slotTime.After(*blockingLimit.RetryAt) {
			blockingLimit = &policy.BlockingPreemptionLimit{
				Limit:   limit,
				Scope:   b.limiter.snapshot.scope(limit),
				RetryAt: slotTime,
			}
		}
	}
	if blockingLimit != nil {
		log := b.limiter.log.V(5)
		if blockingLimit.RetryAt != nil {
			log = log.WithValues("nextSlotTime", *blockingLimit.RetryAt)
		}
		log.Info("Skipping preemption candidate exceeding a PreemptionLimit",
			"candidate", klog.KObj(target.Obj),
			"preemptionLimit", blockingLimit.Limit,
			"scopeValue", consumedLimits[blockingLimit.Limit].ScopeValue,
		)
		return nil, blockingLimit, false
	}
	for limit, consumed := range consumedLimits {
		b.taken.add(limit, consumed.ScopeValue, 1)
	}
	return consumedLimits, nil, true
}

// remaining returns how many more preemptions can be taken from this budget for limit and scopeValue.
func (b *PreemptionLimitBudget) remaining(limit policy.PreemptionLimitReference, scopeValue policy.PreemptionLimitScopeValue) int32 {
	return b.limiter.snapshot.allowance(limit, scopeValue) - b.taken[limit][scopeValue]
}
