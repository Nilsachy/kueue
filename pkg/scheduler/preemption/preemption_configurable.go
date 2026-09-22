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

package preemption

import (
	"slices"

	"k8s.io/apimachinery/pkg/util/sets"
	"sigs.k8s.io/controller-runtime/pkg/client"

	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	"sigs.k8s.io/kueue/pkg/features"
	preemptioncommon "sigs.k8s.io/kueue/pkg/scheduler/preemption/common"
	configurable "sigs.k8s.io/kueue/pkg/scheduler/preemption/config"
	"sigs.k8s.io/kueue/pkg/workload"
)

// This file holds the whole integration of ConfigurablePreemption with the classical and
// the Fair Sharing algorithms: resolving the PreemptionConfig, checking which triggers
// are activated, and merging the candidates their rules select into the targets of the
// running algorithm, without ever selecting the same workload twice.
//
// Both algorithms apply the same triggers as a fallback when their own candidates are not
// enough to admit the incoming workload, in the order the API defines them: the Always
// trigger first, as a baseline, then InsufficientQuota while the quota is not sufficient,
// and finally QuotaFeasibleAndInsufficientTopology once the quota is sufficient but no
// topology assignment can be found.
//
// It is reached from three places only:
//   - getTargets resolves the evaluator, once per attempt;
//   - classicalPreemptions calls mergeWithCheckFitConfigurableCandidates once its
//     candidate walk failed to admit the workload;
//   - fairPreemptions calls mergeWithCheckFitConfigurableCandidates once both its
//     strategies failed.
//
// TODO(#15893): delete this file, along with those call sites, once
// ConfigurablePreemption becomes an algorithm of its own, mutually exclusive with the
// classical and the Fair Sharing preemption.

// newConfigurableEvaluator returns the evaluator for the PreemptionConfig referenced by
// the preemptor's ClusterQueue, or nil if the ConfigurablePreemption feature is
// disabled, the ClusterQueue references no PreemptionConfig, or it cannot be read.
func (p *Preemptor) newConfigurableEvaluator(preemptionCtx *preemptionCtx) *configurable.PreemptionEvaluator {
	if !features.Enabled(features.ConfigurablePreemption) || preemptionCtx.preemptorCQ.PreemptionAnnotation == nil {
		return nil
	}
	preemptionConfig := &kueue.PreemptionConfig{}
	preemptionConfigName := *preemptionCtx.preemptorCQ.PreemptionAnnotation
	if err := p.client.Get(preemptionCtx.ctx, client.ObjectKey{Name: preemptionConfigName}, preemptionConfig); err != nil {
		preemptionCtx.log.Error(err, "Failed to get PreemptionConfig", "preemptionConfigName", preemptionConfigName)
		return nil
	}
	return configurable.NewPreemptionEvaluator(preemptionCtx.ctx, preemptionCtx.log, preemptionCtx.clock, *preemptionConfig, p.client)
}

// configurableCandidates returns the candidates selected by the rules of the
// PreemptionConfig activated by the given trigger, ordered from the most to the least
// preferred one, or no candidate if the ClusterQueue uses no PreemptionConfig.
// Only the candidates still admitted in the snapshot are returned, so a trigger
// evaluated after some workloads have been preempted never returns those again.
func (p *Preemptor) configurableCandidates(preemptionCtx *preemptionCtx, trigger kueue.PreemptionConfigActivationTrigger) []*workload.Info {
	if preemptionCtx.configurableEvaluator == nil {
		return nil
	}
	candidates, err := preemptionCtx.configurableEvaluator.Candidates(preemptionCtx.snapshot, &preemptionCtx.preemptor, preemptionCtx.frsNeedPreemption, trigger)
	if err != nil {
		preemptionCtx.log.Error(err, "Failed to get candidates for preemption", "trigger", trigger)
		return nil
	}
	slices.SortFunc(candidates, p.candidatesOrdering(preemptionCtx))
	return candidates
}

// hasConfigurableRules returns whether the PreemptionConfig holds any rule at all. It
// inspects the configuration only, which lets the callers keep going without evaluating
// the candidates of a trigger before the phase actually reached it.
func hasConfigurableRules(preemptionCtx *preemptionCtx) bool {
	return preemptionCtx.configurableEvaluator != nil &&
		preemptionCtx.configurableEvaluator.HasRulesFor(kueue.Always, kueue.InsufficientQuota, kueue.QuotaFeasibleAndInsufficientTopology)
}

// hasConditionalConfigurableRules returns whether the PreemptionConfig holds any rule of
// a trigger which is only reached once the preceding ones are not enough. It inspects
// the configuration only, and therefore lets the callers skip the fit checks guarding
// the evaluation of those triggers.
func hasConditionalConfigurableRules(preemptionCtx *preemptionCtx) bool {
	return preemptionCtx.configurableEvaluator != nil &&
		preemptionCtx.configurableEvaluator.HasRulesFor(kueue.InsufficientQuota, kueue.QuotaFeasibleAndInsufficientTopology)
}

// mergeWithCheckFitConfigurableCandidates preempts the candidates of the PreemptionConfig
// on behalf of the running preemption algorithm, merging them with the targets already
// selected, re-sorting the combined targets when the workload fits, and returning
// (fits, targets).
//
// The candidates of a trigger are preempted before the next one is evaluated, so the fit
// checks observe the state the preceding triggers left behind, and the evaluator no
// longer returns the candidates they consumed.
func (p *Preemptor) mergeWithCheckFitConfigurableCandidates(preemptionCtx *preemptionCtx, targets []*Target, allowBorrowing bool) (bool, []*Target) {
	if !hasConfigurableRules(preemptionCtx) {
		return false, targets
	}
	fits, targets := p.preemptConfigurableCandidates(preemptionCtx, targets, kueue.Always, allowBorrowing)
	if !fits && hasConditionalConfigurableRules(preemptionCtx) {
		if !workloadQuotaFits(preemptionCtx, allowBorrowing) {
			fits, targets = p.preemptConfigurableCandidates(preemptionCtx, targets, kueue.InsufficientQuota, allowBorrowing)
		}
		if !fits && workloadQuotaFits(preemptionCtx, allowBorrowing) {
			// The topology trigger requires a feasible quota, so it is only applied once
			// the quota fits while the workload still doesn't fit (meaning topology is
			// what keeps the workload out).
			fits, targets = p.preemptConfigurableCandidates(preemptionCtx, targets, kueue.QuotaFeasibleAndInsufficientTopology, allowBorrowing)
		}
	}
	if fits {
		ordering := p.candidatesOrdering(preemptionCtx)
		slices.SortFunc(targets, func(a, b *Target) int {
			return ordering(a.WorkloadInfo, b.WorkloadInfo)
		})
	}
	return fits, targets
}

// preemptConfigurableCandidates removes the candidates selected by the rules of the given
// trigger from the snapshot and appends them to the targets, from the most to the least
// preferred one, stopping as soon as workloadFits returns true.
// The candidates are preempted regardless of what the classical or Fair Sharing rules
// allow, as the PreemptionConfig selects them explicitly, and are thus reported with the
// ConfigurablePreemption reason.
func (p *Preemptor) preemptConfigurableCandidates(preemptionCtx *preemptionCtx, targets []*Target, trigger kueue.PreemptionConfigActivationTrigger, allowBorrowing bool) (bool, []*Target) {
	preempted := preemptedKeys(targets)
	for _, candidate := range p.configurableCandidates(preemptionCtx, trigger) {
		// The candidates of the trigger are evaluated against the snapshot the
		// preceding phases already mutated, and RemoveWorkload drops the workload
		// from the very map the evaluator iterates, so a target cannot be selected
		// twice. Kept as a safety net: preempting a workload again would leave a
		// duplicate target behind, which restoreSnapshot would then add back once
		// per copy. Skipping preserves the order of the remaining candidates.
		if preempted.Has(workload.Key(candidate.Obj)) {
			continue
		}
		preemptionCtx.snapshot.RemoveWorkload(candidate)
		targets = append(targets, &Target{
			WorkloadInfo: candidate,
			Reason:       preemptioncommon.ConfigurablePreemptionReason,
			WorkloadCq:   preemptionCtx.snapshot.ClusterQueue(candidate.ClusterQueue),
		})
		if workloadFits(preemptionCtx, allowBorrowing) {
			return true, targets
		}
	}
	return false, targets
}

// preemptedKeys returns the keys of the workloads already selected as targets.
func preemptedKeys(targets []*Target) sets.Set[workload.Reference] {
	preempted := sets.New[workload.Reference]()
	for _, target := range targets {
		preempted.Insert(workload.Key(target.WorkloadInfo.Obj))
	}
	return preempted
}
