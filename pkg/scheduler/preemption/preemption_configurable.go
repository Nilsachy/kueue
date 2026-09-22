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
	"sigs.k8s.io/kueue/pkg/scheduler/preemption/classical"
	preemptioncommon "sigs.k8s.io/kueue/pkg/scheduler/preemption/common"
	configurable "sigs.k8s.io/kueue/pkg/scheduler/preemption/config"
	"sigs.k8s.io/kueue/pkg/workload"
)

// This file holds the whole integration of ConfigurablePreemption with the classical and
// the Fair Sharing algorithms: resolving the PreemptionConfig, checking which triggers
// are activated, and merging the candidates their rules select into the targets of the
// running algorithm, without ever selecting the same workload twice.
//
// Both algorithms walk the same tiers, in the order the API defines them: the Always
// trigger first, as a baseline, then InsufficientQuota while the quota is not sufficient,
// and finally QuotaFeasibleAndInsufficientTopology once the quota is sufficient but no
// topology assignment can be found. They differ in when they reach them: the classical
// algorithm merges the tiers into the candidates it orders and walks, while Fair Sharing
// preempts them as a last resort, once its strategies failed.
//
// It is reached from three places only:
//   - getTargets resolves the evaluator, once per attempt;
//   - classicalPreemptions resolves its configurable candidates upfront via
//     classicalConfigurableCandidates when building its candidate iterator;
//   - fairPreemptions calls preemptFairSharingConfigurableCandidates once both its
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

// classicalConfigurableCandidates returns the candidates the PreemptionConfig contributes
// to the classical algorithm, deduplicated against the ones the algorithm collected on
// its own. They are merged into the set it orders and walks, so that they take part in
// the same ordering and the same backfilling, rather than being appended to its targets
// afterwards.
//
// A conditional trigger is activated by asking whether the incoming workload would be
// admitted if every candidate collected so far were preempted, which is what the API
// defines the tiers against.
//
// The probes leave the snapshot as they found it, so unlike a phase running after the
// walk, the evaluator still sees the candidates gathered so far and returns them again:
// the tiers are therefore deduplicated explicitly.
func (p *Preemptor) classicalConfigurableCandidates(preemptionCtx *preemptionCtx, hierarchicalReclaimCtx *classical.HierarchicalPreemptionCtx) []*workload.Info {
	if !hasConfigurableRules(preemptionCtx) {
		return nil
	}
	// collected holds the candidates the probes preempt, the classical ones included,
	// while contributed holds only the ones this function returns, as the classical
	// candidates reach the iterator through the hierarchical collection instead.
	var collected, contributed []*workload.Info
	conditional := hasConditionalConfigurableRules(preemptionCtx)
	if conditional {
		// Only the probes need the candidates of the classical algorithm.
		collected = classical.FindCandidates(hierarchicalReclaimCtx)
	}
	known := workloadKeys(collected)
	contribute := func(trigger kueue.PreemptionConfigActivationTrigger) {
		for _, candidate := range p.configurableCandidates(preemptionCtx, trigger) {
			key := workload.Key(candidate.Obj)
			if known.Has(key) {
				continue
			}
			known.Insert(key)
			collected = append(collected, candidate)
			contributed = append(contributed, candidate)
		}
	}

	contribute(kueue.Always)
	if !conditional {
		// Nothing left to contribute. Checked before the probes, as they remove every
		// candidate from the snapshot, and the topology one runs the TAS solver.
		return contributed
	}
	check := classicalFitChecker(preemptionCtx, true)
	if !probeFullPreemption(preemptionCtx, collected, check.quota) {
		contribute(kueue.InsufficientQuota)
		if !probeFullPreemption(preemptionCtx, collected, check.quota) {
			// The topology trigger requires a feasible quota, so it must not be
			// applied to a workload the quota alone keeps out.
			return contributed
		}
	}
	if !probeFullPreemption(preemptionCtx, collected, check.topology) {
		contribute(kueue.QuotaFeasibleAndInsufficientTopology)
	}
	return contributed
}

// probeFullPreemption reports whether check holds once every candidate is removed from
// the snapshot. The snapshot is restored before returning, so the caller observes no
// change; the candidates must therefore be free of duplicates.
// It is a single check over the whole set, never one per candidate: the walk which
// follows keeps its early exit, so its cost stays proportional to the number of targets
// rather than to the number of candidates.
func probeFullPreemption(preemptionCtx *preemptionCtx, candidates []*workload.Info, check func() bool) bool {
	for _, candidate := range candidates {
		preemptionCtx.snapshot.RemoveWorkload(candidate)
	}
	fits := check()
	for _, candidate := range candidates {
		preemptionCtx.snapshot.AddWorkload(candidate)
	}
	return fits
}

// preemptFairSharingConfigurableCandidates preempts the candidates of the PreemptionConfig
// on behalf of the Fair Sharing algorithm, appending them to the targets its strategies
// already selected, and returns (fits, targets).
//
// Unlike the classical algorithm, which probes the activation of the conditional triggers
// on a snapshot it leaves untouched, each tier is preempted before the next one is
// evaluated, so the fit checks observe the state the preceding tiers left behind, and the
// evaluator no longer returns the candidates they consumed.
func (p *Preemptor) preemptFairSharingConfigurableCandidates(preemptionCtx *preemptionCtx, targets []*Target) (bool, []*Target) {
	if !hasConfigurableRules(preemptionCtx) {
		return false, targets
	}
	check := fairSharingFitChecker(preemptionCtx)
	fits, targets := p.preemptConfigurableTier(preemptionCtx, targets, kueue.Always, check.fits)
	if fits || !hasConditionalConfigurableRules(preemptionCtx) {
		return fits, targets
	}
	if !check.quota() {
		fits, targets = p.preemptConfigurableTier(preemptionCtx, targets, kueue.InsufficientQuota, check.fits)
		if fits || !check.quota() {
			// The topology trigger requires a feasible quota, so it must not be
			// applied to a workload the quota alone keeps out.
			return fits, targets
		}
	}
	// The quota is sufficient while the workload still doesn't fit, and fits is the
	// conjunction of the two checks, so the topology is what keeps the workload out.
	return p.preemptConfigurableTier(preemptionCtx, targets, kueue.QuotaFeasibleAndInsufficientTopology, check.fits)
}

// preemptConfigurableTier removes the candidates selected by the rules of the given
// trigger from the snapshot and appends them to the targets, from the most to the least
// preferred one, stopping as soon as fits returns true.
// The candidates are preempted regardless of what the Fair Sharing rules allow, as the
// PreemptionConfig selects them explicitly, and are thus reported with the
// ConfigurablePreemption reason.
func (p *Preemptor) preemptConfigurableTier(preemptionCtx *preemptionCtx, targets []*Target, trigger kueue.PreemptionConfigActivationTrigger, fits func() bool) (bool, []*Target) {
	preempted := preemptedKeys(targets)
	for _, candidate := range p.configurableCandidates(preemptionCtx, trigger) {
		// The tier is evaluated against the snapshot the preceding phases already
		// mutated, and RemoveWorkload drops the workload from the very map the
		// evaluator iterates, so a target cannot be selected twice. Kept as a safety
		// net: preempting a workload again would leave a duplicate target behind,
		// which restoreSnapshot would then add back once per copy. Skipping preserves
		// the order of the remaining candidates.
		if preempted.Has(workload.Key(candidate.Obj)) {
			continue
		}
		preemptionCtx.snapshot.RemoveWorkload(candidate)
		targets = append(targets, &Target{
			WorkloadInfo: candidate,
			Reason:       preemptioncommon.ConfigurablePreemptionReason,
			WorkloadCq:   preemptionCtx.snapshot.ClusterQueue(candidate.ClusterQueue),
		})
		if fits() {
			return true, targets
		}
	}
	return false, targets
}

// workloadKeys returns the keys of the given workloads.
func workloadKeys(candidates []*workload.Info) sets.Set[workload.Reference] {
	keys := sets.New[workload.Reference]()
	for _, candidate := range candidates {
		keys.Insert(workload.Key(candidate.Obj))
	}
	return keys
}

// preemptedKeys returns the keys of the workloads already selected as targets.
func preemptedKeys(targets []*Target) sets.Set[workload.Reference] {
	preempted := sets.New[workload.Reference]()
	for _, target := range targets {
		preempted.Insert(workload.Key(target.WorkloadInfo.Obj))
	}
	return preempted
}
