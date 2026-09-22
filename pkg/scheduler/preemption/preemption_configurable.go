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
// It is reached from three places only:
//   - getTargets resolves the evaluator, once per attempt;
//   - classicalPreemptions resolves its configurable candidates upfront via
//     classicalConfigurableCandidates when building its candidate iterator;
//   - fairPreemptions extends the candidates upfront with fairSharingConfigurableCandidates
//     and preempts them between its two strategies.
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

// baselineConfigurableCandidates returns the candidates of the Always trigger. The API
// counts them as baseline candidates, next to the ones selected by the preemption policy
// of the ClusterQueue, which is why both algorithms consider them before the conditional
// triggers below rather than as a last resort.
func (p *Preemptor) baselineConfigurableCandidates(preemptionCtx *preemptionCtx) []*workload.Info {
	return p.configurableCandidates(preemptionCtx, kueue.Always)
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

// extendedConfigurableCandidates returns the baseline candidates extended with the ones
// of the conditional triggers, so that the classical algorithm orders and walks a single
// merged set rather than appending the tiers to its targets afterwards.
//
// A trigger is activated by asking whether the incoming workload would be admitted if
// every candidate collected so far were preempted, which is what the API defines the
// tiers against. The InsufficientQuota trigger is only used while that set does not yield
// enough quota, and the QuotaFeasibleAndInsufficientTopology trigger only once the quota,
// including the one the preceding tier would free, is sufficient but no topology
// assignment can be found.
//
// collected holds the candidates already gathered, classical and baseline alike; baseline
// holds only the ones this function may extend, as the classical candidates reach the
// iterator through the hierarchical collection instead.
//
// The probes leave the snapshot as they found it, so unlike a phase running after the
// walk, the evaluator still sees the candidates gathered so far and returns them again:
// the tiers are therefore deduplicated explicitly.
func (p *Preemptor) extendedConfigurableCandidates(preemptionCtx *preemptionCtx, collected, baseline []*workload.Info, check fitChecker) []*workload.Info {
	if !hasConditionalConfigurableRules(preemptionCtx) {
		// Nothing left to contribute. Checked before the probes, as they remove every
		// candidate from the snapshot, and the topology one runs the TAS solver.
		return baseline
	}
	known := workloadKeys(collected)
	extend := func(trigger kueue.PreemptionConfigActivationTrigger) {
		for _, candidate := range p.configurableCandidates(preemptionCtx, trigger) {
			key := workload.Key(candidate.Obj)
			if known.Has(key) {
				continue
			}
			known.Insert(key)
			baseline = append(baseline, candidate)
			collected = append(collected, candidate)
		}
	}

	if !probeFullPreemption(preemptionCtx, collected, check.quota) {
		extend(kueue.InsufficientQuota)
		if !probeFullPreemption(preemptionCtx, collected, check.quota) {
			// The topology trigger requires a feasible quota, so it must not be
			// applied to a workload the quota alone keeps out.
			return baseline
		}
	}
	if !probeFullPreemption(preemptionCtx, collected, check.topology) {
		extend(kueue.QuotaFeasibleAndInsufficientTopology)
	}
	return baseline
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

// workloadKeys returns the keys of the given workloads.
func workloadKeys(candidates []*workload.Info) sets.Set[workload.Reference] {
	keys := sets.New[workload.Reference]()
	for _, candidate := range candidates {
		keys.Insert(workload.Key(candidate.Obj))
	}
	return keys
}

// extendConfigurableCandidates returns the baseline and conditional candidates of
// the PreemptionConfig, deduplicated against candidates and extended upfront with the
// conditional tiers if the workload doesn't fit with the given check.
func (p *Preemptor) extendConfigurableCandidates(preemptionCtx *preemptionCtx, candidates []*workload.Info, check fitChecker) []*workload.Info {
	if !hasConfigurableRules(preemptionCtx) {
		return nil
	}
	baseline := p.baselineConfigurableCandidates(preemptionCtx)
	collected := slices.Clone(candidates)
	known := workloadKeys(collected)
	for _, wl := range baseline {
		if !known.Has(workload.Key(wl.Obj)) {
			collected = append(collected, wl)
			known.Insert(workload.Key(wl.Obj))
		}
	}
	return p.extendedConfigurableCandidates(preemptionCtx, collected, baseline, check)
}

// classicalConfigurableCandidates returns the baseline and conditional candidates of
// the PreemptionConfig, deduplicated against the candidates of the classical
// algorithm and extended upfront with the conditional tiers if the workload doesn't fit.
func (p *Preemptor) classicalConfigurableCandidates(preemptionCtx *preemptionCtx, hierarchicalReclaimCtx *classical.HierarchicalPreemptionCtx) []*workload.Info {
	var classicalCandidates []*workload.Info
	if hasConditionalConfigurableRules(preemptionCtx) {
		classicalCandidates = classical.FindCandidates(hierarchicalReclaimCtx)
	}
	return p.extendConfigurableCandidates(preemptionCtx, classicalCandidates, classicalFitChecker(preemptionCtx, true))
}

// fairSharingConfigurableCandidates returns the baseline and conditional candidates of
// the PreemptionConfig, deduplicated against the candidates of the Fair Sharing
// algorithm and extended upfront with the conditional tiers if the workload doesn't fit.
func (p *Preemptor) fairSharingConfigurableCandidates(preemptionCtx *preemptionCtx, candidates []*workload.Info, check fitChecker) []*workload.Info {
	return p.extendConfigurableCandidates(preemptionCtx, candidates, check)
}

// preemptConfigurableCandidates removes the given candidates from the snapshot and
// appends them to the targets, in order, stopping as soon as fits returns true.
// The candidates are preempted regardless of what the Fair Sharing rules allow, as the
// PreemptionConfig selects them explicitly, and are thus reported with the
// ConfigurablePreemption reason.
// Only the Fair Sharing algorithm uses it; the classical one merges the candidates into
// the set it orders and walks instead, see extendedConfigurableCandidates.
func preemptConfigurableCandidates(preemptionCtx *preemptionCtx, targets []*Target, candidates []*workload.Info, fits func() bool) (bool, []*Target) {
	preempted := preemptedKeys(targets)
	for _, candidate := range candidates {
		// Callers evaluate the candidates of a trigger against the snapshot the
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
		if fits() {
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

// remainingCandidates returns the candidates that are not targets yet, as a workload
// can't be preempted twice. The filtering is only needed because the baseline
// candidates are preempted between the two Fair Sharing strategies; once that phase is
// gone, retryCandidates can be passed straight to runSecondFsStrategy.
func remainingCandidates(candidates []*workload.Info, targets []*Target) []*workload.Info {
	preempted := preemptedKeys(targets)
	return slices.DeleteFunc(slices.Clone(candidates), func(candidate *workload.Info) bool {
		return preempted.Has(workload.Key(candidate.Obj))
	})
}
