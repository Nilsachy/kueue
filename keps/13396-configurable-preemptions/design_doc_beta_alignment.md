# Configurable Preemptions (KEP-13396): Beta Alignment — Feature Parity, Global Configuration, and Alpha Breaking Changes

| | |
|---|---|
| **Status** | DRAFT — for discussion |
| **Authors** | @nilsachy |
| **Reviewers** | @dkaluza, @mwielgus, @michalwozniak |
| **Approvers** | TBD (KEP approvers: @mimowo, @tenzen-y) |
| **Last updated** | 2026-10-12 |
| **KEP** | [KEP-13396: Configurable Preemptions](https://github.com/kubernetes-sigs/kueue/tree/main/keps/13396-configurable-preemptions) |
| **Tracking issues** | kubernetes-sigs/kueue#13396 (KEP), kubernetes-sigs/kueue#15893 (mutual exclusivity / parity TODOs in code) |
| **Feature gate** | `ConfigurablePreemptions` (Alpha, v0.20) |

---

## 0. TL;DR

The KEP's Beta graduation criteria say three things. First, `PreemptionConfig` must reach **full feature parity** with classical and Fair Sharing preemption. Second, both strategies become **mutually exclusive** per ClusterQueue. Third, the Alpha annotation `kueue.x-k8s.io/preemption-config-name` gets **replaced by a formal field**. Before we update the KEP (candidate ordering, dynamically adjusted candidate generation), this doc asks maintainers to agree on three questions:

1. **Feature parity:** what `PreemptionConfig` must look like so that every configuration supported today can be written with it. That covers `ClusterQueue.spec.preemption` and Fair Sharing configured through the Kueue Configuration (ConfigMap) API.
2. **Global configuration:** how a per-ClusterQueue `PreemptionConfig` interacts with global Kueue configuration, mainly `fairSharing`. For example, what happens when Fair Sharing is enabled in the ConfigMap but the `PreemptionConfig` has no Fair Sharing rules?
3. **Breaking changes from Alpha:** whether current use cases and configs can be expressed with `PreemptionConfig` alone in the new API, and what Alpha users must change.

Each section has a summary of today's behavior, a gap analysis, options with pros and cons, a tentative proposal, and open questions. **Section 5 collects the decisions we need from maintainers.**

---

## 1. Background

### 1.1 Preemption settings available today

| Surface | Field / setting | Scope | Effect on preemption |
|---|---|---|---|
| `ClusterQueue.spec.preemption` | `withinClusterQueue`: `Never` / `LowerPriority` / `LowerOrNewerEqualPriority` | per CQ (preemptor side) | Lets the preemptor take candidates from its own CQ. `LowerOrNewerEqualPriority` compares the *queue order timestamp*. |
| `ClusterQueue.spec.preemption` | `reclaimWithinCohort`: `Never` / `LowerPriority` / `Any` | per CQ | Classical: reclaim from CQs in the cohort tree that are above nominal in the contested flavor-resources. With hierarchical cohorts, uses "hierarchical advantage". Fair Sharing: candidates must also pass the FS strategies. |
| `ClusterQueue.spec.preemption` | `borrowWithinCohort.policy`: `Never` / `LowerPriority`, plus `maxPriorityThreshold` | per CQ | Classical only (ignored with FS). Lets the preemptor preempt lower-priority workloads in other CQs *while borrowing*. Optional absolute priority ceiling. |
| `ClusterQueue.spec.fairSharing.weight`, `Cohort.spec.fairSharing.weight` | weight | per CQ / Cohort | Input to the Dominant Resource Share (DRS). Weight 0 means an infinite share when borrowing. |
| `ClusterQueue.spec.admissionScope` | `UsageBasedAdmissionFairSharing` | per CQ | Turns on LocalQueue usage, which is used in candidate ordering (AFS). |
| `ClusterQueue.spec.flavorFungibility` | `whenCanPreempt`, `preference` | per CQ | Flavor assignment decides whether "Preempt" mode is possible. The flavor assigner reads `spec.preemption` (`canPreemptWhileBorrowing`). |
| Kueue Configuration | `fairSharing` (presence enables it) + `fairSharing.preemptionStrategies` | **global** | Switches the whole Preemptor to the FS algorithm. Also switches admission ordering to DRS, DRS accounting in cache and status, and DRS metrics. |
| Kueue Configuration | `admissionFairSharing` | **global** | Turns on LQ usage tracking, which feeds candidate ordering. |
| Kueue Configuration | `waitForPodsReady.requeuingStrategy.timestamp` | **global** | Changes the queue order timestamp used by `LowerOrNewerEqualPriority`. |
| Feature gates | `FairSharingPreemptWithinNominal`, `FairSharingReevaluatePreemptionCandidates`, `PriorityBoost`, `ConfigurablePreemptions` | **global** | Change FS candidate eligibility, the re-evaluation passes, and effective (boosted) priority. |
| Annotation (Alpha) | `kueue.x-k8s.io/preemption-config-name` | per CQ | References a `PreemptionConfig`. |

### 1.2 How preemption works today (relevant to parity)

**Classical algorithm.** See `pkg/scheduler/preemption/classical_strategy.go` and `classical/`.
- Candidates fall into three groups:
  1. *Hierarchical reclaim* candidates: the preemptor has hierarchical advantage. These are valid even while the preemptor borrows.
  2. *Priority* candidates: cross-CQ, priority-gated. These are either `ReclaimWithoutBorrowing` or `ReclaimWhileBorrowing`, depending on `borrowWithinCohort` and `maxPriorityThreshold`.
  3. *Same-CQ* candidates.
- Only CQs that are above nominal in the contested flavor-resources are considered (`IsWithinNominalInResources` on the path to the LCA). Validity is re-checked dynamically as candidates are removed (`candidateIsValid`).
- The algorithm runs up to two **independent attempts**: `allowBorrowing = true` and `allowBorrowing = false`, in an order that depends on the configuration.
- Order inside each group: evicted first → other CQ before same CQ → (AFS) higher LocalQueue usage first → lower effective priority → most recently admitted first → UID. See `common/ordering.go`.

**Fair Sharing algorithm.** See `fair_strategy.go` and `fairsharing/`.
- Candidates come from `findCandidates`: the same CQ under the `withinClusterQueue` policy, plus all *borrowing* CQs in the cohort tree under the `reclaimWithinCohort` policy.
- Candidates are visited through a DRS-based ClusterQueue tournament (`MakeClusterQueueOrdering`, which compares AlmostLCA shares). Ordering is dynamic: it is recomputed after each pick.
- Strategy S2-a, `LessThanOrEqualToFinalShare`, runs first. Optionally it is re-run when the preemptor CQ itself lost workloads (`FairSharingReevaluatePreemptionCandidates`). Then S2-b, `LessThanInitialShare`, runs over the retry candidates.
- With `FairSharingPreemptWithinNominal`: if the preemptor CQ stays within nominal, cross-CQ candidates are taken without checking the FS strategy.
- `allowBorrowing` is always `true`.

**Alpha `PreemptionConfig`.** API in `apis/kueue/v1alpha1/preemptionconfig_types.go`, evaluator in `pkg/scheduler/preemption/config/evaluator.go`.
- Implemented selectors: `scope`, `priority{mode, comparison}`, `numericLabels`, `labelSelector`, `clusterQueueSelector`, and the rule-level `preemptorSelector`. Triggers: `Always`, `InsufficientQuota`, `QuotaFeasibleAndInsufficientTopology`.
- Not implemented: `ordering`, `quota`/DRS selectors, time-based selectors, priority-class selectors, `PreemptionLimit`.
- Integration as implemented: configurable candidates are appended **after** the classical/FS candidates are used up, inside the same attempt. They are not merged with those candidates and globally re-sorted as the KEP text says. This happens in each classical attempt and in the single FS attempt. When a `PreemptionConfig` is referenced, the flavor assigner also widens `canPreemptWhileBorrowing` (TODO #15893).

> **To confirm:** the KEP text ("merged, deduplicated, and ordered") and the implementation ("classical/FS first, then configurable") disagree. The Beta design must pick one reference behavior, because it changes what "parity" means for Alpha users (see Section 4).

---

## 2. Issue 1 — Feature parity: what does `PreemptionConfig` need to look like?

### 2.1 Goal

Every configuration expressible today with `spec.preemption` plus global `fairSharing` must also be expressible with `PreemptionConfig` alone. "Expressible" means it selects the **same victim set** (modulo documented, intentional differences) and produces **equivalent observability** (reasons, metrics).

### 2.2 Today's behavior broken into primitives

To express today's behavior as rules, we break it into primitives. **Bold** rows have no Alpha equivalent.

| # | Primitive | Used by | Alpha API equivalent |
|---|---|---|---|
| P1 | Candidate in same CQ | `withinClusterQueue` | `scope: WithinClusterQueue` ✅ |
| P2 | Candidate anywhere in the cohort tree | `reclaimWithinCohort`, FS | `scope: WithinCohortTree` ✅ |
| P3 | Candidate priority < preemptor (effective) | `LowerPriority` | `priority: {mode: Boosted, comparison: LessThan}` ✅ |
| **P4** | **Candidate priority == preemptor AND candidate newer (queue order timestamp)** | `LowerOrNewerEqualPriority` | ❌ no timestamp relation (time selectors are Future Work and use creation time, not queue order timestamp) |
| **P5** | **Candidate's CQ (and the path to the LCA) is above nominal in contested resources, re-checked after each pick** | `reclaimWithinCohort`, FS candidate set | ❌ (Future Work `BorrowingCapacityFromPreemptor` is narrower: "borrowing *from preemptor*") |
| **P6** | **Preemptor must not borrow after preemption (`allowBorrowing=false` attempt)** | `ReclaimWithoutBorrowing` | ❌ |
| **P7** | **Preemptor has hierarchical advantage (fits in a subtree whose quota the candidate uses)** | hierarchical classical reclaim | ❌ |
| **P8** | **Absolute priority ceiling for candidates (`maxPriorityThreshold`)** | `borrowWithinCohort` | ❌ (absolute bounds were rejected in Future Work "Priority Selectors → Alternatives") |
| **P9** | **FS strategy S2-a: `LessThanOrEqualToFinalShare`** | FS | ❌ (Future Work `quota: DRSLessThanOrEqualToFinalShare`) |
| **P10** | **FS strategy S2-b: `LessThanInitialShare`, applied only if S2-a was not enough** | FS | ❌ (Future Work; the fallback could map to the `InsufficientQuota` tier) |
| **P11** | **FS "preempt within nominal" bypass** | `FairSharingPreemptWithinNominal` | ❌ |
| **P12** | **Independent attempts (borrowing vs. not borrowing) vs. cumulative tiers** | classical | ❌ (triggers are *cumulative* tiers, not independent attempts) |
| P13 | Candidate uses a contested flavor-resource | all | implicit in the evaluator ✅ |
| **P14** | **Ordering: evicted → other CQ first → LQ usage (AFS) → priority → recency → UID** | all | ⚠️ reused implicitly; the `ordering` field is not implemented |
| **P15** | **Ordering: DRS ClusterQueue tournament (AlmostLCA shares), recomputed after each pick** | FS | ❌ (KEP proposes `ClusterQueueDRS`, but that is not the AlmostLCA tournament) |
| **P16** | **Preemption reason per victim: `InClusterQueue` / `InCohortReclamation` / `InCohortReclaimWhileBorrowing` / `InCohortFairSharing`** | conditions, events, `preempted_workloads_total{reason}` | ❌ everything becomes `ConfigurablePreemption` |
| **P17** | **Flavor assigner knows whether preemption while borrowing is possible** | `flavorFungibility` | ⚠️ coarse: "any PreemptionConfig ⇒ may preempt while borrowing" |

### 2.3 Design options for parity

**Option A — Primitive selectors only (fully decomposed).**
Add the missing primitives (P4–P12, P14–P17) as first-class selector, rule, and ordering fields.
- Pros: one coherent model, composable (e.g. FS + defrag + hero in one config), matches the KEP vision.
- Cons: large API surface before Beta. Exact parity with hierarchical classical reclaim (P7, P12) is hard to express and validate. Higher regression risk. The algorithm needs Dynamically Adjusted Candidate Generation for P5, P9, P10 and P15.

**Option B — Embed the legacy policies as a "baseline" block inside `PreemptionConfig`.**
For example, `spec.baseline: {withinClusterQueue, reclaimWithinCohort, borrowWithinCohort, fairSharing: {preemptionStrategies}}`, with the rules layered on top (the same "merge" semantics as Alpha, but inside one object).
- Pros: trivial migration (copy `spec.preemption` into the config), exact parity by reusing the existing code paths, small API.
- Cons: two candidate models inside one object. The legacy semantics are frozen into a new API. FS strategies move from global to per-config (see Issue 2). Custom ordering cannot easily reorder legacy candidates.

**Option C — Hybrid: primitives plus named "presets".**
Primitives (Option A) are the long-term model. For Beta, a small set of `preset` selectors or rules (`ClassicalReclaimWithinCohort`, `ClassicalBorrowWithinCohort`, `FairSharing`) maps one-to-one onto today's code paths. Presets could later be redefined as sugar over the primitives.
- Pros: parity for Beta without first designing every primitive. Migration is mechanical. Leaves room to grow.
- Cons: two ways to do the same thing. Presets need precise documentation and versioning.

**Tentative proposal (for discussion):** Option C. Use presets for the hard-to-decompose behaviors (P7, P12, P15). Add primitives where they are clearly reusable on their own: P4, P5, P6, P8, P9/P10, P14.

### 2.4 Draft API delta (illustrative, names are placeholders)

```go
type PreemptionConfigSpec struct {
  Rules    []PreemptionConfigPreemptionRule `json:"rules,omitempty"`
  // NEW (from KEP): explicit ordering; defaults to today's classical/FS ordering.
  Ordering []PreemptionConfigOrder          `json:"ordering,omitempty"`
}

type PreemptionConfigPreemptionRule struct {
  Name               string
  PreemptorSelector  *metav1.LabelSelector
  ActivationPolicy   PreemptionConfigActivationPolicy
  // NEW (P6): whether the preemptor may borrow after preempting candidates of this rule.
  // Never => candidates of this rule are only valid in a non-borrowing admission.
  PreemptorBorrowing *PreemptorBorrowingPolicy // Allowed | Forbidden (default TBD)
  // NEW (Option C): a preset reproducing an existing algorithm; exclusive with CandidateSelectors.
  Preset             *PreemptionPreset
  CandidateSelectors []PreemptionConfigPreemptionCandidateSelector
}

type PreemptionConfigPreemptionCandidateSelector struct {
  Scope                PreemptionConfigPreemptionQueueScope
  Priority             *PreemptionConfigPriorityConstraint  // + NEW MaxValue (P8)
  NumericLabels        []PreemptionConfigNumericLabelConstraint
  LabelSelector        *metav1.LabelSelector
  ClusterQueueSelector *metav1.LabelSelector
  // NEW (P4): compares the queue order timestamp of the candidate and the preemptor.
  QueueOrderTimestamp  *TimestampRelation                   // e.g. {comparison: After}
  // NEW (P5, P9, P10): dynamic quota-based constraint.
  Quota                *PreemptionConfigQuotaConstraint      // CandidateCQAboveNominal | BorrowingCapacityFromPreemptor |
                                                             // DRSLessThanOrEqualToFinalShare | DRSLessThanInitialShare
}

type PreemptionPreset struct {
  ClassicalReclaimWithinCohort *ClassicalReclaimPreset // {policy: LowerPriority|Any}  -> P2+P3+P5+P7
  ClassicalBorrowWithinCohort  *ClassicalBorrowPreset  // {maxPriorityThreshold}       -> P5+P6+P8
  FairSharing                  *FairSharingPreset      // {strategies: [...], preemptWithinNominal: bool} -> P9..P11,P15
}
```

Ordering keys to add or confirm: `IsEvicted` (implicit), `IsOtherCQ`, `LocalQueueDRS`, `Priority`, `AdmissionTimestamp`, plus **NEW** `FairSharingTournament` (the AlmostLCA DRS tournament) for P15.

### 2.5 Mapping today's configurations to `PreemptionConfig` (draft)

> Fields marked `# NEW` do not exist in Alpha. Each example assumes `ordering` is omitted, so it defaults to today's ordering.

**(a) `withinClusterQueue: LowerPriority`** — expressible in Alpha.
```yaml
rules:
- name: within-cq
  activationPolicy: {trigger: Always}
  candidateSelectors:
  - scope: WithinClusterQueue
    priority: {mode: Boosted, comparison: LessThan}
```

**(b) `withinClusterQueue: LowerOrNewerEqualPriority`**
```yaml
rules:
- name: within-cq
  activationPolicy: {trigger: Always}
  candidateSelectors:
  - scope: WithinClusterQueue
    priority: {mode: Boosted, comparison: LessThan}
  - scope: WithinClusterQueue
    priority: {mode: Boosted, comparison: LessThanOrEqual}
    queueOrderTimestamp: {comparison: After}        # NEW (P4)
```

**(c) `reclaimWithinCohort: Any` (flat cohort, `borrowWithinCohort: Never`)**
```yaml
rules:
- name: reclaim
  activationPolicy: {trigger: Always}
  preemptorBorrowing: Forbidden                     # NEW (P6)
  candidateSelectors:
  - scope: WithinCohortTree
    quota: CandidateCQAboveNominal                  # NEW (P5)
# or, with presets:
- name: reclaim
  activationPolicy: {trigger: Always}
  preset: {classicalReclaimWithinCohort: {policy: Any}}   # NEW (Option C)
```

**(d) `reclaimWithinCohort: LowerPriority` + `borrowWithinCohort: {policy: LowerPriority, maxPriorityThreshold: 100}`**
```yaml
rules:
- name: reclaim
  activationPolicy: {trigger: Always}
  preemptorBorrowing: Forbidden                     # NEW
  candidateSelectors:
  - scope: WithinCohortTree
    quota: CandidateCQAboveNominal                  # NEW
    priority: {mode: Boosted, comparison: LessThan}
- name: borrow-within-cohort
  activationPolicy: {trigger: Always}
  preemptorBorrowing: Allowed                       # NEW
  candidateSelectors:
  - scope: WithinCohortTree
    quota: CandidateCQAboveNominal                  # NEW
    priority: {mode: Boosted, comparison: LessThan, maxValue: 100}   # NEW (P8)
```
> Open: today the classical algorithm runs the borrowing and non-borrowing passes as **independent attempts** (P12). Rules with different `preemptorBorrowing` values imply the same split. Is per-rule `preemptorBorrowing` enough, or do we need explicit "attempt groups"?

**(e) Hierarchical cohorts with classical reclaim.** Only reproducible exactly through the preset (P7). Open question: do we commit to decomposing hierarchical advantage into primitives for Beta, or keep it preset-only?

**(f) Fair Sharing: `reclaimWithinCohort: Any`, `withinClusterQueue: LowerPriority`, strategies `[LessThanOrEqualToFinalShare, LessThanInitialShare]`**
```yaml
rules:
- name: within-cq
  activationPolicy: {trigger: Always}
  candidateSelectors:
  - {scope: WithinClusterQueue, priority: {mode: Boosted, comparison: LessThan}}
- name: fs-s2a
  activationPolicy: {trigger: Always}
  candidateSelectors:
  - {scope: WithinCohortTree, quota: DRSLessThanOrEqualToFinalShare}    # NEW (P9)
- name: fs-s2b
  activationPolicy: {trigger: InsufficientQuota}                         # fallback tier = "only if S2-a was not enough"
  candidateSelectors:
  - {scope: WithinCohortTree, quota: DRSLessThanInitialShare}           # NEW (P10)
ordering:
- {orderingField: FairSharingTournament}                                 # NEW (P15)
- {orderingField: Priority, direction: Ascending}
- {orderingField: AdmissionTimestamp, direction: Descending}
# or: preset: {fairSharing: {strategies: [LessThanOrEqualToFinalShare, LessThanInitialShare]}}
```
> Open: does the `InsufficientQuota` tier exactly match S2-b running "only on retry candidates of S2-a"? Today S2-b only sees workloads that failed S2-a. With cumulative tiers, S2-b sees all remaining workloads.

**(g) Classical/FS + defragmentation (the typical Alpha "layered" setup).** Concatenate the rules from (a)–(f) with the defrag rule (`QuotaFeasibleAndInsufficientTopology`).

### 2.6 Open questions (Issue 1)

1. Option A, B, or C? If C, which presets are in scope for Beta?
2. Do we need byte-for-byte victim parity, or "equivalent with documented differences"? Who signs off on the differences?
3. Do we accept an absolute `maxValue` priority bound (P8) for parity, even though Future Work prefers priority-class names?
4. Should reasons and metrics keep legacy values when a rule or preset matches a legacy behavior (P16)? Options: a per-rule `reason` field, a preset-derived reason, or always `ConfigurablePreemption`.
5. How should the flavor assigner (P17) learn from the rules whether "Preempt while borrowing" is possible, instead of being widened unconditionally?
6. Parity depends on the KEP's **Dynamically Adjusted Candidate Generation** for P5, P9, P10 and P15. Is that in scope for Beta?
7. How do we verify parity? Proposed: run the existing preemption unit and integration suites twice, once with the legacy config and once with the generated `PreemptionConfig`, and diff the victim sets. Add the perf suite comparison as well.

---

## 3. Issue 2 — Intersection with global Kueue configuration

### 3.1 What global `fairSharing` controls today

Enabling `fairSharing` in the Kueue Configuration is not only about preemption. It also switches:

| Concern | Code entry point | Per-CQ or global today |
|---|---|---|
| Admission ordering across CQs in a cohort (DRS tournament) | `scheduler.makeIterator(..., fairsharing.Enabled)` | global |
| DRS accounting in cache, `status.fairSharing` on CQ/Cohort, DRS metrics | `cache.WithFairSharing`, `CohortReconcilerWithFairSharing` | global |
| Flavor assignment (`canPreemptWhileBorrowing` when FS + reclaim) | `flavorassigner` | global switch, per-CQ input |
| Preemption algorithm (classical vs. FS) and `preemptionStrategies` | `preemption.New(..., fs, ...)` | global |

**Principle to agree on:** split "Fair Sharing as an **admission/accounting model**" (stays global) from "Fair Sharing as a **preemption policy**" (moves into `PreemptionConfig` for CQs that use it).

### 3.2 Options

**Option 1 — `PreemptionConfig` is authoritative (explicit).**
For a CQ that references a `PreemptionConfig`, only its rules decide preemption. Global `fairSharing.preemptionStrategies` is ignored for that CQ. Global FS still drives admission ordering and DRS accounting.
- Pros: explicit and predictable. Matches "mutually exclusive" in the KEP.
- Cons: a config without FS rules in an FS-enabled cluster silently turns off FS preemption for that CQ. That needs a warning.

**Option 2 — Implicit inheritance.**
If global FS is enabled and the `PreemptionConfig` has no FS rules, implicitly append FS rules derived from `preemptionStrategies`.
- Pros: no surprise for FS clusters.
- Cons: hidden behavior. You cannot opt out without disabling FS globally. Contradicts mutual exclusivity.

**Option 3 — Global default `PreemptionConfig`.**
Add a Configuration field (e.g. `preemption.defaultPreemptionConfigName`). CQs without an explicit reference use it. `fairSharing.preemptionStrategies` is deprecated in favor of FS rules or presets in that default config.
- Pros: one place to define cluster-wide policy. A clean path to retire the legacy global settings.
- Cons: a new Configuration field, a defaulting and precedence story, and the question of what happens when the referenced object is missing.

**Tentative proposal:** Option 1 for Beta, plus a warning condition (see 3.4). Consider Option 3 as a follow-up toward GA.

### 3.3 Behavior matrix (to be filled in and agreed)

| # | Global FS (Configuration) | CQ references PC | PC has FS/DRS rules | Proposed behavior (Option 1) | Alternatives / notes |
|---|---|---|---|---|---|
| 1 | off | no | – | Classical (unchanged) | – |
| 2 | on | no | – | FS preemption with global strategies (unchanged) | – |
| 3 | off | yes | no | PC rules only | – |
| 4 | off | yes | yes | ? | (a) CQ inactive with condition `PreemptionConfigInvalid`; (b) DRS rules match nothing, plus a warning; (c) compute DRS anyway (decouple DRS accounting from the FS switch) |
| 5 | **on** | yes | **no** | **PC rules only. No FS preemption *by* this CQ. FS still drives admission ordering and DRS. Warning condition on the CQ.** | Option 2 (implicit FS rules) or reject |
| 6 | on | yes | yes | PC FS rules with the PC's strategies. Global `preemptionStrategies` ignored for this CQ. CQ/Cohort weights still apply. | Should `preemptionStrategies` be the default for an FS preset that omits strategies? |

### 3.4 Validation and observability

- A CQ condition or event (e.g. `PreemptionConfigWarning`) for row 5, and for row 4 if we choose (b).
- Webhook validation: is it possible? The webhook runs in the manager and can read the Configuration, but the `PreemptionConfig` object can change after the CQ is validated. A controller-side condition is probably the right place.
- Document clearly that preemption is configured **on the preemptor side**. A CQ using a PC without FS rules can still be preempted by FS rules of other CQs in the same cohort. Victim-side protection is out of scope (Future Work `PreemptionLimit`).

### 3.5 Mixed cohorts

During migration, one cohort can contain CQs with legacy `spec.preemption` and CQs with `PreemptionConfig`.
- Is FS fairness still well defined when only some CQs preempt by DRS? (It should be: DRS accounting stays global.)
- Do we want guidance or validation that a cohort uses one model? (Probably guidance only.)

### 3.6 Other global settings

| Setting | Interaction | Proposal |
|---|---|---|
| `admissionFairSharing` | `LocalQueueDRS` ordering key and AFS ordering | If AFS is disabled, the key is a no-op (all equal). Document this. |
| `waitForPodsReady.requeuingStrategy.timestamp` | Defines the queue order timestamp used by P4 | P4 uses the same `workload.Ordering` as today. |
| `FairSharingPreemptWithinNominal` | Changes FS candidate eligibility | PC: explicit (`preemptWithinNominal` in the preset, or a rule). Should the FG be ignored for PC CQs? |
| `FairSharingReevaluatePreemptionCandidates` | Re-runs S2-a | PC: handled by dynamic candidate generation. Should the FG be ignored for PC CQs? |
| `PriorityBoost` | Effective priority | Already covered by `priority.mode: Boosted / Base`. |

### 3.7 Open questions (Issue 2)

1. Option 1, 2, or 3?
2. Row 4 (FS off + DRS rules): reject, no-op with a warning, or decouple DRS accounting from FS?
3. Should global FS feature gates apply to PC-driven CQs, or only to explicit PC fields?
4. Do we plan to deprecate `fairSharing.preemptionStrategies`, and if so, when?

---

## 4. Issue 3 — Breaking changes from Alpha

### 4.1 Changes from Alpha to Beta and their compatibility impact

Each row is one change between the Alpha behavior and the proposed Beta behavior. For each change, the table says whether it breaks existing Alpha users and how we plan to soften the impact. The IDs (B1–B10) are referenced elsewhere in this doc.

| # | Area | Alpha behavior | Beta proposal | Breaking? | Mitigation |
|---|---|---|---|---|---|
| B1 | Reference | Annotation `kueue.x-k8s.io/preemption-config-name` | Formal field on `ClusterQueueSpec` (e.g. `spec.preemptionConfigName` or `spec.preemption.configName`) | Yes, for Alpha users | Keep the annotation with **merged** semantics for N releases (deprecated). The field has **exclusive** semantics from day one, so no field ever changes meaning. Emit a deprecation warning. |
| B2 | Strategy interaction | Classical/FS candidates and PC candidates combined (implementation: classical/FS first, then PC) | Mutually exclusive per CQ | Yes, for layered setups | Rewrite `spec.preemption` into PC rules or presets (Section 2.5). Provide golden configs, and possibly a migration helper (`kueuectl`?). |
| B3 | `spec.preemption` defaulting | `+kubebuilder:default={}` fills `Never` values | Unchanged | No, if validation treats "all `Never`" as unset | Validation: when the PC field is set, reject `spec.preemption` values other than `Never`. Accept the defaulted all-`Never` struct. |
| B4 | Trigger semantics | "Baseline" = `spec.preemption` candidates + `Always` rules | Baseline = `Always` rules only | Semantic change | Document it. It only matters in combination with B2. |
| B5 | Candidate ordering | Implicit legacy ordering; PC candidates after classical/FS | `ordering` field. Default = legacy ordering, applied to all candidates | Possible change in victim selection | Make the default ordering reproduce the observed Alpha behavior, or document the difference. |
| B6 | Preemption reason | `ConfigurablePreemption` for PC victims; legacy reasons for classical/FS victims | All victims of PC CQs get `ConfigurablePreemption` (unless 2.6 Q4 decides otherwise) | Yes, for dashboards and alerts on `preempted_workloads_total{reason}` and condition reasons | Per-rule or preset reason, or a release note. |
| B7 | API version | `kueue.x-k8s.io/v1alpha1` `PreemptionConfig` | Promote to `v1beta2` (served alongside v1alpha1 for a deprecation window?) | Yes (new group version) | Conversion, or a manual re-create (Alpha, so allowed). Needs a decision. |
| B8 | Flavor assigner | Any PC ⇒ `canPreemptWhileBorrowing = true` | Derived from the rules (P17) | Behavior change | Release note. |
| B9 | Feature gate | `ConfigurablePreemptions` Alpha (default off) | Beta (default on) | No | Standard. |
| B10 | Field renames / validation | e.g. `priority.mode` required | e.g. `priority.mode` optional with a default (Future Work proposes optional) | Loosening only | – |

### 4.2 Can every current use case be expressed with `PreemptionConfig` alone?

| Use case | Alpha API alone | Proposed Beta API alone | Notes |
|---|---|---|---|
| `withinClusterQueue: LowerPriority` | ✅ | ✅ | Example (a) |
| `withinClusterQueue: LowerOrNewerEqualPriority` | ❌ | ✅ (P4) | Example (b) |
| `reclaimWithinCohort: LowerPriority / Any` (flat) | ❌ (no "candidate CQ above nominal", no "preemptor must not borrow") | ✅ (P5, P6) or preset | Example (c) |
| `borrowWithinCohort` + `maxPriorityThreshold` | ❌ | ✅ (P6, P8) or preset | Example (d) |
| Hierarchical cohort reclaim | ❌ | ⚠️ preset only (P7, P12) | Example (e) |
| Fair Sharing S2-a / S2-b | ❌ | ✅ (P9, P10, P15) or preset | Example (f); depends on dynamic candidate generation |
| `FairSharingPreemptWithinNominal` | ❌ | ✅ preset flag (P11) | – |
| AFS LQ-usage ordering | ✅ (implicit) | ✅ (`LocalQueueDRS`) | – |
| Defragmentation | ✅ | ✅ | KEP Story 1 |
| Hero workload | ✅ | ✅ | KEP Story 2 |
| Business rules (labels, numeric labels, priority) | ✅ partially | ✅ partially | Time-based and priority-class selectors remain Future Work |
| Legacy reasons and metrics | ❌ | ? | Depends on 2.6 Q4 |

**Answer to Issue 3 (draft):** with the Alpha API alone, **no**. Only `withinClusterQueue: LowerPriority`, defragmentation, and hero setups can be expressed. With the proposed Beta additions (Section 2.4), **yes for all flat-cohort classical and FS setups**. Hierarchical classical reclaim remains preset-only unless we decide to decompose it.

### 4.3 Migration paths

1. **Users with no PC:** nothing changes (legacy `spec.preemption` stays fully supported; not deprecated as part of this KEP).
2. **Alpha users with a PC and all-`Never` `spec.preemption`:** move the annotation to the field. Behavior is identical except for B5 and B6.
3. **Alpha users with a PC and a non-`Never` `spec.preemption`:** translate `spec.preemption` (plus global FS) into rules or presets using the golden configs, then switch to the field.
4. Optional tooling: generate the equivalent `PreemptionConfig` from an existing CQ and the Configuration, plus a dry-run "victim diff" mode.

### 4.4 Proposed timeline (strawman)

| Release | Change |
|---|---|
| v0.20 | Alpha: annotation, merged semantics (current) |
| v0.2X (Beta) | Field with exclusive semantics. Annotation deprecated but still honored (merged semantics) with a warning. `PreemptionConfig` served in v1beta2. Parity primitives or presets. |
| v0.2X+2 | Annotation removed. v1alpha1 `PreemptionConfig` no longer served. |

### 4.5 Open questions (Issue 3)

1. Is a deprecation window for the annotation required, given the feature is Alpha?
2. Which reference behavior defines "no regression" for Alpha users (B5): the KEP text or the current implementation?
3. Should we ship built-in or golden `PreemptionConfig` manifests in the repo (e.g. `config/samples/preemption/`)?
4. Is a migration helper in `kueuectl` worth the effort?

---

## 5. Decisions requested from maintainers

| # | Decision | Options | Proposed | Owner | Status |
|---|---|---|---|---|---|
| D1 | Parity model | A (primitives) / B (embedded legacy block) / C (primitives + presets) | C | | Open |
| D2 | Parity bar | exact victim parity / documented differences | documented differences + diff test suite | | Open |
| D3 | Global FS with a PC lacking FS rules | Option 1 / 2 / 3 | Option 1 + warning | | Open |
| D4 | FS disabled but PC has DRS rules | reject / no-op + warning / decouple DRS | no-op + warning | | Open |
| D5 | Beta reference field name and location | `spec.preemptionConfigName` / `spec.preemption.configName` | TBD | | Open |
| D6 | Annotation deprecation window | none / N releases | 2 releases | | Open |
| D7 | Preemption reasons for PC victims | always `ConfigurablePreemption` / per rule / per preset | per preset (legacy reasons) | | Open |
| D8 | Dynamic candidate generation in Beta scope | yes / no | yes (required for P5, P9, P10, P15) | | Open |

---

## 6. Out of scope for this doc

- Detailed design of candidate ordering and dynamically adjusted candidate generation (covered in the KEP update that follows).
- `PreemptionLimit`, time-based selectors, priority-class selectors, DRA triggers (KEP Future Work).
- MultiKueue orchestrated preemption (KEP-8303).

---

## Appendix A — Code references

- ClusterQueue preemption API: `apis/kueue/v1beta2/clusterqueue_types.go` (`ClusterQueuePreemption`, `BorrowWithinCohort`)
- Fair Sharing configuration: `apis/config/v1beta2/configuration_types.go` (`FairSharing`, `PreemptionStrategy`, `AdmissionFairSharing`)
- Alpha API: `apis/kueue/v1alpha1/preemptionconfig_types.go`
- Preemptor entry point and strategy selection: `pkg/scheduler/preemption/preemption.go`
- Classical algorithm: `pkg/scheduler/preemption/classical_strategy.go`, `pkg/scheduler/preemption/classical/{candidate_generator.go,hierarchical_preemption.go}`
- Fair Sharing algorithm: `pkg/scheduler/preemption/fair_strategy.go`, `pkg/scheduler/preemption/fairsharing/`
- Shared ordering and policy: `pkg/scheduler/preemption/common/{ordering.go,preemption_policy.go}`
- Configurable evaluator: `pkg/scheduler/preemption/config/evaluator.go`, `pkg/scheduler/preemption/config/filters/`
- Flavor assigner interaction: `pkg/scheduler/flavorassigner/flavorassigner.go` (`canPreemptWhileBorrowing`, `usesConfigurablePreemption`)

## Appendix B — Glossary

- **Classical preemption:** the preemption algorithm used when Fair Sharing is disabled globally.
- **DRS:** Dominant Resource Share, the Fair Sharing metric for a CQ or Cohort.
- **AlmostLCA(x, y):** the last-but-one node on the path from x to the lowest common ancestor of x and y.
- **Hierarchical advantage:** the preemptor fits in a subtree whose quota the candidate is using, so it may reclaim regardless of borrowing.
- **Queue order timestamp:** the timestamp used for queue ordering (creation or eviction time, depending on configuration).
