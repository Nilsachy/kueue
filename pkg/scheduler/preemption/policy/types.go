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

package policy

import (
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	kueuealpha "sigs.k8s.io/kueue/apis/kueue/v1alpha1"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	"sigs.k8s.io/kueue/pkg/cache/scheduler"
	"sigs.k8s.io/kueue/pkg/util/logging"
	stringsutils "sigs.k8s.io/kueue/pkg/util/strings"
	"sigs.k8s.io/kueue/pkg/workload"
)

// Target represents a workload selected for preemption.
type Target struct {
	WorkloadInfo *workload.Info
	Reason       string
	WorkloadCq   *scheduler.ClusterQueueSnapshot

	// ConfigurablePreemptionReasonData stores data which resulted in eviction.
	// Specified only when eviction is due to configurable preemption.
	ConfigurablePreemptionReasonData *ConfigurablePreemptionReasonData
}

type ConfigurablePreemptionReasonData struct {
	ConfigName PreemptionConfigReference
	// RuleNameToSelectorIndexes maps rule names to the indexes of selectors
	// that the workload satisfies.
	RuleNameToSelectorIndexes map[PreemptionConfigRuleReference][]int
	// ConsumedLimits are the PreemptionLimits consumed by preempting this target.
	ConsumedLimits ConsumedLimits
}

// PreemptionConfigReference is a dedicated type to reference PreemptionConfig
type PreemptionConfigReference string

// PreemptionConfigRuleReference is a dedicated type to reference PreemptionConfigPreemptionRule
type PreemptionConfigRuleReference string

// PreemptionLimitReference is a dedicated type to reference PreemptionLimit
type PreemptionLimitReference string

// PreemptionLimitScopeValue identifies a scoped entity within a PreemptionLimit
// (e.g. "Global", a ClusterQueue name, or "<namespace>/<workload-name>").
type PreemptionLimitScopeValue string

// GlobalScopeValue is the scope value for a PreemptionLimit with GlobalPreemptionLimitScope.
const GlobalScopeValue = PreemptionLimitScopeValue(kueuealpha.GlobalPreemptionLimitScope)

// PreemptionLimitScopeCounts maps each scope value of a single PreemptionLimit
// to its number of preemptions.
type PreemptionLimitScopeCounts = map[PreemptionLimitScopeValue]int32

// ConsumedLimit is the scope value of a PreemptionLimit consumed by a
// preemption.
type ConsumedLimit struct {
	ScopeValue PreemptionLimitScopeValue
	// uid and generation identify the version of the PreemptionLimit
	// ScopeValue was resolved against (see IsStaleFor).
	uid        types.UID
	generation int64
}

// NewConsumedLimit returns the scopeValue of limit consumed by a preemption.
func NewConsumedLimit(limit *kueuealpha.PreemptionLimit, scopeValue PreemptionLimitScopeValue) ConsumedLimit {
	return ConsumedLimit{ScopeValue: scopeValue, uid: limit.UID, generation: limit.Generation}
}

// IsStaleFor reports whether c was resolved against another PreemptionLimit
// than the one with uid, or against a generation older than minGeneration,
// so it no longer counts for the events tracked for that PreemptionLimit.
func (c ConsumedLimit) IsStaleFor(uid types.UID, minGeneration int64) bool {
	return c.uid != uid || c.generation < minGeneration
}

// ConsumedLimits maps each PreemptionLimit to the limit consumed by a preemption.
type ConsumedLimits map[PreemptionLimitReference]ConsumedLimit

// PreemptionLimitBlock identifies a PreemptionLimit that prevented preempting a
// candidate and when that candidate's exhausted limits may clear.
// RetryAt is nil when waiting for recorded events to expire cannot unblock it
// (e.g., spec.limit is 0, or taken preemptions alone exhaust spec.limit).
type PreemptionLimitBlock struct {
	Limit   PreemptionLimitReference
	RetryAt *time.Time
}

// Message formats the human-readable reason why preemption was blocked by the limit.
func (b PreemptionLimitBlock) Message() string {
	if b.RetryAt == nil {
		return fmt.Sprintf("preemption blocked by PreemptionLimit %s", b.Limit)
	}
	return fmt.Sprintf("preemption blocked by PreemptionLimit %s until %s", b.Limit, b.RetryAt.UTC().Format(time.RFC3339))
}

// EarlierPreemptionLimitBlock returns the block that clears sooner between two
// alternative candidates or attempts, or nil if both are nil.
// A finite RetryAt clears sooner than a nil (non-expiring) RetryAt; ties are
// broken deterministically by Limit name.
func EarlierPreemptionLimitBlock(a, b *PreemptionLimitBlock) *PreemptionLimitBlock {
	switch {
	case a == nil:
		return b
	case b == nil:
		return a
	case a.RetryAt == nil && b.RetryAt == nil:
		if a.Limit <= b.Limit {
			return a
		}
		return b
	case a.RetryAt == nil:
		return b
	case b.RetryAt == nil:
		return a
	case a.RetryAt.Before(*b.RetryAt):
		return a
	case b.RetryAt.Before(*a.RetryAt):
		return b
	case a.Limit <= b.Limit:
		return a
	default:
		return b
	}
}

func (d *ConfigurablePreemptionReasonData) EvictionMessage(preemptor *kueue.Workload) string {
	return fmt.Sprintf("Preempted by %s because of preemption config %s rule %s",
		workload.Key(preemptor),
		d.ConfigName,
		stringsutils.JoinMap(d.RuleNameToSelectorIndexes, "/", ",", "; "))
}

type yieldCandidate = func(*Target) bool

// YieldFromSnapshot wraps a candidate (Target) yielder with
// logic removing the candidate from the provided snapshot.
func YieldFromSnapshot(snapshot *scheduler.Snapshot, yield yieldCandidate) yieldCandidate {
	return func(t *Target) bool {
		snapshot.RemoveWorkload(t.WorkloadInfo)
		return yield(t)
	}
}

// ensures that Target implements ObjectRefProvider interface at compile time
var _ logging.ObjectRefProvider = (*Target)(nil)

// GetObject implements the ObjectRefProvider interface.
func (t *Target) GetObject() client.Object {
	return t.WorkloadInfo.Obj
}

// PreemptionPossibility represents the result
// of a preemption simulation.
type PreemptionPossibility int

const (
	// NoCandidates were found.
	NoCandidates PreemptionPossibility = iota
	// Preemption targets were found.
	Preempt
	// Preemption targets were found, and
	// all of them are outside of preempting
	// ClusterQueue.
	Reclaim
)

func (p PreemptionPossibility) String() string {
	switch p {
	case NoCandidates:
		return "NoCandidates"
	case Preempt:
		return "Preempt"
	case Reclaim:
		return "Reclaim"
	}
	return "Unknown"
}
