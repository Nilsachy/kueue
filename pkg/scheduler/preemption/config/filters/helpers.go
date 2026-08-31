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
	"github.com/go-logr/logr"

	kueuev1beta2 "sigs.k8s.io/kueue/apis/kueue/v1beta2"
)

// matchesRelation evaluates relation constraints between candidate and preemptor values.
// It returns true if rel is nil, and false if an unsupported relation constraint is encountered.
func matchesRelation(log logr.Logger, rel *kueuev1beta2.RelativeConstraint, candidateVal, preemptorVal int64) bool {
	if rel == nil {
		return true // Default behavior when missing
	}
	switch *rel {
	case kueuev1beta2.Lower:
		return candidateVal < preemptorVal
	case kueuev1beta2.LowerOrEqual:
		return candidateVal <= preemptorVal
	case kueuev1beta2.Greater:
		return candidateVal > preemptorVal
	case kueuev1beta2.GreaterOrEqual:
		return candidateVal >= preemptorVal
	default:
		log.V(3).Info("Unsupported or unhandled relation constraint evaluated", "relation", *rel)
		return false
	}
}
