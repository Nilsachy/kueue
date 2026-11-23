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
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	kueuealpha "sigs.k8s.io/kueue/apis/kueue/v1alpha1"
)

func TestBlockingPreemptionLimitMessage(t *testing.T) {
	utcPlus2 := time.FixedZone("UTC+2", 2*60*60)
	cases := map[string]struct {
		block BlockingPreemptionLimit
		want  string
	}{
		"non-expiring limit omits until timestamp": {
			block: BlockingPreemptionLimit{
				Limit:       "blocked",
				Aggregation: kueuealpha.GlobalLimitAggregation,
			},
			want: "preemptions blocked by PreemptionLimit blocked (aggregation Global)",
		},
		"expiring limit formats timestamp as UTC RFC3339": {
			block: BlockingPreemptionLimit{
				Limit:       "per-cq",
				Aggregation: kueuealpha.ByPreempteeCQLimitAggregation,
				RetryAt:     new(time.Date(2026, time.January, 1, 0, 1, 0, 0, time.UTC)),
			},
			want: "preemptions blocked by PreemptionLimit per-cq (aggregation ByPreempteeClusterQueue) until 2026-01-01T00:01:00Z",
		},
		"non-UTC timestamp is normalized to UTC RFC3339": {
			block: BlockingPreemptionLimit{
				Limit:       "per-cq",
				Aggregation: kueuealpha.ByPreemptorCQLimitAggregation,
				RetryAt:     new(time.Date(2026, time.January, 1, 2, 1, 0, 0, utcPlus2)),
			},
			want: "preemptions blocked by PreemptionLimit per-cq (aggregation ByPreemptorClusterQueue) until 2026-01-01T00:01:00Z",
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if diff := cmp.Diff(tc.want, tc.block.Message()); diff != "" {
				t.Errorf("Message() (-want,+got):\n%s", diff)
			}
		})
	}
}

func TestBlockingPreemptionLimitEarlier(t *testing.T) {
	t1 := time.Date(2026, time.January, 1, 0, 0, 10, 0, time.UTC)
	t2 := time.Date(2026, time.January, 1, 0, 0, 20, 0, time.UTC)

	cases := map[string]struct {
		a    *BlockingPreemptionLimit
		b    *BlockingPreemptionLimit
		want *BlockingPreemptionLimit
	}{
		"both nil": {},
		"first nil returns second": {
			b:    &BlockingPreemptionLimit{Limit: "b", RetryAt: &t1},
			want: &BlockingPreemptionLimit{Limit: "b", RetryAt: &t1},
		},
		"second nil returns first": {
			a:    &BlockingPreemptionLimit{Limit: "a", RetryAt: &t1},
			want: &BlockingPreemptionLimit{Limit: "a", RetryAt: &t1},
		},
		"finite RetryAt beats nil RetryAt on second": {
			a:    &BlockingPreemptionLimit{Limit: "z", RetryAt: &t2},
			b:    &BlockingPreemptionLimit{Limit: "a"},
			want: &BlockingPreemptionLimit{Limit: "z", RetryAt: &t2},
		},
		"finite RetryAt beats nil RetryAt on first": {
			a:    &BlockingPreemptionLimit{Limit: "a"},
			b:    &BlockingPreemptionLimit{Limit: "z", RetryAt: &t2},
			want: &BlockingPreemptionLimit{Limit: "z", RetryAt: &t2},
		},
		"earlier RetryAt wins when first is earlier": {
			a:    &BlockingPreemptionLimit{Limit: "z", RetryAt: &t1},
			b:    &BlockingPreemptionLimit{Limit: "a", RetryAt: &t2},
			want: &BlockingPreemptionLimit{Limit: "z", RetryAt: &t1},
		},
		"earlier RetryAt wins when second is earlier": {
			a:    &BlockingPreemptionLimit{Limit: "a", RetryAt: &t2},
			b:    &BlockingPreemptionLimit{Limit: "z", RetryAt: &t1},
			want: &BlockingPreemptionLimit{Limit: "z", RetryAt: &t1},
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if diff := cmp.Diff(tc.want, tc.a.Earlier(tc.b)); diff != "" {
				t.Errorf("Earlier() (-want,+got):\n%s", diff)
			}
		})
	}
}
