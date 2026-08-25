/*
Copyright 2026 The llm-d Authors

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

package semaphore

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/go-logr/logr"
)

func defaultCfg() AIMDConfig {
	return AIMDConfig{
		MinLimit:         5,
		MaxLimit:         100,
		BackoffFactor:    0.5,
		AdditiveIncrease: 1,
	}
}

func newTestController(cfg AIMDConfig, initial int) (*AIMDController, *atomic.Int32) {
	var latest atomic.Int32
	latest.Store(int32(initial))
	c := NewAIMDController(cfg, initial, func(n int) {
		latest.Store(int32(n))
	}, logr.Discard())
	return c, &latest
}

func TestAIMDInitialClamp(t *testing.T) {
	tests := []struct {
		name      string
		initial   int
		want      int
		wantSetFn bool // true if setFn should have been called
	}{
		{name: "within range", initial: 50, want: 50, wantSetFn: false},
		{name: "below min", initial: 1, want: 5, wantSetFn: true},
		{name: "above max", initial: 200, want: 100, wantSetFn: true},
		{name: "at min", initial: 5, want: 5, wantSetFn: false},
		{name: "at max", initial: 100, want: 100, wantSetFn: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, latest := newTestController(defaultCfg(), tt.initial)
			if got := c.Limit(); got != tt.want {
				t.Fatalf("Limit() = %d, want %d", got, tt.want)
			}
			if tt.wantSetFn {
				if got := latest.Load(); got != int32(tt.want) {
					t.Fatalf("setFn called with %d, want %d", got, tt.want)
				}
			}
		})
	}
}

func TestAIMDAdditiveIncrease(t *testing.T) {
	t.Run("increases after one full window", func(t *testing.T) {
		cfg := defaultCfg()
		cfg.MaxLimit = 20
		c, latest := newTestController(cfg, 10)

		// 10 successes = one window at limit 10
		for i := 0; i < 10; i++ {
			c.RecordSuccess()
		}
		if got := c.Limit(); got != 11 {
			t.Fatalf("after window: Limit() = %d, want 11", got)
		}
		if got := latest.Load(); got != 11 {
			t.Fatalf("setFn called with %d, want 11", got)
		}
	})

	t.Run("does not increase before full window", func(t *testing.T) {
		c, _ := newTestController(defaultCfg(), 10)
		for i := 0; i < 9; i++ {
			c.RecordSuccess()
		}
		if got := c.Limit(); got != 10 {
			t.Fatalf("before window complete: Limit() = %d, want 10", got)
		}
	})

	t.Run("clamps at MaxLimit", func(t *testing.T) {
		cfg := defaultCfg()
		cfg.MaxLimit = 11
		c, _ := newTestController(cfg, 10)

		// window of 10 → limit becomes 11
		for i := 0; i < 10; i++ {
			c.RecordSuccess()
		}
		if got := c.Limit(); got != 11 {
			t.Fatalf("Limit() = %d, want 11", got)
		}

		// another window of 11 → should stay at 11 (MaxLimit)
		for i := 0; i < 11; i++ {
			c.RecordSuccess()
		}
		if got := c.Limit(); got != 11 {
			t.Fatalf("Limit() = %d, want 11 (clamped)", got)
		}
	})

	t.Run("additive increase > 1", func(t *testing.T) {
		cfg := defaultCfg()
		cfg.AdditiveIncrease = 5
		cfg.MaxLimit = 100
		c, _ := newTestController(cfg, 10)

		for i := 0; i < 10; i++ {
			c.RecordSuccess()
		}
		if got := c.Limit(); got != 15 {
			t.Fatalf("Limit() = %d, want 15", got)
		}
	})
}

func TestAIMDMultiplicativeDecrease(t *testing.T) {
	t.Run("halves on rate limit", func(t *testing.T) {
		c, latest := newTestController(defaultCfg(), 100)

		c.RecordRateLimit("429")
		if got := c.Limit(); got != 50 {
			t.Fatalf("after 1 rate limit: Limit() = %d, want 50", got)
		}
		if got := latest.Load(); got != 50 {
			t.Fatalf("setFn called with %d, want 50", got)
		}
	})

	t.Run("multiple decreases", func(t *testing.T) {
		c, _ := newTestController(defaultCfg(), 100)

		c.RecordRateLimit("429") // 100 → 50
		c.RecordRateLimit("5xx") // 50 → 25
		c.RecordRateLimit("429") // 25 → 12
		if got := c.Limit(); got != 12 {
			t.Fatalf("Limit() = %d, want 12", got)
		}
	})

	t.Run("clamps at MinLimit", func(t *testing.T) {
		c, _ := newTestController(defaultCfg(), 10)

		c.RecordRateLimit("429") // 10 → 5
		c.RecordRateLimit("429") // 5 → stays 5 (MinLimit)
		if got := c.Limit(); got != 5 {
			t.Fatalf("Limit() = %d, want 5 (clamped)", got)
		}
	})

	t.Run("resets success counter", func(t *testing.T) {
		c, _ := newTestController(defaultCfg(), 10)

		// 9 successes (almost a full window)
		for i := 0; i < 9; i++ {
			c.RecordSuccess()
		}

		// rate limit resets counter
		c.RecordRateLimit("429") // 10 → 5

		// need 5 more successes for new window (not 1)
		for i := 0; i < 4; i++ {
			c.RecordSuccess()
		}
		if got := c.Limit(); got != 5 {
			t.Fatalf("Limit() = %d, want 5 (counter was reset)", got)
		}
		c.RecordSuccess() // 5th success completes window
		if got := c.Limit(); got != 6 {
			t.Fatalf("Limit() = %d, want 6", got)
		}
	})
}

func TestAIMDBackoffFactor(t *testing.T) {
	tests := []struct {
		name   string
		factor float64
		start  int
		want   int
	}{
		{name: "0.5 factor", factor: 0.5, start: 100, want: 50},
		{name: "0.7 factor", factor: 0.7, start: 100, want: 70},
		{name: "0.9 factor", factor: 0.9, start: 100, want: 90},
		{name: "0.3 factor floors", factor: 0.3, start: 10, want: 5}, // floor(3.0) = 3, but MinLimit=5
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := defaultCfg()
			cfg.BackoffFactor = tt.factor
			c, _ := newTestController(cfg, tt.start)
			c.RecordRateLimit("429")
			if got := c.Limit(); got != tt.want {
				t.Fatalf("Limit() = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestAIMDSawtoothPattern(t *testing.T) {
	t.Run("increase then decrease then increase", func(t *testing.T) {
		cfg := defaultCfg()
		cfg.MinLimit = 5
		cfg.MaxLimit = 20
		c, _ := newTestController(cfg, 10)

		// Window of 10 → 11
		for i := 0; i < 10; i++ {
			c.RecordSuccess()
		}
		if got := c.Limit(); got != 11 {
			t.Fatalf("after increase: Limit() = %d, want 11", got)
		}

		// Rate limit → 5
		c.RecordRateLimit("429")
		if got := c.Limit(); got != 5 {
			t.Fatalf("after decrease: Limit() = %d, want 5", got)
		}

		// Window of 5 → 6
		for i := 0; i < 5; i++ {
			c.RecordSuccess()
		}
		if got := c.Limit(); got != 6 {
			t.Fatalf("after re-increase: Limit() = %d, want 6", got)
		}
	})
}

func TestAIMDConcurrency(t *testing.T) {
	t.Run("concurrent success and rate limit signals", func(t *testing.T) {
		cfg := defaultCfg()
		cfg.MinLimit = 1
		cfg.MaxLimit = 1000
		c, _ := newTestController(cfg, 100)

		var wg sync.WaitGroup
		for i := 0; i < 200; i++ {
			wg.Add(1)
			go func(n int) {
				defer wg.Done()
				if n%20 == 0 {
					c.RecordRateLimit("429")
				} else {
					c.RecordSuccess()
				}
			}(i)
		}
		wg.Wait()

		limit := c.Limit()
		if limit < cfg.MinLimit || limit > cfg.MaxLimit {
			t.Fatalf("Limit() = %d, out of [%d, %d]", limit, cfg.MinLimit, cfg.MaxLimit)
		}
	})
}

func TestAIMDNoCallbackWhenUnchanged(t *testing.T) {
	t.Run("no setFn call when already at MinLimit", func(t *testing.T) {
		var calls atomic.Int32
		cfg := defaultCfg()
		c := NewAIMDController(cfg, 5, func(n int) {
			calls.Add(1)
		}, logr.Discard())

		// Already at MinLimit=5, floor(5*0.5)=2 → clamped to 5 → no change
		c.RecordRateLimit("429")
		if got := calls.Load(); got != 0 {
			t.Fatalf("setFn called %d times, want 0", got)
		}
	})

	t.Run("no setFn call when already at MaxLimit", func(t *testing.T) {
		var calls atomic.Int32
		cfg := defaultCfg()
		c := NewAIMDController(cfg, 100, func(n int) {
			calls.Add(1)
		}, logr.Discard())

		// Window of 100 successes → limit stays 100 (MaxLimit)
		for i := 0; i < 100; i++ {
			c.RecordSuccess()
		}
		if got := calls.Load(); got != 0 {
			t.Fatalf("setFn called %d times, want 0", got)
		}
	})
}

func BenchmarkAIMDRecordSuccess(b *testing.B) {
	c, _ := newTestController(defaultCfg(), 50)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.RecordSuccess()
	}
}

func BenchmarkAIMDRecordRateLimit(b *testing.B) {
	c, _ := newTestController(defaultCfg(), 50)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.RecordRateLimit("429")
	}
}

// TestAIMDTrajectory pins the limit sequence a signal sequence produces, so a
// change to the window or backoff rule shows up as a diff against the spec
// (docs/design/aimd-controller.md).
func TestAIMDTrajectory(t *testing.T) {
	type step struct {
		signal string // "ok" or a rate-limit reason
		times  int
	}
	tests := []struct {
		name     string
		cfg      AIMDConfig
		initial  int
		steps    []step
		wantSets []int // every setFn call, in order
		wantEnd  int
	}{
		{
			name:    "one window per increase, window length is the current limit",
			cfg:     AIMDConfig{MinLimit: 1, MaxLimit: 10, BackoffFactor: 0.5, AdditiveIncrease: 1},
			initial: 2,
			steps:   []step{{"ok", 2}, {"ok", 3}, {"ok", 4}},
			// 2 successes -> 3, then 3 -> 4, then 4 -> 5
			wantSets: []int{3, 4, 5},
			wantEnd:  5,
		},
		{
			name:    "decrease halves and rounds down",
			cfg:     AIMDConfig{MinLimit: 1, MaxLimit: 100, BackoffFactor: 0.5, AdditiveIncrease: 1},
			initial: 7,
			steps:   []step{{"429", 1}, {"429", 1}},
			// floor(7*0.5)=3, floor(3*0.5)=1
			wantSets: []int{3, 1},
			wantEnd:  1,
		},
		{
			name:    "decrease clamps at the floor and stops calling setFn",
			cfg:     AIMDConfig{MinLimit: 5, MaxLimit: 20, BackoffFactor: 0.5, AdditiveIncrease: 1},
			initial: 20,
			steps:   []step{{"503", 3}},
			// 20 -> 10 -> 5 -> 5 (unchanged, no call)
			wantSets: []int{10, 5},
			wantEnd:  5,
		},
		{
			name:    "a decrease discards partial window progress",
			cfg:     AIMDConfig{MinLimit: 1, MaxLimit: 20, BackoffFactor: 0.5, AdditiveIncrease: 1},
			initial: 10,
			// 9 successes, decrease to 5, then 4 successes: no increase yet, the 5th triggers it
			steps:    []step{{"ok", 9}, {"capacity_retry", 1}, {"ok", 4}, {"ok", 1}},
			wantSets: []int{5, 6},
			wantEnd:  6,
		},
		{
			name:    "a decrease at the floor still resets the window",
			cfg:     AIMDConfig{MinLimit: 5, MaxLimit: 20, BackoffFactor: 0.5, AdditiveIncrease: 1},
			initial: 5,
			// 4 successes, a no-op decrease, then 4 more: still no increase; the 5th after reset increases
			steps:    []step{{"ok", 4}, {"429", 1}, {"ok", 4}, {"ok", 1}},
			wantSets: []int{6},
			wantEnd:  6,
		},
		{
			name:    "increase clamps at the ceiling and stops calling setFn",
			cfg:     AIMDConfig{MinLimit: 1, MaxLimit: 3, BackoffFactor: 0.5, AdditiveIncrease: 5},
			initial: 2,
			steps:   []step{{"ok", 2}, {"ok", 3}},
			// 2 -> min(7,3)=3, then 3 -> 3 (unchanged, no call)
			wantSets: []int{3},
			wantEnd:  3,
		},
		{
			name:     "recovery from the floor takes min successes per step",
			cfg:      AIMDConfig{MinLimit: 5, MaxLimit: 20, BackoffFactor: 0.5, AdditiveIncrease: 1},
			initial:  20,
			steps:    []step{{"429", 2}, {"ok", 5}, {"ok", 6}, {"ok", 7}},
			wantSets: []int{10, 5, 6, 7, 8},
			wantEnd:  8,
		},
		{
			name:    "backoff factor other than one half",
			cfg:     AIMDConfig{MinLimit: 1, MaxLimit: 100, BackoffFactor: 0.8, AdditiveIncrease: 2},
			initial: 10,
			// floor(10*0.8)=8; window of 8 -> 10
			steps:    []step{{"5xx", 1}, {"ok", 8}},
			wantSets: []int{8, 10},
			wantEnd:  10,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var sets []int
			c := NewAIMDController(tt.cfg, tt.initial, func(n int) { sets = append(sets, n) }, logr.Discard())
			for _, st := range tt.steps {
				for range st.times {
					if st.signal == "ok" {
						c.RecordSuccess()
					} else {
						c.RecordRateLimit(st.signal)
					}
				}
			}
			if got := c.Limit(); got != tt.wantEnd {
				t.Errorf("Limit() = %d, want %d", got, tt.wantEnd)
			}
			if len(sets) != len(tt.wantSets) {
				t.Fatalf("setFn calls = %v, want %v", sets, tt.wantSets)
			}
			for i := range sets {
				if sets[i] != tt.wantSets[i] {
					t.Fatalf("setFn calls = %v, want %v", sets, tt.wantSets)
				}
			}
		})
	}
}

// TestAIMDSemaphoreNeverDiverges drives a real AdaptiveSemaphore through the
// controller from many goroutines and checks, after every signal and at the
// end, that the semaphore's limit equals the controller's. setFn runs under
// the controller mutex, so the two can never be observed apart.
func TestAIMDSemaphoreNeverDiverges(t *testing.T) {
	sem, err := NewAdaptive(64, nil)
	if err != nil {
		t.Fatal(err)
	}
	cfg := AIMDConfig{MinLimit: 2, MaxLimit: 64, BackoffFactor: 0.5, AdditiveIncrease: 3}
	c := NewAIMDController(cfg, 64, sem.SetLimit, logr.Discard())

	var wg sync.WaitGroup
	var mismatches atomic.Int32
	for g := range 16 {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := range 500 {
				if (i+g)%7 == 0 {
					c.RecordRateLimit("429")
				} else {
					c.RecordSuccess()
				}
				// Read both under no lock: a diverged pair is a real observable state.
				cl, sl := c.Limit(), sem.Limit()
				if cl != sl {
					// Another goroutine may have moved both between the two reads;
					// re-read once to separate a torn read from a real divergence.
					if cl2, sl2 := c.Limit(), sem.Limit(); cl2 == cl && sl2 == sl {
						mismatches.Add(1)
					}
				}
			}
		}(g)
	}
	wg.Wait()

	if got := mismatches.Load(); got != 0 {
		t.Fatalf("observed %d stable controller/semaphore limit mismatches", got)
	}
	if c.Limit() != sem.Limit() {
		t.Fatalf("final controller limit %d != semaphore limit %d", c.Limit(), sem.Limit())
	}
	if l := c.Limit(); l < cfg.MinLimit || l > cfg.MaxLimit {
		t.Fatalf("Limit() = %d, out of [%d, %d]", l, cfg.MinLimit, cfg.MaxLimit)
	}
}
