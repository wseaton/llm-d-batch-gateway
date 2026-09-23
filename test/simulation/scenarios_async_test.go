//go:build simulation

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

package simulation

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/llm-d/llm-d-batch-gateway/internal/shared/openai"
)

// TestAsyncResultDestruction reproduces this failure: in async dispatch
// the processor submits requests fire-and-forget with an in-memory pending
// map as the only record, and long-lived ResultBroadcasters pop results from
// its result route destructively. Kill the processor after submission: the
// pending map dies with it, the replacement starts fresh broadcasters with no
// subscribers, and when the results arrive they are popped and discarded. The
// reconciler terminalizes the orphan as failed.
//
// Violated invariant (work conservation): inference that was paid for and
// whose results were durably produced must not be silently destroyed.
func TestAsyncResultDestruction(t *testing.T) {
	const scenario = "async_result_destruction"
	const lines = 4
	h := asyncHarness(t)
	baseline := h.inferenceWitness()
	client := newAPIClient()

	// ~9s generations so the kill lands after submission, before results.
	fileID, err := client.uploadFile("async-result-destruction.jsonl", inputJSONL(lines, 300))
	if err != nil {
		t.Fatalf("upload input file: %v", err)
	}
	batch, err := client.createBatch(fileID, "24h")
	if err != nil {
		t.Fatalf("create batch: %v", err)
	}

	obsCtx, cancelObs := context.WithCancel(context.Background())
	defer cancelObs()
	tl := observe(obsCtx, client, batch.ID, h.rec)

	if _, ok := waitForStatus(client, batch.ID, 60*time.Second, openai.BatchStatusInProgress); !ok {
		t.Fatal("batch never reached in_progress")
	}
	// All requests are enqueued to the async queue right after in_progress.
	time.Sleep(1500 * time.Millisecond)
	h.kill("processor")
	h.restart("processor")

	final, terminal := waitForStatus(client, batch.ID, 3*time.Minute,
		openai.BatchStatusFailed, openai.BatchStatusCompleted, openai.BatchStatusExpired)
	if !terminal {
		t.Fatalf("batch never terminalized after worker loss; last status %s", final.Status)
	}

	// Wait for the engine to finish serving every submitted request and for
	// the replacement's broadcaster to drain whatever arrives.
	served := 0
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		served = h.inferenceWitness() - baseline
		if served >= lines && h.sqlInt(`SELECT count(*) FROM async_results`) == 0 {
			break
		}
		time.Sleep(2 * time.Second)
	}
	time.Sleep(5 * time.Second) // let the broadcaster pop stragglers
	remaining := h.sqlInt(`SELECT count(*) FROM async_results`)
	h.rec.event("witness", map[string]any{"served": served, "resultsRemaining": remaining})
	time.Sleep(1 * time.Second)
	cancelObs()

	destroyed := final.Status == openai.BatchStatusFailed &&
		final.OutputFileID == nil && final.ErrorFileID == nil &&
		served >= lines && remaining == 0
	detail := fmt.Sprintf("observed sequence %v, final %s, engine served %d/%d, results left unconsumed %d",
		tl.statuses(), final.Status, served, lines, remaining)
	judge(t, scenario, destroyed, detail)
}
