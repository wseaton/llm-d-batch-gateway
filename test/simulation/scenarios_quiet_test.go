//go:build simulation

package simulation

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/llm-d/llm-d-batch-gateway/internal/shared/openai"
)

// quietGenTokens makes each request run about 45s (30ms per token), far
// longer than the 2s progress interval, so no result arrives to make the
// processor write progress while the scenario changes the job's row.
const quietGenTokens = 1500

// TestQuietIntervalCancelLost: the apiserver writes cancelling and dies
// before it inserts the cancel event, while every request of the batch is
// still generating. No result arrives, so nothing on the processor's
// result path looks at the row.
//
// Invariant (bounded cancel latency): a persisted cancelling is honoured
// within a few progress intervals, not when the in-flight requests finish.
func TestQuietIntervalCancelLost(t *testing.T) {
	const scenario = "quiet_interval_cancel_lost"
	const bound = 10 * time.Second
	h := newHarness(t, map[string]string{
		"APISERVER_FAILPOINTS": "apiserver/after-cancel-dbupdate=exit",
	})
	client := newAPIClient()
	fileID, err := client.uploadFile("quiet-cancel-lost.jsonl", inputJSONL(2, quietGenTokens))
	if err != nil {
		t.Fatalf("upload input file: %v", err)
	}
	batch, err := client.createBatch(fileID, "24h")
	if err != nil {
		t.Fatalf("create batch: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tl := observe(ctx, client, batch.ID, h.rec)

	if _, ok := waitForStatus(client, batch.ID, 60*time.Second, openai.BatchStatusInProgress); !ok {
		t.Fatal("batch never reached in_progress")
	}
	time.Sleep(5 * time.Second)
	if _, err := client.cancelBatch(batch.ID); err == nil {
		t.Fatal("cancel succeeded; expected the armed failpoint to kill the apiserver after the DB write")
	}
	cancelledAt := time.Now()
	h.rec.event("cancel-failed", map[string]any{"batch": batch.ID})

	h.setEnv("APISERVER_FAILPOINTS", "")
	h.restart("apiserver")

	honoured, ok := waitForStatus(client, batch.ID, bound-time.Since(cancelledAt), openai.BatchStatusCancelled)
	latency := time.Since(cancelledAt)
	final := honoured
	if !ok {
		final, _ = waitForStatus(client, batch.ID, 2*time.Minute,
			openai.BatchStatusCompleted, openai.BatchStatusCancelled, openai.BatchStatusFailed)
		latency = time.Since(cancelledAt)
	}
	time.Sleep(1 * time.Second)
	cancel()

	var msgs []string
	for _, v := range tl.checkTransitions() {
		msgs = append(msgs, v.String())
	}
	reproduced := !ok || len(msgs) > 0
	detail := fmt.Sprintf("%s %s after the cancelling write (bound %s); sequence %v",
		final.Status, latency.Round(time.Second), bound, tl.statuses())
	if len(msgs) > 0 {
		detail = strings.Join(msgs, "; ") + "; " + detail
	}
	judge(t, scenario, reproduced, detail)
}

// TestQuietIntervalReclaim: a reclaimer bumps the job's epoch while the
// owner's in-flight requests are all still generating and more lines wait
// behind the per-endpoint limit. The owner's writes are fenced, but no
// write happens until a result arrives.
//
// Invariant (bounded waste): once ownership has moved, the previous owner
// sends no further inference requests.
func TestQuietIntervalReclaim(t *testing.T) {
	const scenario = "quiet_interval_reclaim"
	// per_endpoint is 5 in config/processor.yaml: five lines in flight, three
	// waiting for a slot.
	const lines = 8
	const inFlight = 5
	h := newHarness(t, nil)
	baseline := h.inferenceWitness()
	client := newAPIClient()
	fileID, err := client.uploadFile("quiet-reclaim.jsonl", inputJSONL(lines, quietGenTokens))
	if err != nil {
		t.Fatalf("upload input file: %v", err)
	}
	batch, err := client.createBatch(fileID, "24h")
	if err != nil {
		t.Fatalf("create batch: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tl := observe(ctx, client, batch.ID, h.rec)

	if _, ok := waitForStatus(client, batch.ID, 60*time.Second, openai.BatchStatusInProgress); !ok {
		t.Fatal("batch never reached in_progress")
	}
	for deadline := time.Now().Add(30 * time.Second); h.inferenceWitness()-baseline < inFlight; {
		if time.Now().After(deadline) {
			t.Fatalf("only %d requests started", h.inferenceWitness()-baseline)
		}
		time.Sleep(500 * time.Millisecond)
	}
	time.Sleep(3 * time.Second)
	startedBefore := h.inferenceWitness() - baseline
	h.rec.event("witness", map[string]any{"phase": "before-reclaim", "started": startedBefore})

	h.execSQL(fmt.Sprintf("UPDATE batch_items SET epoch = epoch + 1 WHERE id = '%s'", batch.ID))

	// Past the point where the in-flight requests would have finished and
	// freed their slots for the waiting lines.
	time.Sleep(time.Duration(quietGenTokens)*30*time.Millisecond + 20*time.Second)
	startedAfter := h.inferenceWitness() - baseline - startedBefore
	h.rec.event("witness", map[string]any{"phase": "after-reclaim", "started": startedAfter})
	cancel()

	reproduced := startedAfter > 0
	detail := fmt.Sprintf("requests started: %d before the epoch bump, %d after (of %d lines); sequence %v",
		startedBefore, startedAfter, lines, tl.statuses())
	judge(t, scenario, reproduced, detail)
}
