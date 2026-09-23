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
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	toxiproxy "github.com/Shopify/toxiproxy/v2/client"
	"github.com/llm-d/llm-d-batch-gateway/internal/shared/openai"
)

// Scenarios for the all-Postgres topology: the gateway control plane and
// llm-d-async's sql transport share one database, and the dispatcher services
// replace the harness-run queue consumer.

// asyncHarness starts the async topology with the result-insert witness armed.
func asyncHarness(t *testing.T) *harness {
	t.Helper()
	h := newHarness(t, map[string]string{"PROCESSOR_CONFIG": "processor-async.yaml"})
	h.waitForSchema()
	h.sql(`CREATE TABLE IF NOT EXISTS sim_result_inserts (id TEXT NOT NULL, request_token TEXT NOT NULL)`)
	h.sql(`CREATE OR REPLACE FUNCTION sim_result_insert() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
	INSERT INTO sim_result_inserts (id, request_token) VALUES (NEW.id, NEW.request_token);
	RETURN NEW;
END $$`)
	h.sql(`DROP TRIGGER IF EXISTS sim_result_insert ON async_results`)
	h.sql(`CREATE TRIGGER sim_result_insert AFTER INSERT ON async_results FOR EACH ROW EXECUTE FUNCTION sim_result_insert()`)
	return h
}

// waitForSchema waits for the dispatcher to create the async tables.
func (h *harness) waitForSchema() {
	h.t.Helper()
	for deadline := time.Now().Add(time.Minute); time.Now().Before(deadline); time.Sleep(time.Second) {
		if h.sql(`SELECT to_regclass('async_results') IS NOT NULL`) == "t" {
			return
		}
	}
	h.t.Fatal("the dispatcher never created the async tables")
}

func (h *harness) sqlInt(query string) int {
	h.t.Helper()
	n, err := strconv.Atoi(h.sql(query))
	if err != nil {
		h.t.Fatalf("query %q: %v", query, err)
	}
	return n
}

// waitDispatched waits until at least n requests are dispatched and in flight.
func (h *harness) waitDispatched(n int, timeout time.Duration) {
	h.t.Helper()
	for deadline := time.Now().Add(timeout); time.Now().Before(deadline); time.Sleep(200 * time.Millisecond) {
		if h.sqlInt(`SELECT count(*) FROM async_requests WHERE dispatch_epoch > 0`) >= n {
			return
		}
	}
	h.t.Fatalf("fewer than %d requests were dispatched within %v; requests by queue and epoch: %q; partition owners: %q",
		n, timeout,
		h.sql(`SELECT string_agg(queue || ' epoch=' || dispatch_epoch || ' n=' || n, ', ') FROM (SELECT queue, dispatch_epoch, count(*) AS n FROM async_requests GROUP BY 1, 2) q`),
		h.sql(`SELECT string_agg(queue || ' ' || owner || ' draining=' || draining || ' n=' || n, ', ') FROM (SELECT queue, owner, draining, count(*) AS n FROM async_partitions GROUP BY 1, 2, 3) p`)+
			"; not_before-now: "+h.sql(`SELECT string_agg((not_before - extract(epoch FROM now())::bigint)::text, ',') FROM async_requests`)+
			"; members: "+h.sql(`SELECT string_agg(queue || ' ' || owner || ' expires_in_ms=' || (expires_ms - (extract(epoch FROM clock_timestamp()) * 1000)::bigint), ', ') FROM async_dispatchers`))
}

// outcome is what the end-state invariants observed for one batch.
type outcome struct {
	final         openai.Batch
	lines         int
	missing       []string
	duplicated    []string
	served        int
	extraBound    int
	leftRequests  int
	leftResults   int
	doubleResults int
}

// violations lists every end-state invariant the batch broke: it must
// complete with each request answered exactly once, the engine must have run
// every request and no more than extraBound again, and the async tables must
// drain with at most one result committed per request generation.
func (o outcome) violations() []string {
	var v []string
	if o.final.Status != openai.BatchStatusCompleted {
		v = append(v, fmt.Sprintf("status %s, want completed", o.final.Status))
	}
	c := o.final.RequestCounts
	if c.Total != int64(o.lines) || c.Completed+c.Failed != c.Total {
		v = append(v, fmt.Sprintf("counts total=%d completed=%d failed=%d for %d lines", c.Total, c.Completed, c.Failed, o.lines))
	}
	if len(o.missing) > 0 {
		v = append(v, fmt.Sprintf("no record for %v", o.missing))
	}
	if len(o.duplicated) > 0 {
		v = append(v, fmt.Sprintf("several records for %v", o.duplicated))
	}
	if o.served < o.lines {
		v = append(v, fmt.Sprintf("engine served %d of %d requests", o.served, o.lines))
	}
	if o.served > o.lines+o.extraBound {
		v = append(v, fmt.Sprintf("engine served %d for %d requests, more than the %d redeliveries the faults allow", o.served, o.lines, o.extraBound))
	}
	if o.leftRequests > 0 || o.leftResults > 0 {
		v = append(v, fmt.Sprintf("async tables not drained: %d requests, %d results", o.leftRequests, o.leftResults))
	}
	if o.doubleResults > 0 {
		v = append(v, fmt.Sprintf("%d request generations had several results committed", o.doubleResults))
	}
	return v
}

func (o outcome) String() string {
	return fmt.Sprintf("status=%s counts=%+v served=%d/%d (+%d allowed) left=%d/%d doubleResults=%d",
		o.final.Status, o.final.RequestCounts, o.served, o.lines, o.extraBound, o.leftRequests, o.leftResults, o.doubleResults)
}

// checkOutcome waits for the batch to terminalize and the async tables to
// drain, then gathers everything outcome.violations judges.
func (h *harness) checkOutcome(client *apiClient, batchID string, lines, baseline, extraBound int) outcome {
	h.t.Helper()
	final, terminal := waitForStatus(client, batchID, 6*time.Minute,
		openai.BatchStatusCompleted, openai.BatchStatusFailed, openai.BatchStatusExpired, openai.BatchStatusCancelled)
	if !terminal {
		h.t.Fatalf("batch %s never terminalized; last status %s", batchID, final.Status)
	}
	o := outcome{final: final, lines: lines, extraBound: extraBound}

	seen := map[string]int{}
	for _, id := range []*string{final.OutputFileID, final.ErrorFileID} {
		if id == nil || *id == "" {
			continue
		}
		body, err := client.fileContent(*id)
		if err != nil {
			h.t.Fatalf("download %s: %v", *id, err)
		}
		for _, line := range strings.Split(strings.TrimSpace(body), "\n") {
			var rec struct {
				CustomID string `json:"custom_id"`
			}
			if err := json.Unmarshal([]byte(line), &rec); err != nil {
				h.t.Fatalf("decode record %q: %v", line, err)
			}
			seen[rec.CustomID]++
		}
	}
	for i := range lines {
		id := fmt.Sprintf("sim-%d", i)
		switch n := seen[id]; {
		case n == 0:
			o.missing = append(o.missing, id)
		case n > 1:
			o.duplicated = append(o.duplicated, id)
		}
	}

	for deadline := time.Now().Add(time.Minute); time.Now().Before(deadline); time.Sleep(time.Second) {
		o.leftRequests = h.sqlInt(`SELECT count(*) FROM async_requests`)
		o.leftResults = h.sqlInt(`SELECT count(*) FROM async_results`)
		if o.leftRequests == 0 && o.leftResults == 0 {
			break
		}
	}
	// Engine requests a crashed dispatcher already sent still finish after it dies.
	for last, stable := -1, 0; stable < 3; time.Sleep(2 * time.Second) {
		o.served = h.inferenceWitness() - baseline
		if o.served == last {
			stable++
		} else {
			last, stable = o.served, 0
		}
	}
	o.doubleResults = h.sqlInt(`SELECT count(*) FROM (SELECT 1 FROM sim_result_inserts GROUP BY id, request_token HAVING count(*) > 1) d`)
	h.rec.event("outcome", map[string]any{"outcome": o.String(), "violations": o.violations()})
	return o
}

func startBatch(t *testing.T, h *harness, client *apiClient, name string, lines, maxTokens int) string {
	t.Helper()
	fileID, err := client.uploadFile(name+".jsonl", inputJSONL(lines, maxTokens))
	if err != nil {
		t.Fatalf("upload input file: %v", err)
	}
	batch, err := client.createBatch(fileID, "24h")
	if err != nil {
		t.Fatalf("create batch: %v", err)
	}
	if _, ok := waitForStatus(client, batch.ID, 60*time.Second, openai.BatchStatusInProgress); !ok {
		t.Fatal("batch never reached in_progress")
	}
	return batch.ID
}

func judgeOutcome(t *testing.T, scenario string, o outcome) {
	t.Helper()
	v := o.violations()
	judge(t, scenario, len(v) > 0, fmt.Sprintf("%s; violations %v", o, v))
}

// TestDispatcherCrashRedelivers kills the only dispatcher with requests in
// flight. Its partitions lapse and its replacement redelivers them.
//
// Invariant: every request answered once, each run at most once more.
func TestDispatcherCrashRedelivers(t *testing.T) {
	const scenario = "dispatcher_crash_redelivers"
	const lines = 8
	h := asyncHarness(t)
	client := newAPIClient()
	baseline := h.inferenceWitness()

	batchID := startBatch(t, h, client, scenario, lines, 300)
	h.waitDispatched(lines, time.Minute)
	h.kill("dispatcher")
	h.restart("dispatcher")

	judgeOutcome(t, scenario, h.checkOutcome(client, batchID, lines, baseline, lines))
}

// TestZombieDispatcherFenced freezes a dispatcher with requests in flight until
// its peer has taken its partitions and redelivered them, then thaws it. The
// thawed dispatcher's late results carry an attempt its peer's redelivery
// replaced, so they must not be committed.
//
// Invariant: at most one result committed per request generation.
func TestZombieDispatcherFenced(t *testing.T) {
	const scenario = "zombie_dispatcher_fenced"
	const lines = 8
	h := asyncHarness(t)
	h.restart("dispatcher-1")
	client := newAPIClient()
	baseline := h.inferenceWitness()

	batchID := startBatch(t, h, client, scenario, lines, 300)
	h.waitDispatched(lines, time.Minute)
	h.pause("dispatcher")
	time.Sleep(20 * time.Second)
	h.unpause("dispatcher")

	judgeOutcome(t, scenario, h.checkOutcome(client, batchID, lines, baseline, lines))
}

// TestPostgresRestartMidBatch crashes the database every component shares
// while requests are in flight.
//
// Invariant: every component reconnects and the batch completes exactly.
func TestPostgresRestartMidBatch(t *testing.T) {
	const scenario = "postgres_restart_mid_batch"
	const lines = 8
	h := asyncHarness(t)
	client := newAPIClient()
	baseline := h.inferenceWitness()

	batchID := startBatch(t, h, client, scenario, lines, 300)
	h.waitDispatched(lines, time.Minute)
	h.kill("postgres")
	time.Sleep(5 * time.Second)
	h.restart("postgres")

	judgeOutcome(t, scenario, h.checkOutcome(client, batchID, lines, baseline, lines))
}

// resetWindow resets every connection on proxyName a fixed time after it
// opens, for window, so statements in flight lose their replies after the
// server has run them.
func (h *harness) resetWindow(proxyName string, after, window time.Duration) {
	h.t.Helper()
	client := h.toxics()
	h.addToxic(client, proxyName, "reset_peer", "downstream", toxiproxy.Attributes{"timeout": int(after.Milliseconds())})
	time.Sleep(window)
	h.healToxics(client)
}

// TestDispatcherPostgresReplyLoss resets the dispatchers' database
// connections shortly after they open, so dispatches, acks and lease writes
// commit without the dispatcher learning they did.
//
// Invariant: lost replies redeliver but never double-commit a result.
func TestDispatcherPostgresReplyLoss(t *testing.T) {
	const scenario = "dispatcher_pg_reply_loss"
	const lines = 16
	h := asyncHarness(t)
	h.restart("dispatcher-1")
	client := newAPIClient()
	baseline := h.inferenceWitness()

	batchID := startBatch(t, h, client, scenario, lines, 100)
	h.resetWindow("dispatcher-postgres", 700*time.Millisecond, 30*time.Second)

	judgeOutcome(t, scenario, h.checkOutcome(client, batchID, lines, baseline, 2*lines))
}

// TestProcessorPostgresReplyLoss resets the processor's database connections
// while it submits requests and reads results.
//
// Invariant: a submit whose reply was lost must not run a request twice as
// two generations, and a lost result read must not drop the result.
func TestProcessorPostgresReplyLoss(t *testing.T) {
	const scenario = "processor_pg_reply_loss"
	const lines = 16
	h := asyncHarness(t)
	client := newAPIClient()
	baseline := h.inferenceWitness()

	batchID := startBatch(t, h, client, scenario, lines, 100)
	h.resetWindow("processor-postgres", 700*time.Millisecond, 30*time.Second)

	judgeOutcome(t, scenario, h.checkOutcome(client, batchID, lines, baseline, lines))
}

// TestDispatcherEngineResets cuts the dispatcher's connections to the engine
// mid-response, so requests run but their responses never arrive.
//
// Invariant: the dispatcher retries until every request is answered once.
func TestDispatcherEngineResets(t *testing.T) {
	const scenario = "dispatcher_engine_resets"
	const lines = 8
	h := asyncHarness(t)
	client := newAPIClient()
	baseline := h.inferenceWitness()

	batchID := startBatch(t, h, client, scenario, lines, 100)
	h.resetWindow("dispatcher-vcr", 2*time.Second, 20*time.Second)

	judgeOutcome(t, scenario, h.checkOutcome(client, batchID, lines, baseline, 4*lines))
}
