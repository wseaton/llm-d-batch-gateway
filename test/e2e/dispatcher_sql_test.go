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

package e2e_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	asyncapi "github.com/llm-d/llm-d-async/api"
	"github.com/openai/openai-go/v3"
)

var testDispatcherSQLRelease = getEnvOrDefault("TEST_DISPATCHER_SQL_RELEASE", "dispatcher-sql")

// testSQLDispatcher runs the dispatcher suite against llm-d-async on the sql
// transport, where requests and results live in the batch gateway's Postgres.
func testSQLDispatcher(t *testing.T) {
	t.Setenv("TEST_DISPATCHER_RELEASE", testDispatcherSQLRelease)

	t.Run("NoRedis", testSQLDeploymentHasNoRedis)
	t.Run("BatchThroughDispatcher", func(t *testing.T) {
		testDispatcherBatchRoundTrip(t, nil)
	})
	t.Run("MultiRequestBatch", func(t *testing.T) {
		testDispatcherMultiRequestBatch(t, nil)
	})
	t.Run("MultiReplicaBatch", testDispatcherMultiReplicaBatch)
	t.Run("HTTPErrorStatusPreserved", testSQLDispatcherHTTPErrorStatusPreserved)
	t.Run("BatchCancel", doTestBatchCancel)
	t.Run("HardKillRecovery", testSQLHardKillRecovery)
}

func testSQLDeploymentHasNoRedis(t *testing.T) {
	out, err := exec.Command("kubectl", "get", "pods,services,statefulsets,deployments",
		"-n", testNamespace, "-o", "name").CombinedOutput()
	if err != nil {
		t.Fatalf("list workloads: %v\n%s", err, out)
	}
	for _, name := range strings.Fields(string(out)) {
		lower := strings.ToLower(name)
		if strings.Contains(lower, "redis") || strings.Contains(lower, "valkey") {
			t.Errorf("found %s in a deployment that should not run Redis", name)
		}
	}

	out, err = exec.Command("kubectl", "get", "secret", "batch-gateway-secrets",
		"-n", testNamespace, "-o", "jsonpath={.data}").CombinedOutput()
	if err != nil {
		t.Fatalf("read gateway secret: %v\n%s", err, out)
	}
	if strings.Contains(string(out), "redis-url") {
		t.Errorf("gateway secret carries a redis-url key: %s", out)
	}
}

// testSQLDispatcherHTTPErrorStatusPreserved is the sql counterpart of
// testDispatcherHTTPErrorStatusPreserved: no dispatcher consumes the inject
// queue, so the test takes the request row and writes a 403 result to the
// Processor's result route itself.
func testSQLDispatcherHTTPErrorStatusPreserved(t *testing.T) {
	jsonl := fmt.Sprintf(
		`{"custom_id":"dreq-403","method":"POST","url":"/v1/chat/completions","body":{"model":"%s","max_tokens":5,"messages":[{"role":"user","content":"expect 403"}]}}`,
		injectModel)
	fileID := mustCreateFile(t, fmt.Sprintf("dispatcher-sql-http-error-%s.jsonl", testRunID), jsonl)
	batchID := mustCreateBatch(t, fileID)
	t.Logf("Created batch %s; waiting for request in %s", batchID, injectReqQueue)

	var id, token, route string
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		row := psqlExec(t, fmt.Sprintf(
			`SELECT id || '|' || request_token || '|' || (envelope::jsonb -> 'internal' ->> 'result_queue_name') FROM async_requests WHERE queue = '%s' LIMIT 1`,
			injectReqQueue))
		if parts := strings.Split(row, "|"); len(parts) == 3 && parts[2] != "" {
			id, token, route = parts[0], parts[1], parts[2]
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if id == "" {
		t.Fatalf("no request appeared in %s within 30s", injectReqQueue)
	}
	t.Logf("Took request %s (route %s); writing a StatusCode=403 result", id, route)

	result, err := json.Marshal(asyncapi.ResultMessage{ID: id, StatusCode: http.StatusForbidden})
	if err != nil {
		t.Fatalf("marshal ResultMessage: %v", err)
	}
	psqlExec(t, fmt.Sprintf(`
WITH taken AS (
	DELETE FROM async_requests WHERE id = '%s' AND request_token = '%s' RETURNING id, request_token
)
INSERT INTO async_results (route, id, request_token, payload, expires_at, created_at)
SELECT '%s', id, request_token, '%s', 0, (extract(epoch FROM now()) * 1000000)::bigint FROM taken`,
		id, token, route, strings.ReplaceAll(string(result), "'", "''")))

	finalBatch := waitForRetryExhaustion(t, batchID, 2*time.Minute)
	if finalBatch.Status != openai.BatchStatusCompleted {
		t.Errorf("expected batch status %q, got %q", openai.BatchStatusCompleted, finalBatch.Status)
	}
	if finalBatch.RequestCounts.Completed != 0 || finalBatch.RequestCounts.Failed != 1 {
		t.Errorf("counts = completed:%d failed:%d, want 0/1",
			finalBatch.RequestCounts.Completed, finalBatch.RequestCounts.Failed)
	}
	if finalBatch.OutputFileID == "" {
		t.Fatal("expected output_file_id with HTTP 403 response")
	}
	if finalBatch.ErrorFileID != "" {
		t.Errorf("expected empty error_file_id (HTTP errors go to output), got %q", finalBatch.ErrorFileID)
	}

	output := fetchOutputFile(t, finalBatch)
	var found403 bool
	for _, line := range strings.Split(strings.TrimSpace(output), "\n") {
		var rl batchResultLine
		if err := json.Unmarshal([]byte(line), &rl); err != nil {
			t.Fatalf("invalid output line: %v\n%s", err, line)
		}
		if rl.Error != nil {
			t.Fatalf("expected HTTP response in output, got error code=%q message=%q", rl.Error.Code, rl.Error.Message)
		}
		if rl.Response == nil {
			t.Fatal("expected response object in output line")
		}
		if rl.Response.StatusCode != http.StatusForbidden {
			t.Errorf("status_code = %d, want 403", rl.Response.StatusCode)
		}
		found403 = true
	}
	if !found403 {
		t.Errorf("expected status_code 403 in output file, got:\n%s", output)
	}
}

// testSQLHardKillRecovery kills the sql dispatcher while it has requests in
// flight. Its partitions pass to the replacement once the lease lapses, which
// redispatches the requests under new attempt tokens; the batch must still
// complete with one record per request.
func testSQLHardKillRecovery(t *testing.T) {
	const requestCount = 2
	countRows := func(where string) int {
		n, err := strconv.Atoi(psqlExec(t, fmt.Sprintf(
			`SELECT count(*) FROM async_requests WHERE queue = '%s'%s`, gateReqQueue, where)))
		if err != nil {
			t.Fatalf("count %s rows: %v", gateReqQueue, err)
		}
		return n
	}
	if n := countRows(""); n != 0 {
		t.Fatalf("%s holds %d rows before the test", gateReqQueue, n)
	}

	engineReleased := false
	releaseEngine := func() error {
		if engineReleased {
			return nil
		}
		if err := tryPatchEngineConfig(t, testSimService, `{"max_num_seqs":128,"time_to_first_token":50,"inter_token_latency":100}`); err != nil {
			return err
		}
		engineReleased = true
		return nil
	}
	var batchID string
	t.Cleanup(func() {
		if err := releaseEngine(); err != nil {
			t.Errorf("cleanup: release engine: %v", err)
		}
		if _, err := waitForReplacementAsyncPod(t, "", 2*time.Minute); err != nil {
			t.Errorf("cleanup: wait for Async pod: %v", err)
		}
		if batchID != "" {
			if err := waitForRecoveryBatchTerminal(batchID, 2*time.Minute); err != nil {
				t.Errorf("cleanup: %v", err)
			}
		}
	})
	patchEngineConfig(t, testSimService, `{"max_num_seqs":1,"time_to_first_token":30000,"inter_token_latency":100}`)

	customIDs := []string{
		fmt.Sprintf("sql-hard-kill-a-%s", testRunID),
		fmt.Sprintf("sql-hard-kill-b-%s", testRunID),
	}
	lines := make([]string, 0, requestCount)
	for _, customID := range customIDs {
		lines = append(lines, fmt.Sprintf(
			`{"custom_id":%q,"method":"POST","url":"/v1/chat/completions","body":{"model":%q,"max_tokens":5,"messages":[{"role":"user","content":"hard kill recovery"}]}}`,
			customID, gateModel))
	}
	fileID := mustCreateFile(t, fmt.Sprintf("dispatcher-sql-hard-kill-%s.jsonl", testRunID), strings.Join(lines, "\n"))
	batchID = mustCreateBatch(t, fileID)

	attempts := func() map[string]string {
		out := psqlExec(t, fmt.Sprintf(
			`SELECT id || '=' || dispatch_attempt FROM async_requests WHERE queue = '%s' AND dispatch_epoch > 0`, gateReqQueue))
		m := map[string]string{}
		for _, line := range strings.Fields(out) {
			if id, attempt, ok := strings.Cut(line, "="); ok {
				m[id] = attempt
			}
		}
		return m
	}
	var before map[string]string
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		if before = attempts(); len(before) > 0 && countRows("") == requestCount {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if len(before) == 0 {
		t.Fatalf("no %s request was dispatched within 60s", gateReqQueue)
	}

	pod, err := readyAsyncPod(t)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("hard-deleting Async pod %s with %d requests in flight: %v", pod.Name, len(before), before)
	out, err := exec.Command("kubectl", "delete", "pod", pod.Name,
		"--namespace", testNamespace, "--grace-period=0", "--force", "--wait=true").CombinedOutput()
	if err != nil {
		t.Fatalf("force-delete Async pod %s: %v\n%s", pod.Name, err, out)
	}
	replacement, err := waitForReplacementAsyncPod(t, pod.UID, 2*time.Minute)
	if err != nil {
		t.Fatal(err)
	}

	deadline = time.Now().Add(60 * time.Second)
	redispatched := false
	for time.Now().Before(deadline) && !redispatched {
		after := attempts()
		redispatched = true
		for id, attempt := range before {
			if got, ok := after[id]; !ok || got == attempt {
				redispatched = false
			}
		}
		if !redispatched {
			time.Sleep(200 * time.Millisecond)
		}
	}
	if !redispatched {
		t.Fatalf("replacement pod %s did not redispatch %v under new attempts", replacement.Name, before)
	}
	t.Logf("replacement pod %s redispatched every in-flight request under a new attempt", replacement.Name)

	if err := releaseEngine(); err != nil {
		t.Fatalf("release engine: %v", err)
	}
	finalBatch, results := waitForBatchStatus(t, batchID, 3*time.Minute, openai.BatchStatusCompleted)
	if finalBatch.RequestCounts.Total != requestCount ||
		finalBatch.RequestCounts.Completed != requestCount ||
		finalBatch.RequestCounts.Failed != 0 {
		t.Errorf("terminal counts = total:%d completed:%d failed:%d, want %d/%d/0",
			finalBatch.RequestCounts.Total, finalBatch.RequestCounts.Completed,
			finalBatch.RequestCounts.Failed, requestCount, requestCount)
	}
	if results == nil {
		t.Fatal("expected terminal output file")
	}
	if results.OutputLines != requestCount || results.ErrorLines != 0 {
		t.Fatalf("terminal file lines = output:%d error:%d, want %d/0", results.OutputLines, results.ErrorLines, requestCount)
	}
	assertUniqueRecoveryRecords(t, results.OutputBody, customIDs)
	if n := countRows(""); n != 0 {
		t.Errorf("%s still holds %d rows after the batch completed", gateReqQueue, n)
	}
}
