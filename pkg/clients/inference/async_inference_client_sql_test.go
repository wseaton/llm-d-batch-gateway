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

package inference

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	producersql "github.com/llm-d/llm-d-async/producer-sql"
)

func TestNewAsyncResolver_SQLTransport(t *testing.T) {
	dsn := os.Getenv("TEST_POSTGRES_URL")
	if dsn == "" {
		t.Skip("TEST_POSTGRES_URL not set")
	}

	cfg := AsyncClientConfig{
		Transport:  AsyncTransportSQL,
		SQLURL:     dsn,
		ConsumerID: testAsyncConsumerID,
		Models: map[string]AsyncModelPoolConfig{
			"model-a": {PoolName: "pool-a", RequestQueueName: "req-a", ResultQueueName: "res-a"},
		},
		ResultPollTimeout: time.Second,
	}
	r, err := NewAsyncResolver(cfg, testLogger(t))
	if err != nil {
		t.Fatalf("NewAsyncResolver: %v", err)
	}
	t.Cleanup(func() { _ = r.Close() })

	client := r.SharedClientFor("model-a")
	if client == nil {
		t.Fatal("expected a client for model-a")
	}
	if got := r.SharedClientFor("unknown"); got != nil {
		t.Fatalf("expected nil for unknown model, got %v", got)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req := &GenerateRequest{
		RequestID: "sql-req-1",
		Params:    map[string]any{"model": "model-a", "prompt": "hi"},
	}
	if cerr := client.Submit(ctx, req); cerr != nil {
		t.Fatalf("Submit: %v", cerr)
	}

	// The request must be visible to a plain producer on the same database
	// under the configured request queue, and results must arrive on the
	// replica-scoped result route.
	p, err := producersql.New(ctx, producersql.Config{
		URL: dsn, RequestQueueName: "req-a", ResultQueueName: "res-a:" + testAsyncConsumerID,
	})
	if err != nil {
		t.Fatalf("producersql.New: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })
	if err := p.CancelRequests(ctx, []string{"sql-req-1"}); err != nil {
		t.Fatalf("CancelRequests: %v", err)
	}
}

func TestNewAsyncResolver_SQLTransportRequiresURL(t *testing.T) {
	_, err := NewAsyncResolver(AsyncClientConfig{
		Transport:         AsyncTransportSQL,
		ConsumerID:        testAsyncConsumerID,
		Models:            map[string]AsyncModelPoolConfig{"m": {PoolName: "p"}},
		ResultPollTimeout: time.Second,
	}, testLogger(t))
	if err == nil {
		t.Fatal("expected an error without a SQL URL")
	}
}

func TestNewAsyncResolver_UnknownTransport(t *testing.T) {
	_, err := NewAsyncResolver(AsyncClientConfig{
		Transport:         "carrier-pigeon",
		ConsumerID:        testAsyncConsumerID,
		Models:            map[string]AsyncModelPoolConfig{"m": {PoolName: "p"}},
		ResultPollTimeout: time.Second,
	}, testLogger(t))
	if err == nil {
		t.Fatal("expected an error for an unknown transport")
	}
}

// GetResult waits through poll windows that end without a result instead of
// reporting them as errors, which the result broadcaster backs off on.
func TestAsyncSharedClient_GetResultWaitsPastEmptyPolls(t *testing.T) {
	dsn := os.Getenv("TEST_POSTGRES_URL")
	if dsn == "" {
		t.Skip("TEST_POSTGRES_URL not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	const route = "empty-poll-results"
	p, err := producersql.New(ctx, producersql.Config{URL: dsn, RequestQueueName: "empty-poll-requests", ResultQueueName: route})
	if err != nil {
		t.Fatalf("producersql.New: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.ExecContext(ctx, `DELETE FROM async_results WHERE route = $1`, route); err != nil {
		t.Fatalf("clear results: %v", err)
	}

	client := newAsyncSharedClient(p, 200*time.Millisecond, testLogger(t))
	type got struct {
		resp *GenerateResponse
		err  error
	}
	done := make(chan got, 1)
	start := time.Now()
	go func() {
		resps, err := client.GetResults(ctx)
		var resp *GenerateResponse
		if len(resps) > 0 {
			resp = resps[0]
		}
		done <- got{resp, err}
	}()

	time.Sleep(700 * time.Millisecond)
	if _, err := db.ExecContext(ctx,
		`INSERT INTO async_results (route, id, request_token, payload, expires_at, created_at) VALUES ($1, 'late-1', 'token-1', '{"id":"late-1","payload":"ok","status_code":200}', 0, 0)`,
		route); err != nil {
		t.Fatalf("insert result: %v", err)
	}

	r := <-done
	if r.err != nil {
		t.Fatalf("GetResult after empty polls: %v", r.err)
	}
	if r.resp.RequestID != "late-1" || string(r.resp.Response) != "ok" {
		t.Fatalf("GetResult = %+v, want late-1 with payload ok", r.resp)
	}
	if elapsed := time.Since(start); elapsed < 700*time.Millisecond {
		t.Fatalf("GetResult returned after %v, before the result existed", elapsed)
	}
}

// The sql producer reads results in batches, so a backlog drains in a few
// round trips instead of one per result.
func TestAsyncSharedClient_GetResultsReadsBatches(t *testing.T) {
	dsn := os.Getenv("TEST_POSTGRES_URL")
	if dsn == "" {
		t.Skip("TEST_POSTGRES_URL not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	const route = "batch-read-results"
	p, err := producersql.New(ctx, producersql.Config{URL: dsn, RequestQueueName: "batch-read-requests", ResultQueueName: route})
	if err != nil {
		t.Fatalf("producersql.New: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.ExecContext(ctx, `DELETE FROM async_results WHERE route = $1`, route); err != nil {
		t.Fatalf("clear results: %v", err)
	}
	const backlog = resultReadBatch + 44
	if _, err := db.ExecContext(ctx, `
INSERT INTO async_results (route, id, request_token, payload, expires_at, created_at)
SELECT $1, 'r-' || g, 't-' || g, '{"id":"r-' || g || '","payload":"ok","status_code":200}', 0, 0 FROM generate_series(1, $2) g`,
		route, backlog); err != nil {
		t.Fatalf("insert results: %v", err)
	}

	client := newAsyncSharedClient(p, time.Second, testLogger(t))
	seen := map[string]bool{}
	var sizes []int
	for len(seen) < backlog {
		resps, err := client.GetResults(ctx)
		if err != nil {
			t.Fatalf("GetResults: %v", err)
		}
		sizes = append(sizes, len(resps))
		for _, r := range resps {
			if seen[r.RequestID] {
				t.Fatalf("result %s returned twice", r.RequestID)
			}
			seen[r.RequestID] = true
		}
	}
	if len(sizes) != 2 || sizes[0] != resultReadBatch || sizes[1] != 44 {
		t.Fatalf("read sizes = %v, want [%d 44]", sizes, resultReadBatch)
	}
}
