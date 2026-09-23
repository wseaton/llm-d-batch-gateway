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
	"os"
	"testing"
	"time"

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
