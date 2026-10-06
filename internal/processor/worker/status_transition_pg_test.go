package worker

import (
	"os"
	"testing"

	"github.com/llm-d/llm-d-batch-gateway/internal/database/postgresql"
)

// TestProcessorTransitionsPostgres runs the processor transition matrix
// against PostgreSQL, where the lifecycle condition is a JSONB predicate and
// the stored request_counts differ from the owner's copy.
func TestProcessorTransitionsPostgres(t *testing.T) {
	url := os.Getenv("TEST_POSTGRES_URL")
	if url == "" {
		t.Skip("TEST_POSTGRES_URL not set")
	}
	batchDB, err := postgresql.NewPostgresBatchDBClient(testLoggerCtx(t), &postgresql.PostgreSQLConfig{Url: url})
	if err != nil {
		t.Fatalf("NewPostgresBatchDBClient: %v", err)
	}
	t.Cleanup(func() { _ = batchDB.Close() })

	testProcessorTransitions(t, batchDB, "pg-transition")
}
