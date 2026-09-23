package pipeline

import (
	"context"
	"errors"
	"time"

	"github.com/go-logr/logr"

	"github.com/llm-d/llm-d-batch-gateway/internal/processor/batchctx"
	batch_types "github.com/llm-d/llm-d-batch-gateway/internal/shared/types"
	"github.com/llm-d/llm-d-batch-gateway/pkg/clients/inference"
)

// AsyncDispatcher submits requests to async queues (fire-and-forget).
// Results arrive via per-model ResultBroadcasters (backed by shared
// clients) that send directly to resultCh.
type AsyncDispatcher struct {
	resolver     *inference.AsyncGatewayResolver
	broadcasters *BroadcasterGroup
	pending      *PendingRequests
	batchSize    int
	linger       time.Duration
	logger       logr.Logger
}

var _ RequestDispatcher = (*AsyncDispatcher)(nil)

// batchSize bounds one enqueue statement; linger is how long a partial batch
// waits for more work. The upstream channel is unbuffered, so without a linger
// the batch closes in the gap between two sends and every request gets its own
// INSERT. Non-positive values fall back to the defaults.
func NewAsyncDispatcher(
	resolver *inference.AsyncGatewayResolver,
	broadcasters *BroadcasterGroup,
	pending *PendingRequests,
	batchSize int,
	linger time.Duration,
	logger logr.Logger,
) *AsyncDispatcher {
	if batchSize < 1 {
		batchSize = defaultSubmitBatchSize
	}
	if linger < 0 {
		linger = defaultSubmitLinger
	}
	return &AsyncDispatcher{
		resolver:     resolver,
		broadcasters: broadcasters,
		pending:      pending,
		batchSize:    batchSize,
		linger:       linger,
		logger:       logger,
	}
}

const (
	defaultSubmitBatchSize = 256
	defaultSubmitLinger    = 25 * time.Millisecond
)

func (d *AsyncDispatcher) Run(ctx context.Context, requestCh <-chan RequestItem, resultCh chan<- ResultItem) error {
	d.broadcasters.Subscribe(resultCh)

	// Submit phase — fast queue writes, batched per model.
	open := true
	for open {
		msg, ok := <-requestCh
		if !ok {
			break
		}

		batch := []RequestItem{msg}
		linger := time.NewTimer(d.linger)
	drain:
		for len(batch) < d.batchSize {
			select {
			case next, more := <-requestCh:
				if !more {
					open = false
					break drain
				}
				batch = append(batch, next)
			case <-linger.C:
				break drain
			}
		}
		linger.Stop()

		d.submitBatch(ctx, batch, resultCh)
	}

	// Wait for all pending results to be resolved by the collector.
	d.pending.Wait(ctx)

	// Best-effort: tell the queue to drop still-pending requests before
	// dispatch. Use a detached context — ctx is often already cancelled.
	if ids := d.pending.IDs(); len(ids) > 0 {
		cancelCtx, cancelFn := context.WithTimeout(context.Background(), 5*time.Second)
		d.cancelPending(cancelCtx, ids)
		cancelFn()
	}

	// Drain submitted-but-uncollected requests as errors so that
	// output_lines + error_lines == total_requests.
	code, message := cancelCode(ctx)
	d.pending.DrainUnresolved(func(msg RequestItem) {
		resultCh <- *msg.Error(code, message)
	})

	// Unsubscribe removes resultCh from the broadcast list. A concurrent
	// send from the broadcaster may still race with close(resultCh);
	// safeChannelSend recovers from the resulting panic.
	d.broadcasters.Unsubscribe(resultCh)
	close(resultCh)

	return nil
}

// submitBatch groups a run of requests by model, since each model has its own
// queue and client, and enqueues each group in one call.
func (d *AsyncDispatcher) submitBatch(ctx context.Context, batch []RequestItem, resultCh chan<- ResultItem) {
	byModel := make(map[string][]RequestItem, 1)
	order := make([]string, 0, 1)

	for _, msg := range batch {
		if d.resolver.SharedClientFor(msg.ModelID) == nil {
			resultCh <- *msg.ModelNotFound()
			continue
		}
		if _, seen := byModel[msg.ModelID]; !seen {
			order = append(order, msg.ModelID)
		}
		byModel[msg.ModelID] = append(byModel[msg.ModelID], msg)
	}

	for _, modelID := range order {
		client := d.resolver.SharedClientFor(modelID)
		if client == nil {
			continue
		}
		msgs := byModel[modelID]

		reqs := make([]*inference.GenerateRequest, len(msgs))
		for i, msg := range msgs {
			d.pending.Store(msg)
			reqs[i] = &inference.GenerateRequest{
				RequestID: msg.RequestID,
				Endpoint:  msg.Endpoint,
				Params:    msg.Body,
				Headers:   msg.Headers,
			}
		}

		for i, submitErr := range client.SubmitBatch(ctx, reqs) {
			if submitErr != nil {
				resultCh <- *msgs[i].Error(
					string(submitErr.Category),
					submitErr.Message,
				)
			}
		}
	}
}

func cancelCode(ctx context.Context) (string, string) {
	cause := context.Cause(ctx)
	if errors.Is(cause, context.DeadlineExceeded) || errors.Is(cause, batchctx.ErrExpired) {
		return string(batch_types.ErrCodeBatchExpired), batch_types.ErrCodeBatchExpired.Message()
	}
	if errors.Is(cause, batchctx.ErrCancelled) {
		return string(batch_types.ErrCodeBatchCancelled), batch_types.ErrCodeBatchCancelled.Message()
	}
	return string(batch_types.ErrCodeBatchFailed), batch_types.ErrCodeBatchFailed.Message()
}

// cancelPending calls Cancel on every shared client with all pending IDs.
// Different models may map to different producers, but CancelRequests
// silently ignores unknown IDs, so broadcasting to all is safe.
func (d *AsyncDispatcher) cancelPending(ctx context.Context, ids []string) {
	for _, modelID := range d.resolver.Models() {
		client := d.resolver.SharedClientFor(modelID)
		if client == nil {
			continue
		}
		if err := client.Cancel(ctx, ids); err != nil {
			d.logger.Error(err, "Failed to cancel pending async requests", "model", modelID)
		}
	}
}
