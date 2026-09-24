package inference

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/go-logr/logr"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"

	asyncapi "github.com/llm-d/llm-d-async/api"

	"github.com/llm-d/llm-d-batch-gateway/internal/util/logging"
	httpclient "github.com/llm-d/llm-d-batch-gateway/pkg/clients/http"

	"github.com/llm-d/llm-d-async/producer"
)

// asyncSharedClient decouples submit from collect.
// GetResult reads directly from the producer — the external ResultBroadcaster
// handles routing to the correct job.
type asyncSharedClient struct {
	producer    producer.Producer
	pollTimeout time.Duration
	logger      logr.Logger
}

var _ AsyncInferenceClient = (*asyncSharedClient)(nil)

func newAsyncSharedClient(p producer.Producer, pollTimeout time.Duration, logger logr.Logger) *asyncSharedClient {
	return &asyncSharedClient{
		producer:    p,
		pollTimeout: pollTimeout,
		logger:      logger,
	}
}

// batchSubmitter is implemented by producers that can enqueue a whole batch in
// one statement. The redis producers do not, so SubmitBatch falls back to
// submitting one at a time.
type batchSubmitter interface {
	SubmitRequests(ctx context.Context, reqs []asyncapi.Request) error
}

// resultReadBatch is the most results one read returns from a producer that
// can read several per round trip.
const resultReadBatch = 256

// batchReader reads up to limit results in one round trip. The sql producer
// does; the redis producers read one at a time.
type batchReader interface {
	GetResults(ctx context.Context, limit int) ([]*asyncapi.ResultMessage, error)
}

func (c *asyncSharedClient) requestMessage(ctx context.Context, req *GenerateRequest) (*asyncapi.RequestMessage, *ClientError) {
	payload, err := json.Marshal(req.Params)
	if err != nil {
		return nil, &ClientError{
			Category: httpclient.ErrCategoryInvalidReq,
			Message:  fmt.Sprintf("marshal async request params: %v", err),
			RawError: err,
		}
	}
	model, _ := req.Params["model"].(string)

	now := time.Now()
	deadline := now.Add(defaultDeadline)
	if dl, ok := ctx.Deadline(); ok {
		deadline = dl
	}

	metadata := make(map[string]string)
	otel.GetTextMapPropagator().Inject(ctx, propagation.MapCarrier(metadata))

	return &asyncapi.RequestMessage{
		ID:       req.RequestID,
		Created:  now.Unix(),
		Deadline: deadline.Unix(),
		Payload:  payload,
		Model:    model,
		Headers:  req.Headers,
		Endpoint: req.Endpoint,
		Metadata: metadata,
	}, nil
}

func submitError(err error) *ClientError {
	return &ClientError{
		Category: httpclient.ErrCategoryServer,
		Message:  fmt.Sprintf("submit async request: %v", err),
		RawError: err,
	}
}

func (c *asyncSharedClient) Submit(ctx context.Context, req *GenerateRequest) *ClientError {
	msg, cerr := c.requestMessage(ctx, req)
	if cerr != nil {
		return cerr
	}
	if err := c.producer.SubmitRequest(ctx, msg); err != nil {
		return submitError(err)
	}

	c.logger.V(logging.TRACE).Info("Submitted async request", "requestID", req.RequestID)
	return nil
}

// SubmitBatch enqueues every request in one statement where the producer
// supports it. The batch is all-or-nothing, so a failure retries the requests
// individually to keep a single bad request from failing the rest.
func (c *asyncSharedClient) SubmitBatch(ctx context.Context, reqs []*GenerateRequest) []*ClientError {
	errs := make([]*ClientError, len(reqs))
	if len(reqs) == 0 {
		return errs
	}

	bulk, ok := c.producer.(batchSubmitter)
	if !ok {
		for i, req := range reqs {
			errs[i] = c.Submit(ctx, req)
		}
		return errs
	}

	msgs := make([]asyncapi.Request, 0, len(reqs))
	batched := make([]int, 0, len(reqs))
	for i, req := range reqs {
		msg, cerr := c.requestMessage(ctx, req)
		if cerr != nil {
			errs[i] = cerr
			continue
		}
		msgs = append(msgs, msg)
		batched = append(batched, i)
	}
	if len(msgs) == 0 {
		return errs
	}

	if err := bulk.SubmitRequests(ctx, msgs); err != nil {
		c.logger.V(logging.INFO).Info("Batch submit failed, retrying individually",
			"count", len(msgs), "err", err.Error())
		for _, i := range batched {
			errs[i] = c.Submit(ctx, reqs[i])
		}
		return errs
	}

	c.logger.V(logging.TRACE).Info("Submitted async request batch", "count", len(reqs))
	return errs
}

func (c *asyncSharedClient) GetResults(ctx context.Context) ([]*GenerateResponse, error) {
	for {
		pollCtx, pollCancel := context.WithTimeout(ctx, c.pollTimeout)
		results, err := c.read(pollCtx)
		empty := err != nil && ctx.Err() == nil && errors.Is(pollCtx.Err(), context.DeadlineExceeded)
		pollCancel()
		if empty {
			continue
		}
		if err != nil {
			return nil, err
		}
		out := make([]*GenerateResponse, len(results))
		for i, r := range results {
			out[i] = &GenerateResponse{
				RequestID:    r.ID,
				Response:     []byte(r.Payload),
				StatusCode:   r.StatusCode,
				ErrorCode:    r.ErrorCode,
				ErrorMessage: r.ErrorMessage,
			}
		}
		return out, nil
	}
}

func (c *asyncSharedClient) read(ctx context.Context) ([]*asyncapi.ResultMessage, error) {
	if br, ok := c.producer.(batchReader); ok {
		return br.GetResults(ctx, resultReadBatch)
	}
	r, err := c.producer.GetResult(ctx)
	if err != nil {
		return nil, err
	}
	return []*asyncapi.ResultMessage{r}, nil
}

func (c *asyncSharedClient) Cancel(ctx context.Context, ids []string) error {
	if len(ids) == 0 {
		return nil
	}
	if err := c.producer.CancelRequests(ctx, ids); err != nil {
		return fmt.Errorf("cancel async requests: %w", err)
	}
	c.logger.V(logging.INFO).Info("Cancelled pending async requests", "count", len(ids))
	return nil
}

func (c *asyncSharedClient) Close() error {
	return nil
}
