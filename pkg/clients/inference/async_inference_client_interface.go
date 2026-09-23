package inference

import "context"

// AsyncInferenceClient defines the interface for non-blocking async dispatch.
type AsyncInferenceClient interface {
	Submit(ctx context.Context, req *GenerateRequest) *ClientError
	// SubmitBatch enqueues a batch of requests, returning one error slot per
	// request in the order given; a nil slot means that request was enqueued.
	SubmitBatch(ctx context.Context, reqs []*GenerateRequest) []*ClientError
	GetResult(ctx context.Context) (*GenerateResponse, error)
	// Cancel marks all still-pending submitted requests as cancelled in the
	// dispatcher (best-effort pre-dispatch). It does not unregister waiters;
	// callers should still Close after local drain.
	Cancel(ctx context.Context, ids []string) error
	Close() error
}
