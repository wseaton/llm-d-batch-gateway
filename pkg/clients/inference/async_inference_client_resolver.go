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
	"errors"
	"fmt"
	"io"
	"time"

	"context"

	"github.com/go-logr/logr"
	"github.com/llm-d/llm-d-async/producer"
	producersql "github.com/llm-d/llm-d-async/producer-sql"
	"github.com/llm-d/llm-d-async/producer-sql/sqlqueue"
	"github.com/redis/go-redis/v9"

	"github.com/llm-d/llm-d-batch-gateway/internal/shared/syncutil"
)

const asyncQueuePrefix = "llm-d-async:"

// AsyncTransport selects the llm-d-async transport the producers speak.
type AsyncTransport string

const (
	// AsyncTransportRedisSortedSet is the Redis sorted-set transport (default).
	AsyncTransportRedisSortedSet AsyncTransport = "redis-sortedset"
	// AsyncTransportSQL is the SQL transport backed by Postgres.
	AsyncTransportSQL AsyncTransport = "sql"
)

// AsyncModelPoolConfig holds the resolved pool and queue settings for one async model.
type AsyncModelPoolConfig struct {
	PoolName         string
	RequestQueueName string
	ResultQueueName  string
}

// AsyncClientConfig holds the resolved configuration for async dispatch.
type AsyncClientConfig struct {
	Transport         AsyncTransport // empty means redis-sortedset
	RedisURL          string         // redis-sortedset transport
	SQLURL            string         // sql transport DSN (postgres:// or sqlite://)
	ConsumerID        string         // identity of this Batch Processor replica
	Models            map[string]AsyncModelPoolConfig
	ResultPollTimeout time.Duration // per-poll timeout in the result dispatcher loop
}

// AsyncGatewayResolver routes models to shared AsyncInferenceClient instances.
// Immutable after construction — safe for concurrent reads.
type AsyncGatewayResolver struct {
	pools           map[string]*asyncPool                          // model → pool
	sharedClients   *syncutil.MutexMap[string, *asyncSharedClient] // model → shared client
	closers         []io.Closer
	clientFactories map[string]func() AsyncInferenceClient // test-only override
	logger          logr.Logger
}

// Models returns all configured model IDs.
func (r *AsyncGatewayResolver) Models() []string {
	if r.clientFactories != nil {
		models := make([]string, 0, len(r.clientFactories))
		for m := range r.clientFactories {
			models = append(models, m)
		}
		return models
	}
	models := make([]string, 0, len(r.pools))
	for m := range r.pools {
		models = append(models, m)
	}
	return models
}

// SharedClientFor returns a shared client for the given model.
// Reuses the same client across calls. Results for this Processor replica are
// isolated on its consumer-specific result queue.
func (r *AsyncGatewayResolver) SharedClientFor(modelID string) AsyncInferenceClient {
	if r.clientFactories != nil {
		if factory, ok := r.clientFactories[modelID]; ok {
			return factory()
		}
		return nil
	}
	if c, ok := r.sharedClients.Load(modelID); ok {
		return c
	}
	pool, ok := r.pools[modelID]
	if !ok {
		return nil
	}
	c := newAsyncSharedClient(pool.producer, pool.pollTimeout, r.logger.WithValues("model", modelID))
	actual, _ := r.sharedClients.LoadOrStore(modelID, c)
	return actual
}

// NewTestAsyncResolver creates a resolver backed by factory functions instead of
// real Redis connections. Each call to SharedClientFor invokes the corresponding factory.
func NewTestAsyncResolver(factories map[string]func() AsyncInferenceClient) *AsyncGatewayResolver {
	return &AsyncGatewayResolver{clientFactories: factories}
}

// Close releases resources held by the resolver (producers, Redis).
func (r *AsyncGatewayResolver) Close() error {
	var errs []error
	for _, c := range r.closers {
		if err := c.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// modelQueues returns a model's request and result queue names.
func modelQueues(mcfg AsyncModelPoolConfig) (string, string) {
	reqQueue := mcfg.RequestQueueName
	resQueue := mcfg.ResultQueueName
	// Deprecated: derived queue names from pool name. Set explicit
	// request_queue_name / result_queue_name in async model config.
	if reqQueue == "" {
		reqQueue = asyncQueuePrefix + "requests:" + mcfg.PoolName
	}
	if resQueue == "" {
		resQueue = asyncQueuePrefix + "results:" + mcfg.PoolName
	}
	return reqQueue, resQueue
}

// NewAsyncResolver creates an AsyncGatewayResolver with one shared pool
// (producer) per model/pool pair.
func NewAsyncResolver(config AsyncClientConfig, logger logr.Logger) (*AsyncGatewayResolver, error) {
	if config.ConsumerID == "" {
		return nil, fmt.Errorf("consumerID must not be empty")
	}

	poolToModel := make(map[string]string, len(config.Models))
	resultQueueToModel := make(map[string]string, len(config.Models))
	for model, mcfg := range config.Models {
		if existing, ok := poolToModel[mcfg.PoolName]; ok {
			return nil, fmt.Errorf("models %q and %q both map to pool %q: each pool must have a single consumer", existing, model, mcfg.PoolName)
		}
		poolToModel[mcfg.PoolName] = model
		_, resQueue := modelQueues(mcfg)
		if existing, ok := resultQueueToModel[resQueue]; ok {
			return nil, fmt.Errorf("models %q and %q both read result queue %q: each result queue must have a single consumer, or one model's reader discards the other's results", existing, model, resQueue)
		}
		resultQueueToModel[resQueue] = model
	}

	if config.ResultPollTimeout <= 0 {
		return nil, fmt.Errorf("resultPollTimeout must be > 0")
	}

	newProducer, conn, err := newAsyncProducerFactory(config)
	if err != nil {
		return nil, err
	}

	pools := make(map[string]*asyncPool, len(config.Models))
	var closers []io.Closer

	for model, mcfg := range config.Models {
		reqQueue, resQueue := modelQueues(mcfg)
		// Each Processor consumes from its own result queue so another healthy
		// replica cannot consume and discard its results.
		resQueue = resQueue + ":" + config.ConsumerID
		logger.Info(
			"Configured async inference queues",
			"model", model,
			"requestQueue", reqQueue,
			"resultQueue", resQueue,
		)

		p, err := newProducer(reqQueue, resQueue)
		if err != nil {
			for _, c := range closers {
				_ = c.Close()
			}
			_ = conn.Close()
			return nil, fmt.Errorf("failed to create producer for model %q (pool %s): %w", model, mcfg.PoolName, err)
		}

		pools[model] = &asyncPool{
			producer:    p,
			pollTimeout: config.ResultPollTimeout,
		}
		closers = append(closers, p)
	}

	closers = append(closers, conn)

	return &AsyncGatewayResolver{
		pools:         pools,
		sharedClients: syncutil.NewMutexMap[string, *asyncSharedClient](),
		closers:       closers,
		logger:        logger,
	}, nil
}

// newAsyncProducerFactory opens the transport connection shared by every
// per-model producer and returns a constructor for them. The connection is
// closed by the caller after the producers.
func newAsyncProducerFactory(config AsyncClientConfig) (func(reqQueue, resQueue string) (producer.Producer, error), io.Closer, error) {
	switch config.Transport {
	case "", AsyncTransportRedisSortedSet:
		opts, err := redis.ParseURL(config.RedisURL)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to parse async inference Redis URL: %w", err)
		}
		rdb := redis.NewClient(opts)
		return func(reqQueue, resQueue string) (producer.Producer, error) {
			return producer.NewRedisSortedSetProducer(
				producer.RedisSortedSetConfig{RequestQueueName: reqQueue, ResultQueueName: resQueue},
				producer.WithRedisClient(rdb),
			)
		}, rdb, nil
	case AsyncTransportSQL:
		if config.SQLURL == "" {
			return nil, nil, fmt.Errorf("async inference sql transport requires a SQL URL")
		}
		store, err := sqlqueue.Open(context.Background(), config.SQLURL)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to open async inference SQL store: %w", err)
		}
		return func(reqQueue, resQueue string) (producer.Producer, error) {
			return producersql.New(context.Background(),
				producersql.Config{RequestQueueName: reqQueue, ResultQueueName: resQueue},
				producersql.WithStore(store),
			)
		}, store, nil
	default:
		return nil, nil, fmt.Errorf("unsupported async inference transport %q (supported: %s, %s)", config.Transport, AsyncTransportRedisSortedSet, AsyncTransportSQL)
	}
}
