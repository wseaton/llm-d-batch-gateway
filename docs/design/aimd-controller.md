# AIMD concurrency controller

The processor bounds how many inference requests it keeps in flight per
endpoint with an adaptive limit: additive increase on sustained success,
multiplicative decrease on backpressure. This document is the contract for
that controller (`internal/util/semaphore/aimd.go`), the signal
classification that feeds it (`internal/processor/pipeline/dispatcher_aimd.go`),
and the metrics that expose it. `TestAIMDTrajectory` and
`TestAIMDSemaphoreNeverDiverges` in `internal/util/semaphore/aimd_test.go`
pin the rules below; a change to the rules is a change to this document
first.

## Where it sits

```
                       per endpoint (process-wide, shared by every job)
                       ┌──────────────────────────────────────────────┐
 request result ──────►│ recordAIMDSignal ──► AIMDController ──► setFn │──► AdaptiveSemaphore.SetLimit
   (status, retry      │   classify signal     limit, window          │      caps in-flight requests
    history)           └──────────────────────────────────────────────┘
                                    │
                                    └──► metrics: aimd_concurrency_limit (gauge),
                                         aimd_increases_total, aimd_decreases_total{signal}
```

One controller and one semaphore exist per configured endpoint for the
lifetime of the processor (`worker.go`, `endpointLimits`). Jobs share them;
a job never gets a fresh limit.

## Configuration

| Field | Config key | Default | Constraint |
|---|---|---|---|
| `MinLimit` | `concurrency.aimd.min` | 5 | > 0, ≤ `concurrency.per_endpoint` |
| `MaxLimit` | `concurrency.per_endpoint` | 10 (20 in dev-deploy GIE mode) | > 0 |
| `BackoffFactor` | `concurrency.aimd.backoff_factor` | 0.5 | in (0, 1) |
| `AdditiveIncrease` | `concurrency.aimd.additive_increase` | 1 | > 0 |
| initial limit | `concurrency.per_endpoint` | | clamped into [Min, Max] |

`concurrency.aimd.enabled: false` removes the controller; the endpoint
semaphore stays fixed at `per_endpoint`.

## State

- `limit`: the current concurrency limit, always in `[MinLimit, MaxLimit]`.
- `successCount`: consecutive successes since the last limit change or
  backpressure signal.

## Rules

**R1. Success.** `RecordSuccess` increments `successCount`. When
`successCount >= limit`, the limit becomes `min(limit + AdditiveIncrease,
MaxLimit)` and `successCount` resets to 0. The window length is the limit
at the moment the window completes, so recovery from the floor costs
`MinLimit` successes for the first step, `MinLimit + AdditiveIncrease` for
the next, and so on.

**R2. Backpressure.** `RecordRateLimit(reason)` sets the limit to
`max(MinLimit, floor(limit × BackoffFactor))` and resets `successCount` to
0. The reset applies even when the limit is already at the floor: a signal
at the floor still discards partial window progress.

**R3. Callback.** `setFn` is called with the new limit only when the limit
changed, and it is called under the controller mutex. Consequently the
sequence of `setFn` arguments is exactly the sequence of limit values the
controller went through, in order, and the semaphore limit equals
`Limit()` at every instant it can be observed. `setFn` must not call back
into the controller (the only caller passes `AdaptiveSemaphore.SetLimit`,
which takes its own mutex and returns).

**R4. Bounds.** After construction and after every signal,
`MinLimit ≤ Limit() ≤ MaxLimit`. `NewAIMDController` clamps the initial
value and calls `setFn` once if clamping changed it.

Worked defaults (`Min 5, Max 20, factor 0.5, increase 1`):
`20 → 10 → 5` on two signals; back to 20 from 5 takes
`5 + 6 + … + 19 = 180` consecutive successes.

## Signals

`recordAIMDSignal` classifies each completed request (after the HTTP
client's own retries) and calls exactly one of the two methods:

| Outcome | Signal | Effect |
|---|---|---|
| HTTP 429 | `429` | R2 |
| HTTP 5xx | `5xx` | R2 |
| 2xx/3xx/4xx after at least one capacity retry (429/5xx on an earlier attempt) | `capacity_retry` | R2 |
| any other HTTP response, including non-429 4xx | success | R1 |
| no HTTP response (transport error, cancellation) | none | no change |

Every R2 call increments `aimd_decreases_total{endpoint, signal}` even when
the limit is already at the floor, so the counter measures backpressure
events, not limit changes. `aimd_increases_total` increments only when R1
actually raised the limit. `aimd_concurrency_limit` is set to `Limit()`
after every classified result and once at startup.

### Note on 5xx

In an llm-d deployment the 5xx a healthy pool sends under load is the EPP's
503 "request timed out in queue" (TTL eviction), which is backpressure and
belongs in R2. A 500 from a broken model server is not backpressure, but it
is treated the same way today. Splitting 503 from other 5xx is an open
decision; whichever way it goes, the table above is the place to record it.

## Non-goals

- Fairness between endpoints: each endpoint's controller is independent.
- Time-based recovery: the limit only moves on request outcomes. An idle
  endpoint keeps whatever limit it last had.
- Global concurrency: `concurrency.global` is a separate fixed semaphore.

## Observability contract for tests

The e2e suite (`test/e2e/aimd_test.go`) provokes real backpressure through
the EPP and logs these gauges rather than asserting on them until the
signal-to-limit path is pinned end to end; the unit tests above pin the
controller. When an assertion is added there it should be phrased in terms
of R1–R4, with the signal counts read from `aimd_decreases_total`.
