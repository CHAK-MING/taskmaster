# HTTP transport design

## Shard-local pool

Each HTTP executor shard owns its idle clients. A client is keyed by the exact
authorized Origin and is never shared across shards or used concurrently.
Idle clients are bounded per Origin and per shard and are expired lazily before
acquisition or release. Quiesce closes all idle clients before waiting for
active requests.

The pool is an executor implementation detail. `HttpClient` only reports
whether the most recent response left the connection reusable.

## Stage timeouts

DNS, TCP connect, TLS handshake, request write, first response byte/header, and
subsequent response reads use separate server-owned timeouts. The Workflow Task
timeout remains the total upper bound and can cancel any stage.

## Error classification

The HTTP transport exposes stage-specific error codes. Timeout variants are
equivalent to `std::errc::timed_out`; external cancellation remains equivalent
to operation cancellation. Workflow Runtime classifies generic error
conditions and does not import HTTP types.

HTTP syntax/parser failures remain permanent protocol errors. DNS, connect,
TLS, write, and read transport failures remain retryable under the existing
Workflow retry policy.
