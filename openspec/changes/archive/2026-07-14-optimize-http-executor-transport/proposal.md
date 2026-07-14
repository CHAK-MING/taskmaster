# Optimize HTTP executor transport

The HTTP executor currently creates a new connection for every Attempt and
uses one connect timeout plus one read timeout for all transport stages. This
makes ordinary HTTPS requests pay repeated DNS/TCP/TLS setup cost and makes
transport failures difficult to diagnose.

This change adds shard-owned, bounded keep-alive pooling, independent transport
stage timeouts, and stage-specific HTTP client errors. Workflow scheduling and
retry policy remain executor-neutral.

Redirects, streaming Artifact bodies, and `Retry-After` scheduling are outside
this change.
