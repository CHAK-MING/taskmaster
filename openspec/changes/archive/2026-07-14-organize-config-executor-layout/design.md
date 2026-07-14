# Design

## Placement model

The repository uses five distinct ownership categories:

1. `config/` owns external deployment configuration DTOs.
2. `workflow/` owns the Task executor seam and scheduling semantics.
3. `executors/<kind>/` owns Workflow-to-capability adaptation for one executor.
4. `http/` and `sandbox/` own reusable transport and isolated-process
   capabilities.
5. `app/` is the composition root and is the only layer that decides which
   concrete executors are enabled.

## Public and private headers

- `include/dagforge/**` contains stable interfaces that external callers may
  include.
- A declaration used by only one `.cpp` belongs in that `.cpp`.
- A declaration shared by multiple implementation files may live in a private
  header under `src/dagforge/<subsystem>/detail/`.
- Private headers are not installed and do not define public configuration
  contracts.

## Configuration model

`config::SystemConfig` aggregates:

- Runtime, Workflow, Admission, Storage, and API configuration.
- `config::ExecutorsConfig`, containing command and HTTP executor deployment
  configuration.

Command configuration separates program/environment authorization from
Minijail resource and filesystem settings. HTTP configuration separates the
Application-level enabled switch from the egress policy settings consumed by
the executor implementation.

The loader retains legacy `[sandbox]` and `[http_executor]` TOML sections and
converts them into the new internal model.

## Dependency direction

```text
config      -> base/util only
workflow    -> core/config admission DTO only
http        -> core/io
sandbox     -> core/io + config DTOs in implementation only
executors   -> workflow + config + http/sandbox
app         -> config + workflow + concrete executors
```

Workflow must not include concrete executor or sandbox headers. Sandbox must
not include Workflow headers.
