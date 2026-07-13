#include "dagforge/workflow/workflow_storage.hpp"

#include "dagforge/util/json.hpp"
#include "dagforge/workflow/workflow_control_plane.hpp"

#include <openssl/evp.h>

#include <array>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <memory>
#include <optional>
#include <string>
#include <utility>

namespace dagforge::workflow::detail {

struct ValueDto {
  std::string type;
  bool boolean{false};
  std::int64_t integer{0};
  double number{0.0};
  std::string text;
  JsonValue json;
  std::string artifact_id;
  std::string media_type;
  std::uint64_t size_bytes{0};
  std::string digest;
};

struct OutputValueDto {
  std::string node_id;
  std::string port;
  ValueDto value;
};

struct AttemptDto {
  std::string attempt_id;
  std::uint32_t number{0};
  std::uint8_t state{0};
  std::optional<std::uint8_t> termination_reason;
  std::optional<std::uint8_t> failure_class;
  std::optional<int> exit_code;
  std::string error;
  std::int64_t created_at_ms{0};
  std::int64_t started_at_ms{0};
  std::int64_t finished_at_ms{0};
};

struct TaskDto {
  std::string node_id;
  std::uint8_t state{0};
  std::uint32_t attempt_count{0};
  std::optional<std::string> active_attempt_id;
  std::optional<std::int64_t> next_attempt_at_ms;
  std::optional<std::uint8_t> skip_reason;
  std::string last_error;
  std::vector<AttemptDto> attempts;
  std::int64_t started_at_ms{0};
  std::int64_t finished_at_ms{0};
};

struct SnapshotDto {
  std::string run_id;
  std::string workflow_id;
  std::string plan_id;
  std::uint8_t state{0};
  std::optional<std::uint8_t> stop_intent;
  std::string stop_reason;
  std::vector<TaskDto> tasks;
  std::int64_t created_at_ms{0};
  std::int64_t started_at_ms{0};
  std::int64_t finished_at_ms{0};
  std::string error;
};

struct TriggerDto {
  std::string trigger_id;
  std::string workflow_id;
  std::string source;
  std::string event_type;
  ValueDto payload;
  std::string idempotency_key;
  std::string principal_subject;
  std::vector<std::string> principal_roles;
  std::string trace_id;
  std::string parent_span_id;
  std::int64_t occurred_at_ms{0};
};

struct CheckpointDto {
  std::uint32_t schema_version{1};
  std::string plan_json;
  TriggerDto trigger;
  SnapshotDto snapshot;
  std::vector<OutputValueDto> values;
  std::int64_t created_at_ms{0};
};

struct EvidenceDto {
  std::string evidence_id;
  std::string run_id;
  std::string node_id;
  std::uint8_t type{0};
  std::int64_t timestamp_ms{0};
  std::string actor_subject;
  std::vector<std::string> actor_roles;
  JsonValue metadata;
  std::optional<ValueDto> artifact;
  std::string content_digest;
};

struct ArtifactMetaDto {
  std::string artifact_id;
  std::string media_type;
  std::uint64_t size_bytes{0};
  std::string digest;
};

} // namespace dagforge::workflow::detail

namespace glz {
template <> struct meta<dagforge::workflow::detail::ValueDto> {
  using T = dagforge::workflow::detail::ValueDto;
  static constexpr auto value = object(
      "type", &T::type, "boolean", &T::boolean, "integer", &T::integer,
      "number", &T::number, "text", &T::text, "json", &T::json,
      "artifact_id", &T::artifact_id, "media_type", &T::media_type,
      "size_bytes", &T::size_bytes, "digest", &T::digest);
};
template <> struct meta<dagforge::workflow::detail::OutputValueDto> {
  using T = dagforge::workflow::detail::OutputValueDto;
  static constexpr auto value = object("node_id", &T::node_id, "port",
                                       &T::port, "value", &T::value);
};
template <> struct meta<dagforge::workflow::detail::AttemptDto> {
  using T = dagforge::workflow::detail::AttemptDto;
  static constexpr auto value = object(
      "attempt_id", &T::attempt_id, "number", &T::number, "state", &T::state,
      "termination_reason", &T::termination_reason, "failure_class",
      &T::failure_class, "exit_code", &T::exit_code, "error", &T::error,
      "created_at_ms", &T::created_at_ms, "started_at_ms", &T::started_at_ms,
      "finished_at_ms", &T::finished_at_ms);
};
template <> struct meta<dagforge::workflow::detail::TaskDto> {
  using T = dagforge::workflow::detail::TaskDto;
  static constexpr auto value = object(
      "node_id", &T::node_id, "state", &T::state, "attempt_count",
      &T::attempt_count, "active_attempt_id", &T::active_attempt_id,
      "next_attempt_at_ms", &T::next_attempt_at_ms, "skip_reason",
      &T::skip_reason, "last_error", &T::last_error, "attempts", &T::attempts,
      "started_at_ms", &T::started_at_ms, "finished_at_ms",
      &T::finished_at_ms);
};
template <> struct meta<dagforge::workflow::detail::SnapshotDto> {
  using T = dagforge::workflow::detail::SnapshotDto;
  static constexpr auto value = object(
      "run_id", &T::run_id, "workflow_id", &T::workflow_id, "plan_id",
      &T::plan_id, "state", &T::state, "stop_intent", &T::stop_intent,
      "stop_reason", &T::stop_reason, "tasks", &T::tasks, "created_at_ms",
      &T::created_at_ms, "started_at_ms", &T::started_at_ms, "finished_at_ms",
      &T::finished_at_ms, "error", &T::error);
};
template <> struct meta<dagforge::workflow::detail::TriggerDto> {
  using T = dagforge::workflow::detail::TriggerDto;
  static constexpr auto value = object(
      "trigger_id", &T::trigger_id, "workflow_id", &T::workflow_id, "source",
      &T::source, "event_type", &T::event_type, "payload", &T::payload,
      "idempotency_key", &T::idempotency_key, "principal_subject",
      &T::principal_subject, "principal_roles", &T::principal_roles,
      "trace_id", &T::trace_id, "parent_span_id", &T::parent_span_id,
      "occurred_at_ms", &T::occurred_at_ms);
};
template <> struct meta<dagforge::workflow::detail::CheckpointDto> {
  using T = dagforge::workflow::detail::CheckpointDto;
  static constexpr auto value = object(
      "schema_version", &T::schema_version, "plan_json", &T::plan_json,
      "trigger", &T::trigger, "snapshot", &T::snapshot, "values", &T::values,
      "created_at_ms", &T::created_at_ms);
};
template <> struct meta<dagforge::workflow::detail::EvidenceDto> {
  using T = dagforge::workflow::detail::EvidenceDto;
  static constexpr auto value = object(
      "evidence_id", &T::evidence_id, "run_id", &T::run_id, "node_id",
      &T::node_id, "type", &T::type, "timestamp_ms", &T::timestamp_ms,
      "actor_subject", &T::actor_subject, "actor_roles", &T::actor_roles,
      "metadata", &T::metadata, "artifact", &T::artifact, "content_digest",
      &T::content_digest);
};
template <> struct meta<dagforge::workflow::detail::ArtifactMetaDto> {
  using T = dagforge::workflow::detail::ArtifactMetaDto;
  static constexpr auto value = object(
      "artifact_id", &T::artifact_id, "media_type", &T::media_type,
      "size_bytes", &T::size_bytes, "digest", &T::digest);
};
} // namespace glz

namespace dagforge::workflow {
namespace {

[[nodiscard]] auto digest_bytes(std::span<const std::byte> data)
    -> Result<std::string> {
  auto context = std::unique_ptr<EVP_MD_CTX, decltype(&EVP_MD_CTX_free)>{
      EVP_MD_CTX_new(), &EVP_MD_CTX_free};
  if (!context || EVP_DigestInit_ex(context.get(), EVP_sha256(), nullptr) != 1 ||
      EVP_DigestUpdate(context.get(), data.data(), data.size()) != 1) {
    return fail(Error::Unknown);
  }

  std::array<unsigned char, EVP_MAX_MD_SIZE> bytes{};
  unsigned int size = 0;
  if (EVP_DigestFinal_ex(context.get(), bytes.data(), &size) != 1) {
    return fail(Error::Unknown);
  }

  static constexpr char kHex[] = "0123456789abcdef";
  std::string out(static_cast<std::size_t>(size) * 2, '\0');
  for (unsigned int i = 0; i < size; ++i) {
    out[static_cast<std::size_t>(i) * 2] = kHex[bytes[i] >> 4U];
    out[static_cast<std::size_t>(i) * 2 + 1] = kHex[bytes[i] & 0x0fU];
  }
  return ok(std::move(out));
}

[[nodiscard]] auto to_milliseconds(
    std::chrono::system_clock::time_point value) -> std::int64_t {
  if (value == std::chrono::system_clock::time_point{}) {
    return 0;
  }
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             value.time_since_epoch())
      .count();
}

[[nodiscard]] auto from_milliseconds(std::int64_t value)
    -> std::chrono::system_clock::time_point {
  return value == 0
             ? std::chrono::system_clock::time_point{}
             : std::chrono::system_clock::time_point{
                   std::chrono::milliseconds(value)};
}

[[nodiscard]] auto value_to_dto(const WorkflowValue &value)
    -> detail::ValueDto {
  return std::visit(
      [](const auto &typed) -> detail::ValueDto {
        using T = std::decay_t<decltype(typed)>;
        detail::ValueDto dto;
        if constexpr (std::same_as<T, std::monostate>) {
          dto.type = "null";
        } else if constexpr (std::same_as<T, bool>) {
          dto.type = "bool";
          dto.boolean = typed;
        } else if constexpr (std::same_as<T, std::int64_t>) {
          dto.type = "int";
          dto.integer = typed;
        } else if constexpr (std::same_as<T, double>) {
          dto.type = "double";
          dto.number = typed;
        } else if constexpr (std::same_as<T, std::string>) {
          dto.type = "string";
          dto.text = typed;
        } else if constexpr (std::same_as<T, JsonValue>) {
          dto.type = "json";
          dto.json = typed;
        } else if constexpr (std::same_as<T, ArtifactRef>) {
          dto.type = "artifact";
          dto.artifact_id = typed.artifact_id.str();
          dto.media_type = typed.media_type;
          dto.size_bytes = typed.size_bytes;
          dto.digest = typed.digest;
        }
        return dto;
      },
      value);
}

[[nodiscard]] auto value_from_dto(detail::ValueDto dto)
    -> Result<WorkflowValue> {
  if (dto.type == "null")
    return ok(WorkflowValue{std::monostate{}});
  if (dto.type == "bool")
    return ok(WorkflowValue{dto.boolean});
  if (dto.type == "int")
    return ok(WorkflowValue{dto.integer});
  if (dto.type == "double")
    return ok(WorkflowValue{dto.number});
  if (dto.type == "string")
    return ok(WorkflowValue{std::move(dto.text)});
  if (dto.type == "json")
    return ok(WorkflowValue{std::move(dto.json)});
  if (dto.type == "artifact" && !dto.artifact_id.empty()) {
    return ok(WorkflowValue{ArtifactRef{
        .artifact_id = ArtifactId{std::move(dto.artifact_id)},
        .media_type = std::move(dto.media_type),
        .size_bytes = dto.size_bytes,
        .digest = std::move(dto.digest),
    }});
  }
  return fail(Error::ParseError);
}

[[nodiscard]] auto attempt_to_dto(const AttemptSnapshot &attempt)
    -> detail::AttemptDto {
  return detail::AttemptDto{
      .attempt_id = attempt.attempt_id.str(),
      .number = attempt.number,
      .state = static_cast<std::uint8_t>(attempt.state),
      .termination_reason = attempt.termination_reason
                                ? std::optional{static_cast<std::uint8_t>(
                                      *attempt.termination_reason)}
                                : std::nullopt,
      .failure_class = attempt.failure_class
                           ? std::optional{static_cast<std::uint8_t>(
                                 *attempt.failure_class)}
                           : std::nullopt,
      .exit_code = attempt.exit_code,
      .error = attempt.error,
      .created_at_ms = to_milliseconds(attempt.created_at),
      .started_at_ms = to_milliseconds(attempt.started_at),
      .finished_at_ms = to_milliseconds(attempt.finished_at),
  };
}

[[nodiscard]] auto attempt_from_dto(detail::AttemptDto dto)
    -> AttemptSnapshot {
  return AttemptSnapshot{
      .attempt_id = AttemptId{std::move(dto.attempt_id)},
      .number = dto.number,
      .state = static_cast<AttemptState>(dto.state),
      .termination_reason = dto.termination_reason
                                ? std::optional{static_cast<TerminationReason>(
                                      *dto.termination_reason)}
                                : std::nullopt,
      .failure_class = dto.failure_class
                           ? std::optional{static_cast<FailureClass>(
                                 *dto.failure_class)}
                           : std::nullopt,
      .exit_code = dto.exit_code,
      .error = std::move(dto.error),
      .created_at = from_milliseconds(dto.created_at_ms),
      .started_at = from_milliseconds(dto.started_at_ms),
      .finished_at = from_milliseconds(dto.finished_at_ms),
  };
}

[[nodiscard]] auto task_to_dto(const TaskSnapshot &task) -> detail::TaskDto {
  detail::TaskDto dto{
      .node_id = task.node_id.str(),
      .state = static_cast<std::uint8_t>(task.state),
      .attempt_count = task.attempt_count,
      .active_attempt_id = task.active_attempt_id
                               ? std::optional{task.active_attempt_id->str()}
                               : std::nullopt,
      .next_attempt_at_ms = task.next_attempt_at
                                ? std::optional{to_milliseconds(
                                      *task.next_attempt_at)}
                                : std::nullopt,
      .skip_reason = task.skip_reason
                         ? std::optional{static_cast<std::uint8_t>(
                               *task.skip_reason)}
                         : std::nullopt,
      .last_error = task.last_error,
      .started_at_ms = to_milliseconds(task.started_at),
      .finished_at_ms = to_milliseconds(task.finished_at),
  };
  dto.attempts.reserve(task.attempts.size());
  for (const auto &attempt : task.attempts) {
    dto.attempts.push_back(attempt_to_dto(attempt));
  }
  return dto;
}

[[nodiscard]] auto task_from_dto(detail::TaskDto dto) -> TaskSnapshot {
  TaskSnapshot task{
      .node_id = WorkflowNodeId{std::move(dto.node_id)},
      .state = static_cast<TaskState>(dto.state),
      .attempt_count = dto.attempt_count,
      .active_attempt_id = dto.active_attempt_id
                               ? std::optional{AttemptId{
                                     std::move(*dto.active_attempt_id)}}
                               : std::nullopt,
      .next_attempt_at = dto.next_attempt_at_ms
                             ? std::optional{from_milliseconds(
                                   *dto.next_attempt_at_ms)}
                             : std::nullopt,
      .skip_reason = dto.skip_reason
                         ? std::optional{static_cast<SkipReason>(
                               *dto.skip_reason)}
                         : std::nullopt,
      .last_error = std::move(dto.last_error),
      .started_at = from_milliseconds(dto.started_at_ms),
      .finished_at = from_milliseconds(dto.finished_at_ms),
  };
  task.attempts.reserve(dto.attempts.size());
  for (auto &attempt : dto.attempts) {
    task.attempts.push_back(attempt_from_dto(std::move(attempt)));
  }
  return task;
}

[[nodiscard]] auto snapshot_to_dto(const RunSnapshot &snapshot)
    -> detail::SnapshotDto {
  detail::SnapshotDto dto{
      .run_id = snapshot.run_id.str(),
      .workflow_id = snapshot.workflow_id.str(),
      .plan_id = snapshot.plan_id.str(),
      .state = static_cast<std::uint8_t>(snapshot.state),
      .stop_intent = snapshot.stop_intent
                         ? std::optional{static_cast<std::uint8_t>(
                               *snapshot.stop_intent)}
                         : std::nullopt,
      .stop_reason = snapshot.stop_reason,
      .created_at_ms = to_milliseconds(snapshot.created_at),
      .started_at_ms = to_milliseconds(snapshot.started_at),
      .finished_at_ms = to_milliseconds(snapshot.finished_at),
      .error = snapshot.error,
  };
  dto.tasks.reserve(snapshot.tasks.size());
  for (const auto &task : snapshot.tasks) {
    dto.tasks.push_back(task_to_dto(task));
  }
  return dto;
}

[[nodiscard]] auto snapshot_from_dto(detail::SnapshotDto dto) -> RunSnapshot {
  RunSnapshot snapshot{
      .run_id = WorkflowRunId{std::move(dto.run_id)},
      .workflow_id = WorkflowId{std::move(dto.workflow_id)},
      .plan_id = WorkflowPlanId{std::move(dto.plan_id)},
      .state = static_cast<RunState>(dto.state),
      .stop_intent = dto.stop_intent
                         ? std::optional{static_cast<StopIntent>(
                               *dto.stop_intent)}
                         : std::nullopt,
      .stop_reason = std::move(dto.stop_reason),
      .created_at = from_milliseconds(dto.created_at_ms),
      .started_at = from_milliseconds(dto.started_at_ms),
      .finished_at = from_milliseconds(dto.finished_at_ms),
      .error = std::move(dto.error),
  };
  snapshot.tasks.reserve(dto.tasks.size());
  for (auto &task : dto.tasks) {
    snapshot.tasks.push_back(task_from_dto(std::move(task)));
  }
  return snapshot;
}

[[nodiscard]] auto trigger_to_dto(const TriggerEnvelope &trigger)
    -> detail::TriggerDto {
  return detail::TriggerDto{
      .trigger_id = trigger.trigger_id.str(),
      .workflow_id = trigger.workflow_id.str(),
      .source = trigger.source,
      .event_type = trigger.event_type,
      .payload = value_to_dto(trigger.payload),
      .idempotency_key = trigger.idempotency_key,
      .principal_subject = trigger.principal.subject,
      .principal_roles = trigger.principal.roles,
      .trace_id = trigger.trace.trace_id,
      .parent_span_id = trigger.trace.parent_span_id,
      .occurred_at_ms = to_milliseconds(trigger.occurred_at),
  };
}

[[nodiscard]] auto trigger_from_dto(detail::TriggerDto dto)
    -> Result<TriggerEnvelope> {
  auto payload = value_from_dto(std::move(dto.payload));
  if (!payload) {
    return fail(payload.error());
  }
  return ok(TriggerEnvelope{
      .trigger_id = WorkflowTriggerId{std::move(dto.trigger_id)},
      .workflow_id = WorkflowId{std::move(dto.workflow_id)},
      .source = std::move(dto.source),
      .event_type = std::move(dto.event_type),
      .payload = std::move(*payload),
      .idempotency_key = std::move(dto.idempotency_key),
      .principal = Principal{.subject = std::move(dto.principal_subject),
                             .roles = std::move(dto.principal_roles)},
      .trace = TraceContext{.trace_id = std::move(dto.trace_id),
                            .parent_span_id =
                                std::move(dto.parent_span_id)},
      .occurred_at = from_milliseconds(dto.occurred_at_ms),
  });
}

[[nodiscard]] auto checkpoint_to_dto(const WorkflowCheckpoint &checkpoint)
    -> Result<detail::CheckpointDto> {
  auto plan_json = WorkflowPlanLoader::to_json(checkpoint.plan);
  if (!plan_json) {
    return fail(plan_json.error());
  }
  detail::CheckpointDto dto{
      .plan_json = std::move(*plan_json),
      .trigger = trigger_to_dto(checkpoint.trigger),
      .snapshot = snapshot_to_dto(checkpoint.snapshot),
      .created_at_ms = to_milliseconds(checkpoint.created_at),
  };
  dto.values.reserve(checkpoint.values.size());
  for (const auto &[output, value] : checkpoint.values) {
    dto.values.push_back(detail::OutputValueDto{
        .node_id = output.node_id.str(),
        .port = output.port.str(),
        .value = value_to_dto(value),
    });
  }
  return ok(std::move(dto));
}

[[nodiscard]] auto checkpoint_from_dto(detail::CheckpointDto dto)
    -> Result<WorkflowCheckpoint> {
  if (dto.schema_version != 1) {
    return fail(Error::Unsupported);
  }
  auto plan = WorkflowPlanLoader::from_json(dto.plan_json);
  auto trigger = trigger_from_dto(std::move(dto.trigger));
  if (!plan || !trigger) {
    return fail(!plan ? plan.error() : trigger.error());
  }
  WorkflowCheckpoint checkpoint{
      .plan = std::move(*plan),
      .trigger = std::move(*trigger),
      .snapshot = snapshot_from_dto(std::move(dto.snapshot)),
      .created_at = from_milliseconds(dto.created_at_ms),
  };
  checkpoint.values.reserve(dto.values.size());
  for (auto &entry : dto.values) {
    auto value = value_from_dto(std::move(entry.value));
    if (!value) {
      return fail(value.error());
    }
    checkpoint.values.emplace_back(
        OutputRef{.node_id = WorkflowNodeId{std::move(entry.node_id)},
                  .port = WorkflowPortId{std::move(entry.port)}},
        std::move(*value));
  }
  return ok(std::move(checkpoint));
}

[[nodiscard]] auto evidence_to_dto(const EvidenceRecord &record)
    -> detail::EvidenceDto {
  detail::EvidenceDto dto{
      .evidence_id = record.evidence_id.str(),
      .run_id = record.run_id.str(),
      .node_id = record.node_id.str(),
      .type = static_cast<std::uint8_t>(record.type),
      .timestamp_ms = to_milliseconds(record.timestamp),
      .actor_subject = record.actor.subject,
      .actor_roles = record.actor.roles,
      .metadata = record.metadata,
      .content_digest = record.content_digest,
  };
  if (record.artifact) {
    dto.artifact = value_to_dto(WorkflowValue{*record.artifact});
  }
  return dto;
}

[[nodiscard]] auto evidence_from_dto(detail::EvidenceDto dto)
    -> Result<EvidenceRecord> {
  std::optional<ArtifactRef> artifact;
  if (dto.artifact) {
    auto value = value_from_dto(std::move(*dto.artifact));
    if (!value) {
      return fail(value.error());
    }
    auto *stored = std::get_if<ArtifactRef>(&*value);
    if (stored == nullptr) {
      return fail(Error::ParseError);
    }
    artifact = std::move(*stored);
  }
  return ok(EvidenceRecord{
      .evidence_id = EvidenceId{std::move(dto.evidence_id)},
      .run_id = WorkflowRunId{std::move(dto.run_id)},
      .node_id = WorkflowNodeId{std::move(dto.node_id)},
      .type = static_cast<EvidenceType>(dto.type),
      .timestamp = from_milliseconds(dto.timestamp_ms),
      .actor = Principal{.subject = std::move(dto.actor_subject),
                         .roles = std::move(dto.actor_roles)},
      .metadata = std::move(dto.metadata),
      .artifact = std::move(artifact),
      .content_digest = std::move(dto.content_digest),
  });
}

[[nodiscard]] auto read_text_file(const std::filesystem::path &path)
    -> Result<std::string> {
  std::ifstream input(path, std::ios::binary);
  if (!input) {
    return fail(Error::NotFound);
  }
  return ok(std::string(std::istreambuf_iterator<char>(input), {}));
}

auto write_text_file_atomic(const std::filesystem::path &path,
                            std::string_view text) -> Result<void> {
  std::error_code error;
  std::filesystem::create_directories(path.parent_path(), error);
  if (error) {
    return fail(error);
  }
  auto temporary = path;
  temporary += ".tmp";
  {
    std::ofstream output(temporary, std::ios::binary | std::ios::trunc);
    if (!output) {
      return fail(Error::Unknown);
    }
    output.write(text.data(), static_cast<std::streamsize>(text.size()));
    output.flush();
    if (!output) {
      return fail(Error::Unknown);
    }
  }
  std::filesystem::rename(temporary, path, error);
  if (error) {
    std::filesystem::remove(path, error);
    error.clear();
    std::filesystem::rename(temporary, path, error);
  }
  return error ? fail(error) : ok();
}

} // namespace

auto InMemoryArtifactStore::put(std::span<const std::byte> data,
                                std::string media_type)
    -> Result<ArtifactRef> {
  auto digest = digest_bytes(data);
  if (!digest) {
    return fail(digest.error());
  }

  ArtifactBlob blob;
  blob.ref = ArtifactRef{
      .artifact_id = generate_artifact_id(),
      .media_type = std::move(media_type),
      .size_bytes = static_cast<std::uint64_t>(data.size()),
      .digest = std::move(*digest),
  };
  blob.data.assign(data.begin(), data.end());

  auto ref = blob.ref;
  std::lock_guard lock(mutex_);
  artifacts_.emplace(ref.artifact_id.str(), std::move(blob));
  return ok(std::move(ref));
}

auto InMemoryArtifactStore::get(const ArtifactId &artifact_id) const
    -> Result<ArtifactBlob> {
  std::lock_guard lock(mutex_);
  const auto it = artifacts_.find(artifact_id.str());
  if (it == artifacts_.end()) {
    return fail(Error::NotFound);
  }
  return ok(it->second);
}

auto InMemoryArtifactStore::erase(const ArtifactId &artifact_id)
    -> Result<void> {
  std::lock_guard lock(mutex_);
  if (artifacts_.erase(artifact_id.str()) == 0) {
    return fail(Error::NotFound);
  }
  return ok();
}

auto InMemoryArtifactStore::size() const -> std::size_t {
  std::lock_guard lock(mutex_);
  return artifacts_.size();
}

FileArtifactStore::FileArtifactStore(std::filesystem::path directory)
    : directory_(std::move(directory)) {
  std::error_code error;
  std::filesystem::create_directories(directory_, error);
}

auto FileArtifactStore::put(std::span<const std::byte> data,
                            std::string media_type)
    -> Result<ArtifactRef> {
  auto digest = digest_bytes(data);
  if (!digest) {
    return fail(digest.error());
  }
  ArtifactRef ref{
      .artifact_id = generate_artifact_id(),
      .media_type = std::move(media_type),
      .size_bytes = static_cast<std::uint64_t>(data.size()),
      .digest = std::move(*digest),
  };
  detail::ArtifactMetaDto metadata{
      .artifact_id = ref.artifact_id.str(),
      .media_type = ref.media_type,
      .size_bytes = ref.size_bytes,
      .digest = ref.digest,
  };
  auto encoded = serialize_json(metadata);
  if (!encoded) {
    return fail(encoded.error());
  }

  std::lock_guard lock(mutex_);
  std::error_code error;
  std::filesystem::create_directories(directory_, error);
  if (error) {
    return fail(error);
  }
  const auto base = directory_ / ref.artifact_id.str();
  const auto data_path = base.string() + ".bin";
  const auto temporary = base.string() + ".bin.tmp";
  {
    std::ofstream output(temporary, std::ios::binary | std::ios::trunc);
    if (!output) {
      return fail(Error::Unknown);
    }
    output.write(reinterpret_cast<const char *>(data.data()),
                 static_cast<std::streamsize>(data.size()));
    output.flush();
    if (!output) {
      return fail(Error::Unknown);
    }
  }
  std::filesystem::rename(temporary, data_path, error);
  if (error) {
    std::filesystem::remove(temporary, error);
    return fail(error);
  }
  auto metadata_result =
      write_text_file_atomic(base.string() + ".json", *encoded);
  if (!metadata_result) {
    std::filesystem::remove(data_path, error);
    return fail(metadata_result.error());
  }
  return ok(std::move(ref));
}

auto FileArtifactStore::get(const ArtifactId &artifact_id) const
    -> Result<ArtifactBlob> {
  std::lock_guard lock(mutex_);
  const auto base = directory_ / artifact_id.str();
  auto metadata_text = read_text_file(base.string() + ".json");
  if (!metadata_text) {
    return fail(metadata_text.error());
  }
  auto metadata = parse_json_as<detail::ArtifactMetaDto>(*metadata_text);
  if (!metadata || metadata->artifact_id != artifact_id.str()) {
    return fail(metadata ? Error::ParseError : metadata.error());
  }
  std::ifstream input(base.string() + ".bin", std::ios::binary);
  if (!input) {
    return fail(Error::NotFound);
  }
  std::vector<char> bytes(std::istreambuf_iterator<char>(input), {});
  ArtifactBlob blob{
      .ref = ArtifactRef{
          .artifact_id = artifact_id.clone(),
          .media_type = std::move(metadata->media_type),
          .size_bytes = metadata->size_bytes,
          .digest = std::move(metadata->digest),
      },
  };
  blob.data.resize(bytes.size());
  std::memcpy(blob.data.data(), bytes.data(), bytes.size());
  auto digest = digest_bytes(blob.data);
  if (!digest || *digest != blob.ref.digest ||
      blob.data.size() != blob.ref.size_bytes) {
    return fail(Error::ProtocolError);
  }
  return ok(std::move(blob));
}

auto FileArtifactStore::erase(const ArtifactId &artifact_id) -> Result<void> {
  std::lock_guard lock(mutex_);
  const auto base = directory_ / artifact_id.str();
  std::error_code data_error;
  const auto removed_data =
      std::filesystem::remove(base.string() + ".bin", data_error);
  std::error_code metadata_error;
  const auto removed_metadata =
      std::filesystem::remove(base.string() + ".json", metadata_error);
  if (data_error)
    return fail(data_error);
  if (metadata_error)
    return fail(metadata_error);
  return removed_data || removed_metadata ? ok() : fail(Error::NotFound);
}

EvidenceLedger::EvidenceLedger(std::filesystem::path file,
                               std::size_t max_records)
    : file_(std::move(file)), max_records_(max_records) {
  load_file();
}

auto EvidenceLedger::load_file() -> void {
  if (file_.empty()) {
    return;
  }
  std::ifstream input(file_);
  std::string line;
  while (std::getline(input, line)) {
    if (line.empty()) {
      continue;
    }
    auto dto = parse_json_as<detail::EvidenceDto>(line);
    if (!dto) {
      continue;
    }
    auto record = evidence_from_dto(std::move(*dto));
    if (record) {
      records_.push_back(std::move(*record));
    }
  }
  if (records_.size() > max_records_) {
    records_.erase(records_.begin(),
                   records_.end() - static_cast<std::ptrdiff_t>(max_records_));
    (void)rewrite_file();
  }
}

auto EvidenceLedger::append_file(const EvidenceRecord &record) -> Result<void> {
  if (file_.empty()) {
    return ok();
  }
  auto encoded = serialize_json(evidence_to_dto(record));
  if (!encoded) {
    return fail(encoded.error());
  }
  std::error_code error;
  std::filesystem::create_directories(file_.parent_path(), error);
  if (error) {
    return fail(error);
  }
  std::ofstream output(file_, std::ios::binary | std::ios::app);
  if (!output) {
    return fail(Error::Unknown);
  }
  output << *encoded << '\n';
  output.flush();
  return output ? ok() : fail(Error::Unknown);
}

auto EvidenceLedger::rewrite_file() -> Result<void> {
  if (file_.empty()) {
    return ok();
  }
  std::string contents;
  for (const auto &record : records_) {
    auto encoded = serialize_json(evidence_to_dto(record));
    if (!encoded) {
      return fail(encoded.error());
    }
    contents.append(*encoded);
    contents.push_back('\n');
  }
  return write_text_file_atomic(file_, contents);
}

auto EvidenceLedger::append(EvidenceRecord record) -> Result<EvidenceId> {
  if (record.run_id.empty()) {
    return fail(Error::InvalidArgument);
  }
  if (record.evidence_id.empty()) {
    record.evidence_id = generate_evidence_id();
  }
  auto id = record.evidence_id;
  std::lock_guard lock(mutex_);
  if (records_.size() < max_records_) {
    auto persisted = append_file(record);
    if (!persisted) {
      return fail(persisted.error());
    }
    records_.push_back(std::move(record));
    return ok(std::move(id));
  }

  auto previous = records_;
  records_.push_back(std::move(record));
  records_.erase(records_.begin());
  auto persisted = rewrite_file();
  if (!persisted) {
    records_ = std::move(previous);
    return fail(persisted.error());
  }
  return ok(std::move(id));
}

auto EvidenceLedger::records(const WorkflowRunId &run_id) const
    -> std::vector<EvidenceRecord> {
  std::vector<EvidenceRecord> out;
  std::lock_guard lock(mutex_);
  for (const auto &record : records_) {
    if (record.run_id == run_id) {
      out.push_back(record);
    }
  }
  return out;
}

auto EvidenceLedger::size() const -> std::size_t {
  std::lock_guard lock(mutex_);
  return records_.size();
}

CheckpointStore::CheckpointStore(std::filesystem::path directory)
    : directory_(std::move(directory)) {
  std::error_code error;
  std::filesystem::create_directories(directory_, error);
}

auto CheckpointStore::file_path(const WorkflowRunId &run_id) const
    -> std::filesystem::path {
  return directory_ / (run_id.str() + ".json");
}

auto CheckpointStore::save(WorkflowCheckpoint checkpoint) -> Result<void> {
  if (checkpoint.snapshot.run_id.empty() ||
      checkpoint.snapshot.plan_id.empty()) {
    return fail(Error::InvalidArgument);
  }
  if (!directory_.empty()) {
    auto dto = checkpoint_to_dto(checkpoint);
    if (!dto) {
      return fail(dto.error());
    }
    auto encoded = serialize_json(*dto);
    if (!encoded) {
      return fail(encoded.error());
    }
    auto written = write_text_file_atomic(
        file_path(checkpoint.snapshot.run_id), *encoded);
    if (!written) {
      return fail(written.error());
    }
  }
  std::lock_guard lock(mutex_);
  checkpoints_[checkpoint.snapshot.run_id.str()] = std::move(checkpoint);
  return ok();
}

auto CheckpointStore::load(const WorkflowRunId &run_id) const
    -> Result<WorkflowCheckpoint> {
  std::lock_guard lock(mutex_);
  const auto it = checkpoints_.find(run_id.str());
  if (it == checkpoints_.end()) {
    if (directory_.empty()) {
      return fail(Error::NotFound);
    }
    auto text = read_text_file(file_path(run_id));
    if (!text) {
      return fail(text.error());
    }
    auto dto = parse_json_as<detail::CheckpointDto>(*text);
    if (!dto) {
      return fail(dto.error());
    }
    return checkpoint_from_dto(std::move(*dto));
  }
  return ok(it->second);
}

auto CheckpointStore::erase(const WorkflowRunId &run_id) -> Result<void> {
  std::lock_guard lock(mutex_);
  const auto erased = checkpoints_.erase(run_id.str()) != 0;
  bool removed = false;
  if (!directory_.empty()) {
    std::error_code error;
    removed = std::filesystem::remove(file_path(run_id), error);
    if (error) {
      return fail(error);
    }
  }
  return erased || removed ? ok() : fail(Error::NotFound);
}

auto CheckpointStore::list() const
    -> Result<std::vector<WorkflowCheckpoint>> {
  std::vector<WorkflowCheckpoint> checkpoints;
  {
    std::lock_guard lock(mutex_);
    checkpoints.reserve(checkpoints_.size());
    for (const auto &[_, checkpoint] : checkpoints_) {
      checkpoints.push_back(checkpoint);
    }
  }
  if (directory_.empty()) {
    return ok(std::move(checkpoints));
  }

  std::unordered_map<std::string, bool> known;
  for (const auto &checkpoint : checkpoints) {
    known.emplace(checkpoint.snapshot.run_id.str(), true);
  }
  std::error_code error;
  for (std::filesystem::directory_iterator it(directory_, error), end;
       !error && it != end; it.increment(error)) {
    if (!it->is_regular_file() || it->path().extension() != ".json") {
      continue;
    }
    auto text = read_text_file(it->path());
    if (!text) {
      return fail(text.error());
    }
    auto dto = parse_json_as<detail::CheckpointDto>(*text);
    if (!dto) {
      return fail(dto.error());
    }
    auto checkpoint = checkpoint_from_dto(std::move(*dto));
    if (!checkpoint) {
      return fail(checkpoint.error());
    }
    if (known.emplace(checkpoint->snapshot.run_id.str(), true).second) {
      checkpoints.push_back(std::move(*checkpoint));
    }
  }
  if (error) {
    return fail(error);
  }
  std::ranges::sort(checkpoints, {}, &WorkflowCheckpoint::created_at);
  return ok(std::move(checkpoints));
}

} // namespace dagforge::workflow
