#include "storage_codec.hpp"

#include "dagforge/util/json.hpp"
#include "dagforge/workflow/workflow_plan_loader.hpp"

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

struct FailureDto {
  std::uint8_t kind{static_cast<std::uint8_t>(Error::Unknown)};
  std::string code;
  std::string message;
  JsonValue details = JsonValue::object_t{};
};

struct AttemptDto {
  std::string attempt_id;
  std::uint32_t number{0};
  std::uint8_t state{0};
  std::optional<std::uint8_t> termination_reason;
  std::optional<std::uint8_t> failure_class;
  std::optional<int> exit_code;
  std::optional<FailureDto> failure;
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
  std::optional<FailureDto> failure;
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
  std::optional<FailureDto> failure;
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
template <> struct meta<dagforge::workflow::detail::FailureDto> {
  using T = dagforge::workflow::detail::FailureDto;
  static constexpr auto value =
      object("kind", &T::kind, "code", &T::code, "message", &T::message,
             "details", &T::details);
};
template <> struct meta<dagforge::workflow::detail::AttemptDto> {
  using T = dagforge::workflow::detail::AttemptDto;
  static constexpr auto value = object(
      "attempt_id", &T::attempt_id, "number", &T::number, "state", &T::state,
      "termination_reason", &T::termination_reason, "failure_class",
      &T::failure_class, "exit_code", &T::exit_code, "failure", &T::failure,
      "created_at_ms", &T::created_at_ms, "started_at_ms", &T::started_at_ms,
      "finished_at_ms", &T::finished_at_ms);
};
template <> struct meta<dagforge::workflow::detail::TaskDto> {
  using T = dagforge::workflow::detail::TaskDto;
  static constexpr auto value = object(
      "node_id", &T::node_id, "state", &T::state, "attempt_count",
      &T::attempt_count, "active_attempt_id", &T::active_attempt_id,
      "next_attempt_at_ms", &T::next_attempt_at_ms, "skip_reason",
      &T::skip_reason, "failure", &T::failure, "attempts", &T::attempts,
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
      &T::finished_at_ms, "failure", &T::failure);
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
  detail::ValueDto dto;
  if (std::holds_alternative<std::monostate>(value)) {
    dto.type = "null";
    return dto;
  }
  if (const auto *boolean = std::get_if<bool>(&value)) {
    dto.type = "bool";
    dto.boolean = *boolean;
    return dto;
  }
  if (const auto *integer = std::get_if<std::int64_t>(&value)) {
    dto.type = "int";
    dto.integer = *integer;
    return dto;
  }
  if (const auto *real = std::get_if<double>(&value)) {
    dto.type = "double";
    dto.number = *real;
    return dto;
  }
  if (const auto *text = std::get_if<std::string>(&value)) {
    dto.type = "string";
    dto.text = *text;
    return dto;
  }
  if (const auto *json = std::get_if<JsonValue>(&value)) {
    dto.type = "json";
    dto.json = *json;
    return dto;
  }
  const auto &artifact = std::get<ArtifactRef>(value);
  dto.type = "artifact";
  dto.artifact_id = artifact.artifact_id.str();
  dto.media_type = artifact.media_type;
  dto.size_bytes = artifact.size_bytes;
  dto.digest = artifact.digest;
  return dto;
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

[[nodiscard]] auto failure_to_dto(const ExecutionFailure &failure)
    -> detail::FailureDto {
  return detail::FailureDto{
      .kind = static_cast<std::uint8_t>(failure.kind),
      .code = failure.code,
      .message = failure.message,
      .details = failure.details,
  };
}

[[nodiscard]] auto valid_failure_dto(
    const std::optional<detail::FailureDto> &failure) -> bool {
  if (!failure) {
    return true;
  }
  return failure->kind > std::to_underlying(Error::Success) &&
         failure->kind <= std::to_underlying(Error::Unknown) &&
         !failure->code.empty() && !failure->message.empty() &&
         failure->details.is_object();
}

[[nodiscard]] auto valid_snapshot_failures(
    const detail::SnapshotDto &snapshot) -> bool {
  if (!valid_failure_dto(snapshot.failure)) {
    return false;
  }
  for (const auto &task : snapshot.tasks) {
    if (!valid_failure_dto(task.failure)) {
      return false;
    }
    for (const auto &attempt : task.attempts) {
      if (!valid_failure_dto(attempt.failure)) {
        return false;
      }
    }
  }
  return true;
}

[[nodiscard]] auto failure_from_dto(detail::FailureDto dto)
    -> ExecutionFailure {
  return make_execution_failure(static_cast<Error>(dto.kind),
                                std::move(dto.code), std::move(dto.message),
                                std::move(dto.details));
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
      .failure = attempt.failure
                     ? std::optional{failure_to_dto(*attempt.failure)}
                     : std::nullopt,
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
      .failure = dto.failure
                     ? std::optional{failure_from_dto(
                           std::move(*dto.failure))}
                     : std::nullopt,
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
      .failure = task.failure
                     ? std::optional{failure_to_dto(*task.failure)}
                     : std::nullopt,
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
      .failure = dto.failure
                     ? std::optional{failure_from_dto(
                           std::move(*dto.failure))}
                     : std::nullopt,
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
      .failure = snapshot.failure
                     ? std::optional{failure_to_dto(*snapshot.failure)}
                     : std::nullopt,
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
      .failure = dto.failure
                     ? std::optional{failure_from_dto(
                           std::move(*dto.failure))}
                     : std::nullopt,
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
  if (!valid_snapshot_failures(dto.snapshot)) {
    return fail(Error::InvalidArgument);
  }
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
  if (!valid_snapshot_failures(dto.snapshot)) {
    return fail(Error::ParseError);
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


namespace storage_detail {

auto compute_digest(std::span<const std::byte> data) -> Result<std::string> {
  return digest_bytes(data);
}

auto encode_artifact_metadata(const ArtifactRef &artifact)
    -> Result<std::string> {
  return serialize_json(detail::ArtifactMetaDto{
      .artifact_id = artifact.artifact_id.str(),
      .media_type = artifact.media_type,
      .size_bytes = artifact.size_bytes,
      .digest = artifact.digest,
  });
}

auto decode_artifact_metadata(std::string_view json) -> Result<ArtifactRef> {
  auto metadata = parse_json_as<detail::ArtifactMetaDto>(json);
  if (!metadata || metadata->artifact_id.empty()) {
    return fail(metadata ? Error::ParseError : metadata.error());
  }
  return ok(ArtifactRef{
      .artifact_id = ArtifactId{std::move(metadata->artifact_id)},
      .media_type = std::move(metadata->media_type),
      .size_bytes = metadata->size_bytes,
      .digest = std::move(metadata->digest),
  });
}

auto encode_evidence(const EvidenceRecord &record) -> Result<std::string> {
  return serialize_json(evidence_to_dto(record));
}

auto decode_evidence(std::string_view json) -> Result<EvidenceRecord> {
  auto dto = parse_json_as<detail::EvidenceDto>(json);
  if (!dto) {
    return fail(dto.error());
  }
  return evidence_from_dto(std::move(*dto));
}

auto encode_checkpoint(const WorkflowCheckpoint &checkpoint)
    -> Result<std::string> {
  auto dto = checkpoint_to_dto(checkpoint);
  if (!dto) {
    return fail(dto.error());
  }
  return serialize_json(*dto);
}

auto decode_checkpoint(std::string_view json) -> Result<WorkflowCheckpoint> {
  auto dto = parse_json_as<detail::CheckpointDto>(json);
  if (!dto) {
    return fail(dto.error());
  }
  return checkpoint_from_dto(std::move(*dto));
}

auto load_text_file(const std::filesystem::path &path) -> Result<std::string> {
  return read_text_file(path);
}

auto store_text_file_atomic(const std::filesystem::path &path,
                            std::string_view text) -> Result<void> {
  return write_text_file_atomic(path, text);
}

} // namespace storage_detail

} // namespace dagforge::workflow
