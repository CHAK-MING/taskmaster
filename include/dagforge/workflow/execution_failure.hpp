#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/core/error.hpp"
#include "dagforge/util/json.hpp"
#include "dagforge/workflow/workflow_value.hpp"

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>
#endif

namespace dagforge::workflow {

struct FailureArtifact {
  std::string name;
  ArtifactRef artifact;
};

struct FailureCause {
  std::string category;
  std::int64_t value{0};
  std::string message;
};

struct ExecutionFailure {
  Error kind{Error::Unknown};
  std::string code{"unknown"};
  std::string message{"Execution failed"};
  JsonPayload details;
  std::vector<FailureArtifact> artifacts;
};

[[nodiscard]] auto normalize_execution_error(std::error_code error) noexcept
    -> Error;

[[nodiscard]] auto
make_execution_failure(Error kind, std::string code, std::string message,
                       JsonPayload details = {})
    -> ExecutionFailure;

[[nodiscard]] auto make_execution_failure(
    std::error_code cause, std::string code, std::string message)
    -> ExecutionFailure;

} // namespace dagforge::workflow

namespace glz {

template <> struct meta<dagforge::Error> {
  static constexpr auto keys = dagforge::kErrorNames;
  static constexpr auto value = [] {
    std::array<dagforge::Error, dagforge::kErrorNames.size()> values{};
    for (std::size_t index = 0; index < values.size(); ++index) {
      values[index] = static_cast<dagforge::Error>(index);
    }
    return values;
  }();
};

template <> struct meta<dagforge::workflow::FailureArtifact> {
  using T = dagforge::workflow::FailureArtifact;

  static constexpr auto read_artifact_id =
      [](T &attachment, dagforge::ArtifactId artifact_id) {
    attachment.artifact.artifact_id = std::move(artifact_id);
  };
  static constexpr auto write_artifact_id =
      [](const T &attachment) -> const dagforge::ArtifactId & {
    return attachment.artifact.artifact_id;
  };
  static constexpr auto read_media_type =
      [](T &attachment, std::string media_type) {
    attachment.artifact.media_type = std::move(media_type);
  };
  static constexpr auto write_media_type =
      [](const T &attachment) -> const std::string & {
    return attachment.artifact.media_type;
  };
  static constexpr auto read_size_bytes =
      [](T &attachment, std::uint64_t size_bytes) {
    attachment.artifact.size_bytes = size_bytes;
  };
  static constexpr auto write_size_bytes =
      [](const T &attachment) -> std::uint64_t {
    return attachment.artifact.size_bytes;
  };
  static constexpr auto read_digest = [](T &attachment, std::string digest) {
    attachment.artifact.digest = std::move(digest);
  };
  static constexpr auto write_digest =
      [](const T &attachment) -> const std::string & {
    return attachment.artifact.digest;
  };

  static constexpr auto value = object(
      "name", &T::name, "artifact_id",
      custom<read_artifact_id, write_artifact_id>, "media_type",
      custom<read_media_type, write_media_type>, "size_bytes",
      custom<read_size_bytes, write_size_bytes>, "digest",
      custom<read_digest, write_digest>);
};

} // namespace glz
