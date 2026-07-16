#include "retry_policy.hpp"

#include <algorithm>
#include <random>
#include <string>

namespace dagforge::workflow::detail {
namespace {

[[nodiscard]] auto retryable(Error failure) noexcept -> bool {
  switch (failure) {
  case Error::Success:
  case Error::FileNotFound:
  case Error::ParseError:
  case Error::InvalidArgument:
  case Error::NotFound:
  case Error::AlreadyExists:
  case Error::Cancelled:
  case Error::CycleDetected:
  case Error::ReadOnly:
  case Error::HasDependents:
  case Error::HasActiveRuns:
  case Error::InvalidUrl:
  case Error::ResourceExhausted:
  case Error::InvalidState:
  case Error::Incomplete:
  case Error::ProtocolError:
  case Error::Unauthorized:
  case Error::Unsupported:
    return false;
  case Error::FileOpenFailed:
  case Error::DatabaseError:
  case Error::DatabaseOpenFailed:
  case Error::DatabaseQueryFailed:
  case Error::Timeout:
  case Error::SystemNotRunning:
  case Error::QueueFull:
  case Error::ProcessForkFailed:
  case Error::RateLimited:
  case Error::PersistenceError:
  case Error::Unknown:
    return true;
  }
  return false;
}

[[nodiscard]] auto exponential_cap(const NodePlan &node,
                                   std::uint32_t attempt_number)
    -> std::chrono::milliseconds {
  auto cap = node.retry_initial_delay;
  for (std::uint32_t current = 1;
       current < attempt_number && cap < node.retry_max_delay; ++current) {
    cap += std::min(cap, node.retry_max_delay - cap);
  }
  return cap;
}

[[nodiscard]] auto retry_seed(const WorkflowRunId &run_id,
                              const WorkflowNodeId &node_id,
                              std::uint32_t attempt_number) -> std::uint64_t {
  constexpr std::uint64_t kOffset = 14695981039346656037ULL;
  constexpr std::uint64_t kPrime = 1099511628211ULL;
  std::string identity;
  identity.reserve(run_id.size() + node_id.size() + 24);
  identity.append(run_id.value());
  identity.push_back('\x1f');
  identity.append(node_id.value());
  identity.push_back('\x1f');
  identity.append(std::to_string(attempt_number));
  auto seed = kOffset;
  for (const unsigned char byte : identity) {
    seed ^= byte;
    seed *= kPrime;
  }
  return seed;
}

[[nodiscard]] auto full_jitter(std::chrono::milliseconds cap,
                               std::uint64_t seed)
    -> std::chrono::milliseconds {
  using unsigned_rep = std::uint64_t;
  const auto range = static_cast<unsigned_rep>(cap.count()) + 1U;
  const auto rejection_threshold = static_cast<unsigned_rep>(-range) % range;
  std::mt19937_64 engine{seed};
  unsigned_rep sample = 0;
  do {
    sample = engine();
  } while (sample < rejection_threshold);
  return std::chrono::milliseconds{
      static_cast<std::chrono::milliseconds::rep>(sample % range)};
}

} // namespace

auto next_retry_delay(const NodePlan &node, Error failure,
                      std::uint32_t attempt_number,
                      const WorkflowRunId &run_id,
                      const WorkflowNodeId &node_id)
    -> std::optional<std::chrono::milliseconds> {
  if (!retryable(failure) || attempt_number == 0 || node.max_retries < 0 ||
      attempt_number > static_cast<std::uint32_t>(node.max_retries)) {
    return std::nullopt;
  }

  const auto cap = exponential_cap(node, attempt_number);
  return full_jitter(cap, retry_seed(run_id, node_id, attempt_number));
}

} // namespace dagforge::workflow::detail
