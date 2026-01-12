#pragma once

#include <chrono>
#include <cstddef>

namespace taskmaster {

namespace io {
inline constexpr std::size_t kEventBufferSize = 4096;
inline constexpr std::size_t kReadBufferSize = 4096;
inline constexpr std::size_t kInitialOutputReserve = 8192;
}

namespace timing {
inline constexpr auto kConfigWatchInterval = std::chrono::milliseconds(100);
inline constexpr auto kShutdownPollInterval = std::chrono::milliseconds(50);
inline constexpr auto kDaemonPollInterval = std::chrono::milliseconds(100);
inline constexpr auto kRuntimeYieldInterval = std::chrono::milliseconds(1);
}

}
