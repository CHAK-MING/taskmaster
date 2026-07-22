#pragma once

#include "dagforge/core/asio_awaitable.hpp"
#include "dagforge/core/coroutine.hpp"
#include "dagforge/core/error.hpp"
#include "dagforge/core/memory.hpp"
#include "dagforge/core/metrics.hpp"
#include "dagforge/core/shard.hpp"
#include "dagforge/io/timing_wheel.hpp"

#include <boost/asio/bind_allocator.hpp>
#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/post.hpp>
#include <boost/lockfree/spsc_queue.hpp>

#include <algorithm>
#include <atomic>
#include <cassert>
#include <chrono>
#include <concepts>
#include <cstdint>
#include <functional>
#include <limits>
#include <memory>
#include <memory_resource>
#include <span>
#include <thread>
#include <vector>

#include "dagforge/core/detail/runtime.inc"
