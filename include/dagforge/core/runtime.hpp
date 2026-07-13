#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/core/memory.hpp"
#include "dagforge/core/coroutine.hpp"
#include "dagforge/core/error.hpp"
#include "dagforge/core/asio_awaitable.hpp"
#include "dagforge/io/timing_wheel.hpp"
#include "dagforge/core/metrics.hpp"
#include "dagforge/core/shard.hpp"
#endif

#include <boost/asio/bind_allocator.hpp>
#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/post.hpp>
#include <boost/lockfree/spsc_queue.hpp>

#include <algorithm>
#include <atomic>
#include <cassert>
#include <chrono>
#include <cstdint>
#include <concepts>
#include <functional>
#include <limits>
#include <memory>
#include <memory_resource>
#include <span>
#include <thread>
#include <vector>

#include "dagforge/core/detail/runtime.inc"
