#pragma once

#include "dagforge/core/runtime.hpp"

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <exception>
#include <future>
#include <utility>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/use_future.hpp>

#include "dagforge/core/detail/sync_wait.inc"
