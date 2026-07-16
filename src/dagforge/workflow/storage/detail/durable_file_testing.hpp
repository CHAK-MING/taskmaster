#pragma once

#include <cstddef>

namespace dagforge::workflow::storage_detail::testing {

auto fail_next_directory_sync() noexcept -> void;
auto fail_directory_sync_after(std::size_t successful_syncs) noexcept -> void;

} // namespace dagforge::workflow::storage_detail::testing
