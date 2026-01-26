#pragma once

#include <cstdint>
#include <cstddef>
#include <bit>

namespace taskmaster::util {

// MurmurHash3 32-bit finalizer - byte-level mixing for pointer-like values
[[nodiscard]] inline constexpr auto murmur3_mix32(std::uint32_t h) noexcept -> std::uint32_t {
    h ^= h >> 16;
    h *= 0x85ebca6b;
    h ^= h >> 13;
    h *= 0xc2b2ae35;
    h ^= h >> 16;
    return h;
}

// MurmurHash3 64-bit finalizer
[[nodiscard]] inline constexpr auto murmur3_mix64(std::uint64_t h) noexcept -> std::uint64_t {
    h ^= h >> 33;
    h *= 0xff51afd7ed558ccdULL;
    h ^= h >> 33;
    h *= 0xc4ceb9fe1a85ec53ULL;
    h ^= h >> 33;
    return h;
}

// Hash pointer-sized values with proper mixing
template<typename T>
[[nodiscard]] inline auto hash_value(T value) noexcept -> std::size_t {
    if constexpr (sizeof(T) <= 4) {
        return murmur3_mix32(static_cast<std::uint32_t>(std::bit_cast<std::uintptr_t>(value)));
    } else {
        return murmur3_mix64(static_cast<std::uint64_t>(std::bit_cast<std::uintptr_t>(value)));
    }
}

// Shard calculation (Seastar-style) - better distribution than modulo of raw pointer
template<typename T>
[[nodiscard]] inline auto shard_of(T value, unsigned shard_count) noexcept -> unsigned {
    return hash_value(value) % shard_count;
}

} // namespace taskmaster::util
