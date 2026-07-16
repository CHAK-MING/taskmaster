#include "sha256.hpp"

#include <openssl/evp.h>

#include <array>
#include <memory>

namespace dagforge::workflow::detail {
namespace {

[[nodiscard]] auto lowercase_hex(std::span<const std::byte> data)
    -> std::string {
  static constexpr char kDigits[] = "0123456789abcdef";
  std::string encoded(data.size() * 2, '\0');
  for (std::size_t index = 0; index < data.size(); ++index) {
    const auto value = std::to_integer<unsigned int>(data[index]);
    encoded[index * 2] = kDigits[value >> 4U];
    encoded[index * 2 + 1] = kDigits[value & 0x0fU];
  }
  return encoded;
}

} // namespace

auto sha256_hex(std::span<const std::byte> data) -> Result<std::string> {
  auto context = std::unique_ptr<EVP_MD_CTX, decltype(&EVP_MD_CTX_free)>{
      EVP_MD_CTX_new(), &EVP_MD_CTX_free};
  if (!context || EVP_DigestInit_ex(context.get(), EVP_sha256(), nullptr) != 1 ||
      EVP_DigestUpdate(context.get(), data.data(), data.size()) != 1) {
    return fail(Error::Unknown);
  }

  std::array<unsigned char, EVP_MAX_MD_SIZE> digest{};
  unsigned int digest_size = 0;
  if (EVP_DigestFinal_ex(context.get(), digest.data(), &digest_size) != 1) {
    return fail(Error::Unknown);
  }
  return ok(lowercase_hex(std::as_bytes(
      std::span{digest.data(), static_cast<std::size_t>(digest_size)})));
}

} // namespace dagforge::workflow::detail
