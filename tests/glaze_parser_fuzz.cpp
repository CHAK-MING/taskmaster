#include "dagforge/config/toml_util.hpp"
#include "dagforge/util/json.hpp"

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace dagforge::test {

struct FuzzToml {
  std::string name;
  std::vector<std::int64_t> values;
};

} // namespace dagforge::test

namespace glz {

template <> struct meta<dagforge::test::FuzzToml> {
  using T = dagforge::test::FuzzToml;
  static constexpr auto value = object("name", &T::name, "values", &T::values);
};

} // namespace glz

extern "C" auto LLVMFuzzerTestOneInput(const std::uint8_t *data,
                                       std::size_t size) -> int {
  static constexpr char kEmpty = '\0';
  const auto *chars = size == 0 ? &kEmpty : reinterpret_cast<const char *>(data);
  const std::string_view input{chars, size};

  (void)dagforge::parse_json(input);
  (void)dagforge::is_valid_json(input);

  dagforge::test::FuzzToml toml{};
  constexpr auto kTomlOpts =
      glz::opts{.format = glz::TOML,
                .null_terminated = false,
                .error_on_unknown_keys = false};
  (void)glz::read<kTomlOpts>(toml, input);

  return 0;
}
