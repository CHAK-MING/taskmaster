#include "dagforge/config/system_config_loader.hpp"
#include "dagforge/util/json.hpp"

#include "../src/dagforge/workflow/storage/detail/storage_codec.hpp"

#include <cstddef>
#include <cstdint>
#include <string_view>

extern "C" auto LLVMFuzzerTestOneInput(const std::uint8_t *data,
                                       std::size_t size) -> int {
  static constexpr char kEmpty = '\0';
  const auto *chars = size == 0 ? &kEmpty : reinterpret_cast<const char *>(data);
  const std::string_view input{chars, size};

  (void)dagforge::parse_json(input);
  (void)dagforge::is_valid_json(input);
  (void)dagforge::config::SystemConfigLoader::load_from_string(input);
  (void)dagforge::workflow::storage_detail::decode_artifact_metadata(input);
  (void)dagforge::workflow::storage_detail::decode_checkpoint(input);
  (void)dagforge::workflow::storage_detail::decode_evidence(input);
  (void)dagforge::workflow::storage_detail::decode_stored_plan(input);

  return 0;
}
