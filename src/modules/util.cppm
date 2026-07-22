module;

#include "dagforge/util/ascii.hpp"
#include "dagforge/util/conv.hpp"
#include "dagforge/util/enum.hpp"
#include "dagforge/util/hash.hpp"
#include "dagforge/util/parse.hpp"
#include "dagforge/util/string_hash.hpp"
#include "dagforge/util/time.hpp"

export module dagforge.util;

export import dagforge.base;

export namespace dagforge {
using ::dagforge::enum_to_string;
using ::dagforge::parse;
using ::dagforge::StringEqual;
using ::dagforge::StringHash;
} // namespace dagforge

export namespace dagforge::util {
using ::dagforge::util::ascii_is_alnum;
using ::dagforge::util::ascii_is_alpha;
using ::dagforge::util::ascii_is_digit;
using ::dagforge::util::ascii_lower;
using ::dagforge::util::ascii_lowercase;
using ::dagforge::util::ascii_upper;
using ::dagforge::util::ascii_uppercase;
using ::dagforge::util::enum_entries_are_valid;
using ::dagforge::util::enum_entry_count;
using ::dagforge::util::enum_names;
using ::dagforge::util::enum_to_code;
using ::dagforge::util::enum_to_string_view;
using ::dagforge::util::enum_token_equal;
using ::dagforge::util::enum_values;
using ::dagforge::util::EnumEntry;
using ::dagforge::util::EnumParsePolicy;
using ::dagforge::util::EnumTraits;
using ::dagforge::util::format_in_zone;
using ::dagforge::util::format_iso8601;
using ::dagforge::util::format_local_timestamp;
using ::dagforge::util::format_local_timestamp_short;
using ::dagforge::util::format_rfc3339_utc;
using ::dagforge::util::format_timestamp;
using ::dagforge::util::from_unix_millis;
using ::dagforge::util::hash_value;
using ::dagforge::util::make_parse_error;
using ::dagforge::util::parse_failure;
using ::dagforge::util::parse_int;
using ::dagforge::util::parse_integer;
using ::dagforge::util::parse_rfc3339_utc;
using ::dagforge::util::ParseError;
using ::dagforge::util::ParseErrorKind;
using ::dagforge::util::ParseResult;
using ::dagforge::util::RegisteredEnum;
using ::dagforge::util::shard_of;
using ::dagforge::util::to_unix_millis;
using ::dagforge::util::to_utc;
using ::dagforge::util::try_parse_enum;
using ::dagforge::util::try_parse_enum_code;
} // namespace dagforge::util
