module;

#include "dagforge/io/result.hpp"

export module dagforge.io;

export import dagforge.base;

export namespace dagforge::io {
using ::dagforge::io::io_error_category;
using ::dagforge::io::IoError;
using ::dagforge::io::IoErrorCategory;
using ::dagforge::io::is_cancelled;
using ::dagforge::io::make_error_code;
} // namespace dagforge::io
