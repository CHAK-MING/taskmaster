module;

#include <boost/url/parse_query.hpp>

#include <string>
#include <string_view>
#include <utility>

module dagforge.http;

namespace dagforge::http {

#include "../dagforge/client/http/detail/query_params_impl.inc"

} // namespace dagforge::http
