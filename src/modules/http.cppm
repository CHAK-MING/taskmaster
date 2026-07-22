module;

#include "dagforge/http/http_types.hpp"

export module dagforge.http;

export import dagforge.base;
export import dagforge.util;

export namespace dagforge::http {
using ::dagforge::http::http_method_name;
using ::dagforge::http::HttpHeaders;
using ::dagforge::http::HttpMethod;
using ::dagforge::http::HttpRequest;
using ::dagforge::http::HttpResponse;
using ::dagforge::http::HttpStatus;
using ::dagforge::http::status_reason_phrase;
} // namespace dagforge::http
