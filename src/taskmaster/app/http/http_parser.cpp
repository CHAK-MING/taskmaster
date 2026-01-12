#include "taskmaster/app/http/http_parser.hpp"

#include <cstring>

namespace taskmaster::http {

struct HttpParser::Impl {
  llhttp_t parser;
  llhttp_settings_t settings;
  HttpRequest current_request;
  bool request_complete = false;
  std::string current_header_field;
  std::string current_header_value;
  bool in_header_field = false;

  static auto on_url(llhttp_t* parser, const char* at, size_t length) -> int {
    auto* impl = static_cast<Impl*>(parser->data);
    std::string_view url(at, length);
    
    auto query_pos = url.find('?');
    if (query_pos != std::string_view::npos) {
      impl->current_request.path.append(url.substr(0, query_pos));
      impl->current_request.query_string.append(url.substr(query_pos + 1));
    } else {
      impl->current_request.path.append(url);
    }
    return 0;
  }

  static auto on_header_field(llhttp_t* parser, const char* at, size_t length)
      -> int {
    auto* impl = static_cast<Impl*>(parser->data);
    if (!impl->in_header_field && !impl->current_header_field.empty()) {
      impl->current_request.headers[impl->current_header_field] =
          impl->current_header_value;
      impl->current_header_field.clear();
      impl->current_header_value.clear();
    }
    impl->current_header_field.append(at, length);
    impl->in_header_field = true;
    return 0;
  }

  static auto on_header_value(llhttp_t* parser, const char* at, size_t length)
      -> int {
    auto* impl = static_cast<Impl*>(parser->data);
    impl->current_header_value.append(at, length);
    impl->in_header_field = false;
    return 0;
  }

  static auto on_headers_complete(llhttp_t* parser) -> int {
    auto* impl = static_cast<Impl*>(parser->data);
    if (!impl->current_header_field.empty()) {
      impl->current_request.headers[impl->current_header_field] =
          impl->current_header_value;
      impl->current_header_field.clear();
      impl->current_header_value.clear();
    }

    switch (llhttp_get_method(&impl->parser)) {
      case HTTP_GET:
        impl->current_request.method = HttpMethod::GET;
        break;
      case HTTP_POST:
        impl->current_request.method = HttpMethod::POST;
        break;
      case HTTP_PUT:
        impl->current_request.method = HttpMethod::PUT;
        break;
      case HTTP_DELETE:
        impl->current_request.method = HttpMethod::DELETE;
        break;
      case HTTP_PATCH:
        impl->current_request.method = HttpMethod::PATCH;
        break;
      case HTTP_OPTIONS:
        impl->current_request.method = HttpMethod::OPTIONS;
        break;
      case HTTP_HEAD:
        impl->current_request.method = HttpMethod::HEAD;
        break;
      default:
        impl->current_request.method = HttpMethod::GET;
    }
    return 0;
  }

  static auto on_body(llhttp_t* parser, const char* at, size_t length) -> int {
    auto* impl = static_cast<Impl*>(parser->data);
    impl->current_request.body.insert(impl->current_request.body.end(), at,
                                      at + length);
    return 0;
  }

  static auto on_message_complete(llhttp_t* parser) -> int {
    auto* impl = static_cast<Impl*>(parser->data);
    impl->request_complete = true;
    return 0;
  }
};

HttpParser::HttpParser() : impl_(std::make_unique<Impl>()) {
  llhttp_settings_init(&impl_->settings);
  impl_->settings.on_url = Impl::on_url;
  impl_->settings.on_header_field = Impl::on_header_field;
  impl_->settings.on_header_value = Impl::on_header_value;
  impl_->settings.on_headers_complete = Impl::on_headers_complete;
  impl_->settings.on_body = Impl::on_body;
  impl_->settings.on_message_complete = Impl::on_message_complete;

  llhttp_init(&impl_->parser, HTTP_REQUEST, &impl_->settings);
  impl_->parser.data = impl_.get();
}

HttpParser::~HttpParser() = default;

auto HttpParser::parse(std::span<const uint8_t> data)
    -> std::optional<HttpRequest> {
  enum llhttp_errno err = llhttp_execute(
      &impl_->parser, reinterpret_cast<const char*>(data.data()), data.size());

  if (err != HPE_OK && err != HPE_PAUSED_UPGRADE) {
    return std::nullopt;
  }

  if (impl_->request_complete) {
    auto req = std::move(impl_->current_request);
    reset();
    return req;
  }

  return std::nullopt;
}

auto HttpParser::reset() -> void {
  impl_->current_request = HttpRequest{};
  impl_->request_complete = false;
  impl_->current_header_field.clear();
  impl_->current_header_value.clear();
  impl_->in_header_field = false;
  llhttp_init(&impl_->parser, HTTP_REQUEST, &impl_->settings);
  impl_->parser.data = impl_.get();
}

}  // namespace taskmaster::http
