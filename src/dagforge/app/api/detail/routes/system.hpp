#pragma once

namespace dagforge::api_detail {

struct ApiContext;

auto register_system_routes(ApiContext &ctx) -> void;

} // namespace dagforge::api_detail
