#pragma once

#include <string>

namespace dagforge {

class Application;

[[nodiscard]] auto render_prometheus_metrics(const Application &app)
    -> std::string;

} // namespace dagforge
