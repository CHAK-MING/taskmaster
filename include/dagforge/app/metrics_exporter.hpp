#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include <string>
#endif

namespace dagforge {

class Application;

[[nodiscard]] auto render_prometheus_metrics(const Application &app)
    -> std::string;

} // namespace dagforge
