#include "dagforge/app/application.hpp"
#include "dagforge/app/metrics_exporter.hpp"

#include "gtest/gtest.h"

using namespace dagforge;

TEST(MetricsExporterTest, RendersCoreMetricFamiliesWithoutStartingApp) {
  Application app;
  ASSERT_TRUE(app.init().has_value());

  const auto text = render_prometheus_metrics(app);

  EXPECT_NE(text.find("dagforge_runtime_running"), std::string::npos);
  EXPECT_NE(text.find("dagforge_runtime_shards"), std::string::npos);
  EXPECT_NE(text.find("dagforge_workflow_active_runs"), std::string::npos);
}

TEST(MetricsExporterTest, RendersHttpActiveRequestsWhenApiServerExists) {
  SystemConfig cfg;
  cfg.api.enabled = true;
  Application app(std::move(cfg));
  ASSERT_TRUE(app.init().has_value());
  ASSERT_NE(app.api_server(), nullptr);

  const auto text = render_prometheus_metrics(app);

  EXPECT_NE(text.find("dagforge_http_active_requests"), std::string::npos);
}

TEST(MetricsExporterTest, PrometheusOutputIncludesCurrentHelpAndTypes) {
  Application app;

  const auto text = render_prometheus_metrics(app);

  EXPECT_NE(
      text.find(
          "# HELP dagforge_runtime_running Whether the DAGForge runtime is running"),
      std::string::npos);
  EXPECT_NE(text.find("# TYPE dagforge_runtime_running gauge"),
            std::string::npos);
}

TEST(MetricsExporterTest, RuntimeRunningGaugeReflectsLifecycle) {
  Application app;

  const auto stopped = render_prometheus_metrics(app);
  EXPECT_NE(stopped.find("dagforge_runtime_running 0"), std::string::npos);

  ASSERT_TRUE(app.start().has_value());
  const auto running = render_prometheus_metrics(app);
  EXPECT_NE(running.find("dagforge_runtime_running 1"), std::string::npos);
  app.stop();
}
