#include "dagforge/app/application.hpp"
#include "dagforge/app/metrics_exporter.hpp"

#include "gtest/gtest.h"

using namespace dagforge;

TEST(MetricsExporterTest, RendersCoreMetricFamiliesWithoutStartingApp) {
  Application app;
  ASSERT_TRUE(app.init().has_value());

  const auto text = render_prometheus_metrics(app);

  EXPECT_NE(text.find("dagforge_active_coroutines_total"), std::string::npos);
  EXPECT_NE(text.find("dagforge_event_bus_queue_length"), std::string::npos);
  EXPECT_NE(text.find("dagforge_compute_queue_depth"), std::string::npos);
  EXPECT_NE(text.find("dagforge_compute_execution_seconds"),
            std::string::npos);
  EXPECT_NE(text.find("dagforge_scheduler_queue_depth"), std::string::npos);
  EXPECT_NE(text.find("dagforge_db_errors_total"), std::string::npos);
  EXPECT_NE(text.find("dagforge_executor_active_count"), std::string::npos);
  EXPECT_NE(text.find("dagforge_task_runs_total"), std::string::npos);
}

TEST(MetricsExporterTest, RendersApiMetricFamiliesWhenApiServerExists) {
  Application app;
  ASSERT_TRUE(app.init().has_value());
  ASSERT_NE(app.api_server(), nullptr);

  const auto text = render_prometheus_metrics(app);

  EXPECT_NE(text.find("dagforge_http_active_connections"), std::string::npos);
  EXPECT_NE(text.find("dagforge_websocket_connections_active"),
            std::string::npos);
  EXPECT_NE(text.find("dagforge_websocket_messages_sent_total"),
            std::string::npos);
}

TEST(MetricsExporterTest, PrometheusOutputIncludesHelpTypeAndShardLabels) {
  Application app;

  const auto text = render_prometheus_metrics(app);

  EXPECT_NE(
      text.find(
          "# HELP dagforge_active_coroutines_total Current number of alive coroutines"),
      std::string::npos);
  EXPECT_NE(text.find("# TYPE dagforge_active_coroutines_total gauge"),
            std::string::npos);
  EXPECT_NE(text.find("dagforge_active_coroutines_total{shard=\"0\"}"),
            std::string::npos);
  EXPECT_NE(text.find("# TYPE dagforge_mysql_batch_write_ops counter"),
            std::string::npos);
}

TEST(MetricsExporterTest, OmitsApiMetricFamiliesWhenApiServerIsAbsent) {
  Application app;

  const auto text = render_prometheus_metrics(app);

  EXPECT_EQ(text.find("dagforge_http_active_connections"), std::string::npos);
  EXPECT_EQ(text.find("dagforge_websocket_connections_active"),
            std::string::npos);
  EXPECT_EQ(text.find("dagforge_websocket_messages_sent_total"),
            std::string::npos);
}
