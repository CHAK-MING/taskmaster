#include "dagforge/app/api/api_server.hpp"
#include "dagforge/app/application.hpp"

#include "gtest/gtest.h"

using namespace dagforge;

TEST(ApiTest, ApiServerConstructs) {
  SystemConfig cfg;
  cfg.api.enabled = false;
  Application app(std::move(cfg));
  ApiServer server(app);
  EXPECT_FALSE(server.is_running());
}

TEST(ApiTest, ApplicationConfigAccessible) {
  Application app;
  EXPECT_EQ(app.config().api.host, "127.0.0.1");
}

TEST(ApiTest, InitCreatesApiServerInstance) {
  SystemConfig cfg;
  cfg.api.enabled = true;
  Application app(std::move(cfg));
  ASSERT_TRUE(app.init().has_value());
  EXPECT_NE(app.api_server(), nullptr);
}

TEST(ApiTest, DisabledApiDoesNotAllocateServer) {
  Application app;
  ASSERT_TRUE(app.init().has_value());
  EXPECT_EQ(app.api_server(), nullptr);
}

TEST(ApiTest, InitReconcilesApiConfigurationChanges) {
  SystemConfig cfg;
  cfg.api.enabled = true;
  Application app(std::move(cfg));
  ASSERT_NE(app.api_server(), nullptr);

  app.config().api.enabled = false;
  ASSERT_TRUE(app.init().has_value());
  EXPECT_EQ(app.api_server(), nullptr);

  app.config().api.enabled = true;
  ASSERT_TRUE(app.init().has_value());
  EXPECT_NE(app.api_server(), nullptr);
}
