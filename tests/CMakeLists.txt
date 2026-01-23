# Test dependencies
find_package(GTest REQUIRED)
find_package(benchmark REQUIRED)
find_package(Threads REQUIRED)

# Test target
add_executable(taskmaster_tests
  test_utils.hpp

  # Core tests
  runtime_test.cpp
  lockfree_queue_test.cpp
  shard_test.cpp

  # Scheduler tests
  engine_test.cpp
  cron_test.cpp

  # DAG tests
  dag_test.cpp
  dag_manager_test.cpp
  dag_run_test.cpp
  trigger_rule_test.cpp
  branching_test.cpp

  # Storage tests
  persistence_test.cpp
  config_test.cpp
  config_watcher_test.cpp
  dag_validation_test.cpp

  # Executor tests
  executor_test.cpp
  docker_executor_test.cpp
  sensor_executor_test.cpp

  # HTTP/Docker client tests
  http_client_test.cpp
  docker_client_test.cpp

  # XCom tests
  xcom_test.cpp
  template_resolver_test.cpp

  # API tests
  api_test.cpp
  websocket_test.cpp

  # Integration tests
  integration_test.cpp
  api_e2e_test.cpp
)

target_link_libraries(taskmaster_tests
  PRIVATE
    TaskMaster::lib
    GTest::GTest
    GTest::Main
    Threads::Threads
)

target_include_directories(taskmaster_tests
  PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
)

taskmaster_configure_target(taskmaster_tests)

# Register tests with CTest
include(GoogleTest)
gtest_discover_tests(taskmaster_tests)

# Benchmark target
add_executable(taskmaster_benchmarks
  test_utils.hpp

  # Core benchmarks
  runtime_bench.cpp
  lockfree_queue_bench.cpp
  shard_bench.cpp

  # Scheduler benchmarks
  engine_bench.cpp
  cron_bench.cpp

  # DAG benchmarks
  dag_bench.cpp
  dag_manager_bench.cpp
  dag_run_bench.cpp

  # Storage benchmarks
  persistence_bench.cpp

  # Executor benchmarks
  executor_bench.cpp

  # Interview-level comprehensive benchmarks
  interview_bench.cpp

  # io_uring vs epoll comparison
  iouring_bench.cpp

  e2e_bench.cpp
)

target_link_libraries(taskmaster_benchmarks
  PRIVATE
    TaskMaster::lib
    benchmark::benchmark
    benchmark::benchmark_main
    Threads::Threads
)

target_include_directories(taskmaster_benchmarks
  PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
)

taskmaster_configure_target(taskmaster_benchmarks)
