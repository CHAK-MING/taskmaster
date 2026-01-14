#include "taskmaster/dag/dag_manager.hpp"
#include "taskmaster/storage/persistence.hpp"

#include <benchmark/benchmark.h>

#include <filesystem>

#include <unistd.h>

using namespace taskmaster;

class PersistenceBenchFixture : public benchmark::Fixture {
public:
  void SetUp(const ::benchmark::State& state) override {
    (void)state;
    test_db_path_ = "/tmp/taskmaster_bench_XXXXXX.db";
    char* tmp_file = &test_db_path_[0];
    int fd = ::mkstemp(tmp_file);
    if (fd >= 0) {
      ::close(fd);
    }
    persistence_ = std::make_unique<Persistence>(test_db_path_);
    auto opened = persistence_->open();
    benchmark::DoNotOptimize(opened);
  }

  void TearDown(const ::benchmark::State& state) override {
    (void)state;
    persistence_->close();
    persistence_.reset();
    if (!test_db_path_.empty()) {
      std::filesystem::remove(test_db_path_);
    }
  }

  std::string test_db_path_;
  std::unique_ptr<Persistence> persistence_;
};

BENCHMARK_F(PersistenceBenchFixture,
            BM_PersistenceListDags)(benchmark::State& state) {
  for (auto _ : state) {
    auto result = persistence_->list_dags();
    benchmark::DoNotOptimize(result);
  }
}

BENCHMARK_F(PersistenceBenchFixture,
            BM_PersistenceGetIncompleteDagRuns)(benchmark::State& state) {
  for (auto _ : state) {
    auto result = persistence_->get_incomplete_dag_runs();
    benchmark::DoNotOptimize(result);
  }
}

BENCHMARK_F(PersistenceBenchFixture,
            BM_PersistenceListRunHistory)(benchmark::State& state) {
  for (auto _ : state) {
    auto result = persistence_->list_run_history();
    benchmark::DoNotOptimize(result);
  }
}

BENCHMARK_F(PersistenceBenchFixture,
            BM_PersistenceBeginCommitTransaction)(benchmark::State& state) {
  for (auto _ : state) {
    auto began = persistence_->begin_transaction();
    auto committed = persistence_->commit_transaction();
    benchmark::DoNotOptimize(began);
    benchmark::DoNotOptimize(committed);
  }
}

BENCHMARK_F(PersistenceBenchFixture,
            BM_PersistenceBeginRollbackTransaction)(benchmark::State& state) {
  for (auto _ : state) {
    auto began = persistence_->begin_transaction();
    auto rolled = persistence_->rollback_transaction();
    benchmark::DoNotOptimize(began);
    benchmark::DoNotOptimize(rolled);
  }
}
