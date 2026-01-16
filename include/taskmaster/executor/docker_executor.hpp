#pragma once

#include "taskmaster/executor/executor.hpp"

#include <memory>

namespace taskmaster {

class DockerExecutor : public IExecutor {
public:
  explicit DockerExecutor(Runtime& rt);
  ~DockerExecutor() override;

  DockerExecutor(const DockerExecutor&) = delete;
  auto operator=(const DockerExecutor&) -> DockerExecutor& = delete;
  DockerExecutor(DockerExecutor&&) noexcept;
  auto operator=(DockerExecutor&&) noexcept -> DockerExecutor&;

  auto start(ExecutorContext ctx, ExecutorRequest req, ExecutionSink sink)
      -> void override;

  auto cancel(const InstanceId& instance_id) -> void override;

private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace taskmaster
