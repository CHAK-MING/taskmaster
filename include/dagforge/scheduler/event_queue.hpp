#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/scheduler/execution_info.hpp"
#include "dagforge/scheduler/task.hpp"
#endif

namespace dagforge {

struct AddTaskEvent {
  ExecutionInfo exec_info;
};

struct RemoveTaskEvent {
  DAGId dag_id;
  TaskId task_id;
};

} // namespace dagforge
