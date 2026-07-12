import { lazy, Suspense, useMemo } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { TaskConfig, RunRecord, TaskRunRecord } from "@/lib/api";
import { ExecutorType, TaskState } from "@/types/dag";
import { formatTime } from "@/lib/status";
import { useI18n } from "@/contexts/I18nContext";

const DAGFlow = lazy(() => import("@/components/DAGFlow").then(m => ({ default: m.DAGFlow })));

interface TaskDefinition extends TaskConfig {
  dagId: string;
}

interface FlowGraphTabProps {
  taskDefinitions: TaskDefinition[];
  runs: RunRecord[];
  selectedRunId?: string;
  taskInstances: TaskRunRecord[];
  onSelectRun: (runId: string) => void;
}

function formatExecutionSeconds(milliseconds: number): string {
  if (milliseconds <= 0) {
    return "<0.001 s";
  }
  return `${(milliseconds / 1000).toFixed(3)} s`;
}

export function FlowGraphTab({
  taskDefinitions,
  runs,
  selectedRunId,
  taskInstances,
  onSelectRun,
}: FlowGraphTabProps) {
  const { t, tf } = useI18n();
  const selectedRun = runs.find(r => r.dag_run_id === selectedRunId);
  const runNumber = selectedRun ? runs.length - runs.indexOf(selectedRun) : 0;

  const taskInstanceById = useMemo(
    () => new Map(taskInstances.map(t => [t.task_id, t])),
    [taskInstances]
  );

  const taskDependencies = useMemo(
    () => taskDefinitions.flatMap((task) =>
      task.dependencies.map((dep) => ({ from: dep.task, to: task.task_id, label: dep.label }))
    ),
    [taskDefinitions]
  );

  const flowTasks = useMemo(() => taskDefinitions.map((td) => {
    let status: TaskState = "pending";
    let duration: string | undefined;
    let attempt: number | undefined;
    let exitCode: number | undefined;
    let error: string | undefined;
    let startedAt: string | undefined;
    let finishedAt: string | undefined;

    if (selectedRun) {
      const taskInstance = taskInstanceById.get(td.task_id);
      if (taskInstance) {
        status = taskInstance.state;
        attempt = taskInstance.attempt;
        exitCode = taskInstance.exit_code;
        error = taskInstance.error || undefined;
        startedAt = taskInstance.started_at && taskInstance.started_at !== "-" ? formatTime(taskInstance.started_at) : undefined;
        finishedAt = taskInstance.finished_at && taskInstance.finished_at !== "-" ? formatTime(taskInstance.finished_at) : undefined;
        if (typeof taskInstance.duration_ms === "number" && taskInstance.duration_ms > 0) {
          duration = formatExecutionSeconds(taskInstance.duration_ms);
        } else if (taskInstance.started_at && taskInstance.finished_at) {
          const start = Date.parse(taskInstance.started_at);
          const end = Date.parse(taskInstance.finished_at);
          if (!Number.isNaN(start) && !Number.isNaN(end) && end >= start) {
            duration = formatExecutionSeconds(Math.max(0, end - start));
          }
        }
      } else if (selectedRun.state === "running") {
        status = "pending";
      }
    }

    return {
      id: td.task_id,
      name: td.name,
      taskId: td.task_id,
      status,
      duration,
      executor: ((td.executor && ["shell", "docker", "sensor", "noop"].includes(td.executor))
        ? td.executor
        : "shell") as ExecutorType,
      dependsOn: (td.dependencies ?? []).map((dep) => dep.task),
      attempt,
      exitCode,
      error,
      startedAt,
      finishedAt,
      command: td.command,
      sensorType: td.sensor_type,
      sensorTarget: td.sensor_target,
      triggerRule: td.trigger_rule || "all_success",
      dependsOnPast: td.depends_on_past,
    };
  }), [taskDefinitions, selectedRun, taskInstanceById]);

  return (
    <Card>
      <CardHeader>
        <div className="flex items-center justify-between">
          <div>
            <CardTitle className="text-base">{t.dagDetail.flowGraph}</CardTitle>
            <p className="text-sm text-muted-foreground mt-1">
              {selectedRun ? tf(t.dagDetail.showingRunStatus, { number: runNumber }) : t.dagDetail.selectRunToView}
            </p>
          </div>
          {runs.length > 0 && (
            <Select
              value={selectedRunId || ""}
              onValueChange={onSelectRun}
            >
              <SelectTrigger className="w-48">
                <SelectValue placeholder={t.dagDetail.selectRun} />
              </SelectTrigger>
              <SelectContent>
                {runs.map((run, index) => {
                  const num = runs.length - index;
                  return (
                    <SelectItem key={run.dag_run_id} value={run.dag_run_id}>
                      Run #{num} - {t.runStatus[run.state]}
                    </SelectItem>
                  );
                })}
              </SelectContent>
            </Select>
          )}
        </div>
      </CardHeader>
      <CardContent className="p-0">
        <Suspense fallback={<Skeleton className="h-[400px] w-full" />}>
          <DAGFlow
            tasks={flowTasks}
            dependencies={taskDependencies}
            className="border-0 rounded-t-none"
          />
        </Suspense>
      </CardContent>
    </Card>
  );
}
