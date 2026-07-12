import { useState, useEffect, useCallback, useRef } from "react";
import { AppLayout } from "@/components/AppLayout";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Play, Pause, ArrowLeft, Calendar, Loader2 } from "lucide-react";
import { useNavigate, useParams } from "react-router-dom";
import { useI18n } from "@/contexts/I18nContext";
import { useWebSocket } from "@/hooks/useWebSocket";
import type { WebSocketEventData } from "@/lib/websocket";

// TanStack Query Hooks
import {
  useDAGQuery,
  usePauseDAGMutation,
  useRunsQuery,
  useRunTasksQuery,
  useTriggerDAGMutation,
  useUnpauseDAGMutation,
  useWebSocketQuerySync,
} from "@/hooks/useDAGQueries";

// Subcomponents
import { RunHistoryTab } from "@/components/dag-detail/RunHistoryTab";
import { FlowGraphTab } from "@/components/dag-detail/FlowGraphTab";
import { TaskDefinitionsTab } from "@/components/dag-detail/TaskDefinitionsTab";
import { RunLogsTab } from "@/components/dag-detail/RunLogsTab";

// API Types
import { getTask, TaskConfig } from "@/lib/api";

interface TaskDefinition extends TaskConfig {
  dagId: string;
}

export default function DAGDetail() {
  const { id } = useParams<{ id: string }>();
  const navigate = useNavigate();
  const { t } = useI18n();

  const [selectedRunId, setSelectedRunId] = useState<string | undefined>();
  const [followLatestRun, setFollowLatestRun] = useState(true);
  const [taskDefinitions, setTaskDefinitions] = useState<TaskDefinition[]>([]);
  const previousLatestRunIdRef = useRef<string | undefined>(undefined);

  // TanStack Query: DAG Info
  const { data: dagInfo, isLoading: loadingDAG } = useDAGQuery(id);

  // TanStack Query: Run History (auto-refresh every 5s)
  const { data: runs = [] } = useRunsQuery(id, 5000);

  // TanStack Query: Task Instances for selected run
  const { data: taskInstances = [] } = useRunTasksQuery(selectedRunId, 3000);

  // TanStack Query: Trigger DAG mutation
  const triggerDAGMutation = useTriggerDAGMutation();
  const pauseDAGMutation = usePauseDAGMutation();
  const unpauseDAGMutation = useUnpauseDAGMutation();

  // WebSocket Query Sync - Push/Pull hybrid strategy
  const { handleTaskStatusChange, handleRunComplete } = useWebSocketQuerySync();

  // Fetch task definitions when DAG info is loaded
  useEffect(() => {
    if (!dagInfo || !id) return;

    const fetchTasks = async () => {
      const taskPromises = (dagInfo.tasks || []).map(async (taskId) => {
        try {
          const taskData = await getTask(id, taskId);
          return { ...taskData, dagId: id };
        } catch {
          return {
            task_id: taskId,
            name: taskId,
            command: "",
            executor: "shell" as const,
            execution_timeout_sec: 0,
            retry_interval_sec: 0,
            max_retries: 0,
            trigger_rule: "all_success",
            depends_on_past: false,
            dependencies: [],
            dagId: id,
          };
        }
      });

      const taskDefs = await Promise.all(taskPromises);
      setTaskDefinitions(taskDefs);
    };

    fetchTasks();
  }, [dagInfo, id]);

  // Auto-select first run when runs are loaded
  useEffect(() => {
    const latestRunId = runs[0]?.dag_run_id;
    const previousLatestRunId = previousLatestRunIdRef.current;

    if (!latestRunId) {
      previousLatestRunIdRef.current = undefined;
      return;
    }

    const latestChanged =
      previousLatestRunId !== undefined && latestRunId !== previousLatestRunId;
    const shouldSwitch =
      !selectedRunId ||
      followLatestRun ||
      (latestChanged && selectedRunId === previousLatestRunId);

    if (shouldSwitch && selectedRunId !== latestRunId) {
      setSelectedRunId(latestRunId);
    }

    previousLatestRunIdRef.current = latestRunId;
  }, [followLatestRun, runs, selectedRunId]);

  // WebSocket event handlers (integrated with TanStack Query) via singleton
  const handleWsTaskStatus = useCallback((data: WebSocketEventData) => {
    if (data.type !== "task_status_changed" || !id) return;
    if (data.dag_id !== id) return;
    handleTaskStatusChange({
      dag_id: data.dag_id,
      run_id: data.run_id,
      task_id: data.task_id,
      status: data.status,
    });
  }, [id, handleTaskStatusChange]);

  const handleWsRunComplete = useCallback((data: WebSocketEventData) => {
    if (data.type !== "dag_run_completed" || !id) return;
    if (data.dag_id !== id) return;
    handleRunComplete({
      dag_id: data.dag_id,
      run_id: data.run_id,
      status: data.status,
    });
  }, [id, handleRunComplete]);

  useWebSocket("task_status_changed", handleWsTaskStatus, !!id);
  useWebSocket("dag_run_completed", handleWsRunComplete, !!id);

  const handleTriggerDAG = () => {
    if (!id) return;
    setFollowLatestRun(true);
    triggerDAGMutation.mutate(id);
  };

  const handleSelectRun = useCallback((runId: string) => {
    setSelectedRunId(runId);
    setFollowLatestRun(runId === runs[0]?.dag_run_id);
  }, [runs]);

  const handleTogglePause = () => {
    if (!id || !dagInfo) return;
    if (dagInfo.is_paused) {
      unpauseDAGMutation.mutate(id);
      return;
    }
    pauseDAGMutation.mutate(id);
  };

  if (loadingDAG || !dagInfo) {
    return (
      <AppLayout>
        <div className="flex items-center justify-center h-64">
          <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
        </div>
      </AppLayout>
    );
  }

  return (
    <AppLayout title={dagInfo.name} subtitle={dagInfo.description || t.dags.noDescription}>
      <div className="mb-6">
        <Button variant="ghost" onClick={() => navigate("/dags")} className="mb-4" aria-label={t.dagDetail.backToDags}>
          <ArrowLeft className="mr-2 h-4 w-4" /> {t.dagDetail.backToDags}
        </Button>

        <div className="flex items-start justify-between">
          <div>
            <div className="flex items-center gap-2">
              <h1 className="text-2xl font-bold">{dagInfo.name}</h1>
              {!dagInfo.cron && (
                <Badge variant="secondary">{t.common.manualOnly}</Badge>
              )}
              {dagInfo.is_paused && (
                <Badge variant="outline">{t.common.paused}</Badge>
              )}
            </div>
            <p className="text-muted-foreground mt-1">{dagInfo.description || t.dags.noDescription}</p>
            {dagInfo.cron && (
              <div className="flex items-center gap-1.5 text-sm text-muted-foreground mt-2">
                <Calendar className="h-4 w-4" />
                <span className="font-mono">{dagInfo.cron}</span>
              </div>
            )}
          </div>
          <div className="flex items-center gap-2">
            <Button
              variant="outline"
              onClick={handleTogglePause}
              disabled={pauseDAGMutation.isPending || unpauseDAGMutation.isPending}
            >
              {(pauseDAGMutation.isPending || unpauseDAGMutation.isPending) ? (
                <Loader2 className="mr-2 h-4 w-4 animate-spin" />
              ) : (
                <Pause className="mr-2 h-4 w-4" />
              )}
              {dagInfo.is_paused ? t.common.resume : t.common.pause}
            </Button>
            <Button onClick={handleTriggerDAG} disabled={triggerDAGMutation.isPending} aria-label={t.dagDetail.triggerRun}>
              {triggerDAGMutation.isPending ? (
                <Loader2 className="mr-2 h-4 w-4 animate-spin" />
              ) : (
                <Play className="mr-2 h-4 w-4" />
              )}
              {t.dagDetail.triggerRun}
            </Button>
          </div>
        </div>
      </div>

      <Tabs defaultValue="tasks" className="space-y-6">
        <TabsList>
          <TabsTrigger value="tasks">{t.dagDetail.taskDefinitions}</TabsTrigger>
          <TabsTrigger value="graph">{t.dagDetail.flowGraph}</TabsTrigger>
          <TabsTrigger value="runs">{t.dagDetail.runInstances}</TabsTrigger>
          <TabsTrigger value="logs">{t.dagDetail.logs}</TabsTrigger>
        </TabsList>

        <TabsContent value="tasks">
          <TaskDefinitionsTab tasks={taskDefinitions} />
        </TabsContent>

        <TabsContent value="graph">
          <FlowGraphTab
            taskDefinitions={taskDefinitions}
            runs={runs}
            selectedRunId={selectedRunId}
            taskInstances={taskInstances}
            onSelectRun={handleSelectRun}
          />
        </TabsContent>

        <TabsContent value="runs">
          <RunHistoryTab
            runs={runs}
            selectedRunId={selectedRunId}
            onSelectRun={handleSelectRun}
          />
        </TabsContent>

        <TabsContent value="logs">
          <RunLogsTab
            runs={runs}
            selectedRunId={selectedRunId}
            taskDefinitions={taskDefinitions}
            onSelectRun={handleSelectRun}
          />
        </TabsContent>

      </Tabs>
    </AppLayout>
  );
}
