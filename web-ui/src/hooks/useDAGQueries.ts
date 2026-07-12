import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import {
  listDAGs,
  getDAG,
  triggerDAG,
  pauseDAG,
  unpauseDAG,
  listHistory,
  getRunDetail,
  getRunTasks,
  getRunLogs,
  DAGInfo,
  RunRecord,
  RunDetail,
  TaskRunRecord,
  TaskLogEntry,
} from "@/lib/api";
import { toast } from "sonner";
import { useI18n } from "@/contexts/I18nContext";

// ============================================================================
// DAG Queries
// ============================================================================

export function useDAGsQuery(refetchInterval?: number) {
  return useQuery<DAGInfo[]>({
    queryKey: ['dags'],
    queryFn: listDAGs,
    refetchInterval,
    staleTime: 3000,
  });
}

export function usePauseDAGMutation() {
  const queryClient = useQueryClient();
  const { t } = useI18n();

  return useMutation({
    mutationFn: (dagId: string) => pauseDAG(dagId),
    onSuccess: () => {
      toast.success(`DAG ${t.common.paused}`);
      queryClient.invalidateQueries({ queryKey: ['dags'] });
      queryClient.invalidateQueries({ queryKey: ['dag'] });
    },
    onError: (error: Error) => {
      toast.error(`${t.common.pause} DAG ${t.common.error}`, {
        description: error.message || t.toast.unknownError,
      });
    },
  });
}

export function useUnpauseDAGMutation() {
  const queryClient = useQueryClient();
  const { t } = useI18n();

  return useMutation({
    mutationFn: (dagId: string) => unpauseDAG(dagId),
    onSuccess: () => {
      toast.success(`DAG ${t.common.resume}`);
      queryClient.invalidateQueries({ queryKey: ['dags'] });
      queryClient.invalidateQueries({ queryKey: ['dag'] });
    },
    onError: (error: Error) => {
      toast.error(`${t.common.resume} DAG ${t.common.error}`, {
        description: error.message || t.toast.unknownError,
      });
    },
  });
}

export function useDAGQuery(dagId: string | undefined) {
  return useQuery<DAGInfo>({
    queryKey: ['dag', dagId],
    queryFn: () => getDAG(dagId!),
    enabled: !!dagId,
    staleTime: 5000,
  });
}

// ============================================================================
// Run History Queries
// ============================================================================

export function useRunsQuery(dagId?: string, refetchInterval?: number) {
  return useQuery<RunRecord[]>({
    queryKey: ['runs', dagId],
    queryFn: () => listHistory(dagId),
    staleTime: 3000,
    refetchInterval,
  });
}

export function useRunDetailQuery(runId: string | undefined) {
  return useQuery<RunDetail>({
    queryKey: ['run', runId],
    queryFn: () => getRunDetail(runId!),
    enabled: !!runId,
    staleTime: 2000,
  });
}

export function useRunTasksQuery(runId: string | undefined, refetchInterval?: number) {
  return useQuery<TaskRunRecord[]>({
    queryKey: ['run-tasks', runId],
    queryFn: () => getRunTasks(runId!),
    enabled: !!runId,
    staleTime: 2000,
    refetchInterval,
  });
}

export function useRunLogsQuery(runId: string | undefined, limit: number = 10000, refetchInterval?: number) {
  return useQuery<TaskLogEntry[]>({
    queryKey: ['run-logs', runId, limit],
    queryFn: () => getRunLogs(runId!, limit),
    enabled: !!runId,
    staleTime: 2000,
    refetchInterval,
  });
}

// ============================================================================
// Mutations
// ============================================================================

export function useTriggerDAGMutation() {
  const queryClient = useQueryClient();
  const { t } = useI18n();

  return useMutation({
    mutationFn: (dagId: string) => triggerDAG(dagId),
    onSuccess: () => {
      toast.success(t.toast.dagTriggered, {
        description: t.toast.newRunCreated,
      });
      // Invalidate and refetch relevant queries
      queryClient.invalidateQueries({ queryKey: ['dags'] });
      queryClient.invalidateQueries({ queryKey: ['runs'] });
    },
    onError: (error: Error) => {
      toast.error(t.toast.dagTriggerFailed, {
        description: error.message || t.toast.unknownError,
      });
    },
  });
}

// ============================================================================
// WebSocket Integration - Push/Pull Hybrid Strategy
// ============================================================================

/**
 * Update query data when WebSocket events arrive
 * This enables instant UI updates without waiting for polling
 */
export function useWebSocketQuerySync() {
  const queryClient = useQueryClient();

  const handleTaskStatusChange = (data: {
    dag_id: string;
    run_id: string;
    task_id: string;
    status: string;
  }) => {
    // Update run tasks query with new task status
    queryClient.setQueryData<TaskRunRecord[]>(
      ['run-tasks', data.run_id],
      (old) => {
        if (!old) return old;
        return old.map(task =>
          task.task_id === data.task_id
            ? { ...task, state: data.status as TaskRunRecord['state'] }
            : task
        );
      }
    );

    // Invalidate run detail to fetch fresh data
    queryClient.invalidateQueries({ queryKey: ['run', data.run_id] });
  };

  const handleRunComplete = (data: {
    dag_id: string;
    run_id: string;
    status: string;
  }) => {
    // Invalidate all run-related queries
    queryClient.invalidateQueries({ queryKey: ['runs'] });
    queryClient.invalidateQueries({ queryKey: ['run', data.run_id] });
    queryClient.invalidateQueries({ queryKey: ['run-tasks', data.run_id] });
  };

  return {
    handleTaskStatusChange,
    handleRunComplete,
  };
}
