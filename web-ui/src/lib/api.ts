import { z } from 'zod';
import { API_BASE } from './config';

export interface TaskConfig {
    task_id: string;
    name: string;
    command: string;
    executor: 'shell' | 'docker' | 'sensor' | 'noop';
    sensor_type: '' | 'file' | 'http' | 'command';
    sensor_target: string;
    execution_timeout_sec: number;
    retry_interval_sec: number;
    max_retries: number;
    trigger_rule: string;
    depends_on_past: boolean;
    dependencies: TaskDependency[];
}

export interface TaskDependency {
    task: string;
    label: string;
}

export interface DAGInfo {
    dag_id: string;
    name: string;
    description: string;
    cron: string;
    max_concurrent_runs: number;
    is_paused: boolean;
    tasks: string[];
}

export interface SystemStatus {
    dag_count: number;
    active_runs: boolean;
    timestamp: string;
}

export interface HealthStatus {
    status: 'healthy' | 'stopped';
}

export interface ApiError {
    error: string;
}

const apiErrorSchema = z.object({ error: z.string() }).passthrough();

interface DAGListResponse {
    dags: DAGInfo[];
}

interface TaskListResponse {
    tasks: string[];
}

interface HistoryListResponse {
    runs: RunRecord[];
}

async function handleResponse<T>(response: Response): Promise<T> {
    if (!response.ok) {
        let message = `HTTP ${response.status}`;
        try {
            const body = await response.json();
            const parsed = apiErrorSchema.safeParse(body);
            if (parsed.success) {
                message = parsed.data.error || message;
            }
        } catch {
            // response body not JSON, keep default message
        }
        throw new Error(message);
    }
    return response.json() as Promise<T>;
}

export async function getHealth(): Promise<HealthStatus> {
    const response = await fetch(`${API_BASE}/health`);
    return handleResponse<HealthStatus>(response);
}

export async function getStatus(): Promise<SystemStatus> {
    const response = await fetch(`${API_BASE}/status`);
    return handleResponse<SystemStatus>(response);
}

export async function listDAGs(): Promise<DAGInfo[]> {
    const response = await fetch(`${API_BASE}/dags`);
    const data = await handleResponse<DAGListResponse>(response);
    return data.dags;
}

export async function getDAG(dagId: string): Promise<DAGInfo> {
    const response = await fetch(`${API_BASE}/dags/${dagId}`);
    return handleResponse<DAGInfo>(response);
}

export async function listTasks(dagId: string): Promise<string[]> {
    const response = await fetch(`${API_BASE}/dags/${dagId}/tasks`);
    const data = await handleResponse<TaskListResponse>(response);
    return data.tasks;
}

export async function getTask(dagId: string, taskId: string): Promise<TaskConfig> {
    const response = await fetch(`${API_BASE}/dags/${dagId}/tasks/${taskId}`);
    return handleResponse<TaskConfig>(response);
}

export async function triggerDAG(dagId: string): Promise<{ dag_run_id: string; status: string }> {
    const response = await fetch(`${API_BASE}/dags/${dagId}/trigger`, {
        method: 'POST',
    });
    return handleResponse<{ dag_run_id: string; status: string }>(response);
}

export async function pauseDAG(dagId: string): Promise<{ dag_id: string; is_paused: boolean }> {
    const response = await fetch(`${API_BASE}/dags/${dagId}/pause`, {
        method: 'POST',
    });
    return handleResponse<{ dag_id: string; is_paused: boolean }>(response);
}

export async function unpauseDAG(dagId: string): Promise<{ dag_id: string; is_paused: boolean }> {
    const response = await fetch(`${API_BASE}/dags/${dagId}/unpause`, {
        method: 'POST',
    });
    return handleResponse<{ dag_id: string; is_paused: boolean }>(response);
}

export interface RunRecord {
    dag_run_id: string;
    dag_id: string;
    dag_name?: string;
    state: 'running' | 'success' | 'failed';
    trigger_type: 'manual' | 'schedule' | 'api';
    started_at: string;
    finished_at: string;
    execution_date: string;
    total_tasks?: number;
    completed_tasks?: number;
    failed_tasks?: number;
}

export interface TaskRunRecord {
    task_id: string;
    state: 'pending' | 'ready' | 'running' | 'success' | 'failed' | 'upstream_failed' | 'retrying' | 'skipped';
    attempt: number;
    started_at: string;
    finished_at: string;
    exit_code: number;
    duration_ms: number;
    error: string;
}

export interface RunDetail extends RunRecord {
    task_runs?: TaskRunRecord[];
}

export async function listHistory(dagId?: string): Promise<RunRecord[]> {
    const url = dagId ? `${API_BASE}/dags/${dagId}/history` : `${API_BASE}/history`;
    const response = await fetch(url);
    const data = await handleResponse<HistoryListResponse>(response);
    return data.runs;
}

export async function getRunDetail(runId: string): Promise<RunDetail> {
    const response = await fetch(`${API_BASE}/history/${runId}`);
    return handleResponse<RunDetail>(response);
}

export interface RunTasksResponse {
    dag_run_id: string;
    tasks: TaskRunRecord[];
}

export interface TaskLogEntry {
    task_id: string;
    attempt: number;
    stream: string;
    logged_at: string;
    content: string;
}

export interface RunLogsResponse {
    dag_run_id: string;
    logs: TaskLogEntry[];
}

export async function getRunTasks(runId: string): Promise<TaskRunRecord[]> {
    const response = await fetch(`${API_BASE}/runs/${runId}/tasks`);
    const data = await handleResponse<RunTasksResponse>(response);
    return data.tasks;
}

export async function getRunLogs(runId: string, limit: number = 10000): Promise<TaskLogEntry[]> {
    const suffix = limit === 10000 ? '' : `?limit=${limit}`;
    const response = await fetch(`${API_BASE}/runs/${runId}/logs${suffix}`);
    const data = await handleResponse<RunLogsResponse>(response);
    return data.logs;
}
