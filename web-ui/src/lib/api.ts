const API_BASE = '/api';

export interface TaskConfig {
    task_id: string;
    name: string;
    command: string;
    dependencies: string[];
}

export interface DAGInfo {
    dag_id: string;
    name: string;
    description: string;
    cron: string;
    max_concurrent_runs: number;
    tasks: string[];
}

export interface SystemStatus {
    dag_count: number;
    active_runs: boolean | number;
    timestamp: string;
}

export interface HealthStatus {
    status: 'healthy' | 'stopped';
}

export interface ApiError {
    error: {
        code: string;
        message: string;
    };
}

interface DAGListResponse {
    dags: DAGInfo[];
}

interface TaskListResponse {
    tasks: string[];
}

interface HistoryListResponse {
    runs: RunRecord[];
}

interface TaskLogsResponse {
    logs: TaskLogEntry[];
}

async function handleResponse<T>(response: Response): Promise<T> {
    if (!response.ok) {
        const error = await response.json() as ApiError;
        throw new Error(error.error?.message || `HTTP ${response.status}`);
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
    state: 'pending' | 'scheduled' | 'running' | 'success' | 'failed' | 'upstream_failed' | 'retrying' | 'skipped';
    attempt: number;
    started_at: string;
    finished_at: string;
    exit_code?: number;
    error?: string;
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

export interface TaskLogEntry {
    timestamp: number;
    level: string;
    stream: string;
    message: string;
}

export async function getTaskLogs(runId: string, taskId: string): Promise<TaskLogEntry[]> {
    const response = await fetch(`${API_BASE}/runs/${runId}/tasks/${taskId}/logs`);
    const data = await handleResponse<TaskLogsResponse>(response);
    return data.logs;
}

export interface XComValue {
    [key: string]: unknown;
}

export interface TaskXComResponse {
    task_id: string;
    xcom: XComValue;
}

export interface RunXComResponse {
    dag_run_id: string;
    xcom: { [taskId: string]: XComValue };
}

export interface RunTasksResponse {
    dag_run_id: string;
    tasks: TaskRunRecord[];
}

export async function getTaskXCom(runId: string, taskId: string): Promise<TaskXComResponse> {
    const response = await fetch(`${API_BASE}/runs/${runId}/tasks/${taskId}/xcom`);
    return handleResponse<TaskXComResponse>(response);
}

export async function getRunXCom(runId: string): Promise<RunXComResponse> {
    const response = await fetch(`${API_BASE}/runs/${runId}/xcom`);
    return handleResponse<RunXComResponse>(response);
}

export async function getRunTasks(runId: string): Promise<TaskRunRecord[]> {
    const response = await fetch(`${API_BASE}/runs/${runId}/tasks`);
    const data = await handleResponse<RunTasksResponse>(response);
    return data.tasks;
}

export function connectLogsWebSocket(onMessage: (data: unknown) => void): WebSocket {
  const protocol = globalThis.location.protocol === 'https:' ? 'wss:' : 'ws:';
  const ws = new WebSocket(`${protocol}//${globalThis.location.host}/ws/logs`);

    ws.onmessage = (event) => {
        try {
            const data = JSON.parse(event.data);
            onMessage(data);
        } catch {
            // ignore
        }
    };

    return ws;
}
