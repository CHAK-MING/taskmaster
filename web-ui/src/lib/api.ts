// API service for TaskMaster backend

const API_BASE = '/api';

export interface TaskConfig {
    id: string;
    name: string;
    command: string;
    executor: string;
    deps: string[];
    timeout: number;
    retry_interval: number;
    max_retries: number;
    enabled: boolean;
    working_dir?: string;
}

export interface DAGInfo {
    id: string;
    name: string;
    description: string;
    cron: string;  // Cron expression at DAG level
    max_concurrent_runs: number;
    is_active: boolean;
    created_at: string;
    updated_at: string;
    tasks: TaskConfig[];
    task_count: number;
    from_config: boolean;
}

export interface SystemStatus {
    running: boolean;
    tasks: number;
    dags: number;
    active_runs: number;
    timestamp: string;
}

export interface HealthStatus {
    status: 'healthy' | 'stopped';
    timestamp: string;
}

export interface ApiError {
    error: {
        code: string;
        message: string;
    };
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
    return handleResponse<DAGInfo[]>(response);
}

export async function getDAG(dagId: string): Promise<DAGInfo> {
    const response = await fetch(`${API_BASE}/dags/${dagId}`);
    return handleResponse<DAGInfo>(response);
}

export interface CreateDAGRequest {
    name: string;
    description?: string;
    cron?: string;
    max_concurrent_runs?: number;
    is_active?: boolean;
}

export async function createDAG(data: CreateDAGRequest): Promise<DAGInfo> {
    const response = await fetch(`${API_BASE}/dags`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(data),
    });
    return handleResponse<DAGInfo>(response);
}

export interface UpdateDAGRequest {
    name?: string;
    description?: string;
    cron?: string;
    max_concurrent_runs?: number;
    is_active?: boolean;
}

export async function updateDAG(dagId: string, updates: UpdateDAGRequest): Promise<DAGInfo> {
    const response = await fetch(`${API_BASE}/dags/${dagId}`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(updates),
    });
    return handleResponse<DAGInfo>(response);
}

export async function deleteDAG(dagId: string): Promise<void> {
    const response = await fetch(`${API_BASE}/dags/${dagId}`, {
        method: 'DELETE',
    });
    if (!response.ok) {
        const error = await response.json() as ApiError;
        throw new Error(error.error?.message || `HTTP ${response.status}`);
    }
}

export async function triggerDAG(dagId: string): Promise<{ status: string; dag_id: string }> {
    const response = await fetch(`${API_BASE}/dags/${dagId}/trigger`, {
        method: 'POST',
    });
    return handleResponse<{ status: string; dag_id: string }>(response);
}

export async function createTask(dagId: string, task: TaskConfig): Promise<TaskConfig> {
    const response = await fetch(`${API_BASE}/dags/${dagId}/tasks`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(task),
    });
    return handleResponse<TaskConfig>(response);
}

export async function updateTask(dagId: string, taskId: string, task: Partial<TaskConfig>): Promise<TaskConfig> {
    const response = await fetch(`${API_BASE}/dags/${dagId}/tasks/${taskId}`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(task),
    });
    return handleResponse<TaskConfig>(response);
}

export async function deleteTask(dagId: string, taskId: string): Promise<void> {
    const response = await fetch(`${API_BASE}/dags/${dagId}/tasks/${taskId}`, {
        method: 'DELETE',
    });
    if (!response.ok) {
        const error = await response.json() as ApiError;
        throw new Error(error.error?.message || `HTTP ${response.status}`);
    }
}

export interface RunRecord {
    run_id: string;
    dag_id: string;
    dag_name: string;
    status: 'running' | 'success' | 'failed';
    trigger_type: 'manual' | 'schedule' | 'api';
    start_time: string;
    end_time: string;
    duration_ms: number;
    total_tasks: number;
    completed_tasks: number;
    failed_tasks: number;
}

export interface TaskRunRecord {
    task_id: string;
    status: 'pending' | 'scheduled' | 'running' | 'success' | 'failed' | 'upstream_failed' | 'retrying';
    attempt: number;
    start_time: string;
    end_time: string;
    exit_code: number;
    error: string;
}

export interface RunDetail extends RunRecord {
    task_runs: TaskRunRecord[];
}

export async function listHistory(dagId?: string): Promise<RunRecord[]> {
    const url = dagId ? `${API_BASE}/dags/${dagId}/history` : `${API_BASE}/history`;
    const response = await fetch(url);
    return handleResponse<RunRecord[]>(response);
}

export async function getRunDetail(runId: string): Promise<RunDetail> {
    const response = await fetch(`${API_BASE}/history/${runId}`);
    return handleResponse<RunDetail>(response);
}

export interface TaskLogEntry {
    id: number;
    run_id: string;
    task_id: string;
    attempt: number;
    timestamp: number;
    level: string;
    stream: string;
    message: string;
}

export async function getRunLogs(runId: string): Promise<TaskLogEntry[]> {
    const response = await fetch(`${API_BASE}/runs/${runId}/logs`);
    return handleResponse<TaskLogEntry[]>(response);
}

export async function getTaskLogs(runId: string, taskId: string, attempt?: number): Promise<TaskLogEntry[]> {
    let url = `${API_BASE}/runs/${runId}/tasks/${taskId}/logs`;
    if (attempt !== undefined && attempt > 0) {
        url += `?attempt=${attempt}`;
    }
    const response = await fetch(url);
    return handleResponse<TaskLogEntry[]>(response);
}

export function connectLogsWebSocket(onMessage: (data: unknown) => void): WebSocket {
    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    const ws = new WebSocket(`${protocol}//${window.location.host}/ws/logs`);

    ws.onmessage = (event) => {
        try {
            const data = JSON.parse(event.data);
            onMessage(data);
        } catch {
            // Ignore parse errors
        }
    };

    return ws;
}
