// Executor types
export type ExecutorType = "shell";

export const executorLabels: Record<ExecutorType, string> = {
    shell: "Shell",
};

// DAG Definition
export interface DAGDefinition {
    id: string;
    name: string;
    description?: string;
    cron?: string;
    maxConcurrentRuns: number;
    createdAt: string;
    updatedAt: string;
    isActive: boolean;
}

// DAG Run
export type DAGRunStatus = "running" | "success" | "failed";

export type DAGDisplayStatus = DAGRunStatus | "no_run" | "inactive";

export const dagStatusLabels: Record<DAGDisplayStatus, string> = {
    running: "运行中",
    success: "成功",
    failed: "失败",
    no_run: "未运行",
    inactive: "未启用",
};

export const dagStatusColors: Record<DAGDisplayStatus, string> = {
    running: "status-running",
    success: "status-success",
    failed: "status-failed",
    no_run: "bg-muted text-muted-foreground border border-border",
    inactive: "bg-muted text-muted-foreground border border-border",
};

export const dagRunStatusLabels = dagStatusLabels;
export const dagRunStatusColors = dagStatusColors;

export interface DAGRun {
    id: string;
    dagId: string;
    runNumber: number;
    status: DAGRunStatus;
    triggerType: "schedule" | "manual" | "api";
    startTime: string;
    endTime?: string;
    duration?: string;
    tasksTotal: number;
    tasksCompleted: number;
    tasksFailed: number;
}

// Task Definition
export interface TaskDefinition {
    id: string;
    dagId: string;
    name: string;
    executor: ExecutorType;
    command: string;
    workingDir?: string;
    dependsOn: string[];
    retryCount: number;
    retryInterval: number;
    timeout: number;
}

// Task Instance
export type TaskInstanceStatus =
    | "pending"
    | "running"
    | "success"
    | "failed"
    | "upstream_failed"
    | "retrying";

export const taskStatusLabels: Record<TaskInstanceStatus, string> = {
    pending: "等待中",
    running: "运行中",
    success: "成功",
    failed: "失败",
    upstream_failed: "上游失败",
    retrying: "重试中",
};

export const taskStatusColors: Record<TaskInstanceStatus, string> = {
    pending: "bg-muted text-muted-foreground border-border",
    running: "bg-blue-500/10 text-blue-600 border-blue-500/30",
    success: "bg-success/10 text-success border-success/30",
    failed: "bg-destructive/10 text-destructive border-destructive/30",
    upstream_failed: "bg-orange-500/10 text-orange-600 border-orange-500/30",
    retrying: "bg-warning/10 text-warning border-warning/30",
};

export interface TaskInstance {
    id: string;
    taskDefinitionId: string;
    dagRunId: string;
    status: TaskInstanceStatus;
    attempt: number;
    startTime?: string;
    endTime?: string;
    duration?: string;
    name: string;
    executor: ExecutorType;
    dependsOn: string[];
}

// Task Log
export interface TaskLog {
    taskInstanceId: string;
    attempt: number;
    logs: LogEntry[];
}

export interface LogEntry {
    timestamp: string;
    level: "INFO" | "WARN" | "ERROR" | "DEBUG";
    message: string;
    attempt?: number;
    taskId?: string;
}

// DAG Card Data
export interface DAGCardData {
    definition: DAGDefinition;
    lastRun?: DAGRun;
    taskCount: number;
    successRate: number;
}
