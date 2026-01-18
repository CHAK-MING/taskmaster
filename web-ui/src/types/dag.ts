export type ExecutorType = "shell";

export type DAGRunState = "running" | "success" | "failed";

export type DAGDisplayStatus = DAGRunState | "no_run" | "inactive";

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

export type TaskState =
    | "pending"
    | "scheduled"
    | "running"
    | "success"
    | "failed"
    | "upstream_failed"
    | "retrying"
    | "skipped";

export const taskStatusLabels: Record<TaskState, string> = {
    pending: "等待中",
    scheduled: "已调度",
    running: "运行中",
    success: "成功",
    failed: "失败",
    upstream_failed: "上游失败",
    retrying: "重试中",
    skipped: "已跳过",
};

export const taskStatusColors: Record<TaskState, string> = {
    pending: "bg-muted text-muted-foreground border-border",
    scheduled: "bg-blue-500/10 text-blue-600 border-blue-500/30",
    running: "bg-blue-500/10 text-blue-600 border-blue-500/30",
    success: "bg-success/10 text-success border-success/30",
    failed: "bg-destructive/10 text-destructive border-destructive/30",
    upstream_failed: "bg-orange-500/10 text-orange-600 border-orange-500/30",
    retrying: "bg-warning/10 text-warning border-warning/30",
    skipped: "bg-slate-500/10 text-slate-500 border-slate-500/30",
};

export interface LogEntry {
    timestamp: string;
    level: "INFO" | "WARN" | "ERROR" | "DEBUG";
    message: string;
    stream?: "stdout" | "stderr";
    attempt?: number;
    taskId?: string;
}
