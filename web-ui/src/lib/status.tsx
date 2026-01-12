import {
    Clock,
    CheckCircle2,
    XCircle,
    Loader2,
    Timer,
    Terminal,
    AlertTriangle,
    RotateCcw,
    CalendarClock,
} from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { cn } from "@/lib/utils";
import {
    DAGDisplayStatus,
    dagStatusLabels,
    dagStatusColors,
    TaskState,
    taskStatusLabels,
    taskStatusColors,
    ExecutorType,
} from "@/types/dag";

export const executorLabels: Record<ExecutorType, string> = {
    shell: "Shell",
};

export const dagStatusIcons: Record<DAGDisplayStatus, React.ElementType> = {
    running: Loader2,
    success: CheckCircle2,
    failed: XCircle,
    no_run: Clock,
    inactive: Timer,
};

export const taskStatusIcons: Record<TaskState, React.ElementType> = {
    pending: Clock,
    scheduled: CalendarClock,
    running: Loader2,
    success: CheckCircle2,
    failed: XCircle,
    upstream_failed: AlertTriangle,
    retrying: RotateCcw,
};

export const executorIcons: Record<ExecutorType, React.ElementType> = {
    shell: Terminal,
};

interface DAGStatusBadgeProps {
    status: DAGDisplayStatus;
    className?: string;
}

export function DAGStatusBadge({ status, className }: DAGStatusBadgeProps) {
    const Icon = dagStatusIcons[status];
    return (
        <Badge
            variant="outline"
            className={cn(
                "flex items-center gap-1.5 px-2.5 py-1 border",
                dagStatusColors[status],
                className
            )}
        >
            <Icon className={cn("h-3.5 w-3.5", status === "running" && "animate-spin")} />
            {dagStatusLabels[status]}
        </Badge>
    );
}

interface TaskStatusBadgeProps {
    status: TaskState;
    className?: string;
}

export function TaskStatusBadge({ status, className }: TaskStatusBadgeProps) {
    const Icon = taskStatusIcons[status];
    return (
        <Badge
            variant="outline"
            className={cn(
                "flex items-center gap-1.5",
                taskStatusColors[status],
                className
            )}
        >
            <Icon className={cn("h-3 w-3", status === "running" && "animate-spin")} />
            {taskStatusLabels[status]}
        </Badge>
    );
}

interface DAGStatusIconProps {
    status: DAGDisplayStatus;
    className?: string;
}

export function DAGStatusIcon({ status, className }: DAGStatusIconProps) {
    const Icon = dagStatusIcons[status];
    const colorClass = status === "success" ? "text-success" :
        status === "failed" ? "text-destructive" :
        status === "running" ? "text-primary" : "text-muted-foreground";
    return (
        <Icon className={cn(
            "h-5 w-5",
            colorClass,
            status === "running" && "animate-spin",
            className
        )} />
    );
}

interface TaskStatusIconProps {
    status: TaskState;
    className?: string;
}

export function TaskStatusIcon({ status, className }: TaskStatusIconProps) {
    const Icon = taskStatusIcons[status];
    const colorClass = status === "success" ? "text-success" :
        (status === "failed" || status === "upstream_failed") ? "text-destructive" :
        status === "running" ? "text-primary" : "text-muted-foreground";
    return (
        <Icon className={cn(
            "h-5 w-5",
            colorClass,
            status === "running" && "animate-spin",
            className
        )} />
    );
}

export function formatDuration(ms: number): string {
    if (ms < 0) return "-";
    const seconds = Math.floor(ms / 1000);
    const minutes = Math.floor(seconds / 60);
    const hours = Math.floor(minutes / 60);
    if (hours > 0) return `${hours}h ${minutes % 60}m`;
    if (minutes > 0) return `${minutes}m ${seconds % 60}s`;
    return `${seconds}s`;
}

export function formatTime(isoString: string): string {
    if (!isoString || isoString === "-") return "-";
    try {
        return new Date(isoString).toLocaleString();
    } catch {
        return isoString;
    }
}
