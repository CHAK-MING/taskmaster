import { cn } from "@/lib/utils";
import { LogEntry } from "@/types/dag";

interface LogTimelineProps {
    readonly logs: LogEntry[];
    readonly className?: string;
}

function parseTaskFromMessage(message: string): { taskId: string | null; content: string } {
    const re = /^\[([^\]]+)\]\s*(.*)$/;
    const match = re.exec(message);
    if (match) {
        return { taskId: match[1], content: match[2] };
    }
    return { taskId: null, content: message };
}

function getTextColorClass(level: string, isSystemLog: boolean, stream?: string): string {
    if (level === "ERROR") return "text-destructive";
    if (level === "WARN") return "text-warning";
    if (stream === "stderr") return "text-muted-foreground";
    if (isSystemLog) return "text-muted-foreground";
    return "text-foreground";
}

function getNodeColorClass(level: string, isSystemLog: boolean, stream?: string): string {
    if (level === "ERROR") return "bg-destructive";
    if (level === "WARN") return "bg-warning";
    if (stream === "stderr") return "bg-muted-foreground";
    if (isSystemLog) return "bg-muted-foreground/50";
    return "bg-primary";
}

export function LogTimeline({ logs, className }: LogTimelineProps) {
    if (logs.length === 0) {
        return (
            <div className="text-center py-8 text-muted-foreground">
                暂无日志
            </div>
        );
    }

    return (
        <div className={cn("relative", className)}>
            <div className="absolute left-[5px] top-2 bottom-2 w-px bg-border" />

            <div className="space-y-0.5">
                {logs.map((log, index) => {
                    const { taskId, content } = parseTaskFromMessage(log.message);
                    const isSystemLog = !taskId;
                    const textColor = getTextColorClass(log.level, isSystemLog, log.stream);
                    const nodeColor = getNodeColorClass(log.level, isSystemLog, log.stream);
                    const isStderr = log.stream === "stderr";

                    return (
                        <div
                            key={`${log.timestamp}-${log.stream ?? ""}-${log.message}`}
                            className="relative flex items-start gap-3 py-1 pl-1 hover:bg-muted/20 rounded transition-colors"
                        >
                            <div className="relative flex-shrink-0 w-3 flex items-center justify-center z-10 mt-1">
                                <div className={cn("w-2 h-2 rounded-full", nodeColor)} />
                            </div>

                            <div className="flex-1 min-w-0 flex items-baseline gap-2 text-xs font-mono">
                                <span className="text-muted-foreground flex-shrink-0">
                                    {log.timestamp.split(" ")[1] || log.timestamp.slice(-8)}
                                </span>

                                {taskId && (
                                    <span className="text-muted-foreground flex-shrink-0">
                                        [{taskId}]
                                    </span>
                                )}

                                <span className={cn("break-all", textColor, isStderr && "italic")}>
                                    {content}
                                </span>
                            </div>
                        </div>
                    );
                })}
            </div>
        </div>
    );
}
