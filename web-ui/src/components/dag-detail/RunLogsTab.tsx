import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { useI18n } from "@/contexts/I18nContext";
import { useRunLogsQuery } from "@/hooks/useDAGQueries";
import { useWebSocket, useWebSocketStatus } from "@/hooks/useWebSocket";
import type { RunRecord, TaskConfig, TaskLogEntry } from "@/lib/api";
import type { WebSocketEventData } from "@/lib/websocket";
import { cn } from "@/lib/utils";
import { Activity, ChevronsDown, Search, Wifi, WifiOff } from "lucide-react";

interface TaskDefinition extends TaskConfig {
  dagId: string;
}

interface DisplayLogEntry {
  key: string;
  task_id: string;
  attempt?: number;
  stream: string;
  logged_at: string;
  content: string;
}

interface RunLogsTabProps {
  runs: RunRecord[];
  selectedRunId?: string;
  taskDefinitions: TaskDefinition[];
  onSelectRun: (runId: string) => void;
}

function toRunLabel(runs: RunRecord[], runId?: string) {
  if (!runId) {
    return "";
  }
  const index = runs.findIndex((run) => run.dag_run_id === runId);
  return index >= 0 ? `Run #${runs.length - index}` : runId;
}

function logIdentity(
  log: Pick<DisplayLogEntry, "logged_at" | "task_id" | "stream" | "content">
) {
  return `${log.logged_at}|${log.task_id}|${log.stream}|${log.content}`;
}

function mapPersistedLogs(logs: TaskLogEntry[]): DisplayLogEntry[] {
  return logs.map((log, index) => ({
    key: `history-${log.logged_at}-${log.task_id}-${log.stream}-${index}`,
    task_id: log.task_id,
    attempt: log.attempt,
    stream: log.stream,
    logged_at: log.logged_at,
    content: log.content,
  }));
}

function formatLogTime(value: string): string {
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return value;
  }
  return parsed.toLocaleTimeString([], {
    hour12: false,
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
}

export function RunLogsTab({
  runs,
  selectedRunId,
  taskDefinitions,
  onSelectRun,
}: RunLogsTabProps) {
  const { t } = useI18n();
  const { isConnected } = useWebSocketStatus();
  const [taskQuery, setTaskQuery] = useState("");
  const [streamFilter, setStreamFilter] = useState<string>("all");
  const [followTail, setFollowTail] = useState(false);
  const [liveLogs, setLiveLogs] = useState<DisplayLogEntry[]>([]);
  const containerRef = useRef<HTMLDivElement | null>(null);
  const connectedRef = useRef<boolean | null>(null);

  const selectedRun = runs.find((run) => run.dag_run_id === selectedRunId);
  const isLiveRun = selectedRun?.state === "running";

  const {
    data: persistedLogs = [],
    refetch: refetchPersistedLogs,
  } = useRunLogsQuery(selectedRunId, 10000);

  useEffect(() => {
    setLiveLogs([]);
    connectedRef.current = null;
  }, [selectedRunId]);

  useEffect(() => {
    setFollowTail(Boolean(isLiveRun));
  }, [isLiveRun, selectedRunId]);

  const persistedDisplayLogs = useMemo(
    () => mapPersistedLogs(persistedLogs),
    [persistedLogs]
  );

  const persistedIdentitySet = useMemo(
    () => new Set(persistedDisplayLogs.map((log) => logIdentity(log))),
    [persistedDisplayLogs]
  );

  const handleLogEvent = useCallback((data: WebSocketEventData) => {
    if (data.type !== "log" || data.dag_run_id !== selectedRunId) {
      return;
    }

    const nextLog: DisplayLogEntry = {
      key: `live-${data.timestamp}-${data.task_id}-${data.stream}-${data.content.length}`,
      task_id: data.task_id,
      stream: data.stream,
      logged_at: data.timestamp,
      content: data.content,
    };
    const nextIdentity = logIdentity(nextLog);

    setLiveLogs((prev) => {
      if (prev.some((log) => logIdentity(log) === nextIdentity)) {
        return prev;
      }
      return [...prev, nextLog];
    });
  }, [selectedRunId]);

  const handleRunComplete = useCallback((data: WebSocketEventData) => {
    if (data.type !== "dag_run_completed" || data.run_id !== selectedRunId) {
      return;
    }

    void refetchPersistedLogs();
  }, [refetchPersistedLogs, selectedRunId]);

  useWebSocket("log", handleLogEvent, !!selectedRunId && !!isLiveRun);
  useWebSocket("dag_run_completed", handleRunComplete, !!selectedRunId);

  useEffect(() => {
    if (!selectedRunId || !isLiveRun) {
      connectedRef.current = isConnected;
      return;
    }

    if (connectedRef.current === false && isConnected) {
      void refetchPersistedLogs();
    }
    connectedRef.current = isConnected;
  }, [isConnected, isLiveRun, refetchPersistedLogs, selectedRunId]);

  useEffect(() => {
    if (persistedIdentitySet.size === 0) {
      return;
    }

    setLiveLogs((prev) =>
      prev.filter((log) => !persistedIdentitySet.has(logIdentity(log)))
    );
  }, [persistedIdentitySet]);

  const unreconciledLiveLogs = useMemo(
    () => liveLogs.filter((log) => !persistedIdentitySet.has(logIdentity(log))),
    [liveLogs, persistedIdentitySet]
  );

  const displayLogs = useMemo(
    () => [...persistedDisplayLogs, ...unreconciledLiveLogs],
    [persistedDisplayLogs, unreconciledLiveLogs]
  );

  const taskNameById = useMemo(
    () => new Map(taskDefinitions.map((task) => [task.task_id, task.name])),
    [taskDefinitions]
  );

  const filteredLogs = useMemo(
    () =>
      displayLogs.filter((log) => {
        if (taskQuery.trim()) {
          const needle = taskQuery.trim().toLowerCase();
          const taskId = log.task_id.toLowerCase();
          const taskName = taskNameById.get(log.task_id)?.toLowerCase() ?? "";
          if (!taskId.includes(needle) && !taskName.includes(needle)) {
            return false;
          }
        }
        if (streamFilter !== "all" && log.stream !== streamFilter) {
          return false;
        }
        return true;
      }),
    [displayLogs, streamFilter, taskNameById, taskQuery]
  );

  const scrollToBottom = useCallback(() => {
    if (!containerRef.current) {
      return;
    }
    containerRef.current.scrollTop = containerRef.current.scrollHeight;
  }, []);

  const handleScroll = useCallback(() => {
    const node = containerRef.current;
    if (!node || !isLiveRun) {
      return;
    }
    const nearBottom = node.scrollHeight - node.scrollTop - node.clientHeight < 40;
    if (nearBottom !== followTail) {
      setFollowTail(nearBottom);
    }
  }, [followTail, isLiveRun]);

  useEffect(() => {
    if (!isLiveRun || !followTail) {
      return;
    }
    scrollToBottom();
  }, [filteredLogs, followTail, isLiveRun, scrollToBottom]);

  const runLabel = toRunLabel(runs, selectedRunId);

  return (
    <Card>
      <CardHeader>
        <div className="flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between">
          <div>
            <CardTitle className="text-base">{t.dagDetail.logs}</CardTitle>
            <p className="mt-1 text-sm text-muted-foreground">
              {t.dagDetail.logsDescription}
            </p>
          </div>

          {runs.length > 0 && (
            <Select value={selectedRunId || ""} onValueChange={onSelectRun}>
              <SelectTrigger className="w-full lg:w-56">
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

      <CardContent className="space-y-4">
        {!selectedRun ? (
          <div className="py-8 text-center text-muted-foreground">
            {t.dagDetail.selectRunToView}
          </div>
        ) : (
          <>
            <div className="flex flex-col gap-3 xl:flex-row xl:items-center xl:justify-between">
              <div className="flex flex-wrap items-center gap-2">
                <Badge variant="secondary" className="gap-1.5">
                  <Activity className="h-3 w-3" />
                  {runLabel}
                </Badge>
                <div className="flex items-center gap-2">
                  {isLiveRun ? (
                    <Badge
                      variant="outline"
                      className={cn(
                        "gap-1.5",
                        isConnected
                          ? "border-emerald-500/30 bg-emerald-500/10 text-emerald-700 dark:text-emerald-300"
                          : "border-amber-500/30 bg-amber-500/10 text-amber-700 dark:text-amber-300"
                      )}
                    >
                      {isConnected ? <Wifi className="h-3 w-3" /> : <WifiOff className="h-3 w-3" />}
                      {isConnected ? t.dagDetail.live : t.dagDetail.disconnected}
                    </Badge>
                  ) : (
                    <Badge variant="outline">{t.runStatus[selectedRun.state]}</Badge>
                  )}
                </div>
                <Badge variant="outline">{filteredLogs.length}</Badge>
              </div>

              <div className="grid gap-3 sm:grid-cols-[minmax(0,1fr)_180px] xl:min-w-[32rem] xl:max-w-[42rem] xl:flex-1">
                <div className="relative">
                  <Search className="pointer-events-none absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
                  <Input
                    value={taskQuery}
                    onChange={(event) => setTaskQuery(event.target.value)}
                    placeholder={t.dagDetail.taskSearchPlaceholder}
                    className="pl-9"
                  />
                </div>

                <Select value={streamFilter} onValueChange={setStreamFilter}>
                  <SelectTrigger>
                    <SelectValue placeholder={t.dagDetail.stream} />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="all">{t.dagDetail.allStreams}</SelectItem>
                    <SelectItem value="stdout">{t.dagDetail.stdout}</SelectItem>
                    <SelectItem value="stderr">{t.dagDetail.stderr}</SelectItem>
                  </SelectContent>
                </Select>
              </div>
            </div>

            <div className="relative overflow-hidden rounded-2xl border border-border/70 bg-background/80 shadow-[0_18px_40px_-24px_rgba(15,23,42,0.14)] dark:border-slate-800/80 dark:bg-slate-950 dark:text-slate-100 dark:shadow-[0_18px_40px_-24px_rgba(15,23,42,0.85)]">
              <div
                ref={containerRef}
                onScroll={handleScroll}
                className="max-h-[560px] overflow-auto"
              >
                {filteredLogs.length > 0 ? (
                  <div className="divide-y divide-border/50 dark:divide-slate-800/80">
                    {filteredLogs.map((log) => {
                      const isStdErr = log.stream === "stderr";
                      const taskName = taskNameById.get(log.task_id);

                      return (
                        <div
                          key={log.key}
                          className={cn(
                            "px-4 py-4 transition-colors",
                            isStdErr
                              ? "bg-destructive/5 hover:bg-destructive/10"
                              : "bg-transparent hover:bg-accent/20 dark:hover:bg-slate-900/60"
                          )}
                        >
                          <div className="flex flex-wrap items-center gap-x-3 gap-y-1.5">
                            <div className="font-mono tabular-nums text-[11px] text-muted-foreground dark:text-slate-400">
                              {formatLogTime(log.logged_at)}
                            </div>
                            <div className="inline-flex items-center rounded-md border border-border/60 bg-muted/35 px-2 py-1 font-mono text-[11px] font-medium text-slate-800 dark:border-slate-800/80 dark:bg-slate-900/75 dark:text-slate-100">
                              {log.task_id}
                            </div>
                            {(taskName && taskName !== log.task_id) || typeof log.attempt === "number" ? (
                              <div className="text-[11px] text-muted-foreground/90 dark:text-slate-400">
                                {[
                                  taskName && taskName !== log.task_id ? taskName : null,
                                  typeof log.attempt === "number" ? `#${log.attempt}` : null,
                                ]
                                  .filter(Boolean)
                                  .join(" · ")}
                              </div>
                            ) : null}
                            <div
                              className={cn(
                                "inline-flex items-center rounded-full border px-2.5 py-1 font-mono text-[10px] font-semibold uppercase tracking-[0.1em]",
                                isStdErr
                                  ? "border-destructive/25 bg-destructive/10 text-destructive"
                                  : "border-emerald-500/25 bg-emerald-500/10 text-emerald-700 dark:text-emerald-300"
                              )}
                            >
                              {log.stream === "stderr" ? t.dagDetail.stderr : t.dagDetail.stdout}
                            </div>
                          </div>

                          <div className="mt-2.5">
                            <div className={cn(
                              "rounded-2xl border px-4 py-3.5 shadow-[0_8px_24px_-18px_rgba(15,23,42,0.22)]",
                              isStdErr
                                ? "border-destructive/15 bg-destructive/5"
                                : "border-border/50 bg-muted/25 dark:border-slate-800/80 dark:bg-slate-900/70"
                            )}>
                              <pre className="min-w-0 whitespace-pre-wrap break-words font-mono text-[12.5px] leading-6 text-slate-800 dark:text-slate-100">
                                <code>{log.content}</code>
                              </pre>
                            </div>
                          </div>
                        </div>
                      );
                    })}
                  </div>
                ) : (
                  <div className="flex min-h-[220px] items-center justify-center text-center text-sm text-muted-foreground dark:text-slate-400">
                    {isLiveRun ? t.dagDetail.waitingLogs : t.dagDetail.noLogs}
                  </div>
                )}
              </div>

              {isLiveRun && !followTail && filteredLogs.length > 0 && (
                <div className="pointer-events-none absolute bottom-4 right-4">
                  <Button
                    type="button"
                    size="sm"
                    variant="secondary"
                    onClick={() => {
                      setFollowTail(true);
                      scrollToBottom();
                    }}
                    className="pointer-events-auto gap-1.5 rounded-full shadow-lg"
                  >
                    <ChevronsDown className="h-3.5 w-3.5" />
                    {t.dagDetail.jumpToLatest}
                  </Button>
                </div>
              )}
            </div>
          </>
        )}
      </CardContent>
    </Card>
  );
}
