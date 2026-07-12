import { useCallback, useEffect, useMemo, useState } from "react";
import ReactFlow, {
  Node,
  Edge,
  Background,
  Controls,
  MiniMap,
  Position,
  MarkerType,
} from "reactflow";
import "reactflow/dist/style.css";
import { cn } from "@/lib/utils";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Separator } from "@/components/ui/separator";
import { toast } from "@/components/ui/use-toast";
import { useI18n } from "@/contexts/I18nContext";
import { TaskState } from "@/types/dag";
import { executorLabels, sensorTypeLabels, triggerRuleLabels } from "@/lib/status";
import { Check, Copy } from "lucide-react";

type FlowTaskStatus = TaskState;

interface Task {
  id: string;
  name: string;
  status: FlowTaskStatus;
  taskId?: string;
  duration?: string;
  executor?: string;
  dependsOn?: string[];
  attempt?: number;
  exitCode?: number;
  error?: string;
  startedAt?: string;
  finishedAt?: string;
  command?: string;
  sensorType?: string;
  sensorTarget?: string;
  triggerRule?: string;
  dependsOnPast?: boolean;
}

interface FlowDependency {
  from: string;
  to: string;
  label?: string;
}

interface DAGFlowProps {
  tasks: Task[];
  dependencies?: FlowDependency[];
  className?: string;
  onTaskClick?: (task: Task) => void;
}

const statusColors: Record<FlowTaskStatus, { bg: string; border: string; text: string }> = {
  success: {
    bg: "hsl(142 71% 45% / 0.15)",
    border: "hsl(142 71% 45%)",
    text: "hsl(142 71% 35%)",
  },
  failed: {
    bg: "hsl(0 84% 60% / 0.15)",
    border: "hsl(0 84% 60%)",
    text: "hsl(0 84% 40%)",
  },
  upstream_failed: {
    bg: "hsl(24 95% 53% / 0.15)",
    border: "hsl(24 95% 53%)",
    text: "hsl(24 95% 38%)",
  },
  running: {
    bg: "hsl(199 89% 48% / 0.15)",
    border: "hsl(199 89% 48%)",
    text: "hsl(199 89% 38%)",
  },
  retrying: {
    bg: "hsl(45 93% 47% / 0.15)",
    border: "hsl(45 93% 47%)",
    text: "hsl(45 93% 32%)",
  },
  ready: {
    bg: "hsl(38 92% 50% / 0.15)",
    border: "hsl(38 92% 50%)",
    text: "hsl(38 92% 35%)",
  },
  pending: {
    bg: "hsl(215 16% 47% / 0.08)",
    border: "hsl(215 16% 60% / 0.6)",
    text: "hsl(215 16% 34%)",
  },
  skipped: {
    bg: "hsl(215 16% 47% / 0.15)",
    border: "hsl(215 16% 47%)",
    text: "hsl(215 16% 37%)",
  },
};

function useStatusLabels(): Record<FlowTaskStatus, string> {
  const { t } = useI18n();
  return {
    success: t.runStatus.success,
    failed: t.runStatus.failed,
    upstream_failed: t.runStatus.upstreamFailed,
    running: t.runStatus.running,
    retrying: t.runStatus.retrying,
    ready: t.runStatus.ready,
    pending: t.runStatus.pending,
    skipped: t.runStatus.skipped,
  };
}

function DetailField({
  label,
  children,
  className,
}: {
  label: string;
  children: React.ReactNode;
  className?: string;
}) {
  return (
    <div className={cn("rounded-xl border border-border/60 bg-muted/20 px-3 py-3", className)}>
      <div className="mb-1.5 text-[11px] font-medium uppercase tracking-[0.08em] text-muted-foreground/80">
        {label}
      </div>
      <div className="text-sm text-foreground">{children}</div>
    </div>
  );
}

export function DAGFlow({ tasks, dependencies, className, onTaskClick }: DAGFlowProps) {
  const { t, tf } = useI18n();
  const statusLabels = useStatusLabels();
  const [selectedTask, setSelectedTask] = useState<Task | null>(null);
  const [copiedCommand, setCopiedCommand] = useState(false);

  useEffect(() => {
    if (!copiedCommand) {
      return;
    }
    const timer = window.setTimeout(() => setCopiedCommand(false), 1600);
    return () => window.clearTimeout(timer);
  }, [copiedCommand]);

  const normalizedDependencies = useMemo<FlowDependency[]>(() => {
    if (dependencies && dependencies.length > 0) {
      return dependencies;
    }

    return tasks.flatMap((task) =>
      (task.dependsOn || []).map((dep) => ({ from: dep, to: task.id }))
    );
  }, [tasks, dependencies]);

  // Build adjacency lists and compute topological layers
  const { nodePositions } = useMemo(() => {
    const nodeWidth = 220;
    const nodeHeight = 96;
    const horizontalGap = 120;
    const verticalGap = 100;

    // Build dependency graph
    const taskMap = new Map(tasks.map(t => [t.id, t]));
    const inDegree = new Map<string, number>();
    const children = new Map<string, string[]>();
    const parents = new Map<string, string[]>();

    // Initialize
    for (const task of tasks) {
      inDegree.set(task.id, 0);
      children.set(task.id, []);
      parents.set(task.id, []);
    }

    for (const { from, to } of normalizedDependencies) {
      if (taskMap.has(from) && taskMap.has(to)) {
        children.get(from)!.push(to);
        parents.get(to)!.push(from);
        inDegree.set(to, (inDegree.get(to) || 0) + 1);
      }
    }

    // Compute layers using longest path (ensures proper DAG depth)
    const layer = new Map<string, number>();
    const queue: string[] = [];

    // Start with nodes that have no parents
    for (const task of tasks) {
      if ((inDegree.get(task.id) || 0) === 0) {
        layer.set(task.id, 0);
        queue.push(task.id);
      }
    }

    // BFS to compute max layer for each node
    while (queue.length > 0) {
      const nodeId = queue.shift()!;
      const currentLayer = layer.get(nodeId)!;
      for (const child of children.get(nodeId) || []) {
        const newLayer = currentLayer + 1;
        if (!layer.has(child) || layer.get(child)! < newLayer) {
          layer.set(child, newLayer);
        }
        // Decrease in-degree and add to queue when all parents processed
        const newDegree = (inDegree.get(child) || 1) - 1;
        inDegree.set(child, newDegree);
        if (newDegree === 0) {
          queue.push(child);
        }
      }
    }

    // Handle disconnected nodes (no dependencies)
    for (const task of tasks) {
      if (!layer.has(task.id)) {
        layer.set(task.id, 0);
      }
    }

    // Group nodes by layer
    const layers = new Map<number, string[]>();
    for (const [nodeId, layerIdx] of layer) {
      if (!layers.has(layerIdx)) {
        layers.set(layerIdx, []);
      }
      layers.get(layerIdx)!.push(nodeId);
    }

    // Calculate positions
    const positions = new Map<string, { x: number; y: number }>();

    // If dependency data is unavailable, avoid fake linear structure and
    // render a compact grid so users don't infer non-existent dependencies.
    const maxLayer = Math.max(...Array.from(layers.keys()), 0);
    if (maxLayer === 0) {
      const columns = Math.max(1, Math.ceil(Math.sqrt(tasks.length)));
      for (const [index, task] of tasks.entries()) {
        const col = index % columns;
        const row = Math.floor(index / columns);
        positions.set(task.id, {
          x: col * (nodeWidth + horizontalGap) + 50,
          y: row * (nodeHeight + verticalGap) + 50,
        });
      }
      return { nodePositions: positions };
    }

    const maxNodesInLayer = Math.max(...Array.from(layers.values()).map(l => l.length), 1);

    for (const [layerIdx, nodeIds] of layers) {
      const layerHeight = nodeIds.length * (nodeHeight + verticalGap);
      const totalHeight = maxNodesInLayer * (nodeHeight + verticalGap);
      const startY = (totalHeight - layerHeight) / 2 + 50;

      nodeIds.forEach((nodeId, idx) => {
        positions.set(nodeId, {
          x: layerIdx * (nodeWidth + horizontalGap) + 50,
          y: startY + idx * (nodeHeight + verticalGap),
        });
      });
    }

    return { nodePositions: positions };
  }, [tasks, normalizedDependencies]);

  const initialNodes: Node[] = useMemo(() => {
    const nodeWidth = 220;
    const nodeMinHeight = 96;

    return tasks.map((task) => {
      const colors = statusColors[task.status];
      const isRunning = task.status === "running";
      const position = nodePositions.get(task.id) || { x: 50, y: 50 };

      return {
        id: task.id,
        data: {
          label: (
            <div className="cursor-pointer text-left">
              <div
                className="line-clamp-2 break-words text-[13px] font-semibold leading-5 tracking-wide text-foreground"
                title={task.name}
              >
                {task.name}
              </div>
              {task.taskId && (
                <div
                  className="mt-1 truncate text-[10px] font-mono text-muted-foreground"
                  title={task.taskId}
                >
                  {task.taskId}
                </div>
              )}
              <div className="mt-3 flex items-center justify-between gap-2">
                <div
                  className="inline-flex min-w-0 max-w-[132px] items-center rounded-full px-2.5 py-1 text-[11px] font-medium"
                  style={{
                    backgroundColor: colors.bg,
                    color: colors.text,
                    border: `1px solid ${colors.border}40`,
                  }}
                  title={statusLabels[task.status]}
                >
                  <span className="truncate">{statusLabels[task.status]}</span>
                </div>
                {task.duration && (
                  <div className="shrink-0 text-[11px] text-muted-foreground">
                    {task.duration}
                  </div>
                )}
              </div>
            </div>
          ),
          task,
        },
        position,
        style: {
          background: "hsl(var(--card) / 0.7)",
          backdropFilter: "blur(12px)",
          border: `1px solid ${colors.border}`,
          borderRadius: "10px",
          padding: "14px 16px",
          width: nodeWidth,
          minHeight: nodeMinHeight,
          boxShadow: isRunning
            ? `0 0 0 1px ${colors.border}55, 0 8px 16px -4px ${colors.border}33`
            : `0 4px 12px -2px hsl(var(--foreground) / 0.05)`,
          color: "hsl(var(--foreground))",
          animation: isRunning ? "node-pulse 2s ease-in-out infinite" : undefined,
          transition: "all 0.3s ease",
        },
        sourcePosition: Position.Right,
        targetPosition: Position.Left,
      };
    });
  }, [nodePositions, statusLabels, tasks]);

  const initialEdges: Edge[] = useMemo(() => {
    if (normalizedDependencies.length === 0) {
      return [];
    }

    return normalizedDependencies.map((dep, index) => {
      const targetTask = tasks.find((t) => t.id === dep.to);
      return {
        id: `e${dep.from}-${dep.to}-${index}`,
        source: dep.from,
        target: dep.to,
        type: "smoothstep",
        animated: targetTask?.status === "running",
        label: dep.label || undefined,
        labelStyle: dep.label
          ? {
            fill: "hsl(var(--foreground))",
            fontSize: 11,
            fontWeight: 600,
          }
          : undefined,
        labelBgStyle: dep.label
          ? {
            fill: "hsl(var(--card))",
            fillOpacity: 0.9,
            stroke: "hsl(var(--border))",
            strokeWidth: 1,
          }
          : undefined,
        labelBgPadding: dep.label ? [6, 2] : undefined,
        labelBgBorderRadius: dep.label ? 4 : undefined,
        style: {
          stroke: "hsl(var(--primary))",
          strokeWidth: 2,
        },
        markerEnd: {
          type: MarkerType.ArrowClosed,
          color: "hsl(var(--primary))",
        },
      };
    });
  }, [normalizedDependencies, tasks]);

  const handleNodeClick = useCallback((_: React.MouseEvent, node: Node) => {
    const task = node.data.task as Task;
    if (onTaskClick) {
      onTaskClick(task);
    } else {
      setSelectedTask(task);
    }
  }, [onTaskClick]);

  const handleCopyCommand = useCallback(async () => {
    if (!selectedTask?.command) {
      return;
    }
    if (!navigator.clipboard?.writeText) {
      toast({
        variant: "destructive",
        title: t.common.error,
        description: t.toast.commandCopyFailed,
      });
      return;
    }

    try {
      await navigator.clipboard.writeText(selectedTask.command);
      setCopiedCommand(true);
      toast({
        title: t.toast.commandCopied,
        description: selectedTask.taskId,
      });
    } catch {
      toast({
        variant: "destructive",
        title: t.common.error,
        description: t.toast.commandCopyFailed,
      });
    }
  }, [selectedTask, t]);

  return (
    <>
      <div className={cn("h-[400px] w-full rounded-lg border border-border bg-card", className)}>
        <ReactFlow
          nodes={initialNodes}
          edges={initialEdges}
          onNodeClick={handleNodeClick}
          fitView
          attributionPosition="bottom-left"
          proOptions={{ hideAttribution: true }}
        >
          <Background color="hsl(var(--muted-foreground))" gap={20} size={1} />
          <Controls
            className="bg-card border border-border rounded-lg"
            showInteractive={false}
          />
          <MiniMap
            className="bg-card border border-border rounded-lg"
            nodeColor={(node) => {
              const task = tasks.find((t) => t.id === node.id);
              if (!task) return "hsl(var(--muted))";
              return statusColors[task.status].border;
            }}
            maskColor="hsl(var(--background) / 0.8)"
          />
        </ReactFlow>
      </div>

      <Dialog
        open={!!selectedTask}
        onOpenChange={(open) => {
          if (!open) {
            setSelectedTask(null);
            setCopiedCommand(false);
          }
        }}
      >
        <DialogContent className="sm:max-w-2xl">
          <DialogHeader className="space-y-3">
            <div className="flex items-start justify-between gap-3 pr-6">
              <div className="min-w-0 flex-1">
                <DialogTitle className="break-words pr-2 leading-snug">
                  {selectedTask?.name}
                </DialogTitle>
                {selectedTask?.taskId && (
                  <div className="mt-2 inline-flex max-w-full rounded-full border border-border/70 bg-muted/30 px-2.5 py-1 font-mono text-[11px] text-muted-foreground">
                    <span className="truncate">{selectedTask.taskId}</span>
                  </div>
                )}
              </div>
              {selectedTask && (
                <Badge
                  variant="outline"
                  className="shrink-0"
                  style={{
                    backgroundColor: statusColors[selectedTask.status].bg,
                    color: statusColors[selectedTask.status].text,
                    borderColor: statusColors[selectedTask.status].border,
                  }}
                >
                  {statusLabels[selectedTask.status]}
                </Badge>
              )}
            </div>
            <DialogDescription>{t.dagDetail.taskDefinitions}</DialogDescription>
          </DialogHeader>
          <Separator />
          <div className="space-y-4 py-2">
            <div className="grid gap-3 sm:grid-cols-2">
              {selectedTask?.executor && (
                <DetailField label={t.dagDetail.executor}>
                  <Badge variant="secondary">
                    {executorLabels[(selectedTask.executor as keyof typeof executorLabels) ?? "shell"] ?? selectedTask.executor}
                  </Badge>
                </DetailField>
              )}
              {selectedTask?.triggerRule && (
                <DetailField label={t.dagDetail.triggerRule}>
                  <Badge variant="outline">
                    {triggerRuleLabels[selectedTask.triggerRule] ?? selectedTask.triggerRule}
                  </Badge>
                </DetailField>
              )}
              {selectedTask?.executor === "sensor" && selectedTask?.sensorType && (
                <DetailField label={t.dagDetail.sensorType}>
                  <Badge variant="outline">
                    {sensorTypeLabels[selectedTask.sensorType] ?? selectedTask.sensorType}
                  </Badge>
                </DetailField>
              )}
              {typeof selectedTask?.attempt === "number" && (
                <DetailField label={t.dagDetail.attempt}>
                  <span>
                    {selectedTask.attempt}
                    {" "}
                    {selectedTask.attempt > 1
                      ? `(${tf(t.dagDetail.retriedTimes, { count: selectedTask.attempt - 1 })})`
                      : `(${t.dagDetail.firstAttempt})`}
                  </span>
                </DetailField>
              )}
              {typeof selectedTask?.exitCode === "number" && selectedTask.exitCode !== 0 && (
                <DetailField label={t.dagDetail.exitCode}>
                  <span>{selectedTask.exitCode}</span>
                </DetailField>
              )}
              {selectedTask?.startedAt && (
                <DetailField label={t.dagDetail.started}>
                  <span>{selectedTask.startedAt}</span>
                </DetailField>
              )}
              {selectedTask?.finishedAt && (
                <DetailField label={t.dagDetail.finished}>
                  <span>{selectedTask.finishedAt}</span>
                </DetailField>
              )}
              {selectedTask?.duration && (
                <DetailField label={t.dagDetail.executionTime}>
                  <span>{selectedTask.duration}</span>
                </DetailField>
              )}
            </div>

            {selectedTask?.dependsOn && selectedTask.dependsOn.length > 0 && (
              <DetailField label={t.dagDetail.dependsOn}>
                <div className="flex flex-wrap gap-2">
                  {selectedTask.dependsOn.map((dep) => (
                    <Badge key={dep} variant="secondary">{dep}</Badge>
                  ))}
                </div>
              </DetailField>
            )}

            {selectedTask?.executor === "sensor" && selectedTask?.sensorTarget && (
              <DetailField label={t.dagDetail.sensorTarget}>
                <div className="font-mono text-[12px] break-all text-muted-foreground">
                  {selectedTask.sensorTarget}
                </div>
              </DetailField>
            )}

            {selectedTask?.command && (
              <div className="overflow-hidden rounded-2xl border border-border/70 bg-muted/25 shadow-[0_18px_40px_-24px_rgba(15,23,42,0.18)] dark:border-slate-800/80 dark:bg-slate-950 dark:text-slate-100 dark:shadow-[0_18px_40px_-24px_rgba(15,23,42,0.85)]">
                <div className="flex items-center justify-between border-b border-border/70 bg-background/70 px-4 py-2.5 backdrop-blur dark:border-slate-800/90 dark:bg-slate-900/80">
                  <div className="flex min-w-0 items-center gap-3">
                    <div className="flex items-center gap-1.5">
                      <span className="h-2.5 w-2.5 rounded-full bg-rose-400/90" />
                      <span className="h-2.5 w-2.5 rounded-full bg-amber-300/90" />
                      <span className="h-2.5 w-2.5 rounded-full bg-emerald-400/90" />
                    </div>
                    <div className="min-w-0">
                      <div className="truncate text-xs font-medium text-foreground dark:text-slate-200">
                        {t.dagDetail.command}
                      </div>
                      <div className="truncate text-[11px] text-muted-foreground dark:text-slate-400">
                        {selectedTask.taskId || selectedTask.name}
                      </div>
                    </div>
                  </div>
                  <Button
                    type="button"
                    size="sm"
                    variant="ghost"
                    onClick={handleCopyCommand}
                    className="h-8 shrink-0 gap-1.5 rounded-lg px-2.5 text-foreground/80 hover:bg-accent hover:text-foreground dark:text-slate-200 dark:hover:bg-slate-800 dark:hover:text-white"
                  >
                    {copiedCommand ? <Check className="h-3.5 w-3.5" /> : <Copy className="h-3.5 w-3.5" />}
                    {copiedCommand ? t.toast.commandCopied : t.common.copy}
                  </Button>
                </div>
                <div className="grid grid-cols-[auto,1fr] gap-3 px-4 py-4">
                  <div className="pt-0.5 font-mono text-xs font-semibold text-emerald-600 dark:text-emerald-400">$</div>
                  <pre className="max-h-72 overflow-auto whitespace-pre-wrap break-words font-mono text-[12.5px] leading-6 text-slate-800 dark:text-slate-100">
                    <code>{selectedTask.command}</code>
                  </pre>
                </div>
              </div>
            )}

            {selectedTask?.error && (
              <DetailField label={t.dagDetail.error}>
                <pre className="max-h-56 overflow-auto whitespace-pre-wrap break-words rounded-xl bg-destructive/8 px-3 py-3 font-mono text-[12px] leading-6 text-destructive">
                  {selectedTask.error}
                </pre>
              </DetailField>
            )}
          </div>
        </DialogContent>
      </Dialog>
    </>
  );
}
