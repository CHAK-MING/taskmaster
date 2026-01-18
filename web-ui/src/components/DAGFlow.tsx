import { useCallback, useMemo, useState } from "react";
import ReactFlow, {
  Node,
  Edge,
  Background,
  Controls,
  MiniMap,
  useNodesState,
  useEdgesState,
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

type FlowTaskStatus = "success" | "failed" | "running" | "pending" | "skipped";

interface Task {
  id: string;
  name: string;
  status: FlowTaskStatus;
  duration?: string;
  executor?: string;
  dependsOn?: string[];
  logs?: Array<{ timestamp: string; level: string; message: string }>;
}

interface DAGFlowProps {
  tasks: Task[];
  dependencies?: { from: string; to: string }[];
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
  running: {
    bg: "hsl(199 89% 48% / 0.15)",
    border: "hsl(199 89% 48%)",
    text: "hsl(199 89% 38%)",
  },
  pending: {
    bg: "hsl(38 92% 50% / 0.15)",
    border: "hsl(38 92% 50%)",
    text: "hsl(38 92% 35%)",
  },
  skipped: {
    bg: "hsl(215 16% 47% / 0.15)",
    border: "hsl(215 16% 47%)",
    text: "hsl(215 16% 37%)",
  },
};

const statusLabels: Record<FlowTaskStatus, string> = {
  success: "成功",
  failed: "失败",
  running: "运行中",
  pending: "未运行",
  skipped: "已跳过",
};

export function DAGFlow({ tasks, dependencies, className, onTaskClick }: DAGFlowProps) {
  const [selectedTask, setSelectedTask] = useState<Task | null>(null);

  const initialNodes: Node[] = useMemo(() => {
    const nodeWidth = 180;
    const nodeHeight = 70;
    const horizontalGap = 80;
    const verticalGap = 100;
    const nodesPerRow = 3;

    return tasks.map((task, index) => {
      const row = Math.floor(index / nodesPerRow);
      const col = index % nodesPerRow;
      const colors = statusColors[task.status];
      const isRunning = task.status === "running";

      return {
        id: task.id,
        data: {
          label: (
            <div className="text-center cursor-pointer">
              <div className="font-medium text-sm mb-1">{task.name}</div>
              <div
                className="text-xs px-2 py-0.5 rounded-full inline-block"
                style={{
                  backgroundColor: colors.bg,
                  color: colors.text,
                }}
              >
                {statusLabels[task.status]}
              </div>
              {task.duration && (
                <div className="text-xs text-muted-foreground mt-1">
                  {task.duration}
                </div>
              )}
            </div>
          ),
          task,
        },
        position: {
          x: col * (nodeWidth + horizontalGap) + 50,
          y: row * (nodeHeight + verticalGap) + 50,
        },
        style: {
          background: "hsl(var(--card))",
          border: `2px solid ${colors.border}`,
          borderRadius: "12px",
          padding: "12px 16px",
          width: nodeWidth,
          boxShadow: isRunning
            ? `0 0 0 4px ${colors.border}33, 0 4px 12px ${colors.border}33`
            : `0 4px 12px ${colors.border}33`,
          animation: isRunning ? "node-pulse 2s ease-in-out infinite" : undefined,
        },
        sourcePosition: Position.Right,
        targetPosition: Position.Left,
      };
    });
  }, [tasks]);

  const initialEdges: Edge[] = useMemo(() => {
    if (!dependencies) {
      return tasks.slice(0, -1).map((task, index) => ({
        id: `e${task.id}-${tasks[index + 1].id}`,
        source: task.id,
        target: tasks[index + 1].id,
        type: "smoothstep",
        animated: tasks[index + 1].status === "running",
        style: {
          stroke: "hsl(var(--primary))",
          strokeWidth: 2,
        },
        markerEnd: {
          type: MarkerType.ArrowClosed,
          color: "hsl(var(--primary))",
        },
      }));
    }

    return dependencies.map((dep) => {
      const targetTask = tasks.find((t) => t.id === dep.to);
      return {
        id: `e${dep.from}-${dep.to}`,
        source: dep.from,
        target: dep.to,
        type: "smoothstep",
        animated: targetTask?.status === "running",
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
  }, [tasks, dependencies]);

  const [nodes, setNodes, onNodesChange] = useNodesState(initialNodes);
  const [edges, setEdges, onEdgesChange] = useEdgesState(initialEdges);

  const handleNodeClick = useCallback((_: React.MouseEvent, node: Node) => {
    const task = node.data.task as Task;
    if (onTaskClick) {
      onTaskClick(task);
    } else {
      setSelectedTask(task);
    }
  }, [onTaskClick]);

  return (
    <>
      <div className={cn("h-[400px] w-full rounded-lg border border-border bg-card", className)}>
        <ReactFlow
          nodes={nodes}
          edges={edges}
          onNodesChange={onNodesChange}
          onEdgesChange={onEdgesChange}
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

      <Dialog open={!!selectedTask} onOpenChange={(open) => !open && setSelectedTask(null)}>
        <DialogContent className="sm:max-w-lg">
          <DialogHeader>
            <DialogTitle className="flex items-center gap-2">
              {selectedTask?.name}
              <Badge
                variant="outline"
                className="ml-2"
                style={{
                  backgroundColor: selectedTask ? statusColors[selectedTask.status].bg : undefined,
                  color: selectedTask ? statusColors[selectedTask.status].text : undefined,
                  borderColor: selectedTask ? statusColors[selectedTask.status].border : undefined,
                }}
              >
                {selectedTask && statusLabels[selectedTask.status]}
              </Badge>
            </DialogTitle>
            <DialogDescription>
              任务详情信息
            </DialogDescription>
          </DialogHeader>
          <div className="space-y-4 py-4">
            {selectedTask?.executor && (
              <div className="flex items-center gap-2">
                <span className="text-sm text-muted-foreground">执行器:</span>
                <Badge variant="secondary">{selectedTask.executor}</Badge>
              </div>
            )}
            {selectedTask?.dependsOn && selectedTask.dependsOn.length > 0 && (
              <div className="flex items-center gap-2 flex-wrap">
                <span className="text-sm text-muted-foreground">依赖任务:</span>
                {selectedTask.dependsOn.map((dep) => (
                  <Badge key={dep} variant="secondary">{dep}</Badge>
                ))}
              </div>
            )}
            {selectedTask?.duration && (
              <div className="flex items-center gap-2">
                <span className="text-sm text-muted-foreground">执行时间:</span>
                <span className="text-sm">{selectedTask.duration}</span>
              </div>
            )}
            {selectedTask?.logs && selectedTask.logs.length > 0 && (
              <div className="space-y-2">
                <span className="text-sm font-medium">运行日志:</span>
                <div className="bg-terminal rounded-lg p-3 max-h-48 overflow-y-auto">
                  <div className="font-mono text-xs space-y-1">
                    {selectedTask.logs.map((log, i) => (
                      <p key={i}>
                        <span className="text-muted-foreground">[{log.timestamp}]</span>
                        <span className={`ml-1 ${log.level === "ERROR" ? "text-destructive" : log.level === "WARN" ? "text-warning" : "text-primary"}`}>
                          [{log.level}]
                        </span>
                        <span className="text-terminal-foreground ml-1">{log.message}</span>
                      </p>
                    ))}
                  </div>
                </div>
              </div>
            )}
          </div>
        </DialogContent>
      </Dialog>
    </>
  );
}
