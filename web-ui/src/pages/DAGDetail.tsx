import { useState, useEffect, useCallback } from "react";
import { AppLayout } from "@/components/AppLayout";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { DAGFlow } from "@/components/DAGFlow";
import {
    Select,
    SelectContent,
    SelectItem,
    SelectTrigger,
    SelectValue,
} from "@/components/ui/select";
import {
    Play,
    ArrowLeft,
    Calendar,
    Loader2,
    Clock,
    Hand,
} from "lucide-react";
import { useNavigate, useParams } from "react-router-dom";
import { toast } from "sonner";
import { LogEntry } from "@/types/dag";
import {
    getDAG,
    getTask,
    triggerDAG,
    listHistory,
    getTaskLogs,
    DAGInfo,
    TaskConfig,
    RunRecord,
} from "@/lib/api";
import {
    DAGStatusBadge,
    DAGStatusIcon,
    executorIcons,
    executorLabels,
    formatTime,
} from "@/lib/status";

interface TaskDefinition extends TaskConfig {
    dagId: string;
}

interface DAGRun {
    id: string;
    dagId: string;
    runNumber: number;
    state: 'running' | 'success' | 'failed';
    triggerType: 'schedule' | 'manual' | 'api';
    startTime: string;
    endTime?: string;
    tasksTotal: number;
}

export default function DAGDetail() {
    const { id } = useParams();
    const navigate = useNavigate();

    const [dagInfo, setDagInfo] = useState<DAGInfo | null>(null);
    const [taskDefinitions, setTaskDefinitions] = useState<TaskDefinition[]>([]);
    const [dagRuns, setDagRuns] = useState<DAGRun[]>([]);
    const [selectedRun, setSelectedRun] = useState<DAGRun | null>(null);
    const [runLogs, setRunLogs] = useState<Map<string, LogEntry[]>>(new Map());
    const [triggering, setTriggering] = useState(false);
    const [loadingLogs, setLoadingLogs] = useState(false);

    const fetchDAGData = useCallback(async () => {
        if (!id) return;
        try {
            const data = await getDAG(id);
            setDagInfo(data);

            const taskDefs: TaskDefinition[] = [];
            for (const taskId of data.tasks || []) {
                try {
                    const taskData = await getTask(id, taskId);
                    taskDefs.push({ ...taskData, dagId: id });
                } catch {
                    taskDefs.push({
                        task_id: taskId,
                        name: taskId,
                        command: "",
                        dependencies: [],
                        dagId: id,
                    });
                }
            }
            setTaskDefinitions(taskDefs);
        } catch (error) {
            toast.error("获取 DAG 详情失败", {
                description: error instanceof Error ? error.message : "未知错误",
            });
        }
    }, [id]);

    const fetchRunHistory = useCallback(async () => {
        if (!id) return;
        try {
            const history = await listHistory();
            const dagHistory = history.filter((r: RunRecord) => r.dag_id === id);
            const taskCount = taskDefinitions.length || dagInfo?.tasks?.length || 0;
            const runs: DAGRun[] = dagHistory.map((record: RunRecord, index: number) => ({
                id: record.dag_run_id,
                dagId: record.dag_id,
                runNumber: dagHistory.length - index,
                state: record.state,
                triggerType: record.trigger_type || 'manual',
                startTime: record.started_at,
                endTime: record.finished_at,
                tasksTotal: taskCount,
            }));
            setDagRuns(runs);
            if (runs.length > 0 && !selectedRun) {
                setSelectedRun(runs[0]);
            }
        } catch {
            // ignore
        }
    }, [id, selectedRun, taskDefinitions.length, dagInfo?.tasks?.length]);

    const fetchLogsForRun = useCallback(async (runId: string) => {
        if (taskDefinitions.length === 0) return;
        if (runLogs.has(runId)) return;

        setLoadingLogs(true);
        const allLogs: LogEntry[] = [];
        for (const taskDef of taskDefinitions) {
            try {
                const logsData = await getTaskLogs(runId, taskDef.task_id);
                logsData.forEach((log) => {
                    const timestamp = typeof log.timestamp === 'number'
                        ? new Date(log.timestamp).toISOString().replace('T', ' ').slice(0, 19)
                        : String(log.timestamp);
                    allLogs.push({
                        timestamp,
                        level: log.level.toUpperCase() as LogEntry["level"],
                        message: `[${taskDef.task_id}] ${log.message}`,
                        taskId: taskDef.task_id,
                    });
                });
            } catch {
                // ignore
            }
        }
        allLogs.sort((a, b) => a.timestamp.localeCompare(b.timestamp));
        setRunLogs(prev => new Map(prev).set(runId, allLogs));
        setLoadingLogs(false);
    }, [taskDefinitions, runLogs]);

    useEffect(() => {
        fetchDAGData();
    }, [fetchDAGData]);

    useEffect(() => {
        if (taskDefinitions.length > 0) {
            fetchRunHistory();
        }
    }, [taskDefinitions, fetchRunHistory]);

    useEffect(() => {
        if (selectedRun && taskDefinitions.length > 0) {
            fetchLogsForRun(selectedRun.id);
        }
    }, [selectedRun, taskDefinitions, fetchLogsForRun]);

    const taskDependencies = taskDefinitions.flatMap((task) =>
        task.dependencies.map((dep) => ({ from: dep, to: task.task_id }))
    );

    const handleTriggerDAG = async () => {
        if (triggering || !dagInfo) return;

        if (taskDefinitions.length === 0) {
            toast.warning("无法触发 DAG", { description: "当前 DAG 没有任务" });
            return;
        }

        setTriggering(true);
        try {
            await triggerDAG(dagInfo.dag_id);
            toast.success("DAG 已触发", { description: "新的运行实例已创建" });
            await fetchRunHistory();
        } catch (error) {
            toast.error("触发 DAG 失败", {
                description: error instanceof Error ? error.message : "未知错误",
            });
        } finally {
            setTriggering(false);
        }
    };

    const flowTasks = taskDefinitions.map((td) => {
        let status: "pending" | "running" | "success" | "failed" = "pending";
        if (selectedRun) {
            if (selectedRun.state === "success") status = "success";
            else if (selectedRun.state === "failed") status = "failed";
            else if (selectedRun.state === "running") status = "running";
        }
        return {
            id: td.task_id,
            name: td.name,
            status,
            duration: undefined,
            executor: "shell" as const,
            logs: [],
        };
    });

    const getLogLevelClass = (level: string) => {
        switch (level) {
            case "ERROR": return "text-destructive";
            case "WARN": return "text-warning";
            case "DEBUG": return "text-muted-foreground";
            default: return "text-foreground";
        }
    };

    const getTriggerLabel = (type: string) => {
        switch (type) {
            case "schedule": return "定时";
            case "manual": return "手动";
            default: return "API";
        }
    };

    const getTriggerIcon = (type: string) => {
        return type === "schedule" ? Clock : Hand;
    };

    const currentLogs = selectedRun ? runLogs.get(selectedRun.id) || [] : [];

    if (!dagInfo) {
        return (
            <AppLayout>
                <div className="flex items-center justify-center h-64">
                    <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
                </div>
            </AppLayout>
        );
    }

    return (
        <AppLayout>
            <div className="mb-6">
                <Button variant="ghost" onClick={() => navigate("/dags")} className="mb-4">
                    <ArrowLeft className="mr-2 h-4 w-4" />
                    返回 DAGs
                </Button>

                <div className="flex items-start justify-between">
                    <div>
                        <h1 className="text-2xl font-bold">{dagInfo.name}</h1>
                        <p className="text-muted-foreground mt-1">{dagInfo.description}</p>
                        {dagInfo.cron && (
                            <div className="flex items-center gap-1.5 text-sm text-muted-foreground mt-2">
                                <Calendar className="h-4 w-4" />
                                <span className="font-mono">{dagInfo.cron}</span>
                            </div>
                        )}
                    </div>
                    <Button onClick={handleTriggerDAG} disabled={triggering}>
                        {triggering ? (
                            <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                        ) : (
                            <Play className="mr-2 h-4 w-4" />
                        )}
                        {triggering ? "触发中..." : "触发运行"}
                    </Button>
                </div>
            </div>

            <Tabs defaultValue="tasks" className="space-y-6">
                <TabsList>
                    <TabsTrigger value="tasks">任务定义</TabsTrigger>
                    <TabsTrigger value="graph">流程图</TabsTrigger>
                    <TabsTrigger value="runs">运行实例</TabsTrigger>
                    <TabsTrigger value="logs">运行日志</TabsTrigger>
                </TabsList>

                <TabsContent value="runs">
                    <Card>
                        <CardHeader>
                            <CardTitle className="text-base">运行实例列表</CardTitle>
                        </CardHeader>
                        <CardContent>
                            {dagRuns.length > 0 ? (
                                <div className="space-y-3">
                                    {dagRuns.map((run) => {
                                        const TriggerIcon = getTriggerIcon(run.triggerType);
                                        return (
                                            <div
                                                key={run.id}
                                                className={`flex items-center justify-between p-4 rounded-lg border transition-colors cursor-pointer ${selectedRun?.id === run.id ? "border-primary bg-primary/5" : "border-border hover:bg-accent/30"}`}
                                                onClick={() => setSelectedRun(run)}
                                            >
                                                <div className="flex items-center gap-4">
                                                    <DAGStatusIcon status={run.state} />
                                                    <div>
                                                        <div className="flex items-center gap-2">
                                                            <span className="font-medium">Run #{run.runNumber}</span>
                                                            <DAGStatusBadge status={run.state} />
                                                            <Badge variant="outline" className="text-xs gap-1">
                                                                <TriggerIcon className="h-3 w-3" />
                                                                {getTriggerLabel(run.triggerType)}
                                                            </Badge>
                                                        </div>
                                                        <p className="text-sm text-muted-foreground mt-1">
                                                            开始: {formatTime(run.startTime)}
                                                            {run.endTime && ` • 结束: ${formatTime(run.endTime)}`}
                                                        </p>
                                                    </div>
                                                </div>
                                                <div className="text-sm text-muted-foreground">
                                                    {run.tasksTotal} 个任务
                                                </div>
                                            </div>
                                        );
                                    })}
                                </div>
                            ) : (
                                <div className="text-center py-8 text-muted-foreground">
                                    暂无运行实例，点击"触发运行"开始执行
                                </div>
                            )}
                        </CardContent>
                    </Card>
                </TabsContent>

                <TabsContent value="graph">
                    <Card>
                        <CardHeader>
                            <div className="flex items-center justify-between">
                                <div>
                                    <CardTitle className="text-base">任务依赖关系图</CardTitle>
                                    <p className="text-sm text-muted-foreground mt-1">
                                        {selectedRun ? `显示 Run #${selectedRun.runNumber} 的状态` : "选择运行实例查看状态"}
                                    </p>
                                </div>
                                {dagRuns.length > 0 && (
                                    <Select
                                        value={selectedRun?.id || ""}
                                        onValueChange={(val) => {
                                            const run = dagRuns.find(r => r.id === val);
                                            if (run) setSelectedRun(run);
                                        }}
                                    >
                                        <SelectTrigger className="w-48">
                                            <SelectValue placeholder="选择运行实例" />
                                        </SelectTrigger>
                                        <SelectContent>
                                            {dagRuns.map((run) => (
                                                <SelectItem key={run.id} value={run.id}>
                                                    Run #{run.runNumber} - {run.state === "success" ? "成功" : run.state === "failed" ? "失败" : "运行中"}
                                                </SelectItem>
                                            ))}
                                        </SelectContent>
                                    </Select>
                                )}
                            </div>
                        </CardHeader>
                        <CardContent className="p-0">
                            <DAGFlow
                                tasks={flowTasks}
                                dependencies={taskDependencies}
                                className="border-0 rounded-t-none"
                            />
                        </CardContent>
                    </Card>
                </TabsContent>

                <TabsContent value="logs">
                    <Card>
                        <CardHeader>
                            <div className="flex items-center justify-between">
                                <CardTitle className="text-base">运行日志</CardTitle>
                                {dagRuns.length > 0 && (
                                    <Select
                                        value={selectedRun?.id || ""}
                                        onValueChange={(val) => {
                                            const run = dagRuns.find(r => r.id === val);
                                            if (run) setSelectedRun(run);
                                        }}
                                    >
                                        <SelectTrigger className="w-48">
                                            <SelectValue placeholder="选择运行实例" />
                                        </SelectTrigger>
                                        <SelectContent>
                                            {dagRuns.map((run) => (
                                                <SelectItem key={run.id} value={run.id}>
                                                    Run #{run.runNumber} - {run.state === "success" ? "成功" : run.state === "failed" ? "失败" : "运行中"}
                                                </SelectItem>
                                            ))}
                                        </SelectContent>
                                    </Select>
                                )}
                            </div>
                        </CardHeader>
                        <CardContent>
                            {!selectedRun ? (
                                <div className="text-center py-8 text-muted-foreground">
                                    请选择一个运行实例查看日志
                                </div>
                            ) : loadingLogs ? (
                                <div className="flex items-center justify-center py-8">
                                    <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
                                </div>
                            ) : currentLogs.length > 0 ? (
                                <div className="bg-muted/50 rounded-lg p-4 max-h-[500px] overflow-y-auto">
                                    <div className="font-mono text-xs space-y-1">
                                        {currentLogs.map((log, i) => (
                                            <p key={i}>
                                                <span className="text-muted-foreground">[{log.timestamp}]</span>
                                                <span className={`ml-1 font-semibold ${getLogLevelClass(log.level)}`}>[{log.level}]</span>
                                                <span className="ml-1">{log.message}</span>
                                            </p>
                                        ))}
                                    </div>
                                </div>
                            ) : (
                                <div className="text-center py-8 text-muted-foreground">
                                    该运行实例暂无日志
                                </div>
                            )}
                        </CardContent>
                    </Card>
                </TabsContent>

                <TabsContent value="tasks">
                    <Card>
                        <CardHeader>
                            <CardTitle className="text-base">任务定义</CardTitle>
                        </CardHeader>
                        <CardContent>
                            <div className="space-y-3">
                                {taskDefinitions.map((task, index) => {
                                    const ExecIcon = executorIcons["shell"];
                                    const dependsTasks = task.dependencies
                                        .map((d) => taskDefinitions.find((t) => t.task_id === d)?.name)
                                        .filter(Boolean);

                                    return (
                                        <div
                                            key={task.task_id}
                                            className="flex items-center justify-between p-4 rounded-lg border border-border bg-card"
                                        >
                                            <div className="flex items-center gap-4">
                                                <span className="text-sm text-muted-foreground w-6">{index + 1}</span>
                                                <div>
                                                    <span className="font-medium">{task.name}</span>
                                                    <div className="flex items-center gap-2 mt-1 flex-wrap">
                                                        <Badge variant="secondary" className="text-xs">
                                                            <ExecIcon className="mr-1 h-3 w-3" />
                                                            {executorLabels["shell"]}
                                                        </Badge>
                                                        {dependsTasks.length > 0 && (
                                                            <Badge variant="outline" className="text-xs">
                                                                依赖: {dependsTasks.join(", ")}
                                                            </Badge>
                                                        )}
                                                    </div>
                                                </div>
                                            </div>
                                        </div>
                                    );
                                })}
                                {taskDefinitions.length === 0 && (
                                    <div className="text-center py-8 text-muted-foreground">
                                        暂无任务
                                    </div>
                                )}
                            </div>
                        </CardContent>
                    </Card>
                </TabsContent>
            </Tabs>
        </AppLayout>
    );
}
