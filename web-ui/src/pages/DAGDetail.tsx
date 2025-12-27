import { useState, useEffect, useCallback } from "react";
import { AppLayout } from "@/components/AppLayout";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { DAGFlow } from "@/components/DAGFlow";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Textarea } from "@/components/ui/textarea";
import {
    Select,
    SelectContent,
    SelectItem,
    SelectTrigger,
    SelectValue,
} from "@/components/ui/select";
import {
    Dialog,
    DialogContent,
    DialogDescription,
    DialogFooter,
    DialogHeader,
    DialogTitle,
    DialogTrigger,
} from "@/components/ui/dialog";
import {
    AlertDialog,
    AlertDialogAction,
    AlertDialogCancel,
    AlertDialogContent,
    AlertDialogDescription,
    AlertDialogFooter,
    AlertDialogHeader,
    AlertDialogTitle,
} from "@/components/ui/alert-dialog";
import {
    DropdownMenu,
    DropdownMenuContent,
    DropdownMenuItem,
    DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import {
    Tooltip,
    TooltipContent,
    TooltipTrigger,
} from "@/components/ui/tooltip";
import {
    Play,
    RotateCcw,
    Settings,
    ArrowLeft,
    Plus,
    Trash2,
    MoreVertical,
    FileText,
    Pencil,
    Calendar,
    Loader2,
} from "lucide-react";
import { useNavigate, useParams } from "react-router-dom";
import { toast } from "sonner";
import {
    DAGDefinition,
    DAGRun,
    TaskDefinition,
    TaskInstance,
    TaskInstanceStatus,
    taskStatusLabels,
    ExecutorType,
    LogEntry,
} from "@/types/dag";
import {
    getDAG,
    createTask,
    updateTask,
    deleteTask,
    triggerDAG,
    listHistory,
    getRunDetail,
    updateDAG,
    getRunLogs,
    getTaskLogs,
    TaskConfig,
} from "@/lib/api";
import {
    DAGStatusBadge,
    TaskStatusBadge,
    DAGStatusIcon,
    TaskStatusIcon,
    executorIcons,
    executorLabels,
    formatTime,
} from "@/lib/status";
import { wsManager } from "@/lib/websocket";

const taskConfigToDefinition = (task: TaskConfig, dagId: string): TaskDefinition => ({
    id: task.id,
    dagId: dagId,
    name: task.name,
    executor: task.executor as ExecutorType,
    command: task.command,
    workingDir: task.working_dir,
    dependsOn: task.deps,
    retryCount: task.max_retries,
    retryInterval: task.retry_interval,
    timeout: task.timeout,
});

export default function DAGDetail() {
    const { id } = useParams();
    const navigate = useNavigate();

    const [dagDefinition, setDagDefinition] = useState<DAGDefinition>({
        id: id || "",
        name: "",
        description: "",
        cron: "",
        maxConcurrentRuns: 1,
        createdAt: "",
        updatedAt: "",
        isActive: true,
    });
    const [taskDefinitions, setTaskDefinitions] = useState<TaskDefinition[]>([]);
    const [taskInstances, setTaskInstances] = useState<TaskInstance[]>([]);
    const [dagRuns, setDagRuns] = useState<DAGRun[]>([]);
    const [selectedRun, setSelectedRun] = useState<DAGRun | null>(null);
    const [dagLogs, setDagLogs] = useState<LogEntry[]>([]);
    const [taskLogs, setTaskLogs] = useState<Record<string, Record<number, LogEntry[]>>>({});

    const [isDagSettingsOpen, setIsDagSettingsOpen] = useState(false);
    const [isAddTaskOpen, setIsAddTaskOpen] = useState(false);
    const [isEditTaskOpen, setIsEditTaskOpen] = useState(false);
    const [isDeleteTaskOpen, setIsDeleteTaskOpen] = useState(false);
    const [taskToDelete, setTaskToDelete] = useState<TaskDefinition | null>(null);
    const [selectedTaskForLogs, setSelectedTaskForLogs] = useState<TaskInstance | null>(null);
    const [selectedAttempt, setSelectedAttempt] = useState<number>(1);
    const [editingTask, setEditingTask] = useState<TaskDefinition | null>(null);

    const [newTask, setNewTask] = useState({
        name: "",
        executor: "shell" as ExecutorType,
        command: "",
        workingDir: "",
        dependsOn: [] as string[],
        retryCount: 3,
        retryInterval: 60,
        timeout: 3600,
    });

    const fetchTaskInstances = useCallback(async (runId: string) => {
        try {
            const detail = await getRunDetail(runId);
            const instances: TaskInstance[] = detail.task_runs.map((tr) => {
                const taskId = tr.task_id.replace(`${runId}_`, '');
                const taskDef = taskDefinitions.find(t => t.id === taskId);
                
                let duration: string | undefined;
                if (tr.end_time && tr.start_time && tr.end_time !== "-" && tr.start_time !== "-") {
                    try {
                        const endMs = new Date(tr.end_time).getTime();
                        const startMs = new Date(tr.start_time).getTime();
                        if (!isNaN(endMs) && !isNaN(startMs)) {
                            duration = `${((endMs - startMs) / 1000).toFixed(1)}s`;
                        }
                    } catch {
                        duration = undefined;
                    }
                }
                
                return {
                    id: `${runId}_${taskId}`,
                    dagRunId: runId,
                    taskDefinitionId: taskId,
                    name: taskDef?.name || taskId,
                    executor: (taskDef?.executor || "shell") as ExecutorType,
                    status: tr.status as TaskInstanceStatus,
                    attempt: tr.attempt,
                    startTime: tr.start_time,
                    endTime: tr.end_time,
                    duration,
                    exitCode: tr.exit_code,
                    error: tr.error,
                    dependsOn: taskDef?.dependsOn || [],
                };
            });
            setTaskInstances(instances);
        } catch {
            // Ignore fetch errors
        }
    }, [taskDefinitions]);

    const fetchDagLogs = useCallback(async (runId: string) => {
        try {
            const logsData = await getRunLogs(runId);
            const logs: LogEntry[] = logsData.map((log) => {
                const timestamp = typeof log.timestamp === 'number' 
                    ? new Date(log.timestamp).toISOString().replace('T', ' ').slice(0, 19)
                    : log.timestamp;
                return {
                    timestamp,
                    level: log.level.toUpperCase() as LogEntry["level"],
                    message: `[${log.task_id}] ${log.message}`,
                    attempt: log.attempt,
                    taskId: log.task_id,
                };
            });
            setDagLogs(logs);
        } catch {
            // Ignore fetch errors
        }
    }, []);

    const fetchDAGData = useCallback(async () => {
        if (!id) return;
        try {
            const data = await getDAG(id);
            setDagDefinition({
                id: data.id,
                name: data.name,
                description: data.description,
                cron: data.cron,
                maxConcurrentRuns: data.max_concurrent_runs,
                createdAt: data.created_at,
                updatedAt: data.updated_at,
                isActive: data.is_active,
            });
            setTaskDefinitions(data.tasks.map((t) => taskConfigToDefinition(t, data.id)));
        } catch (error) {
            toast.error("获取 DAG 详情失败", {
                description: error instanceof Error ? error.message : "未知错误",
            });
        }
    }, [id]);

    const fetchRunHistory = useCallback(async () => {
        if (!id) return;
        try {
            const history = await listHistory(id);
            const mapStatus = (status: string): DAGRun["status"] => {
                if (status === "queued") return "running";
                if (status === "running" || status === "success" || status === "failed") {
                    return status as DAGRun["status"];
                }
                return "running";
            };
            const runs: DAGRun[] = history.map((record, index) => ({
                id: record.run_id,
                dagId: record.dag_id,
                runNumber: history.length - index,
                status: mapStatus(record.status),
                triggerType: "manual" as const,
                startTime: record.start_time,
                endTime: record.end_time,
                duration: record.duration_ms > 0 ? `${(record.duration_ms / 1000).toFixed(1)}s` : undefined,
                tasksTotal: record.total_tasks,
                tasksCompleted: record.completed_tasks,
                tasksFailed: record.failed_tasks,
            }));
            setDagRuns(runs);
            if (runs.length > 0 && !selectedRun) {
                setSelectedRun(runs[0]);
            }
        } catch {
            // Ignore fetch errors
        }
    }, [id, selectedRun]);

    // WebSocket real-time updates
    useEffect(() => {
        if (!id) return;
        
        wsManager.connect();
        
        const unsubscribeTaskStatus = wsManager.on('task_status_changed', (data) => {
            if ('task_id' in data && data.dag_id === id && selectedRun && data.run_id === selectedRun.id) {
                fetchTaskInstances(selectedRun.id);
                fetchDagLogs(selectedRun.id);
            }
        });
        
        const unsubscribeDAGComplete = wsManager.on('dag_run_completed', (data) => {
            if (data.dag_id === id) {
                fetchRunHistory();
                if (selectedRun && data.run_id === selectedRun.id) {
                    fetchTaskInstances(selectedRun.id);
                    fetchDagLogs(selectedRun.id);
                }
            }
        });
        
        return () => {
            unsubscribeTaskStatus();
            unsubscribeDAGComplete();
        };
    }, [id, selectedRun, fetchRunHistory, fetchTaskInstances, fetchDagLogs]);

    // Initial fetch
    useEffect(() => {
        fetchDAGData();
        fetchRunHistory();
    }, [fetchDAGData, fetchRunHistory]);

    useEffect(() => {
        if (selectedRun && dagRuns.length > 0) {
            const updatedRun = dagRuns.find(r => r.id === selectedRun.id);
            if (updatedRun && updatedRun.status !== selectedRun.status) {
                setSelectedRun(updatedRun);
            }
        }
    }, [dagRuns, selectedRun]);

    const taskDependencies = taskDefinitions.flatMap((task) =>
        task.dependsOn.map((dep) => ({ from: dep, to: task.id }))
    );

    const handleAddTask = async () => {
        if (!newTask.name.trim() || !newTask.command.trim()) {
            toast.error("请填写任务名称和命令");
            return;
        }
        
        const taskId = newTask.name.toLowerCase().replace(/\s+/g, "_");
        if (taskDefinitions.some(t => t.id === taskId)) {
            toast.error("任务已存在", { description: `任务 ID "${taskId}" 已被使用` });
            return;
        }
        
        try {
            await createTask(dagDefinition.id, {
                id: taskId,
                name: newTask.name,
                command: newTask.command,
                executor: newTask.executor,
                working_dir: newTask.workingDir,
                deps: newTask.dependsOn,
                timeout: newTask.timeout,
                retry_interval: newTask.retryInterval,
                max_retries: newTask.retryCount,
                enabled: true,
            });
            setNewTask({ name: "", executor: "shell", command: "", workingDir: "", dependsOn: [], retryCount: 3, retryInterval: 60, timeout: 3600 });
            setIsAddTaskOpen(false);
            toast.success("任务已添加");
            fetchDAGData();
        } catch (error) {
            toast.error("添加任务失败", {
                description: error instanceof Error ? error.message : "未知错误",
            });
        }
    };

    const handleEditTask = async () => {
        if (!editingTask) return;
        try {
            await updateTask(dagDefinition.id, editingTask.id, {
                id: editingTask.id,
                name: editingTask.name,
                command: editingTask.command,
                executor: editingTask.executor,
                working_dir: editingTask.workingDir || "",
                deps: editingTask.dependsOn,
                timeout: editingTask.timeout,
                retry_interval: editingTask.retryInterval,
                max_retries: editingTask.retryCount,
                enabled: true,
            });
            setIsEditTaskOpen(false);
            setEditingTask(null);
            toast.success("任务已更新");
            fetchDAGData();
        } catch (error) {
            toast.error("更新任务失败", {
                description: error instanceof Error ? error.message : "未知错误",
            });
        }
    };

    const handleDeleteTask = async (taskId: string) => {
        try {
            await deleteTask(dagDefinition.id, taskId);
            // Update local state immediately
            setTaskDefinitions(prev => prev.filter(t => t.id !== taskId));
            toast.success("任务已删除");
        } catch (error) {
            toast.error("删除任务失败", {
                description: error instanceof Error ? error.message : "未知错误",
            });
        }
    };

    const handleClearState = (_instanceId: string) => {
        toast.error("清除任务状态", {
            description: "此功能需要后端 API 支持，目前尚未实现",
        });
    };

    const [triggering, setTriggering] = useState(false);

    const handleTriggerDAG = async () => {
        if (triggering) return;
        
        if (taskDefinitions.length === 0) {
            toast.warning("无法触发 DAG", { description: "当前 DAG 没有任务" });
            return;
        }
        
        setTriggering(true);
        try {
            await triggerDAG(dagDefinition.id);
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

    const handleSaveDAGSettings = async () => {
        try {
            await updateDAG(dagDefinition.id, {
                name: dagDefinition.name,
                description: dagDefinition.description,
                cron: dagDefinition.cron,
                max_concurrent_runs: dagDefinition.maxConcurrentRuns,
                is_active: dagDefinition.isActive,
            });
            setIsDagSettingsOpen(false);
            toast.success("DAG 设置已保存");
            fetchDAGData();
        } catch (error) {
            toast.error("保存 DAG 设置失败", {
                description: error instanceof Error ? error.message : "未知错误",
            });
        }
    };

    // Fetch task instances and logs when a run is selected
    useEffect(() => {
        if (selectedRun) {
            fetchTaskInstances(selectedRun.id);
            fetchDagLogs(selectedRun.id);
        }
    }, [selectedRun, fetchTaskInstances, fetchDagLogs]);

    // Fetch task-specific logs when a task is selected for logs dialog
    useEffect(() => {
        if (selectedTaskForLogs && selectedRun) {
            const fetchTaskSpecificLogs = async () => {
                try {
                    const logsData = await getTaskLogs(selectedRun.id, selectedTaskForLogs.taskDefinitionId, selectedAttempt);
                    const logs: LogEntry[] = logsData.map((log) => {
                        const timestamp = typeof log.timestamp === 'number' 
                            ? new Date(log.timestamp).toISOString().replace('T', ' ').slice(0, 19)
                            : log.timestamp;
                        return {
                            timestamp,
                            level: log.level.toUpperCase() as LogEntry["level"],
                            message: log.message,
                            attempt: log.attempt,
                        };
                    });
                    setTaskLogs(prev => ({
                        ...prev,
                        [selectedTaskForLogs.id]: {
                            ...prev[selectedTaskForLogs.id],
                            [selectedAttempt]: logs,
                        }
                    }));
                } catch {
                    // Ignore fetch errors
                }
            };
            fetchTaskSpecificLogs();
        }
    }, [selectedTaskForLogs, selectedRun, selectedAttempt]);

    const flowTasks = taskInstances.length > 0
        ? taskInstances.map((ti) => ({
            id: ti.taskDefinitionId,
            name: ti.name,
            status: ti.status === "success" ? "success" as const :
                (ti.status === "failed" || ti.status === "upstream_failed") ? "failed" as const :
                    ti.status === "running" ? "running" as const : "pending" as const,
            duration: ti.duration,
            executor: ti.executor,
            logs: taskLogs[ti.id]?.[ti.attempt] || [],
        }))
        : taskDefinitions.map((td) => ({
            id: td.id,
            name: td.name,
            status: "pending" as const,
            duration: undefined,
            executor: td.executor,
            logs: [],
        }));

    const handleFlowTaskClick = (task: { id: string; name: string }) => {
        const instance = taskInstances.find((ti) => ti.taskDefinitionId === task.id);
        if (instance) {
            setSelectedTaskForLogs(instance);
            setSelectedAttempt(instance.attempt || 1);
        }
    };

    const getLogLevelClass = (level: string) => {
        switch (level) {
            case "ERROR": return "text-destructive";
            case "WARN": return "text-warning";
            case "DEBUG": return "text-muted-foreground";
            default: return "text-primary";
        }
    };

    return (
        <AppLayout>
            <div className="mb-6">
                <Button variant="ghost" onClick={() => navigate("/dags")} className="mb-4">
                    <ArrowLeft className="mr-2 h-4 w-4" />
                    返回 DAGs
                </Button>

                <div className="flex items-start justify-between">
                    <div>
                        <div className="flex items-center gap-3">
                            <h1 className="text-2xl font-bold">{dagDefinition.name || "新建 DAG"}</h1>
                            {selectedRun ? (
                                <DAGStatusBadge status={selectedRun.status} />
                            ) : dagRuns.length === 0 ? (
                                <DAGStatusBadge status="no_run" />
                            ) : null}
                        </div>
                        <p className="text-muted-foreground mt-1">{dagDefinition.description}</p>
                        {dagDefinition.cron && (
                            <div className="flex items-center gap-1.5 text-sm text-muted-foreground mt-2">
                                <Calendar className="h-4 w-4" />
                                <span className="font-mono">{dagDefinition.cron}</span>
                            </div>
                        )}
                    </div>
                    <div className="flex items-center gap-2">
                        <Button onClick={handleTriggerDAG} disabled={triggering}>
                            {triggering ? (
                                <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                            ) : (
                                <Play className="mr-2 h-4 w-4" />
                            )}
                            {triggering ? "触发中..." : "触发运行"}
                        </Button>
                        <Dialog open={isDagSettingsOpen} onOpenChange={setIsDagSettingsOpen}>
                            <DialogTrigger asChild>
                                <Button variant="outline" size="icon"><Settings className="h-4 w-4" /></Button>
                            </DialogTrigger>
                            <DialogContent className="sm:max-w-md">
                                <DialogHeader>
                                    <DialogTitle>DAG 设置</DialogTitle>
                                    <DialogDescription>编辑 DAG 的基本信息和调度规则</DialogDescription>
                                </DialogHeader>
                                <div className="grid gap-4 py-4">
                                    <div className="grid gap-2">
                                        <Label>DAG 名称</Label>
                                        <Input value={dagDefinition.name} onChange={(e) => setDagDefinition({ ...dagDefinition, name: e.target.value })} />
                                    </div>
                                    <div className="grid gap-2">
                                        <Label>描述</Label>
                                        <Textarea rows={2} value={dagDefinition.description} onChange={(e) => setDagDefinition({ ...dagDefinition, description: e.target.value })} />
                                    </div>
                                    <div className="grid gap-2">
                                        <Label>Cron 表达式（暂不支持自动调度）</Label>
                                        <Input placeholder="例如: 0 10 * * *" value={dagDefinition.cron || ""} onChange={(e) => setDagDefinition({ ...dagDefinition, cron: e.target.value })} disabled />
                                        <p className="text-xs text-muted-foreground">当前版本仅支持手动触发</p>
                                    </div>
                                    <div className="grid gap-2">
                                        <Label>最大并发运行数（暂未实现）</Label>
                                        <Input type="number" min={1} max={10} value={dagDefinition.maxConcurrentRuns} onChange={(e) => setDagDefinition({ ...dagDefinition, maxConcurrentRuns: parseInt(e.target.value) || 1 })} disabled />
                                        <p className="text-xs text-muted-foreground">此功能暂未实现，当前不限制并发运行数</p>
                                    </div>
                                    <div className="flex items-center justify-between">
                                        <div className="space-y-0.5">
                                            <Label>启用 DAG</Label>
                                            <p className="text-xs text-muted-foreground">禁用后将不会显示在活跃列表中</p>
                                        </div>
                                        <label className="relative inline-flex items-center cursor-pointer">
                                            <input
                                                type="checkbox"
                                                checked={dagDefinition.isActive}
                                                onChange={(e) => setDagDefinition({ ...dagDefinition, isActive: e.target.checked })}
                                                className="sr-only peer"
                                            />
                                            <div className="w-11 h-6 bg-muted peer-focus:outline-none peer-focus:ring-4 peer-focus:ring-primary/20 rounded-full peer peer-checked:after:translate-x-full rtl:peer-checked:after:-translate-x-full peer-checked:after:border-white after:content-[''] after:absolute after:top-[2px] after:start-[2px] after:bg-white after:border-gray-300 after:border after:rounded-full after:h-5 after:w-5 after:transition-all peer-checked:bg-primary"></div>
                                        </label>
                                    </div>
                                </div>
                                <DialogFooter>
                                    <Button variant="outline" onClick={() => setIsDagSettingsOpen(false)}>取消</Button>
                                    <Button onClick={handleSaveDAGSettings}>保存</Button>
                                </DialogFooter>
                            </DialogContent>
                        </Dialog>
                    </div>
                </div>
            </div>

            <Tabs defaultValue="tasks" className="space-y-6">
                <TabsList>
                    <TabsTrigger value="tasks">任务定义</TabsTrigger>
                    <TabsTrigger value="graph">流程图</TabsTrigger>
                    <TabsTrigger value="runs">运行实例</TabsTrigger>
                    <TabsTrigger value="logs">运行日志</TabsTrigger>
                </TabsList>

                {/* Task Definitions Tab */}
                <TabsContent value="tasks">
                    <Card>
                        <CardHeader className="flex flex-row items-center justify-between">
                            <CardTitle className="text-base">任务定义</CardTitle>
                            <Dialog open={isAddTaskOpen} onOpenChange={setIsAddTaskOpen}>
                                <DialogTrigger asChild>
                                    <Button size="sm"><Plus className="mr-2 h-4 w-4" />添加任务</Button>
                                </DialogTrigger>
                                <DialogContent className="sm:max-w-lg">
                                    <DialogHeader>
                                        <DialogTitle>添加新任务</DialogTitle>
                                        <DialogDescription>定义任务的执行逻辑和依赖关系</DialogDescription>
                                    </DialogHeader>
                                    <div className="space-y-4 py-4 max-h-[60vh] overflow-y-auto">
                                        <div className="grid gap-2">
                                            <Label>任务名称 *</Label>
                                            <Input placeholder="例如: process_data" value={newTask.name} onChange={(e) => setNewTask({ ...newTask, name: e.target.value })} />
                                        </div>
                                        <div className="grid gap-2">
                                            <Label>执行器类型</Label>
                                            <Select value={newTask.executor} onValueChange={(v) => setNewTask({ ...newTask, executor: v as ExecutorType })}>
                                                <SelectTrigger><SelectValue /></SelectTrigger>
                                                <SelectContent>
                                                    {(Object.keys(executorLabels) as ExecutorType[]).map((exec) => {
                                                        const Icon = executorIcons[exec];
                                                        return (
                                                            <SelectItem key={exec} value={exec}>
                                                                <div className="flex items-center gap-2"><Icon className="h-4 w-4" />{executorLabels[exec]}</div>
                                                            </SelectItem>
                                                        );
                                                    })}
                                                </SelectContent>
                                            </Select>
                                        </div>
                                        <div className="grid gap-2">
                                            <Label>命令 *</Label>
                                            <Textarea placeholder="要执行的命令..." rows={2} value={newTask.command} onChange={(e) => setNewTask({ ...newTask, command: e.target.value })} />
                                        </div>
                                        <div className="grid gap-2">
                                            <Label>工作目录</Label>
                                            <Input placeholder="例如: /home/user/project" value={newTask.workingDir} onChange={(e) => setNewTask({ ...newTask, workingDir: e.target.value })} />
                                            <p className="text-xs text-muted-foreground">命令执行的工作目录，留空则使用默认目录</p>
                                        </div>
                                        <div className="grid gap-2">
                                            <Label>依赖任务</Label>
                                            <Select value={newTask.dependsOn[0] || "__none__"} onValueChange={(v) => setNewTask({ ...newTask, dependsOn: v === "__none__" ? [] : [v] })}>
                                                <SelectTrigger><SelectValue placeholder="选择依赖任务" /></SelectTrigger>
                                                <SelectContent>
                                                    <SelectItem value="__none__">无依赖</SelectItem>
                                                    {taskDefinitions.map((t) => <SelectItem key={t.id} value={t.id}>{t.name}</SelectItem>)}
                                                </SelectContent>
                                            </Select>
                                        </div>
                                        <div className="grid grid-cols-3 gap-4">
                                            <div className="grid gap-2">
                                                <Label>重试次数</Label>
                                                <Input type="number" min={0} value={newTask.retryCount} onChange={(e) => setNewTask({ ...newTask, retryCount: parseInt(e.target.value) || 0 })} />
                                            </div>
                                            <div className="grid gap-2">
                                                <Label>重试间隔(秒)</Label>
                                                <Input type="number" min={0} value={newTask.retryInterval} onChange={(e) => setNewTask({ ...newTask, retryInterval: parseInt(e.target.value) || 0 })} />
                                            </div>
                                            <div className="grid gap-2">
                                                <Label>超时(秒)</Label>
                                                <Input type="number" min={0} value={newTask.timeout} onChange={(e) => setNewTask({ ...newTask, timeout: parseInt(e.target.value) || 0 })} />
                                            </div>
                                        </div>
                                    </div>
                                    <DialogFooter>
                                        <Button variant="outline" onClick={() => setIsAddTaskOpen(false)}>取消</Button>
                                        <Button onClick={handleAddTask}>添加</Button>
                                    </DialogFooter>
                                </DialogContent>
                            </Dialog>
                        </CardHeader>
                        <CardContent>
                            <div className="space-y-3">
                                {taskDefinitions.map((task, index) => {
                                    const ExecIcon = executorIcons[task.executor];
                                    const instance = taskInstances.find((ti) => ti.taskDefinitionId === task.id);
                                    const taskStatus = instance ? instance.status : "pending";
                                    const dependsTasks = task.dependsOn.map((d) => taskDefinitions.find((t) => t.id === d)?.name).filter(Boolean);

                                    return (
                                        <div key={task.id} className="flex items-center justify-between p-4 rounded-lg border border-border bg-card hover:bg-accent/30 transition-colors">
                                            <div className="flex items-center gap-4">
                                                <span className="text-sm text-muted-foreground w-6">{index + 1}</span>
                                                <TaskStatusIcon status={taskStatus} />
                                                <div>
                                                    <div className="flex items-center gap-2">
                                                        <span className="font-medium">{task.name}</span>
                                                        {instance && <TaskStatusBadge status={instance.status} />}
                                                    </div>
                                                    <div className="flex items-center gap-2 mt-1 flex-wrap">
                                                        <Badge variant="secondary" className="text-xs"><ExecIcon className="mr-1 h-3 w-3" />{executorLabels[task.executor]}</Badge>
                                                        {dependsTasks.length > 0 && <Badge variant="outline" className="text-xs">依赖: {dependsTasks.join(", ")}</Badge>}
                                                        <span className="text-xs text-muted-foreground">重试 {task.retryCount} 次 • 超时 {task.timeout}s</span>
                                                    </div>
                                                </div>
                                            </div>
                                            <div className="flex items-center gap-1">
                                                {instance && (instance.status === "failed" || instance.status === "upstream_failed") && (
                                                    <Tooltip>
                                                        <TooltipTrigger asChild>
                                                            <Button size="icon" variant="ghost" onClick={() => handleClearState(instance.id)}><RotateCcw className="h-4 w-4" /></Button>
                                                        </TooltipTrigger>
                                                        <TooltipContent>清除状态 (重试)</TooltipContent>
                                                    </Tooltip>
                                                )}
                                                {instance && (
                                                    <Tooltip>
                                                        <TooltipTrigger asChild>
                                                            <Button size="icon" variant="ghost" onClick={() => { setSelectedTaskForLogs(instance); setSelectedAttempt(instance.attempt); }}><FileText className="h-4 w-4" /></Button>
                                                        </TooltipTrigger>
                                                        <TooltipContent>查看日志</TooltipContent>
                                                    </Tooltip>
                                                )}
                                                <DropdownMenu>
                                                    <DropdownMenuTrigger asChild>
                                                        <Button variant="ghost" size="icon"><MoreVertical className="h-4 w-4" /></Button>
                                                    </DropdownMenuTrigger>
                                                    <DropdownMenuContent align="end">
                                                        <DropdownMenuItem onClick={() => { setEditingTask(task); setIsEditTaskOpen(true); }}><Pencil className="mr-2 h-4 w-4" />编辑任务</DropdownMenuItem>
                                                        <DropdownMenuItem className="text-destructive" onClick={() => { setTaskToDelete(task); setIsDeleteTaskOpen(true); }}><Trash2 className="mr-2 h-4 w-4" />删除任务</DropdownMenuItem>
                                                    </DropdownMenuContent>
                                                </DropdownMenu>
                                            </div>
                                        </div>
                                    );
                                })}
                                {taskDefinitions.length === 0 && (
                                    <div className="text-center py-8 text-muted-foreground">
                                        暂无任务，点击上方按钮添加
                                    </div>
                                )}
                            </div>
                        </CardContent>
                    </Card>
                </TabsContent>

                {/* Flow Chart Tab */}
                <TabsContent value="graph">
                    <Card>
                        <CardHeader>
                            <CardTitle className="text-base">任务依赖关系图</CardTitle>
                            <p className="text-sm text-muted-foreground">点击节点可查看任务详情和日志</p>
                        </CardHeader>
                        <CardContent className="p-0">
                            <DAGFlow tasks={flowTasks} dependencies={taskDependencies} className="border-0 rounded-t-none" onTaskClick={handleFlowTaskClick} />
                        </CardContent>
                    </Card>
                </TabsContent>

                {/* DAG Runs Tab */}
                <TabsContent value="runs">
                    <Card>
                        <CardHeader><CardTitle className="text-base">DAG 运行实例</CardTitle></CardHeader>
                        <CardContent>
                            {dagRuns.length > 0 ? (
                                <div className="space-y-3">
                                    {dagRuns.map((run) => (
                                        <div
                                            key={run.id}
                                            className={`flex items-center justify-between p-4 rounded-lg border transition-colors cursor-pointer ${selectedRun?.id === run.id ? "border-primary bg-primary/5" : "border-border hover:bg-accent/30"}`}
                                            onClick={() => setSelectedRun(run)}
                                        >
                                            <div className="flex items-center gap-4">
                                                <DAGStatusIcon status={run.status} />
                                                <div>
                                                    <div className="flex items-center gap-2">
                                                        <span className="font-medium">Run #{run.runNumber}</span>
                                                        <DAGStatusBadge status={run.status} />
                                                        <Badge variant="outline" className="text-xs">{run.triggerType === "schedule" ? "定时" : run.triggerType === "manual" ? "手动" : "API"}</Badge>
                                                    </div>
                                                    <p className="text-sm text-muted-foreground mt-1">开始: {formatTime(run.startTime)}{run.endTime && ` • 结束: ${formatTime(run.endTime)}`}</p>
                                                </div>
                                            </div>
                                            <div className="text-right">
                                                <div className="text-sm">{run.tasksCompleted}/{run.tasksTotal} 任务完成</div>
                                                {run.duration && <div className="text-xs text-muted-foreground">{run.duration}</div>}
                                            </div>
                                        </div>
                                    ))}
                                </div>
                            ) : (
                                <div className="text-center py-8 text-muted-foreground">
                                    暂无运行实例，点击"触发运行"开始执行
                                </div>
                            )}
                        </CardContent>
                    </Card>
                </TabsContent>

                {/* Logs Tab */}
                <TabsContent value="logs">
                    <Card>
                        <CardHeader><CardTitle className="text-base">DAG 运行日志</CardTitle></CardHeader>
                        <CardContent>
                            {dagLogs.length > 0 ? (
                                <div className="bg-terminal rounded-lg p-4 max-h-96 overflow-y-auto">
                                    <div className="font-mono text-xs space-y-1">
                                        {(() => {
                                            // Group logs by task and attempt
                                            const groupedLogs: { taskId: string; attempt: number; logs: typeof dagLogs }[] = [];
                                            let currentGroup: { taskId: string; attempt: number; logs: typeof dagLogs } | null = null;
                                            
                                            dagLogs.forEach((log) => {
                                                const taskId = log.taskId || '';
                                                const attempt = log.attempt || 1;
                                                
                                                if (!currentGroup || currentGroup.taskId !== taskId || currentGroup.attempt !== attempt) {
                                                    currentGroup = { taskId, attempt, logs: [] };
                                                    groupedLogs.push(currentGroup);
                                                }
                                                currentGroup.logs.push(log);
                                            });
                                            
                                            // Check if any task has multiple attempts
                                            const taskAttemptCounts: Record<string, number> = {};
                                            groupedLogs.forEach(g => {
                                                taskAttemptCounts[g.taskId] = Math.max(taskAttemptCounts[g.taskId] || 0, g.attempt);
                                            });
                                            
                                            return groupedLogs.map((group, groupIdx) => {
                                                const hasRetries = taskAttemptCounts[group.taskId] > 1;
                                                const showHeader = hasRetries;
                                                
                                                return (
                                                    <div key={groupIdx} className="mb-3">
                                                        {showHeader && (
                                                            <div className={`font-semibold mb-1 border-b pb-1 ${group.attempt > 1 ? 'text-warning border-warning/30' : 'text-muted-foreground border-border'}`}>
                                                                ── {group.taskId} (尝试 #{group.attempt}) ──
                                                            </div>
                                                        )}
                                                        {group.logs.map((log, i) => (
                                                            <p key={i}>
                                                                <span className="text-muted-foreground">[{log.timestamp}]</span>
                                                                <span className={`ml-1 ${getLogLevelClass(log.level)}`}>[{log.level}]</span>
                                                                <span className="text-terminal-foreground ml-1">{log.message}</span>
                                                            </p>
                                                        ))}
                                                    </div>
                                                );
                                            });
                                        })()}
                                    </div>
                                </div>
                            ) : (
                                <div className="text-center py-8 text-muted-foreground">
                                    暂无日志
                                </div>
                            )}
                        </CardContent>
                    </Card>
                </TabsContent>
            </Tabs>

            {/* Edit Task Dialog */}
            <Dialog open={isEditTaskOpen} onOpenChange={setIsEditTaskOpen}>
                <DialogContent className="sm:max-w-lg">
                    <DialogHeader>
                        <DialogTitle>编辑任务</DialogTitle>
                        <DialogDescription>修改任务的执行逻辑和依赖关系</DialogDescription>
                    </DialogHeader>
                    {editingTask && (
                        <div className="space-y-4 py-4 max-h-[60vh] overflow-y-auto">
                            <div className="grid gap-2">
                                <Label>任务名称</Label>
                                <Input value={editingTask.name} onChange={(e) => setEditingTask({ ...editingTask, name: e.target.value })} />
                            </div>
                            <div className="grid gap-2">
                                <Label>执行器类型</Label>
                                <Select value={editingTask.executor} onValueChange={(v) => setEditingTask({ ...editingTask, executor: v as ExecutorType })}>
                                    <SelectTrigger><SelectValue /></SelectTrigger>
                                    <SelectContent>
                                        {(Object.keys(executorLabels) as ExecutorType[]).map((exec) => {
                                            const Icon = executorIcons[exec];
                                            return <SelectItem key={exec} value={exec}><div className="flex items-center gap-2"><Icon className="h-4 w-4" />{executorLabels[exec]}</div></SelectItem>;
                                        })}
                                    </SelectContent>
                                </Select>
                            </div>
                            <div className="grid gap-2">
                                <Label>命令</Label>
                                <Textarea rows={2} value={editingTask.command} onChange={(e) => setEditingTask({ ...editingTask, command: e.target.value })} />
                            </div>
                            <div className="grid gap-2">
                                <Label>工作目录</Label>
                                <Input placeholder="例如: /home/user/project" value={editingTask.workingDir || ""} onChange={(e) => setEditingTask({ ...editingTask, workingDir: e.target.value })} />
                            </div>
                            <div className="grid gap-2">
                                <Label>依赖任务</Label>
                                <Select value={editingTask.dependsOn[0] || "__none__"} onValueChange={(v) => setEditingTask({ ...editingTask, dependsOn: v === "__none__" ? [] : [v] })}>
                                    <SelectTrigger><SelectValue placeholder="选择依赖任务" /></SelectTrigger>
                                    <SelectContent>
                                        <SelectItem value="__none__">无依赖</SelectItem>
                                        {taskDefinitions.filter((t) => t.id !== editingTask.id).map((t) => <SelectItem key={t.id} value={t.id}>{t.name}</SelectItem>)}
                                    </SelectContent>
                                </Select>
                            </div>
                            <div className="grid grid-cols-3 gap-4">
                                <div className="grid gap-2">
                                    <Label>重试次数</Label>
                                    <Input type="number" min={0} value={editingTask.retryCount} onChange={(e) => setEditingTask({ ...editingTask, retryCount: parseInt(e.target.value) || 0 })} />
                                </div>
                                <div className="grid gap-2">
                                    <Label>重试间隔(秒)</Label>
                                    <Input type="number" min={0} value={editingTask.retryInterval} onChange={(e) => setEditingTask({ ...editingTask, retryInterval: parseInt(e.target.value) || 0 })} />
                                </div>
                                <div className="grid gap-2">
                                    <Label>超时(秒)</Label>
                                    <Input type="number" min={0} value={editingTask.timeout} onChange={(e) => setEditingTask({ ...editingTask, timeout: parseInt(e.target.value) || 0 })} />
                                </div>
                            </div>
                        </div>
                    )}
                    <DialogFooter>
                        <Button variant="outline" onClick={() => setIsEditTaskOpen(false)}>取消</Button>
                        <Button onClick={handleEditTask}>保存</Button>
                    </DialogFooter>
                </DialogContent>
            </Dialog>

            {/* Task Logs Dialog */}
            <Dialog open={!!selectedTaskForLogs} onOpenChange={(open) => !open && setSelectedTaskForLogs(null)}>
                <DialogContent className="sm:max-w-2xl">
                    <DialogHeader>
                        <DialogTitle>任务日志 - {selectedTaskForLogs?.name}</DialogTitle>
                        <DialogDescription>
                            状态: {selectedTaskForLogs && taskStatusLabels[selectedTaskForLogs.status]} • 尝试次数: {selectedAttempt}
                        </DialogDescription>
                    </DialogHeader>
                    <div className="space-y-4">
                        {selectedTaskForLogs && selectedTaskForLogs.attempt > 1 && (
                            <Select value={String(selectedAttempt)} onValueChange={(v) => setSelectedAttempt(Number(v))}>
                                <SelectTrigger className="w-40">
                                    <SelectValue />
                                </SelectTrigger>
                                <SelectContent>
                                    {Array.from({ length: selectedTaskForLogs.attempt }, (_, i) => (
                                        <SelectItem key={i + 1} value={String(i + 1)}>Attempt #{i + 1}</SelectItem>
                                    ))}
                                </SelectContent>
                            </Select>
                        )}
                        <div className="bg-terminal rounded-lg p-4 max-h-80 overflow-y-auto">
                            <div className="font-mono text-xs space-y-1">
                                {selectedTaskForLogs && taskLogs[selectedTaskForLogs.id]?.[selectedAttempt]?.map((log, i) => (
                                    <p key={i}>
                                        <span className="text-muted-foreground">[{log.timestamp}]</span>
                                        <span className={`ml-1 ${getLogLevelClass(log.level)}`}>[{log.level}]</span>
                                        <span className="text-terminal-foreground ml-1">{log.message}</span>
                                    </p>
                                )) || <p className="text-muted-foreground">暂无日志</p>}
                            </div>
                        </div>
                    </div>
                </DialogContent>
            </Dialog>

            <AlertDialog open={isDeleteTaskOpen} onOpenChange={setIsDeleteTaskOpen}>
                <AlertDialogContent>
                    <AlertDialogHeader>
                        <AlertDialogTitle>确认删除任务</AlertDialogTitle>
                        <AlertDialogDescription>确定要删除任务 "{taskToDelete?.name}" 吗？</AlertDialogDescription>
                    </AlertDialogHeader>
                    <AlertDialogFooter>
                        <AlertDialogCancel>取消</AlertDialogCancel>
                        <AlertDialogAction onClick={() => { if (taskToDelete) { handleDeleteTask(taskToDelete.id); setIsDeleteTaskOpen(false); setTaskToDelete(null); } }}>删除</AlertDialogAction>
                    </AlertDialogFooter>
                </AlertDialogContent>
            </AlertDialog>
        </AppLayout>
    );
}
