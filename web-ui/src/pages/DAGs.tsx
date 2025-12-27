import { useState, useEffect, useCallback } from "react";
import { AppLayout } from "@/components/AppLayout";
import { DAGCard } from "@/components/DAGCard";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
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
  DialogHeader,
  DialogTitle,
  DialogTrigger,
  DialogFooter,
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
import { Label } from "@/components/ui/label";
import { Textarea } from "@/components/ui/textarea";
import { Plus, Search, LayoutGrid, List } from "lucide-react";
import { DAGTable } from "@/components/DAGTable";
import { useNavigate, useSearchParams } from "react-router-dom";
import { toast } from "sonner";
import { DAGCardData, DAGDefinition } from "@/types/dag";
import { listDAGs, createDAG, triggerDAG, deleteDAG, DAGInfo, listHistory } from "@/lib/api";
import { wsManager } from "@/lib/websocket";

const dagInfoToCardData = async (dag: DAGInfo, allHistory?: Map<string, any[]>): Promise<DAGCardData> => {
  let lastRun: DAGCardData["lastRun"] = undefined;
  
  try {
    let history: any[];
    if (allHistory && allHistory.has(dag.id)) {
      history = allHistory.get(dag.id) || [];
    } else {
      history = await listHistory(dag.id);
    }
    
    if (history.length > 0) {
      const latest = history[0];
      const mapStatus = (status: string): "running" | "success" | "failed" => {
        if (status === "running") return "running";
        if (status === "success") return "success";
        if (status === "failed") return "failed";
        return "running";
      };
      const mapTriggerType = (type: string): "schedule" | "manual" | "api" => {
        if (type === "schedule") return "schedule";
        if (type === "api") return "api";
        return "manual";
      };
      lastRun = {
        id: latest.run_id,
        dagId: latest.dag_id,
        runNumber: history.length,
        status: mapStatus(latest.status),
        triggerType: mapTriggerType(latest.trigger_type),
        startTime: latest.start_time,
        endTime: latest.end_time,
        duration: latest.duration_ms > 0 ? `${(latest.duration_ms / 1000).toFixed(1)}s` : undefined,
        tasksTotal: latest.total_tasks,
        tasksCompleted: latest.completed_tasks,
        tasksFailed: latest.failed_tasks,
      };
    }
  } catch {
    // Ignore fetch errors
  }

  return {
    definition: {
      id: dag.id,
      name: dag.name,
      description: dag.description,
      cron: dag.cron,
      maxConcurrentRuns: dag.max_concurrent_runs,
      createdAt: dag.created_at,
      updatedAt: dag.updated_at,
      isActive: dag.is_active,
    },
    taskCount: dag.task_count,
    successRate: lastRun ? Math.round((lastRun.tasksCompleted / lastRun.tasksTotal) * 100) : 0,
    lastRun,
  };
};

export default function DAGs() {
  const navigate = useNavigate();
  const [searchParams] = useSearchParams();
  const [viewMode, setViewMode] = useState<"grid" | "table">("grid");
  const [searchQuery, setSearchQuery] = useState(searchParams.get("search") || "");
  const [statusFilter, setStatusFilter] = useState<string>("all");
  const [isCreateDialogOpen, setIsCreateDialogOpen] = useState(false);
  const [isDeleteDialogOpen, setIsDeleteDialogOpen] = useState(false);
  const [dagToDelete, setDagToDelete] = useState<{ id: string; name: string } | null>(null);
  const [dags, setDags] = useState<DAGCardData[]>([]);
  const [loading, setLoading] = useState(true);
  const [newDAG, setNewDAG] = useState({
    name: "",
    description: "",
    cron: "",
    maxConcurrentRuns: 1,
    isActive: true,
  });

  const fetchDAGs = useCallback(async () => {
    try {
      setLoading(true);
      const [dagList, allHistory] = await Promise.all([
        listDAGs(),
        listHistory()
      ]);
      
      const historyByDag = new Map<string, any[]>();
      allHistory.forEach(record => {
        if (!historyByDag.has(record.dag_id)) {
          historyByDag.set(record.dag_id, []);
        }
        historyByDag.get(record.dag_id)!.push(record);
      });
      
      const dagsWithHistory = await Promise.all(
        dagList.map(dag => dagInfoToCardData(dag, historyByDag))
      );
      setDags(dagsWithHistory);
    } catch (error) {
      toast.error("获取 DAG 列表失败", {
        description: error instanceof Error ? error.message : "未知错误",
      });
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchDAGs();
    
    wsManager.connect();
    
    const unsubscribe = wsManager.on('dag_run_completed', () => {
      fetchDAGs();
    });
    
    return () => {
      unsubscribe();
    };
  }, [fetchDAGs]);

  const filteredDAGs = dags.filter((dag) => {
    const matchesSearch =
      dag.definition.name.toLowerCase().includes(searchQuery.toLowerCase()) ||
      dag.definition.description?.toLowerCase().includes(searchQuery.toLowerCase());

    if (statusFilter === "all") return matchesSearch;
    if (!dag.lastRun) return false;
    return matchesSearch && dag.lastRun.status === statusFilter;
  });

  const handleTriggerDAG = async (id: string) => {
    try {
      await triggerDAG(id);
      toast.success("DAG 已触发", {
        description: "新的运行实例已创建",
      });
      // Refresh DAG list
      fetchDAGs();
    } catch (error) {
      toast.error("触发 DAG 失败", {
        description: error instanceof Error ? error.message : "未知错误",
      });
    }
  };

  const handleViewDAG = (id: string) => {
    navigate(`/dags/${id}`);
  };

  const handleEditDAG = (id: string) => {
    navigate(`/dags/${id}`);
  };

  const handleDeleteDAG = async (id: string) => {
    const dag = dags.find(d => d.definition.id === id);
    if (dag) {
      setDagToDelete({ id, name: dag.definition.name });
      setIsDeleteDialogOpen(true);
    }
  };

  const confirmDeleteDAG = async () => {
    if (!dagToDelete) return;
    try {
      await deleteDAG(dagToDelete.id);
      toast.success("DAG 已删除");
      setIsDeleteDialogOpen(false);
      setDagToDelete(null);
      fetchDAGs();
    } catch (error) {
      toast.error("删除 DAG 失败", {
        description: error instanceof Error ? error.message : "未知错误",
      });
    }
  };

  const handleCreateDAG = async () => {
    if (!newDAG.name.trim()) {
      toast.error("请输入 DAG 名称");
      return;
    }
    try {
      await createDAG({
        name: newDAG.name,
        description: newDAG.description,
        cron: newDAG.cron || undefined,
        max_concurrent_runs: newDAG.maxConcurrentRuns,
        is_active: newDAG.isActive,
      });
      setIsCreateDialogOpen(false);
      setNewDAG({
        name: "",
        description: "",
        cron: "",
        maxConcurrentRuns: 1,
        isActive: true,
      });
      toast.success("DAG 创建成功");
      fetchDAGs();
    } catch (error) {
      toast.error("创建 DAG 失败", {
        description: error instanceof Error ? error.message : "未知错误",
      });
    }
  };

  return (
    <AppLayout title="DAGs" subtitle="管理和监控所有工作流">
      {/* Toolbar */}
      <div className="flex flex-col sm:flex-row items-start sm:items-center justify-between gap-4 mb-6">
        <div className="flex items-center gap-3 flex-1 w-full sm:w-auto">
          <div className="relative flex-1 sm:max-w-xs">
            <Search className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
            <Input
              placeholder="搜索 DAG..."
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              className="pl-9"
            />
          </div>
          <Select value={statusFilter} onValueChange={setStatusFilter}>
            <SelectTrigger className="w-32">
              <SelectValue placeholder="状态" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="all">全部状态</SelectItem>
              <SelectItem value="running">运行中</SelectItem>
              <SelectItem value="success">成功</SelectItem>
              <SelectItem value="failed">失败</SelectItem>
            </SelectContent>
          </Select>
        </div>

        <div className="flex items-center gap-2">
          <div className="flex items-center border border-border rounded-lg p-1">
            <Button
              variant={viewMode === "grid" ? "secondary" : "ghost"}
              size="icon"
              className="h-8 w-8"
              onClick={() => setViewMode("grid")}
            >
              <LayoutGrid className="h-4 w-4" />
            </Button>
            <Button
              variant={viewMode === "table" ? "secondary" : "ghost"}
              size="icon"
              className="h-8 w-8"
              onClick={() => setViewMode("table")}
            >
              <List className="h-4 w-4" />
            </Button>
          </div>

          <Dialog open={isCreateDialogOpen} onOpenChange={setIsCreateDialogOpen}>
            <DialogTrigger asChild>
              <Button>
                <Plus className="mr-2 h-4 w-4" />
                新建 DAG
              </Button>
            </DialogTrigger>
            <DialogContent className="sm:max-w-md">
              <DialogHeader>
                <DialogTitle>创建新 DAG</DialogTitle>
                <DialogDescription>定义工作流的基本信息和调度规则</DialogDescription>
              </DialogHeader>
              <div className="grid gap-4 py-4">
                <div className="grid gap-2">
                  <Label>DAG 名称 *</Label>
                  <Input
                    placeholder="例如: data_pipeline"
                    value={newDAG.name}
                    onChange={(e) => setNewDAG({ ...newDAG, name: e.target.value })}
                  />
                </div>
                <div className="grid gap-2">
                  <Label>描述</Label>
                  <Textarea
                    placeholder="描述这个 DAG 的用途..."
                    rows={2}
                    value={newDAG.description}
                    onChange={(e) => setNewDAG({ ...newDAG, description: e.target.value })}
                  />
                </div>
                <div className="grid gap-2">
                  <Label>Cron 表达式（可选，用于定时触发）</Label>
                  <Input
                    placeholder="例如: */5 * * * * (每5分钟)"
                    value={newDAG.cron}
                    onChange={(e) => setNewDAG({ ...newDAG, cron: e.target.value })}
                  />
                  <p className="text-xs text-muted-foreground">留空表示仅手动触发，填写 cron 表达式启用定时调度</p>
                </div>
                <div className="grid gap-2">
                  <Label>最大并发运行数（暂未实现）</Label>
                  <Input
                    type="number"
                    min={1}
                    max={10}
                    value={newDAG.maxConcurrentRuns}
                    onChange={(e) => setNewDAG({ ...newDAG, maxConcurrentRuns: parseInt(e.target.value) || 1 })}
                    disabled
                  />
                  <p className="text-xs text-muted-foreground">此功能暂未实现，当前不限制并发运行数</p>
                </div>
              </div>
              <DialogFooter>
                <Button variant="outline" onClick={() => setIsCreateDialogOpen(false)}>取消</Button>
                <Button onClick={handleCreateDAG}>创建</Button>
              </DialogFooter>
            </DialogContent>
          </Dialog>
        </div>
      </div>

      <p className="text-sm text-muted-foreground mb-4">共 {filteredDAGs.length} 个 DAG</p>

      {viewMode === "grid" ? (
        <div className="grid gap-4 sm:grid-cols-1 lg:grid-cols-2">
          {filteredDAGs.map((dag) => (
            <DAGCard
              key={dag.definition.id}
              dag={dag}
              onTrigger={handleTriggerDAG}
              onClick={handleViewDAG}
              onEdit={handleEditDAG}
              onDelete={handleDeleteDAG}
            />
          ))}
        </div>
      ) : (
        <DAGTable dags={filteredDAGs} onTrigger={handleTriggerDAG} onView={handleViewDAG} />
      )}

      {filteredDAGs.length === 0 && (
        <div className="flex flex-col items-center justify-center py-12 text-center">
          <div className="rounded-full bg-muted p-4 mb-4">
            <Search className="h-8 w-8 text-muted-foreground" />
          </div>
          <h3 className="text-lg font-medium">未找到匹配的 DAG</h3>
          <p className="text-muted-foreground mt-1">尝试调整搜索条件或创建新的 DAG</p>
        </div>
      )}

      <AlertDialog open={isDeleteDialogOpen} onOpenChange={setIsDeleteDialogOpen}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>确认删除 DAG</AlertDialogTitle>
            <AlertDialogDescription>
              确定要删除 DAG "{dagToDelete?.name}" 吗？此操作无法撤销。
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>取消</AlertDialogCancel>
            <AlertDialogAction onClick={confirmDeleteDAG}>删除</AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </AppLayout>
  );
}
