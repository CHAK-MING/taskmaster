import { useState, useEffect } from "react";
import { AppLayout } from "@/components/AppLayout";
import { StatCard } from "@/components/StatCard";
import { DAGTable } from "@/components/DAGTable";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import {
  GitBranch,
  Play,
  CheckCircle2,
  Clock,
  ArrowRight,
} from "lucide-react";
import { useNavigate } from "react-router-dom";
import { getStatus, listDAGs, triggerDAG, DAGInfo, SystemStatus } from "@/lib/api";
import { DAGCardData } from "@/types/dag";

export default function Dashboard() {
  const navigate = useNavigate();
  const [status, setStatus] = useState<SystemStatus | null>(null);
  const [dags, setDags] = useState<DAGInfo[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const loadData = async () => {
      try {
        const [statusData, dagsData] = await Promise.all([
          getStatus(),
          listDAGs(),
        ]);
        setStatus(statusData);
        setDags(dagsData || []);
      } catch {
        // Ignore load errors
      } finally {
        setLoading(false);
      }
    };
    loadData();
    const interval = setInterval(loadData, 5000);
    return () => clearInterval(interval);
  }, []);

  const totalTasks = dags.reduce((sum, dag) => sum + (dag.task_count || dag.tasks?.length || 0), 0);

  // Convert DAGInfo[] to DAGCardData[] for DAGTable
  const dagCardData: DAGCardData[] = dags.map(dag => ({
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
    taskCount: dag.task_count || dag.tasks?.length || 0,
    successRate: 0,
    lastRun: undefined,
  }));

  const handleTriggerDAG = async (id: string) => {
    try {
      await triggerDAG(id);
    } catch {
      // Ignore trigger errors
    }
  };

  const handleViewDAG = (id: string) => {
    navigate(`/dags/${id}`);
  };

  return (
    <AppLayout title="仪表盘" subtitle="任务调度系统概览">
      <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4 mb-8">
        <StatCard
          title="DAG 数量"
          value={dags.length}
          icon={GitBranch}
        />
        <StatCard
          title="活跃运行"
          value={status?.active_runs ?? 0}
          icon={Play}
          variant="default"
        />
        <StatCard
          title="任务总数"
          value={totalTasks}
          icon={Clock}
          variant="success"
        />
        <StatCard
          title="系统状态"
          value={status?.running ? "运行中" : "已停止"}
          icon={CheckCircle2}
          variant={status?.running ? "success" : "destructive"}
        />
      </div>

      <div className="flex items-center justify-between mb-6">
        <h2 className="text-xl font-semibold text-foreground">最近 DAGs</h2>
        <div className="flex items-center gap-3">
          <Button variant="outline" onClick={() => navigate("/dags")}>
            查看全部
            <ArrowRight className="ml-2 h-4 w-4" />
          </Button>
        </div>
      </div>

      {loading ? (
        <div className="text-center py-8 text-muted-foreground">加载中...</div>
      ) : (
        <DAGTable dags={dagCardData} onTrigger={handleTriggerDAG} onView={handleViewDAG} />
      )}

      <div className="grid gap-6 md:grid-cols-2 mt-8">
        <Card>
          <CardHeader>
            <CardTitle className="text-base font-semibold">系统状态</CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            {[
              { label: "调度引擎", status: status?.running ? "运行中" : "已停止", healthy: status?.running },
              { label: "DAG 数量", status: `${dags.length} 个`, healthy: true },
              { label: "活跃运行", status: `${status?.active_runs ?? 0}`, healthy: true },
            ].map((item, i) => (
              <div key={i} className="flex items-center justify-between">
                <span className="text-sm text-muted-foreground">{item.label}</span>
                <div className="flex items-center gap-2">
                  <div className={`h-2 w-2 rounded-full ${item.healthy ? "bg-success" : "bg-destructive"}`} />
                  <span className={`text-sm font-medium ${item.healthy ? "text-success" : "text-destructive"}`}>
                    {item.status}
                  </span>
                </div>
              </div>
            ))}
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle className="text-base font-semibold">快速操作</CardTitle>
          </CardHeader>
          <CardContent className="space-y-3">
            {dags.slice(0, 5).map((dag) => (
              <div
                key={dag.id}
                className="flex items-center justify-between p-3 rounded-lg border border-border"
              >
                <div>
                  <span className="font-medium">{dag.name}</span>
                  <span className="text-sm text-muted-foreground ml-2">
                    {dag.task_count || dag.tasks?.length || 0} 个任务
                  </span>
                </div>
                <Button
                  size="sm"
                  variant="outline"
                  onClick={() => handleTriggerDAG(dag.id)}
                >
                  <Play className="h-3 w-3 mr-1" />
                  运行
                </Button>
              </div>
            ))}
            {dags.length === 0 && (
              <p className="text-muted-foreground text-sm text-center py-4">
                暂无 DAG
              </p>
            )}
          </CardContent>
        </Card>
      </div>
    </AppLayout>
  );
}
