import { useState, useEffect } from "react";
import { AppLayout } from "@/components/AppLayout";
import { StatCard } from "@/components/StatCard";
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
import { getStatus, getHealth, listDAGs, triggerDAG, DAGInfo, SystemStatus, HealthStatus } from "@/lib/api";

export default function Dashboard() {
  const navigate = useNavigate();
  const [status, setStatus] = useState<SystemStatus | null>(null);
  const [health, setHealth] = useState<HealthStatus | null>(null);
  const [dags, setDags] = useState<DAGInfo[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const loadData = async () => {
      try {
        const [statusData, healthData, dagsData] = await Promise.all([
          getStatus(),
          getHealth(),
          listDAGs(),
        ]);
        setStatus(statusData);
        setHealth(healthData);
        setDags(dagsData || []);
      } catch {
        setHealth(null);
      } finally {
        setLoading(false);
      }
    };
    loadData();
    const interval = setInterval(loadData, 5000);
    return () => clearInterval(interval);
  }, []);

  const totalTasks = dags.reduce((sum, dag) => sum + (dag.tasks?.length || 0), 0);
  const activeRuns = status?.active_runs ? (typeof status.active_runs === 'boolean' ? (status.active_runs ? 1 : 0) : status.active_runs) : 0;
  const isHealthy = health?.status === 'healthy';

  const handleTriggerDAG = async (id: string) => {
    try {
      await triggerDAG(id);
    } catch {
      // ignore
    }
  };

  return (
    <AppLayout title="仪表盘" subtitle="任务调度系统概览">
      <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4 mb-8">
        <StatCard
          title="DAG 数量"
          value={status?.dag_count ?? dags.length}
          icon={GitBranch}
        />
        <StatCard
          title="活跃运行"
          value={activeRuns}
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
          value={isHealthy ? "运行中" : "已停止"}
          icon={CheckCircle2}
          variant={isHealthy ? "success" : "destructive"}
        />
      </div>

      <div className="flex items-center justify-between mb-6">
        <h2 className="text-xl font-semibold text-foreground">DAGs</h2>
        <Button variant="outline" onClick={() => navigate("/dags")}>
          查看全部
          <ArrowRight className="ml-2 h-4 w-4" />
        </Button>
      </div>

      {loading ? (
        <div className="text-center py-8 text-muted-foreground">加载中...</div>
      ) : (
        <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
          {dags.slice(0, 6).map((dag) => (
            <Card
              key={dag.dag_id}
              className="cursor-pointer hover:bg-accent/30 transition-colors"
              onClick={() => navigate(`/dags/${dag.dag_id}`)}
            >
              <CardHeader className="pb-2">
                <CardTitle className="text-base">{dag.name}</CardTitle>
              </CardHeader>
              <CardContent>
                <p className="text-sm text-muted-foreground mb-2 line-clamp-2">
                  {dag.description || "无描述"}
                </p>
                <div className="flex items-center justify-between">
                  <span className="text-xs text-muted-foreground">
                    {dag.tasks?.length || 0} 个任务
                  </span>
                  <Button
                    size="sm"
                    variant="outline"
                    onClick={(e) => {
                      e.stopPropagation();
                      handleTriggerDAG(dag.dag_id);
                    }}
                  >
                    <Play className="h-3 w-3 mr-1" />
                    运行
                  </Button>
                </div>
              </CardContent>
            </Card>
          ))}
        </div>
      )}

      {!loading && dags.length === 0 && (
        <div className="text-center py-8 text-muted-foreground">
          暂无 DAG
        </div>
      )}

      <div className="grid gap-6 md:grid-cols-2 mt-8">
        <Card>
          <CardHeader>
            <CardTitle className="text-base font-semibold">系统状态</CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            {[
              { label: "调度引擎", status: isHealthy ? "运行中" : "已停止", healthy: isHealthy },
              { label: "DAG 数量", status: `${status?.dag_count ?? dags.length} 个`, healthy: true },
              { label: "活跃运行", status: `${activeRuns}`, healthy: true },
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
                key={dag.dag_id}
                className="flex items-center justify-between p-3 rounded-lg border border-border"
              >
                <div>
                  <span className="font-medium">{dag.name}</span>
                  <span className="text-sm text-muted-foreground ml-2">
                    {dag.tasks?.length || 0} 个任务
                  </span>
                </div>
                <Button
                  size="sm"
                  variant="outline"
                  onClick={() => handleTriggerDAG(dag.dag_id)}
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
