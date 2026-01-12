import { useState, useEffect, useCallback } from "react";
import { AppLayout } from "@/components/AppLayout";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Search, Play, RefreshCw } from "lucide-react";
import { useNavigate, useSearchParams } from "react-router-dom";
import { toast } from "sonner";
import { listDAGs, triggerDAG, DAGInfo } from "@/lib/api";

export default function DAGs() {
  const navigate = useNavigate();
  const [searchParams] = useSearchParams();
  const [searchQuery, setSearchQuery] = useState(searchParams.get("search") || "");
  const [dags, setDags] = useState<DAGInfo[]>([]);
  const [loading, setLoading] = useState(true);

  const fetchDAGs = useCallback(async () => {
    try {
      setLoading(true);
      const dagList = await listDAGs();
      setDags(dagList);
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
  }, [fetchDAGs]);

  const filteredDAGs = dags.filter((dag) => {
    return (
      dag.name.toLowerCase().includes(searchQuery.toLowerCase()) ||
      dag.description?.toLowerCase().includes(searchQuery.toLowerCase())
    );
  });

  const handleTriggerDAG = async (id: string) => {
    try {
      await triggerDAG(id);
      toast.success("DAG 已触发", {
        description: "新的运行实例已创建",
      });
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

  return (
    <AppLayout title="DAGs" subtitle="查看所有工作流">
      <div className="flex flex-col sm:flex-row items-start sm:items-center justify-between gap-4 mb-6">
        <div className="relative flex-1 sm:max-w-xs">
          <Search className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
          <Input
            placeholder="搜索 DAG..."
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            className="pl-9"
          />
        </div>

        <Button variant="outline" onClick={fetchDAGs} disabled={loading}>
          <RefreshCw className={`mr-2 h-4 w-4 ${loading ? "animate-spin" : ""}`} />
          刷新
        </Button>
      </div>

      <p className="text-sm text-muted-foreground mb-4">共 {filteredDAGs.length} 个 DAG</p>

      {loading && dags.length === 0 ? (
        <div className="text-center py-8 text-muted-foreground">加载中...</div>
      ) : (
        <div className="grid gap-4 sm:grid-cols-1 lg:grid-cols-2">
          {filteredDAGs.map((dag) => {
            return (
              <Card
                key={dag.dag_id}
                className="cursor-pointer hover:bg-accent/30 transition-colors"
                onClick={() => handleViewDAG(dag.dag_id)}
              >
                <CardHeader className="pb-2">
                  <CardTitle className="text-base">{dag.name}</CardTitle>
                </CardHeader>
                <CardContent>
                  <p className="text-sm text-muted-foreground mb-3 line-clamp-2">
                    {dag.description || "无描述"}
                  </p>
                  <div className="flex items-center justify-between">
                    <div className="text-xs text-muted-foreground space-x-3">
                      <span>{dag.tasks?.length || 0} 个任务</span>
                      {dag.cron && <span className="font-mono">{dag.cron}</span>}
                    </div>
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
            );
          })}
        </div>
      )}

      {filteredDAGs.length === 0 && !loading && (
        <div className="flex flex-col items-center justify-center py-12 text-center">
          <div className="rounded-full bg-muted p-4 mb-4">
            <Search className="h-8 w-8 text-muted-foreground" />
          </div>
          <h3 className="text-lg font-medium">未找到匹配的 DAG</h3>
          <p className="text-muted-foreground mt-1">尝试调整搜索条件</p>
        </div>
      )}
    </AppLayout>
  );
}
