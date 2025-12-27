import { useEffect, useState } from "react";
import { AppLayout } from "@/components/AppLayout";
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Loader2, Server, Database, Clock, CheckCircle2, XCircle } from "lucide-react";
import { toast } from "sonner";
import { getStatus, getHealth, type SystemStatus, type HealthStatus } from "@/lib/api";

export default function Settings() {
    const [status, setStatus] = useState<SystemStatus | null>(null);
    const [health, setHealth] = useState<HealthStatus | null>(null);
    const [loading, setLoading] = useState(true);

    useEffect(() => {
        async function fetchData() {
            try {
                const [statusData, healthData] = await Promise.all([
                    getStatus(),
                    getHealth(),
                ]);
                setStatus(statusData);
                setHealth(healthData);
            } catch (error) {
                toast.error("获取系统信息失败", {
                    description: error instanceof Error ? error.message : "未知错误",
                });
            } finally {
                setLoading(false);
            }
        }
        fetchData();
        const interval = setInterval(fetchData, 5000);
        return () => clearInterval(interval);
    }, []);

    if (loading) {
        return (
            <AppLayout title="系统设置" subtitle="查看系统配置和状态">
                <div className="flex items-center justify-center h-64">
                    <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
                </div>
            </AppLayout>
        );
    }

    return (
        <AppLayout title="系统设置" subtitle="查看系统配置和状态">
            <div className="grid gap-6 md:grid-cols-2">
                {/* System Status */}
                <Card>
                    <CardHeader>
                        <CardTitle className="flex items-center gap-2">
                            <Server className="h-5 w-5" />
                            系统状态
                        </CardTitle>
                        <CardDescription>当前系统运行状态</CardDescription>
                    </CardHeader>
                    <CardContent className="space-y-4">
                        <div className="flex items-center justify-between">
                            <span className="text-sm text-muted-foreground">健康状态</span>
                            <Badge variant={health?.status === "healthy" ? "default" : "destructive"}>
                                {health?.status === "healthy" ? (
                                    <><CheckCircle2 className="h-3 w-3 mr-1" /> 健康</>
                                ) : (
                                    <><XCircle className="h-3 w-3 mr-1" /> 异常</>
                                )}
                            </Badge>
                        </div>
                        <div className="flex items-center justify-between">
                            <span className="text-sm text-muted-foreground">运行状态</span>
                            <Badge variant={status?.running ? "default" : "secondary"}>
                                {status?.running ? "运行中" : "已停止"}
                            </Badge>
                        </div>
                        <div className="flex items-center justify-between">
                            <span className="text-sm text-muted-foreground">活跃执行</span>
                            <span className="text-sm font-medium">{status?.active_runs ?? 0}</span>
                        </div>
                        <div className="flex items-center justify-between">
                            <span className="text-sm text-muted-foreground">最后更新</span>
                            <span className="text-sm font-medium">
                                {health?.timestamp ? new Date(health.timestamp).toLocaleString() : "-"}
                            </span>
                        </div>
                    </CardContent>
                </Card>

                {/* Resource Stats */}
                <Card>
                    <CardHeader>
                        <CardTitle className="flex items-center gap-2">
                            <Database className="h-5 w-5" />
                            资源统计
                        </CardTitle>
                        <CardDescription>DAG 和任务统计信息</CardDescription>
                    </CardHeader>
                    <CardContent className="space-y-4">
                        <div className="flex items-center justify-between">
                            <span className="text-sm text-muted-foreground">DAG 总数</span>
                            <span className="text-sm font-medium">{status?.dags ?? 0}</span>
                        </div>
                        <div className="flex items-center justify-between">
                            <span className="text-sm text-muted-foreground">任务总数</span>
                            <span className="text-sm font-medium">{status?.tasks ?? 0}</span>
                        </div>
                    </CardContent>
                </Card>

            </div>
        </AppLayout>
    );
}
