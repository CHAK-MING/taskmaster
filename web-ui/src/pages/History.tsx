import { AppLayout } from "@/components/AppLayout";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import {
    Select,
    SelectContent,
    SelectItem,
    SelectTrigger,
    SelectValue,
} from "@/components/ui/select";
import { Badge } from "@/components/ui/badge";
import { Search, RefreshCw, Loader2, Clock, Hand, Zap } from "lucide-react";
import { useState, useEffect } from "react";
import { toast } from "sonner";
import { listHistory, type RunRecord } from "@/lib/api";
import { DAGStatusBadge, DAGStatusIcon, formatDuration, formatTime } from "@/lib/status";
import { DAGRunStatus } from "@/types/dag";
import { wsManager } from "@/lib/websocket";

const TriggerTypeBadge = ({ type }: { type: string }) => {
    switch (type) {
        case "schedule":
            return (
                <Badge variant="outline" className="text-xs gap-1">
                    <Clock className="h-3 w-3" />
                    定时触发
                </Badge>
            );
        case "api":
            return (
                <Badge variant="outline" className="text-xs gap-1">
                    <Zap className="h-3 w-3" />
                    API 触发
                </Badge>
            );
        default:
            return (
                <Badge variant="outline" className="text-xs gap-1">
                    <Hand className="h-3 w-3" />
                    手动触发
                </Badge>
            );
    }
};

export default function History() {
    const [history, setHistory] = useState<RunRecord[]>([]);
    const [loading, setLoading] = useState(true);
    const [searchQuery, setSearchQuery] = useState("");
    const [statusFilter, setStatusFilter] = useState("all");

    const fetchHistory = async () => {
        try {
            const data = await listHistory();
            setHistory(data);
        } catch (error) {
            toast.error("获取历史记录失败", {
                description: error instanceof Error ? error.message : "未知错误",
            });
        } finally {
            setLoading(false);
        }
    };

    useEffect(() => {
        fetchHistory();
        
        wsManager.connect();
        
        const unsubscribe = wsManager.on('dag_run_completed', () => {
            fetchHistory();
        });
        
        return () => {
            unsubscribe();
        };
    }, []);

    const filteredHistory = history.filter((run) => {
        const matchesSearch = run.dag_name.toLowerCase().includes(searchQuery.toLowerCase());
        const matchesStatus = statusFilter === "all" || run.status === statusFilter;
        return matchesSearch && matchesStatus;
    });

    return (
        <AppLayout title="运行历史" subtitle="查看所有 DAG 的执行记录">
            <div className="flex flex-col sm:flex-row gap-4 mb-6">
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
                        <SelectItem value="success">成功</SelectItem>
                        <SelectItem value="failed">失败</SelectItem>
                        <SelectItem value="running">运行中</SelectItem>
                    </SelectContent>
                </Select>
                <Button variant="outline" onClick={() => { setLoading(true); fetchHistory(); }}>
                    <RefreshCw className={`mr-2 h-4 w-4 ${loading ? "animate-spin" : ""}`} />
                    刷新
                </Button>
            </div>

            <Card>
                <CardHeader>
                    <CardTitle className="text-base">执行记录 ({filteredHistory.length})</CardTitle>
                </CardHeader>
                <CardContent>
                    {loading && history.length === 0 ? (
                        <div className="flex items-center justify-center h-32">
                            <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
                        </div>
                    ) : filteredHistory.length === 0 ? (
                        <div className="text-center py-8 text-muted-foreground">
                            暂无执行记录
                        </div>
                    ) : (
                        <div className="space-y-3">
                            {filteredHistory.map((run) => (
                                <div
                                    key={run.run_id}
                                    className="flex items-center justify-between p-4 rounded-lg border border-border hover:bg-accent/30 transition-colors cursor-pointer"
                                >
                                    <div className="flex items-center gap-4">
                                        <DAGStatusIcon status={run.status as DAGRunStatus} />
                                        <div>
                                            <p className="font-medium">{run.dag_name}</p>
                                            <p className="text-sm text-muted-foreground">{formatTime(run.start_time)}</p>
                                        </div>
                                    </div>
                                    <div className="flex items-center gap-6">
                                        <TriggerTypeBadge type={run.trigger_type} />
                                        <div className="text-right">
                                            <p className="text-sm font-medium">
                                                {run.status === "running" ? "进行中" : formatDuration(run.duration_ms)}
                                            </p>
                                            <p className="text-xs text-muted-foreground">
                                                {run.completed_tasks}/{run.total_tasks} 个任务
                                                {run.failed_tasks > 0 && <span className="text-destructive"> ({run.failed_tasks} 失败)</span>}
                                            </p>
                                        </div>
                                        <DAGStatusBadge status={run.status as DAGRunStatus} />
                                    </div>
                                </div>
                            ))}
                        </div>
                    )}
                </CardContent>
            </Card>
        </AppLayout>
    );
}
