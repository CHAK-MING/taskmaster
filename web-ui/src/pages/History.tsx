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
import { useState } from "react";
import { DAGStatusBadge, DAGStatusIcon, formatDuration, formatTime } from "@/lib/status";
import { DAGRunState } from "@/types/dag";
import { useI18n } from "@/contexts/I18nContext";
import { matchesSearchQuery } from "@/lib/search";

const TriggerTypeBadge = ({ type, t }: { type: string; t: Translations }) => {
    switch (type) {
        case "schedule":
            return (
                <Badge variant="outline" className="text-xs gap-1">
                    <Clock className="h-3 w-3" />
                    {t.triggerType.schedule}
                </Badge>
            );
        case "api":
            return (
                <Badge variant="outline" className="text-xs gap-1">
                    <Zap className="h-3 w-3" />
                    API
                </Badge>
            );
        default:
            return (
                <Badge variant="outline" className="text-xs gap-1">
                    <Hand className="h-3 w-3" />
                    {t.triggerType.manual}
                </Badge>
            );
    }
};

import type { Translations } from "@/lib/i18n";
import { useRunsQuery } from "@/hooks/useDAGQueries";

export default function History() {
    const { t, tf } = useI18n();
    const [searchQuery, setSearchQuery] = useState("");
    const [statusFilter, setStatusFilter] = useState("all");

    // Replace raw manual data fetching and web-sockets with React Query!
    // Real-time synchronization is handled deeply by useRunsQuery + useWebSocketQuerySync inside parent if active
    const { data: history = [], isLoading: loading, isRefetching, refetch } = useRunsQuery(undefined, 5000);
    const isRefreshing = loading || isRefetching;

    const filteredHistory = history.filter((run) => {
        const dagName = run.dag_name || run.dag_id;
        const matchesSearch = matchesSearchQuery(
            searchQuery,
            run.dag_id,
            run.dag_name,
            dagName,
            run.dag_run_id
        );
        const matchesStatus = statusFilter === "all" || run.state === statusFilter;
        return matchesSearch && matchesStatus;
    });

    return (
        <AppLayout title={t.history.title} subtitle={t.history.subtitle}>
            <div className="flex flex-col sm:flex-row gap-4 mb-6">
                <div className="relative flex-1 sm:max-w-xs">
                    <Search className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
                    <Input
                        placeholder={t.history.searchPlaceholder}
                        value={searchQuery}
                        onChange={(e) => setSearchQuery(e.target.value)}
                        className="pl-9"
                    />
                </div>
                <Select value={statusFilter} onValueChange={setStatusFilter}>
                    <SelectTrigger className="w-32">
                        <SelectValue placeholder={t.runStatus.running} />
                    </SelectTrigger>
                    <SelectContent>
                        <SelectItem value="all">{t.history.allStatus}</SelectItem>
                        <SelectItem value="success">{t.runStatus.success}</SelectItem>
                        <SelectItem value="failed">{t.runStatus.failed}</SelectItem>
                        <SelectItem value="running">{t.runStatus.running}</SelectItem>
                    </SelectContent>
                </Select>
                <Button variant="outline" onClick={() => refetch()} disabled={isRefreshing}>
                    <RefreshCw className={`mr-2 h-4 w-4 ${isRefreshing ? "animate-spin" : ""}`} />
                    {t.common.refresh}
                </Button>
            </div>

            <Card>
                <CardHeader>
                    <CardTitle className="text-base">{tf(t.history.records, { count: filteredHistory.length })}</CardTitle>
                </CardHeader>
                <CardContent>
                    {loading && history.length === 0 ? (
                        <div className="flex items-center justify-center h-32">
                            <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
                        </div>
                    ) : filteredHistory.length === 0 ? (
                        <div className="text-center py-8 text-muted-foreground">
                            {t.history.noRecords}
                        </div>
                    ) : (
                        <div className="space-y-3">
                            {filteredHistory.map((run) => (
                                <div
                                    key={run.dag_run_id}
                                    className="flex items-center justify-between p-4 rounded-lg border border-border hover:bg-accent/30 transition-colors cursor-pointer"
                                >
                                    <div className="flex items-center gap-4">
                                        <DAGStatusIcon status={run.state as DAGRunState} />
                                        <div>
                                            <p className="font-medium">{run.dag_name || run.dag_id}</p>
                                            <p className="text-sm text-muted-foreground">{formatTime(run.started_at)}</p>
                                            {run.execution_date && (
                                                <p className="text-xs text-muted-foreground">
                                                    {t.history.executionDate}: {run.execution_date.split('T')[0]}
                                                </p>
                                            )}
                                        </div>
                                    </div>
                                    <div className="flex items-center gap-6">
                                        <TriggerTypeBadge type={run.trigger_type} t={t} />
                                        <div className="text-right">
                                            <p className="text-sm font-medium">
                                                {run.state === "running" ? t.history.inProgress : (run.finished_at ? formatDuration(new Date(run.finished_at).getTime() - new Date(run.started_at).getTime()) : "-")}
                                            </p>
                                            {run.total_tasks !== undefined && (
                                                <p className="text-xs text-muted-foreground">
                                                    {run.completed_tasks}/{run.total_tasks} {t.history.tasks}
                                                    {(run.failed_tasks || 0) > 0 && <span className="text-destructive"> ({run.failed_tasks} {t.history.failed})</span>}
                                                </p>
                                            )}
                                        </div>
                                        <DAGStatusBadge status={run.state as DAGRunState} />
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
