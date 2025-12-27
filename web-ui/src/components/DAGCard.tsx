import { Play, MoreHorizontal, Calendar, Clock } from "lucide-react";
import { Card, CardContent, CardHeader } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import {
    DropdownMenu,
    DropdownMenuContent,
    DropdownMenuItem,
    DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import { DAGCardData, DAGDisplayStatus } from "@/types/dag";
import { DAGStatusBadge } from "@/lib/status";
import { Badge } from "@/components/ui/badge";

interface DAGCardProps {
    dag: DAGCardData;
    onTrigger?: (id: string) => void;
    onClick?: (id: string) => void;
    onEdit?: (id: string) => void;
    onDelete?: (id: string) => void;
}

export function DAGCard({ dag, onTrigger, onClick, onEdit, onDelete }: DAGCardProps) {
    const { definition, lastRun, taskCount, successRate } = dag;

    const displayStatus: DAGDisplayStatus = lastRun?.status || "no_run";

    return (
        <Card
            className="group cursor-pointer transition-all hover:shadow-lg hover:border-primary/30 hover:-translate-y-0.5"
            onClick={() => onClick?.(definition.id)}
        >
            <CardHeader className="pb-3">
                <div className="flex items-start justify-between">
                    <div className="flex-1 min-w-0">
                        <p className="text-base font-semibold truncate">{definition.name}</p>
                        {definition.description && (
                            <p className="text-sm text-muted-foreground mt-1 line-clamp-1">
                                {definition.description}
                            </p>
                        )}
                    </div>
                    <div className="flex items-center gap-2 ml-4">
                        {definition.cron && (
                            <Badge variant="outline" className="text-xs gap-1">
                                <Clock className="h-3 w-3" />
                                定时
                            </Badge>
                        )}
                        <DAGStatusBadge status={displayStatus} />
                    </div>
                </div>
            </CardHeader>
            <CardContent className="pt-0">
                {definition.cron && (
                    <div className="flex items-center gap-1.5 text-xs text-muted-foreground mb-3">
                        <Calendar className="h-3.5 w-3.5" />
                        <span className="font-mono">{definition.cron}</span>
                    </div>
                )}

                <div className="flex items-center justify-between">
                    <div className="flex items-center gap-4 text-sm text-muted-foreground">
                        <span>{taskCount} 个任务</span>
                        <span>成功率 {successRate}%</span>
                    </div>
                    <div className="flex items-center gap-1 opacity-0 group-hover:opacity-100 transition-opacity">
                        <Button
                            size="icon"
                            variant="ghost"
                            className="h-8 w-8 text-primary hover:text-primary hover:bg-primary/10"
                            onClick={(e) => {
                                e.stopPropagation();
                                onTrigger?.(definition.id);
                            }}
                        >
                            <Play className="h-4 w-4" />
                        </Button>
                        <DropdownMenu>
                            <DropdownMenuTrigger asChild>
                                <Button
                                    size="icon"
                                    variant="ghost"
                                    className="h-8 w-8"
                                    onClick={(e) => e.stopPropagation()}
                                >
                                    <MoreHorizontal className="h-4 w-4" />
                                </Button>
                            </DropdownMenuTrigger>
                            <DropdownMenuContent align="end">
                                <DropdownMenuItem onClick={(e) => { e.stopPropagation(); onClick?.(definition.id); }}>查看详情</DropdownMenuItem>
                                <DropdownMenuItem onClick={(e) => { e.stopPropagation(); onEdit?.(definition.id); }}>编辑</DropdownMenuItem>
                                <DropdownMenuItem className="text-destructive" onClick={(e) => { e.stopPropagation(); onDelete?.(definition.id); }}>删除</DropdownMenuItem>
                            </DropdownMenuContent>
                        </DropdownMenu>
                    </div>
                </div>

                {lastRun && (
                    <div className="flex items-center justify-between text-xs text-muted-foreground mt-3 pt-3 border-t border-border">
                        <span>Run #{lastRun.runNumber}</span>
                        <span>
                            {lastRun.triggerType === "schedule" ? "定时触发" :
                                lastRun.triggerType === "manual" ? "手动触发" : "API 触发"}
                        </span>
                        {lastRun.duration && <span>{lastRun.duration}</span>}
                    </div>
                )}
            </CardContent>
        </Card>
    );
}
