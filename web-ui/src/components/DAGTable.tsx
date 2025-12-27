import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { Button } from "@/components/ui/button";
import { Play, Eye, Calendar } from "lucide-react";
import { DAGCardData, DAGDisplayStatus } from "@/types/dag";
import { DAGStatusBadge } from "@/lib/status";
import { cn } from "@/lib/utils";

interface DAGTableProps {
  dags: DAGCardData[];
  onTrigger?: (id: string) => void;
  onView?: (id: string) => void;
}

export function DAGTable({ dags, onTrigger, onView }: DAGTableProps) {
  return (
    <div className="rounded-lg border border-border overflow-hidden">
      <Table>
        <TableHeader>
          <TableRow className="bg-muted/50 hover:bg-muted/50">
            <TableHead className="font-semibold">DAG 名称</TableHead>
            <TableHead className="font-semibold">Cron</TableHead>
            <TableHead className="font-semibold">状态</TableHead>
            <TableHead className="font-semibold">任务数</TableHead>
            <TableHead className="font-semibold">成功率</TableHead>
            <TableHead className="font-semibold">最近 Run</TableHead>
            <TableHead className="font-semibold text-right">操作</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {dags.map((dag) => {
            const { definition, lastRun, taskCount, successRate } = dag;
            const displayStatus: DAGDisplayStatus = !definition.isActive
              ? "inactive"
              : lastRun?.status || "no_run";

            return (
              <TableRow key={definition.id} className="group">
                <TableCell>
                  <div>
                    <span className="font-medium">{definition.name}</span>
                    {definition.description && (
                      <p className="text-xs text-muted-foreground truncate max-w-[200px]">
                        {definition.description}
                      </p>
                    )}
                  </div>
                </TableCell>
                <TableCell>
                  {definition.cron ? (
                    <span className="font-mono text-xs flex items-center gap-1">
                      <Calendar className="h-3 w-3" />
                      {definition.cron}
                    </span>
                  ) : (
                    <span className="text-muted-foreground">-</span>
                  )}
                </TableCell>
                <TableCell>
                  <DAGStatusBadge status={displayStatus} />
                </TableCell>
                <TableCell>{taskCount}</TableCell>
                <TableCell>
                  <span className={cn(
                    "font-medium",
                    successRate >= 90 ? "text-success" :
                      successRate >= 70 ? "text-warning" : "text-destructive"
                  )}>
                    {successRate}%
                  </span>
                </TableCell>
                <TableCell className="text-muted-foreground">
                  {lastRun ? (
                    <div className="text-xs">
                      <span>#{lastRun.runNumber}</span>
                      {lastRun.duration && <span className="ml-2">{lastRun.duration}</span>}
                    </div>
                  ) : "-"}
                </TableCell>
                <TableCell className="text-right">
                  <div className="flex items-center justify-end gap-1 opacity-0 group-hover:opacity-100 transition-opacity">
                    <Button
                      size="icon"
                      variant="ghost"
                      className="h-8 w-8"
                      onClick={() => onView?.(definition.id)}
                    >
                      <Eye className="h-4 w-4" />
                    </Button>
                    <Button
                      size="icon"
                      variant="ghost"
                      className="h-8 w-8 text-primary hover:text-primary hover:bg-primary/10"
                      onClick={() => onTrigger?.(definition.id)}
                    >
                      <Play className="h-4 w-4" />
                    </Button>
                  </div>
                </TableCell>
              </TableRow>
            );
          })}
        </TableBody>
      </Table>
    </div>
  );
}
