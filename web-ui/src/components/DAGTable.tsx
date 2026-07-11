import { useNavigate } from "react-router-dom";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Play } from "lucide-react";
import { RunSparkline } from "@/components/RunSparkline";
import { useI18n } from "@/contexts/I18nContext";
import type { DAGInfo, RunRecord } from "@/lib/api";

interface DAGTableProps {
  dags: DAGInfo[];
  dagRunsMap: Map<string, RunRecord[]>;
  dagHealthMap: Map<string, "healthy" | "failed">;
  onTrigger: (id: string) => void;
}

export function DAGTable({ dags, dagRunsMap, dagHealthMap, onTrigger }: DAGTableProps) {
  const navigate = useNavigate();
  const { t, locale } = useI18n();

  return (
    <div className="rounded-lg border bg-card shadow-sm overflow-hidden mb-6">
      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead className="border-b bg-muted/40">
            <tr>
              <th className="px-4 py-3 text-left font-medium text-muted-foreground">{t.dashboard.tableName}</th>
              <th className="px-4 py-3 text-left font-medium text-muted-foreground">{t.dashboard.tableTasks}</th>
              <th className="px-4 py-3 text-left font-medium text-muted-foreground">{locale === 'zh' ? '调度' : 'Schedule'}</th>
              <th className="px-4 py-3 text-left font-medium text-muted-foreground">{t.dashboard.tableHistory}</th>
              <th className="px-4 py-3 text-left font-medium text-muted-foreground">{t.dashboard.tableStatus}</th>
              <th className="px-4 py-3 text-right font-medium text-muted-foreground">{t.dashboard.tableActions}</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-border">
            {dags.map((dag) => {
              const dagRuns = dagRunsMap.get(dag.dag_id) || [];
              const dagHealth = dagHealthMap.get(dag.dag_id) || "healthy";
              return (
                <tr
                  key={dag.dag_id}
                  className="group cursor-pointer bg-card transition-colors hover:bg-muted/50"
                  onClick={() => navigate(`/dags/${dag.dag_id}`)}
                >
                  <td className="px-4 py-3">
                    <div className="flex items-center gap-3">
                      <div className={`h-2 w-2 rounded-full ${dagHealth === "failed" ? "bg-destructive" : dag.is_paused ? "bg-muted-foreground" : "bg-success"}`} />
                      <div>
                        <div className="font-medium text-foreground">{dag.name}</div>
                        <div className="mt-0.5 text-xs text-muted-foreground line-clamp-1">
                          {dag.description || dag.dag_id}
                        </div>
                      </div>
                    </div>
                  </td>
                  <td className="px-4 py-3 text-muted-foreground">{dag.tasks?.length || 0}</td>
                  <td className="px-4 py-3 font-mono text-xs text-muted-foreground">{dag.cron || t.common.manualOnly}</td>
                  <td className="px-4 py-3">
                    <div className="w-[120px]">
                      <RunSparkline runs={dagRuns} maxBars={10} />
                    </div>
                  </td>
                  <td className="px-4 py-3">
                    {dag.is_paused ? (
                      <Badge variant="secondary" className="bg-muted text-muted-foreground hover:bg-muted">{t.common.paused}</Badge>
                    ) : dagHealth === "failed" ? (
                      <Badge variant="destructive" className="bg-destructive/10 text-destructive border-destructive/20 hover:bg-destructive/20">{t.dashboard.abnormal}</Badge>
                    ) : (
                      <Badge variant="secondary" className="bg-success/10 text-success border-success/20 hover:bg-success/20">{t.dashboard.healthy}</Badge>
                    )}
                  </td>
                  <td className="px-4 py-3">
                    <div className="flex justify-end gap-2 opacity-0 transition-opacity group-hover:opacity-100">
                      <Button
                        size="icon"
                        variant="secondary"
                        onClick={(e) => {
                          e.stopPropagation();
                          onTrigger(dag.dag_id);
                        }}
                        className="h-8 w-8 text-foreground shadow-sm hover:bg-primary hover:text-primary-foreground"
                        title={t.common.run}
                      >
                        <Play className="h-4 w-4" />
                      </Button>
                    </div>
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}
