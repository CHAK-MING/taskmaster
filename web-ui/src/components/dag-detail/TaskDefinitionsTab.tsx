import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { executorIcons, executorLabels, sensorTypeLabels, triggerRuleLabels } from "@/lib/status";
import { TaskConfig } from "@/lib/api";
import type { ExecutorType } from "@/types/dag";
import { useI18n } from "@/contexts/I18nContext";

interface TaskDefinition extends TaskConfig {
  dagId: string;
}

interface TaskDefinitionsTabProps {
  tasks: TaskDefinition[];
}

export function TaskDefinitionsTab({ tasks }: TaskDefinitionsTabProps) {
  const { t } = useI18n();

  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-base">{t.dagDetail.taskDefinitions}</CardTitle>
      </CardHeader>
      <CardContent>
        <div className="space-y-3">
          {tasks.map((task, index) => {
            const executorKey: ExecutorType =
              task.executor && task.executor in executorIcons
                ? (task.executor as ExecutorType)
                : "shell";
            const ExecIcon = executorIcons[executorKey];
            const dependsTasks = (task.dependencies ?? [])
              .map((d) => tasks.find((t) => t.task_id === d.task)?.name)
              .filter(Boolean);

            return (
              <div
                key={task.task_id}
                className="flex items-center justify-between p-4 rounded-lg border border-border bg-card"
              >
                <div className="flex items-center gap-4">
                  <span className="text-sm text-muted-foreground w-6">{index + 1}</span>
                  <div>
                    <div className="font-medium">{task.name}</div>
                    <div className="text-xs text-muted-foreground font-mono mt-0.5">{task.task_id}</div>
                    <div className="flex items-center gap-2 mt-2 flex-wrap">
                      <Badge variant="secondary" className="text-xs">
                        <ExecIcon className="mr-1 h-3 w-3" />
                        {executorLabels[executorKey]}
                      </Badge>
                      {task.executor === "sensor" && task.sensor_type && (
                        <Badge variant="outline" className="text-xs">
                          {sensorTypeLabels[task.sensor_type] ?? task.sensor_type}
                        </Badge>
                      )}
                      <Badge variant="outline" className="text-xs">
                        {triggerRuleLabels[task.trigger_rule || "all_success"] || task.trigger_rule || "All success"}
                      </Badge>
                      {dependsTasks.length > 0 && (
                        <Badge variant="outline" className="text-xs">
                          {t.dagDetail.dependsOn}: {dependsTasks.join(", ")}
                        </Badge>
                      )}
                    </div>
                    {task.executor === "sensor" && task.sensor_target && (
                      <div className="mt-2 text-xs text-muted-foreground">
                        {t.dagDetail.sensorTarget}: <span className="font-mono break-all">{task.sensor_target}</span>
                      </div>
                    )}
                  </div>
                </div>
                <div>
                  <span className="text-xs text-muted-foreground">{task.dependencies?.length ?? 0} {t.dagDetail.dependenciesShort}</span>
                </div>
              </div>
            );
          })}
          {tasks.length === 0 && (
            <div className="text-center py-8 text-muted-foreground">
              {t.common.noData}
            </div>
          )}
        </div>
      </CardContent>
    </Card>
  );
}
