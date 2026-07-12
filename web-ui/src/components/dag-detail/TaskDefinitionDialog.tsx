import { useCallback, useEffect, useState } from "react";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Badge } from "@/components/ui/badge";
import { Separator } from "@/components/ui/separator";
import { CodeBlock } from "@/components/ui/code-block";
import { useI18n } from "@/contexts/I18nContext";
import { executorLabels, sensorTypeLabels, triggerRuleLabels } from "@/lib/status";
import { toast } from "@/components/ui/use-toast";

export interface TaskDefinitionDialogTask {
  name: string;
  taskId?: string;
  executor?: string;
  command?: string;
  luaScript?: string;
  luaScriptFile?: string;
  sensorType?: string;
  sensorTarget?: string;
  triggerRule?: string;
  dependsOnPast?: boolean;
  dependsOn?: string[];
  triggerDagTarget?: string;
  triggerDagMode?: string;
  triggerPayloadCount?: number;
}

function DetailField({
  label,
  children,
  className,
}: {
  label: string;
  children: React.ReactNode;
  className?: string;
}) {
  return (
    <div className={["rounded-xl border border-border/60 bg-muted/20 px-3 py-3", className]
      .filter(Boolean)
      .join(" ")}
    >
      <div className="mb-1.5 text-[11px] font-medium uppercase tracking-[0.08em] text-muted-foreground/80">
        {label}
      </div>
      <div className="text-sm text-foreground">{children}</div>
    </div>
  );
}

export function TaskDefinitionDialog({
  task,
  open,
  onOpenChange,
}: {
  task: TaskDefinitionDialogTask | null;
  open: boolean;
  onOpenChange: (open: boolean) => void;
}) {
  const { t } = useI18n();
  const [copiedCommand, setCopiedCommand] = useState(false);

  useEffect(() => {
    if (!copiedCommand) {
      return;
    }
    const timer = window.setTimeout(() => setCopiedCommand(false), 1600);
    return () => window.clearTimeout(timer);
  }, [copiedCommand]);

  const handleCopyCommand = useCallback(async () => {
    if (!task?.command) {
      return;
    }
    if (!navigator.clipboard?.writeText) {
      toast({
        variant: "destructive",
        title: t.common.error,
        description: t.toast.commandCopyFailed,
      });
      return;
    }

    try {
      await navigator.clipboard.writeText(task.command);
      setCopiedCommand(true);
      toast({
        title: t.toast.commandCopied,
        description: task.taskId,
      });
    } catch {
      toast({
        variant: "destructive",
        title: t.common.error,
        description: t.toast.commandCopyFailed,
      });
    }
  }, [task, t]);

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-2xl">
        <DialogHeader className="space-y-3">
          <div className="flex items-start justify-between gap-3 pr-6">
            <div className="min-w-0 flex-1">
              <DialogTitle className="break-words pr-2 leading-snug">
                {task?.name}
              </DialogTitle>
              {task?.taskId ? (
                <div className="mt-2 inline-flex max-w-full rounded-full border border-border/70 bg-muted/30 px-2.5 py-1 font-mono text-[11px] text-muted-foreground">
                  <span className="truncate">{task.taskId}</span>
                </div>
              ) : null}
            </div>
            {task?.executor ? (
              <Badge variant="outline" className="shrink-0">
                {executorLabels[(task.executor as keyof typeof executorLabels) ?? "shell"] ?? task.executor}
              </Badge>
            ) : null}
          </div>
          <DialogDescription>{t.dagDetail.taskDefinitions}</DialogDescription>
        </DialogHeader>
        <Separator />
        <div className="space-y-4 py-2">
          <div className="grid gap-3 sm:grid-cols-2">
            {task?.executor ? (
              <DetailField label={t.dagDetail.executor}>
                <Badge variant="secondary">
                  {executorLabels[(task.executor as keyof typeof executorLabels) ?? "shell"] ?? task.executor}
                </Badge>
              </DetailField>
            ) : null}
            {task?.triggerRule ? (
              <DetailField label={t.dagDetail.triggerRule}>
                <Badge variant="outline">
                  {triggerRuleLabels[task.triggerRule] ?? task.triggerRule}
                </Badge>
              </DetailField>
            ) : null}
            {task?.executor === "sensor" && task.sensorType ? (
              <DetailField label={t.dagDetail.sensorType}>
                <Badge variant="outline">
                  {sensorTypeLabels[task.sensorType] ?? task.sensorType}
                </Badge>
              </DetailField>
            ) : null}
            {task?.triggerDagTarget ? (
              <DetailField label={t.dagDetail.childRun}>
                <div className="flex flex-wrap items-center gap-2">
                  <Badge variant="outline">{task.triggerDagMode || "trigger"}</Badge>
                  <span className="font-mono text-xs">{task.triggerDagTarget}</span>
                  {task.triggerPayloadCount ? (
                    <Badge variant="secondary">{task.triggerPayloadCount} payload</Badge>
                  ) : null}
                </div>
              </DetailField>
            ) : null}
          </div>

          {task?.dependsOn && task.dependsOn.length > 0 ? (
            <DetailField label={t.dagDetail.dependsOn}>
              <div className="flex flex-wrap gap-2">
                {task.dependsOn.map((dep) => (
                  <Badge key={dep} variant="secondary">{dep}</Badge>
                ))}
              </div>
            </DetailField>
          ) : null}

          {task?.sensorTarget ? (
            <CodeBlock
              title={t.dagDetail.sensorTarget}
              subtitle={task.sensorType || executorLabels.sensor}
              code={task.sensorTarget}
              language={task.sensorType === "command" ? "bash" : "text"}
            />
          ) : null}

          {task?.command ? (
            <CodeBlock
              title={t.dagDetail.command}
              subtitle={task.taskId || task.name}
              code={task.command}
              language={task.executor === "docker" ? "docker" : "bash"}
              onCopy={handleCopyCommand}
              copied={copiedCommand}
              copyLabel={t.common.copy}
              copiedLabel={t.toast.commandCopied}
            />
          ) : null}

          {task?.luaScript ? (
            <CodeBlock
              title={t.dagDetail.luaScript}
              subtitle={task.taskId || task.name}
              code={task.luaScript}
              language="lua"
            />
          ) : null}

          {task?.luaScriptFile && !task?.luaScript ? (
            <CodeBlock
              title={t.dagDetail.luaScriptFile}
              subtitle={task.taskId || task.name}
              code={task.luaScriptFile}
              language="text"
            />
          ) : null}
        </div>
      </DialogContent>
    </Dialog>
  );
}
