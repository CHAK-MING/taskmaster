import { useMemo } from "react";
import Prism from "prismjs";
import "prismjs/components/prism-bash";
import "prismjs/components/prism-lua";
import "prismjs/components/prism-json";
import "prismjs/components/prism-docker";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { cn } from "@/lib/utils";
import { Check, Copy } from "lucide-react";

type CodeLanguage = "bash" | "docker" | "lua" | "json" | "text";

interface CodeBlockProps {
  code: string;
  language?: CodeLanguage;
  title?: string;
  subtitle?: string;
  className?: string;
  copyLabel?: string;
  copiedLabel?: string;
  onCopy?: () => void;
  copied?: boolean;
}

const languageLabels: Record<CodeLanguage, string> = {
  bash: "Shell",
  docker: "Docker",
  lua: "Lua",
  json: "JSON",
  text: "Text",
};

function escapeHtml(value: string): string {
  return value
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;");
}

function toPrismLanguage(language: CodeLanguage): string | null {
  switch (language) {
    case "bash":
      return "bash";
    case "docker":
      return "docker";
    case "lua":
      return "lua";
    case "json":
      return "json";
    default:
      return null;
  }
}

export function CodeBlock({
  code,
  language = "text",
  title,
  subtitle,
  className,
  copyLabel = "Copy",
  copiedLabel = "Copied",
  onCopy,
  copied = false,
}: CodeBlockProps) {
  const highlighted = useMemo(() => {
    const prismLanguage = toPrismLanguage(language);
    if (!prismLanguage) {
      return escapeHtml(code);
    }

    const grammar = Prism.languages[prismLanguage];
    if (!grammar) {
      return escapeHtml(code);
    }

    return Prism.highlight(code, grammar, prismLanguage);
  }, [code, language]);

  return (
    <div
      className={cn(
        "overflow-hidden rounded-2xl border border-border/80 bg-card shadow-sm",
        className,
      )}
    >
      <div className="flex items-center justify-between gap-3 border-b border-border/70 bg-muted/30 px-4 py-3">
        <div className="min-w-0">
          {title ? (
            <div className="truncate text-sm font-medium text-foreground">{title}</div>
          ) : null}
          {subtitle ? (
            <div className="truncate text-xs text-muted-foreground">{subtitle}</div>
          ) : null}
        </div>
        <div className="flex items-center gap-2">
          <Badge variant="outline" className="rounded-md text-[10px] uppercase tracking-[0.12em]">
            {languageLabels[language]}
          </Badge>
          {onCopy ? (
            <Button
              type="button"
              size="sm"
              variant="ghost"
              onClick={onCopy}
              className="h-8 gap-1.5 rounded-lg px-2.5 text-muted-foreground hover:text-foreground"
            >
              {copied ? <Check className="h-3.5 w-3.5" /> : <Copy className="h-3.5 w-3.5" />}
              {copied ? copiedLabel : copyLabel}
            </Button>
          ) : null}
        </div>
      </div>
      <div className="bg-[linear-gradient(180deg,hsl(var(--background))_0%,hsl(var(--muted)/0.45)_100%)] px-4 py-4">
        <pre className="max-h-80 overflow-auto whitespace-pre-wrap break-words font-mono text-[12.5px] leading-6 text-foreground">
          <code
            className="code-block-content"
            dangerouslySetInnerHTML={{ __html: highlighted }}
          />
        </pre>
      </div>
    </div>
  );
}
