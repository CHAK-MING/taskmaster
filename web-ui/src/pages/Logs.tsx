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
import { Search, Download, Trash2, Pause, Play, Filter, Wifi, WifiOff } from "lucide-react";
import { useState, useEffect, useRef, useCallback } from "react";
import { toast } from "sonner";

interface LogEntry {
    id: string;
    timestamp: string;
    level: "INFO" | "WARN" | "ERROR" | "DEBUG" | "SUCCESS";
    source: string;
    message: string;
}

interface WebSocketMessage {
    type: string;
    timestamp?: string;
    run_id?: string;
    task_id?: string;
    stream?: string;
    content?: string;
    level?: string;
    source?: string;
    message?: string;
    [key: string]: unknown;
}

function parseLogLevel(content: string, stream?: string): LogEntry["level"] {
    if (stream === "stderr") return "ERROR";
    if (content.includes("[ERROR]") || content.includes("failed")) return "ERROR";
    if (content.includes("[SUCCESS]") || content.includes("completed")) return "SUCCESS";
    if (content.includes("[WARN]")) return "WARN";
    if (content.includes("[DEBUG]")) return "DEBUG";
    return "INFO";
}

const levelStyles = {
    INFO: "text-primary",
    WARN: "text-warning",
    ERROR: "text-destructive",
    DEBUG: "text-muted-foreground",
    SUCCESS: "text-success",
};

function downloadLogs(logs: LogEntry[], filename: string = "taskmaster-logs.txt") {
    const content = logs.map(log =>
        `[${log.timestamp}] [${log.source}] ${log.level} ${log.message}`
    ).join("\n");
    const blob = new Blob([content], { type: "text/plain" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = filename;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
}

export default function Logs() {
    const [logs, setLogs] = useState<LogEntry[]>([]);
    const [searchQuery, setSearchQuery] = useState("");
    const [levelFilter, setLevelFilter] = useState("all");
    const [isPaused, setIsPaused] = useState(false);
    const [connected, setConnected] = useState(false);
    const logContainerRef = useRef<HTMLDivElement>(null);
    const wsRef = useRef<WebSocket | null>(null);
    const logIdRef = useRef(0);
    const reconnectAttemptRef = useRef(0);
    const reconnectTimeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null);

    const addLog = useCallback((entry: Omit<LogEntry, "id">) => {
        if (isPaused) return;
        setLogs(prev => {
            const newLog = { ...entry, id: String(++logIdRef.current) };
            const updated = [newLog, ...prev];
            return updated.slice(0, 1000); // Keep last 1000 logs
        });
    }, [isPaused]);

    useEffect(() => {
        const protocol = globalThis.location.protocol === 'https:' ? 'wss:' : 'ws:';
        const wsUrl = `${protocol}//${globalThis.location.host}/ws/logs`;

        function getReconnectDelay(): number {
            // Exponential backoff: 1s, 2s, 4s, 8s, 16s, max 30s
            const delay = Math.min(1000 * Math.pow(2, reconnectAttemptRef.current), 30000);
            return delay;
        }

        function connect() {
            // Prevent multiple connections
            if (wsRef.current?.readyState === WebSocket.OPEN ||
                wsRef.current?.readyState === WebSocket.CONNECTING) {
                return;
            }

            const ws = new WebSocket(wsUrl);
            wsRef.current = ws;

            ws.onopen = () => {
                setConnected(true);
                reconnectAttemptRef.current = 0; // Reset on successful connection
                addLog({
                    timestamp: new Date().toISOString().replace('T', ' ').slice(0, 19),
                    level: "INFO",
                    source: "system",
                    message: "WebSocket 连接已建立",
                });
            };

            ws.onmessage = (event) => {
                try {
                    const data = JSON.parse(event.data) as WebSocketMessage;
                    if (data.type === "connected") {
                        return; // Welcome message
                    }

                    // Handle backend log format: { type, timestamp, run_id, task_id, stream, content }
                    if (data.type === "log" && data.content) {
                        const source = data.task_id
                            ? `${data.run_id?.split('_')[0] || 'dag'}:${data.task_id}`
                            : data.run_id || "server";
                        addLog({
                            timestamp: data.timestamp || new Date().toISOString().replace('T', ' ').slice(0, 19),
                            level: parseLogLevel(data.content, data.stream),
                            source: source,
                            message: data.content,
                        });
                        return;
                    }

                    // Fallback for other message formats
                    addLog({
                        timestamp: data.timestamp || new Date().toISOString().replace('T', ' ').slice(0, 19),
                        level: parseLogLevel(data.message || "", data.stream),
                        source: data.source || "server",
                        message: data.message || data.content || JSON.stringify(data),
                    });
                } catch {
                    addLog({
                        timestamp: new Date().toISOString().replace('T', ' ').slice(0, 19),
                        level: "DEBUG",
                        source: "ws",
                        message: event.data,
                    });
                }
            };

            ws.onclose = () => {
                setConnected(false);
                const delay = getReconnectDelay();
                reconnectAttemptRef.current++;
                addLog({
                    timestamp: new Date().toISOString().replace('T', ' ').slice(0, 19),
                    level: "WARN",
                    source: "system",
                    message: `WebSocket 连接已断开，${delay / 1000}秒后重连...`,
                });
                reconnectTimeoutRef.current = setTimeout(connect, delay);
            };

            ws.onerror = () => {
                setConnected(false);
            };
        }

        connect();

        return () => {
            if (reconnectTimeoutRef.current) {
                clearTimeout(reconnectTimeoutRef.current);
            }
            wsRef.current?.close();
        };
    }, [addLog]);

    const filteredLogs = logs.filter((log) => {
        const matchesSearch = log.message.toLowerCase().includes(searchQuery.toLowerCase()) ||
            log.source.toLowerCase().includes(searchQuery.toLowerCase());
        const matchesLevel = levelFilter === "all" || log.level === levelFilter;
        return matchesSearch && matchesLevel;
    });

    return (
        <AppLayout title="终端日志" subtitle="实时查看系统日志">
            {/* Toolbar */}
            <div className="flex flex-col sm:flex-row gap-4 mb-6">
                <div className="relative flex-1 sm:max-w-xs">
                    <Search className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
                    <Input
                        placeholder="搜索日志..."
                        value={searchQuery}
                        onChange={(e) => setSearchQuery(e.target.value)}
                        className="pl-9"
                    />
                </div>
                <Select value={levelFilter} onValueChange={setLevelFilter}>
                    <SelectTrigger className="w-32">
                        <SelectValue placeholder="级别" />
                    </SelectTrigger>
                    <SelectContent>
                        <SelectItem value="all">全部级别</SelectItem>
                        <SelectItem value="INFO">INFO</SelectItem>
                        <SelectItem value="WARN">WARN</SelectItem>
                        <SelectItem value="ERROR">ERROR</SelectItem>
                        <SelectItem value="DEBUG">DEBUG</SelectItem>
                        <SelectItem value="SUCCESS">SUCCESS</SelectItem>
                    </SelectContent>
                </Select>
                <div className="flex items-center gap-2">
                    <Button
                        variant="outline"
                        size="icon"
                        onClick={() => setIsPaused(!isPaused)}
                    >
                        {isPaused ? <Play className="h-4 w-4" /> : <Pause className="h-4 w-4" />}
                    </Button>
                    <Button variant="outline" size="icon" onClick={() => downloadLogs(filteredLogs)}>
                        <Download className="h-4 w-4" />
                    </Button>
                    <Button variant="outline" size="icon" onClick={() => setLogs([])}>
                        <Trash2 className="h-4 w-4" />
                    </Button>
                </div>
            </div>

            {/* Log Terminal */}
            <Card className="bg-terminal overflow-hidden">
                <CardContent className="p-0">
                    <div
                        ref={logContainerRef}
                        className="h-[600px] overflow-auto p-4 font-mono text-sm"
                    >
                        {filteredLogs.length === 0 ? (
                            <div className="flex items-center justify-center h-full text-muted-foreground">
                                暂无日志
                            </div>
                        ) : (
                            <div className="space-y-1">
                                {filteredLogs.map((log) => (
                                    <div key={log.id} className="flex gap-4 hover:bg-muted/10 px-2 py-1 rounded">
                                        <span className="text-muted-foreground whitespace-nowrap">
                                            [{log.timestamp}]
                                        </span>
                                        <span className="text-muted-foreground w-20">
                                            [{log.source}]
                                        </span>
                                        <span className={`w-16 font-semibold ${levelStyles[log.level]}`}>
                                            {log.level}
                                        </span>
                                        <span className="text-terminal-foreground flex-1">
                                            {log.message}
                                        </span>
                                    </div>
                                ))}
                            </div>
                        )}
                    </div>
                </CardContent>
            </Card>

            {/* Status bar */}
            <div className="flex items-center justify-between mt-4 text-sm text-muted-foreground">
                <span>{filteredLogs.length} 条日志</span>
                <div className="flex items-center gap-4">
                    <span className="flex items-center gap-2">
                        {connected ? (
                            <Wifi className="h-4 w-4 text-success" />
                        ) : (
                            <WifiOff className="h-4 w-4 text-destructive" />
                        )}
                        {connected ? "已连接" : "未连接"}
                    </span>
                    <span className="flex items-center gap-2">
                        <div className={`h-2 w-2 rounded-full ${isPaused ? "bg-warning" : "bg-success animate-pulse"}`} />
                        {isPaused ? "已暂停" : "实时更新中"}
                    </span>
                </div>
            </div>
        </AppLayout>
    );
}
