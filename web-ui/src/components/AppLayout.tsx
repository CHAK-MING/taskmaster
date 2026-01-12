import { ReactNode, useState, useEffect } from "react";
import { SidebarProvider, SidebarTrigger, SidebarInset } from "@/components/ui/sidebar";
import { AppSidebar } from "@/components/AppSidebar";
import { ThemeToggle } from "@/components/ThemeToggle";
import { Bell, CheckCircle2, XCircle, AlertCircle } from "lucide-react";
import {
    DropdownMenu,
    DropdownMenuContent,
    DropdownMenuItem,
    DropdownMenuTrigger,
    DropdownMenuSeparator,
} from "@/components/ui/dropdown-menu";
import { Button } from "@/components/ui/button";

interface AppLayoutProps {
    children: ReactNode;
    title?: string;
    subtitle?: string;
}

interface Notification {
    id: string;
    type: "success" | "error" | "info";
    message: string;
    time: Date;
}

export function AppLayout({ children, title, subtitle }: AppLayoutProps) {
    const [notifications, setNotifications] = useState<Notification[]>([]);

    useEffect(() => {
        const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
        const wsUrl = `${protocol}//${window.location.host}/ws/logs`;
        const ws = new WebSocket(wsUrl);

        ws.onmessage = (event) => {
            try {
                const data = JSON.parse(event.data);

                // Handle log messages (type: 'log')
                if (data.type === "log") {
                    const content = data.content || "";
                    const isError = data.stream === "stderr" || content.toLowerCase().includes("error") || content.toLowerCase().includes("failed");
                    const isSuccess = content.toLowerCase().includes("success") || content.toLowerCase().includes("completed");

                    if (isError || isSuccess) {
                        const newNotification: Notification = {
                            id: Date.now().toString(),
                            type: isError ? "error" : "success",
                            message: content || "Unknown event",
                            time: new Date(),
                        };
                        setNotifications(prev => [newNotification, ...prev].slice(0, 20));
                    }
                }

                // Handle event messages (type: 'event')
                if (data.type === "event" && data.data) {
                    try {
                        const eventData = JSON.parse(data.data);
                        const isError = eventData.status === "failed";
                        const isSuccess = eventData.status === "success";

                        if (data.event === "dag_run_completed") {
                            const newNotification: Notification = {
                                id: Date.now().toString(),
                                type: isError ? "error" : isSuccess ? "success" : "info",
                                message: `DAG run ${isSuccess ? "completed" : isError ? "failed" : "finished"}`,
                                time: new Date(),
                            };
                            setNotifications(prev => [newNotification, ...prev].slice(0, 20));
                        }
                    } catch { }
                }
            } catch { }
        };

        return () => ws.close();
    }, []);

    const clearNotifications = () => setNotifications([]);

    const notificationIcon = (type: string) => {
        switch (type) {
            case "success": return <CheckCircle2 className="h-4 w-4 text-success" />;
            case "error": return <XCircle className="h-4 w-4 text-destructive" />;
            default: return <AlertCircle className="h-4 w-4 text-muted-foreground" />;
        }
    };

    return (
        <SidebarProvider>
            <div className="flex min-h-screen w-full">
                <AppSidebar />
                <SidebarInset className="flex flex-1 flex-col">
                    {/* Header */}
                    <header className="sticky top-0 z-10 flex h-16 items-center justify-between border-b border-border bg-background/95 backdrop-blur supports-[backdrop-filter]:bg-background/60 px-6">
                        <div className="flex items-center gap-4">
                            <SidebarTrigger className="h-9 w-9" />
                            {title && (
                                <div className="flex flex-col">
                                    <h1 className="text-lg font-semibold text-foreground">{title}</h1>
                                    {subtitle && (
                                        <p className="text-sm text-muted-foreground">{subtitle}</p>
                                    )}
                                </div>
                            )}
                        </div>

                        <div className="flex items-center gap-3">
                            {/* Notifications */}
                            <DropdownMenu>
                                <DropdownMenuTrigger asChild>
                                    <Button variant="ghost" size="icon" className="relative h-9 w-9">
                                        <Bell className="h-5 w-5" />
                                        {notifications.length > 0 && (
                                            <span className="absolute -top-0.5 -right-0.5 h-4 w-4 rounded-full bg-destructive text-[10px] font-medium text-destructive-foreground flex items-center justify-center">
                                                {notifications.length > 9 ? "9+" : notifications.length}
                                            </span>
                                        )}
                                    </Button>
                                </DropdownMenuTrigger>
                                <DropdownMenuContent align="end" className="w-80">
                                    <div className="flex items-center justify-between px-3 py-2">
                                        <span className="font-medium">通知</span>
                                        {notifications.length > 0 && (
                                            <button onClick={clearNotifications} className="text-xs text-muted-foreground hover:text-foreground">
                                                清除全部
                                            </button>
                                        )}
                                    </div>
                                    <DropdownMenuSeparator />
                                    {notifications.length === 0 ? (
                                        <div className="px-3 py-6 text-center text-sm text-muted-foreground">
                                            暂无通知
                                        </div>
                                    ) : (
                                        <div className="max-h-80 overflow-y-auto">
                                            {notifications.map((n) => (
                                                <DropdownMenuItem key={n.id} className="flex items-start gap-2 px-3 py-2">
                                                    {notificationIcon(n.type)}
                                                    <div className="flex-1 min-w-0">
                                                        <p className="text-sm truncate">{n.message}</p>
                                                        <p className="text-xs text-muted-foreground">
                                                            {n.time.toLocaleTimeString()}
                                                        </p>
                                                    </div>
                                                </DropdownMenuItem>
                                            ))}
                                        </div>
                                    )}
                                </DropdownMenuContent>
                            </DropdownMenu>

                            {/* Theme Toggle */}
                            <ThemeToggle />
                        </div>
                    </header>

                    {/* Main Content */}
                    <main className="flex-1 p-6">
                        {children}
                    </main>
                </SidebarInset>
            </div>
        </SidebarProvider>
    );
}
