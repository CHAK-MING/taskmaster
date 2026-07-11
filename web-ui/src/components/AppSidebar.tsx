import { NavLink, useLocation } from "react-router-dom";
import {
    LayoutDashboard,
    GitBranch,
    Settings,
} from "lucide-react";
import {
    Sidebar,
    SidebarContent,
    SidebarGroup,
    SidebarGroupContent,
    SidebarGroupLabel,
    SidebarMenu,
    SidebarMenuButton,
    SidebarMenuItem,
    SidebarHeader,
    SidebarFooter,
    useSidebar,
} from "@/components/ui/sidebar";
import { cn } from "@/lib/utils";
import { useI18n } from "@/contexts/I18nContext";

export function AppSidebar() {
    const { t } = useI18n();
    const { state } = useSidebar();
    const collapsed = state === "collapsed";
    const location = useLocation();

    const isActive = (path: string) => {
        if (path === "/") return location.pathname === "/";
        return location.pathname.startsWith(path);
    };

    const mainNavItems = [
        { title: t.nav.dashboard, url: "/", icon: LayoutDashboard },
        { title: t.nav.dags, url: "/dags", icon: GitBranch },
    ];

    const systemNavItems = [
        { title: t.nav.settings, url: "/settings", icon: Settings },
    ];

    return (
        <Sidebar collapsible="icon" className="border-r border-sidebar-border">
            <SidebarHeader className="p-4">
                <div className="flex items-center gap-3">
                    <div className="flex shrink-0 h-10 w-10 items-center justify-center rounded-lg overflow-hidden bg-black ring-1 ring-border shadow-sm">
                        <img src="/logo.png" alt="DAGForge" className="h-full w-full object-cover" />
                    </div>
                    {!collapsed && (
                        <div className="flex flex-col">
                            <span className="text-lg font-bold text-foreground">DAGForge</span>
                            <span className="text-xs text-muted-foreground">{t.sidebar.mainFeatures}</span>
                        </div>
                    )}
                </div>
            </SidebarHeader>

            <SidebarContent className="px-2">
                <SidebarGroup>
                    <SidebarGroupLabel className="text-xs font-medium text-muted-foreground uppercase tracking-wider">
                        {t.sidebar.mainFeatures}
                    </SidebarGroupLabel>
                    <SidebarGroupContent>
                        <SidebarMenu>
                            {mainNavItems.map((item) => (
                                <SidebarMenuItem key={item.title}>
                                    <SidebarMenuButton
                                        asChild
                                        isActive={isActive(item.url)}
                                        tooltip={item.title}
                                    >
                                        <NavLink
                                            to={item.url}
                                            className={cn(
                                                "flex items-center gap-3 rounded-lg px-3 py-2.5 transition-colors",
                                                isActive(item.url)
                                                    ? "bg-sidebar-accent text-sidebar-accent-foreground font-medium"
                                                    : "text-sidebar-foreground hover:bg-sidebar-accent/50"
                                            )}
                                        >
                                            <item.icon className="h-5 w-5 shrink-0" />
                                            {!collapsed && <span>{item.title}</span>}
                                        </NavLink>
                                    </SidebarMenuButton>
                                </SidebarMenuItem>
                            ))}
                        </SidebarMenu>
                    </SidebarGroupContent>
                </SidebarGroup>

                <SidebarGroup className="mt-4">
                    <SidebarGroupLabel className="text-xs font-medium text-muted-foreground uppercase tracking-wider">
                        {t.sidebar.system}
                    </SidebarGroupLabel>
                    <SidebarGroupContent>
                        <SidebarMenu>
                            {systemNavItems.map((item) => (
                                <SidebarMenuItem key={item.title}>
                                    <SidebarMenuButton
                                        asChild
                                        isActive={isActive(item.url)}
                                        tooltip={item.title}
                                    >
                                        <NavLink
                                            to={item.url}
                                            className={cn(
                                                "flex items-center gap-3 rounded-lg px-3 py-2.5 transition-colors",
                                                isActive(item.url)
                                                    ? "bg-sidebar-accent text-sidebar-accent-foreground font-medium"
                                                    : "text-sidebar-foreground hover:bg-sidebar-accent/50"
                                            )}
                                        >
                                            <item.icon className="h-5 w-5 shrink-0" />
                                            {!collapsed && <span>{item.title}</span>}
                                        </NavLink>
                                    </SidebarMenuButton>
                                </SidebarMenuItem>
                            ))}
                        </SidebarMenu>
                    </SidebarGroupContent>
                </SidebarGroup>
            </SidebarContent>

            <SidebarFooter className="p-4">
                {!collapsed && (
                    <div className="flex items-center gap-2 rounded-lg bg-success/10 px-3 py-2 text-sm">
                        <div className="h-2 w-2 rounded-full bg-success animate-pulse-glow" />
                        <span className="text-success font-medium">{t.sidebar.apiConnected}</span>
                    </div>
                )}
                {collapsed && (
                    <div className="flex justify-center">
                        <div className="h-3 w-3 rounded-full bg-success animate-pulse-glow" />
                    </div>
                )}
            </SidebarFooter>
        </Sidebar>
    );
}
