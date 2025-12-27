import { LucideIcon } from "lucide-react";
import { Card, CardContent } from "@/components/ui/card";
import { cn } from "@/lib/utils";

interface StatCardProps {
    title: string;
    value: string | number;
    icon: LucideIcon;
    trend?: {
        value: number;
        isPositive: boolean;
    };
    variant?: "default" | "success" | "warning" | "destructive";
}

const variantStyles = {
    default: "bg-primary/10 text-primary",
    success: "bg-success/10 text-success",
    warning: "bg-warning/10 text-warning",
    destructive: "bg-destructive/10 text-destructive",
};

export function StatCard({ title, value, icon: Icon, trend, variant = "default" }: StatCardProps) {
    return (
        <Card className="overflow-hidden transition-all hover:shadow-md hover:border-primary/20">
            <CardContent className="p-6">
                <div className="flex items-start justify-between">
                    <div className="flex flex-col gap-1">
                        <span className="text-sm font-medium text-muted-foreground">{title}</span>
                        <span className="text-3xl font-bold text-foreground">{value}</span>
                        {trend && (
                            <div className="flex items-center gap-1 mt-1">
                                <span
                                    className={cn(
                                        "text-xs font-medium",
                                        trend.isPositive ? "text-success" : "text-destructive"
                                    )}
                                >
                                    {trend.isPositive ? "↑" : "↓"} {Math.abs(trend.value)}%
                                </span>
                                <span className="text-xs text-muted-foreground">vs 昨日</span>
                            </div>
                        )}
                    </div>
                    <div className={cn("rounded-xl p-3", variantStyles[variant])}>
                        <Icon className="h-6 w-6" />
                    </div>
                </div>
            </CardContent>
        </Card>
    );
}
