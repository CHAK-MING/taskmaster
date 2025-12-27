import { useLocation, Link } from "react-router-dom";
import { Home, ArrowLeft, Search } from "lucide-react";
import { Button } from "@/components/ui/button";

const NotFound = () => {
  const location = useLocation();

  return (
    <div className="flex min-h-screen items-center justify-center bg-gradient-to-br from-background via-background to-muted/50 p-4">
      {/* Background decorative elements */}
      <div className="absolute inset-0 overflow-hidden pointer-events-none">
        <div className="absolute top-1/4 left-1/4 w-96 h-96 bg-primary/5 rounded-full blur-3xl" />
        <div className="absolute bottom-1/4 right-1/4 w-80 h-80 bg-accent/10 rounded-full blur-3xl" />
      </div>

      <div className="relative text-center max-w-lg mx-auto">
        {/* 404 Number with gradient */}
        <div className="relative mb-8">
          <h1 className="text-[12rem] font-black leading-none tracking-tighter bg-gradient-to-b from-foreground via-foreground/80 to-foreground/20 bg-clip-text text-transparent select-none">
            404
          </h1>
          <div className="absolute inset-0 flex items-center justify-center">
            <Search className="w-16 h-16 text-muted-foreground/30 animate-pulse" />
          </div>
        </div>

        {/* Error message */}
        <div className="space-y-3 mb-10">
          <h2 className="text-2xl font-semibold text-foreground">
            页面不存在
          </h2>
          <p className="text-muted-foreground text-lg">
            抱歉，您访问的页面 <code className="px-2 py-1 bg-muted rounded-md text-sm font-mono text-primary">{location.pathname}</code> 未找到
          </p>
        </div>

        {/* Action buttons */}
        <div className="flex flex-col sm:flex-row items-center justify-center gap-4">
          <Button asChild size="lg" className="min-w-[160px]">
            <Link to="/">
              <Home className="w-4 h-4 mr-2" />
              返回首页
            </Link>
          </Button>
          <Button asChild variant="outline" size="lg" className="min-w-[160px]">
            <Link to="/dags">
              <ArrowLeft className="w-4 h-4 mr-2" />
              查看 DAGs
            </Link>
          </Button>
        </div>

        {/* Decorative bottom line */}
        <div className="mt-16 flex items-center justify-center gap-2">
          <div className="h-px w-12 bg-gradient-to-r from-transparent to-border" />
          <span className="text-xs text-muted-foreground/60 uppercase tracking-widest">TaskMaster</span>
          <div className="h-px w-12 bg-gradient-to-l from-transparent to-border" />
        </div>
      </div>
    </div>
  );
};

export default NotFound;
