import { defineConfig } from "vite";
import react from "@vitejs/plugin-react-swc";
import path from "path";
import { componentTagger } from "lovable-tagger";

// https://vitejs.dev/config/
export default defineConfig(({ mode }) => ({
    server: {
        // VS Code port forwarding: browser hits http://localhost:8080
        // while this dev server runs remotely.
        host: true,
        port: 8080,
        strictPort: true,
        hmr: {
            host: 'localhost',
            port: 8080,
            clientPort: 8080,
            protocol: 'ws',
        },
        proxy: {
            '/api': {
                target: 'http://127.0.0.1:8888',
                changeOrigin: true,
            },
            '/ws': {
                target: 'ws://127.0.0.1:8888',
                ws: true,
            },
        },
    },
    plugins: [react(), mode === "development" && componentTagger()].filter(Boolean),
    resolve: {
        alias: {
            "@": path.resolve(__dirname, "./src"),
        },
    },
}));
