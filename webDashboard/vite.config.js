import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// The Pi serves the build from webDashboard/dist behind FastAPI, so every asset URL stays relative
export default defineConfig({
    plugins: [react()],
    base: "./",
    build: {
        outDir: "dist",
        emptyOutDir: true,
        sourcemap: false,
        chunkSizeWarningLimit: 1200,
    },
    server: {
        // Port 3000 is the origin already whitelisted in config.yaml -> api.allowedOrigins
        port: 3000,
        host: true,
        proxy: {
            "/api": {
                target: "http://127.0.0.1:8000",
                changeOrigin: true,
                ws: true,
            },
        },
    },
});
