import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
  },
  build: {
    outDir: "build",
    emptyOutDir: true,
  },
  define: {
    "process.env.REACT_APP_API_URL": JSON.stringify(process.env.REACT_APP_API_URL || ""),
  },
});
