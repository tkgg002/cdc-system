import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// https://vite.dev/config/
export default defineConfig({
  plugins: [react()],
  build: {
    // Split heavy third-party libs into dedicated vendor chunks so they stay
    // cacheable across page navigations and the main app bundle shrinks.
    rollupOptions: {
      output: {
        manualChunks(id) {
          if (!id.includes('node_modules')) return;
          if (id.includes('/react-router') || id.match(/\/(react|react-dom|scheduler)\//)) {
            return 'vendor-react';
          }
          if (id.includes('/@ant-design/icons')) {
            return 'vendor-antd-icons';
          }
          if (id.includes('/antd/') || id.includes('/@ant-design/') || id.includes('/rc-')) {
            return 'vendor-antd';
          }
          if (id.includes('/@tanstack/') || id.includes('/axios/')) {
            return 'vendor-query';
          }
          return 'vendor-misc';
        },
      },
    },
    // Raised from default 500 KB: vendor-antd core alone is ~1 MB (raw) /
    // ~335 KB gzip (Ant Design v6 bundles every component class). It is
    // isolated into a dedicated vendor chunk so the browser caches it
    // across navigations — route chunks all stay well under 20 KB.
    chunkSizeWarningLimit: 1100,
  },
})
