import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import tailwindcss from '@tailwindcss/vite'

// https://vite.dev/config/
export default defineConfig({
  plugins: [react(), tailwindcss()],
  base: '/surgedps',
  server: {
    proxy: {
      // Forward /surgedps/api/* to the Python cell server during local dev.
      // Rewrite strips the /surgedps prefix so the backend receives /api/...
      // (FastAPI routes are defined at /api/..., not /surgedps/api/...).
      '/surgedps/api': {
        target: 'http://localhost:8000',
        changeOrigin: true,
        rewrite: (path) => path.replace(/^\/surgedps/, ''),
      },
    },
  },
})
