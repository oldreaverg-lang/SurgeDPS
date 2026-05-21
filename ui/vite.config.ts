import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import tailwindcss from '@tailwindcss/vite'

// https://vite.dev/config/
export default defineConfig({
  plugins: [react(), tailwindcss()],
  base: '/surgedps',
  build: {
    // Manual chunk strategy targets the mobile-LCP path called out by
    // Lighthouse: pre-split, index-*.js was 1.44 MB raw / 393 KB gzipped
    // and took 5.5 s to load on mobile. The heavy dep is maplibre-gl
    // (~1 MB raw / ~340 KB gzipped). Splitting it into its own chunk
    // lets the browser parallelize the download AND lets a code-only
    // deploy keep the maplibre chunk cached across versions — content
    // hash changes only when maplibre itself changes (rare) instead of
    // every push.
    //
    // React goes in its own chunk for the same reason — it's stable
    // across our deploys, so users who already have it from a previous
    // visit skip re-downloading on the next one.
    rollupOptions: {
      output: {
        manualChunks(id) {
          // Heavy map renderer
          if (id.includes('node_modules/maplibre-gl')
              || id.includes('node_modules/@mapbox/')
              || id.includes('node_modules/@maplibre/')) {
            return 'maplibre'
          }
          // React core
          if (id.includes('node_modules/react/')
              || id.includes('node_modules/react-dom/')
              || id.includes('node_modules/scheduler/')) {
            return 'vendor-react'
          }
          // Geospatial helpers (turf, polygon-clipping, geojson-vt) —
          // used by hotspot dedupe + cell math. Stable across deploys.
          if (id.includes('node_modules/@turf/')
              || id.includes('node_modules/polygon-clipping')
              || id.includes('node_modules/supercluster')
              || id.includes('node_modules/geojson-vt')) {
            return 'vendor-geo'
          }
          // Everything else from node_modules
          if (id.includes('node_modules/')) {
            return 'vendor'
          }
          // App code stays in the default index chunk
        },
      },
    },
    // Bump the warn threshold so a single 500 KB+ vendor chunk isn't a
    // noisy warning. Real concern is the main index chunk size, which
    // should now sit well under this.
    chunkSizeWarningLimit: 700,
  },
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
