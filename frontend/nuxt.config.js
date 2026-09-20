// Nuxt configuration. The backend API is reached through the Vite/Nuxt
// dev proxy in development and through the compose network in production
// (server-side base URL is set via NUXT_PUBLIC_API_BASE / internal alias).
export default defineNuxtConfig({
  ssr: false,
  devtools: { enabled: false },
  css: ['~/assets/css/main.css'],
  nitro: {
    // Emit a static SPA bundle (.output/public) served directly by nginx.
    preset: process.env.NUXT_PRESET || 'node-server',
  },
  app: {
    head: {
      title: 'TaskForge · 异步任务控制台',
      meta: [
        { charset: 'utf-8' },
        { name: 'viewport', content: 'width=device-width, initial-scale=1' },
      ],
    },
  },
  runtimeConfig: {
    public: {
      // Browser-facing base. Empty means same-origin (proxied by nginx/compose).
      apiBase: process.env.NUXT_PUBLIC_API_BASE || '',
    },
  },
})
