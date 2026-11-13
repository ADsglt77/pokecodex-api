/** Ce que les tests reçoivent en plus (`vitest.config.ts`) : les migrations à appliquer. */
declare namespace Cloudflare {
  interface Env {
    MIGRATIONS: import('cloudflare:test').D1Migration[]
  }
}
