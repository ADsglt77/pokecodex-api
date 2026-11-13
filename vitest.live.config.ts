/**
 * Test d'intégration RÉEL contre l'API et le CDN de TCGdex (`npm run test:live`), dans workerd,
 * avec le code de production (client, parseurs, validation WebP). Une quinzaine de requêtes
 * espacées : à lancer à la main, jamais dans la boucle de tests ordinaire (`npm test`).
 */

import { cloudflareTest } from '@cloudflare/vitest-plugin'
import { defineConfig } from 'vitest/config'

export default defineConfig({
  plugins: [cloudflareTest({ wrangler: { configPath: './wrangler.jsonc' } })],
  test: { include: ['live/**/*.live.test.ts'], testTimeout: 60_000 },
})
