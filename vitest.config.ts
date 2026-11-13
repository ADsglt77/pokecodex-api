/**
 * Tests du Worker dans le moteur de Cloudflare (workerd) : D1 migrée et R2 vides, propres à chaque
 * fichier de test. Aucun appel réseau : TCGdex est simulé (`tests/helpers.ts`).
 */

import { cloudflareTest, readD1Migrations } from '@cloudflare/vitest-plugin'
import { defineConfig } from 'vitest/config'

export default defineConfig(async () => ({
  plugins: [
    cloudflareTest({
      wrangler: { configPath: './wrangler.jsonc' },
      miniflare: {
        bindings: {
          ADMIN_TOKEN: 'jeton-de-test-admin-0123456789abcdef-0123456789',
          MIGRATIONS: await readD1Migrations('migrations'),
        },
      },
    }),
  ],
  test: { include: ['tests/**/*.test.ts'], setupFiles: ['./tests/setup.ts'] },
}))
