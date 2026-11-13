/** Avant chaque fichier de test : la D1 migrée comme en production (`migrations/`). */

import { applyD1Migrations } from 'cloudflare:test'
import { env } from 'cloudflare:workers'

await applyD1Migrations(env.DB, env.MIGRATIONS)
