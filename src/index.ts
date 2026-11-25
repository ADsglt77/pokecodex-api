/**
 * PokéCodex — importateur TCGdex FR/JP.
 *
 *   fetch     : API d'administration protégée (`/admin/*`) et `/health` (sans aucune donnée).
 *   scheduled : Cron `PROCESS_CRON` (toutes les 5 min) → une exécution du traitement des tâches ;
 *               Cron `SYNC_CRON` (lundi 03:07 UTC) → synchronisation hebdomadaire du catalogue.
 * Tout le travail est persistant (D1) et repris par le Cron suivant : l'importation continue sans
 * aucun processus local.
 */

import { handleAdmin, json } from './admin/router'
import { parseConfig, type Config, type ConfigSource } from './config'
import { Db } from './db'
import { runTick } from './services/runner'
import { imagesBindingAdapter } from './services/set-media-import'
import { startSync } from './services/sync'
import { log, logError } from './utils/log'

/** Variables textuelles du Worker, lues par `parseConfig`. */
export function configFromEnv(env: Env): Config {
  const source: ConfigSource = {}
  for (const [key, value] of Object.entries(env)) if (typeof value === 'string') source[key] = value
  return parseConfig(source)
}

export default {
  async fetch(request, env, ctx): Promise<Response> {
    const url = new URL(request.url)
    if (url.pathname === '/health') return new Response('ok\n', { headers: { 'content-type': 'text/plain', 'cache-control': 'no-store' } })
    if (url.pathname === '/admin' || url.pathname.startsWith('/admin/')) {
      const config = configFromEnv(env)
      return handleAdmin(request, {
        config,
        d1: env.DB,
        bucket: env.BUCKET,
        fetcher: (input, init) => fetch(input, init),
        adminToken: env.ADMIN_TOKEN,
        waitUntil: (promise) => ctx.waitUntil(promise),
        mediaImages: imagesBindingAdapter(env.IMAGES, config.maxImageBytes),
      })
    }
    return json({ error: 'route inconnue' }, 404)
  },

  async scheduled(controller, env): Promise<void> {
    const config = configFromEnv(env)
    try {
      if (controller.cron === config.syncCron) {
        const result = await startSync(new Db(env.DB), config, Date.now(), { kind: 'weekly' })
        log('weekly_sync', { ...result })
        return
      }
      if (controller.cron !== config.processCron) log('unknown_cron', { cron: controller.cron })
      await runTick({ config, d1: env.DB, bucket: env.BUCKET, fetcher: (input, init) => fetch(input, init) }, { trigger: 'cron' })
    } catch (error) {
      logError('scheduled_error', error, { cron: controller.cron })
      throw error
    }
  },
} satisfies ExportedHandler<Env>
