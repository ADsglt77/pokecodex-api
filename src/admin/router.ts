/**
 * API d'administration (toutes les routes `/admin/*` exigent le jeton ADMIN_TOKEN).
 *
 *   GET  /admin/status                 rapport texte (?format=json pour du JSON)
 *   GET  /admin/stats                  statistiques détaillées (?sets=1 : détail par extension)
 *   GET  /admin/missing                cartes sans image (?language=fr|ja facultatif, &limit=&after=)
 *   GET  /admin/errors                 erreurs (images en erreur, tâches en échec)
 *   GET  /admin/reports                rapports enregistrés dans R2 (?prefix=latest/|history/&cursor=)
 *   GET  /admin/reports/<chemin>       contenu d'un rapport (ex. latest/missing_fr.csv)
 *   GET  /admin/r2                     objets R2 de l'importateur (?prefix=cards/fr/swsh3/&limit=)
 *   POST /admin/sync/start             démarre une synchronisation (?language=fr,ja&sets=swsh3,SV1a)
 *   POST /admin/sync/pause             met en pause
 *   POST /admin/sync/resume            reprend (lève aussi une attente imposée, signalée en retour)
 *   POST /admin/sync/cancel            abandonne les synchronisations en cours (?language=)
 *   POST /admin/sync/retry-missing     relance images manquantes / en erreur et tâches en échec
 *   POST /admin/sync/backfill-media    crée les tâches des petites images, logos et symboles manquants (?language=)
 *   POST /admin/reports/generate       demande un nouvel audit et de nouveaux exports
 *   POST /admin/tick                   lance une exécution courte (10 s au plus) tout de suite (?max_jobs=)
 *   POST /admin/selftest               vérifie D1, R2 et l'accès à TCGdex
 *   POST /admin/import/catalog         ajoute extensions et cartes d'une autre source (JSON, jamais d'écrasement)
 *   POST /admin/import/image           envoie l'image d'une carte ou le logo d'une extension (corps WebP ;
 *                                      ?kind=main|low|logo&language=fr|ja&target=<id>&source=<adresse d'origine>)
 *   POST /admin/import/symbol          copie le symbole d'une extension, que le Worker télécharge lui-même
 *                                      (?language=fr|ja&target=<id>&source=<adresse TCGdex ou Pokécardex>)
 *   POST /admin/import/logo            copie le logo d'une extension, que le Worker télécharge lui-même
 *                                      (?language=fr|ja&target=<id>&source=<adresse TCGdex>)
 */

import { IMAGES_PREFIX, isLanguage, REPORTS_PREFIX, type Config, type Language } from '../config'
import { Db } from '../db'
import { ERROR_IMAGE_STATUSES, MISSING_IMAGE_STATUSES } from '../repositories/catalog'
import { enqueueStmts } from '../repositories/jobs'
import { importCatalog, InputError, MAX_CATALOG_BYTES, storeImage, type ImageKind } from '../services/manual-import'
import { failedJobs, problemCards, setCoverage } from '../services/reports'
import { storeCardFromUrl, storeSetMedia, type SetMediaImages } from '../services/set-media-import'
import { runTick } from '../services/runner'
import { collectStats, formatStatus } from '../services/stats'
import { backfillMedia, cancelSync, pauseSync, resumeSync, retryMissing, startSync } from '../services/sync'
import { Health, TcgdexClient } from '../tcgdex/client'
import { parseSeriesList } from '../tcgdex/types'
import { RateLimiter } from '../utils/limiter'
import { errorMessage } from '../utils/log'
import { iso } from '../utils/time'
import { checkAdminAuth } from './auth'

export interface AdminDeps {
  config: Config
  d1: D1Database
  bucket: R2Bucket
  fetcher: typeof fetch
  adminToken: string | undefined
  now?: () => number
  /** Prolonge le travail après une déconnexion du client (`ctx.waitUntil`, 30 s au plus). */
  waitUntil?: (promise: Promise<unknown>) => void
  /** Dimensions et conversion en WebP des logos et symboles copiés (liaison Cloudflare Images). */
  mediaImages?: SetMediaImages
}

/**
 * Exécution lancée par `POST /admin/tick` : elle doit finir dans les 30 s que `waitUntil` accorde
 * après une déconnexion du client, dépassement compris (10 s de budget, requêtes de 4 s au plus).
 */
const ADMIN_TICK_MAX_MS = 10_000
const ADMIN_FETCH_TIMEOUT_MS = 4_000

const SECURITY_HEADERS = {
  'cache-control': 'no-store',
  'x-content-type-options': 'nosniff',
  'referrer-policy': 'no-referrer',
}

export const json = (body: unknown, status = 200): Response =>
  new Response(`${JSON.stringify(body, null, 2)}\n`, { status, headers: { 'content-type': 'application/json; charset=utf-8', ...SECURITY_HEADERS } })

const text = (body: string, status = 200): Response =>
  new Response(body, { status, headers: { 'content-type': 'text/plain; charset=utf-8', ...SECURITY_HEADERS } })

const GET_ROUTES = ['/admin/status', '/admin/stats', '/admin/missing', '/admin/errors', '/admin/reports', '/admin/r2']
const POST_ROUTES = [
  '/admin/sync/start',
  '/admin/sync/pause',
  '/admin/sync/resume',
  '/admin/sync/cancel',
  '/admin/sync/retry-missing',
  '/admin/sync/backfill-media',
  '/admin/reports/generate',
  '/admin/tick',
  '/admin/selftest',
  '/admin/import/catalog',
  '/admin/import/image',
  '/admin/import/symbol',
  '/admin/import/logo',
  '/admin/import/card',
]

/** Langues demandées (`?language=fr,ja`), toutes par défaut ; `null` si une valeur est inconnue. */
function languagesParam(url: URL, config: Config): Language[] | null {
  const raw = url.searchParams.get('language')
  if (!raw) return config.languages
  const values = raw.split(',').map((value) => value.trim())
  return values.every(isLanguage) ? (values as Language[]) : null
}

const intParam = (url: URL, name: string, fallback: number, min: number, max: number): number => {
  const value = Number(url.searchParams.get(name))
  return Number.isFinite(value) && url.searchParams.has(name) ? Math.min(max, Math.max(min, Math.floor(value))) : fallback
}

const flagParam = (url: URL, name: string): boolean => ['1', 'true'].includes(url.searchParams.get(name) ?? '')

/** Chemin relatif sûr (pas de segment vide, `.` ou `..`, encodage valide), ou `null`. */
function safeRelativePath(raw: string): string | null {
  let decoded: string
  try {
    decoded = decodeURIComponent(raw)
  } catch {
    return null
  }
  if (!decoded || decoded.split('/').some((part) => part === '' || part === '.' || part === '..')) return null
  return decoded
}

export async function handleAdmin(request: Request, deps: AdminDeps): Promise<Response> {
  const auth = await checkAdminAuth(request, deps.adminToken)
  if (!auth.ok) return json({ error: auth.message }, auth.status)
  const url = new URL(request.url)
  const path = url.pathname.replace(/\/+$/, '')
  const method = request.method.toUpperCase()
  const now = deps.now ?? Date.now
  const db = new Db(deps.d1)
  const { config } = deps

  try {
    // Une route connue appelée avec une autre méthode que la sienne : 405 (avant les chemins à paramètre).
    if ((GET_ROUTES.includes(path) && method !== 'GET') || (POST_ROUTES.includes(path) && method !== 'POST')) {
      return json({ error: 'méthode non autorisée' }, 405)
    }
    if (path.startsWith('/admin/reports/') && !POST_ROUTES.includes(path) && method !== 'GET') return json({ error: 'méthode non autorisée' }, 405)

    if (method === 'GET') {
      if (path === '/admin/status') {
        const stats = await collectStats(db, config, now())
        return url.searchParams.get('format') === 'json' ? json(stats) : text(formatStatus(stats))
      }
      if (path === '/admin/stats') {
        const stats = await collectStats(db, config, now())
        return json(flagParam(url, 'sets') ? { ...stats, sets: await setCoverage(db) } : stats)
      }
      if (path === '/admin/missing') {
        const languages = languagesParam(url, config)
        if (!languages) return json({ error: 'langue inconnue (fr ou ja)' }, 400)
        const language = url.searchParams.has('language') && languages.length === 1 ? (languages[0] ?? null) : null
        const limit = intParam(url, 'limit', 100, 1, 1_000)
        const items = await problemCards(db, MISSING_IMAGE_STATUSES, { language, after: url.searchParams.get('after') ?? '', limit })
        return json({ count: items.length, next: items.length === limit ? items.at(-1)?.id : null, items })
      }
      if (path === '/admin/errors') {
        const limit = intParam(url, 'limit', 100, 1, 1_000)
        const cards = await problemCards(db, ERROR_IMAGE_STATUSES, { language: null, after: url.searchParams.get('after') ?? '', limit })
        const jobs = await failedJobs(db, { afterId: intParam(url, 'after_job', 0, 0, Number.MAX_SAFE_INTEGER), limit })
        return json({ images: cards, nextImages: cards.length === limit ? cards.at(-1)?.id : null, failedJobs: jobs, nextJobs: jobs.length === limit ? jobs.at(-1)?.id : null })
      }
      if (path === '/admin/reports') {
        const sub = url.searchParams.get('prefix') ?? ''
        if (sub && (sub.includes('..') || sub.startsWith('/'))) return json({ error: 'préfixe invalide' }, 400)
        const cursor = url.searchParams.get('cursor')
        const listed = await deps.bucket.list({ prefix: `${REPORTS_PREFIX}${sub}`, limit: 1_000, ...(cursor ? { cursor } : {}) })
        return json({
          reports: listed.objects.map((object) => ({ key: object.key, size: object.size, uploaded: object.uploaded.toISOString() })),
          cursor: listed.truncated ? listed.cursor : null,
        })
      }
      if (path.startsWith('/admin/reports/')) {
        const relative = safeRelativePath(path.slice('/admin/reports/'.length))
        if (!relative) return json({ error: 'chemin invalide' }, 400)
        const object = await deps.bucket.get(`${REPORTS_PREFIX}${relative}`)
        if (!object) return json({ error: 'rapport introuvable' }, 404)
        return new Response(object.body, { headers: { 'content-type': object.httpMetadata?.contentType ?? 'application/octet-stream', ...SECURITY_HEADERS } })
      }
      if (path === '/admin/r2') {
        // Vérification à distance des objets écrits (métadonnées seulement, jamais le contenu).
        const prefix = url.searchParams.get('prefix') ?? IMAGES_PREFIX
        if (!(prefix.startsWith(IMAGES_PREFIX) || prefix.startsWith(REPORTS_PREFIX)) || prefix.includes('..')) {
          return json({ error: `préfixe limité à ${IMAGES_PREFIX} ou ${REPORTS_PREFIX}` }, 400)
        }
        const cursor = url.searchParams.get('cursor')
        const listed = await deps.bucket.list({ prefix, limit: intParam(url, 'limit', 50, 1, 1_000), include: ['customMetadata', 'httpMetadata'], ...(cursor ? { cursor } : {}) })
        return json({
          objects: listed.objects.map((object) => ({
            key: object.key,
            size: object.size,
            uploaded: object.uploaded.toISOString(),
            contentType: object.httpMetadata?.contentType ?? null,
            metadata: object.customMetadata ?? {},
          })),
          cursor: listed.truncated ? listed.cursor : null,
        })
      }
    }

    if (method === 'POST') {
      if (path === '/admin/sync/start') {
        const languages = languagesParam(url, config)
        if (!languages) return json({ error: 'langue inconnue (fr ou ja)' }, 400)
        const sets = (url.searchParams.get('sets') ?? '').split(',').map((value) => value.trim()).filter(Boolean)
        return json(await startSync(db, config, now(), { languages, scope: sets.length ? { sets } : {} }))
      }
      if (path === '/admin/sync/pause') {
        await pauseSync(db, now())
        return json({ paused: true })
      }
      if (path === '/admin/sync/resume') {
        return json({ paused: false, ...(await resumeSync(db, now())) })
      }
      if (path === '/admin/sync/cancel') {
        const languages = languagesParam(url, config)
        if (!languages) return json({ error: 'langue inconnue (fr ou ja)' }, 400)
        return json(await cancelSync(db, now(), languages))
      }
      if (path === '/admin/sync/retry-missing') {
        const languages = languagesParam(url, config)
        if (!languages) return json({ error: 'langue inconnue (fr ou ja)' }, 400)
        return json(await retryMissing(db, config, now(), languages))
      }
      if (path === '/admin/sync/backfill-media') {
        const languages = languagesParam(url, config)
        if (!languages) return json({ error: 'langue inconnue (fr ou ja)' }, 400)
        const result = await backfillMedia(db, config, now(), languages)
        return json({ ...result, note: result.complete ? 'tâches créées : la prochaine exécution les traitera (après toutes les autres tâches)' : 'plafond de pages atteint : rappelez la route pour la suite' })
      }
      if (path === '/admin/reports/generate') {
        await db.batch(enqueueStmts(db, 'report', null, [{ target: null, cardId: null, dedupeKey: 'report:*:*', payload: { manual: true } }], { runId: null, maxAttempts: config.maxAttempts, nowMs: now() }))
        return json({ queued: true, note: 'le rapport sera produit par la prochaine exécution (ou POST /admin/tick)' })
      }
      if (path === '/admin/tick') {
        const tick = runTick(
          { config: { ...config, fetchTimeoutMs: Math.min(config.fetchTimeoutMs, ADMIN_FETCH_TIMEOUT_MS) }, d1: deps.d1, bucket: deps.bucket, fetcher: deps.fetcher, now },
          {
            trigger: 'admin',
            budgetMs: intParam(url, 'budget_ms', ADMIN_TICK_MAX_MS, 1_000, Math.min(ADMIN_TICK_MAX_MS, config.runTimeBudgetMs)),
            maxJobs: intParam(url, 'max_jobs', config.maxJobsPerRun, 1, config.maxJobsPerRun),
          },
        )
        // Si le client se déconnecte, l'exécution va quand même au bout (verrou libéré, bilan écrit).
        deps.waitUntil?.(tick.catch(() => undefined))
        return json(await tick)
      }
      if (path === '/admin/selftest') return json(await selfTest(deps, db, now))
      if (path === '/admin/import/catalog') {
        const bytes = await readBody(request, MAX_CATALOG_BYTES)
        if (!bytes) return json({ error: `corps JSON attendu (${MAX_CATALOG_BYTES} octets au plus)` }, 413)
        let raw: unknown
        try {
          raw = JSON.parse(new TextDecoder().decode(bytes))
        } catch {
          return json({ error: 'JSON invalide' }, 400)
        }
        return json(await importCatalog(db, raw, now()))
      }
      if (path === '/admin/import/image') {
        const kind = url.searchParams.get('kind') as ImageKind | null
        const language = url.searchParams.get('language')
        const target = url.searchParams.get('target') ?? ''
        const source = url.searchParams.get('source') ?? ''
        if (!kind || !['main', 'low', 'logo'].includes(kind)) return json({ error: 'kind : main, low ou logo' }, 400)
        if (!isLanguage(language)) return json({ error: 'langue inconnue (fr ou ja)' }, 400)
        if (!target || target.length > 80) return json({ error: 'target manquant' }, 400)
        if (!/^https:\/\/[^\s]{1,400}$/.test(source)) return json({ error: 'source : adresse https d’origine attendue' }, 400)
        const bytes = await readBody(request, config.maxImageBytes)
        if (!bytes) return json({ error: `corps WebP attendu (${config.maxImageBytes} octets au plus)` }, 413)
        const outcome = await storeImage(db, deps.bucket, config, { kind, language, target, sourceUrl: source, bytes }, now())
        const status = { stored: 201, reconciled: 200, unchanged: 200, conflict: 409, not_found: 404, invalid: 422 }[outcome.status]
        return json(outcome, status)
      }
      if (path === '/admin/import/card') {
        const language = url.searchParams.get('language')
        const target = url.searchParams.get('target') ?? ''
        if (!isLanguage(language)) return json({ error: 'langue inconnue (fr ou ja)' }, 400)
        if (!target || target.length > 80) return json({ error: 'target manquant' }, 400)
        if (!deps.mediaImages) return json({ error: 'liaison Images absente : conversion impossible' }, 500)
        const outcome = await storeCardFromUrl(db, deps.bucket, config, deps.fetcher, deps.mediaImages, { language, target, sourceUrl: url.searchParams.get('source') ?? '' }, now)
        const status = { stored: 201, reconciled: 200, unchanged: 200, conflict: 409, not_found: 404, invalid: 422, source_not_found: 424, source_refused: 424, source_error: 502 }[outcome.status]
        return json(outcome, status)
      }
      if (path === '/admin/import/symbol' || path === '/admin/import/logo') {
        const kind = path === '/admin/import/logo' ? 'logo' : 'symbol'
        const language = url.searchParams.get('language')
        const target = url.searchParams.get('target') ?? ''
        if (!isLanguage(language)) return json({ error: 'langue inconnue (fr ou ja)' }, 400)
        if (!target || target.length > 80) return json({ error: 'target manquant' }, 400)
        if (!deps.mediaImages) return json({ error: 'liaison Images absente : conversion impossible' }, 500)
        const request = { kind, language, target, sourceUrl: url.searchParams.get('source') ?? '' } as const
        const outcome = await storeSetMedia(db, deps.bucket, config, deps.fetcher, deps.mediaImages, request, now)
        // 424 : la source n'a pas le fichier (définitif) ; 502 : panne passagère de la source (à retenter).
        const status = { stored: 201, reconciled: 200, unchanged: 200, conflict: 409, not_found: 404, invalid: 422, source_not_found: 424, source_refused: 424, source_error: 502 }[outcome.status]
        return json(outcome, status)
      }
    }

    return json({ error: 'route inconnue' }, 404)
  } catch (error) {
    if (error instanceof InputError) return json({ error: error.message }, 400)
    return json({ error: errorMessage(error) }, 500)
  }
}

/** Corps de la requête, ou `null` s'il est vide ou dépasse `max` octets (lecture plafonnée). */
async function readBody(request: Request, max: number): Promise<Uint8Array | null> {
  const declared = Number(request.headers.get('content-length') ?? NaN)
  if (Number.isFinite(declared) && declared > max) return null
  if (!request.body) return null
  const reader = request.body.getReader()
  const parts: Uint8Array[] = []
  let size = 0
  for (;;) {
    const { done, value } = await reader.read()
    if (done) break
    size += value.byteLength
    if (size > max) {
      await reader.cancel()
      return null
    }
    parts.push(value)
  }
  if (!size) return null
  const out = new Uint8Array(size)
  let offset = 0
  for (const part of parts) {
    out.set(part, offset)
    offset += part.byteLength
  }
  return out
}

/** Vérifie les liaisons réelles : écriture et relecture D1, écriture R2 (sous reports/), TCGdex. */
async function selfTest(deps: AdminDeps, db: Db, now: () => number): Promise<Record<string, unknown>> {
  const out: Record<string, unknown> = { at: iso(now()) }
  try {
    const marker = crypto.randomUUID()
    await db.run(
      `INSERT INTO runtime_state (key, value, updated_at) VALUES ('selftest', ?1, ?2)
       ON CONFLICT (key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at`,
      marker,
      iso(now()),
    )
    const read = await db.first<{ value: string }>(`SELECT value FROM runtime_state WHERE key = 'selftest'`)
    const tables = await db.all<{ name: string }>(`SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' AND name NOT LIKE '_cf_%' ORDER BY name`)
    out.d1 = { ok: read?.value === marker, tables: tables.map((table) => table.name) }
  } catch (error) {
    out.d1 = { ok: false, error: errorMessage(error) }
  }
  try {
    const key = `${REPORTS_PREFIX}selftest.json`
    const body = JSON.stringify({ at: out.at })
    const put = await deps.bucket.put(key, body, { httpMetadata: { contentType: 'application/json' } })
    const head = await deps.bucket.head(key)
    out.r2 = { ok: put !== null && head?.size === body.length, key }
  } catch (error) {
    out.r2 = { ok: false, error: errorMessage(error) }
  }
  const client = new TcgdexClient({
    config: deps.config,
    fetcher: deps.fetcher,
    limiter: new RateLimiter(deps.config.minRequestIntervalMs, now),
    usage: db.usage,
    health: new Health(),
    now,
    deadlineMs: now() + 30_000,
  })
  const tcgdex: Record<string, unknown> = {}
  for (const language of deps.config.languages) {
    const outcome = await client.getJson(client.apiUrl(language, 'series'))
    tcgdex[language] = outcome.kind === 'ok' ? { ok: true, series: parseSeriesList(outcome.data).length } : { ok: false, outcome: outcome.kind, status: outcome.status }
  }
  out.tcgdex = tcgdex
  return out
}
