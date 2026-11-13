/**
 * API d'administration (src/admin/*), rapports (src/services/reports.ts), statistiques
 * (src/services/stats.ts) et handler planifié (src/index.ts).
 *
 * Les routes qui ne contactent pas TCGdex passent par le Worker complet (`SELF.fetch`) ; `/admin/tick`
 * et `/admin/selftest` appellent `handleAdmin` directement avec le faux TCGdex. Aucun appel réseau
 * réel : pour le handler planifié, le `fetch` global est espionné et neutralisé.
 */

import { createExecutionContext, createScheduledController, SELF, waitOnExecutionContext } from 'cloudflare:test'
import { env } from 'cloudflare:workers'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { handleAdmin, type AdminDeps } from '../src/admin/router'
import type { ConfigSource } from '../src/config'
import { Db } from '../src/db'
import worker from '../src/index'
import type { ProblemCard, SetCoverage } from '../src/services/reports'
import type { RunnerDeps, TickResult } from '../src/services/runner'
import type { Stats } from '../src/services/stats'
import { startSync } from '../src/services/sync'
import { API, ASSETS, Clock, drain, FakeTcgdex, makeDeps, one, resetAll, rows, sampleCatalog, sha256Hex, webp } from './helpers'

/** Jeton d'administration de test (vitest.config.ts / .dev.vars), jamais écrit en dur ici. */
const TOKEN = env.ADMIN_TOKEN
const ORIGIN = 'https://importateur.test'

beforeEach(resetAll)
afterEach(() => {
  vi.restoreAllMocks()
})

// ——— Outils locaux ———

interface Ctx {
  fake: FakeTcgdex
  clock: Clock
  deps: RunnerDeps
}

function setup(fake: FakeTcgdex = sampleCatalog(), overrides: ConfigSource = {}, start?: string): Ctx {
  const clock = new Clock(start)
  return { fake, clock, deps: makeDeps(fake, clock, overrides) }
}

/** Petit import complet (FR + JA) sur le faux TCGdex, comme le ferait le Cron. */
async function importSample(ctx: Ctx): Promise<void> {
  await startSync(new Db(env.DB), ctx.deps.config, ctx.clock.now())
  const ticks = await drain(ctx.deps, ctx.clock)
  expect(ticks.at(-1)?.status).toBe('idle')
}

/** Requête vers le Worker complet (src/index.ts), comme depuis l'extérieur. */
function call(path: string, init: { method?: string; authorization?: string | null } = {}): Promise<Response> {
  const headers = new Headers()
  const authorization = init.authorization === undefined ? `Bearer ${TOKEN}` : init.authorization
  if (authorization !== null) headers.set('authorization', authorization)
  return SELF.fetch(`${ORIGIN}${path}`, { method: init.method ?? 'GET', headers })
}

/** Appel direct du routeur avec le faux TCGdex et l'horloge simulée. */
function direct(ctx: Ctx, method: string, path: string, extra: Partial<AdminDeps> = {}, authorization: string | null = `Bearer ${TOKEN}`): Promise<Response> {
  const headers = new Headers()
  if (authorization !== null) headers.set('authorization', authorization)
  const deps: AdminDeps = { config: ctx.deps.config, d1: env.DB, bucket: env.BUCKET, fetcher: ctx.fake.fetcher, adminToken: TOKEN, now: ctx.clock.now, ...extra }
  return handleAdmin(new Request(`${ORIGIN}${path}`, { method, headers }), deps)
}

async function body<T>(response: Response): Promise<T> {
  return (await response.json()) as T
}

async function r2Keys(prefix = ''): Promise<string[]> {
  const keys: string[] = []
  let cursor: string | undefined
  do {
    const listed = await env.BUCKET.list({ prefix, ...(cursor ? { cursor } : {}) })
    keys.push(...listed.objects.map((object) => object.key))
    cursor = listed.truncated ? listed.cursor : undefined
  } while (cursor)
  return keys.sort()
}

async function r2Text(key: string): Promise<string> {
  const object = await env.BUCKET.get(key)
  if (!object) throw new Error(`objet R2 absent : ${key}`)
  return object.text()
}

async function deleteReports(): Promise<void> {
  const keys = await r2Keys('reports/')
  if (keys.length) await env.BUCKET.delete(keys)
}

/** Analyse CSV (RFC 4180 : guillemets doublés, CRLF). */
function parseCsv(text: string): string[][] {
  const out: string[][] = []
  let row: string[] = []
  let cell = ''
  let quoted = false
  for (let i = 0; i < text.length; i++) {
    const c = text.charAt(i)
    if (quoted) {
      if (c === '"' && text.charAt(i + 1) === '"') {
        cell += '"'
        i++
      } else if (c === '"') quoted = false
      else cell += c
      continue
    }
    if (c === '"') quoted = true
    else if (c === ',') {
      row.push(cell)
      cell = ''
    } else if (c === '\r' && text.charAt(i + 1) === '\n') {
      row.push(cell)
      out.push(row)
      row = []
      cell = ''
      i++
    } else cell += c
  }
  if (cell !== '' || row.length) {
    row.push(cell)
    out.push(row)
  }
  return out
}

/** Lignes CSV sous forme d'objets (clés = en-têtes). */
function csvObjects(text: string): Record<string, string>[] {
  const [header = [], ...data] = parseCsv(text)
  return data.map((cells) => Object.fromEntries(header.map((name, i) => [name, cells[i] ?? ''])))
}

const escapeRegExp = (value: string) => value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')

/** Ligne « Libellé   : valeur » du rapport texte. */
function statusLine(label: string, value: string): RegExp {
  return new RegExp(`^${escapeRegExp(label)}\\s*: ${escapeRegExp(value)}$`, 'm')
}

/** Sections du rapport texte, séparées par des lignes vides (titre → lignes). */
function statusSections(text: string): Map<string, string> {
  const map = new Map<string, string>()
  for (const block of text.trimEnd().split('\n\n')) map.set(block.split('\n')[0] ?? '', block)
  return map
}

/** Neutralise le `fetch` global (vrai réseau) et permet de vérifier qu'il n'est jamais appelé. */
function forbidNetwork() {
  return vi.spyOn(globalThis, 'fetch').mockImplementation(async () => {
    throw new Error('réseau réel interdit dans les tests')
  })
}

async function runScheduled(cron: string, scheduledTime: number): Promise<void> {
  const handler: ExportedHandler<Env> = worker
  const executionCtx = createExecutionContext()
  await handler.scheduled?.(createScheduledController({ cron, scheduledTime }), env, executionCtx)
  await waitOnExecutionContext(executionCtx)
}

function expectSecurityHeaders(response: Response): void {
  expect(response.headers.get('cache-control')).toBe('no-store')
  expect(response.headers.get('x-content-type-options')).toBe('nosniff')
}

/** Catalogue de référence avec des images plus lourdes (2 Mo au total) pour un volume lisible. */
function heavyCatalog(): FakeTcgdex {
  const fake = sampleCatalog()
  fake.addImage(`${ASSETS}/fr/swsh/swsh3/136/high.webp`, webp(500_000, 1))
  fake.addImage(`${ASSETS}/fr/swsh/swsh3/137/high.webp`, webp(500_000, 2))
  fake.addImage(`${ASSETS}/ja/SV/SV1a/001/high.webp`, webp(500_000, 3))
  fake.addImage(`${ASSETS}/ja/SV/SV1a/002/high.webp`, webp(500_000, 4))
  return fake
}

const nonReportJobs = async () => (await one<{ n: number }>(`SELECT COUNT(*) AS n FROM import_jobs WHERE job_type != 'report'`)).n

/** Toutes les routes, existantes ou non, pour les contrôles d'accès. */
const ROUTES: [string, string][] = [
  ['GET', '/admin/status'],
  ['GET', '/admin/status?format=json'],
  ['GET', '/admin/stats'],
  ['GET', '/admin/stats?sets=1'],
  ['GET', '/admin/missing?language=fr'],
  ['GET', '/admin/missing'],
  ['GET', '/admin/errors'],
  ['GET', '/admin/reports'],
  ['GET', '/admin/reports?prefix=latest/'],
  ['GET', '/admin/reports/latest/summary.csv'],
  ['GET', '/admin/r2'],
  ['GET', '/admin/r2?prefix=reports/'],
  ['POST', '/admin/sync/start'],
  ['POST', '/admin/sync/pause'],
  ['POST', '/admin/sync/resume'],
  ['POST', '/admin/sync/cancel'],
  ['POST', '/admin/sync/retry-missing'],
  ['POST', '/admin/reports/generate'],
  ['POST', '/admin/tick'],
  ['POST', '/admin/selftest'],
  ['GET', '/admin'],
  ['GET', '/admin/inconnue'],
  ['POST', '/admin/inconnue'],
  ['DELETE', '/admin/status'],
]

// ——— Protection ———

describe('protection de l’API d’administration', () => {
  it('le jeton de test configuré est assez long pour être accepté', () => {
    expect(typeof TOKEN).toBe('string')
    expect(TOKEN.length).toBeGreaterThanOrEqual(32)
  })

  it('sans en-tête Authorization : 401 sur chaque route /admin (GET et POST), sans aucun effet', async () => {
    await env.BUCKET.put('reports/latest/summary.csv', 'contenu-confidentiel')
    for (const [method, path] of ROUTES) {
      const response = await call(path, { method, authorization: null })
      expect(response.status, `${method} ${path}`).toBe(401)
      expectSecurityHeaders(response)
      const text = await response.text()
      expect(JSON.parse(text)).toEqual({ error: 'authentification requise' })
      expect(text).not.toContain('contenu-confidentiel')
    }
    // Aucune commande n'a été exécutée.
    expect(await rows(`SELECT id FROM sync_runs`)).toEqual([])
    expect(await rows(`SELECT id FROM import_jobs`)).toEqual([])
    expect(await rows(`SELECT key FROM runtime_state`)).toEqual([])
    expect(await r2Keys()).toEqual(['reports/latest/summary.csv'])
  })

  it('mauvais jeton : 401 sur les routes GET et POST', async () => {
    const wrong = [TOKEN + 'x', TOKEN.slice(0, -1), 'x'.repeat(TOKEN.length), 'mauvais-jeton']
    if (TOKEN.toUpperCase() !== TOKEN) wrong.push(TOKEN.toUpperCase())
    for (const token of wrong) {
      for (const [method, path] of ROUTES) {
        const response = await call(path, { method, authorization: `Bearer ${token}` })
        expect(response.status, `${method} ${path}`).toBe(401)
        expect(await body(response)).toEqual({ error: 'jeton invalide' })
      }
    }
    expect(await rows(`SELECT key FROM runtime_state`)).toEqual([])
    expect(await rows(`SELECT id FROM import_jobs`)).toEqual([])
  })

  it('en-tête « Bearer » mal formé : 401', async () => {
    const malformed = ['Bearer', 'Bearer ', `Bearer${TOKEN}`, `Basic ${TOKEN}`, `Token ${TOKEN}`, TOKEN, `Bearer: ${TOKEN}`, `Bearer ${TOKEN} ${TOKEN}`, `Bearer "${TOKEN}"`, `Bearer ${TOKEN},`]
    for (const authorization of malformed) {
      for (const [method, path] of [
        ['GET', '/admin/status'],
        ['POST', '/admin/sync/pause'],
      ] as const) {
        const response = await call(path, { method, authorization })
        expect(response.status, `${method} ${path} avec « ${authorization.replace(TOKEN, '<jeton>')} »`).toBe(401)
        expectSecurityHeaders(response)
      }
    }
    expect(await rows(`SELECT key FROM runtime_state`)).toEqual([])
  })

  it('schéma insensible à la casse et espaces multiples tolérés (RFC 7235)', async () => {
    expect((await call('/admin/stats', { authorization: `bearer ${TOKEN}` })).status).toBe(200)
    expect((await call('/admin/stats', { authorization: `Bearer    ${TOKEN}` })).status).toBe(200)
  })

  it('jeton absent ou trop court dans la configuration : 503 (échec fermé), même avec un en-tête correspondant', async () => {
    const ctx = setup()
    const cases: [string | undefined, string][] = [
      [undefined, `Bearer ${TOKEN}`],
      ['', `Bearer ${TOKEN}`],
      ['court', 'Bearer court'],
      ['x'.repeat(31), `Bearer ${'x'.repeat(31)}`],
    ]
    for (const [adminToken, authorization] of cases) {
      for (const [method, path] of ROUTES) {
        const response = await direct(ctx, method, path, { adminToken }, authorization)
        expect(response.status, `${method} ${path} (jeton configuré : ${String(adminToken?.length)})`).toBe(503)
        expectSecurityHeaders(response)
        expect((await body<{ error: string }>(response)).error).toContain('ADMIN_TOKEN')
      }
    }
    expect(ctx.fake.requests).toEqual([])
    expect(await rows(`SELECT key FROM runtime_state`)).toEqual([])
    // 32 caractères : la longueur minimale est acceptée.
    const ok = await direct(ctx, 'GET', '/admin/stats', { adminToken: 'y'.repeat(32) }, `Bearer ${'y'.repeat(32)}`)
    expect(ok.status).toBe(200)
  })

  it('bon jeton : 200 sur les routes de lecture et de commande', async () => {
    await env.BUCKET.put('reports/latest/summary.csv', 'a,b\r\n', { httpMetadata: { contentType: 'text/csv; charset=utf-8' } })
    for (const [method, path] of [
      ['GET', '/admin/status'],
      ['GET', '/admin/status?format=json'],
      ['GET', '/admin/stats'],
      ['GET', '/admin/stats?sets=1'],
      ['GET', '/admin/missing?language=fr'],
      ['GET', '/admin/missing?language=ja'],
      ['GET', '/admin/missing'],
      ['GET', '/admin/errors'],
      ['GET', '/admin/reports'],
      ['GET', '/admin/reports?prefix=latest/'],
      ['GET', '/admin/reports/latest/summary.csv'],
      ['GET', '/admin/r2'],
      ['GET', '/admin/status/'],
      ['POST', '/admin/sync/pause'],
      ['POST', '/admin/sync/resume'],
      ['POST', '/admin/sync/cancel'],
      ['POST', '/admin/sync/retry-missing'],
      ['POST', '/admin/reports/generate'],
      ['POST', '/admin/sync/start'],
    ] as const) {
      const response = await call(path, { method })
      expect(response.status, `${method} ${path}`).toBe(200)
      expectSecurityHeaders(response)
      await response.arrayBuffer()
    }
  })
})

// ——— Réponses HTTP ———

describe('réponses HTTP', () => {
  it('cache-control no-store et nosniff sur toutes les réponses de l’administration', async () => {
    await env.BUCKET.put('reports/latest/summary.json', '{}', { httpMetadata: { contentType: 'application/json; charset=utf-8' } })
    const responses: [string, Response][] = [
      ['200 JSON', await call('/admin/stats')],
      ['200 texte', await call('/admin/status')],
      ['200 rapport R2', await call('/admin/reports/latest/summary.json')],
      ['400', await call('/admin/missing?language=de')],
      ['400 chemin mal encodé', await call('/admin/reports/latest/%E0%A4%A')],
      ['401', await call('/admin/stats', { authorization: null })],
      ['404 admin', await call('/admin/inconnue')],
      ['404 rapport', await call('/admin/reports/latest/absent.csv')],
      ['405', await call('/admin/tick', { method: 'GET' })],
      ['405 chemin à paramètre', await call('/admin/reports/generate')],
      ['404 public', await call('/inconnue', { authorization: null })],
      ['503', await direct(setup(), 'GET', '/admin/stats', { adminToken: undefined })],
    ]
    for (const [label, response] of responses) {
      expect(response.headers.get('cache-control'), label).toBe('no-store')
      expect(response.headers.get('x-content-type-options'), label).toBe('nosniff')
      expect(response.headers.get('referrer-policy'), label).toBe('no-referrer')
      await response.arrayBuffer()
    }
  })

  it('/health est public et ne renvoie que « ok », sans aucune donnée', async () => {
    const ctx = setup()
    await importSample(ctx)
    const response = await call('/health', { authorization: null })
    expect(response.status).toBe(200)
    expect(response.headers.get('cache-control')).toBe('no-store')
    expect(response.headers.get('content-type')).toContain('text/plain')
    expect((await response.text()).trim()).toBe('ok')
    // Un jeton invalide n'y change rien (la route ne lit pas l'en-tête).
    expect((await (await call('/health', { authorization: 'Bearer faux' })).text()).trim()).toBe('ok')
  })

  it('route inconnue : 404 (publique, et sous /admin avec le bon jeton)', async () => {
    for (const path of ['/', '/inconnue', '/administration', '/healthz']) {
      const response = await call(path, { authorization: null })
      expect(response.status, path).toBe(404)
      expect(await body(response)).toEqual({ error: 'route inconnue' })
    }
    for (const [method, path] of [
      ['GET', '/admin'],
      ['GET', '/admin/inconnue'],
      ['POST', '/admin/inconnue'],
      ['POST', '/admin/sync'],
      ['GET', '/admin/sync/start/plus'],
    ] as const) {
      const response = await call(path, { method })
      expect(response.status, `${method} ${path}`).toBe(404)
      expect(await body(response)).toEqual({ error: 'route inconnue' })
    }
    // Un rapport ne se lit qu'en GET : toute autre méthode sur ce chemin est refusée (405).
    const post = await call('/admin/reports/latest/summary.csv', { method: 'POST' })
    expect(post.status).toBe(405)
    expect(await body(post)).toEqual({ error: 'méthode non autorisée' })
  })

  it('mauvaise méthode (GET ↔ POST) : 405', async () => {
    for (const [method, path] of [
      ['POST', '/admin/status'],
      ['POST', '/admin/stats'],
      ['POST', '/admin/missing'],
      ['POST', '/admin/errors'],
      ['POST', '/admin/reports'],
      ['POST', '/admin/r2'],
      ['POST', '/admin/r2/'],
      ['GET', '/admin/sync/start'],
      ['GET', '/admin/sync/pause'],
      ['GET', '/admin/sync/resume'],
      ['GET', '/admin/sync/cancel'],
      ['GET', '/admin/sync/retry-missing'],
      ['GET', '/admin/tick'],
      ['GET', '/admin/selftest'],
      ['GET', '/admin/reports/generate'],
      ['GET', '/admin/reports/generate/'],
    ] as const) {
      const response = await call(path, { method })
      expect(response.status, `${method} ${path}`).toBe(405)
      expect(await body(response)).toEqual({ error: 'méthode non autorisée' })
    }
    // Aucune commande exécutée par un GET.
    expect(await rows(`SELECT id FROM sync_runs`)).toEqual([])
    expect(await rows(`SELECT id FROM import_jobs`)).toEqual([])
    expect(await rows(`SELECT key FROM runtime_state`)).toEqual([])
  })

  it('mauvaise méthode (PUT, DELETE, PATCH) sur une route connue : 405', async () => {
    for (const [method, path] of [
      ['PUT', '/admin/sync/start'],
      ['DELETE', '/admin/status'],
      ['PATCH', '/admin/stats'],
      ['PUT', '/admin/r2'],
      ['DELETE', '/admin/sync/cancel'],
    ] as const) {
      const response = await call(path, { method })
      expect(response.status, `${method} ${path}`).toBe(405)
      expect(await body(response)).toEqual({ error: 'méthode non autorisée' })
    }
  })

  it('GET /admin/reports/generate : 405 (route POST uniquement), jamais lu comme un rapport R2', async () => {
    // Même si un objet `reports/generate` existait, la route connue prime sur le chemin à paramètre.
    await env.BUCKET.put('reports/generate', 'ne doit pas être servi')
    const response = await call('/admin/reports/generate')
    expect(response.status).toBe(405)
    const text = await response.text()
    expect(JSON.parse(text)).toEqual({ error: 'méthode non autorisée' })
    expect(text).not.toContain('ne doit pas être servi')
    expect(await rows(`SELECT id FROM import_jobs`)).toEqual([])
  })
})

// ——— Statut et statistiques ———

describe('statut et statistiques', () => {
  it('GET /admin/status : rapport texte au format demandé, avec les chiffres réels d’un petit import', async () => {
    const ctx = setup(heavyCatalog())
    await importSample(ctx)
    const jobs = await nonReportJobs()

    const response = await call('/admin/status')
    expect(response.status).toBe(200)
    expect(response.headers.get('content-type')).toBe('text/plain; charset=utf-8')
    const text = await response.text()
    const sections = statusSections(text)
    expect(text.startsWith('POKÉCODEX — TCGDEX IMPORTER\n')).toBe(true)
    expect(text).toMatch(/^Statut : TERMINÉ$/m)

    const fr = sections.get('FRANÇAIS') ?? ''
    expect(fr).toMatch(statusLine('Cartes connues', '4'))
    expect(fr).toMatch(statusLine('Images téléchargées', '1'))
    expect(fr).toMatch(statusLine('Images retrouvées CDN', '1'))
    expect(fr).toMatch(statusLine('Images manquantes', '2'))
    expect(fr).toMatch(statusLine('Erreurs', '0'))
    expect(fr).toMatch(statusLine('En attente', '0'))

    const ja = sections.get('JAPONAIS') ?? ''
    expect(ja).toMatch(statusLine('Cartes connues', '3'))
    expect(ja).toMatch(statusLine('Images téléchargées', '1'))
    expect(ja).toMatch(statusLine('Images retrouvées CDN', '1'))
    expect(ja).toMatch(statusLine('Images manquantes', '1'))
    expect(ja).toMatch(statusLine('Erreurs', '0'))
    expect(ja).toMatch(statusLine('En attente', '0'))

    const total = sections.get('TOTAL') ?? ''
    expect(total).toMatch(statusLine('Cartes traitées', '7 / 7'))
    expect(total).toMatch(statusLine('Images stockées R2', '4'))
    expect(total).toMatch(statusLine('Volume stocké', '0,002 Go'))
    expect(total).toMatch(statusLine('Progression', `100 % (${jobs} / ${jobs} tâches)`))
    expect(total).toMatch(/^Dernière exécution\s*: \d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{3}Z \(.+\)$/m)
    expect(total).toMatch(/^Dernière activité\s*: \d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{3}Z$/m)
    const next = /^Prochaine exécution\s*: (\S+)$/m.exec(total)?.[1] ?? ''
    expect(new Date(next).toISOString()).toBe(next)
    expect(new Date(next).getUTCMinutes() % 5).toBe(0)
    // Cron hebdomadaire « 7 3 * * MON » : le lundi à 03:07 UTC.
    const sync = /^Prochaine synchro\s*: (\d{4}-\d\d-\d\dT03:07:00\.000Z)$/m.exec(total)?.[1] ?? ''
    expect(new Date(sync).getUTCDay()).toBe(1)
    // Ordre des sections.
    expect([...sections.keys()]).toEqual(['POKÉCODEX — TCGDEX IMPORTER', 'Statut : TERMINÉ', 'FRANÇAIS', 'JAPONAIS', 'TOTAL'])
  })

  it('GET /admin/status?format=json : statistiques structurées', async () => {
    const ctx = setup(heavyCatalog())
    await importSample(ctx)
    const response = await call('/admin/status?format=json')
    expect(response.status).toBe(200)
    expect(response.headers.get('content-type')).toBe('application/json; charset=utf-8')
    const stats = await body<Stats>(response)
    expect(stats.status).toBe('TERMINÉ')
    expect(stats.languages.map((lang) => lang.language)).toEqual(['fr', 'ja'])
    expect(stats.languages[0]).toMatchObject({
      series: 2,
      sets: 2,
      cards: 4,
      withApiImageUrl: 1,
      detailsFetched: 4,
      imagesPending: 0,
      imagesDownloaded: 1,
      imagesRecovered: 1,
      imagesStored: 2,
      imagesNotFoundOnCdn: 2,
      imagesMissing: 2,
      errors: 0,
      bytes: 1_000_000,
      coveragePercent: 50,
    })
    expect(stats.languages[1]).toMatchObject({ cards: 3, imagesStored: 2, imagesMissing: 1, bytes: 1_000_000, coveragePercent: 66.67 })
    expect(stats.total).toMatchObject({ cards: 7, cardsProcessed: 7, imagesStored: 4, imagesRecovered: 2, imagesMissing: 3, errors: 0, bytes: 2_000_000, progressPercent: 100, jobsActive: 0, jobsFailed: 0 })
    expect(stats.total.jobsTotal).toBe(await nonReportJobs())
    expect(stats.syncRuns).toHaveLength(2)
    expect(stats.limits).toMatchObject({ processCron: '*/5 * * * *', syncCron: '7 3 * * MON' })
  })

  it('prochaine exécution et prochaine synchronisation calculées depuis les Cron (MON = lundi, 03:07)', async () => {
    // Vendredi 13/11/2026 10:02:30 UTC.
    const ctx = setup(sampleCatalog(), {}, '2026-11-13T10:02:30.000Z')
    const stats = await body<Stats>(await direct(ctx, 'GET', '/admin/status?format=json'))
    expect(stats.nextTick).toBe('2026-11-13T10:05:00.000Z')
    expect(stats.nextCatalogSync).toBe('2026-11-16T03:07:00.000Z')
    const text = await (await direct(ctx, 'GET', '/admin/status')).text()
    expect(text).toMatch(statusLine('Prochaine exécution', '2026-11-13T10:05:00.000Z'))
    expect(text).toMatch(statusLine('Prochaine synchro', '2026-11-16T03:07:00.000Z'))
    // Lundi 03:00 : la synchronisation du jour n'a pas encore eu lieu (03:07).
    ctx.clock.t = Date.parse('2026-11-16T03:00:00.000Z')
    const mondayEarly = await body<Stats>(await direct(ctx, 'GET', '/admin/status?format=json'))
    expect(mondayEarly.nextTick).toBe('2026-11-16T03:05:00.000Z')
    expect(mondayEarly.nextCatalogSync).toBe('2026-11-16T03:07:00.000Z')
    // Lundi 03:07 pile : la synchronisation suivante est le lundi d'après ; le traitement suit à 03:10.
    ctx.clock.t = Date.parse('2026-11-16T03:07:00.000Z')
    const monday = await body<Stats>(await direct(ctx, 'GET', '/admin/status?format=json'))
    expect(monday.nextTick).toBe('2026-11-16T03:10:00.000Z')
    expect(monday.nextCatalogSync).toBe('2026-11-23T03:07:00.000Z')
  })

  it('base vide : statut NON DÉMARRÉ, compteurs à zéro, aucune exécution', async () => {
    const text = await (await call('/admin/status')).text()
    expect(text).toMatch(/^Statut : NON DÉMARRÉ$/m)
    expect(text).toMatch(statusLine('Cartes traitées', '0 / 0'))
    expect(text).toMatch(statusLine('Volume stocké', '0,000 Go'))
    expect(text).toMatch(statusLine('Progression', '0 % (0 / 0 tâches)'))
    expect(text).toMatch(statusLine('Dernière exécution', 'aucune'))
  })

  it('GET /admin/stats, puis ?sets=1 : détail réel par extension', async () => {
    const ctx = setup()
    await importSample(ctx)
    const plain = await body<Stats & { sets?: unknown }>(await call('/admin/stats'))
    expect(plain.total).toMatchObject({ cards: 7, imagesStored: 4, imagesMissing: 3, bytes: 300 + 310 + 320 + 330 })
    expect(plain.sets).toBeUndefined()

    const detailed = await body<Stats & { sets: SetCoverage[] }>(await call('/admin/stats?sets=1'))
    expect(detailed.total).toEqual(plain.total)
    expect(detailed.sets.map((set) => [set.language, set.set_id])).toEqual([
      ['fr', 'exu'],
      ['fr', 'swsh3'],
      ['ja', 'SM1+'],
      ['ja', 'SV1a'],
    ])
    const swsh3 = detailed.sets.find((set) => set.set_id === 'swsh3')
    expect(swsh3).toMatchObject({ set_name: 'Ténèbres Embrasées', series_name: 'Épée et Bouclier', cards: 3, with_api_url: 1, stored: 2, downloaded: 1, recovered: 1, missing: 1, errors: 0, pending: 0, bytes: 610 })
    expect(detailed.sets.find((set) => set.set_id === 'exu')).toMatchObject({ cards: 1, stored: 0, missing: 1, bytes: 0 })
    expect(detailed.sets.find((set) => set.set_id === 'SV1a')).toMatchObject({ cards: 2, stored: 2, missing: 0, bytes: 650 })
    expect(detailed.sets.find((set) => set.set_id === 'SM1+')).toMatchObject({ cards: 1, stored: 0, missing: 1 })

    // ?sets=true : même détail.
    const withTrue = await body<Stats & { sets: SetCoverage[] }>(await call('/admin/stats?sets=true'))
    expect(withTrue.sets).toEqual(detailed.sets)
  })

  it('GET /admin/stats?sets=… : seules les valeurs 1 et true donnent le détail par extension', async () => {
    const ctx = setup()
    await importSample(ctx)
    for (const query of ['?sets=0', '?sets=false', '?sets=', '?sets', '?sets=non', '?sets=2', '?sets=no']) {
      const stats = await body<Stats & { sets?: unknown }>(await call(`/admin/stats${query}`))
      expect(stats.sets, query).toBeUndefined()
      expect(stats.total.cards, query).toBe(7)
    }
    for (const query of ['?sets=1', '?sets=true']) {
      const stats = await body<Stats & { sets?: SetCoverage[] }>(await call(`/admin/stats${query}`))
      expect(stats.sets, query).toHaveLength(4)
    }
  })

  it('GET /admin/stats?sets=oui / ?sets=yes : pas de détail (seulement 1 ou true)', async () => {
    for (const query of ['?sets=oui', '?sets=yes']) {
      const stats = await body<Stats & { sets?: unknown }>(await call(`/admin/stats${query}`))
      expect(stats.sets, query).toBeUndefined()
    }
  })

  it('progression cohérente quand seul un rapport est en attente (tâches report exclues)', async () => {
    const ctx = setup()
    await importSample(ctx)
    const jobs = await nonReportJobs()
    expect((await call('/admin/reports/generate', { method: 'POST' })).status).toBe(200)
    const text = await (await call('/admin/status')).text()
    expect(text).toMatch(statusLine('Progression', `100 % (${jobs} / ${jobs} tâches)`))
    expect(text).toMatch(/^Statut : TERMINÉ$/m)
    const stats = await body<Stats>(await call('/admin/status?format=json'))
    expect(stats.total).toMatchObject({ jobsTotal: jobs, jobsActive: 0, jobsFailed: 0, progressPercent: 100 })
  })

  it('une tâche report en échec n’est pas comptée dans jobsFailed', async () => {
    const ctx = setup()
    await importSample(ctx)
    await env.DB.prepare(`UPDATE import_jobs SET status = 'failed', last_error = 'panne simulée' WHERE job_type = 'report'`).run()
    expect((await one<{ n: number }>(`SELECT COUNT(*) AS n FROM import_jobs WHERE job_type = 'report' AND status = 'failed'`)).n).toBe(1)
    const stats = await body<Stats>(await call('/admin/status?format=json'))
    expect(stats.total).toMatchObject({ jobsActive: 0, jobsFailed: 0 })
    expect(stats.status).toBe('TERMINÉ')
  })
})

// ——— Cartes manquantes et erreurs ———

/** Import où l'image officielle de fr:swsh3-136 renvoie toujours HTTP 500 (une seule tentative). */
async function importWithImageError(): Promise<Ctx> {
  const fake = sampleCatalog()
  fake.images.set(`${ASSETS}/fr/swsh/swsh3/136/high.webp`, { status: 500 })
  const ctx = setup(fake, { MAX_ATTEMPTS: '1' })
  await importSample(ctx)
  return ctx
}

describe('cartes manquantes et erreurs', () => {
  it('GET /admin/missing?language=fr : pagination limit / after / next', async () => {
    const ctx = setup()
    await importSample(ctx)

    const all = await body<{ count: number; next: string | null; items: ProblemCard[] }>(await call('/admin/missing?language=fr'))
    expect(all.count).toBe(2)
    expect(all.next).toBeNull()
    expect(all.items.map((item) => item.id)).toEqual(['fr:exu-%3F', 'fr:swsh3-138'])
    expect(all.items[1]).toMatchObject({
      tcgdex_id: 'swsh3-138',
      language: 'fr',
      name: 'Carte introuvable',
      local_id: '138',
      set_id: 'swsh3',
      set_name: 'Ténèbres Embrasées',
      image_status: 'not_found_on_cdn',
      image_http_status: 404,
      image_failure_reason: 'URL reconstruite introuvable sur le CDN (404)',
    })

    const seen: string[] = []
    let after: string | null = ''
    for (let page = 0; after !== null && page < 5; page++) {
      const query: URLSearchParams = new URLSearchParams({ language: 'fr', limit: '1', after })
      const result: { count: number; next: string | null; items: ProblemCard[] } = await body(await call(`/admin/missing?${query}`))
      expect(result.count).toBe(result.items.length)
      seen.push(...result.items.map((item) => item.id))
      if (result.items.length) expect(result.next).toBe(result.items.at(-1)?.id)
      after = result.next
    }
    expect(seen).toEqual(['fr:exu-%3F', 'fr:swsh3-138'])

    const ja = await body<{ items: ProblemCard[] }>(await call('/admin/missing?language=ja'))
    expect(ja.items.map((item) => item.id)).toEqual(['ja:SM1+-001'])
    // Les images valides n'y figurent jamais.
    expect(ja.items.some((item) => item.id === 'ja:SV1a-001')).toBe(false)
  })

  it('GET /admin/missing sans langue (ou fr,ja) : 200 avec les deux langues, paginé', async () => {
    const ctx = setup()
    await importSample(ctx)
    const expected = ['fr:exu-%3F', 'fr:swsh3-138', 'ja:SM1+-001']
    for (const query of ['', '?language=', '?language=fr,ja', '?language=ja,fr', '?language=fr,%20ja']) {
      const response = await call(`/admin/missing${query}`)
      expect(response.status, query).toBe(200)
      const result = await body<{ count: number; next: string | null; items: ProblemCard[] }>(response)
      expect(result.items.map((item) => item.id), query).toEqual(expected)
      expect(result.count, query).toBe(3)
      expect(result.next, query).toBeNull()
    }
    // Pagination à travers les deux langues.
    const first = await body<{ next: string | null; items: ProblemCard[] }>(await call('/admin/missing?limit=2'))
    expect(first.items.map((item) => item.id)).toEqual(['fr:exu-%3F', 'fr:swsh3-138'])
    expect(first.next).toBe('fr:swsh3-138')
    const second = await body<{ next: string | null; items: ProblemCard[] }>(await call(`/admin/missing?${new URLSearchParams({ limit: '2', after: first.next ?? '' })}`))
    expect(second.items.map((item) => item.id)).toEqual(['ja:SM1+-001'])
    expect(second.next).toBeNull()
  })

  it('GET /admin/missing avec une langue inconnue : 400', async () => {
    for (const query of ['?language=de', '?language=FR', '?language=fr,', '?language=fr,xx', '?language=%20']) {
      const response = await call(`/admin/missing${query}`)
      expect(response.status, query).toBe(400)
      expect(await body(response)).toEqual({ error: 'langue inconnue (fr ou ja)' })
    }
  })

  it('GET /admin/errors : images en erreur et tâches en échec', async () => {
    await importWithImageError()
    const result = await body<{ images: ProblemCard[]; nextImages: string | null; failedJobs: { job_type: string; language: string; target_id: string; last_http_status: number; last_error: string; attempts: number }[]; nextJobs: number | null }>(
      await call('/admin/errors'),
    )
    expect(result.images).toHaveLength(1)
    expect(result.images[0]).toMatchObject({ id: 'fr:swsh3-136', image_status: 'download_error', image_http_status: 500, image_failure_reason: 'HTTP 500' })
    expect(result.failedJobs).toHaveLength(1)
    expect(result.failedJobs[0]).toMatchObject({ job_type: 'image', language: 'fr', target_id: 'swsh3-136', last_http_status: 500, attempts: 1 })
    expect(result.failedJobs[0]?.last_error).toContain('HTTP 500')
    expect(result.nextImages).toBeNull()
    expect(result.nextJobs).toBeNull()
  })

  it('GET /admin/errors sans erreur : listes vides', async () => {
    const ctx = setup()
    await importSample(ctx)
    expect(await body(await call('/admin/errors'))).toEqual({ images: [], nextImages: null, failedJobs: [], nextJobs: null })
  })
})

// ——— Commandes de synchronisation ———

describe('commandes de synchronisation', () => {
  it('POST /admin/sync/start : langue invalide → 400 ; ?language et ?sets respectés', async () => {
    for (const query of ['?language=de', '?language=fr,xx', '?language=FR']) {
      const response = await call(`/admin/sync/start${query}`, { method: 'POST' })
      expect(response.status, query).toBe(400)
      expect(await body(response)).toEqual({ error: 'langue inconnue (fr ou ja)' })
    }
    expect(await rows(`SELECT id FROM sync_runs`)).toEqual([])

    const response = await call('/admin/sync/start?language=fr&sets=swsh3,%20exu', { method: 'POST' })
    expect(response.status).toBe(200)
    const result = await body<{ started: { language: string; runId: number; kind: string }[]; skipped: unknown[] }>(response)
    expect(result.started.map((run) => [run.language, run.kind])).toEqual([['fr', 'initial']])
    expect(result.skipped).toEqual([])
    const run = await one<{ language: string; kind: string; status: string; scope_json: string }>(`SELECT language, kind, status, scope_json FROM sync_runs`)
    expect(run).toMatchObject({ language: 'fr', kind: 'initial', status: 'running' })
    expect(JSON.parse(run.scope_json)).toEqual({ sets: ['swsh3', 'exu'] })
    const job = await one<{ job_type: string; language: string; status: string; payload_json: string }>(`SELECT job_type, language, status, payload_json FROM import_jobs`)
    expect(job).toMatchObject({ job_type: 'catalog', language: 'fr', status: 'pending' })
    expect(JSON.parse(job.payload_json)).toEqual({ sets: ['swsh3', 'exu'] })
  })

  it('POST /admin/sync/start : langue déjà en cours ignorée ; après un import complet, synchronisation manual', async () => {
    const first = await body<{ started: { language: string; kind: string }[] }>(await call('/admin/sync/start', { method: 'POST' }))
    expect(first.started.map((run) => [run.language, run.kind])).toEqual([
      ['fr', 'initial'],
      ['ja', 'initial'],
    ])
    const again = await body<{ started: unknown[]; skipped: { language: string; reason: string }[] }>(await call('/admin/sync/start?language=ja', { method: 'POST' }))
    expect(again.started).toEqual([])
    expect(again.skipped).toEqual([{ language: 'ja', reason: 'synchronisation déjà en cours' }])

    // Import mené à son terme, puis nouvelle demande : synchronisation manuelle.
    const ctx = setup(sampleCatalog(), {}, new Date(Date.now() + 60_000).toISOString())
    const ticks = await drain(ctx.deps, ctx.clock)
    expect(ticks.at(-1)?.status).toBe('idle')
    expect(await rows(`SELECT status FROM sync_runs WHERE status != 'completed'`)).toEqual([])
    const manual = await body<{ started: { language: string; kind: string }[] }>(await call('/admin/sync/start?language=fr', { method: 'POST' }))
    expect(manual.started.map((run) => [run.language, run.kind])).toEqual([['fr', 'manual']])
  })

  it('pause puis reprise : runtime_state, statut affiché et exécution suspendue', async () => {
    const ctx = setup()
    expect(await body(await direct(ctx, 'POST', '/admin/sync/start'))).toMatchObject({ skipped: [] })

    expect(await body(await call('/admin/sync/pause', { method: 'POST' }))).toEqual({ paused: true })
    const paused = await rows<{ key: string; value: string | null }>(`SELECT key, value FROM runtime_state WHERE key IN ('paused', 'pause_reason') ORDER BY key`)
    expect(paused).toEqual([
      { key: 'pause_reason', value: 'pause manuelle' },
      { key: 'paused', value: '1' },
    ])
    expect(await (await call('/admin/status')).text()).toMatch(/^Statut : EN PAUSE — pause manuelle$/m)

    // En pause : une exécution manuelle ne traite rien et ne contacte pas TCGdex.
    const tick = await body<TickResult>(await direct(ctx, 'POST', '/admin/tick'))
    expect(tick).toMatchObject({ status: 'skipped', claimed: 0, httpRequests: 0 })
    expect(tick.reason).toMatch(/^en pause/)
    expect(ctx.fake.requests).toEqual([])
    expect(await rows(`SELECT status, attempts FROM import_jobs`)).toEqual([
      { status: 'pending', attempts: 0 },
      { status: 'pending', attempts: 0 },
    ])

    // Aucune attente imposée en cours : rien à lever.
    expect(await body(await call('/admin/sync/resume', { method: 'POST' }))).toEqual({ paused: false, backoffLifted: null })
    const resumed = await rows<{ key: string; value: string | null }>(`SELECT key, value FROM runtime_state WHERE key IN ('paused', 'pause_reason') ORDER BY key`)
    expect(resumed).toEqual([
      { key: 'pause_reason', value: null },
      { key: 'paused', value: null },
    ])
    expect(await (await call('/admin/status')).text()).toMatch(/^Statut : EN COURS$/m)
  })

  it('reprise : lève aussi une attente imposée (429)', async () => {
    const ctx = setup()
    await direct(ctx, 'POST', '/admin/sync/start')
    const until = new Date(Date.now() + 3_600_000).toISOString()
    await env.DB.batch([
      env.DB.prepare(`INSERT INTO runtime_state (key, value, updated_at) VALUES ('backoff_until', ?1, ?1)`).bind(until),
      env.DB.prepare(`INSERT INTO runtime_state (key, value, updated_at) VALUES ('backoff_reason', 'HTTP 429 de TCGdex', ?1)`).bind(until),
    ])
    expect((await body<Stats>(await call('/admin/status?format=json'))).status).toBe('EN ATTENTE')
    // L'attente levée est rendue, pour rester visible.
    expect(await body(await call('/admin/sync/resume', { method: 'POST' }))).toEqual({ paused: false, backoffLifted: until })
    expect(await rows(`SELECT value FROM runtime_state WHERE key IN ('backoff_until', 'backoff_reason')`)).toEqual([{ value: null }, { value: null }])
    expect((await body<Stats>(await call('/admin/status?format=json'))).status).toBe('EN COURS')
    // Une seconde reprise n'a plus rien à lever.
    expect(await body(await call('/admin/sync/resume', { method: 'POST' }))).toEqual({ paused: false, backoffLifted: null })
  })

  it('reprise : une attente imposée déjà expirée n’est pas signalée comme levée', async () => {
    const ctx = setup()
    await direct(ctx, 'POST', '/admin/sync/start')
    const past = new Date(ctx.clock.now() - 60_000).toISOString()
    await env.DB.prepare(`INSERT INTO runtime_state (key, value, updated_at) VALUES ('backoff_until', ?1, ?1)`).bind(past).run()
    expect(await body(await direct(ctx, 'POST', '/admin/sync/resume'))).toEqual({ paused: false, backoffLifted: null })
    expect(await one(`SELECT value FROM runtime_state WHERE key = 'backoff_until'`)).toEqual({ value: null })
  })

  it('POST /admin/sync/cancel : abandonne les synchronisations en cours (?language), les tâches restent', async () => {
    const ctx = setup()
    expect((await body<{ started: unknown[] }>(await direct(ctx, 'POST', '/admin/sync/start'))).started).toHaveLength(2)

    expect((await direct(ctx, 'POST', '/admin/sync/cancel?language=de')).status).toBe(400)
    expect(await body(await direct(ctx, 'POST', '/admin/sync/cancel?language=de'))).toEqual({ error: 'langue inconnue (fr ou ja)' })
    expect(await rows(`SELECT status FROM sync_runs WHERE status != 'running'`)).toEqual([])

    ctx.clock.advance(1_000)
    expect(await body(await direct(ctx, 'POST', '/admin/sync/cancel?language=fr'))).toEqual({ cancelled: 1 })
    expect(await rows(`SELECT language, status, finished_at FROM sync_runs ORDER BY language`)).toEqual([
      { language: 'fr', status: 'cancelled', finished_at: new Date(ctx.clock.now()).toISOString() },
      { language: 'ja', status: 'running', finished_at: null },
    ])
    // Les tâches ne sont pas supprimées : elles seront traitées normalement.
    expect(await rows(`SELECT job_type, language, status FROM import_jobs ORDER BY language`)).toEqual([
      { job_type: 'catalog', language: 'fr', status: 'pending' },
      { job_type: 'catalog', language: 'ja', status: 'pending' },
    ])

    // Sans langue : toutes les langues ; une seconde demande n'a plus rien à abandonner.
    expect(await body(await direct(ctx, 'POST', '/admin/sync/cancel'))).toEqual({ cancelled: 1 })
    expect(await body(await direct(ctx, 'POST', '/admin/sync/cancel'))).toEqual({ cancelled: 0 })
    expect(await rows(`SELECT status FROM sync_runs WHERE status = 'running'`)).toEqual([])

    // Une nouvelle synchronisation peut ensuite démarrer (une seule « running » par langue).
    ctx.clock.advance(1_000)
    const restarted = await body<{ started: { language: string }[]; skipped: unknown[] }>(await direct(ctx, 'POST', '/admin/sync/start?language=fr'))
    expect(restarted.started.map((run) => run.language)).toEqual(['fr'])
    expect(restarted.skipped).toEqual([])
    expect((await one<{ n: number }>(`SELECT COUNT(*) AS n FROM sync_runs WHERE language = 'fr' AND status = 'running'`)).n).toBe(1)
  })

  it('POST /admin/sync/retry-missing : relance les images manquantes et les tâches en échec, jamais les images valides', async () => {
    const fake = sampleCatalog()
    // Le détail de fr:swsh3-138 est introuvable une fois : sa tâche `card` échoue définitivement (404).
    fake.queue(`${API}/fr/cards/swsh3-138`, () => new Response('{}', { status: 404 }))
    const ctx = setup(fake)
    await importSample(ctx)
    expect(await one(`SELECT status, last_http_status FROM import_jobs WHERE dedupe_key = 'card:fr:swsh3-138'`)).toEqual({ status: 'failed', last_http_status: 404 })

    expect((await direct(ctx, 'POST', '/admin/sync/retry-missing?language=de')).status).toBe(400)

    ctx.clock.advance(60_000)
    const result = await body<{ images: number; failedJobs: number }>(await direct(ctx, 'POST', '/admin/sync/retry-missing'))
    expect(result).toEqual({ images: 3, failedJobs: 1 })
    const imageJobs = await rows<{ target_id: string; status: string }>(`SELECT target_id, status FROM import_jobs WHERE job_type = 'image' ORDER BY language, target_id`)
    expect(imageJobs).toEqual([
      { target_id: 'exu-%3F', status: 'pending' },
      { target_id: 'swsh3-136', status: 'done' },
      { target_id: 'swsh3-137', status: 'done' },
      { target_id: 'swsh3-138', status: 'pending' },
      { target_id: 'SM1+-001', status: 'pending' },
      { target_id: 'SV1a-001', status: 'done' },
      { target_id: 'SV1a-002', status: 'done' },
    ])
    expect(await one(`SELECT status, attempts, last_error FROM import_jobs WHERE dedupe_key = 'card:fr:swsh3-138'`)).toEqual({ status: 'pending', attempts: 0, last_error: null })

    // Exécution : images absentes revérifiées, détail récupéré, aucune image valide retéléchargée.
    const validImageRequests = ctx.fake.count((url) => url.endsWith('/136/high.webp') || url.endsWith('/001/high.webp'))
    await drain(ctx.deps, ctx.clock)
    expect(ctx.fake.count((url) => url.endsWith('/136/high.webp') || url.endsWith('/001/high.webp'))).toBe(validImageRequests + 1 /* SM1+-001, absente */)
    expect(await one(`SELECT detail_status FROM cards WHERE id = 'fr:swsh3-138'`)).toEqual({ detail_status: 'fetched' })
    expect(await rows(`SELECT id FROM import_jobs WHERE status IN ('pending', 'running', 'failed')`)).toEqual([])

    // Restreint à une langue.
    ctx.clock.advance(60_000)
    expect(await body(await direct(ctx, 'POST', '/admin/sync/retry-missing?language=ja'))).toEqual({ images: 1, failedJobs: 0 })
  })
})

// ——— Rapports ———

/** Réponse de GET /admin/reports. */
interface ReportList {
  reports: { key: string; size: number; uploaded: string }[]
  cursor: string | null
}

/** En-tête attendu de missing_fr.csv / missing_ja.csv. */
const MISSING_HEADER = ['id_tcgdex', 'langue', 'nom', 'extension', 'nom_extension', 'numero', 'serie', 'statut', 'url_api', 'urls_testees', 'code_http', 'motif', 'verifie_le']
const ERRORS_HEADER = ['type', 'id', 'langue', 'nom', 'extension', 'numero', 'statut', 'urls_testees', 'code_http', 'motif', 'date']
const LATEST = ['reports/latest/errors.csv','reports/latest/missing_fr.csv', 'reports/latest/missing_ja.csv', 'reports/latest/summary.csv', 'reports/latest/summary.json']
const stampOf = (ms: number) => new Date(ms).toISOString().replaceAll(':', '-').replace(/\.\d+Z$/, 'Z')

describe('rapports', () => {
  it('POST /admin/reports/generate puis exécution : fichiers latest et historique dans R2', async () => {
    const ctx = setup()
    await importSample(ctx)
    await deleteReports()
    ctx.clock.advance(3_600_000)

    const response = await direct(ctx, 'POST', '/admin/reports/generate')
    expect(response.status).toBe(200)
    expect(await body(response)).toMatchObject({ queued: true })
    expect(await one(`SELECT job_type, status, dedupe_key, language FROM import_jobs WHERE job_type = 'report'`)).toEqual({ job_type: 'report', status: 'pending', dedupe_key: 'report:*:*', language: null })
    // Une seconde demande ne crée pas de doublon.
    await direct(ctx, 'POST', '/admin/reports/generate')
    expect((await one<{ n: number }>(`SELECT COUNT(*) AS n FROM import_jobs WHERE job_type = 'report'`)).n).toBe(1)

    const requestsBefore = ctx.fake.requests.length
    const tick = await body<TickResult>(await direct(ctx, 'POST', '/admin/tick'))
    expect(tick).toMatchObject({ status: 'ran', claimed: 1, done: 1, failed: 0, httpRequests: 0 })
    expect(ctx.fake.requests.length).toBe(requestsBefore)

    const stamp = stampOf(ctx.clock.now())
    expect(await r2Keys('reports/')).toEqual([`reports/history/${stamp}/summary.csv`, `reports/history/${stamp}/summary.json`, ...LATEST])
    expect(await r2Text(`reports/history/${stamp}/summary.json`)).toBe(await r2Text('reports/latest/summary.json'))
    for (const key of LATEST) {
      const head = await env.BUCKET.head(key)
      expect(head?.httpMetadata?.contentType, key).toBe(key.endsWith('.json') ? 'application/json; charset=utf-8' : 'text/csv; charset=utf-8')
      expect(head?.httpMetadata?.cacheControl, key).toBe('private, no-store')
    }
    // Chaque CSV a son en-tête, et des lignes terminées par CRLF.
    for (const key of LATEST.filter((name) => name.endsWith('.csv'))) {
      const text = await r2Text(key)
      expect(text.endsWith('\r\n'), key).toBe(true)
      expect(parseCsv(text)[0]?.length, key).toBeGreaterThan(5)
    }
  })

  it('missing_fr.csv / missing_ja.csv : une ligne par carte manquante, identifiable précisément', async () => {
    const ctx = setup()
    await importSample(ctx)

    const frText = await r2Text('reports/latest/missing_fr.csv')
    const jaText = await r2Text('reports/latest/missing_ja.csv')
    for (const text of [frText, jaText]) {
      expect(parseCsv(text)[0]).toEqual(MISSING_HEADER)
    }
    const fr = csvObjects(frText)
    expect(fr).toHaveLength((await one<{ n: number }>(`SELECT COUNT(*) AS n FROM cards WHERE language = 'fr' AND image_status IN ('missing_api_url', 'not_found_on_cdn')`)).n)
    expect(fr.map((row) => row.id_tcgdex)).toEqual(['exu-%3F', 'swsh3-138'])
    expect(fr[0]).toEqual({
      id_tcgdex: 'exu-%3F',
      langue: 'fr',
      nom: 'Zarbi',
      extension: 'exu',
      nom_extension: 'Unseen Forces',
      numero: '%3F',
      serie: 'ex',
      statut: 'not_found_on_cdn',
      url_api: '',
      urls_testees: `${ASSETS}/fr/ex/exu/%253F/high.webp (404)`,
      code_http: '404',
      motif: 'URL reconstruite introuvable sur le CDN (404)',
      verifie_le: expect.stringMatching(/^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{3}Z$/),
    })
    expect(fr[1]).toMatchObject({
      id_tcgdex: 'swsh3-138',
      nom: 'Carte introuvable',
      extension: 'swsh3',
      nom_extension: 'Ténèbres Embrasées',
      numero: '138',
      serie: 'swsh',
      url_api: '',
      urls_testees: `${ASSETS}/fr/swsh/swsh3/138/high.webp (404)`,
      code_http: '404',
    })
    expect(fr[1]?.verifie_le).toMatch(/^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{3}Z$/)

    const ja = csvObjects(jaText)
    expect(ja).toEqual([
      expect.objectContaining({
        id_tcgdex: 'SM1+-001',
        langue: 'ja',
        nom: 'テスト',
        extension: 'SM1+',
        nom_extension: 'サン&ムーン強化',
        numero: '001',
        serie: 'SM',
        statut: 'not_found_on_cdn',
        url_api: '',
        urls_testees: `${ASSETS}/ja/SM/${encodeURIComponent('SM1+')}/001/high.webp (404)`,
        code_http: '404',
      }),
    ])
  })

  it('missing_fr.csv : url_api et toutes les URL testées « url (code) » séparées par « ; »', async () => {
    const fake = sampleCatalog()
    // URL officielle fournie par l'API, mais absente du CDN en qualité principale comme de secours.
    fake.sets.fr[0]?.cards.push({ id: 'swsh3-139', localId: '139', name: 'Image officielle absente', image: `${ASSETS}/fr/swsh/swsh3/139` })
    const ctx = setup(fake)
    await importSample(ctx)
    const row = csvObjects(await r2Text('reports/latest/missing_fr.csv')).find((line) => line.id_tcgdex === 'swsh3-139')
    expect(row).toMatchObject({
      statut: 'not_found_on_cdn',
      url_api: `${ASSETS}/fr/swsh/swsh3/139`,
      urls_testees: `${ASSETS}/fr/swsh/swsh3/139/high.webp (404) ; ${ASSETS}/fr/swsh/swsh3/139/low.webp (404)`,
      code_http: '404',
      motif: 'URL officielle de l’API introuvable sur le CDN (404)',
    })
    // La même liste que celle enregistrée en base, dans l'ordre (principale puis secours).
    const tested = JSON.parse((await one<{ image_tested_urls: string }>(`SELECT image_tested_urls FROM cards WHERE id = 'fr:swsh3-139'`)).image_tested_urls) as { url: string; status: number }[]
    expect(tested.map((item) => `${item.url} (${item.status})`).join(' ; ')).toBe(row?.urls_testees)
  })

  it('summary.json et summary.csv : langues, total, extensions et audit', async () => {
    const ctx = setup()
    await importSample(ctx)

    const summary = JSON.parse(await r2Text('reports/latest/summary.json')) as {
      generatedAt: string
      status: string
      languages: ({ language: string; setsWithCards: number } & Record<string, unknown>)[]
      total: Record<string, unknown>
      sets: SetCoverage[]
      audit: Record<string, number>
      syncRuns: unknown[]
    }
    expect(summary.languages.map((lang) => lang.language)).toEqual(['fr', 'ja'])
    expect(summary.languages[0]).toMatchObject({ cards: 4, imagesStored: 2, imagesMissing: 2, setsWithCards: 2, coveragePercent: 50 })
    expect(summary.languages[1]).toMatchObject({ cards: 3, imagesStored: 2, imagesMissing: 1, setsWithCards: 2 })
    expect(summary.total).toMatchObject({ cards: 7, imagesStored: 4, imagesMissing: 3, bytes: 1260, errors: 0 })
    expect(summary.sets).toHaveLength(4)
    expect(summary.audit).toEqual({ r2Objects: 4, r2Bytes: 1260, unknownObjects: 0, reconciled: 0, missingObjects: 0 })
    expect(summary.syncRuns).toHaveLength(2)

    const csv = csvObjects(await r2Text('reports/latest/summary.csv'))
    expect(csv.map((row) => [row.langue, row.extension])).toEqual([
      ['fr', 'exu'],
      ['fr', 'swsh3'],
      ['ja', 'SM1+'],
      ['ja', 'SV1a'],
      ['fr', 'TOTAL'],
      ['ja', 'TOTAL'],
    ])
    expect(csv[1]).toMatchObject({ nom_extension: 'Ténèbres Embrasées', serie: 'Épée et Bouclier', cartes: '3', url_api: '1', stockees: '2', telechargees: '1', retrouvees_cdn: '1', manquantes: '1', octets: '610', couverture_pct: '66.67' })
    expect(csv[4]).toMatchObject({ cartes: '4', stockees: '2', manquantes: '2', octets: '610', couverture_pct: '50.00' })
    expect(csv[5]).toMatchObject({ cartes: '3', stockees: '2', manquantes: '1', octets: '650', couverture_pct: '66.67' })
  })

  it('summary.json d’un import terminé indique le statut TERMINÉ (la tâche report ne se compte pas)', async () => {
    const ctx = setup()
    await importSample(ctx)
    const summary = JSON.parse(await r2Text('reports/latest/summary.json')) as { status: string; total: { jobsActive: number; jobsFailed: number } }
    expect(summary.status).toBe('TERMINÉ')
    expect(summary.total.jobsActive).toBe(0)
    expect(summary.total.jobsFailed).toBe(0)
  })

  it('errors.csv : image en erreur et tâche en échec', async () => {
    await importWithImageError()
    const text = await r2Text('reports/latest/errors.csv')
    expect(parseCsv(text)[0]).toEqual(ERRORS_HEADER)
    const lines = csvObjects(text)
    expect(lines).toHaveLength(2)
    expect(lines[0]).toMatchObject({
      type: 'image',
      id: 'swsh3-136',
      langue: 'fr',
      nom: 'Fouinar',
      extension: 'swsh3',
      numero: '136',
      statut: 'download_error',
      urls_testees: `${ASSETS}/fr/swsh/swsh3/136/high.webp (500)`,
      code_http: '500',
      motif: 'HTTP 500',
    })
    expect(lines[1]).toMatchObject({ type: 'tache:image', id: 'swsh3-136', langue: 'fr', statut: 'échec après 1 tentative(s)', urls_testees: '', code_http: '500' })
    expect(lines[1]?.motif).toContain('HTTP 500')
    // Le bilan compte l'erreur.
    const summary = JSON.parse(await r2Text('reports/latest/summary.json')) as { total: { errors: number } }
    expect(summary.total.errors).toBe(1)
  })

  it('CSV : une cellule commençant par « = » est neutralisée (pas de formule dans un tableur)', async () => {
    const fake = sampleCatalog()
    fake.sets.fr[0]?.cards.push({ id: 'swsh3-139', localId: '139', name: '=HYPERLINK("https://exemple.test","x")' })
    const ctx = setup(fake)
    await importSample(ctx)
    const text = await r2Text('reports/latest/missing_fr.csv')
    const row = csvObjects(text).find((line) => line.id_tcgdex === 'swsh3-139')
    expect(row?.nom).toBe(`'=HYPERLINK("https://exemple.test","x")`)
    expect(text).not.toMatch(/(^|,)=HYPERLINK/m)
  })

  it('GET /admin/reports liste les rapports ; GET /admin/reports/<chemin> renvoie leur contenu', async () => {
    const ctx = setup()
    await importSample(ctx)
    const keys = await r2Keys('reports/')

    const listed = await body<ReportList & { truncated?: unknown }>(await call('/admin/reports'))
    expect(listed.cursor).toBeNull()
    expect(listed.truncated).toBeUndefined()
    expect(listed.reports.map((report) => report.key).sort()).toEqual(keys)
    for (const report of listed.reports) {
      expect(report.size).toBeGreaterThan(0)
      expect(new Date(report.uploaded).toISOString()).toBe(report.uploaded)
    }
    // Les images (cards/) ne sont jamais listées.
    expect(listed.reports.every((report) => report.key.startsWith('reports/'))).toBe(true)

    const csv = await call('/admin/reports/latest/summary.csv')
    expect(csv.status).toBe(200)
    expect(csv.headers.get('content-type')).toBe('text/csv; charset=utf-8')
    expectSecurityHeaders(csv)
    expect(await csv.text()).toBe(await r2Text('reports/latest/summary.csv'))

    const history = keys.find((key) => key.startsWith('reports/history/') && key.endsWith('/summary.json'))
    expect(history).toBeDefined()
    const old = await call(`/admin/${history}`)
    expect(old.status).toBe(200)
    expect(old.headers.get('content-type')).toBe('application/json; charset=utf-8')
    expect(JSON.parse(await old.text())).toMatchObject({ total: { cards: 7 } })
  })

  it('chemins de rapport invalides (.., ., segments vides) : 400 ; rapport absent : 404', async () => {
    const ctx = setup()
    await importSample(ctx)
    expect(await env.BUCKET.head('cards/fr/swsh3/swsh3-136.webp')).not.toBeNull()
    for (const path of [
      '/admin/reports/..%2Fcards%2Ffr%2Fswsh3%2Fswsh3-136.webp',
      '/admin/reports/latest%2F..%2F..%2Fcards%2Ffr%2Fswsh3%2Fswsh3-136.webp',
      '/admin/reports/%2E%2E%2Fcards%2Ffr%2Fswsh3%2Fswsh3-136.webp',
      '/admin/reports/.%2Flatest%2Fsummary.csv',
      '/admin/reports/latest//summary.csv',
      '/admin/reports/latest%2F%2Fsummary.csv',
      '/admin/reports//latest/summary.csv',
    ]) {
      const response = await call(path)
      expect(response.status, path).toBe(400)
      expect(await body(response)).toEqual({ error: 'chemin invalide' })
    }
    const missing = await call('/admin/reports/latest/absent.csv')
    expect(missing.status).toBe(404)
    expect(await body(missing)).toEqual({ error: 'rapport introuvable' })
  })

  it('chemin de rapport mal encodé (%E0%A4%A, %ZZ, %) : 400 « chemin invalide », pas 500', async () => {
    for (const path of ['/admin/reports/latest/%E0%A4%A', '/admin/reports/latest/%ZZ.csv', '/admin/reports/latest/summary%', '/admin/reports/%C0%AF']) {
      const response = await call(path)
      expect(response.status, path).toBe(400)
      const text = await response.text()
      expect(JSON.parse(text), path).toEqual({ error: 'chemin invalide' })
      // Aucun message d'erreur interne (URIError) n'est exposé.
      expect(text, path).not.toMatch(/URI|malformed/i)
    }
  })

  it('GET /admin/reports?prefix= : restreint sous reports/ (latest/, history/) ; préfixe hors de reports/ refusé', async () => {
    const ctx = setup()
    await importSample(ctx)
    await env.BUCKET.put('reports/selftest.json', '{}')

    const latest = await body<ReportList>(await call('/admin/reports?prefix=latest/'))
    expect(latest.reports.map((report) => report.key).sort()).toEqual(LATEST)
    expect(latest.cursor).toBeNull()

    const history = await body<ReportList>(await call('/admin/reports?prefix=history/'))
    expect(history.reports.length).toBe(2)
    expect(history.reports.every((report) => /^reports\/history\/[^/]+\/summary\.(csv|json)$/.test(report.key))).toBe(true)

    // Le préfixe est toujours relatif à reports/ : « cards/ » ne liste rien (jamais les images).
    const cards = await body<ReportList>(await call('/admin/reports?prefix=cards/'))
    expect(cards.reports).toEqual([])
    expect(await env.BUCKET.head('cards/fr/swsh3/swsh3-136.webp')).not.toBeNull()

    for (const prefix of ['../cards/', '/cards/', 'latest/../../cards/', '..']) {
      const response = await call(`/admin/reports?${new URLSearchParams({ prefix })}`)
      expect(response.status, prefix).toBe(400)
      expect(await body(response), prefix).toEqual({ error: 'préfixe invalide' })
    }
  })

  it('GET /admin/reports?cursor= : pagination par curseur au-delà de 1 000 rapports', async () => {
    // 1 001 petits objets sous reports/bulk/ (aucun sous cards/).
    const keys = Array.from({ length: 1_001 }, (_, i) => `reports/bulk/${String(i).padStart(4, '0')}.txt`)
    for (let i = 0; i < keys.length; i += 100) {
      await Promise.all(keys.slice(i, i + 100).map((key) => env.BUCKET.put(key, 'x')))
    }

    const first = await body<ReportList>(await call('/admin/reports'))
    expect(first.reports).toHaveLength(1_000)
    expect(typeof first.cursor).toBe('string')
    expect(first.cursor).not.toBe('')
    const second = await body<ReportList>(await call(`/admin/reports?${new URLSearchParams({ cursor: first.cursor ?? '' })}`))
    expect(second.cursor).toBeNull()
    expect([...first.reports, ...second.reports].map((report) => report.key).sort()).toEqual(keys)

    // Le curseur se combine avec le préfixe.
    const prefixed = await body<ReportList>(await call('/admin/reports?prefix=bulk/'))
    expect(prefixed.reports).toHaveLength(1_000)
    const rest = await body<ReportList>(await call(`/admin/reports?${new URLSearchParams({ prefix: 'bulk/', cursor: prefixed.cursor ?? '' })}`))
    expect(rest.reports.map((report) => report.key)).toEqual(['reports/bulk/1000.txt'])
    expect(rest.cursor).toBeNull()

    // Nettoyage ici (dans le délai du test) plutôt que dans le beforeEach suivant.
    await env.BUCKET.delete(keys.slice(0, 1_000))
    await env.BUCKET.delete(keys.slice(1_000))
  }, 60_000)
})

// ——— Objets R2 (métadonnées) ———

interface R2Listing {
  objects: { key: string; size: number; uploaded: string; contentType: string | null; metadata: Record<string, string> }[]
  cursor: string | null
}

describe('GET /admin/r2 : métadonnées des objets sous cards/ et reports/', () => {
  it('liste les images (cards/ par défaut) avec leurs métadonnées, jamais leur contenu', async () => {
    const ctx = setup()
    await importSample(ctx)
    const response = await call('/admin/r2')
    expect(response.status).toBe(200)
    expectSecurityHeaders(response)
    expect(response.headers.get('content-type')).toBe('application/json; charset=utf-8')
    const listed = await body<R2Listing>(response)
    expect(listed.cursor).toBeNull()
    expect(listed.objects.map((object) => object.key)).toEqual(await r2Keys('cards/'))
    expect(listed.objects).toHaveLength(4)
    for (const object of listed.objects) {
      expect(Object.keys(object).sort()).toEqual(['contentType', 'key', 'metadata', 'size', 'uploaded'])
      expect(new Date(object.uploaded).toISOString()).toBe(object.uploaded)
    }

    const fouinar = listed.objects.find((object) => object.key === 'cards/fr/swsh3/swsh3-136.webp')
    const card = await one<{ image_sha256: string; image_file_size: number }>(`SELECT image_sha256, image_file_size FROM cards WHERE id = 'fr:swsh3-136'`)
    expect(fouinar).toMatchObject({
      size: card.image_file_size,
      contentType: 'image/webp',
      metadata: { source: 'api', quality: 'high', sha256: card.image_sha256, language: 'fr', tcgdexId: 'swsh3-136', sourceUrl: `${ASSETS}/fr/swsh/swsh3/136/high.webp` },
    })
    expect(listed.objects.find((object) => object.key === 'cards/fr/swsh3/swsh3-137.webp')?.metadata).toMatchObject({ source: 'cdn_reconstructed' })
  })

  it('?prefix= limité à cards/… ou reports/… ; ?limit= et curseur', async () => {
    const ctx = setup()
    await importSample(ctx)
    await env.BUCKET.put('images/production.webp', webp(200, 8))
    await env.BUCKET.put('snapshots/production.json', '{}')

    const swsh3 = await body<R2Listing>(await call('/admin/r2?prefix=cards/fr/swsh3/'))
    expect(swsh3.objects.map((object) => object.key)).toEqual(['cards/fr/swsh3/swsh3-136.webp', 'cards/fr/swsh3/swsh3-137.webp'])

    const reports = await body<R2Listing>(await call('/admin/r2?prefix=reports/latest/'))
    expect(reports.objects.map((object) => object.key)).toEqual(LATEST)
    expect(reports.objects.find((object) => object.key === 'reports/latest/summary.json')?.contentType).toBe('application/json; charset=utf-8')

    // Jamais hors des préfixes de l'importateur (bucket partagé avec la production).
    for (const prefix of ['', 'images/', 'snapshots/', 'cards', 'card', 'cards/../images/', 'reports/../images/', '/cards/', 'xcards/']) {
      const response = await call(`/admin/r2?${new URLSearchParams({ prefix })}`)
      expect(response.status, `préfixe « ${prefix} »`).toBe(400)
      const text = await response.text()
      expect(JSON.parse(text), prefix).toEqual({ error: 'préfixe limité à cards/ ou reports/' })
      expect(text).not.toContain('production')
    }

    // Pagination : ?limit=1 puis curseur, jusqu'au bout.
    const seen: string[] = []
    let cursor: string | null = null
    for (let page = 0; page < 10; page++) {
      const query: URLSearchParams = new URLSearchParams({ prefix: 'cards/', limit: '1', ...(cursor ? { cursor } : {}) })
      const result: R2Listing = await body(await call(`/admin/r2?${query}`))
      expect(result.objects.length).toBeLessThanOrEqual(1)
      seen.push(...result.objects.map((object) => object.key))
      cursor = result.cursor
      if (!cursor) break
    }
    expect(seen).toEqual(await r2Keys('cards/'))
    expect(cursor).toBeNull()

    // ?limit hors bornes : ramené dans [1, 1 000].
    expect((await body<R2Listing>(await call('/admin/r2?limit=0'))).objects).toHaveLength(1)
    expect((await body<R2Listing>(await call('/admin/r2?limit=5000'))).objects).toHaveLength(4)
  })
})

// ——— Audit D1 ↔ R2 ———

describe('audit D1 ↔ R2 (tâche report)', () => {
  it('objet présent dans R2 pour une carte pending → tâche d’image, puis already_present sans téléchargement', async () => {
    const ctx = setup(sampleCatalog(), { MIN_REQUEST_INTERVAL_MS: '50' })
    await importSample(ctx)
    const key = 'cards/fr/swsh3/swsh3-138.webp'
    const bytes = webp(400, 9)
    const sha256 = await sha256Hex(bytes)
    await env.BUCKET.put(key, bytes, { customMetadata: { sha256, source: 'cdn_reconstructed', sourceUrl: `${ASSETS}/fr/swsh/swsh3/138/high.webp`, quality: 'high' } })
    await env.DB.prepare(`UPDATE cards SET image_status = 'pending', image_failure_reason = NULL WHERE id = 'fr:swsh3-138'`).run()
    const requestsBefore = ctx.fake.requests.length

    ctx.clock.advance(60_000)
    await direct(ctx, 'POST', '/admin/reports/generate')
    const reportTick = await body<TickResult>(await direct(ctx, 'POST', '/admin/tick?max_jobs=1'))
    expect(reportTick).toMatchObject({ claimed: 1, done: 1 })
    expect(await one(`SELECT status FROM import_jobs WHERE dedupe_key = 'image:fr:swsh3-138'`)).toEqual({ status: 'pending' })
    const audit = (JSON.parse(await r2Text('reports/latest/summary.json')) as { audit: Record<string, number> }).audit
    expect(audit).toMatchObject({ r2Objects: 5, reconciled: 1, unknownObjects: 0, missingObjects: 0 })

    const imageTick = await body<TickResult>(await direct(ctx, 'POST', '/admin/tick'))
    expect(imageTick).toMatchObject({ claimed: 1, done: 1, httpRequests: 0 })
    expect(await one(`SELECT image_status, image_r2_key, image_file_size, image_sha256, image_source FROM cards WHERE id = 'fr:swsh3-138'`)).toEqual({
      image_status: 'already_present',
      image_r2_key: key,
      image_file_size: 400,
      image_sha256: sha256,
      image_source: 'cdn_reconstructed',
    })
    // Aucun téléchargement : l'objet existant est conservé tel quel.
    expect(ctx.fake.requests.length).toBe(requestsBefore)
    expect(new Uint8Array(await ((await env.BUCKET.get(key)) as R2ObjectBody).arrayBuffer())).toEqual(bytes)
  })

  it('carte downloaded dont l’objet a disparu de R2 → remise pending, puis recopiée', async () => {
    const ctx = setup(sampleCatalog(), { MIN_REQUEST_INTERVAL_MS: '50' })
    await importSample(ctx)
    const key = 'cards/fr/swsh3/swsh3-136.webp'
    await env.BUCKET.delete(key)

    ctx.clock.advance(60_000)
    await direct(ctx, 'POST', '/admin/reports/generate')
    await direct(ctx, 'POST', '/admin/tick?max_jobs=1')
    expect(await one(`SELECT image_status, image_failure_reason FROM cards WHERE id = 'fr:swsh3-136'`)).toEqual({ image_status: 'pending', image_failure_reason: 'objet absent de R2 (audit)' })
    expect(await one(`SELECT status FROM import_jobs WHERE dedupe_key = 'image:fr:swsh3-136'`)).toEqual({ status: 'pending' })
    const audit = (JSON.parse(await r2Text('reports/latest/summary.json')) as { audit: Record<string, number> }).audit
    expect(audit).toMatchObject({ r2Objects: 3, missingObjects: 1, reconciled: 0 })

    const downloads = ctx.fake.count((url) => url === `${ASSETS}/fr/swsh/swsh3/136/high.webp`)
    const tick = await body<TickResult>(await direct(ctx, 'POST', '/admin/tick'))
    expect(tick).toMatchObject({ claimed: 1, done: 1, httpRequests: 1 })
    expect(ctx.fake.count((url) => url === `${ASSETS}/fr/swsh/swsh3/136/high.webp`)).toBe(downloads + 1)
    expect(await one(`SELECT image_status FROM cards WHERE id = 'fr:swsh3-136'`)).toEqual({ image_status: 'downloaded' })
    expect(new Uint8Array(await ((await env.BUCKET.get(key)) as R2ObjectBody).arrayBuffer())).toEqual(webp(300, 1))
  })

  it('les chiffres du rapport reflètent l’état réconcilié (corrections de l’audit appliquées avant le calcul)', async () => {
    const ctx = setup()
    await importSample(ctx)
    const before = JSON.parse(await r2Text('reports/latest/summary.json')) as { status: string; languages: Record<string, number>[]; total: Record<string, number> }
    expect(before.status).toBe('TERMINÉ')
    expect(before.languages[0]).toMatchObject({ imagesStored: 2, imagesDownloaded: 1, imagesPending: 0, bytes: 610 })
    expect(await one(`SELECT image_status, image_r2_key IS NOT NULL AS has_key, image_quality FROM cards WHERE id = 'fr:swsh3-136'`)).toEqual({ image_status: 'downloaded', has_key: 1, image_quality: 'high' })

    // L'objet de la carte téléchargée disparaît de R2.
    await env.BUCKET.delete('cards/fr/swsh3/swsh3-136.webp')
    ctx.clock.advance(60_000)
    await direct(ctx, 'POST', '/admin/reports/generate')
    const tick = await body<TickResult>(await direct(ctx, 'POST', '/admin/tick?max_jobs=1'))
    expect(tick).toMatchObject({ claimed: 1, done: 1, failed: 0, httpRequests: 0 })

    // La carte perd toutes ses informations d'image.
    expect(
      await one(
        `SELECT image_status, image_r2_key, image_sha256, image_file_size, image_source, image_quality, image_downloaded_at, image_failure_reason
         FROM cards WHERE id = 'fr:swsh3-136'`,
      ),
    ).toEqual({
      image_status: 'pending',
      image_r2_key: null,
      image_sha256: null,
      image_file_size: null,
      image_source: null,
      image_quality: null,
      image_downloaded_at: null,
      image_failure_reason: 'objet absent de R2 (audit)',
    })

    // Le rapport produit par cette même tâche compte déjà l'état corrigé.
    const summary = JSON.parse(await r2Text('reports/latest/summary.json')) as {
      status: string
      languages: Record<string, number>[]
      total: Record<string, number>
      audit: Record<string, number>
      sets: SetCoverage[]
    }
    expect(summary.audit).toMatchObject({ missingObjects: 1, r2Objects: 3, r2Bytes: 310 + 320 + 330 })
    expect(summary.languages[0]).toMatchObject({ cards: 4, imagesStored: 1, imagesDownloaded: 0, imagesRecovered: 1, imagesPending: 1, bytes: 310, coveragePercent: 25 })
    expect(summary.total).toMatchObject({ imagesStored: 3, bytes: 310 + 320 + 330, jobsActive: 1 })
    // Une tâche d'image attend : l'importation n'est plus « TERMINÉ ».
    expect(summary.status).toBe('EN COURS')
    expect(summary.sets.find((set) => set.set_id === 'swsh3')).toMatchObject({ stored: 1, downloaded: 0, pending: 1, bytes: 310 })
    const csv = csvObjects(await r2Text('reports/latest/summary.csv'))
    expect(csv.find((row) => row.extension === 'swsh3')).toMatchObject({ stockees: '1', telechargees: '0', en_attente: '1', octets: '310', couverture_pct: '33.33' })
    expect(csv.find((row) => row.langue === 'fr' && row.extension === 'TOTAL')).toMatchObject({ stockees: '1', en_attente: '1', octets: '310', couverture_pct: '25.00' })
    // Le statut affiché par l'administration est cohérent avec le rapport.
    const stats = await body<Stats>(await direct(ctx, 'GET', '/admin/status?format=json'))
    expect(stats.languages[0]).toMatchObject({ imagesStored: 1, imagesPending: 1, bytes: 310 })
  })

  it('objet inconnu sous cards/ : compté par l’audit, jamais supprimé ni rattaché', async () => {
    const ctx = setup()
    await importSample(ctx)
    await env.BUCKET.put('cards/fr/inconnue/carte-inconnue.webp', webp(200, 7))
    const jobsBefore = (await one<{ n: number }>(`SELECT COUNT(*) AS n FROM import_jobs`)).n

    ctx.clock.advance(60_000)
    await direct(ctx, 'POST', '/admin/reports/generate')
    await direct(ctx, 'POST', '/admin/tick')
    const audit = (JSON.parse(await r2Text('reports/latest/summary.json')) as { audit: Record<string, number> }).audit
    expect(audit).toMatchObject({ r2Objects: 5, r2Bytes: 1460, unknownObjects: 1, reconciled: 0, missingObjects: 0 })
    expect(await env.BUCKET.head('cards/fr/inconnue/carte-inconnue.webp')).not.toBeNull()
    expect((await one<{ n: number }>(`SELECT COUNT(*) AS n FROM import_jobs`)).n).toBe(jobsBefore)
    expect(await rows(`SELECT id FROM import_jobs WHERE status != 'done'`)).toEqual([])
  })
})

// ——— Autotest et exécution manuelle ———

describe('autotest et exécution manuelle', () => {
  it('POST /admin/selftest : D1, R2 (sous reports/ uniquement) et TCGdex FR / JA, avec le faux TCGdex', async () => {
    const ctx = setup(sampleCatalog(), { MIN_REQUEST_INTERVAL_MS: '50' })
    const response = await direct(ctx, 'POST', '/admin/selftest')
    expect(response.status).toBe(200)
    expectSecurityHeaders(response)
    const result = await body<{ at: string; d1: { ok: boolean; tables: string[] }; r2: { ok: boolean; key: string }; tcgdex: Record<string, { ok: boolean; series?: number }> }>(response)
    expect(result.at).toBe(new Date(ctx.clock.now()).toISOString())
    expect(result.d1.ok).toBe(true)
    expect(result.d1.tables).toEqual(expect.arrayContaining(['cards', 'import_jobs', 'runtime_state', 'series', 'sets', 'sync_runs', 'tick_runs']))
    expect(result.r2).toEqual({ ok: true, key: 'reports/selftest.json' })
    expect(result.tcgdex).toEqual({ fr: { ok: true, series: 2 }, ja: { ok: true, series: 2 } })

    // Écritures limitées à reports/ (R2) et à runtime_state (D1).
    expect(await r2Keys()).toEqual(['reports/selftest.json'])
    expect(JSON.parse(await r2Text('reports/selftest.json'))).toEqual({ at: result.at })
    expect(await rows(`SELECT key FROM runtime_state`)).toEqual([{ key: 'selftest' }])
    expect(await rows(`SELECT id FROM import_jobs`)).toEqual([])
    // Deux requêtes, vers l'API de TCGdex uniquement, avec l'agent utilisateur générique.
    expect(ctx.fake.requests.map((request) => request.url)).toEqual([`${API}/fr/series`, `${API}/ja/series`])
    expect(ctx.fake.requests.every((request) => request.headers['user-agent'] === 'tcgdex-r2-importer/1.0')).toBe(true)
  })

  it('POST /admin/selftest : une panne de TCGdex est signalée sans faire échouer la route', async () => {
    const fake = sampleCatalog()
    fake.queue(`${API}/ja/series`, () => new Response('indisponible', { status: 503 }))
    const ctx = setup(fake, { MIN_REQUEST_INTERVAL_MS: '50' })
    const response = await direct(ctx, 'POST', '/admin/selftest')
    expect(response.status).toBe(200)
    const result = await body<{ d1: { ok: boolean }; r2: { ok: boolean }; tcgdex: Record<string, unknown> }>(response)
    expect(result.d1.ok).toBe(true)
    expect(result.r2.ok).toBe(true)
    expect(result.tcgdex).toEqual({ fr: { ok: true, series: 2 }, ja: { ok: false, outcome: 'server_error', status: 503 } })
  })

  it('POST /admin/tick?max_jobs=… traite le nombre de tâches demandé et renvoie un bilan', async () => {
    const ctx = setup(sampleCatalog(), { MIN_REQUEST_INTERVAL_MS: '50' })
    await direct(ctx, 'POST', '/admin/sync/start?language=fr')

    const first = await direct(ctx, 'POST', '/admin/tick?max_jobs=1')
    expect(first.status).toBe(200)
    const tick = await body<TickResult>(first)
    expect(tick).toMatchObject({ status: 'ran', reason: 'maximum de 1 tâches atteint', claimed: 1, done: 1, retried: 0, failed: 0, released: 0, httpRequests: 2, bytesStored: 0 })
    expect(tick.d1Queries).toBeGreaterThan(0)
    expect(ctx.fake.requests.map((request) => request.url)).toEqual([`${API}/fr/series`, `${API}/fr/sets`])
    expect(await rows(`SELECT job_type, target_id, status FROM import_jobs ORDER BY id`)).toEqual([
      { job_type: 'catalog', target_id: null, status: 'done' },
      { job_type: 'set', target_id: 'swsh3', status: 'pending' },
      { job_type: 'set', target_id: 'exu', status: 'pending' },
      { job_type: 'card_index', target_id: null, status: 'pending' },
    ])
    const tickRun = await one<{ trigger: string; jobs_claimed: number; jobs_done: number; http_requests: number }>(`SELECT trigger, jobs_claimed, jobs_done, http_requests FROM tick_runs`)
    expect(tickRun).toEqual({ trigger: 'admin', jobs_claimed: 1, jobs_done: 1, http_requests: 2 })
    const lastTick = JSON.parse((await one<{ value: string }>(`SELECT value FROM runtime_state WHERE key = 'last_tick'`)).value) as { trigger: string; stopReason: string }
    expect(lastTick).toMatchObject({ trigger: 'admin', stopReason: 'maximum de 1 tâches atteint' })

    // Deux tâches de plus : les deux extensions (priorité avant l'index des cartes).
    const second = await body<TickResult>(await direct(ctx, 'POST', '/admin/tick?max_jobs=2'))
    expect(second).toMatchObject({ status: 'ran', claimed: 2, done: 2, httpRequests: 2 })
    expect(await rows(`SELECT target_id FROM import_jobs WHERE job_type = 'set' AND status = 'done' ORDER BY target_id`)).toEqual([{ target_id: 'exu' }, { target_id: 'swsh3' }])

    // max_jobs hors bornes : ramené à 1.
    const clamped = await body<TickResult>(await direct(ctx, 'POST', '/admin/tick?max_jobs=0'))
    expect(clamped.claimed).toBe(1)
    // Toutes les requêtes sont allées au faux TCGdex (API ou CDN), jamais ailleurs.
    expect(ctx.fake.requests.every((request) => request.url.startsWith(`${API}/`) || request.url.startsWith(`${ASSETS}/`))).toBe(true)
  })

  it('POST /admin/tick sans tâche : idle, aucune requête', async () => {
    const ctx = setup()
    const tick = await body<TickResult>(await direct(ctx, 'POST', '/admin/tick'))
    expect(tick).toMatchObject({ status: 'idle', claimed: 0, httpRequests: 0 })
    expect(ctx.fake.requests).toEqual([])
  })

  it('POST /admin/tick : budget de temps plafonné à 10 000 ms, même avec ?budget_ms plus grand', async () => {
    /**
     * Synchronisation FR démarrée ; la réponse `/fr/sets` (dernière requête de la tâche catalog) fait
     * avancer l'horloge simulée de `jumpMs`. Après cette première tâche, l'exécution continue (deux
     * tâches `set` prêtes) seulement si le budget n'est pas encore écoulé.
     */
    async function scenario(jumpMs: number, query: string): Promise<TickResult> {
      await resetAll()
      const ctx = setup(sampleCatalog(), { MIN_REQUEST_INTERVAL_MS: '50' })
      await direct(ctx, 'POST', '/admin/sync/start?language=fr')
      const fetcher: typeof fetch = async (input, init) => {
        const response = await ctx.fake.fetcher(input, init)
        const url = typeof input === 'string' ? input : input instanceof URL ? input.toString() : input.url
        if (url === `${API}/fr/sets`) ctx.clock.advance(jumpMs)
        return response
      }
      return body<TickResult>(await direct(ctx, 'POST', `/admin/tick?${query}`, { fetcher }))
    }

    // Configuration : 240 000 ms par exécution planifiée ; ?budget_ms=600000 est ramené à 10 000
    // (l'exécution doit finir dans les 30 s de waitUntil, dépassement compris).
    expect(setup().deps.config.runTimeBudgetMs).toBeGreaterThan(10_000)
    const capped = await scenario(11_000, 'budget_ms=600000&max_jobs=3')
    expect(capped).toMatchObject({ status: 'ran', reason: 'budget de temps épuisé', claimed: 1, done: 1 })
    expect(capped.durationMs).toBeGreaterThanOrEqual(11_000)
    expect(await rows(`SELECT target_id FROM import_jobs WHERE job_type = 'set' AND status = 'pending' ORDER BY target_id`)).toEqual([{ target_id: 'exu' }, { target_id: 'swsh3' }])

    // Sans ?budget_ms : 10 000 ms aussi.
    expect(await scenario(11_000, 'max_jobs=3')).toMatchObject({ reason: 'budget de temps épuisé', claimed: 1 })
    // 9 s écoulées : encore dans le budget, l'exécution continue jusqu'au maximum de tâches.
    expect(await scenario(9_000, 'budget_ms=600000&max_jobs=3')).toMatchObject({ reason: 'maximum de 3 tâches atteint', claimed: 3, done: 3 })
    // Un budget plus petit que le plafond est respecté.
    expect(await scenario(6_000, 'budget_ms=5000&max_jobs=3')).toMatchObject({ reason: 'budget de temps épuisé', claimed: 1 })
  })

  it('POST /admin/tick : l’exécution est confiée à waitUntil (elle va au bout si le client se déconnecte)', async () => {
    const ctx = setup(sampleCatalog(), { MIN_REQUEST_INTERVAL_MS: '50' })
    await direct(ctx, 'POST', '/admin/sync/start?language=fr')
    const kept: Promise<unknown>[] = []
    const waitUntil = vi.fn((promise: Promise<unknown>) => {
      kept.push(promise)
    })

    const response = await direct(ctx, 'POST', '/admin/tick?max_jobs=1', { waitUntil })
    expect(response.status).toBe(200)
    expect(waitUntil).toHaveBeenCalledTimes(1)
    const tick = await body<TickResult>(response)
    expect(tick).toMatchObject({ status: 'ran', claimed: 1, done: 1 })
    // La promesse confiée à waitUntil est l'exécution elle-même.
    expect(await kept[0]).toEqual(tick)
    // Exécution allée au bout : verrou de l'exécuteur libéré (expiré dès la fin) et bilan écrit.
    const lock = await one<{ expires_at: string }>(`SELECT expires_at FROM runtime_state WHERE key = 'runner_lock'`)
    expect(Date.parse(lock.expires_at)).toBeLessThanOrEqual(ctx.clock.now())
    expect(await one(`SELECT trigger, jobs_claimed FROM tick_runs`)).toEqual({ trigger: 'admin', jobs_claimed: 1 })

    // Les autres routes n'utilisent pas waitUntil.
    for (const [method, path] of [
      ['GET', '/admin/status'],
      ['POST', '/admin/sync/pause'],
      ['POST', '/admin/sync/resume'],
      ['POST', '/admin/reports/generate'],
    ] as const) {
      expect((await direct(ctx, method, path, { waitUntil })).status, `${method} ${path}`).toBe(200)
    }
    expect(waitUntil).toHaveBeenCalledTimes(1)
  })

  it('POST /admin/tick : une exécution qui échoue renvoie 500, et la promesse de waitUntil ne rejette jamais', async () => {
    const ctx = setup()
    const broken = {
      prepare() {
        throw new Error('D1 indisponible (simulé)')
      },
      batch() {
        throw new Error('D1 indisponible (simulé)')
      },
    } as unknown as D1Database
    const kept: Promise<unknown>[] = []
    const response = await direct(ctx, 'POST', '/admin/tick', { d1: broken, waitUntil: (promise) => kept.push(promise) })
    expect(response.status).toBe(500)
    expect((await body<{ error: string }>(response)).error).toContain('D1 indisponible (simulé)')
    expect(kept).toHaveLength(1)
    await expect(kept[0]).resolves.toBeUndefined()
  })

  it('src/index.ts : POST /admin/tick reçoit ctx.waitUntil du Worker', async () => {
    const network = forbidNetwork()
    const handler: ExportedHandler<Env> = worker
    const executionCtx = createExecutionContext()
    const spy = vi.spyOn(executionCtx, 'waitUntil')
    const request = new Request(`${ORIGIN}/admin/tick`, { method: 'POST', headers: { authorization: `Bearer ${TOKEN}` } })
    const response = await handler.fetch?.(request as Request<unknown, IncomingRequestCfProperties>, env, executionCtx)
    await waitOnExecutionContext(executionCtx)
    expect(response?.status).toBe(200)
    expect(await response?.json()).toMatchObject({ status: 'idle', claimed: 0 })
    expect(spy).toHaveBeenCalledTimes(1)
    expect(spy.mock.calls[0]?.[0]).toBeInstanceOf(Promise)
    expect(network).not.toHaveBeenCalled()
  })
})

// ——— Handler planifié ———

describe('handler planifié (src/index.ts)', () => {
  it('Cron hebdomadaire « 7 3 * * MON » : crée les synchronisations weekly sans aucune requête réseau', async () => {
    // Variable du Worker (wrangler.jsonc) et configuration lue : lundi 03:07 UTC.
    expect(env.SYNC_CRON).toBe('7 3 * * MON')
    expect(setup().deps.config.syncCron).toBe('7 3 * * MON')
    const network = forbidNetwork()
    await runScheduled('7 3 * * MON', Date.parse('2026-11-16T03:07:00.000Z'))
    expect(network).not.toHaveBeenCalled()
    expect(await rows(`SELECT language, kind, status FROM sync_runs ORDER BY language`)).toEqual([
      { language: 'fr', kind: 'weekly', status: 'running' },
      { language: 'ja', kind: 'weekly', status: 'running' },
    ])
    // Les tâches catalogue attendent le Cron de traitement : rien n'a été exécuté.
    expect(await rows(`SELECT job_type, language, status, attempts FROM import_jobs ORDER BY language`)).toEqual([
      { job_type: 'catalog', language: 'fr', status: 'pending', attempts: 0 },
      { job_type: 'catalog', language: 'ja', status: 'pending', attempts: 0 },
    ])
    expect(await rows(`SELECT id FROM tick_runs`)).toEqual([])
    expect(await r2Keys()).toEqual([])

    // Un second déclenchement (doublon du Cron) ne crée rien de plus.
    await runScheduled('7 3 * * MON', Date.parse('2026-11-16T03:07:00.000Z'))
    expect((await one<{ n: number }>(`SELECT COUNT(*) AS n FROM sync_runs`)).n).toBe(2)
    expect(network).not.toHaveBeenCalled()
  })

  it('l’ancienne expression « 0 3 * * MON » ne déclenche plus de synchronisation hebdomadaire', async () => {
    const network = forbidNetwork()
    await runScheduled('0 3 * * MON', Date.parse('2026-11-16T03:00:00.000Z'))
    expect(network).not.toHaveBeenCalled()
    // Traitée comme un Cron inconnu : simple exécution, sans tâche donc sans écriture.
    expect(await rows(`SELECT id FROM sync_runs`)).toEqual([])
    expect(await rows(`SELECT id FROM import_jobs`)).toEqual([])
    expect(await rows(`SELECT id FROM tick_runs`)).toEqual([])
  })

  it('Cron de traitement sans tâche : idle, aucune requête réseau, aucune écriture', async () => {
    const network = forbidNetwork()
    await runScheduled('*/5 * * * *', Date.now())
    expect(network).not.toHaveBeenCalled()
    expect(await rows(`SELECT key FROM runtime_state`)).toEqual([])
    expect(await rows(`SELECT id FROM tick_runs`)).toEqual([])
    expect(await r2Keys()).toEqual([])
  })

  it('Cron de traitement en pause, avec des tâches en attente : aucune requête réseau', async () => {
    const network = forbidNetwork()
    await call('/admin/sync/start', { method: 'POST' })
    await call('/admin/sync/pause', { method: 'POST' })
    await runScheduled('*/5 * * * *', Date.now())
    expect(network).not.toHaveBeenCalled()
    expect(await rows(`SELECT status, attempts FROM import_jobs`)).toEqual([
      { status: 'pending', attempts: 0 },
      { status: 'pending', attempts: 0 },
    ])
    expect(await rows(`SELECT id FROM tick_runs`)).toEqual([])
  })
})

// ——— Préfixes R2 ———

describe('préfixes R2', () => {
  it('aucune clé R2 écrite par l’importateur ne sort de cards/ et reports/', async () => {
    const fake = sampleCatalog()
    // Images aussi pour les identifiants à caractères spéciaux (« % » et « + »).
    fake.addImage(`${ASSETS}/fr/ex/exu/%253F/high.webp`, webp(260, 5))
    fake.addImage(`${ASSETS}/ja/SM/${encodeURIComponent('SM1+')}/001/high.webp`, webp(270, 6))
    const ctx = setup(fake, { MIN_REQUEST_INTERVAL_MS: '50' })
    await importSample(ctx)
    ctx.clock.advance(60_000)
    await direct(ctx, 'POST', '/admin/sync/retry-missing')
    await direct(ctx, 'POST', '/admin/reports/generate')
    await direct(ctx, 'POST', '/admin/tick')
    await direct(ctx, 'POST', '/admin/selftest')

    const keys = await r2Keys()
    expect(keys.length).toBeGreaterThan(8)
    for (const key of keys) expect(key.startsWith('cards/') || key.startsWith('reports/'), key).toBe(true)
    const images = keys.filter((key) => key.startsWith('cards/'))
    expect(images).toEqual([
      'cards/fr/exu/exu-_253F.webp',
      'cards/fr/swsh3/swsh3-136.webp',
      'cards/fr/swsh3/swsh3-137.webp',
      'cards/ja/SM1_2B/SM1_2B-001.webp',
      'cards/ja/SV1a/SV1a-001.webp',
      'cards/ja/SV1a/SV1a-002.webp',
    ])
    for (const key of images) expect(key).toMatch(/^cards\/(fr|ja)\/[A-Za-z0-9._-]+\/[A-Za-z0-9._-]+\.webp$/)
    for (const key of keys.filter((name) => name.startsWith('reports/'))) {
      expect(key).toMatch(/^reports\/(selftest\.json|latest\/[a-z_]+\.(csv|json)|history\/\d{4}-\d\d-\d\dT\d\d-\d\d-\d\dZ\/summary\.(csv|json))$/)
    }
  })
})
