/**
 * Exécuteur des tâches et synchronisation : verrous, reprise après interruption, concurrence,
 * budgets, plafonds, pause, commandes de synchronisation et synchronisation hebdomadaire
 * (src/services/runner.ts, sync.ts, catalog.ts, cards.ts).
 *
 * Aucun appel réseau : TCGdex est simulé (FakeTcgdex) et l'horloge aussi (Clock). Chaque test part
 * d'une D1 et d'un R2 vides (resetAll).
 */

import { env } from 'cloudflare:workers'
import { beforeEach, describe, expect, it } from 'vitest'
import { handleAdmin } from '../src/admin/router'
import type { Language } from '../src/config'
import { Db } from '../src/db'
import { claimJobs, reclaimExpired } from '../src/repositories/jobs'
import { acquireRunnerLease } from '../src/repositories/state'
import { finalizeSyncRuns, mapPool, runTick, type RunnerDeps, type TickResult } from '../src/services/runner'
import { cancelSync, pauseSync, resumeSync, retryMissing, startSync } from '../src/services/sync'
import { iso } from '../src/utils/time'
import { API, ASSETS, Clock, drain, jsonResponse, makeDeps, one, resetAll, rows, sampleCatalog, sha256Hex, webp, type FakeSet, type FakeTcgdex } from './helpers'

beforeEach(resetAll)

const DAY = 86_400_000
const MIN = 60_000

const URL_136 = `${ASSETS}/fr/swsh/swsh3/136/high.webp`
const URL_137 = `${ASSETS}/fr/swsh/swsh3/137/high.webp`
const URL_138 = `${ASSETS}/fr/swsh/swsh3/138/high.webp`
const URL_EXU = `${ASSETS}/fr/ex/exu/%253F/high.webp`

interface CardRow {
  id: string
  name: string
  local_id: string
  set_id: string | null
  series_id: string | null
  api_image_url: string | null
  image_status: string
  image_source: string | null
  image_r2_key: string | null
  image_sha256: string | null
  image_file_size: number | null
  image_failure_reason: string | null
  image_checked_at: string | null
  image_downloaded_at: string | null
  detail_status: string
  detail_fetched_at: string | null
  brief_hash: string | null
  removed_at: string | null
  updated_at: string
}

interface JobRow {
  id: number
  job_type: string
  dedupe_key: string
  status: string
  attempts: number
  max_attempts: number
  releases: number
  last_error: string | null
  last_http_status: number | null
  next_attempt_at: string
  locked_until: string | null
  lock_token: string | null
  run_id: number | null
  payload_json: string | null
  completed_at: string | null
}

interface SyncRunRow {
  id: number
  language: string
  kind: string
  status: string
  scope_json: string | null
  series_found: number
  sets_found: number
  cards_found: number
  cards_processed: number
  images_downloaded: number
  images_recovered: number
  images_already_present: number
  images_missing: number
  images_failed: number
  bytes_stored: number
}

const db = () => new Db(env.DB)
const tick = (deps: RunnerDeps, opts: { maxJobs?: number; budgetMs?: number } = {}) => runTick(deps, { trigger: 'test', ...opts })
const card = (id: string) => one<CardRow>(`SELECT * FROM cards WHERE id = ?1`, id)
const job = (dedupeKey: string) => one<JobRow>(`SELECT * FROM import_jobs WHERE dedupe_key = ?1`, dedupeKey)
const notDone = () => rows<{ dedupe_key: string; status: string }>(`SELECT dedupe_key, status FROM import_jobs WHERE status != 'done' ORDER BY id`)
const urlOf = (input: RequestInfo | URL): string => (typeof input === 'string' ? input : input instanceof URL ? input.href : input.url)

async function runtimeState(): Promise<Record<string, string | null>> {
  const list = await rows<{ key: string; value: string | null }>(`SELECT key, value FROM runtime_state`)
  return Object.fromEntries(list.map((row) => [row.key, row.value]))
}

/** Synchronisation initiale complète (FR par défaut), menée à son terme. */
async function importAll(deps: RunnerDeps, clock: Clock, languages: Language[] = ['fr']): Promise<TickResult[]> {
  const started = await startSync(db(), deps.config, clock.now(), { languages })
  expect(started.started.map((run) => run.language)).toEqual(languages)
  const ticks = await drain(deps, clock)
  expect(ticks.at(-1)?.status).toBe('idle')
  expect(await rows(`SELECT id FROM sync_runs WHERE status = 'running'`)).toEqual([])
  return ticks
}

/** Synchronisation hebdomadaire `days` jours plus tard (FR), menée à son terme. */
async function weeklySync(deps: RunnerDeps, clock: Clock, days = 7): Promise<number> {
  clock.advance(days * DAY)
  const started = await startSync(db(), deps.config, clock.now(), { kind: 'weekly', languages: ['fr'] })
  expect(started.started.map((run) => run.kind)).toEqual(['weekly'])
  const ticks = await drain(deps, clock)
  expect(ticks.at(-1)?.status).toBe('idle')
  return (started.started[0] as { runId: number }).runId
}

/**
 * Amène la synchronisation FR au point où ses 4 tâches d'image et ses 4 tâches de détail sont prêtes :
 * une exécution limitée à 4 tâches traite le catalogue et les 2 extensions ; l'index des cartes est
 * rendu (il attend la fin des extensions).
 */
async function prepareImageJobs(deps: RunnerDeps, clock: Clock): Promise<void> {
  await startSync(db(), deps.config, clock.now(), { languages: ['fr'] })
  const first = await tick(deps, { maxJobs: 4 })
  expect(first).toMatchObject({ status: 'ran', claimed: 4, done: 3, released: 1 })
  const ready = await rows<{ job_type: string; n: number }>(
    `SELECT job_type, COUNT(*) AS n FROM import_jobs WHERE status = 'pending' AND next_attempt_at <= ?1 GROUP BY job_type ORDER BY job_type`,
    iso(clock.now()),
  )
  expect(ready).toEqual([
    { job_type: 'card', n: 4 },
    { job_type: 'image', n: 4 },
  ])
}

/**
 * R2 dont `head` échoue pour une clé donnée, `failures` fois au plus : `reject` (panne passagère :
 * la promesse est rejetée) ou `corrupt` (réponse incohérente : lire l'objet rendu lève une exception,
 * hors de toute gestion d'erreur de la tâche).
 */
function flakyBucket(failingKey: string, failures = Number.POSITIVE_INFINITY, mode: 'reject' | 'corrupt' = 'reject'): R2Bucket {
  let left = failures
  return new Proxy(env.BUCKET, {
    get(target, property) {
      if (property === 'head') {
        return async (key: string) => {
          if (key === failingKey && left > 0) {
            left--
            if (mode === 'reject') throw new Error('R2 indisponible (simulé)')
            return {
              get size(): number {
                throw new Error('R2 incohérent (simulé)')
              },
            } as unknown as R2Object
          }
          return target.head(key)
        }
      }
      const value: unknown = Reflect.get(target, property, target)
      return typeof value === 'function' ? (value as (...args: unknown[]) => unknown).bind(target) : value
    },
  })
}

/** R2 qui compte ses opérations (sous-requêtes au sens de Cloudflare). */
function countingBucket(): { bucket: R2Bucket; ops: () => number } {
  let ops = 0
  const bucket = new Proxy(env.BUCKET, {
    get(target, property) {
      const value: unknown = Reflect.get(target, property, target)
      if (typeof value !== 'function') return value
      return (...args: unknown[]) => {
        ops++
        return (value as (...args: unknown[]) => unknown).apply(target, args)
      }
    },
  })
  return { bucket, ops: () => ops }
}

interface D1Probe {
  d1: D1Database
  /** Requêtes D1 réellement envoyées (un batch compte pour une), comme les compte Cloudflare. */
  queries: number
  /** Taille demandée de chaque réservation de tâches (`LIMIT` de claimJobs), dans l'ordre. */
  claimLimits: number[]
}

/**
 * D1 instrumentée : compte chaque requête réellement envoyée et relève la taille des réservations ;
 * `before` est appelé (et attendu) avant l'envoi de chaque requête isolée.
 */
function probeD1(before?: (sql: string) => Promise<void> | void): D1Probe {
  const real = new WeakMap<object, D1PreparedStatement>()
  const probe: D1Probe = { d1: env.DB, queries: 0, claimLimits: [] }
  const wrap = (statement: D1PreparedStatement, sql: string, params: unknown[]): D1PreparedStatement => {
    const proxy = new Proxy(statement, {
      get(target, property) {
        const value: unknown = Reflect.get(target, property, target)
        if (property === 'bind') return (...values: unknown[]) => wrap(target.bind(...values), sql, values)
        if (typeof value !== 'function') return value
        const method = (value as (...args: unknown[]) => unknown).bind(target)
        if (property !== 'first' && property !== 'all' && property !== 'run' && property !== 'raw') return method
        return async (...args: unknown[]) => {
          if (before) await before(sql)
          probe.queries++
          if (/SET status = 'running', attempts = attempts \+ 1/.test(sql)) probe.claimLimits.push(Number(params[3]))
          return method(...args)
        }
      },
    })
    real.set(proxy, statement)
    return proxy
  }
  probe.d1 = new Proxy(env.DB, {
    get(target, property) {
      if (property === 'prepare') return (sql: string) => wrap(target.prepare(sql), sql, [])
      if (property === 'batch') {
        return async (statements: D1PreparedStatement[]) => {
          probe.queries++
          return target.batch(statements.map((statement) => real.get(statement) ?? statement))
        }
      }
      const value: unknown = Reflect.get(target, property, target)
      return typeof value === 'function' ? (value as (...args: unknown[]) => unknown).bind(target) : value
    },
  })
  return probe
}

interface Measured {
  result: TickResult
  /** Requêtes D1 de toute l'invocation, clôture comprise. */
  d1: number
  /** Sous-requêtes de toute l'invocation : D1, fetch et R2. */
  subrequests: number
}

/** Comme `drain`, en mesurant les requêtes réellement envoyées par chaque exécution. */
async function drainMeasured(deps: RunnerDeps, clock: Clock, fake: FakeTcgdex, probe: D1Probe, r2: { ops: () => number }, maxTicks = 60): Promise<Measured[]> {
  const measured: Measured[] = []
  for (let i = 0; i < maxTicks; i++) {
    const before = { d1: probe.queries, fetch: fake.requests.length, r2: r2.ops() }
    const result = await tick(deps)
    const d1 = probe.queries - before.d1
    measured.push({ result, d1, subrequests: d1 + fake.requests.length - before.fetch + r2.ops() - before.r2 })
    if (result.status === 'idle') {
      const next = await one<{ at: string | null }>(`SELECT MIN(next_attempt_at) AS at FROM import_jobs WHERE status = 'pending'`)
      if (!next.at) break
      clock.t = Math.max(clock.t, Date.parse(next.at)) + 1_000
      continue
    }
    clock.advance(5 * MIN)
  }
  return measured
}

/** Extension synthétique de `n` cartes sans image officielle et absentes du CDN. */
function bulkSet(id: string, n: number): FakeSet {
  return {
    id,
    name: `Extension ${id}`,
    serie: { id: 'swsh', name: 'Épée et Bouclier' },
    cards: Array.from({ length: n }, (_, i) => ({ id: `${id}-${i + 1}`, localId: String(i + 1), name: `Carte ${i + 1}` })),
  }
}

/** Écrit une valeur de `runtime_state` directement (autre exécution, administration). */
async function setState(key: string, value: string | null, nowMs: number): Promise<void> {
  await env.DB.prepare(
    `INSERT INTO runtime_state (key, value, updated_at) VALUES (?1, ?2, ?3)
     ON CONFLICT (key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at`,
  )
    .bind(key, value, iso(nowMs))
    .run()
}

/** Avance l'horloge jusqu'à ce que la tâche soit de nouveau prête. */
async function untilReady(clock: Clock, dedupeKey: string): Promise<void> {
  clock.t = Math.max(clock.t, Date.parse((await job(dedupeKey)).next_attempt_at))
}

/** Ajoute à la D1 de test un déclencheur qui refuse toute écriture du statut d'image d'une carte. */
async function refuseImageWrites(cardId: string): Promise<() => Promise<void>> {
  await env.DB.prepare(
    `CREATE TRIGGER test_refus_image BEFORE UPDATE OF image_status ON cards WHEN NEW.id = '${cardId}'
     BEGIN SELECT RAISE(ABORT, 'écriture refusée par le test'); END`,
  ).run()
  return async () => {
    await env.DB.prepare(`DROP TRIGGER IF EXISTS test_refus_image`).run()
  }
}

// ——— Reprise après interruption et verrous ———

describe('reprise après interruption et verrous', () => {
  it('reprend après JOB_LEASE_MS les tâches d’une exécution interrompue, sans rien refaire ni perdre R2', async () => {
    const fake = sampleCatalog()
    const clock = new Clock()
    const deps = makeDeps(fake, clock)
    await prepareImageJobs(deps, clock)
    const doneBefore = await rows<{ dedupe_key: string; completed_at: string; attempts: number }>(
      `SELECT dedupe_key, completed_at, attempts FROM import_jobs WHERE status = 'done' ORDER BY id`,
    )
    expect(doneBefore.map((row) => row.dedupe_key)).toEqual(['catalog:fr:*', 'set:fr:swsh3', 'set:fr:exu'])

    // Une exécution réserve deux tâches d'image, copie la première image dans R2, puis « plante »
    // avant d'avoir mis D1 à jour.
    const claimed = await claimJobs(db(), { limit: 2, token: 'execution-morte', nowMs: clock.now(), leaseMs: deps.config.jobLeaseMs })
    expect(claimed.map((item) => item.card_id)).toEqual(['fr:swsh3-136', 'fr:swsh3-137'])
    const body = webp(300, 1)
    const sha256 = await sha256Hex(body)
    await env.BUCKET.put('cards/fr/swsh3/swsh3-136.webp', body, {
      httpMetadata: { contentType: 'image/webp' },
      customMetadata: { source: 'api', sourceUrl: URL_136, quality: 'high', sha256 },
    })

    // Avant l'expiration de leur verrou : le reste avance, ces deux tâches restent à l'exécution morte.
    expect((await tick(deps)).status).toBe('ran')
    const locked = await rows<{ status: string; lock_token: string }>(
      `SELECT status, lock_token FROM import_jobs WHERE dedupe_key IN ('image:fr:swsh3-136', 'image:fr:swsh3-137') ORDER BY id`,
    )
    expect(locked).toEqual([
      { status: 'running', lock_token: 'execution-morte' },
      { status: 'running', lock_token: 'execution-morte' },
    ])
    expect((await card('fr:swsh3-136')).image_status).toBe('pending')

    // Après JOB_LEASE_MS : l'exécution suivante récupère les deux tâches et les termine.
    clock.advance(deps.config.jobLeaseMs + 1)
    await drain(deps, clock)
    const resumed = await rows<{ dedupe_key: string; status: string; attempts: number }>(
      `SELECT dedupe_key, status, attempts FROM import_jobs WHERE dedupe_key IN ('image:fr:swsh3-136', 'image:fr:swsh3-137') ORDER BY id`,
    )
    expect(resumed).toEqual([
      { dedupe_key: 'image:fr:swsh3-136', status: 'done', attempts: 2 },
      { dedupe_key: 'image:fr:swsh3-137', status: 'done', attempts: 2 },
    ])

    // L'image déjà copiée est réconciliée avec R2 sans nouveau téléchargement ; l'autre est copiée.
    expect(await card('fr:swsh3-136')).toMatchObject({ image_status: 'already_present', image_r2_key: 'cards/fr/swsh3/swsh3-136.webp', image_sha256: sha256, image_file_size: 300 })
    expect((await card('fr:swsh3-137')).image_status).toBe('recovered_from_cdn')
    expect(fake.count((url) => url === URL_136)).toBe(0)
    expect(fake.count((url) => url === URL_137)).toBe(1)
    const object = await env.BUCKET.get('cards/fr/swsh3/swsh3-136.webp')
    expect(object).not.toBeNull()
    expect(new Uint8Array(await (object as R2ObjectBody).arrayBuffer())).toEqual(body)

    // Rien de ce qui était terminé n'a été refait.
    const after = await rows(`SELECT dedupe_key, completed_at, attempts FROM import_jobs WHERE dedupe_key IN ('catalog:fr:*', 'set:fr:swsh3', 'set:fr:exu') ORDER BY id`)
    expect(after).toEqual(doneBefore)
    for (const path of ['series', 'sets', 'sets/swsh3', 'sets/exu']) expect(fake.count((url) => url === `${API}/fr/${path}`)).toBe(1)
    expect(await notDone()).toEqual([])
  })

  it('deux exécutions lancées en même temps : une seule travaille, l’autre s’arrête ; chaque image n’est téléchargée qu’une fois', async () => {
    const fake = sampleCatalog()
    const clock = new Clock()
    const deps = makeDeps(fake, clock)
    await startSync(db(), deps.config, clock.now())

    const pairs: TickResult[][] = []
    for (let i = 0; i < 40; i++) {
      const pair = await Promise.all([runTick(deps, { trigger: 'cron' }), runTick(deps, { trigger: 'admin' })])
      pairs.push(pair)
      if (pair.every((result) => result.status === 'idle')) {
        const next = await one<{ at: string | null }>(`SELECT MIN(next_attempt_at) AS at FROM import_jobs WHERE status = 'pending'`)
        if (!next.at) break
        clock.t = Math.max(clock.t, Date.parse(next.at)) + 1_000
        continue
      }
      clock.advance(5 * MIN)
    }

    const active = pairs.filter((pair) => pair.some((result) => result.status !== 'idle'))
    expect(active.length).toBeGreaterThan(2)
    for (const pair of active) {
      expect(pair.filter((result) => result.status === 'ran')).toHaveLength(1)
      const skipped = pair.filter((result) => result.status === 'skipped')
      expect(skipped).toHaveLength(1)
      expect(skipped[0]).toMatchObject({ reason: 'une autre exécution est en cours', claimed: 0, httpRequests: 0, d1Queries: 3 })
    }

    // Tout est importé ; aucune image ni aucune ressource de l'API n'a été demandée deux fois.
    expect(await rows(`SELECT language FROM sync_runs WHERE status != 'completed'`)).toEqual([])
    expect(await notDone()).toEqual([])
    const urls = fake.requests.map((request) => request.url)
    const images = urls.filter((url) => url.endsWith('.webp'))
    expect(images).toHaveLength(7)
    expect(new Set(images).size).toBe(images.length)
    const api = urls.filter((url) => url.startsWith(`${API}/`))
    expect(new Set(api).size).toBe(api.length)
  })

  it('verrou d’exécuteur expiré (exécution précédente morte) : repris par l’exécution suivante', async () => {
    const fake = sampleCatalog()
    const clock = new Clock()
    const deps = makeDeps(fake, clock)
    await startSync(db(), deps.config, clock.now(), { languages: ['fr'] })
    expect(await acquireRunnerLease(db(), 'execution-morte', clock.now(), deps.config.runnerLeaseMs)).toBe(true)

    // Verrou encore valide : l'exécution s'arrête sans rien faire.
    clock.advance(deps.config.runnerLeaseMs - 1_000)
    expect(await tick(deps)).toMatchObject({ status: 'skipped', reason: 'une autre exécution est en cours', claimed: 0, httpRequests: 0 })
    expect(fake.requests).toHaveLength(0)
    expect((await job('catalog:fr:*')).status).toBe('pending')

    // Verrou expiré : repris.
    clock.advance(1_000)
    const resumed = await tick(deps)
    expect(resumed.status).toBe('ran')
    expect(resumed.done).toBeGreaterThan(0)
    const lock = await one<{ value: string; expires_at: string }>(`SELECT value, expires_at FROM runtime_state WHERE key = 'runner_lock'`)
    expect(lock.value).not.toBe('execution-morte')
    // Libéré en fin d'exécution : la suivante peut le prendre aussitôt.
    expect(Date.parse(lock.expires_at)).toBeLessThanOrEqual(clock.now())
    expect(await acquireRunnerLease(db(), 'suivante', clock.now(), deps.config.runnerLeaseMs)).toBe(true)
  })

  it('tâche dont le verrou expire après sa dernière tentative : échec définitif, image en download_error, jamais retentée', async () => {
    const fake = sampleCatalog()
    const clock = new Clock()
    const deps = makeDeps(fake, clock, { MAX_ATTEMPTS: '1' })
    await prepareImageJobs(deps, clock)
    const claimed = await claimJobs(db(), { limit: 4, token: 'execution-morte', nowMs: clock.now(), leaseMs: deps.config.jobLeaseMs })
    expect(claimed.map((item) => [item.job_type, item.attempts, item.max_attempts])).toEqual(Array.from({ length: 4 }, () => ['image', 1, 1]))

    clock.advance(deps.config.jobLeaseMs + 1)
    expect((await tick(deps)).status).toBe('ran')

    const reason = 'verrou expiré : nombre maximal de tentatives atteint'
    const ids = ['fr:swsh3-136', 'fr:swsh3-137', 'fr:swsh3-138', 'fr:exu-%3F']
    const jobs = await rows<{ card_id: string; status: string; last_error: string; completed: number }>(
      `SELECT card_id, status, last_error, completed_at IS NOT NULL AS completed FROM import_jobs WHERE job_type = 'image' ORDER BY id`,
    )
    expect(jobs).toEqual(ids.map((cardId) => ({ card_id: cardId, status: 'failed', last_error: reason, completed: 1 })))
    for (const id of ids) expect(await card(id)).toMatchObject({ image_status: 'download_error', image_failure_reason: reason })

    // Aucune boucle : les exécutions suivantes ne les reprennent pas.
    await drain(deps, clock)
    expect(fake.count((url) => url.endsWith('.webp'))).toBe(0)
    expect((await job('image:fr:swsh3-136')).status).toBe('failed')
  })

  it('remet en attente (sans échec) une tâche au verrou expiré qui a encore des tentatives', async () => {
    const fake = sampleCatalog()
    const clock = new Clock()
    const deps = makeDeps(fake, clock)
    await prepareImageJobs(deps, clock)
    await claimJobs(db(), { limit: 1, token: 'execution-morte', nowMs: clock.now(), leaseMs: deps.config.jobLeaseMs })
    // Verrou pas encore expiré : rien n'est repris.
    expect(await reclaimExpired(db(), clock.now() + deps.config.jobLeaseMs - 1)).toEqual([])
    expect((await job('image:fr:swsh3-136')).status).toBe('running')
    // Expiré : remise en attente, tentative conservée, aucune tâche rendue comme « en échec ».
    expect(await reclaimExpired(db(), clock.now() + deps.config.jobLeaseMs)).toEqual([])
    expect(await job('image:fr:swsh3-136')).toMatchObject({ status: 'pending', attempts: 1, lock_token: null, locked_until: null, last_error: 'verrou expiré : exécution interrompue' })
  })

  it('un batch D1 refusé pour une tâche (jeton changé entre-temps) n’empêche pas d’enregistrer les autres du même lot', async () => {
    const fake = sampleCatalog()
    const clock = new Clock()
    const base = makeDeps(fake, clock)
    await prepareImageJobs(base, clock)
    // Pendant le téléchargement de l'image 137, une autre exécution s'empare de sa tâche.
    const deps: RunnerDeps = {
      ...base,
      fetcher: async (input, init) => {
        const response = await fake.fetcher(input, init)
        if (urlOf(input) === URL_137) await env.DB.prepare(`UPDATE import_jobs SET lock_token = 'jeton-vole' WHERE dedupe_key = 'image:fr:swsh3-137'`).run()
        return response
      },
    }

    const result = await tick(deps)
    expect(result).toMatchObject({ status: 'ran', claimed: 8, done: 7, retried: 0, failed: 0, released: 0, lost: 1 })
    // La perte est comptée dans le bilan enregistré.
    expect(await one(`SELECT jobs_claimed, jobs_done, jobs_lost, jobs_released FROM tick_runs ORDER BY id DESC LIMIT 1`)).toEqual({
      jobs_claimed: 8,
      jobs_done: 7,
      jobs_lost: 1,
      jobs_released: 0,
    })

    // La tâche perdue : aucune écriture D1 (l'objet R2 est déjà là), elle reste à l'autre jeton.
    expect(await job('image:fr:swsh3-137')).toMatchObject({ status: 'running', lock_token: 'jeton-vole' })
    expect((await card('fr:swsh3-137')).image_status).toBe('pending')
    expect(await env.BUCKET.head('cards/fr/swsh3/swsh3-137.webp')).not.toBeNull()

    // Les autres tâches du même lot sont bien enregistrées.
    const done = (await rows<{ dedupe_key: string }>(`SELECT dedupe_key FROM import_jobs WHERE status = 'done' AND job_type IN ('image', 'card')`)).map((row) => row.dedupe_key)
    expect(done.sort()).toEqual(['card:fr:exu-%3F', 'card:fr:swsh3-136', 'card:fr:swsh3-137', 'card:fr:swsh3-138', 'image:fr:exu-%3F', 'image:fr:swsh3-136', 'image:fr:swsh3-138'])
    expect((await card('fr:swsh3-136')).image_status).toBe('downloaded')
    expect((await card('fr:swsh3-138')).image_status).toBe('not_found_on_cdn')
    expect(await rows(`SELECT id FROM cards WHERE detail_status != 'fetched'`)).toEqual([])

    // Une fois son verrou expiré, la tâche est reprise et réconciliée avec R2, sans nouveau téléchargement.
    clock.advance(base.config.jobLeaseMs + 1)
    await drain(base, clock)
    expect((await card('fr:swsh3-137')).image_status).toBe('already_present')
    expect(fake.count((url) => url === URL_137)).toBe(1)
    expect(await notDone()).toEqual([])
  })

  it('écriture D1 refusée pour une tâche (hors perte de verrou) : retentée plus tard sans ses écritures, les autres enregistrées', async () => {
    const fake = sampleCatalog()
    const clock = new Clock()
    const deps = makeDeps(fake, clock)
    await prepareImageJobs(deps, clock)
    const restore = await refuseImageWrites('fr:swsh3-137')
    let result: TickResult
    try {
      result = await tick(deps)
    } finally {
      await restore()
    }
    expect(result).toMatchObject({ status: 'ran', claimed: 8, done: 7, retried: 1, failed: 0, lost: 0 })
    const retried = await job('image:fr:swsh3-137')
    expect(retried).toMatchObject({ status: 'pending', attempts: 1, lock_token: null })
    expect(retried.last_error).toContain('écriture D1 échouée')
    expect(retried.last_error).toContain('écriture refusée par le test')
    expect(Date.parse(retried.next_attempt_at)).toBeGreaterThan(clock.now())
    expect((await card('fr:swsh3-137')).image_status).toBe('pending')
    expect((await card('fr:swsh3-136')).image_status).toBe('downloaded')
    expect((await job('card:fr:swsh3-137')).status).toBe('done')

    // Nouvelle tentative plus tard : réconciliée avec l'objet R2 déjà copié.
    await drain(deps, clock)
    expect((await card('fr:swsh3-137')).image_status).toBe('already_present')
    expect(fake.count((url) => url === URL_137)).toBe(1)
  })

  it('écriture D1 refusée à la dernière tentative : tâche en échec, comptée en échec dans le bilan', async () => {
    const fake = sampleCatalog()
    const clock = new Clock()
    const deps = makeDeps(fake, clock, { MAX_ATTEMPTS: '1' })
    await prepareImageJobs(deps, clock)
    const restore = await refuseImageWrites('fr:swsh3-137')
    let result: TickResult
    try {
      result = await tick(deps)
    } finally {
      await restore()
    }
    expect((await job('image:fr:swsh3-137')).status).toBe('failed')
    const tickRun = await one<{ jobs_failed: number; jobs_retried: number; jobs_lost: number }>(`SELECT jobs_failed, jobs_retried, jobs_lost FROM tick_runs ORDER BY id DESC LIMIT 1`)
    expect({ failed: result.failed, retried: result.retried, lost: result.lost, tickRun }).toEqual({
      failed: 1,
      retried: 0,
      lost: 0,
      tickRun: { jobs_failed: 1, jobs_retried: 0, jobs_lost: 0 },
    })
  })

  it('verrou de l’exécuteur renouvelé avec chaque lot : une exécution qui dure plus que son verrou initial le garde', async () => {
    const fake = sampleCatalog()
    const clock = new Clock()
    const base = makeDeps(fake, clock)
    await startSync(db(), base.config, clock.now(), { languages: ['fr'] })
    const startMs = clock.now()
    const leaseMs = base.config.runnerLeaseMs
    const seen: { expiresAt: number; intruder: boolean }[] = []
    const deps: RunnerDeps = {
      ...base,
      fetcher: async (input, init) => {
        // Chaque requête « dure » 10 s.
        clock.advance(10_000)
        if (urlOf(input) === `${API}/fr/sets/swsh3`) {
          // Deuxième lot : le premier a renouvelé le verrou. Une autre exécution qui arriverait juste
          // après l'échéance du verrou initial ne peut pas le prendre.
          const lock = await one<{ expires_at: string }>(`SELECT expires_at FROM runtime_state WHERE key = 'runner_lock'`)
          seen.push({ expiresAt: Date.parse(lock.expires_at), intruder: await acquireRunnerLease(db(), 'intrus', startMs + leaseMs + 1, leaseMs) })
        }
        return fake.fetcher(input, init)
      },
    }

    const result = await tick(deps)
    expect(result).toMatchObject({ status: 'ran', reason: 'plus aucune tâche prête', lost: 0 })
    expect(seen).toHaveLength(1)
    expect(seen[0]?.intruder).toBe(false)
    expect(seen[0]?.expiresAt).toBeGreaterThan(startMs + leaseMs)
    expect(seen[0]?.expiresAt).toBeLessThanOrEqual(clock.now() + leaseMs)
    // Toujours libéré en fin d'exécution.
    const lock = await one<{ value: string; expires_at: string }>(`SELECT value, expires_at FROM runtime_state WHERE key = 'runner_lock'`)
    expect(lock.value).not.toBe('intrus')
    expect(Date.parse(lock.expires_at)).toBeLessThanOrEqual(clock.now())
  })

  it('verrou de l’exécuteur perdu en cours d’exécution : le lot en cours est enregistré, puis arrêt sans toucher au verrou de l’autre', async () => {
    const fake = sampleCatalog()
    const clock = new Clock()
    const base = makeDeps(fake, clock)
    await startSync(db(), base.config, clock.now(), { languages: ['fr'] })
    const otherExpiry = iso(clock.now() + 60 * MIN)
    const deps: RunnerDeps = {
      ...base,
      fetcher: async (input, init) => {
        // Pendant le premier lot, une autre exécution s'empare du verrou de l'exécuteur.
        if (urlOf(input) === `${API}/fr/sets`) {
          await env.DB.prepare(`UPDATE runtime_state SET value = 'autre-executeur', expires_at = ?1 WHERE key = 'runner_lock'`).bind(otherExpiry).run()
        }
        return fake.fetcher(input, init)
      },
    }

    const result = await tick(deps)
    const reason = 'verrou de l’exécuteur perdu : arrêt'
    expect(result).toMatchObject({ status: 'ran', reason, claimed: 1, done: 1, httpRequests: 2 })
    // Le lot en cours (catalogue) est enregistré, avec ses opérations du jour ; rien d'autre n'est réservé.
    expect((await job('catalog:fr:*')).status).toBe('done')
    expect(await rows(`SELECT dedupe_key, status, attempts FROM import_jobs WHERE job_type != 'catalog' ORDER BY id`)).toEqual([
      { dedupe_key: 'set:fr:swsh3', status: 'pending', attempts: 0 },
      { dedupe_key: 'set:fr:exu', status: 'pending', attempts: 0 },
      { dedupe_key: 'card_index:fr:*', status: 'pending', attempts: 0 },
    ])
    expect(fake.requests).toHaveLength(2)
    expect((await runtimeState())['ops:2026-11-13']).toBe('2')
    // Le verrou de l'autre exécution n'est ni libéré ni modifié.
    expect(await one(`SELECT value, expires_at FROM runtime_state WHERE key = 'runner_lock'`)).toEqual({ value: 'autre-executeur', expires_at: otherExpiry })
    expect(await one(`SELECT stop_reason, jobs_claimed, jobs_done FROM tick_runs`)).toEqual({ stop_reason: reason, jobs_claimed: 1, jobs_done: 1 })
  })

  it('état relu après la prise du verrou : une attente imposée enregistrée entre-temps arrête l’exécution avant toute réservation', async () => {
    const fake = sampleCatalog()
    const clock = new Clock()
    const base = makeDeps(fake, clock)
    await startSync(db(), base.config, clock.now(), { languages: ['fr'] })
    const until = iso(clock.now() + 30 * MIN)
    const backoffReason = 'HTTP 429 de TCGdex (autre exécution)'
    let injected = 0
    // Le 429 d'une autre exécution est enregistré après la première lecture de l'état, juste avant
    // la prise du verrou.
    const probe = probeD1(async (sql) => {
      if (!injected && sql.includes('INSERT INTO runtime_state') && sql.includes('RETURNING value')) {
        injected++
        await setState('backoff_until', until, clock.now())
        await setState('backoff_reason', backoffReason, clock.now())
      }
    })

    const result = await tick({ ...base, d1: probe.d1 })
    expect(injected).toBe(1)
    expect(result).toMatchObject({ status: 'skipped', reason: `attente imposée jusqu'à ${until} (${backoffReason})`, claimed: 0, httpRequests: 0 })
    expect(probe.claimLimits).toEqual([])
    expect(fake.requests).toHaveLength(0)
    expect(await job('catalog:fr:*')).toMatchObject({ status: 'pending', attempts: 0 })
    // Verrou libéré ; aucune réservation possible pendant l'attente, même hors exécuteur.
    const lock = await one<{ expires_at: string }>(`SELECT expires_at FROM runtime_state WHERE key = 'runner_lock'`)
    expect(Date.parse(lock.expires_at)).toBeLessThanOrEqual(clock.now())
    expect(await claimJobs(db(), { limit: 10, token: 'pendant-l-attente', nowMs: clock.now(), leaseMs: MIN })).toEqual([])
    expect(await rows(`SELECT id FROM tick_runs`)).toEqual([])

    // L'attente terminée, le travail reprend.
    clock.t = Date.parse(until) + 1
    expect(await tick(base)).toMatchObject({ status: 'ran' })
    expect((await job('catalog:fr:*')).status).toBe('done')
  })
})

// ——— Budgets, plafonds et pause ———

describe('budgets, plafonds et pause', () => {
  it('MAX_JOBS_PER_RUN petit : chaque exécution s’arrête au maximum, la suivante continue jusqu’au bout', async () => {
    const fake = sampleCatalog()
    const clock = new Clock()
    const deps = makeDeps(fake, clock, { MAX_JOBS_PER_RUN: '3' })
    await startSync(db(), deps.config, clock.now(), { languages: ['fr'] })

    const first = await tick(deps)
    expect(first).toMatchObject({ status: 'ran', claimed: 3, done: 3, reason: 'maximum de 3 tâches atteint' })
    expect((await rows<{ dedupe_key: string }>(`SELECT dedupe_key FROM import_jobs WHERE status = 'done' ORDER BY id`)).map((row) => row.dedupe_key)).toEqual([
      'catalog:fr:*',
      'set:fr:swsh3',
      'set:fr:exu',
    ])
    expect(await job('card_index:fr:*')).toMatchObject({ status: 'pending', attempts: 0 })

    const ticks = [first, ...(await drain(deps, clock))]
    for (const result of ticks) expect(result.claimed).toBeLessThanOrEqual(3)
    expect(ticks.filter((result) => result.reason === 'maximum de 3 tâches atteint').length).toBeGreaterThan(2)
    const recorded = await rows<{ jobs_claimed: number }>(`SELECT jobs_claimed FROM tick_runs`)
    expect(Math.max(...recorded.map((row) => row.jobs_claimed))).toBe(3)

    expect(await notDone()).toEqual([])
    expect((await one<{ n: number }>(`SELECT COUNT(*) AS n FROM cards WHERE image_status != 'pending' AND detail_status = 'fetched'`)).n).toBe(4)
  })

  it('RUN_TIME_BUDGET_MS dépassé en cours de lot : les tâches non commencées sont rendues sans consommer de tentative', async () => {
    const fake = sampleCatalog()
    const clock = new Clock()
    const base = makeDeps(fake, clock)
    await prepareImageJobs(base, clock)
    // Le téléchargement de la première image prend 10 s : le budget de 5 s est dépassé en plein lot.
    const deps: RunnerDeps = {
      ...makeDeps(fake, clock, { RUN_TIME_BUDGET_MS: '5000', MAX_CONCURRENCY: '1' }),
      fetcher: async (input, init) => {
        const response = await fake.fetcher(input, init)
        if (urlOf(input) === URL_136) clock.advance(10_000)
        return response
      },
    }

    const result = await tick(deps)
    expect(result).toMatchObject({ status: 'ran', claimed: 8, done: 1, released: 7, retried: 0, failed: 0, reason: 'budget de temps épuisé' })
    expect(fake.count((url) => url.endsWith('.webp'))).toBe(1)
    expect((await card('fr:swsh3-136')).image_status).toBe('downloaded')

    const released = await rows<{ status: string; attempts: number; last_error: string; ready: number }>(
      `SELECT status, attempts, last_error, next_attempt_at <= ?1 AS ready FROM import_jobs
       WHERE job_type IN ('image', 'card') AND dedupe_key != 'image:fr:swsh3-136'`,
      iso(clock.now()),
    )
    expect(released).toHaveLength(7)
    for (const row of released) expect(row).toEqual({ status: 'pending', attempts: 0, last_error: 'exécution arrêtée avant cette tâche', ready: 1 })

    // L'exécution suivante les traite normalement.
    await drain(base, clock)
    expect(await notDone()).toEqual([])
    expect(await rows(`SELECT dedupe_key FROM import_jobs WHERE job_type IN ('image', 'card') AND attempts != 1`)).toEqual([])
  })

  it('D1_QUERY_BUDGET bas (borne minimale, 50) : l’exécution s’arrête avant la limite, clôture comprise ; la suivante continue', async () => {
    const fake = sampleCatalog()
    fake.sets.fr.push(bulkSet('swsh4', 60))
    const clock = new Clock()
    const probe = probeD1()
    const r2 = countingBucket()
    const deps: RunnerDeps = { ...makeDeps(fake, clock, { D1_QUERY_BUDGET: '30' }), d1: probe.d1, bucket: r2.bucket }
    // Valeur sous la borne minimale : ramenée à 50.
    expect(deps.config.d1QueryBudget).toBe(50)
    await startSync(db(), deps.config, clock.now(), { languages: ['fr'] })

    const ticks = await drainMeasured(deps, clock, fake, probe, r2)
    const first = ticks[0]?.result as TickResult
    expect(first.status).toBe('ran')
    expect(first.claimed).toBeGreaterThan(0)
    expect(first.reason).toMatch(/^budget de requêtes D1 épuisé \(\d+\)$/)
    // Requêtes réellement envoyées par chaque invocation (bilan et clôture des synchronisations compris).
    for (const { result, d1 } of ticks) {
      expect(result.d1Queries).toBeLessThanOrEqual(d1)
      expect(d1).toBeLessThanOrEqual(50)
    }
    for (const row of await rows<{ d1_queries: number }>(`SELECT d1_queries FROM tick_runs`)) expect(row.d1_queries).toBeLessThanOrEqual(50)
    // Lots réduits à l'approche de la limite.
    expect(Math.min(...probe.claimLimits)).toBeLessThan(deps.config.claimBatchSize)
    expect(await notDone()).toEqual([])
    expect(await rows(`SELECT status FROM sync_runs`)).toEqual([{ status: 'completed' }])
  })

  it('D1_QUERY_BUDGET à sa borne minimale (50) avec CLAIM_BATCH_SIZE maximal (50) : lots réduits au budget restant, l’importation avance', async () => {
    const fake = sampleCatalog()
    const clock = new Clock()
    const probe = probeD1()
    const r2 = countingBucket()
    const deps: RunnerDeps = { ...makeDeps(fake, clock, { D1_QUERY_BUDGET: '20', CLAIM_BATCH_SIZE: '50' }), d1: probe.d1, bucket: r2.bucket }
    expect([deps.config.d1QueryBudget, deps.config.claimBatchSize]).toEqual([50, 50])
    await startSync(db(), deps.config, clock.now(), { languages: ['fr'] })
    const ticks = await drainMeasured(deps, clock, fake, probe, r2, 30)
    expect(await rows(`SELECT status FROM sync_runs`)).toEqual([{ status: 'completed' }])
    expect(probe.claimLimits.length).toBeGreaterThan(0)
    for (const limit of probe.claimLimits) {
      expect(limit).toBeGreaterThan(0)
      expect(limit).toBeLessThan(50)
    }
    for (const { d1 } of ticks) expect(d1).toBeLessThanOrEqual(50)
  })

  it('SUBREQUEST_BUDGET à sa borne minimale (200) avec CLAIM_BATCH_SIZE maximal (50) : lots réduits, arrêt avant la limite, l’importation avance', async () => {
    const fake = sampleCatalog()
    fake.sets.fr.push(bulkSet('swsh4', 60))
    const clock = new Clock()
    const probe = probeD1()
    const r2 = countingBucket()
    const deps: RunnerDeps = { ...makeDeps(fake, clock, { SUBREQUEST_BUDGET: '50', CLAIM_BATCH_SIZE: '50' }), d1: probe.d1, bucket: r2.bucket }
    expect([deps.config.subrequestBudget, deps.config.claimBatchSize]).toEqual([200, 50])
    await startSync(db(), deps.config, clock.now(), { languages: ['fr'] })

    const ticks = await drainMeasured(deps, clock, fake, probe, r2)
    const first = ticks[0]?.result as TickResult
    expect(first.status).toBe('ran')
    expect(first.claimed).toBeGreaterThan(0)
    expect(first.reason).toMatch(/^budget de sous-requêtes épuisé \(\d+\)$/)
    // Sous-requêtes réellement envoyées par chaque invocation : D1, TCGdex et R2, clôture comprise.
    for (const { subrequests } of ticks) expect(subrequests).toBeLessThanOrEqual(200)
    for (const limit of probe.claimLimits) {
      expect(limit).toBeGreaterThan(0)
      expect(limit).toBeLessThan(50)
    }
    expect(await notDone()).toEqual([])
    expect(await rows(`SELECT status FROM sync_runs`)).toEqual([{ status: 'completed' }])
  })

  it('opérations du jour enregistrées avec chaque lot (pas seulement en fin d’exécution), chacune une seule fois', async () => {
    const fake = sampleCatalog()
    const clock = new Clock()
    const base = makeDeps(fake, clock)
    await startSync(db(), base.config, clock.now(), { languages: ['fr'] })
    const opsToday = async () => Number((await runtimeState())['ops:2026-11-13'] ?? 0)
    const seen: Record<string, number> = {}
    const deps: RunnerDeps = {
      ...base,
      fetcher: async (input, init) => {
        const url = urlOf(input)
        if (url === `${API}/fr/sets/swsh3` || url === URL_136) seen[url] = await opsToday()
        return fake.fetcher(input, init)
      },
    }

    const result = await tick(deps)
    expect(result).toMatchObject({ status: 'ran', claimed: 12, httpRequests: 12 })
    // Pendant le 2e lot : les 2 requêtes du catalogue ; pendant le 3e : celles des 2 extensions en plus.
    expect(seen).toEqual({ [`${API}/fr/sets/swsh3`]: 2, [URL_136]: 4 })
    // Fin d'exécution : requêtes TCGdex + écritures R2 (images 136 et 137), sans double comptage.
    const written = (await env.BUCKET.list({ prefix: 'cards/' })).objects.length
    expect(written).toBe(2)
    expect(await opsToday()).toBe(result.httpRequests + written)
  })

  it('MAX_RELEASES : une tâche rendue trop souvent consomme ensuite ses tentatives, puis échoue (jamais de boucle)', async () => {
    const fake = sampleCatalog()
    const clock = new Clock()
    const deps = makeDeps(fake, clock, { MAX_RELEASES: '2', MAX_ATTEMPTS: '3' })
    await startSync(db(), deps.config, clock.now(), { languages: ['fr'] })
    expect(await tick(deps, { maxJobs: 1 })).toMatchObject({ claimed: 1, done: 1 })
    // Une exécution morte garde l'extension swsh3 pour longtemps : l'index des cartes l'attend.
    const [held] = await claimJobs(db(), { limit: 1, token: 'execution-morte', nowMs: clock.now(), leaseMs: 30 * DAY })
    expect(held).toMatchObject({ job_type: 'set', target_id: 'swsh3' })

    // Deux rendus, sans consommer de tentative.
    expect(await tick(deps)).toMatchObject({ status: 'ran', released: 1, retried: 0 })
    expect(await job('card_index:fr:*')).toMatchObject({ status: 'pending', attempts: 0, releases: 1 })
    await untilReady(clock, 'card_index:fr:*')
    expect(await tick(deps)).toMatchObject({ status: 'ran', claimed: 1, released: 1, retried: 0 })
    expect(await job('card_index:fr:*')).toMatchObject({ status: 'pending', attempts: 0, releases: 2, last_error: 'en attente de 1 extension(s)' })

    // Au-delà du plafond : le rendu devient une nouvelle tentative, qui consomme la tentative.
    await untilReady(clock, 'card_index:fr:*')
    expect(await tick(deps)).toMatchObject({ status: 'ran', claimed: 1, released: 0, retried: 1 })
    expect(await job('card_index:fr:*')).toMatchObject({ status: 'pending', attempts: 1, releases: 2, last_error: 'en attente de 1 extension(s) (rendue 2 fois)' })

    // Tentatives épuisées : échec définitif ; l'index des cartes n'a jamais été demandé.
    await drain(deps, clock)
    expect(await job('card_index:fr:*')).toMatchObject({
      status: 'failed',
      attempts: 3,
      releases: 2,
      last_error: 'en attente de 1 extension(s) (rendue 2 fois) (après 3 tentative(s))',
    })
    expect(fake.count((url) => url === `${API}/fr/cards`)).toBe(0)
    expect(await one(`SELECT SUM(jobs_released) AS released, SUM(jobs_retried) AS retried, SUM(jobs_failed) AS failed FROM tick_runs`)).toEqual({ released: 2, retried: 2, failed: 1 })

    // Synchronisation abandonnée puis relancée : la tâche repart avec ses compteurs à zéro.
    expect(await cancelSync(db(), clock.now(), ['fr'])).toEqual({ cancelled: 1 })
    await startSync(db(), deps.config, clock.now(), { languages: ['fr'] })
    expect(await tick(deps, { maxJobs: 1 })).toMatchObject({ claimed: 1, done: 1 })
    expect(await job('card_index:fr:*')).toMatchObject({ status: 'pending', attempts: 0, releases: 0, last_error: null })
  })

  it('pause : exécution sautée sans requête ni réservation ; reprise : le travail continue', async () => {
    const fake = sampleCatalog()
    const clock = new Clock()
    const deps = makeDeps(fake, clock)
    const database = db()
    await startSync(database, deps.config, clock.now(), { languages: ['fr'] })
    await pauseSync(database, clock.now())

    expect(await tick(deps)).toMatchObject({ status: 'skipped', reason: 'en pause : pause manuelle', claimed: 0, httpRequests: 0 })
    expect(fake.requests).toHaveLength(0)
    // Ni le verrou de l'exécuteur ni aucune tâche ne sont pris pendant la pause.
    expect(await rows(`SELECT key FROM runtime_state WHERE key = 'runner_lock'`)).toEqual([])
    expect(await claimJobs(database, { limit: 10, token: 'pendant-la-pause', nowMs: clock.now(), leaseMs: MIN })).toEqual([])
    expect(await job('catalog:fr:*')).toMatchObject({ status: 'pending', attempts: 0 })

    await resumeSync(database, clock.now())
    expect(await runtimeState()).toMatchObject({ paused: null, pause_reason: null })
    clock.advance(5 * MIN)
    expect((await tick(deps)).status).toBe('ran')
    expect(fake.requests.length).toBeGreaterThan(0)
    await drain(deps, clock)
    expect(await rows(`SELECT status FROM sync_runs`)).toEqual([{ status: 'completed' }])
  })

  it('plafond de stockage (MAX_STORED_BYTES minuscule) : pause automatique avec son motif, plus aucune requête', async () => {
    const fake = sampleCatalog()
    fake.addImage(URL_136, webp(1_100_000, 1))
    const clock = new Clock()
    const deps = makeDeps(fake, clock, { MAX_STORED_BYTES: '1000000' })
    await startSync(db(), deps.config, clock.now(), { languages: ['fr'] })

    const ticks = await drain(deps, clock)
    const reason = 'plafond de stockage atteint (1100310 octets) : pause automatique'
    expect(ticks.find((result) => result.reason.startsWith('plafond de stockage'))).toMatchObject({ status: 'skipped', reason, claimed: 0 })
    expect(ticks.at(-1)).toMatchObject({ status: 'skipped', reason: `en pause : ${reason}` })
    expect(await runtimeState()).toMatchObject({ paused: '1', pause_reason: reason })

    // Le travail restant est conservé (index des cartes en attente), mais plus rien ne part.
    expect(await notDone()).toEqual([{ dedupe_key: 'card_index:fr:*', status: 'pending' }])
    const requests = fake.requests.length
    clock.advance(DAY)
    expect(await tick(deps)).toMatchObject({ status: 'skipped', reason: `en pause : ${reason}` })
    expect(fake.requests.length).toBe(requests)

    // Une reprise manuelle sans libérer de place : nouvelle pause automatique dès l'exécution suivante.
    await resumeSync(db(), clock.now())
    expect(await tick(deps)).toMatchObject({ status: 'skipped', reason })
    expect(fake.requests.length).toBe(requests)
    expect((await runtimeState()).paused).toBe('1')
  })

  it('plafond quotidien (MAX_DAILY_OPERATIONS petit) : lots réduits pour ne jamais le dépasser, sortie sans écriture une fois atteint, reprise le lendemain UTC', async () => {
    const fake = sampleCatalog()
    const clock = new Clock('2026-11-13T23:00:00.000Z')
    const probe = probeD1()
    const deps: RunnerDeps = { ...makeDeps(fake, clock, { MAX_DAILY_OPERATIONS: '100' }), d1: probe.d1 }
    const ops = async (day: string) => Number((await rows<{ value: string }>(`SELECT value FROM runtime_state WHERE key = ?1`, `ops:${day}`))[0]?.value ?? 0)
    const stored = async () => (await env.BUCKET.list({ prefix: 'cards/' })).objects.length
    await startSync(db(), deps.config, clock.now(), { languages: ['fr'] })
    // 85 opérations déjà comptées aujourd'hui (UTC) : il en reste 15, soit 5 tâches au pire (3 chacune).
    await setState('ops:2026-11-13', '85', clock.now())

    // Les lots sont réduits à ce qui reste du plafond ; l'exécution s'arrête quand il ne reste plus de
    // quoi traiter une seule tâche au pire, sans jamais le dépasser.
    const first = await tick(deps)
    expect(first).toMatchObject({ status: 'ran', reason: 'plafond quotidien d’opérations atteint' })
    expect(first.claimed).toBeGreaterThan(4)
    expect(Math.max(...probe.claimLimits)).toBe(5)
    expect(probe.claimLimits.at(-1)).toBe(1)
    const afterFirst = await ops('2026-11-13')
    expect(afterFirst).toBe(85 + first.httpRequests + (await stored()))
    expect(afterFirst).toBeLessThanOrEqual(100)
    expect(afterFirst).toBeGreaterThan(100 - 3)

    // Il reste moins d'une tâche au pire : l'exécution suivante ne réserve rien.
    clock.advance(5 * MIN)
    expect(await tick(deps)).toMatchObject({ status: 'ran', reason: 'plafond quotidien d’opérations atteint', claimed: 0, httpRequests: 0 })
    const requests = fake.requests.length

    // Plafond atteint : sortie rapide, avant le verrou, sans aucune écriture.
    await setState('ops:2026-11-13', '100', clock.now())
    const stateBefore = await rows(`SELECT * FROM runtime_state ORDER BY key`)
    const ticksBefore = await rows(`SELECT * FROM tick_runs ORDER BY id`)
    clock.advance(5 * MIN)
    expect(await tick(deps)).toMatchObject({ status: 'skipped', reason: 'plafond quotidien atteint (100 opérations)', claimed: 0, httpRequests: 0 })
    expect(await rows(`SELECT * FROM runtime_state ORDER BY key`)).toEqual(stateBefore)
    expect(await rows(`SELECT * FROM tick_runs ORDER BY id`)).toEqual(ticksBefore)
    expect(fake.requests).toHaveLength(requests)

    // Le lendemain (UTC), le compteur repart de zéro : l'importation reprend et se termine.
    clock.t = Date.parse('2026-11-14T00:00:30.000Z')
    const storedBefore = await stored()
    await drain(deps, clock)
    expect(await rows(`SELECT status FROM sync_runs`)).toEqual([{ status: 'completed' }])
    expect(await ops('2026-11-13')).toBe(100)
    const nextDay = await ops('2026-11-14')
    expect(nextDay).toBeGreaterThanOrEqual(fake.requests.length - requests + (await stored()) - storedBefore)
    expect(nextDay).toBeLessThanOrEqual(100)
  })

  it('série d’erreurs serveur (ERROR_STREAK_LIMIT bas) : attente imposée enregistrée, exécutions sautées jusqu’à son terme', async () => {
    const fake = sampleCatalog()
    const clock = new Clock()
    await prepareImageJobs(makeDeps(fake, clock), clock)
    for (const url of [URL_136, URL_137, URL_138, URL_EXU]) fake.images.set(url, { status: 503 })
    const deps = makeDeps(fake, clock, { ERROR_STREAK_LIMIT: '3', MAX_CONCURRENCY: '1' })

    const result = await tick(deps)
    const reason = '3 erreurs serveur ou réseau consécutives : pause de 15 min'
    expect(result).toMatchObject({ status: 'ran', reason, done: 0, retried: 3, released: 5, failed: 0 })
    const state = await runtimeState()
    expect(state.backoff_reason).toBe(reason)
    expect(Date.parse(state.backoff_until as string)).toBe(clock.now() + 900_000)
    // Les tâches en erreur consomment une tentative ; celles qui n'ont pas commencé, non.
    expect(await job('image:fr:swsh3-138')).toMatchObject({ status: 'pending', attempts: 1, last_http_status: 503 })
    expect(await job('image:fr:exu-%3F')).toMatchObject({ status: 'pending', attempts: 0 })
    expect(await job('card:fr:swsh3-136')).toMatchObject({ status: 'pending', attempts: 0 })

    // Pendant l'attente imposée : exécution sautée, sans aucune requête.
    const requests = fake.requests.length
    clock.advance(5 * MIN)
    const skipped = await tick(deps)
    expect(skipped.status).toBe('skipped')
    expect(skipped.reason).toBe(`attente imposée jusqu'à ${state.backoff_until} (${reason})`)
    expect(fake.requests.length).toBe(requests)

    // Fin de l'attente, CDN rétabli : tout se termine.
    fake.addImage(URL_136, webp(300, 1))
    fake.addImage(URL_137, webp(310, 2))
    fake.images.delete(URL_138)
    fake.images.delete(URL_EXU)
    clock.advance(15 * MIN)
    await drain(deps, clock)
    expect(await notDone()).toEqual([])
    expect((await card('fr:swsh3-136')).image_status).toBe('downloaded')
    expect((await card('fr:swsh3-137')).image_status).toBe('recovered_from_cdn')
  })
})

// ——— Exécuteur : robustesse ———

describe('exécuteur : robustesse', () => {
  it('mapPool respecte la concurrence maximale et l’ordre des résultats', async () => {
    let active = 0
    let peak = 0
    const tracked = async <T>(value: T, delayMs: number): Promise<T> => {
      active++
      peak = Math.max(peak, active)
      await new Promise((resolve) => setTimeout(resolve, delayMs))
      active--
      return value
    }

    const items = Array.from({ length: 12 }, (_, i) => i)
    // Durées volontairement désordonnées : les résultats restent dans l'ordre des éléments.
    expect(await mapPool(items, 3, (item) => tracked(item * 10, (item * 7) % 5))).toEqual(items.map((item) => item * 10))
    expect(peak).toBe(3)

    peak = 0
    expect(await mapPool([1, 2], 8, (item) => tracked(item, 1))).toEqual([1, 2])
    expect(peak).toBe(2)

    peak = 0
    expect(await mapPool([3, 2, 1], 0, (item) => tracked(item, item))).toEqual([3, 2, 1])
    expect(peak).toBe(1)

    expect(await mapPool([], 3, (item: number) => tracked(item, 0))).toEqual([])
  })

  it('une exception dans une tâche : nouvelle tentative différée, l’exécution continue', async () => {
    const fake = sampleCatalog()
    const clock = new Clock()
    const base = makeDeps(fake, clock)
    await prepareImageJobs(base, clock)

    const result = await tick({ ...base, bucket: flakyBucket('cards/fr/swsh3/swsh3-136.webp', 1, 'corrupt') })
    expect(result).toMatchObject({ status: 'ran', claimed: 8, done: 7, retried: 1, failed: 0 })
    const failed = await job('image:fr:swsh3-136')
    expect(failed).toMatchObject({ status: 'pending', attempts: 1, last_error: 'exception : Error: R2 incohérent (simulé)', lock_token: null })
    expect(Date.parse(failed.next_attempt_at)).toBeGreaterThan(clock.now())
    expect((await card('fr:swsh3-136')).image_status).toBe('pending')
    // Les autres tâches du même lot sont allées au bout.
    expect((await card('fr:swsh3-137')).image_status).toBe('recovered_from_cdn')
    expect(fake.count((url) => url === URL_136)).toBe(0)

    await drain(base, clock)
    expect(await card('fr:swsh3-136')).toMatchObject({ image_status: 'downloaded', image_source: 'api' })
    expect((await job('image:fr:swsh3-136')).attempts).toBe(2)
  })

  it('lecture R2 (head) en échec : nouvelle tentative différée avec son motif, l’exécution continue', async () => {
    const fake = sampleCatalog()
    const clock = new Clock()
    const base = makeDeps(fake, clock)
    await prepareImageJobs(base, clock)

    const result = await tick({ ...base, bucket: flakyBucket('cards/fr/swsh3/swsh3-136.webp', 1) })
    expect(result).toMatchObject({ status: 'ran', claimed: 8, done: 7, retried: 1, failed: 0 })
    const failed = await job('image:fr:swsh3-136')
    expect(failed).toMatchObject({ status: 'pending', attempts: 1, last_error: 'image fr:swsh3-136 : lecture R2 échouée : Error: R2 indisponible (simulé)', lock_token: null })
    expect(Date.parse(failed.next_attempt_at)).toBeGreaterThan(clock.now())
    expect((await card('fr:swsh3-136')).image_status).toBe('pending')
    expect((await card('fr:swsh3-137')).image_status).toBe('recovered_from_cdn')
    expect(fake.count((url) => url === URL_136)).toBe(0)

    await drain(base, clock)
    expect(await card('fr:swsh3-136')).toMatchObject({ image_status: 'downloaded', image_source: 'api' })
    expect((await job('image:fr:swsh3-136')).attempts).toBe(2)
  })

  it('exception à la dernière tentative d’une tâche d’image : carte en download_error, image relancée par retryMissing', async () => {
    const fake = sampleCatalog()
    const clock = new Clock()
    const base = makeDeps(fake, clock, { MAX_ATTEMPTS: '1' })
    await prepareImageJobs(base, clock)
    const result = await tick({ ...base, bucket: flakyBucket('cards/fr/swsh3/swsh3-136.webp', Number.POSITIVE_INFINITY, 'corrupt') })
    expect(result).toMatchObject({ failed: 1, retried: 0 })
    const message = 'exception : Error: R2 incohérent (simulé) (après 1 tentative(s))'
    expect(await job('image:fr:swsh3-136')).toMatchObject({ status: 'failed', last_error: message })
    // L'échec est visible sur la carte (et donc dans les rapports), comme après une erreur HTTP.
    expect(await card('fr:swsh3-136')).toMatchObject({ image_status: 'download_error', image_failure_reason: message })

    // R2 rétabli : la relance des images manquantes ou en erreur la reprend (la tâche d'image en
    // échec est comptée parmi les tâches remises en attente).
    await drain(base, clock)
    expect(await retryMissing(db(), base.config, clock.now(), ['fr'])).toEqual({ images: 3, failedJobs: 1 })
    expect(await job('image:fr:swsh3-136')).toMatchObject({ status: 'pending', attempts: 0, releases: 0, run_id: null })
    await drain(base, clock)
    expect((await card('fr:swsh3-136')).image_status).toBe('downloaded')
  })
})

// ——— Commandes de synchronisation ———

describe('commandes de synchronisation', () => {
  it('startSync refuse une seconde synchronisation d’une langue en cours ; « initial » puis « manual »', async () => {
    const fake = sampleCatalog()
    const clock = new Clock()
    const deps = makeDeps(fake, clock)
    const database = db()

    const first = await startSync(database, deps.config, clock.now(), { languages: ['fr'] })
    expect(first).toEqual({ started: [{ language: 'fr', runId: expect.any(Number), kind: 'initial' }], skipped: [] })
    expect(await startSync(database, deps.config, clock.now(), { languages: ['fr'] })).toEqual({
      started: [],
      skipped: [{ language: 'fr', reason: 'synchronisation déjà en cours' }],
    })
    const both = await startSync(database, deps.config, clock.now())
    expect(both.started.map((run) => [run.language, run.kind])).toEqual([['ja', 'initial']])
    expect(both.skipped).toEqual([{ language: 'fr', reason: 'synchronisation déjà en cours' }])
    expect(await rows(`SELECT language, kind, status FROM sync_runs ORDER BY id`)).toEqual([
      { language: 'fr', kind: 'initial', status: 'running' },
      { language: 'ja', kind: 'initial', status: 'running' },
    ])
    expect((await one<{ n: number }>(`SELECT COUNT(*) AS n FROM import_jobs WHERE job_type = 'catalog'`)).n).toBe(2)

    await drain(deps, clock)
    const later = await startSync(database, deps.config, clock.now(), { languages: ['fr'] })
    expect(later).toEqual({ started: [{ language: 'fr', runId: expect.any(Number), kind: 'manual' }], skipped: [] })
    const laterRunId = (later.started[0] as { runId: number }).runId
    // La tâche de catalogue (terminée) est remise à zéro et rattachée à la nouvelle synchronisation.
    expect(await job('catalog:fr:*')).toMatchObject({ status: 'pending', attempts: 0, run_id: laterRunId, completed_at: null })
    expect(await rows(`SELECT language, kind, status FROM sync_runs ORDER BY id`)).toEqual([
      { language: 'fr', kind: 'initial', status: 'completed' },
      { language: 'ja', kind: 'initial', status: 'completed' },
      { language: 'fr', kind: 'manual', status: 'running' },
    ])
  })

  it('périmètre ?sets (scope { sets: [swsh3] }) : seules ces extensions, pas d’index complet des cartes', async () => {
    const fake = sampleCatalog()
    const clock = new Clock()
    const deps = makeDeps(fake, clock)
    await startSync(db(), deps.config, clock.now(), { languages: ['fr'], scope: { sets: ['swsh3'] } })
    expect((await job('catalog:fr:*')).payload_json).toBe('{"sets":["swsh3"]}')
    await drain(deps, clock)

    expect(await rows(`SELECT id FROM sets ORDER BY id`)).toEqual([{ id: 'fr:swsh3' }])
    expect(await rows(`SELECT id FROM cards ORDER BY id`)).toEqual([{ id: 'fr:swsh3-136' }, { id: 'fr:swsh3-137' }, { id: 'fr:swsh3-138' }])
    expect(await rows(`SELECT dedupe_key FROM import_jobs WHERE job_type IN ('set', 'card_index')`)).toEqual([{ dedupe_key: 'set:fr:swsh3' }])
    expect(fake.count((url) => url === `${API}/fr/sets/exu`)).toBe(0)
    expect(fake.count((url) => url === `${API}/fr/cards`)).toBe(0)
    expect(fake.count((url) => url.startsWith(`${API}/ja/`))).toBe(0)
    expect(await one(`SELECT kind, status, scope_json, cards_found FROM sync_runs`)).toEqual({ kind: 'initial', status: 'completed', scope_json: '{"sets":["swsh3"]}', cards_found: 3 })

    // Une synchronisation limitée ne compte pas comme synchronisation complète : la suivante, complète,
    // est encore « initial ».
    const full = await startSync(db(), deps.config, clock.now(), { languages: ['fr'] })
    expect(full).toEqual({ started: [{ language: 'fr', runId: expect.any(Number), kind: 'initial' }], skipped: [] })
    expect((await job('catalog:fr:*')).payload_json).toBe('{}')
  })

  it('startSync : synchronisation et tâche catalog créées ensemble ou pas du tout ; deux démarrages simultanés n’en créent qu’une', async () => {
    const fake = sampleCatalog()
    const clock = new Clock()
    const deps = makeDeps(fake, clock)

    // La tâche catalog ne peut pas être écrite : aucune synchronisation sans tâche n'est créée.
    await env.DB.prepare(
      `CREATE TRIGGER test_refus_catalog BEFORE INSERT ON import_jobs WHEN NEW.job_type = 'catalog'
       BEGIN SELECT RAISE(ABORT, 'tâche refusée par le test'); END`,
    ).run()
    try {
      await expect(startSync(db(), deps.config, clock.now(), { languages: ['fr'] })).rejects.toThrow(/tâche refusée par le test/)
    } finally {
      await env.DB.prepare(`DROP TRIGGER IF EXISTS test_refus_catalog`).run()
    }
    expect(await rows(`SELECT id FROM sync_runs`)).toEqual([])
    expect(await rows(`SELECT id FROM import_jobs`)).toEqual([])

    // Deux démarrages simultanés (Cron et administration) : une seule synchronisation, une seule tâche.
    const [a, b] = await Promise.all([startSync(db(), deps.config, clock.now(), { languages: ['fr'] }), startSync(db(), deps.config, clock.now(), { languages: ['fr'] })])
    const started = [...a.started, ...b.started]
    expect(started).toEqual([{ language: 'fr', runId: expect.any(Number), kind: 'initial' }])
    expect([...a.skipped, ...b.skipped]).toEqual([{ language: 'fr', reason: 'synchronisation déjà en cours' }])
    const runId = (started[0] as { runId: number }).runId
    expect(await rows(`SELECT id, status FROM sync_runs`)).toEqual([{ id: runId, status: 'running' }])
    expect(await rows(`SELECT job_type, run_id FROM import_jobs`)).toEqual([{ job_type: 'catalog', run_id: runId }])
    // La base elle-même refuse une seconde synchronisation en cours pour la même langue.
    await expect(
      env.DB.prepare(`INSERT INTO sync_runs (language, kind, started_at, status) VALUES ('fr', 'manual', ?1, 'running')`).bind(iso(clock.now())).run(),
    ).rejects.toThrow(/UNIQUE/)
    // L'unicité est par langue : une synchronisation japonaise reste possible.
    await env.DB.prepare(`INSERT INTO sync_runs (language, kind, started_at, status) VALUES ('ja', 'manual', ?1, 'running')`).bind(iso(clock.now())).run()
    expect((await one<{ n: number }>(`SELECT COUNT(*) AS n FROM sync_runs WHERE status = 'running'`)).n).toBe(2)
  })

  it('synchronisation « en cours » depuis plus de 6 jours : abandonnée au démarrage suivant et remplacée', async () => {
    const fake = sampleCatalog()
    const clock = new Clock()
    const deps = makeDeps(fake, clock)
    const first = await startSync(db(), deps.config, clock.now(), { languages: ['fr'] })
    const oldId = (first.started[0] as { runId: number }).runId

    // Jamais traitée (Cron arrêté). Six jours pile : encore considérée en cours.
    clock.advance(6 * DAY)
    expect(await startSync(db(), deps.config, clock.now(), { languages: ['fr'] })).toEqual({ started: [], skipped: [{ language: 'fr', reason: 'synchronisation déjà en cours' }] })

    // Au-delà : abandonnée (cancelled) et remplacée, dans le même batch que la nouvelle.
    clock.advance(1)
    const second = await startSync(db(), deps.config, clock.now(), { languages: ['fr'] })
    expect(second).toEqual({ started: [{ language: 'fr', runId: expect.any(Number), kind: 'initial' }], skipped: [] })
    const newId = (second.started[0] as { runId: number }).runId
    expect(newId).not.toBe(oldId)
    expect(await rows(`SELECT id, status, finished_at FROM sync_runs ORDER BY id`)).toEqual([
      { id: oldId, status: 'cancelled', finished_at: iso(clock.now()) },
      { id: newId, status: 'running', finished_at: null },
    ])
    // La tâche catalog encore en attente est rattachée à la nouvelle synchronisation.
    expect(await job('catalog:fr:*')).toMatchObject({ status: 'pending', run_id: newId })

    await drain(deps, clock)
    expect(await rows(`SELECT id, status FROM sync_runs ORDER BY id`)).toEqual([
      { id: oldId, status: 'cancelled' },
      { id: newId, status: 'completed' },
    ])
    expect(await notDone()).toEqual([])
  })

  it('cancelSync : abandonne la synchronisation en cours d’une langue ; ses tâches restent traitées, sans clôture de son bilan', async () => {
    const fake = sampleCatalog()
    const clock = new Clock()
    const deps = makeDeps(fake, clock)
    const started = await startSync(db(), deps.config, clock.now())
    const runId = Object.fromEntries(started.started.map((run) => [run.language, run.runId])) as Record<Language, number>

    expect(await cancelSync(db(), clock.now(), ['fr'])).toEqual({ cancelled: 1 })
    expect(await cancelSync(db(), clock.now(), ['fr'])).toEqual({ cancelled: 0 })
    expect(await rows(`SELECT id, status, finished_at FROM sync_runs ORDER BY id`)).toEqual([
      { id: runId.fr, status: 'cancelled', finished_at: iso(clock.now()) },
      { id: runId.ja, status: 'running', finished_at: null },
    ])

    // Les tâches FR sont traitées normalement ; seule la synchronisation JA est close.
    await drain(deps, clock)
    expect(await notDone()).toEqual([])
    expect((await one<{ n: number }>(`SELECT COUNT(*) AS n FROM cards WHERE language = 'fr' AND image_status != 'pending'`)).n).toBe(4)
    expect(await rows(`SELECT id, status, cards_found FROM sync_runs ORDER BY id`)).toEqual([
      { id: runId.fr, status: 'cancelled', cards_found: null },
      { id: runId.ja, status: 'completed', cards_found: 3 },
    ])
    expect(JSON.parse((await job('report:*:*')).payload_json as string)).toEqual({ syncRuns: [runId.ja] })

    // Une nouvelle synchronisation FR peut démarrer aussitôt (aucune complète : « initial »).
    expect(await startSync(db(), deps.config, clock.now(), { languages: ['fr'] })).toEqual({ started: [{ language: 'fr', runId: expect.any(Number), kind: 'initial' }], skipped: [] })
  })

  it('la route POST /admin/sync/start transmet ?sets comme périmètre', async () => {
    const fake = sampleCatalog()
    const clock = new Clock()
    const deps = makeDeps(fake, clock)
    const response = await handleAdmin(
      new Request('https://importateur.example/admin/sync/start?language=fr&sets=swsh3,%20SV1a', { method: 'POST', headers: { authorization: `Bearer ${env.ADMIN_TOKEN}` } }),
      { config: deps.config, d1: env.DB, bucket: env.BUCKET, fetcher: fake.fetcher, adminToken: env.ADMIN_TOKEN, now: clock.now },
    )
    expect(response.status).toBe(200)
    expect(await response.json()).toEqual({ started: [{ language: 'fr', runId: expect.any(Number), kind: 'initial' }], skipped: [] })
    expect(await one(`SELECT language, scope_json FROM sync_runs`)).toEqual({ language: 'fr', scope_json: '{"sets":["swsh3","SV1a"]}' })
    expect((await job('catalog:fr:*')).payload_json).toBe('{"sets":["swsh3","SV1a"]}')
    expect(fake.requests).toHaveLength(0)
  })

  it('retryMissing relance les images manquantes ou en erreur et remet en attente les tâches en échec, jamais les images valides', async () => {
    const fake = sampleCatalog()
    fake.images.set(URL_137, { status: 500 })
    fake.queue(`${API}/fr/cards/swsh3-138`, () => new Response('erreur', { status: 500 }))
    const clock = new Clock()
    const deps = makeDeps(fake, clock, { MAX_ATTEMPTS: '1' })
    await importAll(deps, clock)
    expect(await job('image:fr:swsh3-137')).toMatchObject({ status: 'failed', last_http_status: 500 })
    expect(await job('card:fr:swsh3-138')).toMatchObject({ status: 'failed', last_http_status: 500 })
    expect((await card('fr:swsh3-137')).image_status).toBe('download_error')
    expect((await card('fr:swsh3-138')).image_status).toBe('not_found_on_cdn')
    const valid = await card('fr:swsh3-136')
    const validJob = await job('image:fr:swsh3-136')
    expect(valid.image_status).toBe('downloaded')

    // Le CDN s'est rétabli et a reçu l'image 138 ; l'image de exu-%3F reste introuvable.
    fake.addImage(URL_137, webp(310, 2))
    fake.addImage(URL_138, webp(350, 6))
    clock.advance(DAY)
    // Tâches en échec remises en attente : l'image 137 et le détail de la carte 138.
    expect(await retryMissing(db(), deps.config, clock.now(), ['fr'])).toEqual({ images: 3, failedJobs: 2 })
    // Remises en attente à zéro et détachées de la synchronisation terminée.
    for (const key of ['image:fr:swsh3-137', 'image:fr:swsh3-138', 'image:fr:exu-%3F', 'card:fr:swsh3-138']) {
      expect(await job(key)).toMatchObject({ status: 'pending', attempts: 0, releases: 0, run_id: null, last_error: null, completed_at: null })
    }
    expect(await job('image:fr:swsh3-136')).toEqual(validJob)

    await drain(deps, clock)
    expect((await card('fr:swsh3-137')).image_status).toBe('recovered_from_cdn')
    expect((await card('fr:swsh3-138')).image_status).toBe('recovered_from_cdn')
    expect((await card('fr:swsh3-138')).detail_status).toBe('fetched')
    expect((await card('fr:exu-%3F')).image_status).toBe('not_found_on_cdn')
    // L'image valide n'a jamais été retéléchargée ni touchée.
    expect(fake.count((url) => url === URL_136)).toBe(1)
    expect(await card('fr:swsh3-136')).toEqual(valid)
    expect(await notDone()).toEqual([])
  })

  it('retryMissing remet aussi en attente une tâche d’image en échec dont la carte n’est pas en erreur (données héritées)', async () => {
    const fake = sampleCatalog()
    const clock = new Clock()
    const deps = makeDeps(fake, clock)
    await importAll(deps, clock)
    // État laissé par l'ancien défaut : tâche d'image en échec définitif, carte restée « pending »,
    // et tâche rendue de nombreuses fois.
    await env.DB.batch([
      env.DB.prepare(`UPDATE cards SET image_status = 'pending' WHERE id = 'fr:swsh3-138'`),
      env.DB.prepare(
        `UPDATE import_jobs SET status = 'failed', attempts = 5, releases = 7, last_error = 'ancien échec', completed_at = ?1
         WHERE dedupe_key = 'image:fr:swsh3-138'`,
      ).bind(iso(clock.now())),
    ])
    fake.addImage(URL_138, webp(350, 6))
    clock.advance(DAY)

    // Seule la carte exu-%3F est en statut manquant ; la tâche de l'image 138 est reprise comme tâche en échec.
    expect(await retryMissing(db(), deps.config, clock.now(), ['fr'])).toEqual({ images: 1, failedJobs: 1 })
    expect(await job('image:fr:swsh3-138')).toMatchObject({ status: 'pending', attempts: 0, releases: 0, run_id: null, last_error: null, completed_at: null })
    await drain(deps, clock)
    expect(await card('fr:swsh3-138')).toMatchObject({ image_status: 'recovered_from_cdn', image_source: 'cdn_reconstructed' })
    expect(await notDone()).toEqual([])
  })

  it('finalizeSyncRuns : bilan réel à la clôture de chaque synchronisation, rapport quand plus aucune n’est en cours', async () => {
    const fake = sampleCatalog()
    const clock = new Clock()
    const deps = makeDeps(fake, clock)
    const started = await startSync(db(), deps.config, clock.now())
    const runId = Object.fromEntries(started.started.map((run) => [run.language, run.runId])) as Record<Language, number>
    expect(await finalizeSyncRuns(db(), deps.config, clock.now())).toEqual([])

    // La tâche de catalogue FR est bloquée par une exécution morte (verrou d'une heure).
    const [blocked] = await claimJobs(db(), { limit: 1, token: 'execution-morte', nowMs: clock.now(), leaseMs: 60 * MIN })
    expect(blocked).toMatchObject({ job_type: 'catalog', language: 'fr', run_id: runId.fr })
    await drain(deps, clock)

    const runs = () => rows<SyncRunRow>(`SELECT * FROM sync_runs ORDER BY id`)
    const [frRun, jaRun] = await runs()
    expect(frRun).toMatchObject({ id: runId.fr, status: 'running', cards_found: null })
    expect(jaRun).toMatchObject({
      id: runId.ja,
      status: 'completed',
      series_found: 2,
      sets_found: 2,
      cards_found: 3,
      cards_processed: 3,
      images_downloaded: 1,
      images_recovered: 1,
      images_already_present: 0,
      images_missing: 1,
      images_failed: 0,
      bytes_stored: 650,
    })
    // Une synchronisation est encore en cours : pas encore de rapport.
    expect(await rows(`SELECT id FROM import_jobs WHERE job_type = 'report'`)).toEqual([])
    expect(await finalizeSyncRuns(db(), deps.config, clock.now())).toEqual([])

    // Le verrou expire : la synchronisation FR reprend, se termine, et le rapport est produit.
    clock.advance(60 * MIN)
    await drain(deps, clock)
    const [frDone] = await runs()
    expect(frDone).toMatchObject({
      status: 'completed',
      series_found: 2,
      sets_found: 2,
      cards_found: 4,
      cards_processed: 4,
      images_downloaded: 1,
      images_recovered: 1,
      images_missing: 2,
      images_failed: 0,
      bytes_stored: 610,
    })
    // Le bilan est celui des tables, pas une estimation.
    const real = await one<{ cards: number; bytes: number }>(
      `SELECT COUNT(*) AS cards, SUM(COALESCE(image_file_size, 0)) AS bytes FROM cards WHERE language = 'fr' AND removed_at IS NULL`,
    )
    expect(real).toEqual({ cards: frDone?.cards_found, bytes: frDone?.bytes_stored })
    const report = await job('report:*:*')
    expect(report.status).toBe('done')
    expect(JSON.parse(report.payload_json as string)).toEqual({ syncRuns: [runId.fr] })
    expect(await env.BUCKET.head('reports/latest/summary.json')).not.toBeNull()
  })
})

// ——— Synchronisation hebdomadaire ———

describe('synchronisation hebdomadaire', () => {
  const swsh3 = (fake: FakeTcgdex) => fake.sets.fr.find((set) => set.id === 'swsh3') as FakeTcgdex['sets']['fr'][number]

  it('nouvelle carte importée, image apparue dans l’API téléchargée, nom modifié mis à jour', async () => {
    const fake = sampleCatalog()
    const clock = new Clock()
    const deps = makeDeps(fake, clock)
    await importAll(deps, clock)
    const before137 = await card('fr:swsh3-137')

    const set = swsh3(fake)
    set.cards.push({ id: 'swsh3-139', localId: '139', name: 'Nouvelle carte', image: `${ASSETS}/fr/swsh/swsh3/139` })
    fake.addImage(`${ASSETS}/fr/swsh/swsh3/139/high.webp`, webp(340, 5))
    const card138 = set.cards.find((item) => item.id === 'swsh3-138')
    const card136 = set.cards.find((item) => item.id === 'swsh3-136')
    if (!card138 || !card136) throw new Error('catalogue de référence inattendu')
    card138.image = `${ASSETS}/fr/swsh/swsh3/138`
    fake.addImage(URL_138, webp(350, 6))
    card136.name = 'Fouinar (nom corrigé)'

    const runId = await weeklySync(deps, clock)

    expect(await card('fr:swsh3-139')).toMatchObject({ name: 'Nouvelle carte', set_id: 'fr:swsh3', image_status: 'downloaded', detail_status: 'fetched', removed_at: null })
    expect(await card('fr:swsh3-138')).toMatchObject({ api_image_url: `${ASSETS}/fr/swsh/swsh3/138`, image_status: 'downloaded', image_source: 'api', image_r2_key: 'cards/fr/swsh3/swsh3-138.webp' })
    expect(await env.BUCKET.head('cards/fr/swsh3/swsh3-138.webp')).not.toBeNull()
    expect(await card('fr:swsh3-136')).toMatchObject({ name: 'Fouinar (nom corrigé)', image_status: 'downloaded' })
    // Image valide jamais retéléchargée ; carte inchangée jamais réécrite.
    expect(fake.count((url) => url === URL_136)).toBe(1)
    expect(await card('fr:swsh3-137')).toEqual(before137)
    expect(await one(`SELECT status, cards_found, images_downloaded, images_recovered, images_missing, bytes_stored FROM sync_runs WHERE id = ?1`, runId)).toEqual({
      status: 'completed',
      cards_found: 5,
      images_downloaded: 3,
      images_recovered: 1,
      images_missing: 1,
      bytes_stored: 300 + 310 + 340 + 350,
    })
  })

  it('carte disparue de son extension : removed_at posé, jamais supprimée ; liste vide : anomalie, rien marqué', async () => {
    const fake = sampleCatalog()
    const clock = new Clock()
    const deps = makeDeps(fake, clock)
    await importAll(deps, clock)

    const set = swsh3(fake)
    const vanished = set.cards.find((item) => item.id === 'swsh3-137')
    if (!vanished) throw new Error('catalogue de référence inattendu')
    set.cards = set.cards.filter((item) => item !== vanished)
    const exu = fake.sets.fr.find((item) => item.id === 'exu')
    if (!exu) throw new Error('catalogue de référence inattendu')
    exu.cards = []

    const runId = await weeklySync(deps, clock)
    const removed = await card('fr:swsh3-137')
    expect(removed.removed_at).not.toBeNull()
    expect(removed).toMatchObject({ image_status: 'recovered_from_cdn', image_r2_key: 'cards/fr/swsh3/swsh3-137.webp' })
    expect(await env.BUCKET.head('cards/fr/swsh3/swsh3-137.webp')).not.toBeNull()
    // Extension listée vide : traitée comme une anomalie de l'API.
    expect((await card('fr:exu-%3F')).removed_at).toBeNull()
    expect((await one<{ n: number }>(`SELECT COUNT(*) AS n FROM cards`)).n).toBe(4)
    expect(await one(`SELECT cards_found, images_recovered FROM sync_runs WHERE id = ?1`, runId)).toEqual({ cards_found: 3, images_recovered: 0 })

    // La carte réapparaît : sa marque est levée, sans retéléchargement de l'image.
    set.cards.push(vanished)
    await weeklySync(deps, clock)
    expect(await card('fr:swsh3-137')).toMatchObject({ removed_at: null, image_status: 'recovered_from_cdn' })
    expect(fake.count((url) => url === URL_137)).toBe(1)
  })

  it('images absentes revérifiées seulement après MISSING_RECHECK_DAYS', async () => {
    const fake = sampleCatalog()
    const clock = new Clock()
    const deps = makeDeps(fake, clock)
    await importAll(deps, clock)
    expect(fake.count((url) => url === URL_138)).toBe(1)
    // Le CDN reçoit l'image après coup.
    fake.addImage(URL_138, webp(350, 6))

    await weeklySync(deps, clock, 7)
    await weeklySync(deps, clock, 21)
    expect(fake.count((url) => url === URL_138)).toBe(1)
    expect((await card('fr:swsh3-138')).image_status).toBe('not_found_on_cdn')

    await weeklySync(deps, clock, 7)
    expect(fake.count((url) => url === URL_138)).toBe(2)
    expect(await card('fr:swsh3-138')).toMatchObject({ image_status: 'recovered_from_cdn', image_source: 'cdn_reconstructed' })
    expect(fake.count((url) => url === URL_EXU)).toBe(2)
    expect(fake.count((url) => url === URL_136)).toBe(1)
  })

  it('détails des cartes rafraîchis seulement après DETAIL_REFRESH_DAYS', async () => {
    const fake = sampleCatalog()
    const clock = new Clock()
    const deps = makeDeps(fake, clock)
    await importAll(deps, clock)
    const details = () => fake.count((url) => url.startsWith(`${API}/fr/cards/`))
    expect(details()).toBe(4)
    const fetchedAt = (await card('fr:swsh3-136')).detail_fetched_at

    await weeklySync(deps, clock, 7)
    await weeklySync(deps, clock, 77)
    expect(details()).toBe(4)
    expect((await card('fr:swsh3-136')).detail_fetched_at).toBe(fetchedAt)

    await weeklySync(deps, clock, 7)
    expect(details()).toBe(8)
    for (const id of ['swsh3-136', 'swsh3-137', 'swsh3-138', 'exu-%3F']) expect(fake.count((url) => url === `${API}/fr/cards/${encodeURIComponent(id)}`)).toBe(2)
    expect(Date.parse((await card('fr:swsh3-136')).detail_fetched_at as string)).toBeGreaterThan(Date.parse(fetchedAt as string) + 90 * DAY)
  })

  it('cartes orphelines (dans /cards mais dans aucune extension listée) : importées via card_index, tâche « set » créée pour leur extension', async () => {
    const fake = sampleCatalog()
    // Extension absente de /sets mais dont le détail existe.
    fake.sets.fr.push({
      id: 'swshp',
      name: 'Promos Épée et Bouclier',
      serie: { id: 'swsh', name: 'Épée et Bouclier' },
      listed: false,
      cards: [{ id: 'swshp-SWSH001', localId: 'SWSH001', name: 'Orphelin promo', image: `${ASSETS}/fr/swsh/swshp/SWSH001` }],
    })
    fake.addImage(`${ASSETS}/fr/swsh/swshp/SWSH001/high.webp`, webp(360, 7))
    // Extension inconnue de l'API (/sets/xy : 404).
    fake.orphanCards.fr.push({ id: 'xy-1', localId: '1', name: 'Orphelin sans extension', image: `${ASSETS}/fr/swsh/xy/1`, setId: 'xy' })
    fake.addImage(`${ASSETS}/fr/swsh/xy/1/high.webp`, webp(370, 8))
    const clock = new Clock()
    const deps = makeDeps(fake, clock)
    await importAll(deps, clock)

    expect((await job('card_index:fr:*')).status).toBe('done')
    expect(await card('fr:swshp-SWSH001')).toMatchObject({
      set_id: 'fr:swshp',
      series_id: 'swsh',
      detail_status: 'fetched',
      image_status: 'downloaded',
      image_r2_key: 'cards/fr/swshp/swshp-SWSH001.webp',
      removed_at: null,
    })
    expect((await job('set:fr:swshp')).status).toBe('done')
    expect(await one(`SELECT name, series_id FROM sets WHERE id = 'fr:swshp'`)).toEqual({ name: 'Promos Épée et Bouclier', series_id: 'fr:swsh' })

    expect(await card('fr:xy-1')).toMatchObject({ set_id: 'fr:xy', image_status: 'downloaded', image_r2_key: 'cards/fr/xy/xy-1.webp' })
    expect(await job('set:fr:xy')).toMatchObject({ status: 'failed', last_http_status: 404 })
    expect(await one(`SELECT status, cards_found FROM sync_runs`)).toEqual({ status: 'completed', cards_found: 6 })

    // La semaine suivante : rien de nouveau, pas de nouvelle tentative sur l'extension introuvable.
    await weeklySync(deps, clock)
    expect(fake.count((url) => url === `${API}/fr/sets/xy`)).toBe(1)
    expect((await job('set:fr:xy')).status).toBe('failed')
    expect((await card('fr:xy-1')).set_id).toBe('fr:xy')
  })

  it('carte orpheline dont le détail échoue (404, ou tentatives épuisées) : créée par l’index avec son image, image quand même cherchée', async () => {
    const fake = sampleCatalog()
    fake.orphanCards.fr.push(
      { id: 'xy-2', localId: '2', name: 'Orphelin sans détail', image: `${ASSETS}/fr/xy/xy1/2`, setId: 'xy' },
      { id: 'xy-3', localId: '3', name: 'Orphelin en erreur', image: `${ASSETS}/fr/xy/xy1/3`, setId: 'xy' },
    )
    fake.addImage(`${ASSETS}/fr/xy/xy1/2/high.webp`, webp(410, 12))
    fake.addImage(`${ASSETS}/fr/xy/xy1/3/high.webp`, webp(420, 13))
    fake.queue(`${API}/fr/cards/xy-2`, (url) => jsonResponse({ type: 'https://tcgdex.dev/errors/not-found', status: 404, endpoint: url }, 404))
    fake.queue(
      `${API}/fr/cards/xy-3`,
      () => new Response('erreur', { status: 500 }),
      () => new Response('erreur', { status: 500 }),
    )
    const clock = new Clock()
    const deps = makeDeps(fake, clock, { MAX_ATTEMPTS: '2' })
    await importAll(deps, clock)

    // Créées par l'index, telles qu'il les décrit (sans extension ni empreinte de liste).
    const fromIndex = { set_id: null, series_id: null, brief_hash: null, removed_at: null }
    expect(await card('fr:xy-2')).toMatchObject({
      ...fromIndex,
      name: 'Orphelin sans détail',
      local_id: '2',
      api_image_url: `${ASSETS}/fr/xy/xy1/2`,
      detail_status: 'not_found',
      image_status: 'downloaded',
      image_r2_key: 'cards/fr/sans-extension/xy-2.webp',
    })
    expect(await card('fr:xy-3')).toMatchObject({
      ...fromIndex,
      name: 'Orphelin en erreur',
      local_id: '3',
      api_image_url: `${ASSETS}/fr/xy/xy1/3`,
      detail_status: 'error',
      image_status: 'downloaded',
      image_r2_key: 'cards/fr/sans-extension/xy-3.webp',
    })
    expect(await job('card:fr:xy-2')).toMatchObject({ status: 'failed', attempts: 1, last_http_status: 404 })
    expect(await job('card:fr:xy-3')).toMatchObject({ status: 'failed', attempts: 2, last_http_status: 500 })
    expect(await job('image:fr:xy-2')).toMatchObject({ status: 'done' })
    expect(await job('image:fr:xy-3')).toMatchObject({ status: 'done' })
    expect(await env.BUCKET.head('cards/fr/sans-extension/xy-2.webp')).not.toBeNull()
    expect(await rows(`SELECT id FROM import_jobs WHERE dedupe_key = 'set:fr:xy'`)).toEqual([])
    expect(await one(`SELECT status, cards_found FROM sync_runs`)).toEqual({ status: 'completed', cards_found: 6 })
  })

  it('carte orpheline rattachée par son détail à une extension listée : jamais marquée retirée par celle-ci', async () => {
    const fake = sampleCatalog()
    // Dans /cards, et son détail indique swsh3, mais le détail de swsh3 ne la liste pas.
    fake.orphanCards.fr.push({ id: 'swsh3-200', localId: '200', name: 'Orpheline de swsh3', image: `${ASSETS}/fr/swsh/swsh3/200`, setId: 'swsh3' })
    fake.addImage(`${ASSETS}/fr/swsh/swsh3/200/high.webp`, webp(400, 11))
    const clock = new Clock()
    const deps = makeDeps(fake, clock)
    await importAll(deps, clock)

    // Rattachée à swsh3 par son détail (l'extension a été relue ensuite), sans être marquée retirée.
    expect(await card('fr:swsh3-200')).toMatchObject({
      set_id: 'fr:swsh3',
      brief_hash: null,
      removed_at: null,
      detail_status: 'fetched',
      image_status: 'downloaded',
      image_r2_key: 'cards/fr/swsh3/swsh3-200.webp',
    })
    expect(fake.count((url) => url === `${API}/fr/sets/swsh3`)).toBe(2)
    expect(await one(`SELECT status, cards_found FROM sync_runs`)).toEqual({ status: 'completed', cards_found: 5 })

    // Les synchronisations suivantes non plus.
    await weeklySync(deps, clock)
    await weeklySync(deps, clock)
    expect(await rows(`SELECT id FROM cards WHERE removed_at IS NOT NULL`)).toEqual([])
  })

  it('seuil de retrait relatif : jusqu’à 20 % des cartes connues de l’extension retirées ; au-delà, anomalie et rien n’est marqué', async () => {
    const fake = sampleCatalog()
    fake.sets.fr.push(bulkSet('swsh4', 10))
    const clock = new Clock()
    const deps = makeDeps(fake, clock)
    await importAll(deps, clock)
    const big = fake.sets.fr.find((set) => set.id === 'swsh4') as FakeSet
    const all = [...big.cards]
    const removed = () => rows<{ id: string }>(`SELECT id FROM cards WHERE removed_at IS NOT NULL ORDER BY id`)

    // 3 cartes sur 10 disparaissent (plus de 20 %) : anomalie de l'API, rien n'est marqué.
    big.cards = all.slice(0, 7)
    await weeklySync(deps, clock)
    expect(await removed()).toEqual([])

    // 2 sur 10 (20 %, floor(10 × 0,2) = 2) : retrait réel, marqué.
    big.cards = all.slice(0, 8)
    await weeklySync(deps, clock)
    expect(await removed()).toEqual([{ id: 'fr:swsh4-10' }, { id: 'fr:swsh4-9' }])
    expect((await one<{ n: number }>(`SELECT COUNT(*) AS n FROM cards WHERE set_id = 'fr:swsh4'`)).n).toBe(10)
  })

  it('carte listée dans pl3 dont le détail indique pl4 (404) : reste rattachée à pl3, sans va-et-vient', async () => {
    const fake = sampleCatalog()
    fake.sets.fr.push({
      id: 'pl3',
      name: 'Suprême Victors',
      serie: { id: 'pl', name: 'Platine' },
      cards: [
        { id: 'pl3-1', localId: '1', name: 'Carte de pl3', image: `${ASSETS}/fr/pl/pl3/1` },
        { id: 'pl4-AR1', localId: 'AR1', name: 'Carte AR', image: `${ASSETS}/fr/pl/pl4/AR1`, detailSetId: 'pl4' },
      ],
    })
    fake.addImage(`${ASSETS}/fr/pl/pl3/1/high.webp`, webp(380, 9))
    fake.addImage(`${ASSETS}/fr/pl/pl4/AR1/high.webp`, webp(390, 10))
    const clock = new Clock()
    const deps = makeDeps(fake, clock)
    await importAll(deps, clock)

    const snapshot = await card('fr:pl4-AR1')
    expect(snapshot).toMatchObject({ set_id: 'fr:pl3', series_id: 'pl', detail_status: 'fetched', image_status: 'downloaded', image_r2_key: 'cards/fr/pl3/pl4-AR1.webp', removed_at: null })
    const noPl4 = async () => {
      expect(await rows(`SELECT id FROM sets WHERE tcgdex_id = 'pl4'`)).toEqual([])
      expect(await rows(`SELECT id FROM import_jobs WHERE dedupe_key = 'set:fr:pl4'`)).toEqual([])
      expect(fake.count((url) => url === `${API}/fr/sets/pl4`)).toBe(0)
    }
    await noPl4()

    // Deux synchronisations hebdomadaires : la carte n'est ni déplacée ni réécrite.
    await weeklySync(deps, clock)
    await weeklySync(deps, clock)
    expect(await card('fr:pl4-AR1')).toEqual(snapshot)
    await noPl4()

    // Rafraîchissement du détail (après DETAIL_REFRESH_DAYS) : toujours rattachée à pl3.
    await weeklySync(deps, clock, 91)
    expect(await card('fr:pl4-AR1')).toMatchObject({ set_id: 'fr:pl3', removed_at: null, image_r2_key: 'cards/fr/pl3/pl4-AR1.webp' })
    await noPl4()
    expect(fake.count((url) => url === `${ASSETS}/fr/pl/pl4/AR1/high.webp`)).toBe(1)
  })
})
