/**
 * Schéma D1, contraintes et dépôts (`src/repositories/*.ts`, `src/db.ts`), directement sur la D1
 * migrée propre à ce fichier de test. Aucun appel réseau : seule D1 est sollicitée, avec des dates
 * fixes (aucune dépendance à l'horloge réelle).
 */

import { env } from 'cloudflare:workers'
import { beforeEach, describe, expect, it } from 'vitest'
import type { Language } from '../src/config'
import { Db, Usage, type SqlValue } from '../src/db'
import {
  ensureSeriesStmt,
  imageFailureStmt,
  imageSuccessStmt,
  insertOrphanBriefsStmts,
  insertSetBriefsStmts,
  markRemovedStmt,
  upsertCardBriefsStmts,
  upsertCardDetailStmt,
  upsertSeriesStmts,
  upsertSetDetailStmt,
  type CardBriefRow,
  type CardDetailRow,
  type ImageStatus,
  type ImageSuccess,
  type SeriesRow,
  type SetBriefRow,
  type SetDetailRow,
} from '../src/repositories/catalog'
import {
  claimJobs,
  completeStmt,
  dedupeKey,
  enqueueStmts,
  failStmt,
  guardStmt,
  reclaimExpired,
  releaseStmt,
  retryStmt,
  type Job,
  type JobType,
  type NewJob,
} from '../src/repositories/jobs'
import {
  cancelSyncRunsStmt,
  completeSyncRunStmt,
  createSyncRunStmt,
  hasCompletedRun,
  insertTickRunStmt,
  languageCounts,
  storedBytes,
  type LanguageCounts,
  type SyncKind,
  type TickSummary,
} from '../src/repositories/runs'
import {
  acquireRunnerLease,
  addDailyOperationsStmt,
  dailyOperations,
  readState,
  releaseRunnerLeaseStmt,
  renewRunnerLeaseStmt,
  setStateStmt,
  STATE,
} from '../src/repositories/state'
import { iso } from '../src/utils/time'
import { one, resetAll, rows } from './helpers'

beforeEach(resetAll)

// ——— Outils locaux ———

const T0 = Date.parse('2026-11-13T10:00:00.000Z')
const MIN = 60_000
const HOUR = 60 * MIN
const DAY = 24 * HOUR
/** Durée du verrou d'une tâche dans ces tests. */
const LEASE = 10 * MIN
const NOW = iso(T0)

const newDb = (): Db => new Db(env.DB)

/** Insertion directe (sans passer par les dépôts), pour éprouver le schéma lui-même. */
function insertRaw(table: string, values: Record<string, SqlValue>): Promise<D1Result> {
  const columns = Object.keys(values)
  return env.DB.prepare(`INSERT INTO ${table} (${columns.join(', ')}) VALUES (${columns.map((_, i) => `?${i + 1}`).join(', ')})`)
    .bind(...Object.values(values))
    .run()
}

type Values = Record<string, SqlValue>
const RAW: Record<'series' | 'sets' | 'cards', (over?: Values) => Values> = {
  series: (over = {}) => ({ id: 'fr:swsh', tcgdex_id: 'swsh', language: 'fr', name: 'Épée et Bouclier', created_at: NOW, updated_at: NOW, ...over }),
  sets: (over = {}) => ({ id: 'fr:swsh3', tcgdex_id: 'swsh3', language: 'fr', name: 'Ténèbres Embrasées', created_at: NOW, updated_at: NOW, ...over }),
  cards: (over = {}) => ({ id: 'fr:swsh3-136', tcgdex_id: 'swsh3-136', language: 'fr', name: 'Fouinar', local_id: '136', created_at: NOW, updated_at: NOW, ...over }),
}
const rawJob = (over: Values = {}): Values => ({
  job_type: 'set',
  language: 'fr',
  dedupe_key: 'set:fr:swsh3',
  priority: 1,
  max_attempts: 5,
  next_attempt_at: NOW,
  created_at: NOW,
  updated_at: NOW,
  ...over,
})

/** Exécute un batch et rend ce que D1 dit avoir écrit (somme sur toutes les requêtes). */
async function written(db: Db, statements: D1PreparedStatement[]): Promise<{ rowsWritten: number; changes: number }> {
  const results = await db.batch(statements)
  return {
    rowsWritten: results.reduce((sum, result) => sum + result.meta.rows_written, 0),
    changes: results.reduce((sum, result) => sum + result.meta.changes, 0),
  }
}

const seriesRow = (id: string, name = `Série ${id}`, hash = `serie-${id}-v1`): SeriesRow => ({ id, name, logo: null, meta: JSON.stringify({ id, name }), hash })

const setBrief = (id: string, over: Partial<SetBriefRow> = {}): SetBriefRow => ({ id, name: `Extension ${id}`, logo: null, symbol: null, total: 3, official: 3, ...over })

const setDetail = (over: Partial<SetDetailRow> = {}): SetDetailRow => ({
  id: 'swsh3',
  name: 'Ténèbres Embrasées',
  logo: 'https://assets.tcgdex.net/fr/swsh/swsh3/logo',
  symbol: null,
  total: 3,
  official: 3,
  seriesTcgdexId: 'swsh',
  releaseDate: '2020-08-14',
  meta: '{"id":"swsh3"}',
  hash: 'set-v1',
  ...over,
})

const brief = (id: string, over: Partial<CardBriefRow> = {}): CardBriefRow => ({
  id,
  localId: id.split('-').at(-1) ?? id,
  name: `Carte ${id}`,
  image: null,
  hash: `brief-${id}-v1`,
  ...over,
})

const cardDetail = (over: Partial<CardDetailRow> = {}): CardDetailRow => ({
  id: 'pl4-AR1',
  localId: 'AR1',
  name: 'Arceus',
  setTcgdexId: 'pl4',
  seriesTcgdexId: 'pl',
  rarity: 'Rare',
  category: 'Pokémon',
  illustrator: 'Illustrateur',
  image: null,
  meta: '{"id":"pl4-AR1"}',
  hash: 'detail-v1',
  ...over,
})

const success = (status: ImageSuccess['status'], key: string, size = 300): ImageSuccess => ({
  status,
  source: status === 'downloaded' ? 'api' : status === 'recovered_from_cdn' ? 'cdn_reconstructed' : null,
  sourceUrl: status === 'already_present' ? null : `https://assets.tcgdex.net/${key}`,
  key,
  quality: 'high',
  size,
  sha256: `sha-${key}`,
  testedUrls: '[]',
  httpStatus: status === 'already_present' ? null : 200,
})

const failure = (status: ImageStatus, reason = 'introuvable sur le CDN') => ({ status, reason, testedUrls: '[{"url":"u","status":404}]', httpStatus: 404 })

/** Une série et des extensions d'une langue, comme après la tâche `catalog`. */
async function seedSets(db: Db, language: Language, seriesId: string, setIds: string[]): Promise<void> {
  await db.batch([...upsertSeriesStmts(db, language, [seriesRow(seriesId)], T0), ...insertSetBriefsStmts(db, language, setIds.map((id) => setBrief(id)), T0)])
}

const newJob = (type: JobType, language: Language | null, target: string, over: Partial<NewJob> = {}): NewJob => ({
  target,
  cardId: type === 'card' || type === 'image' ? `${language ?? 'fr'}:${target}` : null,
  dedupeKey: dedupeKey(type, language, target),
  ...over,
})

interface EnqueueOpts {
  runId?: number | null
  maxAttempts?: number
  nowMs?: number
  notBeforeMs?: number
}

function enqueue(db: Db, type: JobType, language: Language | null, targets: string[], opts: EnqueueOpts = {}): Promise<D1Result[]> {
  const { runId = null, maxAttempts = 5, nowMs = T0, notBeforeMs } = opts
  return db.batch(
    enqueueStmts(
      db,
      type,
      language,
      targets.map((target) => newJob(type, language, target)),
      notBeforeMs === undefined ? { runId, maxAttempts, nowMs } : { runId, maxAttempts, nowMs, notBeforeMs },
    ),
  )
}

/**
 * Crée une synchronisation par `createSyncRunStmt` (comme `startSync`, dans un batch) et rend son id ;
 * échoue si aucune n'a été créée (une autre est déjà en cours pour cette langue).
 */
async function createSyncRun(db: Db, language: Language, kind: SyncKind, scope: unknown, nowMs: number): Promise<number> {
  const [result] = await db.batch([createSyncRunStmt(db, language, kind, scope, nowMs)])
  const created = result?.results[0] as { id: number } | undefined
  if (!created) throw new Error(`aucune synchronisation créée pour ${language}`)
  return created.id
}

const ZERO_COUNTS: LanguageCounts = {
  series_found: 0,
  sets_found: 0,
  cards_found: 0,
  cards_processed: 0,
  images_downloaded: 0,
  images_recovered: 0,
  images_already_present: 0,
  images_missing: 0,
  images_failed: 0,
  bytes_stored: 0,
}

const claim = (db: Db, token: string, nowMs = T0, limit = 50): Promise<Job[]> => claimJobs(db, { limit, token, nowMs, leaseMs: LEASE })

/** Réserve exactement une tâche (échoue sinon). */
async function claimOne(db: Db, token: string, nowMs = T0): Promise<Job> {
  const jobs = await claim(db, token, nowMs, 1)
  const [job] = jobs
  if (!job || jobs.length !== 1) throw new Error(`une tâche réservée attendue, ${jobs.length} obtenue(s)`)
  return job
}

interface JobRow {
  id: number
  job_type: string
  language: string | null
  target_id: string | null
  card_id: string | null
  dedupe_key: string
  priority: number
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
  created_at: string
  updated_at: string
  completed_at: string | null
}

const jobRow = (key: string): Promise<JobRow> => one<JobRow>(`SELECT * FROM import_jobs WHERE dedupe_key = ?1`, key)
const jobById = (id: number): Promise<JobRow> => one<JobRow>(`SELECT * FROM import_jobs WHERE id = ?1`, id)
const countOf = async (table: string): Promise<number> => (await one<{ n: number }>(`SELECT COUNT(*) AS n FROM ${table}`)).n

// ——— Schéma ———

describe('schéma D1 (migrations)', () => {
  it('les migrations créent toutes les tables attendues', async () => {
    const tables = (await rows<{ name: string }>(`SELECT name FROM sqlite_master WHERE type = 'table'`)).map((row) => row.name)
    expect(tables).toEqual(
      expect.arrayContaining(['series', 'sets', 'cards', 'sync_runs', 'import_jobs', 'job_lock_guard', 'tick_runs', 'runtime_state', 'card_marketplace_links']),
    )
  })

  it('les migrations créent les index attendus, dont les index partiels de la file des tâches', async () => {
    const indexes = await rows<{ name: string; tbl_name: string; sql: string | null }>(`SELECT name, tbl_name, sql FROM sqlite_master WHERE type = 'index'`)
    const byName = new Map(indexes.map((index) => [index.name, index]))
    const expected: [string, string][] = [
      ['sets_series', 'sets'],
      ['cards_set', 'cards'],
      ['cards_language_status', 'cards'],
      ['cards_stored', 'cards'],
      ['cards_low_stored', 'cards'],
      ['sync_runs_status', 'sync_runs'],
      ['sync_runs_one_running', 'sync_runs'],
      ['import_jobs_ready', 'import_jobs'],
      ['import_jobs_locked', 'import_jobs'],
      ['import_jobs_active_run', 'import_jobs'],
      ['tick_runs_started', 'tick_runs'],
    ]
    for (const [name, table] of expected) expect(byName.get(name)?.tbl_name, name).toBe(table)
    // Les index partiels ne portent que sur les tâches actives.
    expect(byName.get('import_jobs_ready')?.sql).toMatch(/WHERE status = 'pending'/)
    expect(byName.get('import_jobs_locked')?.sql).toMatch(/WHERE status = 'running'/)
    expect(byName.get('import_jobs_active_run')?.sql).toMatch(/WHERE status IN \('pending', 'running'\)/)
    // Index couvrant du volume stocké, et une seule synchronisation `running` par langue.
    expect(byName.get('cards_stored')?.sql).toMatch(/ON cards \(image_status, image_file_size\)/)
    expect(byName.get('sync_runs_one_running')?.sql).toMatch(/^CREATE UNIQUE INDEX sync_runs_one_running ON sync_runs \(language\) WHERE status = 'running'$/)
    const syncIndexes = await rows<{ name: string; unique: number; partial: number }>(`PRAGMA index_list(sync_runs)`)
    expect(syncIndexes.find((index) => index.name === 'sync_runs_one_running')).toMatchObject({ unique: 1, partial: 1 })
    // Les contraintes UNIQUE ont leur index automatique.
    for (const table of ['series', 'sets', 'cards', 'import_jobs', 'card_marketplace_links']) {
      expect(indexes.some((index) => index.tbl_name === table && index.name.startsWith('sqlite_autoindex_')), table).toBe(true)
    }
  })

  it('storedBytes est lu sur l’index couvrant cards_stored (sans relire la table)', async () => {
    const plan = await rows<{ detail: string }>(
      `EXPLAIN QUERY PLAN SELECT COALESCE(SUM(image_file_size), 0) AS bytes FROM cards
       WHERE image_status IN ('downloaded', 'recovered_from_cdn', 'already_present')`,
    )
    expect(plan.map((step) => step.detail).join('\n')).toMatch(/USING COVERING INDEX cards_stored/)
  })

  it('les nouvelles colonnes existent, NOT NULL et nulles par défaut : import_jobs.releases, tick_runs.jobs_released et jobs_lost', async () => {
    type Column = { name: string; type: string; notnull: number; dflt_value: string | null }
    const columns = async (table: string) => new Map((await rows<Column>(`PRAGMA table_info(${table})`)).map((column) => [column.name, column]))
    expect((await columns('import_jobs')).get('releases')).toMatchObject({ type: 'INTEGER', notnull: 1, dflt_value: '0' })
    const ticks = await columns('tick_runs')
    for (const name of ['jobs_released', 'jobs_lost']) expect(ticks.get(name), name).toMatchObject({ type: 'INTEGER', notnull: 1, dflt_value: '0' })

    // Une ligne insérée sans ces colonnes part de zéro.
    await insertRaw('import_jobs', rawJob())
    expect((await jobRow('set:fr:swsh3')).releases).toBe(0)
    await insertRaw('tick_runs', { trigger: 'cron', started_at: NOW })
    expect(await one(`SELECT jobs_released, jobs_lost FROM tick_runs`)).toEqual({ jobs_released: 0, jobs_lost: 0 })
    // NOT NULL : une valeur NULL explicite est refusée.
    await expect(insertRaw('import_jobs', rawJob({ dedupe_key: 'set:fr:autre', releases: null }))).rejects.toThrow(/NOT NULL constraint failed: import_jobs.releases/)
    await expect(insertRaw('tick_runs', { trigger: 'cron', started_at: NOW, jobs_lost: null })).rejects.toThrow(/NOT NULL constraint failed: tick_runs.jobs_lost/)
  })

  it('sync_runs_one_running : une seule synchronisation running par langue, les synchronisations closes ne comptent pas', async () => {
    const run = (over: Values = {}): Values => ({ language: 'fr', kind: 'manual', started_at: NOW, ...over })
    await insertRaw('sync_runs', run({ status: 'completed', finished_at: NOW }))
    await insertRaw('sync_runs', run({ status: 'cancelled', finished_at: NOW }))
    // Statut `running` par défaut.
    await insertRaw('sync_runs', run({ kind: 'initial' }))
    await expect(insertRaw('sync_runs', run({ kind: 'weekly' }))).rejects.toThrow('UNIQUE constraint failed: sync_runs.language')
    // L'autre langue a sa propre synchronisation en cours.
    await insertRaw('sync_runs', run({ language: 'ja' }))
    // Rouvrir une synchronisation close pendant qu'une autre court est refusé aussi.
    await expect(env.DB.prepare(`UPDATE sync_runs SET status = 'running' WHERE language = 'fr' AND status = 'completed'`).run()).rejects.toThrow(
      /UNIQUE constraint failed/,
    )
    expect(await rows(`SELECT language, kind, status FROM sync_runs ORDER BY id`)).toEqual([
      { language: 'fr', kind: 'manual', status: 'completed' },
      { language: 'fr', kind: 'manual', status: 'cancelled' },
      { language: 'fr', kind: 'initial', status: 'running' },
      { language: 'ja', kind: 'manual', status: 'running' },
    ])
    // Une fois la synchronisation en cours close, une nouvelle peut démarrer.
    await env.DB.prepare(`UPDATE sync_runs SET status = 'cancelled', finished_at = ?1 WHERE language = 'fr' AND status = 'running'`).bind(NOW).run()
    await insertRaw('sync_runs', run({ kind: 'weekly' }))
    expect(await countOf('sync_runs')).toBe(5)
  })

  it('job_lock_guard refuse toute valeur autre que 1 et reste vide', async () => {
    await expect(insertRaw('job_lock_guard', { ok: 0 })).rejects.toThrow(/CHECK constraint failed/)
    expect(await countOf('job_lock_guard')).toBe(0)
  })
})

// ——— Contraintes ———

describe('contraintes du schéma', () => {
  for (const table of ['series', 'sets', 'cards'] as const) {
    it(`unicité (language, tcgdex_id) sur ${table} : un doublon inséré directement est refusé`, async () => {
      await insertRaw(table, RAW[table]())
      // Même langue et même id TCGdex sous un autre id interne : refusé par UNIQUE (language, tcgdex_id).
      await expect(insertRaw(table, RAW[table]({ id: 'fr:doublon' }))).rejects.toThrow(`UNIQUE constraint failed: ${table}.language, ${table}.tcgdex_id`)
      // Même id interne : refusé par la clé primaire.
      await expect(insertRaw(table, RAW[table]())).rejects.toThrow(/UNIQUE constraint failed/)
      expect(await countOf(table)).toBe(1)
    })
  }

  it('une même tcgdex_id en fr et en ja coexistent (insertion directe)', async () => {
    for (const table of ['series', 'sets', 'cards'] as const) {
      const fr = RAW[table]()
      await insertRaw(table, fr)
      await insertRaw(table, { ...fr, id: String(fr.id).replace(/^fr:/, 'ja:'), language: 'ja' })
      expect(await rows<{ language: string }>(`SELECT language FROM ${table} WHERE tcgdex_id = ?1 ORDER BY language`, String(fr.tcgdex_id))).toEqual([
        { language: 'fr' },
        { language: 'ja' },
      ])
    }
  })

  it('une même tcgdex_id en fr et en ja coexistent (par les dépôts)', async () => {
    const db = newDb()
    for (const language of ['fr', 'ja'] as const) {
      await seedSets(db, language, 'SV', ['SV1a'])
      await db.batch(upsertCardBriefsStmts(db, language, 'SV1a', 'SV', [brief('SV1a-001')], T0))
    }
    expect(await rows(`SELECT id, language FROM series ORDER BY id`)).toEqual([
      { id: 'fr:SV', language: 'fr' },
      { id: 'ja:SV', language: 'ja' },
    ])
    expect(await rows(`SELECT id, series_id FROM sets ORDER BY id`)).toEqual([
      { id: 'fr:SV1a', series_id: null },
      { id: 'ja:SV1a', series_id: null },
    ])
    expect(await rows(`SELECT id, tcgdex_id, set_id FROM cards ORDER BY id`)).toEqual([
      { id: 'fr:SV1a-001', tcgdex_id: 'SV1a-001', set_id: 'fr:SV1a' },
      { id: 'ja:SV1a-001', tcgdex_id: 'SV1a-001', set_id: 'ja:SV1a' },
    ])
  })

  it('dedupe_key est unique sur import_jobs', async () => {
    await insertRaw('import_jobs', rawJob())
    await expect(insertRaw('import_jobs', rawJob({ job_type: 'card', priority: 5 }))).rejects.toThrow('UNIQUE constraint failed: import_jobs.dedupe_key')
    expect(await countOf('import_jobs')).toBe(1)
  })

  it('(card_id, marketplace) est unique sur card_marketplace_links', async () => {
    await insertRaw('cards', RAW.cards())
    const link = (marketplace: string): Values => ({ card_id: 'fr:swsh3-136', marketplace, created_at: NOW, updated_at: NOW })
    await insertRaw('card_marketplace_links', link('cardmarket'))
    await expect(insertRaw('card_marketplace_links', link('cardmarket'))).rejects.toThrow(
      'UNIQUE constraint failed: card_marketplace_links.card_id, card_marketplace_links.marketplace',
    )
    // Une autre marketplace pour la même carte est acceptée.
    await insertRaw('card_marketplace_links', link('tcgplayer'))
    expect(await countOf('card_marketplace_links')).toBe(2)
  })

  it('CHECK : un image_status invalide est refusé, y compris par imageFailureStmt', async () => {
    await expect(insertRaw('cards', RAW.cards({ image_status: 'perdue' }))).rejects.toThrow(/CHECK constraint failed/)
    await insertRaw('cards', RAW.cards())
    const db = newDb()
    await expect(db.batch([imageFailureStmt(db, 'fr:swsh3-136', failure('perdue' as ImageStatus), T0)])).rejects.toThrow(/CHECK constraint failed/)
    expect((await one<{ image_status: string }>(`SELECT image_status FROM cards`)).image_status).toBe('pending')
  })

  it('CHECK : une langue invalide est refusée sur series, sets, cards, import_jobs et sync_runs', async () => {
    for (const table of ['series', 'sets', 'cards'] as const) {
      await expect(insertRaw(table, RAW[table]({ id: 'en:x', language: 'en' })), table).rejects.toThrow(/CHECK constraint failed/)
    }
    await expect(insertRaw('import_jobs', rawJob({ language: 'en' }))).rejects.toThrow(/CHECK constraint failed/)
    await expect(insertRaw('sync_runs', { language: 'en', kind: 'manual', started_at: NOW })).rejects.toThrow(/CHECK constraint failed/)
    // Les dépôts ne contournent pas la contrainte.
    const db = newDb()
    await expect(db.batch(upsertSeriesStmts(db, 'en' as Language, [seriesRow('swsh')], T0))).rejects.toThrow(/CHECK constraint failed/)
    for (const table of ['series', 'sets', 'cards', 'import_jobs', 'sync_runs']) expect(await countOf(table), table).toBe(0)
  })

  it('CHECK : type ou statut de tâche invalide refusé', async () => {
    await expect(insertRaw('import_jobs', rawJob({ job_type: 'autre' }))).rejects.toThrow(/CHECK constraint failed/)
    await expect(insertRaw('import_jobs', rawJob({ status: 'perdue' }))).rejects.toThrow(/CHECK constraint failed/)
  })

  it('les clés étrangères sont appliquées : une carte ne peut pas pointer vers une extension inconnue', async () => {
    await expect(insertRaw('cards', RAW.cards({ set_id: 'fr:inconnue' }))).rejects.toThrow(/FOREIGN KEY constraint failed/)
    expect(await countOf('cards')).toBe(0)
  })
})

// ——— Catalogue ———

describe('catalogue : écritures idempotentes', () => {
  it('upsertSeriesStmts : rejouer la même donnée n’écrit rien, une donnée modifiée met à jour', async () => {
    const db = newDb()
    const created = await written(db, upsertSeriesStmts(db, 'fr', [seriesRow('swsh'), seriesRow('sm')], T0))
    expect(created.changes).toBe(2)
    // Le compteur de lignes écrites est significatif (non nul quand D1 écrit réellement).
    expect(created.rowsWritten).toBeGreaterThan(0)
    const before = await rows(`SELECT * FROM series ORDER BY id`)
    expect(before).toEqual([
      expect.objectContaining({ id: 'fr:sm', tcgdex_id: 'sm', language: 'fr', name: 'Série sm', content_hash: 'serie-sm-v1', created_at: NOW, updated_at: NOW }),
      expect.objectContaining({ id: 'fr:swsh', tcgdex_id: 'swsh', language: 'fr', name: 'Série swsh', metadata_json: '{"id":"swsh","name":"Série swsh"}' }),
    ])

    // Même donnée, le lendemain : aucune ligne écrite.
    expect(await written(db, upsertSeriesStmts(db, 'fr', [seriesRow('swsh'), seriesRow('sm')], T0 + DAY))).toEqual({ rowsWritten: 0, changes: 0 })
    expect(await rows(`SELECT * FROM series ORDER BY id`)).toEqual(before)

    // Donnée modifiée (empreinte différente) : seule la série modifiée est réécrite.
    const changed = await written(db, upsertSeriesStmts(db, 'fr', [seriesRow('swsh', 'Épée et Bouclier', 'serie-swsh-v2'), seriesRow('sm')], T0 + 2 * DAY))
    expect(changed.changes).toBe(1)
    expect(await one(`SELECT name, content_hash, created_at, updated_at FROM series WHERE id = 'fr:swsh'`)).toEqual({
      name: 'Épée et Bouclier',
      content_hash: 'serie-swsh-v2',
      created_at: NOW,
      updated_at: iso(T0 + 2 * DAY),
    })
    expect((await one<{ updated_at: string }>(`SELECT updated_at FROM series WHERE id = 'fr:sm'`)).updated_at).toBe(NOW)
  })

  it('upsertSeriesStmts découpe les gros volumes et ensureSeriesStmt n’écrase pas une série complète', async () => {
    const db = newDb()
    const many = Array.from({ length: 850 }, (_, i) => seriesRow(`s${i}`))
    const statements = upsertSeriesStmts(db, 'ja', many, T0)
    expect(statements.length).toBeGreaterThan(1)
    await db.batch(statements)
    expect(await countOf('series')).toBe(850)

    await db.batch([ensureSeriesStmt(db, 'ja', { id: 's1', name: 'Nom partiel' }, T0 + DAY)])
    expect(await one(`SELECT name, updated_at FROM series WHERE id = 'ja:s1'`)).toEqual({ name: 'Série s1', updated_at: NOW })
  })

  it('upsertSetDetailStmt : complète l’extension, rejouer n’écrit rien, une donnée modifiée met à jour', async () => {
    const db = newDb()
    await seedSets(db, 'fr', 'swsh', ['swsh3'])
    // L'extension connue par la liste n'a encore ni série ni empreinte.
    expect(await one(`SELECT series_id, content_hash, detail_fetched_at FROM sets WHERE id = 'fr:swsh3'`)).toEqual({ series_id: null, content_hash: null, detail_fetched_at: null })

    expect((await written(db, [upsertSetDetailStmt(db, 'fr', setDetail(), T0 + HOUR)])).changes).toBe(1)
    const before = await one(`SELECT * FROM sets WHERE id = 'fr:swsh3'`)
    expect(before).toMatchObject({
      series_id: 'fr:swsh',
      name: 'Ténèbres Embrasées',
      release_date: '2020-08-14',
      logo_url: 'https://assets.tcgdex.net/fr/swsh/swsh3/logo',
      metadata_json: '{"id":"swsh3"}',
      content_hash: 'set-v1',
      detail_fetched_at: iso(T0 + HOUR),
      created_at: NOW,
      updated_at: iso(T0 + HOUR),
    })

    expect(await written(db, [upsertSetDetailStmt(db, 'fr', setDetail(), T0 + DAY)])).toEqual({ rowsWritten: 0, changes: 0 })
    expect(await one(`SELECT * FROM sets WHERE id = 'fr:swsh3'`)).toEqual(before)

    expect((await written(db, [upsertSetDetailStmt(db, 'fr', setDetail({ name: 'Ténèbres Embrasées (v2)', total: 4, hash: 'set-v2' }), T0 + 2 * DAY)])).changes).toBe(1)
    expect(await one(`SELECT name, total_cards, content_hash, created_at, updated_at FROM sets WHERE id = 'fr:swsh3'`)).toEqual({
      name: 'Ténèbres Embrasées (v2)',
      total_cards: 4,
      content_hash: 'set-v2',
      created_at: NOW,
      updated_at: iso(T0 + 2 * DAY),
    })

    // La liste officielle ne réécrit pas une extension déjà connue.
    await db.batch(insertSetBriefsStmts(db, 'fr', [setBrief('swsh3', { name: 'Ancien nom' })], T0 + 3 * DAY))
    expect((await one<{ name: string }>(`SELECT name FROM sets WHERE id = 'fr:swsh3'`)).name).toBe('Ténèbres Embrasées (v2)')

    // Sans série dans le détail : series_id reste NULL (pas de « fr: » orphelin).
    await db.batch([upsertSetDetailStmt(db, 'fr', setDetail({ id: 'promo', seriesTcgdexId: null, hash: 'promo-v1' }), T0)])
    expect((await one<{ series_id: string | null }>(`SELECT series_id FROM sets WHERE id = 'fr:promo'`)).series_id).toBeNull()
  })

  it('upsertCardBriefsStmts : rejouer n’écrit rien, une donnée modifiée, retirée ou déplacée met à jour', async () => {
    const db = newDb()
    await seedSets(db, 'fr', 'swsh', ['swsh3', 'swsh4'])
    const a = brief('swsh3-136', { image: 'https://assets.tcgdex.net/fr/swsh/swsh3/136' })
    const b = brief('swsh3-137')
    expect((await written(db, upsertCardBriefsStmts(db, 'fr', 'swsh3', 'swsh', [a, b], T0))).changes).toBe(2)
    expect(await one(`SELECT * FROM cards WHERE id = 'fr:swsh3-136'`)).toMatchObject({
      tcgdex_id: 'swsh3-136',
      language: 'fr',
      name: 'Carte swsh3-136',
      local_id: '136',
      set_id: 'fr:swsh3',
      series_id: 'swsh',
      api_image_url: 'https://assets.tcgdex.net/fr/swsh/swsh3/136',
      brief_hash: 'brief-swsh3-136-v1',
      image_status: 'pending',
      detail_status: 'pending',
      removed_at: null,
      created_at: NOW,
      updated_at: NOW,
    })
    const before = await rows(`SELECT * FROM cards ORDER BY id`)

    // Même donnée : rien n'est écrit, même sans série connue (la série existante est conservée).
    expect(await written(db, upsertCardBriefsStmts(db, 'fr', 'swsh3', 'swsh', [a, b], T0 + DAY))).toEqual({ rowsWritten: 0, changes: 0 })
    expect(await written(db, upsertCardBriefsStmts(db, 'fr', 'swsh3', null, [a, b], T0 + DAY))).toEqual({ rowsWritten: 0, changes: 0 })
    expect(await rows(`SELECT * FROM cards ORDER BY id`)).toEqual(before)

    // Carte modifiée : seule celle-ci est réécrite.
    const b2 = brief('swsh3-137', { name: 'Nouveau nom', hash: 'brief-swsh3-137-v2' })
    expect((await written(db, upsertCardBriefsStmts(db, 'fr', 'swsh3', 'swsh', [a, b2], T0 + 2 * DAY))).changes).toBe(1)
    expect(await one(`SELECT name, brief_hash, updated_at FROM cards WHERE id = 'fr:swsh3-137'`)).toEqual({
      name: 'Nouveau nom',
      brief_hash: 'brief-swsh3-137-v2',
      updated_at: iso(T0 + 2 * DAY),
    })
    expect((await one<{ updated_at: string }>(`SELECT updated_at FROM cards WHERE id = 'fr:swsh3-136'`)).updated_at).toBe(NOW)

    // Carte retirée puis de nouveau listée : la marque disparaît.
    await db.batch([markRemovedStmt(db, ['fr:swsh3-136'], T0 + 3 * DAY)])
    expect((await one<{ removed_at: string | null }>(`SELECT removed_at FROM cards WHERE id = 'fr:swsh3-136'`)).removed_at).toBe(iso(T0 + 3 * DAY))
    expect((await written(db, upsertCardBriefsStmts(db, 'fr', 'swsh3', 'swsh', [a], T0 + 4 * DAY))).changes).toBe(1)
    expect((await one<{ removed_at: string | null }>(`SELECT removed_at FROM cards WHERE id = 'fr:swsh3-136'`)).removed_at).toBeNull()

    // Carte listée par une autre extension (même empreinte) : rattachement mis à jour.
    expect((await written(db, upsertCardBriefsStmts(db, 'fr', 'swsh4', 'swsh', [a], T0 + 5 * DAY))).changes).toBe(1)
    expect((await one<{ set_id: string }>(`SELECT set_id FROM cards WHERE id = 'fr:swsh3-136'`)).set_id).toBe('fr:swsh4')
  })

  it('upsertCardDetailStmt garde l’extension qui liste la carte (pl4-AR* listée dans pl3)', async () => {
    const db = newDb()
    await seedSets(db, 'fr', 'pl', ['pl3', 'pl4'])
    await db.batch(upsertCardBriefsStmts(db, 'fr', 'pl3', 'pl', [brief('pl4-AR1', { localId: 'AR1', image: 'https://assets.tcgdex.net/fr/pl/pl3/AR1' })], T0))

    // Le détail annonce pl4 et ne donne pas d'image : l'extension et l'image de la liste restent.
    await db.batch([upsertCardDetailStmt(db, 'fr', cardDetail(), T0 + HOUR)])
    expect(await one(`SELECT * FROM cards WHERE id = 'fr:pl4-AR1'`)).toMatchObject({
      set_id: 'fr:pl3',
      series_id: 'pl',
      name: 'Arceus',
      rarity: 'Rare',
      category: 'Pokémon',
      illustrator: 'Illustrateur',
      api_image_url: 'https://assets.tcgdex.net/fr/pl/pl3/AR1',
      detail_hash: 'detail-v1',
      detail_status: 'fetched',
      detail_fetched_at: iso(T0 + HOUR),
      metadata_json: '{"id":"pl4-AR1"}',
      brief_hash: 'brief-pl4-AR1-v1',
      created_at: NOW,
    })

    // Une carte connue seulement par son détail prend l'extension du détail.
    await db.batch([upsertCardDetailStmt(db, 'fr', cardDetail({ id: 'pl4-AR2', localId: 'AR2' }), T0 + HOUR)])
    expect(await one(`SELECT set_id, series_id, detail_status FROM cards WHERE id = 'fr:pl4-AR2'`)).toEqual({ set_id: 'fr:pl4', series_id: 'pl', detail_status: 'fetched' })

    // Sans extension dans le détail : pas de rattachement inventé.
    await db.batch([upsertCardDetailStmt(db, 'fr', cardDetail({ id: 'orphan-1', localId: '1', setTcgdexId: null, seriesTcgdexId: null }), T0)])
    expect((await one<{ set_id: string | null }>(`SELECT set_id FROM cards WHERE id = 'fr:orphan-1'`)).set_id).toBeNull()
  })

  it('imageFailureStmt ne touche jamais une carte dont l’image est déjà valide', async () => {
    const db = newDb()
    await seedSets(db, 'fr', 'swsh', ['swsh3'])
    const ids = ['swsh3-1', 'swsh3-2', 'swsh3-3', 'swsh3-4']
    await db.batch(upsertCardBriefsStmts(db, 'fr', 'swsh3', 'swsh', ids.map((id) => brief(id)), T0))
    await db.batch([
      imageSuccessStmt(db, 'fr:swsh3-1', success('downloaded', 'cards/fr/swsh3/swsh3-1.webp'), T0),
      imageSuccessStmt(db, 'fr:swsh3-2', success('recovered_from_cdn', 'cards/fr/swsh3/swsh3-2.webp'), T0),
      imageSuccessStmt(db, 'fr:swsh3-3', success('already_present', 'cards/fr/swsh3/swsh3-3.webp'), T0),
    ])
    const valid = await rows(`SELECT * FROM cards WHERE id IN ('fr:swsh3-1', 'fr:swsh3-2', 'fr:swsh3-3') ORDER BY id`)
    expect(valid.map((row) => row.image_status)).toEqual(['downloaded', 'recovered_from_cdn', 'already_present'])

    for (const status of ['not_found_on_cdn', 'missing_api_url', 'download_error', 'invalid_image', 'pending'] as const) {
      const result = await written(
        db,
        ['fr:swsh3-1', 'fr:swsh3-2', 'fr:swsh3-3'].map((id) => imageFailureStmt(db, id, failure(status), T0 + DAY)),
      )
      expect(result.changes, status).toBe(0)
    }
    expect(await rows(`SELECT * FROM cards WHERE id IN ('fr:swsh3-1', 'fr:swsh3-2', 'fr:swsh3-3') ORDER BY id`)).toEqual(valid)

    // Une carte sans image valide enregistre bien l'échec, puis un échec plus récent.
    await db.batch([imageFailureStmt(db, 'fr:swsh3-4', failure('not_found_on_cdn'), T0 + HOUR)])
    expect(await one(`SELECT image_status, image_failure_reason, image_tested_urls, image_http_status, image_checked_at FROM cards WHERE id = 'fr:swsh3-4'`)).toEqual({
      image_status: 'not_found_on_cdn',
      image_failure_reason: 'introuvable sur le CDN',
      image_tested_urls: '[{"url":"u","status":404}]',
      image_http_status: 404,
      image_checked_at: iso(T0 + HOUR),
    })
    await db.batch([imageFailureStmt(db, 'fr:swsh3-4', failure('download_error', 'HTTP 500'), T0 + 2 * HOUR)])
    expect((await one<{ image_status: string }>(`SELECT image_status FROM cards WHERE id = 'fr:swsh3-4'`)).image_status).toBe('download_error')
    // Une réussite efface le motif d'échec.
    await db.batch([imageSuccessStmt(db, 'fr:swsh3-4', success('downloaded', 'cards/fr/swsh3/swsh3-4.webp', 512), T0 + 3 * HOUR)])
    expect(await one(`SELECT image_status, image_failure_reason, image_file_size, image_r2_key FROM cards WHERE id = 'fr:swsh3-4'`)).toEqual({
      image_status: 'downloaded',
      image_failure_reason: null,
      image_file_size: 512,
      image_r2_key: 'cards/fr/swsh3/swsh3-4.webp',
    })
  })

  it('imageFailureStmt garde l’historique : URL testées si la nouvelle liste est « [] », code HTTP si le nouveau est NULL', async () => {
    const db = newDb()
    await seedSets(db, 'fr', 'swsh', ['swsh3'])
    await db.batch(upsertCardBriefsStmts(db, 'fr', 'swsh3', 'swsh', [brief('swsh3-1'), brief('swsh3-2')], T0))
    const state = () => one(`SELECT image_status, image_failure_reason, image_tested_urls, image_http_status, image_checked_at FROM cards WHERE id = 'fr:swsh3-1'`)

    await db.batch([imageFailureStmt(db, 'fr:swsh3-1', failure('not_found_on_cdn'), T0 + HOUR)])
    // Échec définitif posé par l'exécuteur (tâche d'image épuisée) : ni URL ni code HTTP propres.
    await db.batch([imageFailureStmt(db, 'fr:swsh3-1', { status: 'download_error', reason: 'stockage R2 indisponible', testedUrls: '[]', httpStatus: null }, T0 + 2 * HOUR)])
    expect(await state()).toEqual({
      image_status: 'download_error',
      image_failure_reason: 'stockage R2 indisponible',
      image_tested_urls: '[{"url":"u","status":404}]',
      image_http_status: 404,
      image_checked_at: iso(T0 + 2 * HOUR),
    })

    // De nouvelles valeurs remplacent l'historique, chacune indépendamment.
    await db.batch([imageFailureStmt(db, 'fr:swsh3-1', { status: 'download_error', reason: 'HTTP 500', testedUrls: '[{"url":"v","status":500}]', httpStatus: 500 }, T0 + 3 * HOUR)])
    expect(await state()).toMatchObject({ image_tested_urls: '[{"url":"v","status":500}]', image_http_status: 500 })
    await db.batch([imageFailureStmt(db, 'fr:swsh3-1', { status: 'download_error', reason: 'HTTP 503', testedUrls: '[]', httpStatus: 503 }, T0 + 4 * HOUR)])
    expect(await state()).toMatchObject({ image_failure_reason: 'HTTP 503', image_tested_urls: '[{"url":"v","status":500}]', image_http_status: 503 })
    await db.batch([imageFailureStmt(db, 'fr:swsh3-1', { status: 'invalid_image', reason: 'contenu tronqué', testedUrls: '[{"url":"w","status":200}]', httpStatus: null }, T0 + 5 * HOUR)])
    expect(await state()).toEqual({
      image_status: 'invalid_image',
      image_failure_reason: 'contenu tronqué',
      image_tested_urls: '[{"url":"w","status":200}]',
      image_http_status: 503,
      image_checked_at: iso(T0 + 5 * HOUR),
    })

    // Carte jamais testée : rien à garder, les colonnes restent NULL (pas de « [] » inventé).
    await db.batch([imageFailureStmt(db, 'fr:swsh3-2', { status: 'missing_api_url', reason: 'aucune URL', testedUrls: '[]', httpStatus: null }, T0)])
    expect(await one(`SELECT image_status, image_tested_urls, image_http_status FROM cards WHERE id = 'fr:swsh3-2'`)).toEqual({
      image_status: 'missing_api_url',
      image_tested_urls: null,
      image_http_status: null,
    })
  })

  it('insertOrphanBriefsStmts crée les cartes de l’index sans extension ni brief_hash, sans écraser une carte connue', async () => {
    const db = newDb()
    await seedSets(db, 'fr', 'swsh', ['swsh3'])
    await db.batch(upsertCardBriefsStmts(db, 'fr', 'swsh3', 'swsh', [brief('swsh3-136', { image: 'https://assets.tcgdex.net/fr/swsh/swsh3/136' })], T0))
    const known = await one(`SELECT * FROM cards WHERE id = 'fr:swsh3-136'`)
    const orphans = [
      { id: 'pl4-AR1', localId: 'AR1', name: 'Arceus', image: 'https://assets.tcgdex.net/fr/pl/pl4/AR1' },
      { id: 'promo-1', localId: '1', name: 'Promo', image: null },
      // Déjà connue par la liste de son extension : laissée telle quelle.
      { id: 'swsh3-136', localId: '136', name: 'Autre nom', image: null },
    ]
    expect((await written(db, insertOrphanBriefsStmts(db, 'fr', orphans, T0 + HOUR))).changes).toBe(2)
    expect(await one(`SELECT * FROM cards WHERE id = 'fr:pl4-AR1'`)).toMatchObject({
      tcgdex_id: 'pl4-AR1',
      language: 'fr',
      name: 'Arceus',
      local_id: 'AR1',
      set_id: null,
      series_id: null,
      api_image_url: 'https://assets.tcgdex.net/fr/pl/pl4/AR1',
      brief_hash: null,
      detail_hash: null,
      detail_status: 'pending',
      image_status: 'pending',
      removed_at: null,
      created_at: iso(T0 + HOUR),
      updated_at: iso(T0 + HOUR),
    })
    expect(await one(`SELECT set_id, brief_hash, api_image_url FROM cards WHERE id = 'fr:promo-1'`)).toEqual({ set_id: null, brief_hash: null, api_image_url: null })
    expect(await one(`SELECT * FROM cards WHERE id = 'fr:swsh3-136'`)).toEqual(known)

    // Rejouer n'écrit rien ; le même id japonais est une autre carte.
    expect(await written(db, insertOrphanBriefsStmts(db, 'fr', orphans, T0 + DAY))).toEqual({ rowsWritten: 0, changes: 0 })
    await db.batch(insertOrphanBriefsStmts(db, 'ja', orphans.slice(0, 1), T0))
    expect(await rows(`SELECT id, set_id FROM cards WHERE tcgdex_id = 'pl4-AR1' ORDER BY id`)).toEqual([
      { id: 'fr:pl4-AR1', set_id: null },
      { id: 'ja:pl4-AR1', set_id: null },
    ])

    // Le détail de la carte orpheline lui donne son extension ; l'image de l'index reste si le détail n'en donne pas.
    await seedSets(db, 'fr', 'pl', ['pl4'])
    await db.batch([upsertCardDetailStmt(db, 'fr', cardDetail(), T0 + 2 * HOUR)])
    expect(await one(`SELECT set_id, series_id, api_image_url, detail_status, brief_hash FROM cards WHERE id = 'fr:pl4-AR1'`)).toEqual({
      set_id: 'fr:pl4',
      series_id: 'pl',
      api_image_url: 'https://assets.tcgdex.net/fr/pl/pl4/AR1',
      detail_status: 'fetched',
      brief_hash: null,
    })

    // Gros volumes : découpés en plusieurs requêtes.
    const many = Array.from({ length: 850 }, (_, i) => ({ id: `idx-${i}`, localId: String(i), name: `Carte ${i}`, image: null }))
    const statements = insertOrphanBriefsStmts(db, 'ja', many, T0)
    expect(statements.length).toBeGreaterThan(1)
    await db.batch(statements)
    expect((await one<{ n: number }>(`SELECT COUNT(*) AS n FROM cards WHERE language = 'ja' AND tcgdex_id LIKE 'idx-%' AND set_id IS NULL AND brief_hash IS NULL`)).n).toBe(850)
  })
})

// ——— File des tâches ———

describe('file des tâches : enqueueStmts', () => {
  it('crée les tâches avec priorité, tentatives, date de départ, synchronisation et charge utile', async () => {
    const db = newDb()
    const runId = await createSyncRun(db, 'fr', 'initial', null, T0)
    await db.batch(
      enqueueStmts(db, 'card', 'fr', [newJob('card', 'fr', 'swsh3-136', { payload: { orphan: true } }), newJob('card', 'fr', 'swsh3-137')], {
        runId,
        maxAttempts: 4,
        nowMs: T0,
        notBeforeMs: T0 + 5 * MIN,
      }),
    )
    await db.batch(enqueueStmts(db, 'report', null, [{ target: null, cardId: null, dedupeKey: dedupeKey('report', null, null) }], { runId: null, maxAttempts: 5, nowMs: T0 }))

    expect(await jobRow('card:fr:swsh3-136')).toMatchObject({
      job_type: 'card',
      language: 'fr',
      target_id: 'swsh3-136',
      card_id: 'fr:swsh3-136',
      priority: 5,
      status: 'pending',
      attempts: 0,
      max_attempts: 4,
      next_attempt_at: iso(T0 + 5 * MIN),
      locked_until: null,
      lock_token: null,
      run_id: runId,
      payload_json: '{"orphan":true}',
      created_at: NOW,
      updated_at: NOW,
      completed_at: null,
    })
    expect((await jobRow('card:fr:swsh3-137')).payload_json).toBeNull()
    expect(await jobRow('report:*:*')).toMatchObject({ job_type: 'report', language: null, target_id: null, priority: 3, next_attempt_at: NOW })
  })

  it('n’écrase pas une tâche en attente ou en cours (tentatives et verrou conservés)', async () => {
    const db = newDb()
    await enqueue(db, 'card', 'fr', ['a', 'b'], { maxAttempts: 5 })
    const [a, b] = await claim(db, 'exec-A', T0, 2)
    if (!a || !b) throw new Error('deux tâches attendues')
    // b échoue une fois : de nouveau en attente, avec une tentative consommée.
    await db.batch([retryStmt(db, b, T0, T0 + 30 * MIN, 'HTTP 500', 500)])
    const runId = await createSyncRun(db, 'fr', 'weekly', null, T0 + MIN)

    await enqueue(db, 'card', 'fr', ['a', 'b'], { runId, maxAttempts: 7, nowMs: T0 + MIN })
    expect(await countOf('import_jobs')).toBe(2)
    expect(await jobById(a.id)).toMatchObject({
      status: 'running',
      attempts: 1,
      lock_token: 'exec-A',
      locked_until: iso(T0 + LEASE),
      run_id: runId,
      max_attempts: 7,
    })
    expect(await jobById(b.id)).toMatchObject({
      status: 'pending',
      attempts: 1,
      next_attempt_at: iso(T0 + 30 * MIN),
      last_error: 'HTTP 500',
      run_id: runId,
    })
  })

  it('remet à zéro une tâche terminée, en échec ou annulée', async () => {
    const db = newDb()
    await enqueue(db, 'image', 'fr', ['x', 'y', 'z'])
    const jobs = await claim(db, 'exec-A')
    const [x, y, z] = jobs
    if (!x || !y || !z) throw new Error('trois tâches attendues')
    await db.batch([completeStmt(db, x, T0 + MIN, 200), failStmt(db, y, T0 + MIN, 'HTTP 404', 404)])
    await env.DB.prepare(`UPDATE import_jobs SET status = 'cancelled', locked_until = NULL, lock_token = NULL, last_error = 'annulée' WHERE id = ?1`).bind(z.id).run()

    await enqueue(db, 'image', 'fr', ['x', 'y', 'z'], { nowMs: T0 + HOUR, notBeforeMs: T0 + 2 * HOUR })
    const after = await rows<JobRow>(`SELECT * FROM import_jobs ORDER BY id`)
    expect(after.map((row) => row.id)).toEqual([x.id, y.id, z.id])
    for (const row of after) {
      expect(row, row.dedupe_key).toMatchObject({
        status: 'pending',
        attempts: 0,
        last_error: null,
        completed_at: null,
        next_attempt_at: iso(T0 + 2 * HOUR),
        created_at: NOW,
        updated_at: iso(T0 + HOUR),
      })
    }
  })

  it('un producteur sans synchronisation ne détache jamais une tâche en attente ou en cours de la sienne', async () => {
    const db = newDb()
    const runId = await createSyncRun(db, 'fr', 'initial', null, T0)
    await enqueue(db, 'image', 'fr', ['cours', 'finie', 'echec'], { runId })
    const claimed = await claim(db, 'exec-A', T0)
    const byTarget = new Map(claimed.map((job) => [job.target_id, job]))
    const [cours, finie, echec] = ['cours', 'finie', 'echec'].map((target) => byTarget.get(target))
    if (!cours || !finie || !echec) throw new Error('trois tâches attendues')
    await db.batch([completeStmt(db, finie, T0 + MIN, 200), failStmt(db, echec, T0 + MIN, 'HTTP 404', 404)])
    await enqueue(db, 'image', 'fr', ['attente'], { runId, notBeforeMs: T0 + DAY })

    // Relance manuelle ou audit (`runId` nul) sur les quatre tâches.
    await enqueue(db, 'image', 'fr', ['cours', 'attente', 'finie', 'echec'], { runId: null, nowMs: T0 + HOUR })
    expect(await rows(`SELECT target_id, status, run_id, attempts FROM import_jobs ORDER BY target_id`)).toEqual([
      { target_id: 'attente', status: 'pending', run_id: runId, attempts: 0 },
      { target_id: 'cours', status: 'running', run_id: runId, attempts: 1 },
      // Une tâche close repart de zéro, détachée de l'ancienne synchronisation.
      { target_id: 'echec', status: 'pending', run_id: null, attempts: 0 },
      { target_id: 'finie', status: 'pending', run_id: null, attempts: 0 },
    ])

    // Une nouvelle synchronisation, elle, rattache toutes les tâches.
    await db.batch([cancelSyncRunsStmt(db, 'fr', T0 + 2 * HOUR)])
    const next = await createSyncRun(db, 'fr', 'weekly', null, T0 + 2 * HOUR)
    await enqueue(db, 'image', 'fr', ['cours', 'attente', 'finie', 'echec'], { runId: next, nowMs: T0 + 2 * HOUR })
    expect((await rows<{ run_id: number }>(`SELECT DISTINCT run_id FROM import_jobs`)).map((row) => row.run_id)).toEqual([next])
    expect((await jobById(cours.id)).status).toBe('running')
  })

  it('releases : conservé pour une tâche en attente ou en cours, remis à zéro avec attempts pour une tâche close', async () => {
    const db = newDb()
    await enqueue(db, 'image', 'fr', ['rendue', 'close'])
    // Deux tours : chaque tâche est rendue (429) une fois, puis « rendue » l'est encore et « close » aboutit.
    for (const job of await claim(db, 'exec-A', T0)) await db.batch([releaseStmt(db, job, T0, T0, 'HTTP 429', 429)])
    for (const job of await claim(db, 'exec-B', T0)) {
      await db.batch([job.target_id === 'rendue' ? releaseStmt(db, job, T0, T0 + MIN, 'HTTP 429', 429) : completeStmt(db, job, T0, 200)])
    }
    expect(await rows(`SELECT target_id, status, attempts, releases FROM import_jobs ORDER BY target_id`)).toEqual([
      { target_id: 'close', status: 'done', attempts: 1, releases: 1 },
      { target_id: 'rendue', status: 'pending', attempts: 0, releases: 2 },
    ])

    await enqueue(db, 'image', 'fr', ['rendue', 'close'], { nowMs: T0 + HOUR, notBeforeMs: T0 + 2 * HOUR })
    expect(await rows(`SELECT target_id, status, attempts, releases, next_attempt_at FROM import_jobs ORDER BY target_id`)).toEqual([
      { target_id: 'close', status: 'pending', attempts: 0, releases: 0, next_attempt_at: iso(T0 + 2 * HOUR) },
      { target_id: 'rendue', status: 'pending', attempts: 0, releases: 2, next_attempt_at: iso(T0 + MIN) },
    ])

    // En cours : ni la tentative ni les rendus ne sont effacés.
    const running = await claimOne(db, 'exec-C', T0 + HOUR)
    expect(running).toMatchObject({ target_id: 'rendue', attempts: 1, releases: 2 })
    await enqueue(db, 'image', 'fr', ['rendue'], { nowMs: T0 + HOUR + MIN })
    expect(await jobById(running.id)).toMatchObject({ status: 'running', attempts: 1, releases: 2, lock_token: 'exec-C' })

    // Tâche annulée : remise à zéro complète, elle aussi.
    await env.DB.prepare(`UPDATE import_jobs SET status = 'cancelled', locked_until = NULL, lock_token = NULL WHERE id = ?1`).bind(running.id).run()
    await enqueue(db, 'image', 'fr', ['rendue'], { nowMs: T0 + 2 * HOUR })
    expect(await jobById(running.id)).toMatchObject({ status: 'pending', attempts: 0, releases: 0 })
  })

  it('accepte plus de 500 tâches en un appel (découpage en plusieurs requêtes)', async () => {
    const db = newDb()
    const targets = Array.from({ length: 1201 }, (_, i) => `swsh3-${i}`)
    const statements = enqueueStmts(db, 'card', 'fr', targets.map((target) => newJob('card', 'fr', target)), { runId: null, maxAttempts: 5, nowMs: T0 })
    expect(statements).toHaveLength(3)
    await db.batch(statements)
    expect(await one(`SELECT COUNT(*) AS n, COUNT(DISTINCT dedupe_key) AS keys, MIN(target_id) AS first FROM import_jobs`)).toEqual({ n: 1201, keys: 1201, first: 'swsh3-0' })
    // Rejouer ne crée aucun doublon.
    await enqueue(db, 'card', 'fr', targets, { nowMs: T0 + HOUR })
    expect(await countOf('import_jobs')).toBe(1201)
  })
})

describe('file des tâches : claimJobs', () => {
  it('réserve par priorité puis par next_attempt_at', async () => {
    const db = newDb()
    await enqueue(db, 'card', 'fr', ['c1'], { notBeforeMs: T0 - 30 * MIN })
    await enqueue(db, 'image', 'fr', ['i1'], { notBeforeMs: T0 - 20 * MIN })
    await enqueue(db, 'set', 'fr', ['s1'], { notBeforeMs: T0 - 10 * MIN })
    await enqueue(db, 'catalog', 'fr', ['cat'], { notBeforeMs: T0 })

    expect((await claim(db, 'A', T0, 2)).map((job) => job.job_type)).toEqual(['catalog', 'set'])
    expect((await claim(db, 'B', T0, 10)).map((job) => job.job_type)).toEqual(['image', 'card'])

    // À priorité égale, la tâche prête depuis le plus longtemps passe d'abord (même créée après).
    await enqueue(db, 'card', 'ja', ['tardive'], { notBeforeMs: T0 - MIN })
    await enqueue(db, 'card', 'ja', ['ancienne'], { notBeforeMs: T0 - 10 * MIN })
    expect((await claimOne(db, 'C')).target_id).toBe('ancienne')
    expect((await claimOne(db, 'D')).target_id).toBe('tardive')
  })

  it('ignore les tâches dont la date de départ n’est pas atteinte', async () => {
    const db = newDb()
    await enqueue(db, 'card', 'fr', ['maintenant'])
    await enqueue(db, 'card', 'fr', ['plus-tard'], { notBeforeMs: T0 + 10 * MIN })
    expect((await claim(db, 'A', T0)).map((job) => job.target_id)).toEqual(['maintenant'])
    expect(await claim(db, 'A', T0 + 10 * MIN - 1)).toEqual([])
    expect((await claim(db, 'B', T0 + 10 * MIN)).map((job) => job.target_id)).toEqual(['plus-tard'])
  })

  it('incrémente attempts et pose le verrou de l’exécution', async () => {
    const db = newDb()
    const runId = await createSyncRun(db, 'fr', 'manual', null, T0)
    await db.batch(enqueueStmts(db, 'card', 'fr', [newJob('card', 'fr', 'swsh3-136', { payload: { orphan: true } })], { runId, maxAttempts: 3, nowMs: T0 }))
    const job = await claimOne(db, 'exec-A', T0 + MIN)
    expect(job).toEqual({
      id: expect.any(Number),
      job_type: 'card',
      language: 'fr',
      target_id: 'swsh3-136',
      card_id: 'fr:swsh3-136',
      priority: 5,
      attempts: 1,
      max_attempts: 3,
      releases: 0,
      run_id: runId,
      payload_json: '{"orphan":true}',
      lock_token: 'exec-A',
    })
    expect(await jobById(job.id)).toMatchObject({ status: 'running', attempts: 1, lock_token: 'exec-A', locked_until: iso(T0 + MIN + LEASE), updated_at: iso(T0 + MIN) })
    // Une tâche en cours n'est pas réservée une seconde fois.
    expect(await claim(db, 'exec-B', T0 + 2 * MIN)).toEqual([])
  })

  it('ne réserve rien quand l’importation est en pause', async () => {
    const db = newDb()
    await enqueue(db, 'card', 'fr', ['a', 'b'])
    await db.batch([setStateStmt(db, STATE.paused, '1', T0)])
    expect(await claim(db, 'A')).toEqual([])
    expect(await rows(`SELECT status, attempts FROM import_jobs ORDER BY id`)).toEqual([
      { status: 'pending', attempts: 0 },
      { status: 'pending', attempts: 0 },
    ])
    // Reprise comme `resume()` (src/services/sync.ts) : la valeur est effacée.
    await db.batch([setStateStmt(db, STATE.paused, null, T0 + MIN)])
    expect(await claim(db, 'A', T0 + MIN, 1)).toHaveLength(1)
    await db.batch([setStateStmt(db, STATE.paused, '0', T0 + MIN)])
    expect(await claim(db, 'A', T0 + MIN)).toHaveLength(1)
  })

  it('ne réserve rien pendant une attente imposée (backoff_until futur), reprend à son terme ou quand elle est levée', async () => {
    const db = newDb()
    await enqueue(db, 'card', 'fr', ['a', 'b', 'c'])
    // Attente imposée par un 429 (comme l'exécuteur : date ISO dans backoff_until).
    await db.batch([setStateStmt(db, STATE.backoffUntil, iso(T0 + 10 * MIN), T0), setStateStmt(db, STATE.backoffReason, 'HTTP 429', T0)])
    expect(await claim(db, 'A', T0)).toEqual([])
    expect(await claim(db, 'A', T0 + 10 * MIN - 1)).toEqual([])
    expect(await rows(`SELECT status, attempts FROM import_jobs ORDER BY id`)).toEqual([
      { status: 'pending', attempts: 0 },
      { status: 'pending', attempts: 0 },
      { status: 'pending', attempts: 0 },
    ])
    // Terme atteint : la réservation reprend.
    expect(await claim(db, 'B', T0 + 10 * MIN, 1)).toHaveLength(1)

    // Nouvelle attente, levée comme `resumeSync()` (valeur effacée) : la réservation reprend aussitôt.
    await db.batch([setStateStmt(db, STATE.backoffUntil, iso(T0 + DAY), T0 + 10 * MIN)])
    expect(await claim(db, 'C', T0 + 10 * MIN)).toEqual([])
    await db.batch([setStateStmt(db, STATE.backoffUntil, null, T0 + 10 * MIN)])
    expect(await claim(db, 'C', T0 + 10 * MIN, 1)).toHaveLength(1)

    // Une attente passée (non effacée) ne bloque rien ; le motif seul non plus.
    await db.batch([setStateStmt(db, STATE.backoffUntil, iso(T0), T0 + 10 * MIN)])
    expect(await claim(db, 'D', T0 + 10 * MIN)).toHaveLength(1)
    expect(await countOf('import_jobs')).toBe(3)
    expect((await one<{ n: number }>(`SELECT COUNT(*) AS n FROM import_jobs WHERE status = 'running'`)).n).toBe(3)
  })

  it('deux réservations concurrentes ne se recouvrent jamais', async () => {
    for (let round = 0; round < 5; round++) {
      await resetAll()
      const db = newDb()
      await enqueue(db, 'image', 'fr', Array.from({ length: 30 }, (_, i) => `swsh3-${i}`))
      const [a, b] = await Promise.all([claimJobs(new Db(env.DB), { limit: 20, token: 'exec-A', nowMs: T0, leaseMs: LEASE }), claimJobs(new Db(env.DB), { limit: 20, token: 'exec-B', nowMs: T0, leaseMs: LEASE })])
      const idsA = new Set(a.map((job) => job.id))
      const idsB = new Set(b.map((job) => job.id))
      expect([...idsA].filter((id) => idsB.has(id)), `tour ${round}`).toEqual([])
      expect(idsA.size + idsB.size, `tour ${round}`).toBe(30)
      expect(a.length + b.length).toBe(30)
      // En base, chaque tâche porte le jeton de celui qui l'a réservée, et une seule tentative.
      const stored = await rows<{ id: number; lock_token: string; attempts: number; status: string }>(`SELECT id, lock_token, attempts, status FROM import_jobs`)
      for (const row of stored) {
        expect(row.status).toBe('running')
        expect(row.attempts).toBe(1)
        expect(row.lock_token).toBe(idsA.has(row.id) ? 'exec-A' : 'exec-B')
      }
    }
  })
})

describe('file des tâches : verrous et clôture', () => {
  it('reclaimExpired remet en attente un verrou expiré, ou fait échouer la tâche à court de tentatives', async () => {
    const db = newDb()
    await enqueue(db, 'card', 'fr', ['epuisee'], { maxAttempts: 1 })
    await enqueue(db, 'card', 'fr', ['reprise'], { maxAttempts: 3 })
    await enqueue(db, 'card', 'fr', ['attente'], { notBeforeMs: T0 + DAY })
    const claimed = await claim(db, 'exec-A', T0)
    expect(claimed).toHaveLength(2)
    const exhausted = claimed.find((job) => job.target_id === 'epuisee') as Job
    const resumed = claimed.find((job) => job.target_id === 'reprise') as Job

    // Verrou encore valide : rien n'est repris.
    expect(await reclaimExpired(db, T0 + LEASE - 1)).toEqual([])
    expect((await jobById(resumed.id)).status).toBe('running')

    const failed = await reclaimExpired(db, T0 + LEASE)
    expect(failed).toEqual([expect.objectContaining({ id: exhausted.id, job_type: 'card', card_id: 'fr:epuisee' })])
    expect(await jobById(exhausted.id)).toMatchObject({
      status: 'failed',
      attempts: 1,
      completed_at: iso(T0 + LEASE),
      locked_until: null,
      lock_token: null,
      last_error: expect.stringContaining('nombre maximal de tentatives'),
    })
    expect(await jobById(resumed.id)).toMatchObject({
      status: 'pending',
      attempts: 1,
      completed_at: null,
      next_attempt_at: iso(T0 + LEASE),
      locked_until: null,
      lock_token: null,
      last_error: expect.stringContaining('exécution interrompue'),
    })
    // La tâche en attente n'est pas touchée ; la tâche reprise compte sa deuxième tentative.
    expect((await jobRow('card:fr:attente')).last_error).toBeNull()
    const again = await claimOne(db, 'exec-B', T0 + LEASE)
    expect(again).toMatchObject({ id: resumed.id, attempts: 2, lock_token: 'exec-B' })
    expect(await reclaimExpired(db, T0 + LEASE)).toEqual([])
  })

  it('guardStmt : un batch écrit avec un jeton périmé est entièrement annulé ; avec le bon jeton tout passe', async () => {
    const db = newDb()
    await enqueue(db, 'catalog', 'fr', ['series'])
    const stale = await claimOne(db, 'exec-A', T0)
    // L'exécution A a perdu son verrou : la tâche est reprise puis réservée par B.
    await reclaimExpired(db, T0 + LEASE)
    const current = await claimOne(db, 'exec-B', T0 + LEASE)
    expect(current.id).toBe(stale.id)

    const writesOf = (job: Job, nowMs: number) => [guardStmt(db, job), ...upsertSeriesStmts(db, 'fr', [seriesRow('swsh')], nowMs), completeStmt(db, job, nowMs, 200)]
    await expect(db.batch(writesOf(stale, T0 + LEASE + MIN))).rejects.toThrow(/CHECK constraint failed/)
    expect(await countOf('series')).toBe(0)
    expect(await countOf('job_lock_guard')).toBe(0)
    expect(await jobById(current.id)).toMatchObject({ status: 'running', lock_token: 'exec-B', completed_at: null })

    await db.batch(writesOf(current, T0 + LEASE + 2 * MIN))
    expect(await countOf('series')).toBe(1)
    expect(await countOf('job_lock_guard')).toBe(0)
    expect(await jobById(current.id)).toMatchObject({ status: 'done', lock_token: null, completed_at: iso(T0 + LEASE + 2 * MIN) })

    // Une tâche déjà close ne peut plus rien écrire, même avec son dernier jeton.
    await expect(db.batch([guardStmt(db, current), ...upsertSeriesStmts(db, 'fr', [seriesRow('sm')], T0 + DAY)])).rejects.toThrow(/CHECK constraint failed/)
    expect(await countOf('series')).toBe(1)
  })

  it('completeStmt clôt la tâche et libère son verrou ; sans effet avec un autre jeton', async () => {
    const db = newDb()
    await enqueue(db, 'set', 'fr', ['swsh3'])
    const job = await claimOne(db, 'exec-A')
    expect((await written(db, [completeStmt(db, { ...job, lock_token: 'exec-X' }, T0 + MIN, 200)])).changes).toBe(0)
    expect((await jobById(job.id)).status).toBe('running')

    expect((await written(db, [completeStmt(db, job, T0 + MIN, 200)])).changes).toBe(1)
    expect(await jobById(job.id)).toMatchObject({
      status: 'done',
      attempts: 1,
      last_error: null,
      last_http_status: 200,
      completed_at: iso(T0 + MIN),
      updated_at: iso(T0 + MIN),
      locked_until: null,
      lock_token: null,
    })
  })

  it('failStmt fait échouer définitivement la tâche', async () => {
    const db = newDb()
    await enqueue(db, 'card', 'fr', ['swsh3-999'])
    const job = await claimOne(db, 'exec-A')
    await db.batch([failStmt(db, job, T0 + MIN, 'carte introuvable', 404)])
    expect(await jobById(job.id)).toMatchObject({ status: 'failed', last_error: 'carte introuvable', last_http_status: 404, completed_at: iso(T0 + MIN), lock_token: null })
    expect(await claim(db, 'exec-B', T0 + DAY)).toEqual([])
  })

  it('retryStmt remet en attente à la date donnée en gardant la tentative consommée', async () => {
    const db = newDb()
    await enqueue(db, 'card', 'fr', ['swsh3-136'])
    const job = await claimOne(db, 'exec-A')
    await db.batch([retryStmt(db, job, T0 + MIN, T0 + 5 * MIN, 'HTTP 500', 500)])
    expect(await jobById(job.id)).toMatchObject({
      status: 'pending',
      attempts: 1,
      next_attempt_at: iso(T0 + 5 * MIN),
      last_error: 'HTTP 500',
      last_http_status: 500,
      updated_at: iso(T0 + MIN),
      locked_until: null,
      lock_token: null,
      completed_at: null,
    })
    expect(await claim(db, 'exec-B', T0 + 5 * MIN - 1)).toEqual([])
    expect((await claimOne(db, 'exec-B', T0 + 5 * MIN)).attempts).toBe(2)
  })

  it('releaseStmt rend la tâche sans consommer de tentative et compte le rendu', async () => {
    const db = newDb()
    await enqueue(db, 'image', 'fr', ['swsh3-136'], { maxAttempts: 2 })
    // Trois réservations rendues de suite (HTTP 429) n'épuisent pas les deux tentatives ; chaque rendu est compté.
    let nowMs = T0
    for (let i = 0; i < 3; i++) {
      const job = await claimOne(db, `exec-${i}`, nowMs)
      expect(job).toMatchObject({ attempts: 1, releases: i })
      await db.batch([releaseStmt(db, job, nowMs, nowMs + 2 * MIN, 'HTTP 429', 429)])
      expect(await jobById(job.id)).toMatchObject({
        status: 'pending',
        attempts: 0,
        releases: i + 1,
        next_attempt_at: iso(nowMs + 2 * MIN),
        last_error: 'HTTP 429',
        last_http_status: 429,
        lock_token: null,
        locked_until: null,
      })
      nowMs += 2 * MIN
    }
    // Un rendu avec un jeton périmé n'a aucun effet (ni statut, ni compteur de rendus).
    const job = await claimOne(db, 'exec-z', nowMs)
    expect((await written(db, [releaseStmt(db, { ...job, lock_token: 'exec-X' }, nowMs, nowMs, 'HTTP 429', 429)])).changes).toBe(0)
    expect(await jobById(job.id)).toMatchObject({ status: 'running', attempts: 1, releases: 3, lock_token: 'exec-z' })
    // Le compteur de tentatives ne descend jamais sous zéro.
    await env.DB.prepare(`UPDATE import_jobs SET attempts = 0 WHERE id = ?1`).bind(job.id).run()
    await db.batch([releaseStmt(db, job, nowMs, nowMs, 'différée', null)])
    expect(await jobById(job.id)).toMatchObject({ attempts: 0, releases: 4 })
    // Les autres clôtures ne touchent pas au compteur de rendus.
    const again = await claimOne(db, 'exec-y', nowMs)
    await db.batch([retryStmt(db, again, nowMs, nowMs, 'HTTP 500', 500)])
    expect((await jobById(job.id)).releases).toBe(4)
  })
})

// ——— État d'exécution ———

describe('état d’exécution (runtime_state)', () => {
  it('acquireRunnerLease : le premier gagne, le second est refusé, reprise possible après expiration', async () => {
    const db = newDb()
    const RUNNER = 6 * MIN
    expect(await acquireRunnerLease(db, 'A', T0, RUNNER)).toBe(true)
    expect(await acquireRunnerLease(db, 'B', T0 + MIN, RUNNER)).toBe(false)
    expect(await one(`SELECT value, expires_at FROM runtime_state WHERE key = ?1`, STATE.runner)).toEqual({ value: 'A', expires_at: iso(T0 + RUNNER) })

    // B ne peut pas libérer le verrou de A.
    await db.batch([releaseRunnerLeaseStmt(db, 'B', T0 + 2 * MIN)])
    expect(await one(`SELECT value, expires_at FROM runtime_state WHERE key = ?1`, STATE.runner)).toEqual({ value: 'A', expires_at: iso(T0 + RUNNER) })
    expect(await acquireRunnerLease(db, 'C', T0 + 3 * MIN, RUNNER)).toBe(false)

    // Verrou expiré : C le prend.
    expect(await acquireRunnerLease(db, 'C', T0 + RUNNER, RUNNER)).toBe(true)
    // A, en retard, ne libère pas le verrou de C.
    await db.batch([releaseRunnerLeaseStmt(db, 'A', T0 + RUNNER + MIN)])
    expect(await acquireRunnerLease(db, 'D', T0 + RUNNER + 2 * MIN, RUNNER)).toBe(false)
    // C libère son propre verrou : D le prend aussitôt.
    await db.batch([releaseRunnerLeaseStmt(db, 'C', T0 + RUNNER + 3 * MIN)])
    expect(await acquireRunnerLease(db, 'D', T0 + RUNNER + 3 * MIN, RUNNER)).toBe(true)
    expect((await one<{ value: string }>(`SELECT value FROM runtime_state WHERE key = ?1`, STATE.runner)).value).toBe('D')
  })

  it('acquireRunnerLease : deux acquisitions simultanées, un seul gagnant', async () => {
    for (let round = 0; round < 5; round++) {
      await resetAll()
      const won = await Promise.all(['A', 'B', 'C'].map((token) => acquireRunnerLease(new Db(env.DB), token, T0, 6 * MIN)))
      expect(won.filter(Boolean), `tour ${round}`).toHaveLength(1)
      const winner = ['A', 'B', 'C'][won.indexOf(true)]
      expect((await one<{ value: string }>(`SELECT value FROM runtime_state WHERE key = ?1`, STATE.runner)).value).toBe(winner)
    }
  })

  it('renewRunnerLeaseStmt prolonge le verrou seulement s’il est encore à nous (meta.changes = 0 sinon)', async () => {
    const db = newDb()
    const RUNNER = 6 * MIN
    const lease = () => one(`SELECT value, expires_at, updated_at FROM runtime_state WHERE key = ?1`, STATE.runner)
    const renew = async (token: string, nowMs: number) => (await db.batch([renewRunnerLeaseStmt(db, token, nowMs, RUNNER)]))[0]?.meta.changes

    // Sans verrou : rien n'est prolongé, rien n'est créé.
    expect(await renew('A', T0)).toBe(0)
    expect(await countOf('runtime_state')).toBe(0)

    expect(await acquireRunnerLease(db, 'A', T0, RUNNER)).toBe(true)
    expect(await renew('A', T0 + 2 * MIN)).toBe(1)
    expect(await lease()).toEqual({ value: 'A', expires_at: iso(T0 + 8 * MIN), updated_at: iso(T0 + 2 * MIN) })
    // B ne prolonge pas le verrou de A.
    expect(await renew('B', T0 + 3 * MIN)).toBe(0)
    expect(await lease()).toEqual({ value: 'A', expires_at: iso(T0 + 8 * MIN), updated_at: iso(T0 + 2 * MIN) })
    // Grâce au renouvellement, le verrou tient au-delà de sa première échéance.
    expect(await acquireRunnerLease(db, 'C', T0 + 7 * MIN, RUNNER)).toBe(false)

    // À l'échéance prolongée, C le prend ; A, qui croit encore le tenir, ne peut plus le prolonger.
    expect(await acquireRunnerLease(db, 'C', T0 + 8 * MIN, RUNNER)).toBe(true)
    expect(await renew('A', T0 + 9 * MIN)).toBe(0)
    expect(await lease()).toEqual({ value: 'C', expires_at: iso(T0 + 14 * MIN), updated_at: iso(T0 + 8 * MIN) })

    // Dans le batch d'un lot (avec d'autres écritures), un renouvellement refusé n'annule rien :
    // c'est son `meta.changes` que l'exécuteur lit pour s'arrêter.
    const results = await db.batch([addDailyOperationsStmt(db, 4, T0 + 10 * MIN), renewRunnerLeaseStmt(db, 'A', T0 + 10 * MIN, RUNNER)])
    expect(results.map((result) => result.meta.changes)).toEqual([1, 0])
    expect(dailyOperations(await readState(db, T0 + 10 * MIN), T0 + 10 * MIN)).toBe(4)
    expect((await lease()).value).toBe('C')
  })

  it('addDailyOperationsStmt cumule les opérations du jour UTC', async () => {
    const db = newDb()
    await db.batch([addDailyOperationsStmt(db, 5, T0)])
    await db.batch([addDailyOperationsStmt(db, 7, T0 + HOUR), addDailyOperationsStmt(db, 100_000, T0 + 2 * HOUR)])
    // Le lendemain (UTC) : un nouveau compteur.
    const nextDay = Date.parse('2026-11-14T00:00:00.000Z')
    await db.batch([addDailyOperationsStmt(db, 3, nextDay)])

    expect(await rows(`SELECT key, value FROM runtime_state WHERE key LIKE 'ops:%' ORDER BY key`)).toEqual([
      { key: 'ops:2026-11-13', value: '100012' },
      { key: 'ops:2026-11-14', value: '3' },
    ])
    expect(dailyOperations(await readState(db, T0), T0)).toBe(100_012)
    expect(dailyOperations(await readState(db, nextDay), nextDay)).toBe(3)
    expect(dailyOperations(await readState(db, nextDay + DAY), nextDay + DAY)).toBe(0)
  })
})

// ——— Db et Usage ———

describe('Db et Usage', () => {
  it('comptent chaque requête D1, un batch comptant pour une seule', async () => {
    const usage = new Usage()
    const db = new Db(env.DB, usage)
    expect(newDb().usage).toBeInstanceOf(Usage)
    expect(newDb().usage.d1Queries).toBe(0)

    const prepared = [db.stmt(`SELECT 1`), db.stmt(`SELECT ?1 AS v`, 'x'), db.stmt(`SELECT ?1 AS v`, 2)]
    expect(usage.d1Queries).toBe(0)
    expect(await db.all<{ x: number }>(`SELECT ?1 AS x`, 1)).toEqual([{ x: 1 }])
    expect(await db.first<{ x: string }>(`SELECT ?1 AS x`, 'a')).toEqual({ x: 'a' })
    await db.run(`INSERT INTO runtime_state (key, value, updated_at) VALUES ('k', 'v', ?1)`, NOW)
    expect(usage.d1Queries).toBe(3)
    expect(await db.batch(prepared)).toHaveLength(3)
    expect(usage.d1Queries).toBe(4)
    expect(await db.batch([])).toEqual([])
    expect(usage.d1Queries).toBe(4)

    // Les dépôts comptent aussi : une réservation ou une prise de verrou = une requête.
    await enqueue(db, 'card', 'fr', ['a', 'b', 'c'])
    expect(usage.d1Queries).toBe(5)
    await claim(db, 'exec-A')
    await acquireRunnerLease(db, 'exec-A', T0, 6 * MIN)
    expect(usage.d1Queries).toBe(7)

    usage.httpRequests = 2
    usage.r2Reads = 1
    usage.r2Writes = 3
    expect(usage.subrequests).toBe(7 + 2 + 1 + 3)
    expect(usage.operations).toBe(5)
  })
})

// ——— Synchronisations ———

describe('synchronisations (sync_runs)', () => {
  it('createSyncRunStmt crée une synchronisation running et rend son id ; rien si une autre court déjà pour la langue', async () => {
    const db = newDb()
    const [first] = await db.batch([createSyncRunStmt(db, 'fr', 'initial', null, T0)])
    const id = (first?.results[0] as { id: number } | undefined)?.id
    expect(id).toEqual(expect.any(Number))
    expect(await one(`SELECT * FROM sync_runs WHERE id = ?1`, id ?? -1)).toMatchObject({
      language: 'fr',
      kind: 'initial',
      scope_json: null,
      started_at: NOW,
      finished_at: null,
      status: 'running',
    })

    // Même langue pendant que la première court : aucune ligne, aucune erreur, aucun id rendu.
    const [second] = await db.batch([createSyncRunStmt(db, 'fr', 'manual', { sets: ['swsh3'] }, T0 + MIN)])
    expect(second?.results).toEqual([])
    expect(second?.meta.changes).toBe(0)
    expect(await countOf('sync_runs')).toBe(1)

    // Autre langue : créée, avec sa portée en JSON.
    const ja = await createSyncRun(db, 'ja', 'manual', { sets: ['SV1a'] }, T0 + MIN)
    expect(await one(`SELECT language, kind, scope_json, status FROM sync_runs WHERE id = ?1`, ja)).toEqual({
      language: 'ja',
      kind: 'manual',
      scope_json: '{"sets":["SV1a"]}',
      status: 'running',
    })

    // Une fois la première close, une nouvelle peut démarrer ; une portée absente est NULL.
    await db.batch([cancelSyncRunsStmt(db, 'fr', T0 + 2 * MIN)])
    const next = await createSyncRun(db, 'fr', 'weekly', undefined, T0 + 3 * MIN)
    expect(next).toBeGreaterThan(id ?? Infinity)
    expect(await one(`SELECT kind, scope_json, started_at, status FROM sync_runs WHERE id = ?1`, next)).toEqual({
      kind: 'weekly',
      scope_json: null,
      started_at: iso(T0 + 3 * MIN),
      status: 'running',
    })
  })

  it('createSyncRunStmt est atomique avec sa tâche catalog : la tâche voit la synchronisation, un échec annule les deux', async () => {
    const db = newDb()
    // Tâche `catalog` rattachée à la synchronisation créée juste avant dans le même batch (comme `startSync`).
    const catalogJob = (language: string) =>
      db.stmt(
        `INSERT INTO import_jobs (job_type, language, dedupe_key, priority, max_attempts, next_attempt_at, run_id, created_at, updated_at)
         SELECT 'catalog', ?1, 'catalog:' || ?1 || ':*', 0, 5, ?2, run.id, ?2, ?2
         FROM sync_runs run WHERE run.language = ?1 AND run.status = 'running'`,
        language,
        NOW,
      )
    const results = await db.batch([createSyncRunStmt(db, 'fr', 'initial', null, T0), catalogJob('fr')])
    const runId = (results[0]?.results[0] as { id: number } | undefined)?.id
    expect((await jobRow('catalog:fr:*')).run_id).toBe(runId)

    // Une écriture refusée dans le même batch : ni synchronisation, ni tâche.
    await expect(db.batch([createSyncRunStmt(db, 'ja', 'initial', null, T0), catalogJob('ja'), db.stmt(`INSERT INTO job_lock_guard (ok) VALUES (0)`)])).rejects.toThrow(
      /CHECK constraint failed/,
    )
    expect(await rows(`SELECT language FROM sync_runs`)).toEqual([{ language: 'fr' }])
    expect(await countOf('import_jobs')).toBe(1)
  })

  it('createSyncRunStmt : des démarrages simultanés pour une langue ne créent qu’une synchronisation', async () => {
    for (let round = 0; round < 5; round++) {
      await resetAll()
      const outcomes = await Promise.allSettled(
        (['initial', 'weekly', 'manual'] as const).map(async (kind) => {
          const db = new Db(env.DB)
          const [result] = await db.batch([createSyncRunStmt(db, 'fr', kind, null, T0)])
          return result?.results.length ?? 0
        }),
      )
      // Chaque démarrage perdant n'insère rien (ou est refusé par l'index unique) ; un seul gagne.
      const created = outcomes.filter((outcome) => outcome.status === 'fulfilled' && outcome.value === 1)
      expect(created, `tour ${round}`).toHaveLength(1)
      for (const outcome of outcomes) if (outcome.status === 'rejected') expect(String(outcome.reason)).toMatch(/UNIQUE/)
      expect(await rows(`SELECT language, status FROM sync_runs`), `tour ${round}`).toEqual([{ language: 'fr', status: 'running' }])
    }
  })

  it('cancelSyncRunsStmt abandonne les synchronisations running de la langue démarrées avant la date donnée', async () => {
    const db = newDb()
    await insertRaw('sync_runs', { language: 'fr', kind: 'initial', started_at: iso(T0 - 30 * DAY), finished_at: iso(T0 - 29 * DAY), status: 'completed' })
    const fr = await createSyncRun(db, 'fr', 'weekly', null, T0)
    const ja = await createSyncRun(db, 'ja', 'weekly', null, T0)
    const changes = async (nowMs: number, startedBeforeMs?: number) => (await db.batch([cancelSyncRunsStmt(db, 'fr', nowMs, startedBeforeMs)]))[0]?.meta.changes

    // Démarrée depuis moins de 6 jours : pas jugée bloquée.
    expect(await changes(T0 + 5 * DAY, T0 - DAY)).toBe(0)
    expect((await one<{ status: string }>(`SELECT status FROM sync_runs WHERE id = ?1`, fr)).status).toBe('running')

    // Bloquée depuis plus de 6 jours : abandonnée, avec sa date de fin.
    expect(await changes(T0 + 7 * DAY, T0 + DAY)).toBe(1)
    expect(await one(`SELECT status, finished_at FROM sync_runs WHERE id = ?1`, fr)).toEqual({ status: 'cancelled', finished_at: iso(T0 + 7 * DAY) })
    // L'autre langue et la synchronisation déjà close ne sont pas touchées ; rejouer ne change rien.
    expect(await one(`SELECT status, finished_at FROM sync_runs WHERE id = ?1`, ja)).toEqual({ status: 'running', finished_at: null })
    expect(await one(`SELECT status, finished_at FROM sync_runs WHERE status = 'completed'`)).toEqual({ status: 'completed', finished_at: iso(T0 - 29 * DAY) })
    expect(await changes(T0 + 8 * DAY)).toBe(0)

    // Sans date limite (abandon sur demande) : toute synchronisation en cours, même démarrée à l'instant.
    const now = await createSyncRun(db, 'fr', 'manual', null, T0 + 9 * DAY)
    expect(await changes(T0 + 9 * DAY)).toBe(1)
    expect(await one(`SELECT status, finished_at FROM sync_runs WHERE id = ?1`, now)).toEqual({ status: 'cancelled', finished_at: iso(T0 + 9 * DAY) })
  })

  it('hasCompletedRun : seule une synchronisation complète (sans portée) aboutie compte', async () => {
    const db = newDb()
    const complete = (runId: number, nowMs: number) => db.batch([completeSyncRunStmt(db, runId, ZERO_COUNTS, nowMs)])
    expect(await hasCompletedRun(db, 'fr')).toBe(false)

    // En cours, puis abandonnée : non.
    await createSyncRun(db, 'fr', 'initial', null, T0)
    expect(await hasCompletedRun(db, 'fr')).toBe(false)
    await db.batch([cancelSyncRunsStmt(db, 'fr', T0 + HOUR)])
    expect(await hasCompletedRun(db, 'fr')).toBe(false)

    // Aboutie mais limitée à quelques extensions : non.
    await complete(await createSyncRun(db, 'fr', 'manual', { sets: ['swsh3'] }, T0 + 2 * HOUR), T0 + 3 * HOUR)
    expect(await one(`SELECT status, scope_json FROM sync_runs WHERE kind = 'manual'`)).toEqual({ status: 'completed', scope_json: '{"sets":["swsh3"]}' })
    expect(await hasCompletedRun(db, 'fr')).toBe(false)

    // Complète et aboutie : oui, pour cette langue seulement.
    await complete(await createSyncRun(db, 'fr', 'initial', null, T0 + 4 * HOUR), T0 + 5 * HOUR)
    expect(await hasCompletedRun(db, 'fr')).toBe(true)
    expect(await hasCompletedRun(db, 'ja')).toBe(false)
  })
})

// ——— Exécutions ———

describe('exécutions (tick_runs)', () => {
  it('insertTickRunStmt enregistre tous les compteurs, dont jobs_released et jobs_lost, et compte sa propre requête', async () => {
    const db = newDb()
    const usage = new Usage()
    usage.httpRequests = 12
    usage.d1Queries = 7
    usage.r2Writes = 3
    usage.bytesStored = 4096
    const tick: TickSummary = {
      trigger: 'cron',
      startedMs: T0,
      finishedMs: T0 + 30_000,
      claimed: 9,
      done: 3,
      retried: 2,
      failed: 1,
      released: 2,
      lost: 1,
      stopReason: 'plus de tâche prête',
      error: null,
    }
    await db.batch([insertTickRunStmt(db, tick, usage)])
    expect(await one(`SELECT * FROM tick_runs`)).toEqual({
      id: expect.any(Number),
      trigger: 'cron',
      started_at: NOW,
      finished_at: iso(T0 + 30_000),
      jobs_claimed: 9,
      jobs_done: 3,
      jobs_retried: 2,
      jobs_failed: 1,
      jobs_released: 2,
      jobs_lost: 1,
      http_requests: 12,
      // La requête d'insertion elle-même est comptée.
      d1_queries: 8,
      bytes_stored: 4096,
      stop_reason: 'plus de tâche prête',
      error: null,
    })

    await db.batch([insertTickRunStmt(db, { ...tick, trigger: 'admin', released: 0, lost: 4, error: 'verrou de l’exécuteur perdu' }, new Usage())])
    expect(await one(`SELECT trigger, jobs_released, jobs_lost, http_requests, d1_queries, error FROM tick_runs WHERE trigger = 'admin'`)).toEqual({
      trigger: 'admin',
      jobs_released: 0,
      jobs_lost: 4,
      http_requests: 0,
      d1_queries: 1,
      error: 'verrou de l’exécuteur perdu',
    })
  })
})

// ——— Bilans ———

describe('bilans (runs)', () => {
  it('languageCounts et storedBytes sur un petit jeu de données', async () => {
    const db = newDb()
    await db.batch([...upsertSeriesStmts(db, 'fr', [seriesRow('swsh'), seriesRow('ex')], T0)])
    await db.batch(insertSetBriefsStmts(db, 'fr', [setBrief('swsh3')], T0))
    await seedSets(db, 'ja', 'SV', ['SV1a'])
    const frIds = ['c1', 'c2', 'c3', 'c4', 'c5', 'c6', 'c7', 'c8', 'c9'].map((id) => `swsh3-${id}`)
    await db.batch([
      ...upsertCardBriefsStmts(db, 'fr', 'swsh3', 'swsh', frIds.map((id) => brief(id)), T0),
      ...upsertCardBriefsStmts(db, 'ja', 'SV1a', 'SV', [brief('SV1a-001'), brief('SV1a-002')], T0),
    ])
    await db.batch([
      imageSuccessStmt(db, 'fr:swsh3-c1', success('downloaded', 'k1', 300), T0),
      imageSuccessStmt(db, 'fr:swsh3-c2', success('recovered_from_cdn', 'k2', 310), T0),
      imageSuccessStmt(db, 'fr:swsh3-c3', success('already_present', 'k3', 320), T0),
      imageFailureStmt(db, 'fr:swsh3-c4', failure('missing_api_url'), T0),
      imageFailureStmt(db, 'fr:swsh3-c5', failure('not_found_on_cdn'), T0),
      imageFailureStmt(db, 'fr:swsh3-c6', failure('download_error'), T0),
      imageFailureStmt(db, 'fr:swsh3-c7', failure('invalid_image'), T0),
      // c8 reste en attente ; c9 a une image mais n'est plus listée par TCGdex.
      imageSuccessStmt(db, 'fr:swsh3-c9', success('downloaded', 'k9', 1000), T0),
      markRemovedStmt(db, ['fr:swsh3-c9'], T0),
      imageSuccessStmt(db, 'ja:SV1a-001', success('downloaded', 'kj1', 500), T0),
    ])

    expect(await languageCounts(db, 'fr')).toEqual({
      series_found: 2,
      sets_found: 1,
      cards_found: 8,
      cards_processed: 7,
      images_downloaded: 1,
      images_recovered: 1,
      images_already_present: 1,
      images_missing: 2,
      images_failed: 2,
      bytes_stored: 930,
    })
    expect(await languageCounts(db, 'ja')).toEqual({
      series_found: 1,
      sets_found: 1,
      cards_found: 2,
      cards_processed: 1,
      images_downloaded: 1,
      images_recovered: 0,
      images_already_present: 0,
      images_missing: 0,
      images_failed: 0,
      bytes_stored: 500,
    })
    // Le stockage compte toutes les images valides, y compris celle d'une carte retirée (elle reste dans R2).
    expect(await storedBytes(db)).toBe(300 + 310 + 320 + 1000 + 500)
  })

  it('languageCounts et storedBytes rendent des zéros sur une base vide', async () => {
    const db = newDb()
    expect(await languageCounts(db, 'ja')).toEqual({
      series_found: 0,
      sets_found: 0,
      cards_found: 0,
      cards_processed: 0,
      images_downloaded: 0,
      images_recovered: 0,
      images_already_present: 0,
      images_missing: 0,
      images_failed: 0,
      bytes_stored: 0,
    })
    expect(await storedBytes(db)).toBe(0)
  })
})
