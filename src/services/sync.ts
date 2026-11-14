/**
 * Commandes de synchronisation : démarrer (initiale, hebdomadaire ou manuelle), mettre en pause,
 * reprendre, abandonner, relancer les images manquantes et les tâches en échec, rattraper les petites
 * images, logos et symboles manquants.
 */

import type { Config, Language } from '../config'
import type { Db } from '../db'
import { ERROR_IMAGE_STATUSES, MISSING_IMAGE_STATUSES, type SetMediaKind } from '../repositories/catalog'
import { dedupeKey, enqueueMediaStmts, enqueueStmts, JOB_PRIORITY, type MediaKind } from '../repositories/jobs'
import { cancelSyncRunsStmt, createSyncRunStmt, hasCompletedRun, runningSyncRuns, type SyncKind } from '../repositories/runs'
import { readState, setStateStmt, STATE } from '../repositories/state'
import { iso } from '../utils/time'
import { imageJob, logoJob, lowImageJob, symbolJob, type SyncScope } from './catalog'

/** Une synchronisation encore « en cours » après ce délai est jugée bloquée et abandonnée. */
const STALE_RUN_MS = 6 * 86_400_000

export interface StartResult {
  started: { language: Language; runId: number; kind: SyncKind }[]
  skipped: { language: Language; reason: string }[]
}

/**
 * Démarre une synchronisation par langue : la synchronisation et sa tâche `catalog` sont créées dans
 * le même batch (une synchronisation n'existe jamais sans tâche, donc ne peut pas être close à vide),
 * et jamais deux à la fois pour une même langue. Une synchronisation bloquée depuis plus de 6 jours
 * est abandonnée au profit de la nouvelle.
 */
export async function startSync(
  db: Db,
  config: Config,
  nowMs: number,
  opts: { languages?: Language[]; kind?: SyncKind; scope?: SyncScope } = {},
): Promise<StartResult> {
  const running = await runningSyncRuns(db)
  const result: StartResult = { started: [], skipped: [] }
  for (const language of opts.languages?.length ? opts.languages : config.languages) {
    const current = running.find((run) => run.language === language)
    const stale = current !== undefined && nowMs - Date.parse(current.started_at) > STALE_RUN_MS
    if (current && !stale) {
      result.skipped.push({ language, reason: 'synchronisation déjà en cours' })
      continue
    }
    const kind = opts.kind ?? ((await hasCompletedRun(db, language)) ? 'manual' : 'initial')
    const scope = opts.scope?.sets?.length ? opts.scope : null
    const statements: D1PreparedStatement[] = []
    if (stale) statements.push(cancelSyncRunsStmt(db, language, nowMs, nowMs - STALE_RUN_MS))
    statements.push(
      createSyncRunStmt(db, language, kind, scope, nowMs),
      // Tâche `catalog` rattachée à la synchronisation créée juste avant, dans la même transaction,
      // et seulement si elle vient réellement d'être créée (`changes()` : un démarrage concurrent
      // refusé ne touche jamais à la tâche de la synchronisation déjà en cours).
      db.stmt(
        `INSERT INTO import_jobs (job_type, language, dedupe_key, priority, status, attempts, max_attempts,
           next_attempt_at, run_id, payload_json, created_at, updated_at)
         SELECT 'catalog', ?1, ?2, ?3, 'pending', 0, ?4, ?5, run.id, ?6, ?5, ?5
         FROM sync_runs run WHERE run.language = ?1 AND run.status = 'running' AND changes() = 1
         ON CONFLICT (dedupe_key) DO UPDATE SET run_id = excluded.run_id, payload_json = excluded.payload_json,
           max_attempts = excluded.max_attempts,
           attempts = CASE WHEN import_jobs.status IN ('done', 'failed', 'cancelled') THEN 0 ELSE import_jobs.attempts END,
           releases = CASE WHEN import_jobs.status IN ('done', 'failed', 'cancelled') THEN 0 ELSE import_jobs.releases END,
           last_error = CASE WHEN import_jobs.status IN ('done', 'failed', 'cancelled') THEN NULL ELSE import_jobs.last_error END,
           next_attempt_at = CASE WHEN import_jobs.status IN ('done', 'failed', 'cancelled') THEN excluded.next_attempt_at ELSE import_jobs.next_attempt_at END,
           completed_at = CASE WHEN import_jobs.status IN ('done', 'failed', 'cancelled') THEN NULL ELSE import_jobs.completed_at END,
           status = CASE WHEN import_jobs.status IN ('done', 'failed', 'cancelled') THEN 'pending' ELSE import_jobs.status END,
           updated_at = excluded.updated_at`,
        language,
        dedupeKey('catalog', language, null),
        JOB_PRIORITY.catalog,
        config.maxAttempts,
        iso(nowMs),
        JSON.stringify(scope ?? {}),
      ),
    )
    try {
      const results = await db.batch(statements)
      const created = results.at(-2)?.results?.[0] as { id: number } | undefined
      if (!created) {
        result.skipped.push({ language, reason: 'synchronisation déjà en cours' })
        continue
      }
      result.started.push({ language, runId: created.id, kind })
    } catch (error) {
      // Course avec un autre démarrage : l'index unique a refusé le doublon, rien n'est écrit.
      if (String(error).includes('UNIQUE')) result.skipped.push({ language, reason: 'synchronisation déjà en cours' })
      else throw error
    }
  }
  return result
}

/** Abandonne les synchronisations en cours (les tâches restent et seront traitées normalement). */
export async function cancelSync(db: Db, nowMs: number, languages: Language[]): Promise<{ cancelled: number }> {
  const results = await db.batch(languages.map((language) => cancelSyncRunsStmt(db, language, nowMs)))
  return { cancelled: results.reduce((sum, item) => sum + (item.meta.changes ?? 0), 0) }
}

export async function pauseSync(db: Db, nowMs: number, reason = 'pause manuelle'): Promise<void> {
  await db.batch([setStateStmt(db, STATE.paused, '1', nowMs), setStateStmt(db, STATE.pauseReason, reason, nowMs)])
}

/**
 * Reprend : lève la pause et l'éventuelle attente imposée (dérogation manuelle : la première réponse
 * 429 suivante la reposera aussitôt). Rend l'attente levée, pour qu'elle reste visible.
 */
export async function resumeSync(db: Db, nowMs: number): Promise<{ backoffLifted: string | null }> {
  const state = await readState(db, nowMs)
  const backoff = state.get(STATE.backoffUntil)?.value ?? null
  await db.batch([
    setStateStmt(db, STATE.paused, null, nowMs),
    setStateStmt(db, STATE.pauseReason, null, nowMs),
    setStateStmt(db, STATE.backoffUntil, null, nowMs),
    setStateStmt(db, STATE.backoffReason, null, nowMs),
  ])
  return { backoffLifted: backoff && Date.parse(backoff) > nowMs ? backoff : null }
}

/**
 * Relance la recherche des images manquantes ou en erreur (une tâche d'image par carte), remet en
 * attente toutes les tâches en échec (images comprises), et relance les cartes restées « en
 * attente » sans tâche d'image active (filet de sécurité). Les images valides ne sont jamais
 * retéléchargées.
 */
export async function retryMissing(db: Db, config: Config, nowMs: number, languages: Language[]): Promise<{ images: number; failedJobs: number }> {
  const statuses = [...MISSING_IMAGE_STATUSES, ...ERROR_IMAGE_STATUSES]
  let images = 0
  // D'abord les tâches en échec (leur nombre est lu sur cette requête, la première du batch).
  const statements: D1PreparedStatement[] = [
    db.stmt(
      `UPDATE import_jobs SET status = 'pending', attempts = 0, releases = 0, next_attempt_at = ?1, last_error = NULL,
         completed_at = NULL, run_id = NULL, updated_at = ?1
       WHERE status = 'failed' AND (language IS NULL OR language IN (SELECT value FROM json_each(?2)))`,
      iso(nowMs),
      JSON.stringify(languages),
    ),
  ]
  for (const language of languages) {
    const cards = await db.all<{ tcgdex_id: string }>(
      `SELECT c.tcgdex_id FROM cards c
       WHERE c.language = ?1 AND c.removed_at IS NULL
         AND (c.image_status IN (SELECT value FROM json_each(?2))
           OR (c.image_status = 'pending' AND NOT EXISTS (
             SELECT 1 FROM import_jobs j WHERE j.dedupe_key = 'image:' || c.language || ':' || c.tcgdex_id
               AND j.status IN ('pending', 'running', 'failed'))))`,
      language,
      JSON.stringify(statuses),
    )
    images += cards.length
    statements.push(...enqueueStmts(db, 'image', language, cards.map((card) => imageJob(language, card.tcgdex_id)), { runId: null, maxAttempts: config.maxAttempts, nowMs }))
  }
  const results = await db.batch(statements)
  const failedJobs = results[0]?.meta.changes ?? 0
  return { images, failedJobs }
}

/** Taille d'une page de rattrapage (lignes lues puis tâches créées, en une requête chacune). */
const BACKFILL_PAGE = 500
/** Pages au plus par appel : ≤ 2 requêtes D1 par page, loin des 1 000 requêtes d'une invocation. */
const BACKFILL_MAX_PAGES = 200

export interface BackfillResult {
  /** Tâches de petite image créées ou remises en attente. */
  lowImages: number
  /** Tâches de logo créées ou remises en attente. */
  logos: number
  /** Tâches de symbole créées ou remises en attente. */
  symbols: number
  /** Faux si le plafond de pages a été atteint : rappeler la route pour la suite. */
  complete: boolean
}

const LOW_BACKFILL_SQL = `SELECT c.id, c.tcgdex_id FROM cards c
  WHERE c.language = ?1 AND c.id > ?2 AND c.removed_at IS NULL
    AND c.image_status IN ('downloaded', 'recovered_from_cdn', 'already_present') AND c.image_r2_key IS NOT NULL
    AND (c.image_low_status IS NULL OR (c.image_low_status != 'downloaded'
      AND (c.image_low_checked_at IS NULL OR c.image_low_checked_at < ?3)))
    AND NOT EXISTS (SELECT 1 FROM import_jobs j WHERE j.dedupe_key = 'image_low:' || c.language || ':' || c.tcgdex_id
      AND j.status IN ('pending', 'running'))
  ORDER BY c.id LIMIT ?4`

/**
 * Extensions dont le logo ou le symbole manque. `kind` vient d'une union fermée : il nomme les
 * colonnes (`logo_*`, `symbol_*`) et le préfixe de `dedupe_key` (`logo:`, `symbol:`).
 */
const setMediaBackfillSql = (kind: SetMediaKind): string => `SELECT s.id, s.tcgdex_id FROM sets s
  WHERE s.language = ?1 AND s.id > ?2 AND s.${kind}_url IS NOT NULL
    AND (s.${kind}_status IS NULL OR (s.${kind}_status != 'downloaded'
      AND (s.${kind}_checked_at IS NULL OR s.${kind}_checked_at < ?3 OR s.${kind}_source_url IS NOT s.${kind}_url || '.webp')))
    AND NOT EXISTS (SELECT 1 FROM import_jobs j WHERE j.dedupe_key = '${kind}:' || s.language || ':' || s.tcgdex_id
      AND j.status IN ('pending', 'running'))
  ORDER BY s.id LIMIT ?4`

const BACKFILL: Record<MediaKind, { sql: string; job: typeof logoJob; counter: 'lowImages' | 'logos' | 'symbols' }> = {
  low: { sql: LOW_BACKFILL_SQL, job: lowImageJob, counter: 'lowImages' },
  logo: { sql: setMediaBackfillSql('logo'), job: logoJob, counter: 'logos' },
  symbol: { sql: setMediaBackfillSql('symbol'), job: symbolJob, counter: 'symbols' },
}

/**
 * Rattrapage des copies secondaires (`POST /admin/sync/backfill-media`) : une tâche de petite image
 * pour chaque carte encore listée dont l'image principale est valide et la petite image pas encore
 * copiée (jamais traitée, ou absente / en erreur depuis plus de `missingRecheckDays` jours), et une
 * tâche de logo (de symbole) pour chaque extension dont l'API donne un logo (un symbole) pas encore
 * copié (même règle, ou URL changée). Les cibles qui ont déjà une tâche active sont ignorées :
 * rappeler la route ne crée rien de plus. Lecture par pages (pagination par identifiant), écriture
 * par `json_each` (un paramètre).
 */
export async function backfillMedia(db: Db, config: Config, nowMs: number, languages: Language[]): Promise<BackfillResult> {
  const cutoff = iso(nowMs - config.missingRecheckDays * 86_400_000)
  const result: BackfillResult = { lowImages: 0, logos: 0, symbols: 0, complete: true }
  let pages = 0
  const opts = { runId: null, maxAttempts: config.maxAttempts, nowMs }
  for (const language of languages) {
    for (const kind of ['low', 'logo', 'symbol'] as const) {
      const backfill = BACKFILL[kind]
      for (let after = ''; ; ) {
        if (pages >= BACKFILL_MAX_PAGES) return { ...result, complete: false }
        pages++
        const rows = await db.all<{ id: string; tcgdex_id: string }>(backfill.sql, language, after, cutoff, BACKFILL_PAGE)
        if (rows.length) {
          await db.batch(enqueueMediaStmts(db, language, rows.map((row) => backfill.job(language, row.tcgdex_id)), opts))
          result[backfill.counter] += rows.length
        }
        if (rows.length < BACKFILL_PAGE) break
        after = (rows[rows.length - 1] as { id: string }).id
      }
    }
  }
  return result
}
