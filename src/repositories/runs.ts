/**
 * Synchronisations du catalogue (`sync_runs`) et exécutions du traitement (`tick_runs`).
 */

import type { Language } from '../config'
import type { Db, Usage } from '../db'
import { iso } from '../utils/time'

export type SyncKind = 'initial' | 'weekly' | 'manual'

export interface SyncRun {
  id: number
  language: Language
  kind: SyncKind
  scope_json: string | null
  started_at: string
  finished_at: string | null
  status: 'running' | 'completed' | 'cancelled'
  series_found: number | null
  sets_found: number | null
  cards_found: number | null
  cards_processed: number | null
  images_downloaded: number | null
  images_recovered: number | null
  images_already_present: number | null
  images_missing: number | null
  images_failed: number | null
  bytes_stored: number | null
}

export function runningSyncRuns(db: Db): Promise<SyncRun[]> {
  return db.all<SyncRun>(`SELECT * FROM sync_runs WHERE status = 'running' ORDER BY id`)
}

/**
 * Crée une synchronisation, seulement si aucune n'est en cours pour cette langue (l'index unique
 * `sync_runs_one_running` le garantit aussi face à des appels concurrents). À exécuter dans le même
 * batch que la création de sa tâche `catalog` : une synchronisation n'existe jamais sans tâche.
 */
export function createSyncRunStmt(db: Db, language: Language, kind: SyncKind, scope: unknown, nowMs: number): D1PreparedStatement {
  return db.stmt(
    `INSERT INTO sync_runs (language, kind, scope_json, started_at, status)
     SELECT ?1, ?2, ?3, ?4, 'running'
     WHERE NOT EXISTS (SELECT 1 FROM sync_runs WHERE language = ?1 AND status = 'running')
     RETURNING id`,
    language,
    kind,
    scope === undefined || scope === null ? null : JSON.stringify(scope),
    iso(nowMs),
  )
}

/** Abandonne les synchronisations en cours d'une langue (bloquées, ou sur demande). */
export function cancelSyncRunsStmt(db: Db, language: Language, nowMs: number, startedBeforeMs?: number): D1PreparedStatement {
  return db.stmt(
    `UPDATE sync_runs SET status = 'cancelled', finished_at = ?2
     WHERE language = ?1 AND status = 'running' AND started_at <= ?3`,
    language,
    iso(nowMs),
    iso(startedBeforeMs ?? nowMs),
  )
}

/** Une synchronisation complète (non limitée à quelques extensions) a-t-elle déjà abouti ? */
export async function hasCompletedRun(db: Db, language: Language): Promise<boolean> {
  const row = await db.first<{ done: number }>(
    `SELECT EXISTS (SELECT 1 FROM sync_runs WHERE language = ?1 AND status = 'completed' AND scope_json IS NULL) AS done`,
    language,
  )
  return row?.done === 1
}

/** La synchronisation a-t-elle encore des tâches en attente ou en cours ? */
export async function syncRunHasActiveJobs(db: Db, runId: number): Promise<boolean> {
  const row = await db.first<{ active: number }>(
    `SELECT EXISTS (SELECT 1 FROM import_jobs WHERE run_id = ?1 AND status IN ('pending', 'running')) AS active`,
    runId,
  )
  return row?.active === 1
}

export interface LanguageCounts {
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

/** Bilan réel d'une langue, calculé sur les tables (cartes encore listées par TCGdex). */
export async function languageCounts(db: Db, language: Language): Promise<LanguageCounts> {
  const row = await db.first<LanguageCounts>(
    `SELECT
       (SELECT COUNT(*) FROM series WHERE language = ?1) AS series_found,
       (SELECT COUNT(*) FROM sets WHERE language = ?1) AS sets_found,
       COUNT(*) AS cards_found,
       COALESCE(SUM(image_status != 'pending'), 0) AS cards_processed,
       COALESCE(SUM(image_status = 'downloaded'), 0) AS images_downloaded,
       COALESCE(SUM(image_status = 'recovered_from_cdn'), 0) AS images_recovered,
       COALESCE(SUM(image_status = 'already_present'), 0) AS images_already_present,
       COALESCE(SUM(image_status IN ('missing_api_url', 'not_found_on_cdn')), 0) AS images_missing,
       COALESCE(SUM(image_status IN ('download_error', 'invalid_image')), 0) AS images_failed,
       COALESCE(SUM(CASE WHEN image_status IN ('downloaded', 'recovered_from_cdn', 'already_present')
         THEN image_file_size END), 0) AS bytes_stored
     FROM cards WHERE language = ?1 AND removed_at IS NULL`,
    language,
  )
  if (!row) throw new Error('bilan impossible')
  return row
}

export function completeSyncRunStmt(db: Db, runId: number, counts: LanguageCounts, nowMs: number): D1PreparedStatement {
  return db.stmt(
    `UPDATE sync_runs SET status = 'completed', finished_at = ?2, series_found = ?3, sets_found = ?4,
       cards_found = ?5, cards_processed = ?6, images_downloaded = ?7, images_recovered = ?8,
       images_already_present = ?9, images_missing = ?10, images_failed = ?11, bytes_stored = ?12
     WHERE id = ?1 AND status = 'running'`,
    runId,
    iso(nowMs),
    counts.series_found,
    counts.sets_found,
    counts.cards_found,
    counts.cards_processed,
    counts.images_downloaded,
    counts.images_recovered,
    counts.images_already_present,
    counts.images_missing,
    counts.images_failed,
    counts.bytes_stored,
  )
}

export function updateSyncRunFoundStmt(db: Db, runId: number, seriesFound: number, setsFound: number): D1PreparedStatement {
  return db.stmt(`UPDATE sync_runs SET series_found = ?2, sets_found = ?3 WHERE id = ?1`, runId, seriesFound, setsFound)
}

/**
 * Volume total stocké sous `cards/`, en octets (contrôle du plafond `MAX_STORED_BYTES`) : images
 * principales valides (index couvrant `cards_stored`), petites images copiées (index couvrant
 * `cards_low_stored` ; une petite image qui réutilise l'objet principal n'a pas de taille propre) et
 * logos copiés (table `sets`, quelques centaines de lignes).
 */
export async function storedBytes(db: Db): Promise<number> {
  const row = await db.first<{ bytes: number }>(
    `SELECT
       (SELECT COALESCE(SUM(image_file_size), 0) FROM cards
         WHERE image_status IN ('downloaded', 'recovered_from_cdn', 'already_present'))
     + (SELECT COALESCE(SUM(image_low_file_size), 0) FROM cards WHERE image_low_status = 'downloaded')
     + (SELECT COALESCE(SUM(logo_file_size), 0) FROM sets WHERE logo_status = 'downloaded') AS bytes`,
  )
  return row?.bytes ?? 0
}

export interface TickSummary {
  trigger: string
  startedMs: number
  finishedMs: number
  claimed: number
  done: number
  retried: number
  failed: number
  released: number
  lost: number
  stopReason: string
  error: string | null
}

export function insertTickRunStmt(db: Db, tick: TickSummary, usage: Usage): D1PreparedStatement {
  return db.stmt(
    `INSERT INTO tick_runs (trigger, started_at, finished_at, jobs_claimed, jobs_done, jobs_retried, jobs_failed,
       jobs_released, jobs_lost, http_requests, d1_queries, bytes_stored, stop_reason, error)
     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)`,
    tick.trigger,
    iso(tick.startedMs),
    iso(tick.finishedMs),
    tick.claimed,
    tick.done,
    tick.retried,
    tick.failed,
    tick.released,
    tick.lost,
    usage.httpRequests,
    usage.d1Queries + 1,
    usage.bytesStored,
    tick.stopReason,
    tick.error,
  )
}
