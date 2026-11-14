/**
 * File des tâches persistantes (table `import_jobs`).
 *
 * Cycle d'une tâche : `pending` → (réservation atomique) `running` → `done` | `failed` |
 * `pending` (nouvelle tentative différée). Chaque réservation incrémente `attempts` et pose un verrou
 * qui expire (`locked_until`) avec un jeton propre à l'exécution (`lock_token`). Une tâche dont le
 * verrou a expiré (exécution interrompue) est remise en attente, ou échoue définitivement si elle a
 * épuisé ses tentatives : jamais de boucle infinie.
 */

import type { Language } from '../config'
import type { Db, SqlValue } from '../db'
import { iso } from '../utils/time'

export const JOB_TYPES = ['catalog', 'set', 'card_index', 'card', 'image', 'report'] as const
export type JobType = (typeof JOB_TYPES)[number]

/**
 * Ordre de traitement : catalogue, extensions, index des cartes, rapport, puis les images avant les
 * détails de cartes. Comme un détail de carte crée la tâche de son image, les images sont copiées au
 * fil de l'eau au lieu d'attendre la fin de tous les détails. Les copies secondaires (petites images,
 * logos, symboles) passent après tout le reste : `MEDIA_PRIORITY`.
 */
export const JOB_PRIORITY: Record<JobType, number> = {
  catalog: 0,
  set: 1,
  card_index: 2,
  report: 3,
  image: 4,
  card: 5,
}

/**
 * Copies secondaires (petite image `low.webp` d'une carte, logo ou symbole d'une extension) : tâches
 * de type `image` (la contrainte CHECK de `job_type` n'est pas reconstruite), reconnues à leur charge
 * utile `{ media: 'low' | 'logo' | 'symbol' }` et à leur `dedupe_key` (`image_low:<langue>:<carte>`,
 * `logo:<langue>:<extension>`, `symbol:<langue>:<extension>`). Priorité la plus basse : après
 * toutes les autres tâches.
 */
export type MediaKind = 'low' | 'logo' | 'symbol'
export const MEDIA_PRIORITY = 6

const MEDIA_KINDS: readonly MediaKind[] = ['low', 'logo', 'symbol']

export const lowImageDedupeKey = (language: Language, cardTcgdexId: string): string => `image_low:${language}:${cardTcgdexId}`
export const logoDedupeKey = (language: Language, setTcgdexId: string): string => `logo:${language}:${setTcgdexId}`
export const symbolDedupeKey = (language: Language, setTcgdexId: string): string => `symbol:${language}:${setTcgdexId}`

/** Nature d'une copie secondaire, ou `null` pour toute autre tâche (dont l'image principale). */
export function mediaKind(job: { job_type: string; payload_json: string | null }): MediaKind | null {
  if (job.job_type !== 'image' || !job.payload_json) return null
  try {
    const media = (JSON.parse(job.payload_json) as { media?: unknown } | null)?.media
    return MEDIA_KINDS.find((kind) => kind === media) ?? null
  } catch {
    return null
  }
}

/**
 * Type affiché d'une tâche (statistiques, erreurs) : `image_low`, `logo` et `symbol` pour les copies
 * secondaires, sinon `job_type`. Expression SQL sur `import_jobs`.
 */
export const JOB_KIND_SQL = `CASE WHEN job_type = 'image' AND substr(dedupe_key, 1, 10) = 'image_low:' THEN 'image_low'
  WHEN job_type = 'image' AND substr(dedupe_key, 1, 5) = 'logo:' THEN 'logo'
  WHEN job_type = 'image' AND substr(dedupe_key, 1, 7) = 'symbol:' THEN 'symbol' ELSE job_type END`

export interface Job {
  id: number
  job_type: JobType
  language: Language | null
  target_id: string | null
  card_id: string | null
  priority: number
  attempts: number
  max_attempts: number
  /** Rendus sans consommer de tentative (429, dépendance pas prête). */
  releases: number
  run_id: number | null
  payload_json: string | null
  lock_token: string
}

export interface NewJob {
  /** Identifiant TCGdex de la cible (extension, carte), si elle en a un. */
  target: string | null
  /** Identifiant interne de la carte (`fr:swsh3-136`), pour les tâches de carte et d'image. */
  cardId: string | null
  dedupeKey: string
  payload?: unknown
}

export const dedupeKey = (type: JobType, language: Language | null, target: string | null): string =>
  [type, language ?? '*', target ?? '*'].join(':')

/**
 * Crée (ou remet en attente) des tâches d'un même type en une seule requête : les lignes passent
 * par un unique paramètre JSON (`json_each`), ce qui contourne la limite de 100 paramètres de D1.
 * Une tâche terminée ou en échec est remise à zéro ; une tâche en attente ou en cours est laissée
 * telle quelle (elle sera traitée) et n'est jamais détachée de sa synchronisation par un producteur
 * qui n'en a pas (`runId` nul : relance manuelle, audit).
 */
export function enqueueStmts(
  db: Db,
  type: JobType,
  language: Language | null,
  jobs: NewJob[],
  opts: { runId: number | null; maxAttempts: number; nowMs: number; notBeforeMs?: number; priority?: number },
): D1PreparedStatement[] {
  const statements: D1PreparedStatement[] = []
  // Blocs de 500 lignes : chaque paramètre reste loin de la limite de 2 Mo par valeur.
  for (let i = 0; i < jobs.length; i += 500) {
    const rows = jobs.slice(i, i + 500).map((job) => ({
      t: job.target,
      c: job.cardId,
      k: job.dedupeKey,
      p: job.payload === undefined ? null : JSON.stringify(job.payload),
    }))
    statements.push(
      db.stmt(
        `INSERT INTO import_jobs (job_type, language, target_id, card_id, dedupe_key, priority, status,
           attempts, max_attempts, next_attempt_at, run_id, payload_json, created_at, updated_at)
         SELECT ?1, ?2, json_extract(value, '$.t'), json_extract(value, '$.c'), json_extract(value, '$.k'),
           ?3, 'pending', 0, ?4, ?5, ?6, json_extract(value, '$.p'), ?7, ?7
         FROM json_each(?8) WHERE true
         ON CONFLICT (dedupe_key) DO UPDATE SET
           run_id = CASE WHEN import_jobs.status IN ('pending', 'running')
             THEN COALESCE(excluded.run_id, import_jobs.run_id) ELSE excluded.run_id END,
           payload_json = excluded.payload_json,
           max_attempts = excluded.max_attempts,
           attempts = CASE WHEN import_jobs.status IN ('done', 'failed', 'cancelled') THEN 0 ELSE import_jobs.attempts END,
           releases = CASE WHEN import_jobs.status IN ('done', 'failed', 'cancelled') THEN 0 ELSE import_jobs.releases END,
           last_error = CASE WHEN import_jobs.status IN ('done', 'failed', 'cancelled') THEN NULL ELSE import_jobs.last_error END,
           next_attempt_at = CASE WHEN import_jobs.status IN ('done', 'failed', 'cancelled') THEN excluded.next_attempt_at ELSE import_jobs.next_attempt_at END,
           completed_at = CASE WHEN import_jobs.status IN ('done', 'failed', 'cancelled') THEN NULL ELSE import_jobs.completed_at END,
           status = CASE WHEN import_jobs.status IN ('done', 'failed', 'cancelled') THEN 'pending' ELSE import_jobs.status END,
           updated_at = excluded.updated_at`,
        type,
        language,
        opts.priority ?? JOB_PRIORITY[type],
        opts.maxAttempts,
        iso(opts.notBeforeMs ?? opts.nowMs),
        opts.runId,
        iso(opts.nowMs),
        JSON.stringify(rows),
      ),
    )
  }
  return statements
}

/**
 * Remet en attente la tâche de petite image d'une carte, si elle existe et n'est plus active : celle
 * qui s'est terminée sans rien copier faute d'image principale (`runLowImageJob` ne l'attend pas).
 * Écrite avec la réussite de l'image principale ; sans tâche existante, rien (une carte sans petite
 * image demandée n'en reçoit pas).
 */
export function reviveLowImageStmt(db: Db, language: Language, cardTcgdexId: string, nowMs: number): D1PreparedStatement {
  return db.stmt(
    `UPDATE import_jobs SET status = 'pending', attempts = 0, releases = 0, last_error = NULL,
       next_attempt_at = ?2, completed_at = NULL, updated_at = ?2
     WHERE dedupe_key = ?1 AND status IN ('done', 'failed', 'cancelled')`,
    lowImageDedupeKey(language, cardTcgdexId),
    iso(nowMs),
  )
}

/** Crée (ou remet en attente) des copies secondaires : type `image`, priorité `MEDIA_PRIORITY`. */
export function enqueueMediaStmts(
  db: Db,
  language: Language,
  jobs: NewJob[],
  opts: { runId: number | null; maxAttempts: number; nowMs: number },
): D1PreparedStatement[] {
  return enqueueStmts(db, 'image', language, jobs, { ...opts, priority: MEDIA_PRIORITY })
}

/**
 * Remet en attente les tâches dont le verrou a expiré (exécution interrompue). Celles qui ont
 * épuisé leurs tentatives échouent définitivement. Rend les tâches passées en échec.
 */
export interface ExpiredJob {
  id: number
  job_type: JobType
  language: Language | null
  target_id: string | null
  card_id: string | null
  payload_json: string | null
}

export async function reclaimExpired(db: Db, nowMs: number): Promise<ExpiredJob[]> {
  const now = iso(nowMs)
  const rows = await db.all<ExpiredJob & { status: string }>(
    `UPDATE import_jobs SET
       status = CASE WHEN attempts >= max_attempts THEN 'failed' ELSE 'pending' END,
       last_error = CASE WHEN attempts >= max_attempts
         THEN 'verrou expiré : nombre maximal de tentatives atteint'
         ELSE 'verrou expiré : exécution interrompue' END,
       completed_at = CASE WHEN attempts >= max_attempts THEN ?1 ELSE NULL END,
       next_attempt_at = ?1, locked_until = NULL, lock_token = NULL, updated_at = ?1
     WHERE status = 'running' AND locked_until <= ?1
     RETURNING id, job_type, language, target_id, card_id, payload_json, status`,
    now,
  )
  return rows
    .filter((row) => row.status === 'failed')
    .map((row) => ({ id: row.id, job_type: row.job_type, language: row.language, target_id: row.target_id, card_id: row.card_id, payload_json: row.payload_json }))
}

/**
 * Réserve atomiquement jusqu'à `limit` tâches prêtes, par priorité. Une seule requête
 * `UPDATE … RETURNING` : deux exécutions concurrentes ne peuvent jamais réserver la même tâche.
 * Rien n'est réservé si l'importation est en pause ou en attente imposée (429, série d'erreurs).
 */
export async function claimJobs(db: Db, opts: { limit: number; token: string; nowMs: number; leaseMs: number }): Promise<Job[]> {
  const jobs = await db.all<Job>(
    `UPDATE import_jobs SET status = 'running', attempts = attempts + 1, locked_until = ?1,
       lock_token = ?2, updated_at = ?3
     WHERE id IN (
       SELECT id FROM import_jobs
       WHERE status = 'pending' AND next_attempt_at <= ?3
       ORDER BY priority, next_attempt_at, id
       LIMIT ?4
     )
     AND NOT EXISTS (SELECT 1 FROM runtime_state WHERE key = 'paused' AND value = '1')
     AND NOT EXISTS (SELECT 1 FROM runtime_state WHERE key = 'backoff_until' AND value > ?3)
     RETURNING id, job_type, language, target_id, card_id, priority, attempts, max_attempts, releases,
       run_id, payload_json, lock_token`,
    iso(opts.nowMs + opts.leaseMs),
    opts.token,
    iso(opts.nowMs),
    opts.limit,
  )
  return jobs.sort((a, b) => a.priority - b.priority || a.id - b.id)
}

/** Y a-t-il une tâche prête, ou un verrou expiré à récupérer ? (lecture seule, peu coûteuse) */
export async function hasWork(db: Db, nowMs: number): Promise<boolean> {
  const now = iso(nowMs)
  const row = await db.first<{ ready: number }>(
    `SELECT EXISTS (SELECT 1 FROM import_jobs WHERE status = 'pending' AND next_attempt_at <= ?1)
         OR EXISTS (SELECT 1 FROM import_jobs WHERE status = 'running' AND locked_until <= ?1) AS ready`,
    now,
  )
  return row?.ready === 1
}

/**
 * Première requête de tout batch d'écritures d'une tâche : échoue (et annule le batch entier) si la
 * tâche n'est plus verrouillée par ce jeton. Voir la table `job_lock_guard`.
 */
export function guardStmt(db: Db, job: Job): D1PreparedStatement {
  return db.stmt(
    `INSERT INTO job_lock_guard (ok) SELECT 0
     WHERE NOT EXISTS (SELECT 1 FROM import_jobs WHERE id = ?1 AND lock_token = ?2 AND status = 'running')`,
    job.id,
    job.lock_token,
  )
}

const finish = (
  db: Db,
  job: Job,
  sets: string,
  params: SqlValue[],
): D1PreparedStatement =>
  db.stmt(
    `UPDATE import_jobs SET ${sets}, locked_until = NULL, lock_token = NULL
     WHERE id = ?1 AND lock_token = ?2`,
    job.id,
    job.lock_token,
    ...params,
  )

export const completeStmt = (db: Db, job: Job, nowMs: number, httpStatus: number | null = null) =>
  finish(db, job, `status = 'done', last_error = NULL, last_http_status = ?3, completed_at = ?4, updated_at = ?4`, [httpStatus, iso(nowMs)])

export const failStmt = (db: Db, job: Job, nowMs: number, error: string, httpStatus: number | null) =>
  finish(db, job, `status = 'failed', last_error = ?3, last_http_status = ?4, completed_at = ?5, updated_at = ?5`, [error, httpStatus, iso(nowMs)])

export const retryStmt = (db: Db, job: Job, nowMs: number, nextAttemptMs: number, error: string, httpStatus: number | null) =>
  finish(db, job, `status = 'pending', next_attempt_at = ?3, last_error = ?4, last_http_status = ?5, updated_at = ?6`, [
    iso(nextAttemptMs),
    error,
    httpStatus,
    iso(nowMs),
  ])

/** Rend la tâche sans consommer de tentative (HTTP 429, tâche différée) ; les rendus sont comptés. */
export const releaseStmt = (db: Db, job: Job, nowMs: number, nextAttemptMs: number, reason: string, httpStatus: number | null) =>
  finish(db, job, `status = 'pending', attempts = MAX(attempts - 1, 0), releases = releases + 1, next_attempt_at = ?3, last_error = ?4, last_http_status = ?5, updated_at = ?6`, [
    iso(nextAttemptMs),
    reason,
    httpStatus,
    iso(nowMs),
  ])

/**
 * Délai avant la tentative suivante : exponentiel (base × 2^(tentative − 1)), plafonné, avec ±20 %
 * d'aléa pour étaler les reprises.
 */
export function retryDelayMs(attempts: number, baseMs: number, maxMs: number, random: () => number = Math.random): number {
  const exponential = Math.min(maxMs, baseMs * 2 ** Math.max(0, attempts - 1))
  return Math.round(exponential * (0.8 + 0.4 * random()))
}
