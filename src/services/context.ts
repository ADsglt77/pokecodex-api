/**
 * Ce que reçoit chaque tâche, et ce qu'elle rend à l'exécuteur.
 *
 * Une tâche ne modifie jamais D1 elle-même (hormis ses lectures) : elle rend ses écritures, que
 * l'exécuteur applique dans un seul batch D1 (transaction), précédé du garde-fou de verrou et suivi
 * de la mise à jour de la tâche. Les écritures R2 (images, rapports) sont faites par la tâche, avant.
 */

import type { Config, Language } from '../config'
import type { Db } from '../db'
import type { Job } from '../repositories/jobs'
import type { TcgdexClient } from '../tcgdex/client'
import { RETRY_AFTER_MAX_MS, type HttpOutcome } from '../utils/http'

export interface JobContext {
  config: Config
  db: Db
  bucket: R2Bucket
  client: TcgdexClient
  now: () => number
}

export type JobResult =
  /** Tâche terminée ; `statements` sont appliquées avec sa clôture. */
  | { kind: 'done'; statements: D1PreparedStatement[]; httpStatus?: number | null }
  /** Échec passager : nouvelle tentative différée, ou `exhausted` appliquées si c'était la dernière. */
  | { kind: 'retry'; error: string; httpStatus: number | null; statements?: D1PreparedStatement[]; exhausted?: D1PreparedStatement[] }
  /** Échec définitif (ex. 404 de l'API) : aucune nouvelle tentative. */
  | { kind: 'failed'; error: string; httpStatus: number | null; statements?: D1PreparedStatement[] }
  /** Tâche rendue sans consommer de tentative (HTTP 429, ou dépendance pas encore prête). */
  | { kind: 'release'; reason: string; httpStatus: number | null; delayMs: number }

/** Langue d'une tâche qui en exige une. */
export function jobLanguage(job: Job): Language {
  if (job.language !== 'fr' && job.language !== 'ja') throw new Error(`tâche ${job.id} sans langue valide`)
  return job.language
}

export function jobTarget(job: Job): string {
  if (!job.target_id) throw new Error(`tâche ${job.id} sans cible`)
  return job.target_id
}

export function jobPayload<T extends object>(job: Job): Partial<T> {
  if (!job.payload_json) return {}
  try {
    const value: unknown = JSON.parse(job.payload_json)
    return value && typeof value === 'object' ? (value as Partial<T>) : {}
  } catch {
    return {}
  }
}

/**
 * Délai avant de reprendre une tâche après un HTTP 429 : le `Retry-After` reçu (borné à 24 h), sinon
 * l'attente par défaut. L'attente globale de l'exécuteur suit la même règle.
 */
export function rateLimitDelayMs(retryAfterMs: number | null, config: Config): number {
  return retryAfterMs !== null ? Math.min(retryAfterMs, RETRY_AFTER_MAX_MS) : config.rateLimitBackoffMs
}

/**
 * Traduit une réponse HTTP non réussie de l'API en résultat de tâche. Le 404 est définitif ; le 429
 * rend la tâche sans consommer de tentative ; le reste est passager.
 */
export function failureFromHttp(
  outcome: Exclude<HttpOutcome, { kind: 'ok' }> | { kind: 'invalid'; status: number; message: string },
  config: Config,
  what: string,
  onNotFound: D1PreparedStatement[] = [],
): JobResult {
  switch (outcome.kind) {
    case 'not_found':
      return { kind: 'failed', error: `${what} : introuvable (HTTP ${outcome.status})`, httpStatus: outcome.status, statements: onNotFound }
    case 'rate_limited':
      return { kind: 'release', reason: `${what} : HTTP 429`, httpStatus: 429, delayMs: rateLimitDelayMs(outcome.retryAfterMs, config) }
    case 'invalid':
      return { kind: 'retry', error: `${what} : réponse illisible (${outcome.message})`, httpStatus: outcome.status }
    case 'network_error':
      return { kind: 'retry', error: `${what} : erreur réseau (${outcome.message})`, httpStatus: null }
    default:
      // 3xx (redirection refusée), 4xx autres que 404/429, 5xx : nouvelle tentative bornée.
      return { kind: 'retry', error: `${what} : HTTP ${outcome.status}`, httpStatus: outcome.status }
  }
}
