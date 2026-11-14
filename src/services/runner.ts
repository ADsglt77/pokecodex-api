/**
 * Exécuteur des tâches, lancé par le Cron (toutes les 5 minutes) ou à la main (`POST /admin/tick`).
 *
 * Une exécution :
 *   1. sort aussitôt, sans rien écrire, si l'importation est en pause, en attente imposée (429),
 *      au plafond quotidien d'opérations ou sans travail ;
 *   2. prend le verrou de l'exécuteur (une seule exécution à la fois, verrou qui expire), puis relit
 *      l'état pour ne pas décider sur une lecture périmée ;
 *   3. remet en attente les tâches dont le verrou a expiré (exécution précédente interrompue) ;
 *   4. vérifie le plafond de volume stocké ;
 *   5. réserve les tâches par lots, les traite avec une concurrence limitée, et applique les
 *      écritures de chaque lot dans un batch D1 (transaction) qui renouvelle aussi le verrou et compte
 *      les opérations, jusqu'à épuisement d'un budget : temps, tâches, requêtes D1, sous-requêtes ;
 *   6. libère son verrou, enregistre son bilan et clôt les synchronisations terminées.
 * L'exécution suivante reprend là où celle-ci s'est arrêtée : rien ne dépend d'un processus local.
 */

import type { Config } from '../config'
import { Db, Usage } from '../db'
import { cardsForJobs, setsForMediaJobs, type CardForJob, type SetForMediaJob } from '../repositories/catalog'
import {
  claimJobs,
  completeStmt,
  enqueueStmts,
  failStmt,
  guardStmt,
  hasWork,
  mediaKind,
  reclaimExpired,
  releaseStmt,
  retryDelayMs,
  retryStmt,
  type Job,
} from '../repositories/jobs'
import { completeSyncRunStmt, insertTickRunStmt, languageCounts, runningSyncRuns, storedBytes, syncRunHasActiveJobs } from '../repositories/runs'
import {
  acquireRunnerLease,
  addDailyOperationsStmt,
  backoffUntilMs,
  dailyOperations,
  isPaused,
  readState,
  releaseRunnerLeaseStmt,
  renewRunnerLeaseStmt,
  setStateStmt,
  STATE,
  type StateRow,
} from '../repositories/state'
import { Health, TcgdexClient } from '../tcgdex/client'
import { RETRY_AFTER_MAX_MS } from '../utils/http'
import { internalId } from '../utils/keys'
import { RateLimiter } from '../utils/limiter'
import { errorMessage, log, logError } from '../utils/log'
import { iso, sleep } from '../utils/time'
import { imageJob, runCardIndexJob, runCatalogJob, runSetJob } from './catalog'
import { runCardJob } from './cards'
import { jobPayload, type JobContext, type JobResult } from './context'
import { runImageJob } from './downloader'
import { imageJobFailureStmts, runLogoJob, runLowImageJob, runSymbolJob } from './media'
import { runReportJob } from './reports'

export interface RunnerDeps {
  config: Config
  d1: D1Database
  bucket: R2Bucket
  fetcher: typeof fetch
  now?: () => number
  wait?: (ms: number) => Promise<void>
  random?: () => number
  /** Délais des relances courtes du client (tests : `[]`). */
  quickRetryDelaysMs?: number[]
}

export interface TickOptions {
  trigger: string
  /** Budget de temps propre à cette exécution (sinon `config.runTimeBudgetMs`). */
  budgetMs?: number
  maxJobs?: number
}

export interface TickResult {
  status: 'idle' | 'skipped' | 'ran'
  reason: string
  claimed: number
  done: number
  retried: number
  failed: number
  released: number
  /** Tâches dont le verrou a été perdu en cours de route (écritures annulées). */
  lost: number
  httpRequests: number
  d1Queries: number
  bytesStored: number
  durationMs: number
}

type Counters = Pick<TickResult, 'claimed' | 'done' | 'retried' | 'failed' | 'released' | 'lost'>

/** Requêtes D1 gardées en réserve pour la clôture de l'exécution (bilan, clôture des synchros). */
const D1_RESERVE = 12
/** Sous-requêtes au pire par tâche : HEAD R2, deux URL candidates, PUT R2, lectures D1. */
const SUBREQUESTS_PER_JOB = 6
/** Plafond d'un délai de reprise (une date au-delà ferait échouer `toISOString`). */
const MAX_DELAY_MS = 7 * 86_400_000

const clampDelay = (ms: number): number => Math.min(MAX_DELAY_MS, Math.max(0, Number.isFinite(ms) ? ms : 0))

/** Attente globale après un HTTP 429 : le Retry-After (≤ 24 h), sinon un délai qui double à chaque 429. */
function rateLimitPause(config: Config, retryAfterMs: number | null, consecutive: number): number {
  if (retryAfterMs !== null) return Math.min(retryAfterMs, RETRY_AFTER_MAX_MS)
  return Math.min(config.rateLimitMaxBackoffMs, config.rateLimitBackoffMs * 2 ** Math.max(0, consecutive - 1))
}

export async function runTick(deps: RunnerDeps, opts: TickOptions): Promise<TickResult> {
  const now = deps.now ?? Date.now
  const wait = deps.wait ?? sleep
  const random = deps.random ?? Math.random
  const { config } = deps
  const startMs = now()
  const usage = new Usage()
  const db = new Db(deps.d1, usage)
  const counters: Counters = { claimed: 0, done: 0, retried: 0, failed: 0, released: 0, lost: 0 }
  const result = (status: TickResult['status'], reason: string): TickResult => ({
    status,
    reason,
    ...counters,
    httpRequests: usage.httpRequests,
    d1Queries: usage.d1Queries,
    bytesStored: usage.bytesStored,
    durationMs: now() - startMs,
  })

  // 1. Sorties rapides, en lecture seule.
  const early = skipReason(await readState(db, startMs), config, startMs)
  if (early) return result('skipped', early)
  if (!(await hasWork(db, startMs))) {
    await finalizeSyncRuns(db, config, now())
    return result('idle', 'aucune tâche prête')
  }

  // 2. Verrou de l'exécuteur, puis état relu (un 429 a pu être enregistré entre-temps).
  const token = crypto.randomUUID()
  if (!(await acquireRunnerLease(db, token, startMs, config.runnerLeaseMs))) return result('skipped', 'une autre exécution est en cours')

  let stopReason = 'budget épuisé'
  let error: string | null = null
  let opsFlushed = 0
  let resetConsecutive429 = false
  const endStatements: D1PreparedStatement[] = []
  try {
    const state = await readState(db, now())
    const late = skipReason(state, config, now())
    if (late) {
      stopReason = late
      return result('skipped', late)
    }

    // 3. Tâches d'une exécution interrompue.
    // Tâche d'image (principale, petite image, logo ou symbole) épuisée : sa cible passe en download_error.
    const expiredFailures = await reclaimExpired(db, now())
    const lostImages = expiredFailures.flatMap((job) => imageJobFailureStmts(db, job, 'verrou expiré : nombre maximal de tentatives atteint', now()))
    if (lostImages.length) await db.batch(lostImages)

    // 4. Plafond de stockage.
    const opsBefore = dailyOperations(state, startMs)
    const stored = await storedBytes(db)
    if (stored >= config.maxStoredBytes) {
      stopReason = `plafond de stockage atteint (${stored} octets) : pause automatique`
      endStatements.push(setStateStmt(db, STATE.paused, '1', now()), setStateStmt(db, STATE.pauseReason, stopReason, now()))
      return result('skipped', stopReason)
    }

    // 5. Traitement par lots.
    const budgetMs = Math.min(opts.budgetMs ?? config.runTimeBudgetMs, config.runTimeBudgetMs)
    const deadlineMs = startMs + budgetMs
    const maxJobs = Math.min(opts.maxJobs ?? config.maxJobsPerRun, config.maxJobsPerRun)
    const health = new Health()
    const limiter = new RateLimiter(config.minRequestIntervalMs, now, wait)
    const client = new TcgdexClient({
      config,
      fetcher: deps.fetcher,
      limiter,
      usage,
      health,
      now,
      deadlineMs,
      wait,
      ...(deps.quickRetryDelaysMs ? { quickRetryDelaysMs: deps.quickRetryDelaysMs } : {}),
    })
    const ctx: JobContext = { config, db, bucket: deps.bucket, client, now }

    for (;;) {
      if (now() >= deadlineMs) {
        stopReason = 'budget de temps épuisé'
        break
      }
      // Taille du lot ajustée à ce qui reste de chaque budget (≈ 3 requêtes D1 par lot + 1 par tâche).
      const limits: [number, string][] = [
        [maxJobs - counters.claimed, `maximum de ${maxJobs} tâches atteint`],
        [config.d1QueryBudget - usage.d1Queries - D1_RESERVE - 3, `budget de requêtes D1 épuisé (${usage.d1Queries})`],
        [Math.floor((config.subrequestBudget - usage.subrequests - 20) / SUBREQUESTS_PER_JOB), `budget de sous-requêtes épuisé (${usage.subrequests})`],
        [Math.floor((config.maxDailyOperations - opsBefore - usage.operations) / 3), 'plafond quotidien d’opérations atteint'],
      ]
      let batchSize = config.claimBatchSize
      for (const [allowed, reason] of limits) {
        if (allowed < batchSize) batchSize = allowed
        if (allowed <= 0) stopReason = reason
      }
      if (batchSize <= 0) break

      const jobs = await claimJobs(db, { limit: batchSize, token, nowMs: now(), leaseMs: config.jobLeaseMs })
      if (!jobs.length) {
        stopReason = 'plus aucune tâche prête'
        break
      }
      counters.claimed += jobs.length
      const cardIds = jobs.filter((job) => (job.job_type === 'card' || job.job_type === 'image') && job.card_id).map((job) => job.card_id as string)
      const cards = await cardsForJobs(db, cardIds)
      // Extensions des tâches de logo et de symbole (une requête de plus, seulement si le lot en contient).
      const mediaSetId = (job: Job): string | null => {
        const media = mediaKind(job)
        return (media === 'logo' || media === 'symbol') && job.language && job.target_id ? internalId(job.language, job.target_id) : null
      }
      const sets = await setsForMediaJobs(db, jobs.map(mediaSetId).filter((id): id is string => id !== null))

      const results = await mapPool(jobs, config.maxConcurrency, async (job): Promise<JobResult> => {
        // Budget dépassé avant le début de la tâche : rendue intacte pour l'exécution suivante.
        if (now() >= deadlineMs || health.rateLimited || health.errorStreak >= config.errorStreakLimit) {
          return { kind: 'release', reason: 'exécution arrêtée avant cette tâche', httpStatus: null, delayMs: 0 }
        }
        try {
          const setId = mediaSetId(job)
          return await runJob(ctx, job, job.card_id ? cards.get(job.card_id) : undefined, setId ? sets.get(setId) : undefined)
        } catch (jobError) {
          logError('job_exception', jobError, { jobId: job.id, type: job.job_type })
          return { kind: 'retry', error: `exception : ${errorMessage(jobError)}`, httpStatus: null }
        }
      })

      // Écritures d'état de ce lot, appliquées dans le même batch que ses tâches : rien n'est perdu
      // si l'invocation s'arrête ensuite (opérations du jour, attente imposée).
      const extra: D1PreparedStatement[] = []
      const opsDelta = usage.operations - opsFlushed
      if (opsDelta > 0) extra.push(addDailyOperationsStmt(db, opsDelta, now()))
      const opsAtFlush = usage.operations
      let stop: string | null = null
      if (health.rateLimited) {
        const consecutive = Number(state.get(STATE.consecutive429)?.value ?? 0) + 1
        const delay = rateLimitPause(config, health.rateLimited.retryAfterMs, consecutive)
        stop = `HTTP 429 de TCGdex : attente de ${Math.round(delay / 1000)} s`
        extra.push(
          setStateStmt(db, STATE.backoffUntil, iso(now() + delay), now()),
          setStateStmt(db, STATE.backoffReason, stop, now()),
          setStateStmt(db, STATE.consecutive429, String(consecutive), now()),
        )
      } else if (health.errorStreak >= config.errorStreakLimit) {
        stop = `${health.errorStreak} erreurs serveur ou réseau consécutives : pause de ${Math.round(config.errorStreakBackoffMs / 60_000)} min`
        extra.push(setStateStmt(db, STATE.backoffUntil, iso(now() + config.errorStreakBackoffMs), now()), setStateStmt(db, STATE.backoffReason, stop, now()))
      }
      const stateWrites = [...extra.slice(opsDelta > 0 ? 1 : 0)]
      // En dernier : le renouvellement du verrou, dont le résultat dit s'il est toujours à nous.
      extra.push(renewRunnerLeaseStmt(db, token, now(), config.runnerLeaseMs))

      const lease = await flush(db, config, jobs, results, counters, now, random, extra)
      if (lease === 'error') {
        // Écritures d'état non enregistrées (D1 en difficulté) : reportées au bilan final, et arrêt.
        endStatements.push(...stateWrites)
        stopReason = stop ?? 'écriture de l’état d’exécution impossible : arrêt'
        break
      }
      opsFlushed = opsAtFlush
      if (lease === 'lost') {
        stopReason = 'verrou de l’exécuteur perdu : arrêt'
        break
      }
      if (stop) {
        stopReason = stop
        break
      }
    }
    resetConsecutive429 = !health.rateLimited && counters.done > 0 && Boolean(state.get(STATE.consecutive429)?.value)
    return result('ran', stopReason)
  } catch (tickError) {
    error = errorMessage(tickError)
    stopReason = 'erreur'
    logError('tick_error', tickError)
    return result('ran', `erreur : ${error}`)
  } finally {
    // 6. Bilan, libération du verrou, clôture des synchronisations terminées.
    const endMs = now()
    const summary = { trigger: opts.trigger, startedMs: startMs, finishedMs: endMs, ...counters, stopReason, error }
    const statements = [
      ...endStatements,
      releaseRunnerLeaseStmt(db, token, endMs),
      setStateStmt(db, STATE.lastTick, JSON.stringify({ ...summary, startedAt: iso(startMs), finishedAt: iso(endMs) }), endMs),
    ]
    if (resetConsecutive429) statements.push(setStateStmt(db, STATE.consecutive429, null, endMs))
    if (usage.operations > opsFlushed) statements.push(addDailyOperationsStmt(db, usage.operations - opsFlushed, endMs))
    if (counters.claimed > 0) {
      statements.push(setStateStmt(db, STATE.lastActiveTick, iso(endMs), endMs), insertTickRunStmt(db, summary, usage))
    }
    try {
      await db.batch(statements)
      await finalizeSyncRuns(db, config, now())
    } catch (endError) {
      logError('tick_end_error', endError)
    }
    log('tick', { ...summary, http: usage.httpRequests, d1: usage.d1Queries, bytes: usage.bytesStored })
  }
}

/** Raison de ne pas travailler (pause, attente imposée, plafond du jour), d'après l'état lu. */
function skipReason(state: Map<string, StateRow>, config: Config, nowMs: number): string | null {
  if (isPaused(state)) return `en pause : ${state.get(STATE.pauseReason)?.value ?? 'demande manuelle'}`
  const backoffUntil = backoffUntilMs(state)
  if (backoffUntil > nowMs) return `attente imposée jusqu'à ${iso(backoffUntil)} (${state.get(STATE.backoffReason)?.value ?? ''})`
  const ops = dailyOperations(state, nowMs)
  if (ops >= config.maxDailyOperations) return `plafond quotidien atteint (${ops} opérations)`
  return null
}

/** Aiguille une tâche vers son traitement. */
async function runJob(ctx: JobContext, job: Job, card: CardForJob | undefined, set: SetForMediaJob | undefined): Promise<JobResult> {
  switch (job.job_type) {
    case 'catalog':
      return runCatalogJob(ctx, job)
    case 'set':
      return runSetJob(ctx, job)
    case 'card_index':
      return runCardIndexJob(ctx, job)
    case 'card':
      return runCardJob(ctx, job, card)
    case 'image': {
      // Copies secondaires (petite image, logo, symbole) : même type de tâche, priorité la plus basse.
      const media = mediaKind(job)
      if (media === 'low') return runLowImageJob(ctx, job, card)
      if (media === 'logo') return runLogoJob(ctx, job, set)
      if (media === 'symbol') return runSymbolJob(ctx, job, set)
      return runImageJob(ctx, job, card)
    }
    case 'report':
      return runReportJob(ctx, job)
  }
}

type Outcome = 'done' | 'failed' | 'retried' | 'released'

/**
 * Écritures de clôture d'une tâche, précédées du garde-fou de verrou. Une tâche d'image qui échoue
 * définitivement sans motif propre passe sa cible (image principale, petite image, logo ou symbole) en
 * `download_error` (visible et relançable).
 * Une tâche rendue trop souvent (`maxReleases`) consomme désormais une tentative : jamais de boucle.
 */
function closingStatements(
  db: Db,
  config: Config,
  job: Job,
  result: JobResult,
  nowMs: number,
  random: () => number,
): { statements: D1PreparedStatement[]; outcome: Outcome } {
  const guard = guardStmt(db, job)
  // Conséquence par défaut d'un échec définitif : une image (principale, petite image, logo, symbole) qui
  // échoue passe en `download_error` ; une carte venue de l'index dont le détail échoue voit quand
  // même son image cherchée.
  const imageFailure = (reason: string): D1PreparedStatement[] => {
    if (job.job_type === 'image') return imageJobFailureStmts(db, job, reason, nowMs)
    if (job.job_type === 'card' && job.language && job.target_id && jobPayload<{ orphan: boolean }>(job).orphan === true) {
      return enqueueStmts(db, 'image', job.language, [imageJob(job.language, job.target_id)], { runId: job.run_id, maxAttempts: config.maxAttempts, nowMs })
    }
    return []
  }
  const retryOrFail = (error: string, httpStatus: number | null, statements: D1PreparedStatement[], exhausted: D1PreparedStatement[] | undefined) => {
    if (job.attempts >= job.max_attempts) {
      const message = `${error} (après ${job.attempts} tentative(s))`
      return { statements: [guard, ...statements, ...(exhausted ?? imageFailure(message)), failStmt(db, job, nowMs, message, httpStatus)], outcome: 'failed' as const }
    }
    const next = nowMs + clampDelay(retryDelayMs(job.attempts, config.retryBaseMs, config.retryMaxMs, random))
    return { statements: [guard, ...statements, retryStmt(db, job, nowMs, next, error, httpStatus)], outcome: 'retried' as const }
  }
  switch (result.kind) {
    case 'done':
      return { statements: [guard, ...result.statements, completeStmt(db, job, nowMs, result.httpStatus ?? null)], outcome: 'done' }
    case 'failed':
      return { statements: [guard, ...(result.statements ?? []), ...imageFailure(result.error), failStmt(db, job, nowMs, result.error, result.httpStatus)], outcome: 'failed' }
    case 'retry':
      return retryOrFail(result.error, result.httpStatus, result.statements ?? [], result.exhausted)
    case 'release':
      if (job.releases + 1 > config.maxReleases) {
        return retryOrFail(`${result.reason} (rendue ${job.releases} fois)`, result.httpStatus, [], undefined)
      }
      return { statements: [guard, releaseStmt(db, job, nowMs, nowMs + clampDelay(result.delayMs), result.reason, result.httpStatus)], outcome: 'released' }
  }
}

/**
 * Applique les écritures d'un lot en un seul batch, avec les écritures d'état `extra` (dont le
 * renouvellement du verrou de l'exécuteur, en dernier). Si le batch échoue (verrou perdu par une
 * tâche, contrainte violée…), chaque tâche est rejouée seule pour qu'une erreur n'en bloque pas
 * d'autres ; une tâche dont les écritures échouent encore est remise à plus tard sans elles.
 * Rend `held` si le verrou de l'exécuteur est toujours détenu, `lost` s'il ne l'est plus, `error` si
 * les écritures d'état n'ont pas pu être enregistrées.
 */
async function flush(
  db: Db,
  config: Config,
  jobs: Job[],
  results: JobResult[],
  counters: Counters,
  now: () => number,
  random: () => number,
  extra: D1PreparedStatement[],
): Promise<'held' | 'lost' | 'error'> {
  const nowMs = now()
  const count = (outcome: Outcome) => {
    if (outcome === 'done') counters.done++
    else if (outcome === 'failed') counters.failed++
    else if (outcome === 'retried') counters.retried++
    else counters.released++
  }
  const leaseHeld = (batchResults: D1Result[]) => ((batchResults.at(-1)?.meta.changes ?? 0) > 0 ? 'held' : 'lost')
  let closings: { statements: D1PreparedStatement[]; outcome: Outcome }[] = []
  try {
    closings = jobs.map((job, i) => closingStatements(db, config, job, results[i] as JobResult, nowMs, random))
    const batchResults = await db.batch([...closings.flatMap((closing) => closing.statements), ...extra])
    closings.forEach((closing) => count(closing.outcome))
    return leaseHeld(batchResults)
  } catch (batchError) {
    logError('flush_batch_error', batchError, { jobs: jobs.length })
  }
  for (const [i, job] of jobs.entries()) {
    const closing = closings[i]
    try {
      if (!closing) throw new Error('écritures de clôture impossibles à construire')
      await db.batch(closing.statements)
      count(closing.outcome)
    } catch (jobError) {
      const message = errorMessage(jobError)
      if (message.includes('CHECK constraint failed')) {
        counters.lost++
        log('job_lock_lost', { jobId: job.id })
        continue
      }
      logError('job_write_error', jobError, { jobId: job.id, type: job.job_type })
      try {
        const final = job.attempts >= job.max_attempts
        const retryAt = nowMs + clampDelay(retryDelayMs(job.attempts, config.retryBaseMs, config.retryMaxMs, random))
        const reason = `écriture D1 échouée : ${message}`
        const failure = final ? imageJobFailureStmts(db, job, reason, nowMs) : []
        if (failure.length) {
          // Échec définitif d'une image : sa cible passe en download_error (sinon, au moins la tâche).
          try {
            await db.batch([guardStmt(db, job), ...failure, failStmt(db, job, nowMs, reason, null)])
            counters.failed++
            continue
          } catch {
            // Repli : la tâche seule.
          }
        }
        await db.batch([guardStmt(db, job), final ? failStmt(db, job, nowMs, reason, null) : retryStmt(db, job, nowMs, retryAt, reason, null)])
        if (final) counters.failed++
        else counters.retried++
      } catch (lastError) {
        // Le verrou expirera et la tâche sera reprise par une exécution suivante.
        counters.lost++
        logError('job_write_error_final', lastError, { jobId: job.id })
      }
    }
  }
  // Écritures d'état seules, avec une seconde chance en cas d'erreur passagère.
  for (let attempt = 0; attempt < 2; attempt++) {
    try {
      return leaseHeld(await db.batch(extra))
    } catch (extraError) {
      logError('flush_extra_error', extraError, { attempt })
    }
  }
  return 'error'
}

/** Exécute `fn` sur chaque élément avec au plus `concurrency` appels simultanés, résultats dans l'ordre. */
export async function mapPool<T, R>(items: T[], concurrency: number, fn: (item: T) => Promise<R>): Promise<R[]> {
  const results = new Array<R>(items.length)
  let next = 0
  const lanes = Array.from({ length: Math.max(1, Math.min(concurrency, items.length)) }, async () => {
    while (next < items.length) {
      const index = next++
      results[index] = await fn(items[index] as T)
    }
  })
  await Promise.all(lanes)
  return results
}

/**
 * Clôt les synchronisations qui n'ont plus de tâche active : bilan réel calculé sur les tables, puis
 * une tâche de rapport quand plus aucune synchronisation n'est en cours.
 */
export async function finalizeSyncRuns(db: Db, config: Config, nowMs: number): Promise<number[]> {
  const runs = await runningSyncRuns(db)
  if (!runs.length) return []
  const completed: number[] = []
  const statements: D1PreparedStatement[] = []
  for (const run of runs) {
    if (await syncRunHasActiveJobs(db, run.id)) continue
    statements.push(completeSyncRunStmt(db, run.id, await languageCounts(db, run.language), nowMs))
    completed.push(run.id)
  }
  if (!completed.length) return []
  if (completed.length === runs.length) {
    statements.push(
      ...enqueueStmts(db, 'report', null, [{ target: null, cardId: null, dedupeKey: 'report:*:*', payload: { syncRuns: completed } }], {
        runId: null,
        maxAttempts: config.maxAttempts,
        nowMs,
      }),
    )
  }
  await db.batch(statements)
  log('sync_runs_completed', { runs: completed })
  return completed
}
