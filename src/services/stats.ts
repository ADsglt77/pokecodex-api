/**
 * Statistiques réelles de l'importation, calculées sur D1 à la demande (API d'administration et
 * rapports), jamais par l'exécuteur planifié.
 */

import type { Config, Language } from '../config'
import type { Db } from '../db'
import { backoffUntilMs, dailyOperations, isPaused, readState, STATE } from '../repositories/state'
import { nextCronTime } from '../utils/cron'
import { iso } from '../utils/time'

export interface LanguageStats {
  language: Language
  series: number
  sets: number
  cards: number
  removed: number
  withApiImageUrl: number
  detailsFetched: number
  detailsPending: number
  detailsNotFound: number
  imagesPending: number
  imagesDownloaded: number
  imagesRecovered: number
  imagesAlreadyPresent: number
  imagesStored: number
  imagesMissingApiUrl: number
  imagesNotFoundOnCdn: number
  imagesMissing: number
  downloadErrors: number
  invalidImages: number
  errors: number
  bytes: number
  /** Couverture : images stockées / cartes répertoriées par TCGdex. */
  coveragePercent: number
}

export interface JobCount {
  job_type: string
  status: string
  n: number
}

export interface Stats {
  generatedAt: string
  status: 'NON DÉMARRÉ' | 'EN COURS' | 'EN PAUSE' | 'EN ATTENTE' | 'TERMINÉ'
  statusDetail: string | null
  languages: LanguageStats[]
  total: {
    cards: number
    cardsProcessed: number
    imagesStored: number
    imagesRecovered: number
    imagesMissing: number
    errors: number
    bytes: number
    progressPercent: number
    jobsTotal: number
    jobsActive: number
    jobsFailed: number
  }
  jobs: JobCount[]
  lastTick: unknown
  lastActiveTick: string | null
  nextTick: string | null
  nextCatalogSync: string | null
  operationsToday: number
  syncRuns: unknown[]
  limits: Record<string, number | string | null>
}

const percent = (part: number, whole: number) => (whole > 0 ? Math.round((part / whole) * 10_000) / 100 : 0)

async function languageStats(db: Db, language: Language): Promise<LanguageStats> {
  const row = await db.first<Omit<LanguageStats, 'language' | 'imagesMissing' | 'errors' | 'imagesStored' | 'coveragePercent'>>(
    `SELECT
       (SELECT COUNT(*) FROM series WHERE language = ?1) AS series,
       (SELECT COUNT(*) FROM sets WHERE language = ?1) AS sets,
       COALESCE(SUM(removed_at IS NULL), 0) AS cards,
       COALESCE(SUM(removed_at IS NOT NULL), 0) AS removed,
       COALESCE(SUM(removed_at IS NULL AND api_image_url IS NOT NULL), 0) AS withApiImageUrl,
       COALESCE(SUM(removed_at IS NULL AND detail_status = 'fetched'), 0) AS detailsFetched,
       COALESCE(SUM(removed_at IS NULL AND detail_status IN ('pending', 'error')), 0) AS detailsPending,
       COALESCE(SUM(removed_at IS NULL AND detail_status = 'not_found'), 0) AS detailsNotFound,
       COALESCE(SUM(removed_at IS NULL AND image_status = 'pending'), 0) AS imagesPending,
       COALESCE(SUM(removed_at IS NULL AND image_status = 'downloaded'), 0) AS imagesDownloaded,
       COALESCE(SUM(removed_at IS NULL AND image_status = 'recovered_from_cdn'), 0) AS imagesRecovered,
       COALESCE(SUM(removed_at IS NULL AND image_status = 'already_present'), 0) AS imagesAlreadyPresent,
       COALESCE(SUM(removed_at IS NULL AND image_status = 'missing_api_url'), 0) AS imagesMissingApiUrl,
       COALESCE(SUM(removed_at IS NULL AND image_status = 'not_found_on_cdn'), 0) AS imagesNotFoundOnCdn,
       COALESCE(SUM(removed_at IS NULL AND image_status = 'download_error'), 0) AS downloadErrors,
       COALESCE(SUM(removed_at IS NULL AND image_status = 'invalid_image'), 0) AS invalidImages,
       COALESCE(SUM(CASE WHEN image_status IN ('downloaded', 'recovered_from_cdn', 'already_present')
         THEN image_file_size END), 0) AS bytes
     FROM cards WHERE language = ?1`,
    language,
  )
  if (!row) throw new Error('statistiques indisponibles')
  const imagesStored = row.imagesDownloaded + row.imagesRecovered + row.imagesAlreadyPresent
  return {
    language,
    ...row,
    imagesStored,
    imagesMissing: row.imagesMissingApiUrl + row.imagesNotFoundOnCdn,
    errors: row.downloadErrors + row.invalidImages,
    coveragePercent: percent(imagesStored, row.cards),
  }
}

export async function collectStats(db: Db, config: Config, nowMs: number): Promise<Stats> {
  const languages = await Promise.all(config.languages.map((language) => languageStats(db, language)))
  const jobs = await db.all<JobCount>(`SELECT job_type, status, COUNT(*) AS n FROM import_jobs GROUP BY job_type, status ORDER BY job_type, status`)
  const state = await readState(db, nowMs)
  const syncRuns = await db.all<Record<string, unknown>>(`SELECT * FROM sync_runs ORDER BY id DESC LIMIT 6`)

  const jobsTotal = jobs.filter((job) => job.job_type !== 'report').reduce((sum, job) => sum + job.n, 0)
  // La tâche de rapport n'est pas du travail d'importation : elle ne compte ni dans le total ni
  // dans l'activité (sinon un rapport en cours se décrirait lui-même « EN COURS »).
  const jobsActive = jobs.filter((job) => job.job_type !== 'report' && (job.status === 'pending' || job.status === 'running')).reduce((sum, job) => sum + job.n, 0)
  const jobsFailed = jobs.filter((job) => job.job_type !== 'report' && job.status === 'failed').reduce((sum, job) => sum + job.n, 0)
  const jobsFinished = jobs.filter((job) => job.job_type !== 'report' && ['done', 'failed', 'cancelled'].includes(job.status)).reduce((sum, job) => sum + job.n, 0)
  const sum = (key: keyof LanguageStats) => languages.reduce((total, lang) => total + (lang[key] as number), 0)
  const cards = sum('cards')

  const backoff = backoffUntilMs(state)
  let status: Stats['status']
  let statusDetail: string | null = null
  if (isPaused(state)) {
    status = 'EN PAUSE'
    statusDetail = state.get(STATE.pauseReason)?.value ?? null
  } else if (backoff > nowMs && jobsActive > 0) {
    status = 'EN ATTENTE'
    statusDetail = `${state.get(STATE.backoffReason)?.value ?? ''} (jusqu'à ${iso(backoff)})`
  } else if (jobsActive > 0) {
    status = 'EN COURS'
  } else if (syncRuns.some((run) => run.status === 'completed')) {
    status = 'TERMINÉ'
  } else {
    status = 'NON DÉMARRÉ'
  }

  let lastTick: unknown = null
  try {
    lastTick = JSON.parse(state.get(STATE.lastTick)?.value ?? 'null')
  } catch {
    lastTick = null
  }
  const nextTick = nextCronTime(config.processCron, nowMs)
  const nextSync = nextCronTime(config.syncCron, nowMs)
  return {
    generatedAt: iso(nowMs),
    status,
    statusDetail,
    languages,
    total: {
      cards,
      cardsProcessed: cards - sum('imagesPending'),
      imagesStored: sum('imagesStored'),
      imagesRecovered: sum('imagesRecovered'),
      imagesMissing: sum('imagesMissing'),
      errors: sum('errors'),
      bytes: sum('bytes'),
      progressPercent: percent(jobsFinished, jobsTotal),
      jobsTotal,
      jobsActive,
      jobsFailed,
    },
    jobs,
    lastTick,
    lastActiveTick: state.get(STATE.lastActiveTick)?.value ?? null,
    nextTick: nextTick === null ? null : iso(nextTick),
    nextCatalogSync: nextSync === null ? null : iso(nextSync),
    operationsToday: dailyOperations(state, nowMs),
    syncRuns,
    limits: {
      maxConcurrency: config.maxConcurrency,
      minRequestIntervalMs: config.minRequestIntervalMs,
      runTimeBudgetMs: config.runTimeBudgetMs,
      maxJobsPerRun: config.maxJobsPerRun,
      maxAttempts: config.maxAttempts,
      maxStoredBytes: config.maxStoredBytes,
      maxDailyOperations: config.maxDailyOperations,
      processCron: config.processCron,
      syncCron: config.syncCron,
    },
  }
}

const LABELS: Record<Language, string> = { fr: 'FRANÇAIS', ja: 'JAPONAIS' }

const pad = (label: string) => label.padEnd(22, ' ')
const n = (value: number) => value.toLocaleString('fr-FR')
const gb = (bytes: number) => (bytes / 1e9).toLocaleString('fr-FR', { minimumFractionDigits: 3, maximumFractionDigits: 3 })

/** Rapport texte de `GET /admin/status`, au format demandé. */
export function formatStatus(stats: Stats): string {
  const lines = ['POKÉCODEX — TCGDEX IMPORTER', '', `Statut : ${stats.status}${stats.statusDetail ? ` — ${stats.statusDetail}` : ''}`, '']
  for (const lang of stats.languages) {
    lines.push(
      LABELS[lang.language],
      `${pad('Cartes connues')}: ${n(lang.cards)}`,
      `${pad('Images téléchargées')}: ${n(lang.imagesDownloaded + lang.imagesAlreadyPresent)}`,
      `${pad('Images retrouvées CDN')}: ${n(lang.imagesRecovered)}`,
      `${pad('Images manquantes')}: ${n(lang.imagesMissing)}`,
      `${pad('Erreurs')}: ${n(lang.errors)}`,
      `${pad('En attente')}: ${n(lang.imagesPending)}`,
      '',
    )
  }
  const lastTick = stats.lastTick as { finishedAt?: string; stopReason?: string } | null
  lines.push(
    'TOTAL',
    `${pad('Cartes traitées')}: ${n(stats.total.cardsProcessed)} / ${n(stats.total.cards)}`,
    `${pad('Images stockées R2')}: ${n(stats.total.imagesStored)}`,
    `${pad('Volume stocké')}: ${gb(stats.total.bytes)} Go`,
    `${pad('Progression')}: ${stats.total.progressPercent.toLocaleString('fr-FR')} % (${n(stats.total.jobsTotal - stats.total.jobsActive)} / ${n(stats.total.jobsTotal)} tâches)`,
    `${pad('Dernière exécution')}: ${lastTick?.finishedAt ?? 'aucune'}${lastTick?.stopReason ? ` (${lastTick.stopReason})` : ''}`,
    `${pad('Dernière activité')}: ${stats.lastActiveTick ?? 'aucune'}`,
    `${pad('Prochaine exécution')}: ${stats.nextTick ?? 'inconnue'}`,
    `${pad('Prochaine synchro')}: ${stats.nextCatalogSync ?? 'inconnue'}`,
  )
  return `${lines.join('\n')}\n`
}
