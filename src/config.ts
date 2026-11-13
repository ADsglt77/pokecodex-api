/**
 * Configuration de l'importateur, lue dans les variables du Worker (`vars` de `wrangler.jsonc`).
 * Chaque valeur est validée ; une valeur absente ou invalide reprend sa valeur par défaut, une
 * valeur hors bornes est ramenée dans ses bornes. Les limites applicatives réduisent les coûts mais
 * ne garantissent pas à elles seules un plafond de facture.
 */

export const LANGUAGES = ['fr', 'ja'] as const
export type Language = (typeof LANGUAGES)[number]

export const isLanguage = (value: unknown): value is Language =>
  typeof value === 'string' && (LANGUAGES as readonly string[]).includes(value)

/** Préfixes R2 réservés à l'importateur. Constantes : jamais configurables. */
export const IMAGES_PREFIX = 'cards/'
export const REPORTS_PREFIX = 'reports/'

export interface Config {
  processCron: string
  syncCron: string
  languages: Language[]
  apiBase: string
  userAgent: string
  maxConcurrency: number
  minRequestIntervalMs: number
  maxRequestIntervalMs: number
  runTimeBudgetMs: number
  maxJobsPerRun: number
  claimBatchSize: number
  d1QueryBudget: number
  subrequestBudget: number
  runnerLeaseMs: number
  jobLeaseMs: number
  maxAttempts: number
  maxReleases: number
  retryBaseMs: number
  retryMaxMs: number
  fetchTimeoutMs: number
  rateLimitBackoffMs: number
  rateLimitMaxBackoffMs: number
  errorStreakLimit: number
  errorStreakBackoffMs: number
  imageQuality: string
  fallbackQuality: string | null
  maxReconstructionUrls: number
  minImageBytes: number
  maxImageBytes: number
  maxJsonBytes: number
  maxStoredBytes: number
  maxDailyOperations: number
  detailRefreshDays: number
  missingRecheckDays: number
}

/** Les variables lues (toutes des chaînes, comme dans `vars`). */
export type ConfigSource = Partial<Record<string, string | undefined>>

function integer(source: ConfigSource, key: string, fallback: number, min: number, max: number): number {
  const raw = source[key]
  if (raw === undefined || raw.trim() === '') return fallback
  const value = Number(raw)
  if (!Number.isFinite(value)) return fallback
  return Math.min(max, Math.max(min, Math.floor(value)))
}

function text(source: ConfigSource, key: string, fallback: string): string {
  const raw = source[key]?.trim()
  return raw ? raw : fallback
}

const QUALITY = /^(high|low)$/

/** Seule origine d'API acceptée : un hôte arbitraire ne doit jamais pouvoir être contacté. */
const DEFAULT_API_BASE = 'https://api.tcgdex.net/v2'

function apiBase(source: ConfigSource): string {
  const raw = text(source, 'TCGDEX_API_BASE', DEFAULT_API_BASE).replace(/\/+$/, '')
  if (raw.includes('?') || raw.includes('#') || raw.includes('@')) return DEFAULT_API_BASE
  try {
    const url = new URL(raw)
    return url.protocol === 'https:' && url.host === 'api.tcgdex.net' && !url.username && !url.password ? raw : DEFAULT_API_BASE
  } catch {
    return DEFAULT_API_BASE
  }
}

/** Durée maximale d'une invocation planifiée chez Cloudflare. */
export const SCHEDULED_MAX_MS = 900_000

export function parseConfig(source: ConfigSource): Config {
  const languages = text(source, 'LANGUAGES', 'fr,ja')
    .split(',')
    .map((value) => value.trim())
    .filter(isLanguage)
  const rawQuality = text(source, 'IMAGE_QUALITY', 'high')
  const imageQuality = QUALITY.test(rawQuality) ? rawQuality : 'high'
  const fallbackQuality = source.FALLBACK_QUALITY?.trim() ?? 'low'
  const minRequestIntervalMs = integer(source, 'MIN_REQUEST_INTERVAL_MS', 250, 50, 60_000)
  const maxRequestIntervalMs = Math.max(minRequestIntervalMs, integer(source, 'MAX_REQUEST_INTERVAL_MS', 5_000, 50, 120_000))
  // Six connexions simultanées au plus par invocation (Cloudflare) : on en garde pour D1 et R2.
  const maxConcurrency = integer(source, 'MAX_CONCURRENCY', 2, 1, 4)
  const fetchTimeoutMs = integer(source, 'FETCH_TIMEOUT_MS', 30_000, 1_000, 120_000)
  // Dépassement possible après l'échéance : une tâche commencée juste avant (deux URL candidates,
  // attente du limiteur, écritures de clôture). Le budget en tient compte pour ne jamais approcher
  // les 15 min d'une invocation planifiée, et les verrous durent plus que l'exécution entière.
  const overrunMs = 2 * fetchTimeoutMs + maxConcurrency * maxRequestIntervalMs + 60_000
  const runTimeBudgetMs = Math.min(integer(source, 'RUN_TIME_BUDGET_MS', 240_000, 5_000, 840_000), SCHEDULED_MAX_MS - overrunMs)
  const runnerLeaseMs = Math.max(integer(source, 'RUNNER_LEASE_MS', 360_000, 30_000, 3_600_000), runTimeBudgetMs + overrunMs)
  const jobLeaseMs = Math.max(integer(source, 'JOB_LEASE_MS', 600_000, 60_000, 7_200_000), runnerLeaseMs)
  return {
    processCron: text(source, 'PROCESS_CRON', '*/5 * * * *'),
    syncCron: text(source, 'SYNC_CRON', '7 3 * * MON'),
    languages: languages.length ? [...new Set(languages)] : [...LANGUAGES],
    apiBase: apiBase(source),
    userAgent: text(source, 'USER_AGENT', 'tcgdex-r2-importer/1.0'),
    maxConcurrency,
    minRequestIntervalMs,
    maxRequestIntervalMs,
    runTimeBudgetMs,
    maxJobsPerRun: integer(source, 'MAX_JOBS_PER_RUN', 900, 1, 5_000),
    claimBatchSize: integer(source, 'CLAIM_BATCH_SIZE', 10, 1, 50),
    // Limite Cloudflare : 1 000 requêtes D1 par invocation (un batch compte pour une).
    d1QueryBudget: integer(source, 'D1_QUERY_BUDGET', 850, 50, 950),
    // Limite Cloudflare par défaut : 10 000 sous-requêtes par invocation.
    subrequestBudget: integer(source, 'SUBREQUEST_BUDGET', 5_000, 200, 9_500),
    runnerLeaseMs,
    jobLeaseMs,
    maxAttempts: integer(source, 'MAX_ATTEMPTS', 5, 1, 20),
    maxReleases: integer(source, 'MAX_RELEASES', 48, 1, 1_000),
    retryBaseMs: integer(source, 'RETRY_BASE_MS', 60_000, 1_000, 3_600_000),
    retryMaxMs: integer(source, 'RETRY_MAX_MS', 21_600_000, 60_000, 172_800_000),
    fetchTimeoutMs,
    rateLimitBackoffMs: integer(source, 'RATE_LIMIT_BACKOFF_MS', 120_000, 1_000, 86_400_000),
    rateLimitMaxBackoffMs: integer(source, 'RATE_LIMIT_MAX_BACKOFF_MS', 3_600_000, 1_000, 86_400_000),
    errorStreakLimit: integer(source, 'ERROR_STREAK_LIMIT', 10, 1, 1_000),
    errorStreakBackoffMs: integer(source, 'ERROR_STREAK_BACKOFF_MS', 900_000, 1_000, 86_400_000),
    imageQuality,
    fallbackQuality: QUALITY.test(fallbackQuality) && fallbackQuality !== imageQuality ? fallbackQuality : null,
    maxReconstructionUrls: integer(source, 'MAX_RECONSTRUCTION_URLS', 1, 0, 5),
    minImageBytes: integer(source, 'MIN_IMAGE_BYTES', 100, 12, 1_000_000),
    maxImageBytes: integer(source, 'MAX_IMAGE_BYTES', 8_000_000, 1_000, 50_000_000),
    // Le décodage d'un JSON demande ≈ 8 fois sa taille en mémoire (128 Mo par isolat) : 12 Mo au plus.
    maxJsonBytes: integer(source, 'MAX_JSON_BYTES', 8_000_000, 100_000, 12_000_000),
    maxStoredBytes: integer(source, 'MAX_STORED_BYTES', 20_000_000_000, 1_000_000, 1_000_000_000_000),
    maxDailyOperations: integer(source, 'MAX_DAILY_OPERATIONS', 150_000, 100, 10_000_000),
    detailRefreshDays: integer(source, 'DETAIL_REFRESH_DAYS', 90, 1, 3_650),
    missingRecheckDays: integer(source, 'MISSING_RECHECK_DAYS', 30, 1, 3_650),
  }
}
