/**
 * Copies secondaires, pour l'affichage du site : petite image `low.webp` de chaque carte (grilles) et
 * logo de chaque extension. Tâches `image` de plus basse priorité (`MEDIA_PRIORITY`), reconnues à
 * leur charge utile `{ media: 'low' | 'logo' }`.
 *
 * Mêmes garanties que l'image principale (`downloader.ts`) :
 *   - un objet R2 valide n'est jamais écrasé ni retéléchargé : un objet déjà présent (copie faite
 *     mais D1 pas mis à jour) est réconcilié en `downloaded` ; un objet étranger à la cible n'est
 *     jamais remplacé ;
 *   - le fichier est lu en flux dans la limite de `maxImageBytes`, puis validé exactement comme
 *     l'image principale (statut, `image/webp`, signature RIFF…WEBP, tailles, MD5 = ETag), et écrit
 *     dans R2 avec le contrôle SHA-256 de R2 ; D1 n'est mis à jour qu'une fois l'écriture confirmée ;
 *   - un 404 est une absence confirmée (aucune nouvelle tentative : revérifiée par la synchronisation
 *     hebdomadaire après `missingRecheckDays` jours) ; un 429 rend la tâche ; une erreur serveur,
 *     réseau ou un transfert corrompu sont retentés, puis `download_error`.
 * Les requêtes passent par le même client (limiteur global ≤ 4 requêtes/s, https sur
 * assets.tcgdex.net seulement, aucune redirection), et sont comptées dans les plafonds de coût.
 */

import { IMAGES_PREFIX } from '../config'
import type { Db } from '../db'
import {
  imageFailureStmt,
  isValidImageStatus,
  logoFailureStmt,
  logoSuccessStmt,
  lowImageFailureStmt,
  lowImageSuccessStmt,
  type CardForJob,
  type MediaFailureStatus,
  type SetForLogoJob,
} from '../repositories/catalog'
import { dedupeKey, mediaKind, type Job } from '../repositories/jobs'
import { checkWebp, isLowQualitySource, logoImageUrl, lowImageUrl } from '../tcgdex/images'
import { BodyTooLargeError, readCapped } from '../utils/http'
import { internalId, lowImageKey, setLogoKey } from '../utils/keys'
import { errorMessage } from '../utils/log'
import { jobLanguage, jobTarget, rateLimitDelayMs, type JobContext, type JobResult } from './context'

/** Petite image réservée dans le même lot que son image principale : rendue jusqu'après ce lot. */
const MAIN_IMAGE_RUNNING_DELAY_MS = 60_000

type CopyResult =
  | { kind: 'stored'; size: number; sha256: string; httpStatus: number }
  | { kind: 'not_found'; httpStatus: number }
  | { kind: 'invalid'; reason: string; httpStatus: number }
  | { kind: 'rate_limited'; retryAfterMs: number | null }
  | { kind: 'transient'; reason: string; httpStatus: number | null }

/**
 * Télécharge `url`, la valide comme une image principale et l'écrit à `key` (sous `cards/`). Ne lit
 * ni n'écrit D1. L'absence d'objet valide à `key` doit avoir été vérifiée avant.
 */
async function copyWebp(ctx: JobContext, url: string, key: string, customMetadata: Record<string, string>): Promise<CopyResult> {
  if (!key.startsWith(IMAGES_PREFIX)) throw new Error(`clé R2 hors du préfixe ${IMAGES_PREFIX} : ${key}`)
  const { client, bucket, config, db } = ctx
  const outcome = await client.get(url, 'image/webp')
  if (outcome.kind === 'not_found') return { kind: 'not_found', httpStatus: outcome.status }
  if (outcome.kind === 'rate_limited') return { kind: 'rate_limited', retryAfterMs: outcome.retryAfterMs }
  if (outcome.kind !== 'ok') {
    // 5xx, réseau, redirection refusée, autre 4xx : nouvelle tentative bornée, comme l'image principale.
    if (outcome.kind === 'network_error') return { kind: 'transient', reason: `erreur réseau : ${outcome.message}`, httpStatus: null }
    return { kind: 'transient', reason: `HTTP ${outcome.status}`, httpStatus: outcome.status }
  }

  let bytes: Uint8Array
  try {
    bytes = await readCapped(outcome.response, config.maxImageBytes)
  } catch (error) {
    if (error instanceof BodyTooLargeError) return { kind: 'invalid', reason: error.message, httpStatus: outcome.status }
    return { kind: 'transient', reason: `lecture interrompue : ${errorMessage(error)}`, httpStatus: outcome.status }
  }
  const check = await checkWebp(bytes, outcome.response.headers, config)
  if (!check.ok) return check.transient ? { kind: 'transient', reason: check.reason, httpStatus: outcome.status } : { kind: 'invalid', reason: check.reason, httpStatus: outcome.status }

  let stored: R2Object | null
  try {
    db.usage.r2Writes++
    stored = await bucket.put(key, bytes, {
      sha256: check.sha256,
      httpMetadata: { contentType: 'image/webp', cacheControl: 'public, max-age=31536000, immutable' },
      customMetadata: { ...customMetadata, sourceUrl: url, sha256: check.sha256, md5: check.md5 },
    })
  } catch (error) {
    return { kind: 'transient', reason: `écriture R2 échouée : ${errorMessage(error)}`, httpStatus: null }
  }
  if (!stored || stored.size !== bytes.byteLength) {
    return { kind: 'transient', reason: `écriture R2 non confirmée (${stored?.size ?? 'aucun objet'} ≠ ${bytes.byteLength} octets)`, httpStatus: null }
  }
  db.usage.bytesStored += bytes.byteLength
  return { kind: 'stored', size: bytes.byteLength, sha256: check.sha256, httpStatus: outcome.status }
}

/** Objet déjà présent à la clé : réconciliable (le nôtre, valide), étranger (jamais écrasé) ou absent. */
type Existing = { kind: 'ours'; size: number; sha256: string } | { kind: 'foreign' } | { kind: 'none' }

async function existingObject(ctx: JobContext, key: string, identity: [string, string]): Promise<Existing> {
  ctx.db.usage.r2Reads++
  const object = await ctx.bucket.head(key)
  if (!object) return { kind: 'none' }
  const meta = object.customMetadata ?? {}
  // Même règle que l'image principale : taille plausible et SHA-256 enregistré par l'importateur.
  if (object.size < ctx.config.minImageBytes || !meta.sha256) return { kind: 'none' }
  return meta[identity[0]] === identity[1] ? { kind: 'ours', size: object.size, sha256: meta.sha256 } : { kind: 'foreign' }
}

/** Traduit l'échec d'une copie en résultat de tâche ; `record` écrit le statut d'échec de la cible. */
function failureResult(copy: Exclude<CopyResult, { kind: 'stored' }>, url: string, what: string, record: (status: MediaFailureStatus, reason: string) => D1PreparedStatement, ctx: JobContext): JobResult {
  switch (copy.kind) {
    case 'not_found':
      return { kind: 'done', httpStatus: copy.httpStatus, statements: [record('not_found_on_cdn', `${what} introuvable sur le CDN (HTTP ${copy.httpStatus}) : ${url}`)] }
    case 'invalid':
      return { kind: 'done', httpStatus: copy.httpStatus, statements: [record('invalid_image', `${what} invalide : ${copy.reason} (${url})`)] }
    case 'rate_limited':
      return { kind: 'release', reason: 'CDN : HTTP 429', httpStatus: 429, delayMs: rateLimitDelayMs(copy.retryAfterMs, ctx.config) }
    case 'transient':
      return { kind: 'retry', error: `${what} : ${copy.reason}`, httpStatus: copy.httpStatus, exhausted: [record('download_error', `${copy.reason} (${url})`)] }
  }
}

/**
 * Petite image d'une carte : `low.webp` de la même base que l'image principale stockée, copiée à la
 * clé de l'image principale suffixée `.low.webp`. Si l'image principale est déjà en qualité `low`,
 * elle est simplement enregistrée comme petite image (même objet, aucun téléchargement).
 */
export async function runLowImageJob(ctx: JobContext, job: Job, card: CardForJob | undefined): Promise<JobResult> {
  const language = jobLanguage(job)
  const { db } = ctx
  if (!card) return { kind: 'failed', error: `petite image : carte ${job.card_id ?? job.target_id} inconnue en base`, httpStatus: null }
  if (card.image_low_status === 'downloaded' && card.image_low_r2_key) return { kind: 'done', statements: [] }

  // Pas (encore) d'image principale valide : rien à faire, sans l'attendre. Copiée, l'image
  // principale remet elle-même cette tâche en file (`runImageJob`) ; absente, la synchronisation
  // suivante ou /admin/sync/backfill-media reprendra la carte. L'attendre en rendant la tâche
  // userait ses rendus (`MAX_RELEASES`) pendant une longue file d'images principales : elle
  // finirait en `download_error`, revérifiée seulement 30 jours plus tard.
  if (!isValidImageStatus(card.image_status) || !card.image_r2_key) {
    // Seule exception : l'image principale est en cours de copie dans ce même lot (réservée par cette
    // exécution). Sa remise en file passerait dans le batch avant la clôture de cette tâche-ci, encore
    // en cours, et serait perdue : la tâche est rendue (au plus une fois par tentative de l'image
    // principale, jamais tant que celle-ci attend dans la file).
    const main = await db.first<{ n: number }>(
      `SELECT COUNT(*) AS n FROM import_jobs WHERE dedupe_key = ?1 AND status = 'running' AND lock_token = ?2`,
      dedupeKey('image', language, card.tcgdex_id),
      job.lock_token,
    )
    if ((main?.n ?? 0) > 0) return { kind: 'release', reason: 'petite image : image principale en cours de copie', httpStatus: null, delayMs: MAIN_IMAGE_RUNNING_DELAY_MS }
    return { kind: 'done', statements: [] }
  }

  const record = (status: MediaFailureStatus, reason: string) => lowImageFailureStmt(db, card.id, { status, reason }, ctx.now())
  // Image principale déjà en qualité low : c'est aussi la petite image (aucune copie, octets déjà comptés).
  if (isLowQualitySource(card.image_source_url, card.image_quality)) {
    return { kind: 'done', statements: [lowImageSuccessStmt(db, card.id, { key: card.image_r2_key, size: null, sha256: card.image_sha256 }, ctx.now())] }
  }
  const key = lowImageKey(card.image_r2_key)
  if (!key) return { kind: 'done', statements: [record('missing_api_url', `clé R2 de l’image principale inattendue : ${card.image_r2_key}`)] }

  let existing: Existing
  try {
    existing = await existingObject(ctx, key, ['tcgdexId', card.tcgdex_id])
  } catch (error) {
    const message = `lecture R2 échouée : ${errorMessage(error)}`
    return { kind: 'retry', error: `petite image ${card.id} : ${message}`, httpStatus: null, exhausted: [record('download_error', message)] }
  }
  if (existing.kind === 'ours') {
    return { kind: 'done', statements: [lowImageSuccessStmt(db, card.id, { key, size: existing.size, sha256: existing.sha256 }, ctx.now())] }
  }
  if (existing.kind === 'foreign') {
    return { kind: 'done', statements: [record('download_error', `objet R2 déjà présent à ${key}, étranger à cette carte : jamais écrasé`)] }
  }

  const url = lowImageUrl(card.image_source_url, language)
  if (!url) return { kind: 'done', statements: [record('missing_api_url', `URL source de l’image principale inconnue ou non reconnue : ${card.image_source_url ?? 'aucune'}`)] }

  const copy = await copyWebp(ctx, url, key, { variant: 'low', quality: 'low', language, tcgdexId: card.tcgdex_id })
  if (copy.kind !== 'stored') return failureResult(copy, url, `petite image ${card.id}`, record, ctx)
  return { kind: 'done', httpStatus: copy.httpStatus, statements: [lowImageSuccessStmt(db, card.id, { key, size: copy.size, sha256: copy.sha256 }, ctx.now())] }
}

/** Logo d'une extension : `<logo_url>.webp`, copié à `cards/<langue>/<extension>/_logo.webp`. */
export async function runLogoJob(ctx: JobContext, job: Job, set: SetForLogoJob | undefined): Promise<JobResult> {
  const language = jobLanguage(job)
  const setTcgdexId = jobTarget(job)
  const { db } = ctx
  if (!set) return { kind: 'failed', error: `logo : extension ${language}/${setTcgdexId} inconnue en base`, httpStatus: null }
  if (set.logo_status === 'downloaded' && set.logo_r2_key) return { kind: 'done', statements: [] }

  const candidate = set.logo_url ? `${set.logo_url}.webp` : null
  const record = (status: MediaFailureStatus, reason: string) => logoFailureStmt(db, set.id, { status, reason, sourceUrl: candidate }, ctx.now())
  const key = setLogoKey(language, set.tcgdex_id)

  let existing: Existing
  try {
    existing = await existingObject(ctx, key, ['setId', set.tcgdex_id])
  } catch (error) {
    const message = `lecture R2 échouée : ${errorMessage(error)}`
    return { kind: 'retry', error: `logo ${set.id} : ${message}`, httpStatus: null, exhausted: [record('download_error', message)] }
  }
  if (existing.kind === 'ours') {
    return { kind: 'done', statements: [logoSuccessStmt(db, set.id, { key, sourceUrl: null, size: existing.size, sha256: existing.sha256 }, ctx.now())] }
  }
  if (existing.kind === 'foreign') return { kind: 'done', statements: [record('download_error', `objet R2 déjà présent à ${key}, étranger à cette extension : jamais écrasé`)] }

  if (!set.logo_url) return { kind: 'done', statements: [record('missing_api_url', 'aucun logo donné par l’API')] }
  const url = logoImageUrl(set.logo_url, language)
  if (!url) return { kind: 'done', statements: [record('missing_api_url', `URL de logo refusée (hors du CDN de TCGdex ou d’une autre langue) : ${set.logo_url}`)] }

  const copy = await copyWebp(ctx, url, key, { variant: 'logo', language, setId: set.tcgdex_id })
  if (copy.kind !== 'stored') return failureResult(copy, url, `logo ${set.id}`, record, ctx)
  return { kind: 'done', httpStatus: copy.httpStatus, statements: [logoSuccessStmt(db, set.id, { key, sourceUrl: url, size: copy.size, sha256: copy.sha256 }, ctx.now())] }
}

/**
 * Écritures d'un échec définitif sans motif propre (tentatives épuisées, verrou expiré, rendue trop
 * souvent) d'une tâche `image` : la cible passe en `download_error` — la carte pour l'image
 * principale, sa petite image, ou le logo de l'extension.
 */
export function imageJobFailureStmts(
  db: Db,
  job: { job_type: string; language: string | null; target_id: string | null; card_id: string | null; payload_json: string | null },
  reason: string,
  nowMs: number,
): D1PreparedStatement[] {
  if (job.job_type !== 'image') return []
  const media = mediaKind(job)
  if (media === 'logo') {
    if ((job.language !== 'fr' && job.language !== 'ja') || !job.target_id) return []
    return [logoFailureStmt(db, internalId(job.language, job.target_id), { status: 'download_error', reason }, nowMs)]
  }
  if (!job.card_id) return []
  if (media === 'low') return [lowImageFailureStmt(db, job.card_id, { status: 'download_error', reason }, nowMs)]
  return [imageFailureStmt(db, job.card_id, { status: 'download_error', reason, testedUrls: '[]', httpStatus: null }, nowMs)]
}
