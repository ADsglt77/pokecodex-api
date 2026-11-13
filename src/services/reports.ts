/**
 * Tâche `report` : audit complet D1 ↔ R2 et exports, écrits dans R2 sous `reports/` (bucket privé,
 * lisibles seulement par l'API d'administration protégée).
 *
 *   reports/latest/summary.json   bilan par langue et par extension
 *   reports/latest/summary.csv    une ligne par extension, plus les totaux par langue
 *   reports/latest/missing_fr.csv cartes françaises sans image, identifiables précisément
 *   reports/latest/missing_ja.csv cartes japonaises sans image
 *   reports/latest/errors.csv     erreurs techniques (images et tâches en échec)
 *   reports/history/<date>/summary.{json,csv}  historique des bilans
 *
 * L'audit liste les objets R2 sous `cards/` et les compare aux cartes : un objet présent mais
 * inconnu de D1 est réconcilié (tâche d'image, qui le marquera `already_present`) ; une image
 * déclarée valide dont l'objet manque est remise en attente.
 */

import { IMAGES_PREFIX, REPORTS_PREFIX, type Config, type Language } from '../config'
import type { Db } from '../db'
import { isValidImageStatus } from '../repositories/catalog'
import { enqueueMediaStmts, enqueueStmts, guardStmt, JOB_KIND_SQL, type Job } from '../repositories/jobs'
import { csvRow } from '../utils/csv'
import { cardImageKey, lowImageKey, setLogoKey } from '../utils/keys'
import { iso } from '../utils/time'
import { imageJob, logoJob, lowImageJob } from './catalog'
import type { JobContext, JobResult } from './context'
import { collectStats } from './stats'

const PAGE = 1_000

export interface SetCoverage {
  language: Language
  set_id: string | null
  set_name: string | null
  series_name: string | null
  release_date: string | null
  cards: number
  with_api_url: number
  stored: number
  downloaded: number
  recovered: number
  already_present: number
  missing: number
  errors: number
  pending: number
  bytes: number
}

export function setCoverage(db: Db): Promise<SetCoverage[]> {
  return db.all<SetCoverage>(
    `SELECT c.language, s.tcgdex_id AS set_id, s.name AS set_name, se.name AS series_name, s.release_date,
       COUNT(*) AS cards,
       SUM(c.api_image_url IS NOT NULL) AS with_api_url,
       SUM(c.image_status IN ('downloaded', 'recovered_from_cdn', 'already_present')) AS stored,
       SUM(c.image_status = 'downloaded') AS downloaded,
       SUM(c.image_status = 'recovered_from_cdn') AS recovered,
       SUM(c.image_status = 'already_present') AS already_present,
       SUM(c.image_status IN ('missing_api_url', 'not_found_on_cdn')) AS missing,
       SUM(c.image_status IN ('download_error', 'invalid_image')) AS errors,
       SUM(c.image_status = 'pending') AS pending,
       COALESCE(SUM(CASE WHEN c.image_status IN ('downloaded', 'recovered_from_cdn', 'already_present') THEN c.image_file_size END), 0) AS bytes
     FROM cards c
     LEFT JOIN sets s ON s.id = c.set_id
     LEFT JOIN series se ON se.id = s.series_id
     WHERE c.removed_at IS NULL
     GROUP BY c.language, c.set_id
     ORDER BY c.language, s.release_date, s.tcgdex_id`,
  )
}

export interface ProblemCard {
  id: string
  tcgdex_id: string
  language: Language
  name: string
  local_id: string
  set_id: string | null
  set_name: string | null
  series_id: string | null
  api_image_url: string | null
  image_status: string
  image_tested_urls: string | null
  image_http_status: number | null
  image_failure_reason: string | null
  image_checked_at: string | null
}

/** Cartes à problème (manquantes ou en erreur), par pages (pagination par identifiant). */
export function problemCards(db: Db, statuses: readonly string[], opts: { language?: Language | null; after?: string; limit: number }): Promise<ProblemCard[]> {
  return db.all<ProblemCard>(
    `SELECT c.id, c.tcgdex_id, c.language, c.name, c.local_id, s.tcgdex_id AS set_id, s.name AS set_name, c.series_id,
       c.api_image_url, c.image_status, c.image_tested_urls, c.image_http_status, c.image_failure_reason, c.image_checked_at
     FROM cards c LEFT JOIN sets s ON s.id = c.set_id
     WHERE c.removed_at IS NULL AND c.image_status IN (SELECT value FROM json_each(?1))
       AND (?2 IS NULL OR c.language = ?2) AND c.id > ?3
     ORDER BY c.id LIMIT ?4`,
    JSON.stringify(statuses),
    opts.language ?? null,
    opts.after ?? '',
    opts.limit,
  )
}

export interface FailedJob {
  id: number
  job_type: string
  language: string | null
  target_id: string | null
  attempts: number
  last_error: string | null
  last_http_status: number | null
  updated_at: string
}

export function failedJobs(db: Db, opts: { afterId?: number; limit: number }): Promise<FailedJob[]> {
  return db.all<FailedJob>(
    `SELECT id, ${JOB_KIND_SQL} AS job_type, language, target_id, attempts, last_error, last_http_status, updated_at
     FROM import_jobs WHERE status = 'failed' AND id > ?1 ORDER BY id LIMIT ?2`,
    opts.afterId ?? 0,
    opts.limit,
  )
}

/**
 * Toutes les URL testées d'une carte, dans l'ordre (principale puis secours), au format
 * `url (code)` séparées par « ; », et le dernier code HTTP obtenu.
 */
function testedUrls(json: string | null): { text: string; lastStatus: number | null } {
  if (!json) return { text: '', lastStatus: null }
  try {
    const list = JSON.parse(json) as { url: string; status: number | null }[]
    if (!Array.isArray(list) || !list.length) return { text: '', lastStatus: null }
    return {
      text: list.map((item) => `${item.url} (${item.status ?? 'réseau'})`).join(' ; '),
      lastStatus: list[list.length - 1]?.status ?? null,
    }
  } catch {
    return { text: '', lastStatus: null }
  }
}

const MISSING_HEADER = ['id_tcgdex', 'langue', 'nom', 'extension', 'nom_extension', 'numero', 'serie', 'statut', 'url_api', 'urls_testees', 'code_http', 'motif', 'verifie_le']

function missingRow(card: ProblemCard): string {
  const tested = testedUrls(card.image_tested_urls)
  return csvRow([
    card.tcgdex_id,
    card.language,
    card.name,
    card.set_id,
    card.set_name,
    card.local_id,
    card.series_id,
    card.image_status,
    card.api_image_url,
    tested.text,
    card.image_http_status ?? tested.lastStatus ?? '',
    card.image_failure_reason,
    card.image_checked_at,
  ])
}

async function allProblemCards(db: Db, statuses: readonly string[], language: Language | null): Promise<ProblemCard[]> {
  const out: ProblemCard[] = []
  for (let after = ''; ; ) {
    const page = await problemCards(db, statuses, { language, after, limit: PAGE })
    out.push(...page)
    if (page.length < PAGE) return out
    after = (page[page.length - 1] as ProblemCard).id
  }
}

interface AuditResult {
  r2Objects: number
  r2Bytes: number
  unknownObjects: number
  reconciled: number
  missingObjects: number
  /** Objets de petites images et de logos trouvés sous `cards/`. */
  lowImageObjects: number
  logoObjects: number
  /** Petites images ou logos présents dans R2 mais pas encore enregistrés : tâche de réconciliation. */
  mediaReconciled: number
  /** Petites images ou logos enregistrés dont l'objet manque : remis à zéro et recopiés. */
  mediaMissingObjects: number
  statements: D1PreparedStatement[]
}

type AuditCard = {
  id: string
  tcgdex_id: string
  language: Language
  set_tcgdex_id: string | null
  image_status: string
  image_r2_key: string | null
  image_low_status: string | null
  image_low_r2_key: string | null
}
type AuditSet = { id: string; tcgdex_id: string; language: Language; logo_url: string | null; logo_status: string | null; logo_r2_key: string | null }
type AuditTarget = { kind: 'main'; card: AuditCard } | { kind: 'low'; card: AuditCard } | { kind: 'logo'; set: AuditSet }

/**
 * Compare les objets R2 sous `cards/` aux cartes et extensions de D1 : images principales, petites
 * images (`.low.webp`) et logos (`_logo.webp`).
 */
async function auditR2(ctx: JobContext, config: Config): Promise<AuditResult> {
  const { db, bucket } = ctx
  const cards = await db.all<AuditCard>(
    `SELECT c.id, c.tcgdex_id, c.language, s.tcgdex_id AS set_tcgdex_id, c.image_status, c.image_r2_key,
       c.image_low_status, c.image_low_r2_key
     FROM cards c LEFT JOIN sets s ON s.id = c.set_id`,
  )
  const sets = await db.all<AuditSet>(`SELECT id, tcgdex_id, language, logo_url, logo_status, logo_r2_key FROM sets`)
  const byKey = new Map<string, AuditTarget>()
  for (const card of cards) {
    const mainKey = card.image_r2_key ?? cardImageKey(card.language, card.set_tcgdex_id ?? 'sans-extension', card.tcgdex_id)
    byKey.set(mainKey, { kind: 'main', card })
    // Une petite image qui réutilise l'objet principal n'a pas d'objet propre.
    const lowKey = card.image_low_r2_key ?? lowImageKey(mainKey)
    if (lowKey && lowKey !== mainKey) byKey.set(lowKey, { kind: 'low', card })
  }
  for (const set of sets) byKey.set(set.logo_r2_key ?? setLogoKey(set.language, set.tcgdex_id), { kind: 'logo', set })

  const seen = new Set<string>()
  let r2Bytes = 0
  let unknownObjects = 0
  let lowImageObjects = 0
  let logoObjects = 0
  const toReconcile: { language: Language; tcgdexId: string }[] = []
  const lowToReconcile: AuditCard[] = []
  const logosToReconcile: AuditSet[] = []
  let cursor: string | undefined
  do {
    db.usage.r2Reads++
    const listed: R2Objects = await bucket.list({ prefix: IMAGES_PREFIX, limit: 1_000, ...(cursor ? { cursor } : {}) })
    for (const object of listed.objects) {
      seen.add(object.key)
      r2Bytes += object.size
      const target = byKey.get(object.key)
      if (!target) unknownObjects++
      else if (target.kind === 'main') {
        if (!isValidImageStatus(target.card.image_status)) toReconcile.push({ language: target.card.language, tcgdexId: target.card.tcgdex_id })
      } else if (target.kind === 'low') {
        lowImageObjects++
        if (target.card.image_low_status !== 'downloaded' && isValidImageStatus(target.card.image_status)) lowToReconcile.push(target.card)
      } else {
        logoObjects++
        if (target.set.logo_status !== 'downloaded') logosToReconcile.push(target.set)
      }
    }
    cursor = listed.truncated ? listed.cursor : undefined
  } while (cursor)

  const missing = cards.filter((card) => isValidImageStatus(card.image_status) && card.image_r2_key && !seen.has(card.image_r2_key))
  const missingLow = cards.filter((card) => card.image_low_status === 'downloaded' && card.image_low_r2_key && !seen.has(card.image_low_r2_key))
  const missingLogos = sets.filter((set) => set.logo_status === 'downloaded' && set.logo_r2_key && !seen.has(set.logo_r2_key))
  const nowMs = ctx.now()
  const statements: D1PreparedStatement[] = []
  if (missingLow.length) {
    statements.push(
      db.stmt(
        `UPDATE cards SET image_low_status = NULL, image_low_r2_key = NULL, image_low_file_size = NULL, image_low_sha256 = NULL,
           image_low_checked_at = NULL, image_low_failure_reason = 'objet absent de R2 (audit)', updated_at = ?1
         WHERE id IN (SELECT value FROM json_each(?2))`,
        iso(nowMs),
        JSON.stringify(missingLow.map((card) => card.id)),
      ),
    )
  }
  if (missingLogos.length) {
    statements.push(
      db.stmt(
        `UPDATE sets SET logo_status = NULL, logo_r2_key = NULL, logo_file_size = NULL, logo_sha256 = NULL,
           logo_checked_at = NULL, logo_failure_reason = 'objet absent de R2 (audit)', updated_at = ?1
         WHERE id IN (SELECT value FROM json_each(?2))`,
        iso(nowMs),
        JSON.stringify(missingLogos.map((set) => set.id)),
      ),
    )
  }
  if (missing.length) {
    statements.push(
      db.stmt(
        `UPDATE cards SET image_status = 'pending', image_failure_reason = 'objet absent de R2 (audit)',
           image_r2_key = NULL, image_sha256 = NULL, image_file_size = NULL, image_source = NULL,
           image_quality = NULL, image_downloaded_at = NULL, updated_at = ?1
         WHERE id IN (SELECT value FROM json_each(?2))`,
        iso(nowMs),
        JSON.stringify(missing.map((card) => card.id)),
      ),
    )
  }
  for (const language of config.languages) {
    const jobs = [
      ...toReconcile.filter((item) => item.language === language).map((item) => imageJob(language, item.tcgdexId)),
      ...missing.filter((card) => card.language === language).map((card) => imageJob(language, card.tcgdex_id)),
    ]
    statements.push(...enqueueStmts(db, 'image', language, jobs, { runId: null, maxAttempts: config.maxAttempts, nowMs }))
    // Petites images et logos : réconciliés (objet déjà là) ou recopiés (objet disparu) par leur tâche.
    const media = [
      ...[...lowToReconcile, ...missingLow].filter((card) => card.language === language).map((card) => lowImageJob(language, card.tcgdex_id)),
      ...[...logosToReconcile, ...missingLogos].filter((set) => set.language === language && set.logo_url).map((set) => logoJob(language, set.tcgdex_id)),
    ]
    statements.push(...enqueueMediaStmts(db, language, media, { runId: null, maxAttempts: config.maxAttempts, nowMs }))
  }
  return {
    r2Objects: seen.size,
    r2Bytes,
    unknownObjects,
    reconciled: toReconcile.length,
    missingObjects: missing.length,
    lowImageObjects,
    logoObjects,
    mediaReconciled: lowToReconcile.length + logosToReconcile.length,
    mediaMissingObjects: missingLow.length + missingLogos.length,
    statements,
  }
}

async function putReport(bucket: R2Bucket, key: string, body: string, contentType: string, ctx: JobContext): Promise<void> {
  if (!key.startsWith(REPORTS_PREFIX)) throw new Error(`clé de rapport hors de ${REPORTS_PREFIX} : ${key}`)
  ctx.db.usage.r2Writes++
  const stored = await bucket.put(key, body, { httpMetadata: { contentType, cacheControl: 'private, no-store' } })
  if (!stored) throw new Error(`écriture R2 non confirmée : ${key}`)
}

export async function runReportJob(ctx: JobContext, job: Job): Promise<JobResult> {
  const { db, bucket, config } = ctx
  const nowMs = ctx.now()
  const audit = await auditR2(ctx, config)
  // Les corrections de l'audit sont appliquées tout de suite (sous le garde-fou de verrou), pour que
  // les chiffres et les listes du rapport reflètent l'état réconcilié, et non l'état d'avant.
  if (audit.statements.length) await db.batch([guardStmt(db, job), ...audit.statements])
  const stats = await collectStats(db, config, nowMs)
  const sets = await setCoverage(db)

  const summary = {
    generatedAt: iso(nowMs),
    note: 'Pourcentages calculés uniquement sur les cartes répertoriées par TCGdex (données TCGdex sous licence MIT).',
    status: stats.status,
    languages: stats.languages.map((lang) => ({
      ...lang,
      setsWithCards: sets.filter((set) => set.language === lang.language).length,
    })),
    total: stats.total,
    audit: {
      r2Objects: audit.r2Objects,
      r2Bytes: audit.r2Bytes,
      unknownObjects: audit.unknownObjects,
      reconciled: audit.reconciled,
      missingObjects: audit.missingObjects,
      lowImageObjects: audit.lowImageObjects,
      logoObjects: audit.logoObjects,
      mediaReconciled: audit.mediaReconciled,
      mediaMissingObjects: audit.mediaMissingObjects,
    },
    syncRuns: stats.syncRuns,
    sets,
  }

  const summaryCsv = [
    csvRow(['langue', 'extension', 'nom_extension', 'serie', 'sortie', 'cartes', 'url_api', 'stockees', 'telechargees', 'retrouvees_cdn', 'deja_presentes', 'manquantes', 'erreurs', 'en_attente', 'octets', 'couverture_pct']),
    ...sets.map((set) =>
      csvRow([set.language, set.set_id, set.set_name, set.series_name, set.release_date, set.cards, set.with_api_url, set.stored, set.downloaded, set.recovered, set.already_present, set.missing, set.errors, set.pending, set.bytes, set.cards ? ((set.stored / set.cards) * 100).toFixed(2) : '0']),
    ),
    ...stats.languages.map((lang) =>
      csvRow([lang.language, 'TOTAL', '', '', '', lang.cards, lang.withApiImageUrl, lang.imagesStored, lang.imagesDownloaded, lang.imagesRecovered, lang.imagesAlreadyPresent, lang.imagesMissing, lang.errors, lang.imagesPending, lang.bytes, lang.coveragePercent.toFixed(2)]),
    ),
  ].join('')

  const files: [string, string, string][] = [
    ['summary.json', `${JSON.stringify(summary, null, 2)}\n`, 'application/json; charset=utf-8'],
    ['summary.csv', summaryCsv, 'text/csv; charset=utf-8'],
  ]
  for (const language of config.languages) {
    const missing = await allProblemCards(db, ['missing_api_url', 'not_found_on_cdn'], language)
    files.push([`missing_${language}.csv`, [csvRow(MISSING_HEADER), ...missing.map(missingRow)].join(''), 'text/csv; charset=utf-8'])
  }
  const errorCards = await allProblemCards(db, ['download_error', 'invalid_image'], null)
  const jobsInError: FailedJob[] = []
  for (let afterId = 0; ; ) {
    const page = await failedJobs(db, { afterId, limit: PAGE })
    jobsInError.push(...page)
    if (page.length < PAGE) break
    afterId = (page[page.length - 1] as FailedJob).id
  }
  files.push([
    'errors.csv',
    [
      csvRow(['type', 'id', 'langue', 'nom', 'extension', 'numero', 'statut', 'urls_testees', 'code_http', 'motif', 'date']),
      ...errorCards.map((card) => {
        const tested = testedUrls(card.image_tested_urls)
        return csvRow(['image', card.tcgdex_id, card.language, card.name, card.set_id, card.local_id, card.image_status, tested.text, card.image_http_status ?? tested.lastStatus ?? '', card.image_failure_reason, card.image_checked_at])
      }),
      ...jobsInError.map((job) => csvRow([`tache:${job.job_type}`, job.target_id, job.language, '', '', '', `échec après ${job.attempts} tentative(s)`, '', job.last_http_status ?? '', job.last_error, job.updated_at])),
    ].join(''),
    'text/csv; charset=utf-8',
  ])

  const stamp = iso(nowMs).replaceAll(':', '-').replace(/\.\d+Z$/, 'Z')
  for (const [name, body, type] of files) {
    await putReport(bucket, `${REPORTS_PREFIX}latest/${name}`, body, type, ctx)
    if (name.startsWith('summary.')) await putReport(bucket, `${REPORTS_PREFIX}history/${stamp}/${name}`, body, type, ctx)
  }
  return { kind: 'done', statements: [] }
}
