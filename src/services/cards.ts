/**
 * Tâche `card` : détail d'une carte (rareté, catégorie, illustrateur, métadonnées brutes sans prix).
 */

import { ensureSetStmt, isValidImageStatus, markDetailStmt, upsertCardDetailStmt, type CardForJob } from '../repositories/catalog'
import { enqueueStmts, type Job } from '../repositories/jobs'
import { seriesFromAssetUrl } from '../tcgdex/images'
import { parseCardDetail } from '../tcgdex/types'
import { canonicalJson, contentHash } from '../utils/hash'
import { internalId } from '../utils/keys'
import { imageJob, setJob } from './catalog'
import { failureFromHttp, jobLanguage, jobPayload, jobTarget, type JobContext, type JobResult } from './context'

export async function runCardJob(ctx: JobContext, job: Job, known: CardForJob | undefined): Promise<JobResult> {
  const language = jobLanguage(job)
  const cardTcgdexId = jobTarget(job)
  const cardId = internalId(language, cardTcgdexId)
  const { client, db, config } = ctx

  const orphanJob = jobPayload<{ orphan: boolean }>(job).orphan === true
  const outcome = await client.getJson(client.apiUrl(language, 'cards', cardTcgdexId))
  if (outcome.kind !== 'ok') {
    const nowMs = ctx.now()
    const result = failureFromHttp(outcome, config, `carte ${language}/${cardTcgdexId}`, known ? [markDetailStmt(db, cardId, 'not_found', nowMs)] : [])
    // Carte venue de l'index : même sans détail, son image (URL de l'index) est cherchée.
    if (!orphanJob || !known) return result
    const image = enqueueStmts(db, 'image', language, [imageJob(language, cardTcgdexId)], { runId: job.run_id, maxAttempts: config.maxAttempts, nowMs })
    if (result.kind === 'failed') return { ...result, statements: [...(result.statements ?? []), ...image] }
    if (result.kind === 'retry') return { ...result, exhausted: [markDetailStmt(db, cardId, 'error', nowMs), ...image] }
    return result
  }
  let detail
  try {
    detail = parseCardDetail(outcome.data)
  } catch (error) {
    return { kind: 'retry', error: `carte ${language}/${cardTcgdexId} illisible : ${String(error)}`, httpStatus: 200 }
  }
  if (detail.id !== cardTcgdexId) return { kind: 'failed', error: `carte ${cardTcgdexId} : l'API a répondu pour ${detail.id}`, httpStatus: 200 }

  const nowMs = ctx.now()
  const set = detail.set
  const seriesId = known?.series_id ?? (set ? (seriesFromAssetUrl(set.logo, set.id) ?? seriesFromAssetUrl(set.symbol, set.id) ?? seriesFromAssetUrl(detail.image, set.id)) : null)
  const meta = canonicalJson(detail.raw)
  const statements: D1PreparedStatement[] = []
  // Extension créée seulement pour une carte qui n'en a pas encore (connue par l'index seul).
  if (set && !known?.set_id) {
    statements.push(ensureSetStmt(db, language, { id: set.id, name: set.name, logo: set.logo, symbol: set.symbol, total: set.cardCount.total, official: set.cardCount.official }, nowMs))
  }
  statements.push(
    upsertCardDetailStmt(
      db,
      language,
      {
        id: detail.id,
        localId: detail.localId,
        name: detail.name,
        setTcgdexId: set?.id ?? null,
        seriesTcgdexId: seriesId,
        rarity: detail.rarity,
        category: detail.category,
        illustrator: detail.illustrator,
        image: detail.image,
        meta,
        hash: await contentHash(detail.raw),
      },
      nowMs,
    ),
  )

  // Image : à chercher pour une carte nouvelle (connue seulement par l'index) ou quand l'API donne
  // désormais une URL officielle pour une carte encore sans image valide.
  const orphan = !known || orphanJob
  const newOfficialImage = detail.image !== null && detail.image !== known?.api_image_url
  if (orphan || (known && !isValidImageStatus(known.image_status) && known.image_provider === null && newOfficialImage)) {
    statements.push(...enqueueStmts(db, 'image', language, [imageJob(language, detail.id)], { runId: job.run_id, maxAttempts: config.maxAttempts, nowMs }))
  }
  // Extension absente de la liste officielle : son détail donne la série (chemin des images).
  if (orphan && set && !known?.set_tcgdex_id) {
    statements.push(...enqueueStmts(db, 'set', language, [setJob(language, set.id)], { runId: job.run_id, maxAttempts: config.maxAttempts, nowMs }))
  }
  return { kind: 'done', statements, httpStatus: outcome.status }
}
