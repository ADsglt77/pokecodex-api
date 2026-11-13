/**
 * Découverte du catalogue : séries et extensions (tâche `catalog`), détail de chaque extension et
 * de ses cartes (tâche `set`), puis contrôle des cartes listées par TCGdex mais absentes du détail
 * de leur extension (tâche `card_index`).
 */

import type { Language } from '../config'
import {
  ensureSeriesStmt,
  existingCardsOfSet,
  insertOrphanBriefsStmts,
  insertSetBriefsStmts,
  isValidImageStatus,
  markRemovedStmt,
  setLogoState,
  upsertCardBriefsStmts,
  upsertSeriesStmts,
  upsertSetDetailStmt,
  type CardBriefRow,
  type ExistingCard,
  type SetLogoState,
} from '../repositories/catalog'
import { dedupeKey, enqueueMediaStmts, enqueueStmts, logoDedupeKey, lowImageDedupeKey, type Job, type NewJob } from '../repositories/jobs'
import { updateSyncRunFoundStmt } from '../repositories/runs'
import { seriesFromAssetUrl } from '../tcgdex/images'
import { parseCardList, parseSeriesList, parseSetDetail, parseSetList, stripPricing, type CardBrief } from '../tcgdex/types'
import { contentHash } from '../utils/hash'
import { internalId } from '../utils/keys'
import { log } from '../utils/log'
import { failureFromHttp, jobLanguage, jobPayload, jobTarget, type JobContext, type JobResult } from './context'

const DAY_MS = 86_400_000

/** Périmètre facultatif d'une synchronisation (tests ciblés) : certaines extensions seulement. */
export interface SyncScope {
  sets?: string[]
}

const cardJob = (language: Language, cardTcgdexId: string, payload?: unknown): NewJob => ({
  target: cardTcgdexId,
  cardId: internalId(language, cardTcgdexId),
  dedupeKey: dedupeKey('card', language, cardTcgdexId),
  payload,
})

export const imageJob = (language: Language, cardTcgdexId: string): NewJob => ({
  target: cardTcgdexId,
  cardId: internalId(language, cardTcgdexId),
  dedupeKey: dedupeKey('image', language, cardTcgdexId),
})

export const setJob = (language: Language, setTcgdexId: string): NewJob => ({
  target: setTcgdexId,
  cardId: null,
  dedupeKey: dedupeKey('set', language, setTcgdexId),
})

/** Copie de la petite image (`low.webp`) d'une carte : tâche `image` de priorité `MEDIA_PRIORITY`. */
export const lowImageJob = (language: Language, cardTcgdexId: string): NewJob => ({
  target: cardTcgdexId,
  cardId: internalId(language, cardTcgdexId),
  dedupeKey: lowImageDedupeKey(language, cardTcgdexId),
  payload: { media: 'low' },
})

/** Copie du logo d'une extension : tâche `image` de priorité `MEDIA_PRIORITY`. */
export const logoJob = (language: Language, setTcgdexId: string): NewJob => ({
  target: setTcgdexId,
  cardId: null,
  dedupeKey: logoDedupeKey(language, setTcgdexId),
  payload: { media: 'logo' },
})

/** Tâche `catalog` : séries et extensions d'une langue, puis une tâche `set` par extension. */
export async function runCatalogJob(ctx: JobContext, job: Job): Promise<JobResult> {
  const language = jobLanguage(job)
  const scope = jobPayload<SyncScope>(job)
  const { client, db, config } = ctx

  const seriesOutcome = await client.getJson(client.apiUrl(language, 'series'))
  if (seriesOutcome.kind !== 'ok') return failureFromHttp(seriesOutcome, config, `séries ${language}`)
  const setsOutcome = await client.getJson(client.apiUrl(language, 'sets'))
  if (setsOutcome.kind !== 'ok') return failureFromHttp(setsOutcome, config, `extensions ${language}`)

  let series
  let sets
  try {
    series = parseSeriesList(seriesOutcome.data)
    sets = parseSetList(setsOutcome.data)
  } catch (error) {
    return { kind: 'retry', error: `catalogue ${language} illisible : ${String(error)}`, httpStatus: 200 }
  }
  const wanted = scope.sets?.length ? new Set(scope.sets) : null
  const selectedSets = wanted ? sets.filter((set) => wanted.has(set.id)) : sets
  // Une extension demandée mais absente de la liste officielle reste tentée par son détail.
  const extraSets = wanted ? [...wanted].filter((id) => !sets.some((set) => set.id === id)) : []

  const nowMs = ctx.now()
  const seriesRows = await Promise.all(
    series.map(async (item) => ({ id: item.id, name: item.name, logo: item.logo, meta: JSON.stringify(item), hash: await contentHash(item) })),
  )
  const statements = [
    ...upsertSeriesStmts(db, language, seriesRows, nowMs),
    ...insertSetBriefsStmts(
      db,
      language,
      selectedSets.map((set) => ({ id: set.id, name: set.name, logo: set.logo, symbol: set.symbol, total: set.cardCount.total, official: set.cardCount.official })),
      nowMs,
    ),
    ...enqueueStmts(db, 'set', language, [...selectedSets.map((set) => set.id), ...extraSets].map((id) => setJob(language, id)), {
      runId: job.run_id,
      maxAttempts: config.maxAttempts,
      nowMs,
    }),
  ]
  // L'index complet des cartes n'a de sens que pour une synchronisation complète.
  if (!wanted) {
    statements.push(
      ...enqueueStmts(db, 'card_index', language, [{ target: null, cardId: null, dedupeKey: dedupeKey('card_index', language, null) }], {
        runId: job.run_id,
        maxAttempts: config.maxAttempts,
        nowMs,
      }),
    )
  }
  if (job.run_id !== null) statements.push(updateSyncRunFoundStmt(db, job.run_id, series.length, selectedSets.length + extraSets.length))
  log('catalog', { language, series: series.length, sets: selectedSets.length + extraSets.length })
  return { kind: 'done', statements, httpStatus: 200 }
}

/** Faut-il (re)demander le détail de cette carte ? */
function needsDetail(card: ExistingCard, nowMs: number, refreshDays: number): boolean {
  if (card.detail_status !== 'fetched') return true
  const fetched = card.detail_fetched_at ? Date.parse(card.detail_fetched_at) : 0
  return nowMs - fetched > refreshDays * DAY_MS
}

/** Faut-il (re)chercher l'image de cette carte ? */
function needsImage(card: ExistingCard, imageChanged: boolean, nowMs: number, recheckDays: number): boolean {
  if (isValidImageStatus(card.image_status)) return false
  if (card.image_status === 'pending' || imageChanged || !card.image_checked_at) return true
  return nowMs - Date.parse(card.image_checked_at) > recheckDays * DAY_MS
}

/**
 * Faut-il (re)copier la petite image de cette carte ? Seulement si son image principale est valide
 * ou cherchée par cette même tâche (la petite image attend alors la principale), et si la petite
 * image n'est pas déjà copiée : jamais traitée, ou absente / en erreur depuis plus de
 * `recheckDays` jours (même règle que l'image principale).
 */
export function needsLowImage(card: ExistingCard | undefined, mainQueued: boolean, nowMs: number, recheckDays: number): boolean {
  if (!card) return mainQueued
  if (card.image_low_status === 'downloaded') return false
  if (!isValidImageStatus(card.image_status) && !mainQueued) return false
  if (card.image_low_status === null || !card.image_low_checked_at) return true
  return nowMs - Date.parse(card.image_low_checked_at) > recheckDays * DAY_MS
}

/**
 * Faut-il (re)copier le logo de cette extension ? Si l'API en donne un et qu'il n'est pas déjà
 * copié : jamais traité, URL changée depuis la dernière vérification, ou absent / en erreur depuis
 * plus de `recheckDays` jours. Un logo copié n'est jamais remplacé.
 */
export function needsLogo(state: SetLogoState | null, logoUrl: string | null, nowMs: number, recheckDays: number): boolean {
  if (!logoUrl) return false
  if (!state || state.logo_status === null) return true
  if (state.logo_status === 'downloaded') return false
  if (state.logo_source_url !== `${logoUrl}.webp` || !state.logo_checked_at) return true
  return nowMs - Date.parse(state.logo_checked_at) > recheckDays * DAY_MS
}

/**
 * Tâche `set` : détail d'une extension et liste de ses cartes. Crée ou met à jour les cartes, puis
 * les tâches nécessaires : image pour toute carte sans image valide, détail pour toute carte
 * nouvelle, modifiée ou dont le détail a plus de `detailRefreshDays` jours, et, en dernière
 * priorité, petite image des cartes qui n'en ont pas et logo de l'extension s'il manque.
 */
export async function runSetJob(ctx: JobContext, job: Job): Promise<JobResult> {
  const language = jobLanguage(job)
  const setId = jobTarget(job)
  const { client, db, config } = ctx

  const outcome = await client.getJson(client.apiUrl(language, 'sets', setId))
  if (outcome.kind !== 'ok') return failureFromHttp(outcome, config, `extension ${language}/${setId}`)
  let detail
  try {
    detail = parseSetDetail(outcome.data)
  } catch (error) {
    return { kind: 'retry', error: `extension ${language}/${setId} illisible : ${String(error)}`, httpStatus: 200 }
  }
  if (detail.id !== setId) return { kind: 'failed', error: `extension ${setId} : l'API a répondu pour ${detail.id}`, httpStatus: 200 }

  // Série : détail de l'extension, sinon URL du logo, du symbole ou d'une image de carte.
  const seriesId =
    detail.serie?.id ??
    seriesFromAssetUrl(detail.logo, setId) ??
    seriesFromAssetUrl(detail.symbol, setId) ??
    detail.cards.map((card) => seriesFromAssetUrl(card.image, setId)).find((id) => id !== null) ??
    null

  const nowMs = ctx.now()
  const existing = new Map((await existingCardsOfSet(db, internalId(language, setId))).map((card) => [card.tcgdex_id, card]))
  const changedBriefs: CardBriefRow[] = []
  const cardJobs: NewJob[] = []
  const imageJobs: NewJob[] = []

  const listedCards = dedupeCards(detail.cards)
  for (const card of listedCards) {
    const brief = { id: card.id, localId: card.localId, name: card.name, image: card.image, series: seriesId }
    const hash = await contentHash(brief)
    const known = existing.get(card.id)
    if (!known) {
      changedBriefs.push({ id: card.id, localId: card.localId, name: card.name, image: card.image, hash })
      cardJobs.push(cardJob(language, card.id))
      imageJobs.push(imageJob(language, card.id))
      continue
    }
    const changed = known.brief_hash !== hash || known.removed_at !== null
    if (changed) changedBriefs.push({ id: card.id, localId: card.localId, name: card.name, image: card.image, hash })
    if (changed || needsDetail(known, nowMs, config.detailRefreshDays)) cardJobs.push(cardJob(language, card.id))
    const imageChanged = card.image !== known.api_image_url || (known.series_id === null && seriesId !== null)
    if (needsImage(known, imageChanged, nowMs, config.missingRecheckDays)) imageJobs.push(imageJob(language, card.id))
  }

  // Petites images (après l'image principale) et logo de l'extension.
  const mainQueued = new Set(imageJobs.map((item) => item.target))
  const lowJobs = listedCards
    .filter((card) => needsLowImage(existing.get(card.id), mainQueued.has(card.id), nowMs, config.missingRecheckDays))
    .map((card) => lowImageJob(language, card.id))
  const logo = detail.logo && needsLogo(await setLogoState(db, internalId(language, setId)), detail.logo, nowMs, config.missingRecheckDays) ? [logoJob(language, setId)] : []

  // Cartes qui ne sont plus listées : marquées, jamais supprimées. Seules comptent les cartes déjà
  // listées par cette extension (`brief_hash` connu) : une carte venue de l'index `/cards` n'y a
  // jamais figuré. Une liste vide ou une disparition de plus de 20 % des cartes est traitée comme
  // une anomalie de l'API, pas comme un retrait réel.
  const listed = new Set(detail.cards.map((card) => card.id))
  const vanished = [...existing.values()]
    .filter((card) => card.removed_at === null && card.brief_hash !== null && !listed.has(card.tcgdex_id))
    .map((card) => card.id)
  const plausibleRemoval = detail.cards.length > 0 && vanished.length <= Math.max(1, Math.floor(existing.size * 0.2))
  if (vanished.length && !plausibleRemoval) log('set_removal_skipped', { language, setId, vanished: vanished.length, listed: detail.cards.length })

  const statements: D1PreparedStatement[] = []
  if (detail.serie) statements.push(ensureSeriesStmt(db, language, detail.serie, nowMs))
  statements.push(
    upsertSetDetailStmt(
      db,
      language,
      {
        id: setId,
        name: detail.name,
        logo: detail.logo,
        symbol: detail.symbol,
        total: detail.cardCount.total,
        official: detail.cardCount.official,
        seriesTcgdexId: detail.serie?.id ?? null,
        releaseDate: detail.releaseDate,
        meta: JSON.stringify({ ...stripPricing(detail.raw), cardsListed: detail.cards.length }),
        hash: await contentHash({ raw: detail.raw, cards: detail.cards.length }),
      },
      nowMs,
    ),
    ...upsertCardBriefsStmts(db, language, setId, seriesId, changedBriefs, nowMs),
    ...enqueueStmts(db, 'image', language, imageJobs, { runId: job.run_id, maxAttempts: config.maxAttempts, nowMs }),
    ...enqueueStmts(db, 'card', language, cardJobs, { runId: job.run_id, maxAttempts: config.maxAttempts, nowMs }),
    ...enqueueMediaStmts(db, language, [...lowJobs, ...logo], { runId: job.run_id, maxAttempts: config.maxAttempts, nowMs }),
  )
  if (vanished.length && plausibleRemoval) statements.push(markRemovedStmt(db, vanished, nowMs))
  return { kind: 'done', statements, httpStatus: outcome.status }
}

/** Une carte listée deux fois dans la même extension n'est traitée qu'une fois. */
function dedupeCards(cards: CardBrief[]): CardBrief[] {
  const seen = new Map<string, CardBrief>()
  for (const card of cards) if (!seen.has(card.id)) seen.set(card.id, card)
  return [...seen.values()]
}

/**
 * Tâche `card_index` : compare la liste complète des cartes d'une langue (`/cards`) aux cartes déjà
 * connues par le détail des extensions. Chaque carte manquante (ex. extension absente de la liste
 * officielle) est créée tout de suite avec ce que donne l'index (nom, numéro, image), même si son
 * détail échoue ensuite, puis reçoit une tâche de détail. Attend que toutes les tâches `set` de la
 * langue soient terminées (attente bornée par le plafond de rendus).
 */
export async function runCardIndexJob(ctx: JobContext, job: Job): Promise<JobResult> {
  const language = jobLanguage(job)
  const { client, db, config } = ctx
  const pendingSets = await db.first<{ n: number }>(
    `SELECT COUNT(*) AS n FROM import_jobs WHERE job_type = 'set' AND language = ?1 AND status IN ('pending', 'running')`,
    language,
  )
  if ((pendingSets?.n ?? 0) > 0) {
    return { kind: 'release', reason: `en attente de ${pendingSets?.n} extension(s)`, httpStatus: null, delayMs: 10 * 60_000 }
  }

  const outcome = await client.getJson(client.apiUrl(language, 'cards'))
  if (outcome.kind !== 'ok') return failureFromHttp(outcome, config, `index des cartes ${language}`)
  let cards
  try {
    cards = parseCardList(outcome.data)
  } catch (error) {
    return { kind: 'retry', error: `index des cartes ${language} illisible : ${String(error)}`, httpStatus: 200 }
  }
  const known = new Set((await db.all<{ tcgdex_id: string }>(`SELECT tcgdex_id FROM cards WHERE language = ?1`, language)).map((row) => row.tcgdex_id))
  const orphans = cards.filter((card) => !known.has(card.id))
  const nowMs = ctx.now()
  log('card_index', { language, listed: cards.length, known: known.size, orphans: orphans.length })
  return {
    kind: 'done',
    httpStatus: outcome.status,
    statements: [
      ...insertOrphanBriefsStmts(db, language, orphans.map(({ id, localId, name, image }) => ({ id, localId, name, image })), nowMs),
      ...enqueueStmts(db, 'card', language, orphans.map((card) => cardJob(language, card.id, { orphan: true })), {
        runId: job.run_id,
        maxAttempts: config.maxAttempts,
        nowMs,
      }),
    ],
  }
}
