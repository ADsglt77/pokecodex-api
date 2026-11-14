/**
 * Séries, extensions et cartes (tables `series`, `sets`, `cards`).
 *
 * Les écritures en masse passent par un seul paramètre JSON (`json_each`) : une extension de 300
 * cartes s'écrit en une requête, sous la limite de 100 paramètres de D1. Les mises à jour ne
 * réécrivent une ligne que si son empreinte (`*_hash`) a changé, pour éviter les écritures inutiles.
 */

import type { Language } from '../config'
import type { Db } from '../db'
import { iso } from '../utils/time'

export const VALID_IMAGE_STATUSES = ['downloaded', 'recovered_from_cdn', 'already_present'] as const
export const MISSING_IMAGE_STATUSES = ['missing_api_url', 'not_found_on_cdn'] as const
export const ERROR_IMAGE_STATUSES = ['download_error', 'invalid_image'] as const
export type ImageStatus = 'pending' | (typeof VALID_IMAGE_STATUSES)[number] | (typeof MISSING_IMAGE_STATUSES)[number] | (typeof ERROR_IMAGE_STATUSES)[number]

export const isValidImageStatus = (status: string): boolean => (VALID_IMAGE_STATUSES as readonly string[]).includes(status)

/**
 * Statuts des petites images, logos et symboles (`image_low_status`, `logo_status`, `symbol_status`,
 * NULL = jamais traité) : `downloaded` est le seul qui signifie « présent dans R2 » (contrat avec le site).
 */
export type MediaStatus = 'downloaded' | (typeof MISSING_IMAGE_STATUSES)[number] | (typeof ERROR_IMAGE_STATUSES)[number]
export type MediaFailureStatus = Exclude<MediaStatus, 'downloaded'>

/** Images d'une extension copiées dans R2 : logo (colonnes `logo_*`) et symbole (`symbol_*`). */
export type SetMediaKind = 'logo' | 'symbol'

/** Blocs de lignes pour `json_each` (chaque valeur liée reste loin de la limite de 2 Mo). */
function chunks<T>(rows: T[], size = 400): T[][] {
  const out: T[][] = []
  for (let i = 0; i < rows.length; i += size) out.push(rows.slice(i, i + size))
  return out
}

// ——— Séries ———

export interface SeriesRow {
  id: string
  name: string
  logo: string | null
  meta: string
  hash: string
}

/** Écrit les séries de la liste officielle (la source la plus complète pour une série). */
export function upsertSeriesStmts(db: Db, language: Language, rows: SeriesRow[], nowMs: number): D1PreparedStatement[] {
  return chunks(rows).map((part) =>
    db.stmt(
      `INSERT INTO series (id, tcgdex_id, language, name, logo_url, metadata_json, content_hash, created_at, updated_at)
       SELECT ?1 || ':' || json_extract(value, '$.id'), json_extract(value, '$.id'), ?1, json_extract(value, '$.name'),
         json_extract(value, '$.logo'), json_extract(value, '$.meta'), json_extract(value, '$.hash'), ?2, ?2
       FROM json_each(?3) WHERE true
       ON CONFLICT (id) DO UPDATE SET name = excluded.name, logo_url = excluded.logo_url,
         metadata_json = excluded.metadata_json, content_hash = excluded.content_hash, updated_at = excluded.updated_at
       WHERE series.content_hash IS NOT excluded.content_hash`,
      language,
      iso(nowMs),
      JSON.stringify(part),
    ),
  )
}

/** Crée une série connue seulement par le détail d'une extension (sans écraser une série complète). */
export function ensureSeriesStmt(db: Db, language: Language, serie: { id: string; name: string }, nowMs: number): D1PreparedStatement {
  return db.stmt(
    `INSERT INTO series (id, tcgdex_id, language, name, metadata_json, created_at, updated_at)
     VALUES (?1 || ':' || ?2, ?2, ?1, ?3, ?4, ?5, ?5)
     ON CONFLICT (id) DO NOTHING`,
    language,
    serie.id,
    serie.name,
    JSON.stringify(serie),
    iso(nowMs),
  )
}

// ——— Extensions ———

export interface SetBriefRow {
  id: string
  name: string
  logo: string | null
  symbol: string | null
  total: number | null
  official: number | null
}

/** Crée les extensions encore inconnues (leur détail complet viendra de leur tâche `set`). */
export function insertSetBriefsStmts(db: Db, language: Language, rows: SetBriefRow[], nowMs: number): D1PreparedStatement[] {
  return chunks(rows).map((part) =>
    db.stmt(
      `INSERT INTO sets (id, tcgdex_id, language, name, logo_url, symbol_url, total_cards, official_cards,
         metadata_json, created_at, updated_at)
       SELECT ?1 || ':' || json_extract(value, '$.id'), json_extract(value, '$.id'), ?1, json_extract(value, '$.name'),
         json_extract(value, '$.logo'), json_extract(value, '$.symbol'), json_extract(value, '$.total'),
         json_extract(value, '$.official'), '{}', ?2, ?2
       FROM json_each(?3) WHERE true
       ON CONFLICT (id) DO NOTHING`,
      language,
      iso(nowMs),
      JSON.stringify(part),
    ),
  )
}

export interface SetDetailRow extends SetBriefRow {
  seriesTcgdexId: string | null
  releaseDate: string | null
  meta: string
  hash: string
}

/** Écrit le détail complet d'une extension, seulement s'il a changé. */
export function upsertSetDetailStmt(db: Db, language: Language, row: SetDetailRow, nowMs: number): D1PreparedStatement {
  return db.stmt(
    `INSERT INTO sets (id, tcgdex_id, series_id, language, name, release_date, total_cards, official_cards,
       logo_url, symbol_url, metadata_json, content_hash, detail_fetched_at, created_at, updated_at)
     VALUES (?1 || ':' || ?2, ?2, CASE WHEN ?3 IS NULL THEN NULL ELSE ?1 || ':' || ?3 END, ?1, ?4, ?5, ?6, ?7,
       ?8, ?9, ?10, ?11, ?12, ?12, ?12)
     ON CONFLICT (id) DO UPDATE SET series_id = excluded.series_id, name = excluded.name,
       release_date = excluded.release_date, total_cards = excluded.total_cards,
       official_cards = excluded.official_cards, logo_url = excluded.logo_url, symbol_url = excluded.symbol_url,
       metadata_json = excluded.metadata_json, content_hash = excluded.content_hash,
       detail_fetched_at = excluded.detail_fetched_at, updated_at = excluded.updated_at
     WHERE sets.content_hash IS NOT excluded.content_hash`,
    language,
    row.id,
    row.seriesTcgdexId,
    row.name,
    row.releaseDate,
    row.total,
    row.official,
    row.logo,
    row.symbol,
    row.meta,
    row.hash,
    iso(nowMs),
  )
}

/** Crée une extension connue seulement par le détail d'une carte (absente de la liste officielle). */
export function ensureSetStmt(db: Db, language: Language, set: SetBriefRow, nowMs: number): D1PreparedStatement {
  return db.stmt(
    `INSERT INTO sets (id, tcgdex_id, language, name, logo_url, symbol_url, total_cards, official_cards,
       metadata_json, created_at, updated_at)
     VALUES (?1 || ':' || ?2, ?2, ?1, ?3, ?4, ?5, ?6, ?7, '{}', ?8, ?8)
     ON CONFLICT (id) DO NOTHING`,
    language,
    set.id,
    set.name,
    set.logo,
    set.symbol,
    set.total,
    set.official,
    iso(nowMs),
  )
}

// ——— Cartes ———

export interface CardBriefRow {
  id: string
  localId: string
  name: string
  image: string | null
  hash: string
}

/**
 * Écrit les cartes listées par le détail d'une extension (nom, numéro, image officielle). Une carte
 * de nouveau listée perd sa marque `removed_at`.
 */
export function upsertCardBriefsStmts(
  db: Db,
  language: Language,
  setTcgdexId: string,
  seriesTcgdexId: string | null,
  rows: CardBriefRow[],
  nowMs: number,
): D1PreparedStatement[] {
  return chunks(rows).map((part) =>
    db.stmt(
      `INSERT INTO cards (id, tcgdex_id, language, name, local_id, set_id, series_id, api_image_url, brief_hash,
         created_at, updated_at)
       SELECT ?1 || ':' || json_extract(value, '$.id'), json_extract(value, '$.id'), ?1, json_extract(value, '$.name'),
         json_extract(value, '$.localId'), ?1 || ':' || ?2, ?3, json_extract(value, '$.image'),
         json_extract(value, '$.hash'), ?4, ?4
       FROM json_each(?5) WHERE true
       ON CONFLICT (id) DO UPDATE SET name = excluded.name, local_id = excluded.local_id, set_id = excluded.set_id,
         series_id = COALESCE(excluded.series_id, cards.series_id), api_image_url = excluded.api_image_url,
         brief_hash = excluded.brief_hash, removed_at = NULL, updated_at = excluded.updated_at
       WHERE cards.brief_hash IS NOT excluded.brief_hash OR cards.removed_at IS NOT NULL
         OR cards.set_id IS NOT excluded.set_id
         OR (excluded.series_id IS NOT NULL AND cards.series_id IS NOT excluded.series_id)`,
      language,
      setTcgdexId,
      seriesTcgdexId,
      iso(nowMs),
      JSON.stringify(part),
    ),
  )
}

/**
 * Crée les cartes de l'index `/cards` absentes du détail de toute extension (sans extension connue
 * pour l'instant ; leur détail la donnera). `brief_hash` reste NULL : une telle carte n'ayant
 * jamais figuré dans la liste d'une extension, elle ne peut pas être marquée retirée par celle-ci.
 */
export function insertOrphanBriefsStmts(db: Db, language: Language, rows: { id: string; localId: string; name: string; image: string | null }[], nowMs: number): D1PreparedStatement[] {
  return chunks(rows).map((part) =>
    db.stmt(
      `INSERT INTO cards (id, tcgdex_id, language, name, local_id, api_image_url, created_at, updated_at)
       SELECT ?1 || ':' || json_extract(value, '$.id'), json_extract(value, '$.id'), ?1, json_extract(value, '$.name'),
         json_extract(value, '$.localId'), json_extract(value, '$.image'), ?2, ?2
       FROM json_each(?3) WHERE true
       ON CONFLICT (id) DO NOTHING`,
      language,
      iso(nowMs),
      JSON.stringify(part),
    ),
  )
}

/** Marque les cartes qui ne sont plus listées par leur extension (sans rien supprimer). */
export function markRemovedStmt(db: Db, cardIds: string[], nowMs: number): D1PreparedStatement {
  return db.stmt(
    `UPDATE cards SET removed_at = ?1, updated_at = ?1
     WHERE removed_at IS NULL AND id IN (SELECT value FROM json_each(?2))`,
    iso(nowMs),
    JSON.stringify(cardIds),
  )
}

/** Cartes connues d'une extension, pour décider ce qui doit être rafraîchi. */
export interface ExistingCard {
  id: string
  tcgdex_id: string
  brief_hash: string | null
  detail_status: string
  detail_fetched_at: string | null
  image_status: string
  image_checked_at: string | null
  api_image_url: string | null
  series_id: string | null
  removed_at: string | null
  image_low_status: MediaStatus | null
  image_low_checked_at: string | null
}

export function existingCardsOfSet(db: Db, setInternalId: string): Promise<ExistingCard[]> {
  return db.all<ExistingCard>(
    `SELECT id, tcgdex_id, brief_hash, detail_status, detail_fetched_at, image_status, image_checked_at,
       api_image_url, series_id, removed_at, image_low_status, image_low_checked_at
     FROM cards WHERE set_id = ?1`,
    setInternalId,
  )
}

/** État du logo d'une extension (décision de la tâche `set`). */
export interface SetLogoState {
  logo_status: MediaStatus | null
  logo_checked_at: string | null
  logo_source_url: string | null
}

/** État du symbole d'une extension (décision de la tâche `set`). */
export interface SetSymbolState {
  symbol_status: MediaStatus | null
  symbol_checked_at: string | null
  symbol_source_url: string | null
}

export type SetMediaState = SetLogoState & SetSymbolState

/** Logo et symbole d'une extension, en une requête ; `null` si l'extension est inconnue. */
export function setMediaState(db: Db, setInternalId: string): Promise<SetMediaState | null> {
  return db.first<SetMediaState>(
    `SELECT logo_status, logo_checked_at, logo_source_url, symbol_status, symbol_checked_at, symbol_source_url FROM sets WHERE id = ?1`,
    setInternalId,
  )
}

export interface CardDetailRow {
  id: string
  localId: string
  name: string
  setTcgdexId: string | null
  seriesTcgdexId: string | null
  rarity: string | null
  category: string | null
  illustrator: string | null
  image: string | null
  meta: string
  hash: string
}

/**
 * Écrit le détail complet d'une carte (crée la carte si elle n'était connue que par l'index).
 * L'extension où la carte est listée prime sur celle de son détail : TCGdex liste par exemple les
 * cartes `pl4-AR*` dans `pl3`, alors que leur détail indique `pl4` (absente de la liste officielle).
 */
export function upsertCardDetailStmt(db: Db, language: Language, row: CardDetailRow, nowMs: number): D1PreparedStatement {
  return db.stmt(
    `INSERT INTO cards (id, tcgdex_id, language, name, local_id, set_id, series_id, rarity, category, illustrator,
       api_image_url, detail_hash, detail_status, detail_fetched_at, metadata_json, created_at, updated_at)
     VALUES (?1 || ':' || ?2, ?2, ?1, ?3, ?4, CASE WHEN ?5 IS NULL THEN NULL ELSE ?1 || ':' || ?5 END, ?6, ?7, ?8, ?9,
       ?10, ?11, 'fetched', ?12, ?13, ?12, ?12)
     ON CONFLICT (id) DO UPDATE SET name = excluded.name, local_id = excluded.local_id,
       set_id = COALESCE(cards.set_id, excluded.set_id), series_id = COALESCE(cards.series_id, excluded.series_id),
       rarity = excluded.rarity, category = excluded.category, illustrator = excluded.illustrator,
       api_image_url = COALESCE(excluded.api_image_url, cards.api_image_url), detail_hash = excluded.detail_hash,
       detail_status = 'fetched', detail_fetched_at = excluded.detail_fetched_at,
       metadata_json = excluded.metadata_json, updated_at = excluded.updated_at`,
    language,
    row.id,
    row.name,
    row.localId,
    row.setTcgdexId,
    row.seriesTcgdexId,
    row.rarity,
    row.category,
    row.illustrator,
    row.image,
    row.hash,
    iso(nowMs),
    row.meta,
  )
}

export function markDetailStmt(db: Db, cardId: string, status: 'not_found' | 'error', nowMs: number): D1PreparedStatement {
  return db.stmt(`UPDATE cards SET detail_status = ?2, detail_fetched_at = ?3, updated_at = ?3 WHERE id = ?1`, cardId, status, iso(nowMs))
}

/** Ce que les tâches de carte et d'image lisent d'une carte. */
export interface CardForJob {
  id: string
  tcgdex_id: string
  language: Language
  name: string
  local_id: string
  set_id: string | null
  set_tcgdex_id: string | null
  series_id: string | null
  set_logo_url: string | null
  set_symbol_url: string | null
  api_image_url: string | null
  image_status: string
  image_r2_key: string | null
  image_checked_at: string | null
  detail_status: string
  /** Image principale réellement copiée : URL, qualité, SHA-256 (base de la petite image). */
  image_source_url: string | null
  image_quality: string | null
  image_sha256: string | null
  image_low_status: MediaStatus | null
  image_low_r2_key: string | null
}

/** Lit en une requête les cartes de plusieurs tâches. */
export async function cardsForJobs(db: Db, cardIds: string[]): Promise<Map<string, CardForJob>> {
  if (!cardIds.length) return new Map()
  const rows = await db.all<CardForJob>(
    `SELECT c.id, c.tcgdex_id, c.language, c.name, c.local_id, c.set_id, s.tcgdex_id AS set_tcgdex_id,
       COALESCE(c.series_id, se.tcgdex_id) AS series_id, s.logo_url AS set_logo_url, s.symbol_url AS set_symbol_url,
       c.api_image_url, c.image_status, c.image_r2_key, c.image_checked_at, c.detail_status,
       c.image_source_url, c.image_quality, c.image_sha256, c.image_low_status, c.image_low_r2_key
     FROM cards c
     LEFT JOIN sets s ON s.id = c.set_id
     LEFT JOIN series se ON se.id = s.series_id
     WHERE c.id IN (SELECT value FROM json_each(?1))`,
    JSON.stringify([...new Set(cardIds)]),
  )
  return new Map(rows.map((row) => [row.id, row]))
}

export interface ImageSuccess {
  status: 'downloaded' | 'recovered_from_cdn' | 'already_present'
  source: 'api' | 'cdn_reconstructed' | null
  sourceUrl: string | null
  key: string
  quality: string | null
  size: number
  sha256: string | null
  testedUrls: string
  httpStatus: number | null
}

export function imageSuccessStmt(db: Db, cardId: string, image: ImageSuccess, nowMs: number): D1PreparedStatement {
  return db.stmt(
    `UPDATE cards SET image_status = ?2, image_source = ?3, image_source_url = ?4, image_r2_key = ?5,
       image_quality = ?6, image_file_size = ?7, image_sha256 = ?8, image_downloaded_at = ?9, image_checked_at = ?9,
       image_tested_urls = ?10, image_http_status = ?11, image_failure_reason = NULL, updated_at = ?9
     WHERE id = ?1`,
    cardId,
    image.status,
    image.source,
    image.sourceUrl,
    image.key,
    image.quality,
    image.size,
    image.sha256,
    iso(nowMs),
    image.testedUrls,
    image.httpStatus,
  )
}

/**
 * Enregistre un échec (image manquante ou erreur), sans jamais toucher à une image déjà valide :
 * une copie réussie n'est pas effacée par une vérification ultérieure.
 */
export function imageFailureStmt(
  db: Db,
  cardId: string,
  failure: { status: ImageStatus; reason: string; testedUrls: string; httpStatus: number | null },
  nowMs: number,
): D1PreparedStatement {
  return db.stmt(
    `UPDATE cards SET image_status = ?2, image_failure_reason = ?3,
       image_tested_urls = COALESCE(NULLIF(?4, '[]'), image_tested_urls), image_http_status = COALESCE(?5, image_http_status),
       image_checked_at = ?6, updated_at = ?6
     WHERE id = ?1 AND image_status NOT IN ('downloaded', 'recovered_from_cdn', 'already_present')`,
    cardId,
    failure.status,
    failure.reason,
    failure.testedUrls,
    failure.httpStatus,
    iso(nowMs),
  )
}

// ——— Petites images et logos ———

/**
 * Petite image copiée (ou déjà présente dans R2, ou image principale déjà en qualité `low`). `size`
 * est NULL quand la petite image réutilise l'objet de l'image principale (octets déjà comptés).
 */
export function lowImageSuccessStmt(db: Db, cardId: string, image: { key: string; size: number | null; sha256: string | null }, nowMs: number): D1PreparedStatement {
  return db.stmt(
    `UPDATE cards SET image_low_status = 'downloaded', image_low_r2_key = ?2, image_low_file_size = ?3,
       image_low_sha256 = ?4, image_low_checked_at = ?5, image_low_failure_reason = NULL, updated_at = ?5
     WHERE id = ?1`,
    cardId,
    image.key,
    image.size,
    image.sha256,
    iso(nowMs),
  )
}

/** Échec d'une petite image, sans jamais effacer une petite image déjà copiée. */
export function lowImageFailureStmt(db: Db, cardId: string, failure: { status: MediaFailureStatus; reason: string }, nowMs: number): D1PreparedStatement {
  return db.stmt(
    `UPDATE cards SET image_low_status = ?2, image_low_failure_reason = ?3, image_low_checked_at = ?4, updated_at = ?4
     WHERE id = ?1 AND image_low_status IS NOT 'downloaded'`,
    cardId,
    failure.status,
    failure.reason,
    iso(nowMs),
  )
}

/** Ce que les tâches de logo et de symbole lisent d'une extension. */
export interface SetForMediaJob {
  id: string
  tcgdex_id: string
  language: Language
  logo_url: string | null
  logo_status: MediaStatus | null
  logo_r2_key: string | null
  symbol_url: string | null
  symbol_status: MediaStatus | null
  symbol_r2_key: string | null
}

/** Lit en une requête les extensions de plusieurs tâches de logo ou de symbole. */
export async function setsForMediaJobs(db: Db, setIds: string[]): Promise<Map<string, SetForMediaJob>> {
  if (!setIds.length) return new Map()
  const rows = await db.all<SetForMediaJob>(
    `SELECT id, tcgdex_id, language, logo_url, logo_status, logo_r2_key, symbol_url, symbol_status, symbol_r2_key
     FROM sets WHERE id IN (SELECT value FROM json_each(?1))`,
    JSON.stringify([...new Set(setIds)]),
  )
  return new Map(rows.map((row) => [row.id, row]))
}

export interface SetMediaSuccess {
  key: string
  /** URL testée ; `null` (objet déjà présent, réconcilié) garde la précédente. */
  sourceUrl: string | null
  size: number
  sha256: string | null
}

export interface SetMediaFailure {
  status: MediaFailureStatus
  reason: string
  sourceUrl?: string | null
}

// `kind` vient d'une union fermée (jamais d'une donnée externe) : seuls `logo_*` et `symbol_*`
// peuvent être nommés ; toutes les valeurs restent des paramètres liés.
function setMediaSuccessStmt(db: Db, kind: SetMediaKind, setId: string, media: SetMediaSuccess, nowMs: number): D1PreparedStatement {
  return db.stmt(
    `UPDATE sets SET ${kind}_status = 'downloaded', ${kind}_r2_key = ?2, ${kind}_source_url = COALESCE(?3, ${kind}_source_url),
       ${kind}_file_size = ?4, ${kind}_sha256 = ?5, ${kind}_checked_at = ?6, ${kind}_failure_reason = NULL, updated_at = ?6
     WHERE id = ?1`,
    setId,
    media.key,
    media.sourceUrl,
    media.size,
    media.sha256,
    iso(nowMs),
  )
}

/** Échec d'une image d'extension, sans jamais effacer une image déjà copiée. */
function setMediaFailureStmt(db: Db, kind: SetMediaKind, setId: string, failure: SetMediaFailure, nowMs: number): D1PreparedStatement {
  return db.stmt(
    `UPDATE sets SET ${kind}_status = ?2, ${kind}_failure_reason = ?3, ${kind}_source_url = COALESCE(?4, ${kind}_source_url),
       ${kind}_checked_at = ?5, updated_at = ?5
     WHERE id = ?1 AND ${kind}_status IS NOT 'downloaded'`,
    setId,
    failure.status,
    failure.reason,
    failure.sourceUrl ?? null,
    iso(nowMs),
  )
}

export const logoSuccessStmt = (db: Db, setId: string, logo: SetMediaSuccess, nowMs: number): D1PreparedStatement => setMediaSuccessStmt(db, 'logo', setId, logo, nowMs)

/** Échec d'un logo, sans jamais effacer un logo déjà copié. */
export const logoFailureStmt = (db: Db, setId: string, failure: SetMediaFailure, nowMs: number): D1PreparedStatement => setMediaFailureStmt(db, 'logo', setId, failure, nowMs)

export const symbolSuccessStmt = (db: Db, setId: string, symbol: SetMediaSuccess, nowMs: number): D1PreparedStatement => setMediaSuccessStmt(db, 'symbol', setId, symbol, nowMs)

/** Échec d'un symbole, sans jamais effacer un symbole déjà copié. */
export const symbolFailureStmt = (db: Db, setId: string, failure: SetMediaFailure, nowMs: number): D1PreparedStatement => setMediaFailureStmt(db, 'symbol', setId, failure, nowMs)
