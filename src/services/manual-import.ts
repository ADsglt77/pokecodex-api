/**
 * Import manuel (API d'administration) de lignes et d'images venues d'une autre source que TCGdex :
 * aujourd'hui la copie Pokécardex, envoyée depuis le poste de l'utilisateur (`scripts/`).
 *
 * Règles, toutes vérifiées ici et non par l'appelant :
 * - **jamais d'écrasement** : une extension ou une carte déjà en base (TCGdex ou import précédent)
 *   n'est pas modifiée ; une image n'est écrite que si la carte n'en a aucune de valide ; un objet R2
 *   déjà présent à la clé n'est jamais remplacé (seul le même contenu, au SHA-256 près, est réconcilié) ;
 * - **contrat du site** (`pokedex-data`, tâche `catalog`) : id d'extension `[A-Za-z0-9._-]{1,32}`
 *   unique par langue au sens de `setKey` (majuscules, sans `-`), numéro `^[a-z]*\d+[a-z]*$` unique dans
 *   l'extension au sens de `numberKey`, catégorie dans le vocabulaire TCGdex de la langue, métadonnées
 *   JSON valides (`dexId`, `variants_detailed` en tableaux, `variants` en objet), date passée ou nulle,
 *   série TCGdex existante de la même langue ;
 * - lignes marquées `data_source = 'pokecardex'`, `brief_hash` NULL (jamais retirées par la
 *   synchronisation TCGdex) ; images et logos marqués `image_provider` / `logo_provider`.
 */

import { IMAGES_PREFIX, type Config, type Language } from '../config'
import type { Db } from '../db'
import { checkWebp } from '../tcgdex/images'
import { cardImageKey, internalId, lowImageKey, setLogoKey } from '../utils/keys'
import { errorMessage } from '../utils/log'
import { iso, utcDay } from '../utils/time'

export const MANUAL_SOURCE = 'pokecardex'

/** Taille maximale d'un lot de catalogue (JSON). */
export const MAX_CATALOG_BYTES = 4_000_000
/** Taille maximale des métadonnées d'une carte ou d'une extension (JSON). */
const MAX_METADATA_BYTES = 16_000

const SET_ID = /^[A-Za-z0-9._-]{1,32}$/
const CARD_ID = /^[A-Za-z0-9._-]{1,80}$/
const LOCAL_ID = /^[A-Za-z]*\d+[A-Za-z]*$/
const DAY = /^\d{4}-\d{2}-\d{2}$/
const CATEGORIES: Record<Language, readonly string[]> = { fr: ['Pokémon', 'Dresseur', 'Énergie'], ja: ['Pokemon', 'Trainer', 'Energy'] }

/** Clé de comparaison des ids d'extension du site : majuscules, sans `-`. */
export const setKey = (id: string): string => id.toUpperCase().replaceAll('-', '')
/** Clé de comparaison des numéros du site : majuscules, zéros de tête du nombre retirés (`017` ≡ `17`). */
export const numberKey = (localId: string): string => localId.toUpperCase().replace(/^([A-Z]*)0+(?=\d)/, '$1')

export interface ManualSeries {
  id: string
  name: string
}

export interface ManualSet {
  id: string
  name: string
  series: string | null
  releaseDate: string | null
  officialCards: number | null
  totalCards: number | null
  metadata: Record<string, unknown>
}

export interface ManualCard {
  id: string
  set: string
  localId: string
  name: string
  rarity: string | null
  category: string | null
  illustrator: string | null
  metadata: Record<string, unknown>
}

export interface Rejection {
  id: string
  reason: string
}

export interface CatalogImportResult {
  series: { inserted: number; existing: number }
  sets: { inserted: number; existing: number; rejected: Rejection[] }
  cards: { inserted: number; existing: number; rejected: Rejection[] }
}

export class InputError extends Error {}

const isObject = (value: unknown): value is Record<string, unknown> => typeof value === 'object' && value !== null && !Array.isArray(value)

function text(value: unknown, field: string, max: number, nullable: true): string | null
function text(value: unknown, field: string, max: number, nullable: false): string
function text(value: unknown, field: string, max: number, nullable: boolean): string | null {
  if (value === null || value === undefined || value === '') {
    if (nullable) return null
    throw new InputError(`${field} manquant`)
  }
  if (typeof value !== 'string' || value.length > max || value.trim() !== value) throw new InputError(`${field} invalide`)
  return value
}

function count(value: unknown, field: string): number | null {
  if (value === null || value === undefined) return null
  if (!Number.isInteger(value) || (value as number) < 0 || (value as number) > 100_000) throw new InputError(`${field} invalide`)
  return value as number
}

/** Métadonnées : objet JSON dont les champs lus par le site ont la bonne forme (sinon sa tâche échoue). */
function metadata(value: unknown, field: string): Record<string, unknown> {
  if (value === undefined || value === null) return {}
  if (!isObject(value)) throw new InputError(`${field} : objet attendu`)
  if ('dexId' in value && !(Array.isArray(value.dexId) && value.dexId.every((n) => Number.isInteger(n) && n > 0 && n <= 2_000))) {
    throw new InputError(`${field}.dexId : tableau d'entiers attendu`)
  }
  if ('variants_detailed' in value && !(Array.isArray(value.variants_detailed) && value.variants_detailed.every(isObject))) {
    throw new InputError(`${field}.variants_detailed : tableau d'objets attendu`)
  }
  if ('variants' in value && !isObject(value.variants)) throw new InputError(`${field}.variants : objet attendu`)
  for (const name of ['types'] as const) {
    if (name in value && !(Array.isArray(value[name]) && value[name].every((item) => typeof item === 'string'))) throw new InputError(`${field}.${name} : tableau de textes attendu`)
  }
  for (const name of ['trainerType', 'energyType'] as const) {
    if (name in value && typeof value[name] !== 'string') throw new InputError(`${field}.${name} : texte attendu`)
  }
  if (JSON.stringify(value).length > MAX_METADATA_BYTES) throw new InputError(`${field} : métadonnées trop volumineuses`)
  return value
}

/** Lit et valide un lot de catalogue ; lève `InputError` au premier défaut de forme. */
export function parseCatalogBatch(raw: unknown, today: string): { language: Language; series: ManualSeries[]; sets: ManualSet[]; cards: ManualCard[] } {
  if (!isObject(raw)) throw new InputError('objet JSON attendu')
  const language = raw.language
  if (language !== 'fr' && language !== 'ja') throw new InputError('language : fr ou ja')
  const rawSeries = raw.series ?? []
  const rawSets = raw.sets ?? []
  const rawCards = raw.cards ?? []
  if (!Array.isArray(rawSeries) || !Array.isArray(rawSets) || !Array.isArray(rawCards)) throw new InputError('series, sets et cards : tableaux attendus')
  if (rawSeries.length > 100 || rawSets.length > 500 || rawCards.length > 2_000) throw new InputError('lot trop grand (100 séries, 500 extensions, 2 000 cartes au plus)')

  const series = rawSeries.map((item, i): ManualSeries => {
    if (!isObject(item)) throw new InputError(`series[${i}] : objet attendu`)
    const id = text(item.id, `series[${i}].id`, 32, false)
    // `tcgp` est la série exclue par le site (Pokémon TCG Pocket) : jamais créée ici.
    if (!SET_ID.test(id) || id === '.' || id === '..' || id.toLowerCase() === 'tcgp') throw new InputError(`series[${i}].id invalide : ${id}`)
    return { id, name: text(item.name, `series[${i}].name`, 200, false) }
  })

  const sets = rawSets.map((item, i): ManualSet => {
    if (!isObject(item)) throw new InputError(`sets[${i}] : objet attendu`)
    const id = text(item.id, `sets[${i}].id`, 32, false)
    if (!SET_ID.test(id) || id === '.' || id === '..') throw new InputError(`sets[${i}].id invalide : ${id}`)
    const releaseDate = text(item.releaseDate, `sets[${i}].releaseDate`, 10, true)
    if (releaseDate !== null && !DAY.test(releaseDate)) throw new InputError(`sets[${i}].releaseDate : AAAA-MM-JJ attendu`)
    return {
      id,
      name: text(item.name, `sets[${i}].name`, 200, false),
      series: text(item.series, `sets[${i}].series`, 64, true),
      // Une date à venir ferait de l'extension la page par défaut du Catalogue du site.
      releaseDate: releaseDate !== null && releaseDate <= today && releaseDate > '1990' ? releaseDate : null,
      officialCards: count(item.officialCards, `sets[${i}].officialCards`),
      totalCards: count(item.totalCards, `sets[${i}].totalCards`),
      metadata: metadata(item.metadata, `sets[${i}].metadata`),
    }
  })
  const cards = rawCards.map((item, i): ManualCard => {
    if (!isObject(item)) throw new InputError(`cards[${i}] : objet attendu`)
    const id = text(item.id, `cards[${i}].id`, 80, false)
    if (!CARD_ID.test(id)) throw new InputError(`cards[${i}].id invalide : ${id}`)
    const localId = text(item.localId, `cards[${i}].localId`, 16, false)
    if (!LOCAL_ID.test(localId)) throw new InputError(`cards[${i}].localId invalide : ${localId}`)
    const category = text(item.category, `cards[${i}].category`, 20, true)
    if (category !== null && !CATEGORIES[language].includes(category)) throw new InputError(`cards[${i}].category hors vocabulaire : ${category}`)
    return {
      id,
      set: text(item.set, `cards[${i}].set`, 32, false),
      localId,
      name: text(item.name, `cards[${i}].name`, 200, false),
      rarity: text(item.rarity, `cards[${i}].rarity`, 60, true),
      category,
      illustrator: text(item.illustrator, `cards[${i}].illustrator`, 200, true),
      metadata: metadata(item.metadata, `cards[${i}].metadata`),
    }
  })
  return { language, series, sets, cards }
}

function chunks<T>(rows: T[], size = 200): T[][] {
  const out: T[][] = []
  for (let i = 0; i < rows.length; i += size) out.push(rows.slice(i, i + size))
  return out
}

/**
 * Ajoute les extensions et les cartes d'un lot, sans jamais modifier une ligne existante. Chaque
 * élément refusé l'est avec son motif ; le reste est écrit dans un seul batch (tout ou rien).
 */
export async function importCatalog(db: Db, raw: unknown, nowMs: number): Promise<CatalogImportResult> {
  const { language, series, sets, cards } = parseCatalogBatch(raw, utcDay(nowMs))
  const now = iso(nowMs)
  const result: CatalogImportResult = {
    series: { inserted: 0, existing: 0 },
    sets: { inserted: 0, existing: 0, rejected: [] },
    cards: { inserted: 0, existing: 0, rejected: [] },
  }

  const knownSets = await db.all<{ tcgdex_id: string }>(`SELECT tcgdex_id FROM sets WHERE language = ?1`, language)
  const knownSeries = new Set((await db.all<{ tcgdex_id: string }>(`SELECT tcgdex_id FROM series WHERE language = ?1`, language)).map((row) => row.tcgdex_id))
  // Séries : ajoutées seulement si l'id est libre dans la langue (une série existante n'est jamais modifiée).
  const newSeries = series.filter((item, i) => !knownSeries.has(item.id) && series.findIndex((other) => other.id === item.id) === i)
  result.series.existing = series.length - newSeries.length
  for (const item of newSeries) knownSeries.add(item.id)
  const setIds = new Set(knownSets.map((row) => row.tcgdex_id))
  const setKeys = new Map(knownSets.map((row) => [setKey(row.tcgdex_id), row.tcgdex_id]))

  const newSets: ManualSet[] = []
  for (const set of sets) {
    if (setIds.has(set.id)) {
      result.sets.existing++
      continue
    }
    const clash = setKeys.get(setKey(set.id))
    if (clash !== undefined) {
      result.sets.rejected.push({ id: set.id, reason: `même code que l'extension ${clash} pour le site (setKey)` })
      continue
    }
    if (set.series !== null && !knownSeries.has(set.series)) {
      result.sets.rejected.push({ id: set.id, reason: `série inconnue dans cette langue : ${set.series}` })
      continue
    }
    newSets.push(set)
    setIds.add(set.id)
    setKeys.set(setKey(set.id), set.id)
  }

  // Cartes : extension connue (en base ou dans le lot), id libre, numéro libre dans l'extension.
  const targetSets = [...new Set(cards.map((card) => card.set))].filter((id) => setIds.has(id))
  const existingCards = new Set<string>()
  const numbers = new Map<string, Set<string>>()
  for (const part of chunks(targetSets, 50)) {
    const rows = await db.all<{ tcgdex_id: string; local_id: string; set_tcgdex: string }>(
      `SELECT c.tcgdex_id, c.local_id, s.tcgdex_id AS set_tcgdex FROM cards c JOIN sets s ON s.id = c.set_id
       WHERE s.language = ?1 AND s.tcgdex_id IN (SELECT value FROM json_each(?2))`,
      language,
      JSON.stringify(part),
    )
    for (const row of rows) {
      const used = numbers.get(row.set_tcgdex) ?? new Set<string>()
      used.add(numberKey(row.local_id))
      numbers.set(row.set_tcgdex, used)
    }
  }
  for (const part of chunks(cards.map((card) => card.id), 500)) {
    const rows = await db.all<{ tcgdex_id: string }>(
      `SELECT tcgdex_id FROM cards WHERE language = ?1 AND tcgdex_id IN (SELECT value FROM json_each(?2))`,
      language,
      JSON.stringify(part),
    )
    for (const row of rows) existingCards.add(row.tcgdex_id)
  }

  const newCards: ManualCard[] = []
  const batchIds = new Set<string>()
  for (const card of cards) {
    if (existingCards.has(card.id)) {
      result.cards.existing++
      continue
    }
    if (!setIds.has(card.set)) {
      result.cards.rejected.push({ id: card.id, reason: `extension inconnue : ${card.set}` })
      continue
    }
    if (batchIds.has(card.id)) {
      result.cards.rejected.push({ id: card.id, reason: 'id en double dans le lot' })
      continue
    }
    const used = numbers.get(card.set) ?? new Set<string>()
    const key = numberKey(card.localId)
    if (used.has(key)) {
      result.cards.rejected.push({ id: card.id, reason: `numéro ${card.localId} déjà pris dans ${card.set} (numberKey)` })
      continue
    }
    used.add(key)
    numbers.set(card.set, used)
    batchIds.add(card.id)
    newCards.push(card)
  }

  const statements: D1PreparedStatement[] = []
  if (newSeries.length) {
    statements.push(
      db.stmt(
        `INSERT INTO series (id, tcgdex_id, language, name, metadata_json, data_source, created_at, updated_at)
         SELECT ?1 || ':' || json_extract(value, '$.id'), json_extract(value, '$.id'), ?1, json_extract(value, '$.name'),
           '{}', ?2, ?3, ?3
         FROM json_each(?4) WHERE true
         ON CONFLICT DO NOTHING`,
        language,
        MANUAL_SOURCE,
        now,
        JSON.stringify(newSeries),
      ),
    )
  }
  for (const part of chunks(newSets)) {
    statements.push(
      db.stmt(
        `INSERT INTO sets (id, tcgdex_id, series_id, language, name, release_date, total_cards, official_cards,
           metadata_json, data_source, created_at, updated_at)
         SELECT ?1 || ':' || json_extract(value, '$.id'), json_extract(value, '$.id'),
           CASE WHEN json_extract(value, '$.series') IS NULL THEN NULL ELSE ?1 || ':' || json_extract(value, '$.series') END,
           ?1, json_extract(value, '$.name'), json_extract(value, '$.releaseDate'), json_extract(value, '$.totalCards'),
           json_extract(value, '$.officialCards'), json(json_extract(value, '$.metadata')), ?2, ?3, ?3
         FROM json_each(?4) WHERE true
         ON CONFLICT DO NOTHING`,
        language,
        MANUAL_SOURCE,
        now,
        JSON.stringify(part.map((set) => ({ ...set, metadata: JSON.stringify(set.metadata) }))),
      ),
    )
  }
  for (const part of chunks(newCards)) {
    statements.push(
      db.stmt(
        `INSERT INTO cards (id, tcgdex_id, language, name, local_id, set_id, series_id, rarity, category, illustrator,
           metadata_json, data_source, created_at, updated_at)
         SELECT ?1 || ':' || json_extract(value, '$.id'), json_extract(value, '$.id'), ?1, json_extract(value, '$.name'),
           json_extract(value, '$.localId'), ?1 || ':' || json_extract(value, '$.set'),
           (SELECT se.tcgdex_id FROM sets s JOIN series se ON se.id = s.series_id WHERE s.id = ?1 || ':' || json_extract(value, '$.set')),
           json_extract(value, '$.rarity'), json_extract(value, '$.category'), json_extract(value, '$.illustrator'),
           json(json_extract(value, '$.metadata')), ?2, ?3, ?3
         FROM json_each(?4) WHERE true
         ON CONFLICT DO NOTHING`,
        language,
        MANUAL_SOURCE,
        now,
        JSON.stringify(part.map((card) => ({ ...card, metadata: JSON.stringify(card.metadata) }))),
      ),
    )
  }
  const written = await db.batch(statements)
  const seriesStatements = newSeries.length ? 1 : 0
  const setStatements = chunks(newSets).length
  written.forEach((res, i) => {
    const changes = res.meta.changes ?? 0
    if (i < seriesStatements) result.series.inserted += changes
    else if (i < seriesStatements + setStatements) result.sets.inserted += changes
    else result.cards.inserted += changes
  })
  return result
}

// ——— Images ———

export type ImageKind = 'main' | 'low' | 'logo'

export interface ImageUpload {
  kind: ImageKind
  language: Language
  /** Id TCGdex de la carte (`main`, `low`) ou de l'extension (`logo`). */
  target: string
  /** Adresse d'origine du fichier, gardée pour la traçabilité (jamais téléchargée par le Worker). */
  sourceUrl: string
  bytes: Uint8Array
}

export type ImageOutcome =
  | { status: 'stored' | 'reconciled'; key: string; size: number; sha256: string }
  | { status: 'unchanged'; key: string | null; reason: string }
  | { status: 'conflict' | 'not_found' | 'invalid'; reason: string }

const VALID_IMAGE = ['downloaded', 'recovered_from_cdn', 'already_present']

interface CardRow {
  id: string
  tcgdex_id: string
  set_tcgdex: string | null
  image_status: string
  image_r2_key: string | null
  image_provider: string | null
  image_low_status: string | null
  removed_at: string | null
}

interface SetRow {
  id: string
  tcgdex_id: string
  logo_status: string | null
  logo_r2_key: string | null
}

/**
 * Écrit une image envoyée par l'administrateur, puis met D1 à jour :
 * - `main` : image principale d'une carte qui n'en a aucune de valide ;
 * - `low` : petite image d'une carte dont l'image principale vient de la même source ;
 * - `logo` : logo d'une extension qui n'en a pas.
 * Le fichier doit être un WebP valide ; un objet déjà présent à la clé n'est jamais remplacé.
 */
export async function storeImage(db: Db, bucket: R2Bucket, config: Pick<Config, 'minImageBytes' | 'maxImageBytes'>, upload: ImageUpload, nowMs: number): Promise<ImageOutcome> {
  const check = await checkWebp(upload.bytes, new Headers({ 'content-type': 'image/webp', 'content-length': String(upload.bytes.byteLength) }), config)
  if (!check.ok) return { status: 'invalid', reason: check.reason }
  const now = iso(nowMs)

  let key: string
  let identity: Record<string, string>
  let update: (size: number, sha256: string) => D1PreparedStatement
  if (upload.kind === 'logo') {
    const set = await db.first<SetRow>(
      `SELECT id, tcgdex_id, logo_status, logo_r2_key FROM sets WHERE id = ?1`,
      internalId(upload.language, upload.target),
    )
    if (!set) return { status: 'not_found', reason: `extension ${upload.language}/${upload.target} inconnue` }
    if (set.logo_status === 'downloaded' && set.logo_r2_key) return { status: 'unchanged', key: set.logo_r2_key, reason: 'logo déjà présent' }
    key = setLogoKey(upload.language, set.tcgdex_id)
    identity = { variant: 'logo', language: upload.language, setId: set.tcgdex_id }
    update = (size, sha256) =>
      db.stmt(
        `UPDATE sets SET logo_status = 'downloaded', logo_r2_key = ?2, logo_checked_at = ?3, logo_source_url = ?4,
           logo_file_size = ?5, logo_sha256 = ?6, logo_failure_reason = NULL, logo_provider = ?7, updated_at = ?3
         WHERE id = ?1 AND logo_status IS NOT 'downloaded'`,
        set.id,
        key,
        now,
        upload.sourceUrl,
        size,
        sha256,
        MANUAL_SOURCE,
      )
  } else {
    const card = await db.first<CardRow>(
      `SELECT c.id, c.tcgdex_id, s.tcgdex_id AS set_tcgdex, c.image_status, c.image_r2_key, c.image_provider,
         c.image_low_status, c.removed_at
       FROM cards c LEFT JOIN sets s ON s.id = c.set_id WHERE c.id = ?1`,
      internalId(upload.language, upload.target),
    )
    if (!card || !card.set_tcgdex) return { status: 'not_found', reason: `carte ${upload.language}/${upload.target} inconnue` }
    const hasMain = VALID_IMAGE.includes(card.image_status) && card.image_r2_key !== null
    if (upload.kind === 'main') {
      if (hasMain) return { status: 'unchanged', key: card.image_r2_key, reason: 'image déjà présente' }
      key = cardImageKey(upload.language, card.set_tcgdex, card.tcgdex_id)
      identity = { tcgdexId: card.tcgdex_id, quality: 'high' }
      update = (size, sha256) =>
        db.stmt(
          `UPDATE cards SET image_status = 'downloaded', image_source = NULL, image_source_url = ?2, image_r2_key = ?3,
             image_quality = 'high', image_file_size = ?4, image_sha256 = ?5, image_downloaded_at = ?6, image_checked_at = ?6,
             image_tested_urls = '[]', image_http_status = NULL, image_failure_reason = NULL, image_provider = ?7, updated_at = ?6
           WHERE id = ?1 AND (image_status NOT IN ('downloaded', 'recovered_from_cdn', 'already_present') OR image_r2_key IS NULL)`,
          card.id,
          upload.sourceUrl,
          key,
          size,
          sha256,
          now,
          MANUAL_SOURCE,
        )
    } else {
      if (!hasMain || card.image_provider !== MANUAL_SOURCE || !card.image_r2_key) {
        return { status: 'conflict', reason: 'petite image refusée : l’image principale de la carte ne vient pas de cette source' }
      }
      if (card.image_low_status === 'downloaded') return { status: 'unchanged', key: null, reason: 'petite image déjà présente' }
      const low = lowImageKey(card.image_r2_key)
      if (!low) return { status: 'conflict', reason: `clé principale inattendue : ${card.image_r2_key}` }
      key = low
      identity = { variant: 'low', language: upload.language, tcgdexId: card.tcgdex_id }
      update = (size, sha256) =>
        db.stmt(
          `UPDATE cards SET image_low_status = 'downloaded', image_low_r2_key = ?2, image_low_checked_at = ?3,
             image_low_file_size = ?4, image_low_sha256 = ?5, image_low_failure_reason = NULL, updated_at = ?3
           WHERE id = ?1 AND image_low_status IS NOT 'downloaded'`,
          card.id,
          key,
          now,
          size,
          sha256,
        )
    }
  }
  if (!key.startsWith(IMAGES_PREFIX)) throw new Error(`clé R2 hors du préfixe ${IMAGES_PREFIX} : ${key}`)

  // Jamais d'écrasement : un objet présent n'est réconcilié que s'il a exactement ce contenu.
  db.usage.r2Reads++
  const existing = await bucket.head(key)
  if (existing) {
    if (existing.customMetadata?.sha256 !== check.sha256) return { status: 'conflict', reason: `objet R2 déjà présent à ${key}, d’un autre contenu : jamais écrasé` }
    const res = await db.batch([update(existing.size, check.sha256)])
    return res[0]?.meta.changes
      ? { status: 'reconciled', key, size: existing.size, sha256: check.sha256 }
      : { status: 'unchanged', key, reason: 'déjà enregistré' }
  }

  let stored: R2Object | null
  try {
    db.usage.r2Writes++
    stored = await bucket.put(key, upload.bytes, {
      sha256: check.sha256,
      httpMetadata: { contentType: 'image/webp', cacheControl: 'public, max-age=31536000, immutable' },
      customMetadata: { ...identity, source: MANUAL_SOURCE, sourceUrl: upload.sourceUrl, sha256: check.sha256, md5: check.md5 },
      // Écriture conditionnelle : si un autre envoi a écrit la clé entre-temps, rien n'est remplacé.
      onlyIf: { etagDoesNotMatch: '*' },
    })
  } catch (error) {
    throw new Error(`écriture R2 échouée (${key}) : ${errorMessage(error)}`)
  }
  if (!stored) return { status: 'conflict', reason: `objet R2 apparu à ${key} pendant l’envoi : jamais écrasé` }
  if (stored.size !== upload.bytes.byteLength) throw new Error(`écriture R2 non confirmée (${stored.size} ≠ ${upload.bytes.byteLength} octets)`)
  db.usage.bytesStored += stored.size
  const res = await db.batch([update(stored.size, check.sha256)])
  return res[0]?.meta.changes
    ? { status: 'stored', key, size: stored.size, sha256: check.sha256 }
    : { status: 'conflict', reason: 'objet écrit, mais la ligne a changé entre-temps : relancer l’envoi pour réconcilier' }
}
