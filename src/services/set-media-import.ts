/**
 * Logos et symboles d'extension copiés par l'API d'administration (`POST /admin/import/logo` et
 * `POST /admin/import/symbol`), hors des tâches `logo` / `symbol` de la synchronisation TCGdex : le
 * Worker télécharge lui-même le fichier à l'adresse donnée (hôtes et chemins de `SET_MEDIA_SOURCES`),
 * le convertit en WebP avec la liaison Cloudflare Images s'il le faut, puis l'écrit à
 * `cards/<langue>/<extension>/_logo.webp` ou `_symbol.webp`. Aucun fichier ne transite par le poste
 * de l'administrateur.
 *
 * Mêmes règles que les autres ajouts (`manual-import.ts`) : jamais d'écrasement (une extension qui a
 * déjà ce logo ou ce symbole n'est pas touchée, un objet R2 présent n'est jamais remplacé) ;
 * `logo_provider` / `symbol_provider` valent 'pokecardex' pour un fichier de Pokécardex, NULL pour
 * une adresse de TCGdex.
 */

import { IMAGES_PREFIX, type Config, type Language } from '../config'
import type { Db } from '../db'
import type { SetMediaKind } from '../repositories/catalog'
import { ASSETS_HOST } from '../tcgdex/client'
import { checkWebp } from '../tcgdex/images'
import { toHex } from '../utils/hash'
import { BodyTooLargeError, fetchClassified, readCapped } from '../utils/http'
import { internalId, setLogoKey, setSymbolKey } from '../utils/keys'
import { errorMessage } from '../utils/log'
import { iso } from '../utils/time'
import { InputError, MANUAL_SOURCE, storeImage, type ImageOutcome } from './manual-import'
import { composeOverlay, type OverlayPlace } from './wasm-images'

/** Hôte d'où un logo ou un symbole peut être copié, avec le chemin exact accepté. */
interface MediaSource {
  host: string
  path: RegExp
  /** Valeur de `logo_provider` / `symbol_provider` : NULL = TCGdex. */
  provider: typeof MANUAL_SOURCE | null
  accept: string
  /** Types MIME acceptés en réponse. */
  types: readonly string[]
  /** L'ETag de l'hôte est le MD5 du contenu (contrôle d'intégrité). */
  md5Etag: boolean
}

/**
 * Les seules sources acceptées, par type d'image (https, sans requête ni fragment, jamais de
 * redirection suivie) :
 * - symboles de TCGdex : symbole hors du champ `symbol` de l'API (PNG quand le WebP manque, symbole
 *   de l'extension mère ou de l'extension internationale équivalente), sous `univ/` ; ETag = MD5 ;
 * - symboles de Pokécardex : un PNG par code d'extension (zones FR et JP), demandé en PNG (son CDN le
 *   convertirait sinon en WebP à sa façon, selon le cas) ;
 * - logos de TCGdex : logo d'une autre langue que celle de l'extension (l'API n'en donne aucun dans
 *   la sienne), `/<langue>/<série>/<extension>/logo.webp|png` ; ETag = MD5.
 */
export const SET_MEDIA_SOURCES: Readonly<Record<SetMediaKind, readonly MediaSource[]>> = {
  symbol: [
    {
      host: ASSETS_HOST,
      path: /^\/univ\/[A-Za-z0-9._-]{1,40}\/[A-Za-z0-9._-]{1,40}\/symbol\.(?:png|webp)$/,
      provider: null,
      accept: 'image/png, image/webp',
      types: ['image/png', 'image/webp', 'application/octet-stream'],
      md5Etag: true,
    },
    {
      host: 'pokecardex.b-cdn.net',
      path: /^\/assets\/images\/symboles(?:_jp)?\/[A-Za-z0-9_-]{1,32}\.png$/,
      provider: MANUAL_SOURCE,
      accept: 'image/png',
      types: ['image/png', 'image/webp'],
      md5Etag: false,
    },
  ],
  logo: [
    {
      host: ASSETS_HOST,
      path: /^\/[a-z]{2}(?:-[a-z]{2})?\/[A-Za-z0-9._-]{1,40}\/[A-Za-z0-9._-]{1,40}\/logo\.(?:png|webp)$/,
      provider: null,
      accept: 'image/png, image/webp',
      types: ['image/png', 'image/webp'],
      md5Etag: true,
    },
    // Wikis autorisés par l'utilisateur le 25/11/2026 (fichiers MediaWiki : /<h>/<hh>/<nom>.png).
    ...['www.pokepedia.fr', 'media.52poke.com', 'archives.bulbagarden.net'].map((host) => ({
      host,
      path: /^\/(?:images|wiki|media\/upload)\/[0-9a-f]\/[0-9a-f]{2}\/[A-Za-z0-9._%()'-]{1,300}\.png$/,
      provider: null,
      accept: 'image/png, image/webp',
      types: ['image/png', 'image/webp'],
      md5Etag: false,
    })),
  ],
}

/** Ce qui distingue le logo du symbole ; le reste de la copie est commun. */
const KINDS: Readonly<Record<SetMediaKind, { label: string; maxSide: number; key: (language: Language, setTcgdexId: string) => string }>> = {
  // « logo » et « symbole » sont tous deux masculins (messages).
  symbol: { label: 'symbole', maxSide: 512, key: setSymbolKey },
  logo: { label: 'logo', maxSide: 2048, key: setLogoKey },
}

/** Agent utilisateur des copies (générique, sans donnée personnelle). */
const MEDIA_USER_AGENT = 'pokecodex-importer/1.0'
/** Côté maximal d'un symbole stocké : au-delà, réduit sans déformation. */
export const SYMBOL_MAX_SIDE = KINDS.symbol.maxSide
/** Côté maximal d'un logo stocké (les logos de TCGdex font moins de 1 000 px de large). */
export const LOGO_MAX_SIDE = KINDS.logo.maxSide
/** Côté minimal : en dessous, ce n'est pas une vraie image (image vide ou de remplacement). */
export const MEDIA_MIN_SIDE = 8

/** Dimensions et conversion d'image : la liaison Cloudflare Images en production, une imitation en test. */
export interface SetMediaImages {
  info(bytes: Uint8Array): Promise<{ format: string; width: number; height: number }>
  /** Le même dessin en WebP, à sa taille d'origine plafonnée à `maxSide` (sans perte par défaut). */
  toWebp(bytes: Uint8Array, maxSide: number, quality?: number): Promise<Uint8Array>
}

/** Adaptateur de la liaison Cloudflare Images (`env.IMAGES`) : rien n'est stocké chez Images. */
export function imagesBindingAdapter(images: ImagesBinding, maxBytes: number): SetMediaImages {
  const stream = (bytes: Uint8Array) => new Response(bytes).body as ReadableStream<Uint8Array>
  return {
    async info(bytes) {
      const info = await images.info(stream(bytes))
      if (!('width' in info)) throw new Error(`format non pris en charge : ${info.format}`)
      return { format: info.format, width: info.width, height: info.height }
    },
    async toWebp(bytes, maxSide, quality = 100) {
      const result = await images
        .input(stream(bytes))
        .transform({ width: maxSide, height: maxSide, fit: 'scale-down' })
        .output({ format: 'image/webp', quality })
      return readCapped(result.response(), maxBytes)
    },
  }
}

export interface SetMediaRequest {
  kind: SetMediaKind
  language: Language
  /** Id TCGdex de l'extension. */
  target: string
  /** Adresse du fichier, sur un hôte de `SET_MEDIA_SOURCES[kind]`. */
  sourceUrl: string
}

export type SetMediaOutcome =
  | ImageOutcome
  /** Fichier absent (404, 410) ou refusé (redirection, 403…) à cette adresse : définitif. */
  | { status: 'source_not_found' | 'source_refused'; reason: string; httpStatus: number | null }
  /** Panne passagère de la source (5xx, 429, réseau, transfert incomplet) : à retenter. */
  | { status: 'source_error'; reason: string; httpStatus: number | null }

/** Source d'une adresse de logo ou de symbole ; `InputError` (400) si l'adresse n'est pas exactement au format attendu. */
export function setMediaSource(kind: SetMediaKind, sourceUrl: string): MediaSource {
  const sources = SET_MEDIA_SOURCES[kind]
  let url: URL | null = null
  try {
    url = new URL(sourceUrl)
  } catch {
    // adresse illisible : refusée ci-dessous
  }
  const host = url?.host
  const source = sources.find((item) => item.host === host)
  // Adresse déjà normale (aucun « .. », aucun encodage réécrit), sans requête ni fragment.
  const exact = url !== null && url.href === sourceUrl && !sourceUrl.includes('?') && !sourceUrl.includes('#')
  if (!url || !source || !exact || url.protocol !== 'https:' || url.username || url.password || url.port || !source.path.test(url.pathname)) {
    throw new InputError(`source refusée : adresse https sur ${sources.map((item) => item.host).join(' ou ')}, au format attendu pour un ${KINDS[kind].label}`)
  }
  return source
}

/**
 * Codes d'erreur de la liaison Images qui tiennent au fichier lui-même (pas une image, trop de
 * pixels, format illisible) ; tout autre code est une panne du service.
 */
const FILE_IMAGE_ERRORS = new Set([9412, 9413, 9520])
const isFileImageError = (error: unknown): boolean => {
  const code = (error as { code?: unknown } | null)?.code
  return typeof code === 'number' ? FILE_IMAGE_ERRORS.has(code) : !(error instanceof Error && 'code' in error)
}

const PNG_SIGNATURE = [0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a]
const isPng = (bytes: Uint8Array): boolean => bytes.byteLength > 8 && PNG_SIGNATURE.every((byte, i) => bytes[i] === byte)
const isWebp = (bytes: Uint8Array): boolean =>
  bytes.byteLength >= 12 && String.fromCharCode(...bytes.subarray(0, 4)) === 'RIFF' && String.fromCharCode(...bytes.subarray(8, 12)) === 'WEBP'

/**
 * Télécharge le fichier d'une source acceptée et contrôle le transfert (type MIME, taille, longueur
 * annoncée, MD5 = ETag si l'hôte le garantit) ; rend les octets, ou l'issue à renvoyer telle quelle.
 */
async function downloadSource(
  db: Db,
  fetcher: typeof fetch,
  source: MediaSource,
  sourceUrl: string,
  config: Pick<Config, 'minImageBytes' | 'maxImageBytes' | 'fetchTimeoutMs'>,
  now: () => number,
): Promise<{ bytes: Uint8Array } | SetMediaOutcome> {
  db.usage.httpRequests++
  const outcome = await fetchClassified(
    fetcher,
    sourceUrl,
    { method: 'GET', headers: { accept: source.accept, 'user-agent': MEDIA_USER_AGENT }, cache: 'no-store', redirect: 'manual' },
    config.fetchTimeoutMs,
    now,
  )
  if (outcome.kind === 'not_found') return { status: 'source_not_found', reason: `HTTP ${outcome.status} à ${sourceUrl}`, httpStatus: outcome.status }
  if (outcome.kind === 'client_error') return { status: 'source_refused', reason: `HTTP ${outcome.status} à ${sourceUrl}`, httpStatus: outcome.status }
  if (outcome.kind !== 'ok') {
    const reason = outcome.kind === 'network_error' ? outcome.message : `HTTP ${outcome.status} à ${sourceUrl}`
    return { status: 'source_error', reason, httpStatus: outcome.status }
  }
  const { response } = outcome
  const type = (response.headers.get('content-type') ?? '').split(';')[0]?.trim().toLowerCase() ?? ''
  if (!source.types.includes(type)) {
    await response.body?.cancel()
    return { status: 'invalid', reason: `type MIME inattendu : ${type || 'absent'}` }
  }
  let bytes: Uint8Array
  try {
    bytes = await readCapped(response, config.maxImageBytes)
  } catch (error) {
    if (error instanceof BodyTooLargeError) return { status: 'invalid', reason: `fichier trop volumineux (${config.maxImageBytes} octets au plus)` }
    return { status: 'source_error', reason: `lecture interrompue : ${errorMessage(error)}`, httpStatus: outcome.status }
  }

  // Intégrité du transfert : longueur annoncée et, chez TCGdex, MD5 = ETag.
  const declared = response.headers.get('content-length')
  const encoded = !['', 'identity'].includes((response.headers.get('content-encoding') ?? '').trim().toLowerCase())
  if (!encoded && declared !== null && /^\d+$/.test(declared) && Number(declared) !== bytes.byteLength) {
    return { status: 'source_error', reason: `longueur reçue ${bytes.byteLength} ≠ annoncée ${declared}`, httpStatus: outcome.status }
  }
  if (source.md5Etag) {
    const etag = (response.headers.get('etag') ?? '').replace(/^W\//, '').replaceAll('"', '').toLowerCase()
    const md5 = toHex(await crypto.subtle.digest('MD5', bytes))
    if (/^[0-9a-f]{32}$/.test(etag) && etag !== md5) return { status: 'source_error', reason: `MD5 ${md5} ≠ ETag ${etag}`, httpStatus: outcome.status }
  }
  if (bytes.byteLength < config.minImageBytes) return { status: 'invalid', reason: `fichier trop petit (${bytes.byteLength} octets)` }
  return { bytes }
}

// ——— Images de cartes copiées par le Worker (POST /admin/import/card) ———

/** Sources des images de cartes (autorisées par l'utilisateur le 25/11/2026) ; une adresse par appel. */
export const CARD_SOURCES: readonly MediaSource[] = [
  {
    host: 'www.pokepedia.fr',
    path: /^\/images\/[0-9a-f]\/[0-9a-f]{2}\/[A-Za-z0-9._%()'!,&+-]{1,300}\.(?:png|jpe?g)$/i,
    provider: MANUAL_SOURCE,
    accept: 'image/png, image/jpeg',
    types: ['image/png', 'image/jpeg'],
    md5Etag: false,
  },
  {
    host: 'assets.pokemon.com',
    path: /^\/assets\/[A-Za-z0-9._\/-]{1,200}\.png$/,
    provider: MANUAL_SOURCE,
    accept: 'image/png',
    types: ['image/png'],
    md5Etag: false,
  },
]
/** Image principale : côté long plafonné, WebP avec perte (comme les images de TCGdex). */
const CARD_MAIN = { maxSide: 1024, quality: 90 }
/** Petite image (grilles) : 342 px de haut pour une carte standard (245×342). */
const CARD_LOW = { maxSide: 342, quality: 85 }
/** En dessous, ce n'est pas un scan de carte exploitable. */
const CARD_MIN_SIDE = 100

const isJpeg = (bytes: Uint8Array): boolean => bytes.byteLength > 3 && bytes[0] === 0xff && bytes[1] === 0xd8 && bytes[2] === 0xff

export interface CardImageRequest {
  language: Language
  /** Id TCGdex de la carte. */
  target: string
  sourceUrl: string
}

/**
 * Copie l'image d'une carte qui n'en a aucune : télécharge `sourceUrl` (hôte de `CARD_SOURCES`),
 * la convertit en WebP (image principale et petite image) avec la liaison Images, puis l'écrit par
 * `storeImage` (mêmes règles que `POST /admin/import/image` : jamais d'écrasement). Carte qui a déjà
 * une image valide → `unchanged`, sans aucune requête.
 */
export async function storeCardFromUrl(
  db: Db,
  bucket: R2Bucket,
  config: Pick<Config, 'minImageBytes' | 'maxImageBytes' | 'fetchTimeoutMs'>,
  fetcher: typeof fetch,
  images: SetMediaImages,
  request: CardImageRequest,
  now: () => number,
): Promise<SetMediaOutcome & { low?: string }> {
  const source = pickSource(CARD_SOURCES, request.sourceUrl, 'une image de carte')
  const card = await db.first<{ image_status: string; image_r2_key: string | null }>(
    `SELECT image_status, image_r2_key FROM cards WHERE id = ?1`,
    internalId(request.language, request.target),
  )
  if (!card) return { status: 'not_found', reason: `carte ${request.language}/${request.target} inconnue` }
  if (['downloaded', 'recovered_from_cdn', 'already_present'].includes(card.image_status) && card.image_r2_key) {
    return { status: 'unchanged', key: card.image_r2_key, reason: 'image déjà présente' }
  }
  const download = await downloadSource(db, fetcher, source, request.sourceUrl, config, now)
  if ('status' in download) return download
  const { bytes } = download
  if (!isPng(bytes) && !isJpeg(bytes) && !isWebp(bytes)) return { status: 'invalid', reason: 'ni PNG, ni JPEG, ni WebP (signature absente)' }
  let size: { width: number; height: number }
  try {
    size = await images.info(bytes)
  } catch (error) {
    if (!isFileImageError(error)) throw error
    return { status: 'invalid', reason: `image illisible : ${errorMessage(error)}` }
  }
  if (size.width < CARD_MIN_SIDE || size.height < CARD_MIN_SIDE) return { status: 'invalid', reason: `image trop petite (${size.width}×${size.height})` }

  const main = await images.toWebp(bytes, CARD_MAIN.maxSide, CARD_MAIN.quality)
  const stored = await storeImage(db, bucket, config, { kind: 'main', language: request.language, target: request.target, sourceUrl: request.sourceUrl, bytes: main }, now())
  if (!['stored', 'reconciled', 'unchanged'].includes(stored.status)) return stored
  const low = await images.toWebp(bytes, CARD_LOW.maxSide, CARD_LOW.quality)
  const lowStored = await storeImage(db, bucket, config, { kind: 'low', language: request.language, target: request.target, sourceUrl: request.sourceUrl, bytes: low }, now())
  return { ...stored, low: lowStored.status }
}

// ——— Images reconstituées (POST /admin/import/card-overlay) ———

export interface CardOverlayRequest {
  language: Language
  /** Carte sans image (ex. carte de deck Académie de Combat). */
  target: string
  /** Carte d'origine dont l'image (déjà dans R2) est reprise. */
  origin: string
  place: OverlayPlace
  /** Calque PNG à coller (tampon du deck). */
  overlay: Uint8Array
  /** Rend l'image composée sans rien écrire. */
  dryRun: boolean
}

export type CardOverlayOutcome = (SetMediaOutcome & { low?: string }) | { status: 'preview'; bytes: Uint8Array }

/**
 * Image reconstituée d'une carte : l'image de sa carte d'origine (même illustration, même texte),
 * à laquelle on colle un calque (tampon du deck), puis enregistrée comme `POST /admin/import/image`
 * (image principale et petite image). L'adresse d'origine enregistrée commence par `reconstitution:`
 * pour que ces images restent reconnaissables et remplaçables par un vrai scan.
 */
export async function storeCardOverlay(
  db: Db,
  bucket: R2Bucket,
  config: Pick<Config, 'minImageBytes' | 'maxImageBytes'>,
  request: CardOverlayRequest,
  now: () => number,
): Promise<CardOverlayOutcome> {
  const card = await db.first<{ image_status: string; image_r2_key: string | null }>(`SELECT image_status, image_r2_key FROM cards WHERE id = ?1`, internalId(request.language, request.target))
  if (!card) return { status: 'not_found', reason: `carte ${request.language}/${request.target} inconnue` }
  if (!request.dryRun && VALID.includes(card.image_status) && card.image_r2_key) return { status: 'unchanged', key: card.image_r2_key, reason: 'image déjà présente' }
  const origin = await db.first<{ image_status: string; image_r2_key: string | null }>(`SELECT image_status, image_r2_key FROM cards WHERE id = ?1`, internalId(request.language, request.origin))
  if (!origin || !VALID.includes(origin.image_status) || !origin.image_r2_key) return { status: 'not_found', reason: `carte d'origine ${request.language}/${request.origin} sans image` }
  db.usage.r2Reads++
  const object = await bucket.get(origin.image_r2_key)
  if (!object) return { status: 'not_found', reason: `objet R2 absent : ${origin.image_r2_key}` }
  const base = new Uint8Array(await object.arrayBuffer())
  const [main, low] = await composeOverlay(base, request.overlay, request.place, [CARD_MAIN, CARD_LOW])
  if (!main || !low) throw new Error('composition incomplète')
  if (request.dryRun) return { status: 'preview', bytes: main }
  const sourceUrl = `reconstitution:${request.language}/${request.origin}`
  const stored = await storeImage(db, bucket, config, { kind: 'main', language: request.language, target: request.target, sourceUrl, bytes: main }, now())
  if (!['stored', 'reconciled', 'unchanged'].includes(stored.status)) return stored
  const lowStored = await storeImage(db, bucket, config, { kind: 'low', language: request.language, target: request.target, sourceUrl, bytes: low }, now())
  return { ...stored, low: lowStored.status }
}
const VALID = ['downloaded', 'recovered_from_cdn', 'already_present']

/** Source d'une adresse parmi `sources` ; `InputError` (400) si l'adresse n'est pas exactement au format attendu. */
function pickSource(sources: readonly MediaSource[], sourceUrl: string, label: string): MediaSource {
  let url: URL | null = null
  try {
    url = new URL(sourceUrl)
  } catch {
    // adresse illisible : refusée ci-dessous
  }
  const source = sources.find((item) => item.host === url?.host)
  const exact = url !== null && url.href === sourceUrl && !sourceUrl.includes('?') && !sourceUrl.includes('#')
  if (!url || !source || !exact || url.protocol !== 'https:' || url.username || url.password || url.port || !source.path.test(url.pathname)) {
    throw new InputError(`source refusée : adresse https sur ${sources.map((item) => item.host).join(' ou ')}, au format attendu pour ${label}`)
  }
  return source
}

interface SetMediaRow {
  id: string
  tcgdex_id: string
  status: string | null
  r2_key: string | null
}

/**
 * Copie le logo ou le symbole d'une extension qui n'en a pas : télécharge `sourceUrl`, vérifie le
 * fichier (type, signature PNG ou WebP, taille, intégrité, dimensions), le convertit en WebP s'il le
 * faut, l'écrit dans R2 puis met D1 à jour. Extension qui l'a déjà → `unchanged`, sans aucune
 * requête ; objet R2 déjà présent → réconcilié s'il est celui de cette extension, sinon `conflict`.
 * Un échec de la source ne modifie pas D1.
 */
export async function storeSetMedia(
  db: Db,
  bucket: R2Bucket,
  config: Pick<Config, 'minImageBytes' | 'maxImageBytes' | 'fetchTimeoutMs'>,
  fetcher: typeof fetch,
  images: SetMediaImages,
  request: SetMediaRequest,
  now: () => number,
): Promise<SetMediaOutcome> {
  const { kind } = request
  const spec = KINDS[kind]
  const source = setMediaSource(kind, request.sourceUrl)
  // `kind` vient d'une union fermée : les noms de colonnes ne dépendent d'aucune donnée reçue.
  const set = await db.first<SetMediaRow>(
    `SELECT id, tcgdex_id, ${kind}_status AS status, ${kind}_r2_key AS r2_key FROM sets WHERE id = ?1`,
    internalId(request.language, request.target),
  )
  if (!set) return { status: 'not_found', reason: `extension ${request.language}/${request.target} inconnue` }
  if (set.status === 'downloaded' && set.r2_key) return { status: 'unchanged', key: set.r2_key, reason: `${spec.label} déjà présent` }
  const key = spec.key(request.language, set.tcgdex_id)
  if (!key.startsWith(IMAGES_PREFIX)) throw new Error(`clé R2 hors du préfixe ${IMAGES_PREFIX} : ${key}`)
  const update = (size: number, sha256: string, sourceUrl: string, provider: string | null): D1PreparedStatement =>
    db.stmt(
      `UPDATE sets SET ${kind}_status = 'downloaded', ${kind}_r2_key = ?2, ${kind}_checked_at = ?3, ${kind}_source_url = ?4,
         ${kind}_file_size = ?5, ${kind}_sha256 = ?6, ${kind}_failure_reason = NULL, ${kind}_provider = ?7, updated_at = ?3
       WHERE id = ?1 AND ${kind}_status IS NOT 'downloaded'`,
      set.id,
      key,
      iso(now()),
      sourceUrl,
      size,
      sha256,
      provider,
    )

  // Objet déjà présent (copie précédente interrompue avant D1, ou tâche TCGdex) : réconcilié s'il est
  // bien le logo ou le symbole de cette extension, jamais remplacé.
  db.usage.r2Reads++
  const existing = await bucket.head(key)
  if (existing) {
    const meta = existing.customMetadata ?? {}
    if (meta.setId !== set.tcgdex_id || !meta.sha256) return { status: 'conflict', reason: `objet R2 déjà présent à ${key}, étranger à cette extension : jamais écrasé` }
    const res = await db.batch([update(existing.size, meta.sha256, meta.sourceUrl ?? request.sourceUrl, meta.source === MANUAL_SOURCE ? MANUAL_SOURCE : null)])
    return res[0]?.meta.changes ? { status: 'reconciled', key, size: existing.size, sha256: meta.sha256 } : { status: 'unchanged', key, reason: 'déjà enregistré' }
  }

  const download = await downloadSource(db, fetcher, source, request.sourceUrl, config, now)
  if ('status' in download) return download
  const { bytes } = download
  const png = isPng(bytes)
  if (!png && !isWebp(bytes)) return { status: 'invalid', reason: 'ni PNG ni WebP (signature absente)' }

  let size: { width: number; height: number }
  try {
    size = await images.info(bytes)
  } catch (error) {
    // Seuls les refus qui tiennent au fichier sont définitifs ; une panne du service Images remonte
    // (HTTP 500), et la copie sera reprise.
    if (!isFileImageError(error)) throw error
    return { status: 'invalid', reason: `image illisible : ${errorMessage(error)}` }
  }
  if (size.width < MEDIA_MIN_SIDE || size.height < MEDIA_MIN_SIDE) return { status: 'invalid', reason: `image trop petite (${size.width}×${size.height})` }
  const oversized = size.width > spec.maxSide || size.height > spec.maxSide
  const webp = png || oversized ? await images.toWebp(bytes, spec.maxSide) : bytes
  const check = await checkWebp(webp, new Headers({ 'content-type': 'image/webp', 'content-length': String(webp.byteLength) }), config)
  if (!check.ok) return { status: 'invalid', reason: `WebP produit refusé : ${check.reason}` }
  const sourceSha256 = toHex(await crypto.subtle.digest('SHA-256', bytes))

  let stored: R2Object | null
  try {
    db.usage.r2Writes++
    stored = await bucket.put(key, webp, {
      sha256: check.sha256,
      httpMetadata: { contentType: 'image/webp', cacheControl: 'public, max-age=31536000, immutable' },
      customMetadata: {
        variant: kind,
        language: request.language,
        setId: set.tcgdex_id,
        ...(source.provider ? { source: source.provider } : {}),
        sourceUrl: request.sourceUrl,
        sourceSha256,
        sha256: check.sha256,
        md5: check.md5,
      },
      // Écriture conditionnelle : si une autre copie a écrit la clé entre-temps, rien n'est remplacé.
      onlyIf: { etagDoesNotMatch: '*' },
    })
  } catch (error) {
    throw new Error(`écriture R2 échouée (${key}) : ${errorMessage(error)}`)
  }
  if (!stored) return { status: 'conflict', reason: `objet R2 apparu à ${key} pendant la copie : jamais écrasé` }
  if (stored.size !== webp.byteLength) throw new Error(`écriture R2 non confirmée (${stored.size} ≠ ${webp.byteLength} octets)`)
  db.usage.bytesStored += stored.size
  const res = await db.batch([update(stored.size, check.sha256, request.sourceUrl, source.provider)])
  return res[0]?.meta.changes
    ? { status: 'stored', key, size: stored.size, sha256: check.sha256 }
    : { status: 'conflict', reason: 'objet écrit, mais la ligne a changé entre-temps : relancer la copie pour réconcilier' }
}
