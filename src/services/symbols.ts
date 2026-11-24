/**
 * Symboles d'extension copiés par l'API d'administration (`POST /admin/import/symbol`), hors de la
 * tâche `symbol` de la synchronisation TCGdex : le Worker télécharge lui-même le fichier à l'adresse
 * donnée (deux hôtes seulement, `SYMBOL_SOURCES`), le convertit en WebP avec la liaison Cloudflare
 * Images s'il le faut, puis l'écrit à `cards/<langue>/<extension>/_symbol.webp`. Aucun fichier ne
 * transite par le poste de l'administrateur.
 *
 * Mêmes règles que les autres ajouts (`manual-import.ts`) : jamais d'écrasement (une extension qui a
 * déjà un symbole n'est pas touchée, un objet R2 présent n'est jamais remplacé) ; `symbol_provider`
 * vaut 'pokecardex' pour un fichier de Pokécardex, NULL pour une adresse de TCGdex.
 */

import { IMAGES_PREFIX, type Config, type Language } from '../config'
import type { Db } from '../db'
import { ASSETS_HOST } from '../tcgdex/client'
import { checkWebp } from '../tcgdex/images'
import { toHex } from '../utils/hash'
import { BodyTooLargeError, fetchClassified, readCapped } from '../utils/http'
import { internalId, setSymbolKey } from '../utils/keys'
import { errorMessage } from '../utils/log'
import { iso } from '../utils/time'
import { InputError, MANUAL_SOURCE, type ImageOutcome } from './manual-import'

/** Hôte d'où un symbole d'extension peut être copié, avec le chemin exact accepté. */
interface SymbolSource {
  host: string
  path: RegExp
  /** Valeur de `symbol_provider` : NULL = TCGdex. */
  provider: typeof MANUAL_SOURCE | null
  accept: string
  /** Types MIME acceptés en réponse. */
  types: readonly string[]
  /** L'ETag de l'hôte est le MD5 du contenu (contrôle d'intégrité). */
  md5Etag: boolean
}

/**
 * Les deux seules sources de symboles (https, sans requête ni fragment, jamais de redirection
 * suivie) :
 * - TCGdex : symbole hors du champ `symbol` de l'API (PNG quand le WebP manque, symbole de l'extension
 *   mère ou de l'extension internationale équivalente), sous `univ/` ; ETag = MD5 du contenu ;
 * - Pokécardex : un PNG par code d'extension (zones FR et JP), demandé en PNG (son CDN le
 *   convertirait sinon en WebP à sa façon, selon le cas).
 */
export const SYMBOL_SOURCES: readonly SymbolSource[] = [
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
]

/** Agent utilisateur des copies de symboles (générique, sans donnée personnelle). */
const SYMBOL_USER_AGENT = 'pokecodex-importer/1.0'
/** Côté maximal d'un symbole stocké : au-delà, réduit sans déformation. */
export const SYMBOL_MAX_SIDE = 512
/** Côté minimal : en dessous, ce n'est pas une vraie icône (image vide ou de remplacement). */
export const SYMBOL_MIN_SIDE = 8

/** Dimensions et conversion d'image : la liaison Cloudflare Images en production, une imitation en test. */
export interface SymbolImages {
  info(bytes: Uint8Array): Promise<{ format: string; width: number; height: number }>
  /** Le même dessin en WebP, à sa taille d'origine plafonnée à `SYMBOL_MAX_SIDE`. */
  toWebp(bytes: Uint8Array): Promise<Uint8Array>
}

/** Adaptateur de la liaison Cloudflare Images (`env.IMAGES`) : rien n'est stocké chez Images. */
export function imagesBindingAdapter(images: ImagesBinding, maxBytes: number): SymbolImages {
  const stream = (bytes: Uint8Array) => new Response(bytes).body as ReadableStream<Uint8Array>
  return {
    async info(bytes) {
      const info = await images.info(stream(bytes))
      if (!('width' in info)) throw new Error(`format non pris en charge : ${info.format}`)
      return { format: info.format, width: info.width, height: info.height }
    },
    async toWebp(bytes) {
      const result = await images
        .input(stream(bytes))
        .transform({ width: SYMBOL_MAX_SIDE, height: SYMBOL_MAX_SIDE, fit: 'scale-down' })
        .output({ format: 'image/webp', quality: 100 })
      return readCapped(result.response(), maxBytes)
    },
  }
}

export interface SymbolRequest {
  language: Language
  /** Id TCGdex de l'extension. */
  target: string
  /** Adresse du fichier, sur un hôte de `SYMBOL_SOURCES`. */
  sourceUrl: string
}

export type SymbolOutcome =
  | ImageOutcome
  /** Fichier absent (404, 410) ou refusé (redirection, 403…) à cette adresse : définitif. */
  | { status: 'source_not_found' | 'source_refused'; reason: string; httpStatus: number | null }
  /** Panne passagère de la source (5xx, 429, réseau, transfert incomplet) : à retenter. */
  | { status: 'source_error'; reason: string; httpStatus: number | null }

/** Source d'une adresse de symbole ; `InputError` (400) si l'adresse n'est pas exactement au format attendu. */
export function symbolSource(sourceUrl: string): SymbolSource {
  let url: URL | null = null
  try {
    url = new URL(sourceUrl)
  } catch {
    // adresse illisible : refusée ci-dessous
  }
  const host = url?.host
  const source = SYMBOL_SOURCES.find((item) => item.host === host)
  // Adresse déjà normale (aucun « .. », aucun encodage réécrit), sans requête ni fragment.
  const exact = url !== null && url.href === sourceUrl && !sourceUrl.includes('?') && !sourceUrl.includes('#')
  if (!url || !source || !exact || url.protocol !== 'https:' || url.username || url.password || url.port || !source.path.test(url.pathname)) {
    throw new InputError(`source refusée : adresse https sur ${SYMBOL_SOURCES.map((item) => item.host).join(' ou ')}, au format attendu`)
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

interface SymbolSetRow {
  id: string
  tcgdex_id: string
  symbol_status: string | null
  symbol_r2_key: string | null
}

/**
 * Copie le symbole d'une extension qui n'en a pas : télécharge `sourceUrl`, vérifie le fichier (type,
 * signature PNG ou WebP, taille, intégrité, dimensions), le convertit en WebP s'il le faut, l'écrit
 * dans R2 puis met D1 à jour. Extension qui a déjà un symbole → `unchanged`, sans aucune requête ;
 * objet R2 déjà présent → réconcilié s'il est le symbole de cette extension, sinon `conflict`. Un
 * échec de la source ne modifie pas D1.
 */
export async function storeSymbol(
  db: Db,
  bucket: R2Bucket,
  config: Pick<Config, 'minImageBytes' | 'maxImageBytes' | 'fetchTimeoutMs'>,
  fetcher: typeof fetch,
  images: SymbolImages,
  request: SymbolRequest,
  now: () => number,
): Promise<SymbolOutcome> {
  const source = symbolSource(request.sourceUrl)
  const set = await db.first<SymbolSetRow>(
    `SELECT id, tcgdex_id, symbol_status, symbol_r2_key FROM sets WHERE id = ?1`,
    internalId(request.language, request.target),
  )
  if (!set) return { status: 'not_found', reason: `extension ${request.language}/${request.target} inconnue` }
  if (set.symbol_status === 'downloaded' && set.symbol_r2_key) return { status: 'unchanged', key: set.symbol_r2_key, reason: 'symbole déjà présent' }
  const key = setSymbolKey(request.language, set.tcgdex_id)
  if (!key.startsWith(IMAGES_PREFIX)) throw new Error(`clé R2 hors du préfixe ${IMAGES_PREFIX} : ${key}`)
  const update = (size: number, sha256: string, sourceUrl: string, provider: string | null): D1PreparedStatement =>
    db.stmt(
      `UPDATE sets SET symbol_status = 'downloaded', symbol_r2_key = ?2, symbol_checked_at = ?3, symbol_source_url = ?4,
         symbol_file_size = ?5, symbol_sha256 = ?6, symbol_failure_reason = NULL, symbol_provider = ?7, updated_at = ?3
       WHERE id = ?1 AND symbol_status IS NOT 'downloaded'`,
      set.id,
      key,
      iso(now()),
      sourceUrl,
      size,
      sha256,
      provider,
    )

  // Objet déjà présent (copie précédente interrompue avant D1, ou tâche TCGdex) : réconcilié s'il est
  // bien le symbole de cette extension, jamais remplacé.
  db.usage.r2Reads++
  const existing = await bucket.head(key)
  if (existing) {
    const meta = existing.customMetadata ?? {}
    if (meta.setId !== set.tcgdex_id || !meta.sha256) return { status: 'conflict', reason: `objet R2 déjà présent à ${key}, étranger à cette extension : jamais écrasé` }
    const res = await db.batch([update(existing.size, meta.sha256, meta.sourceUrl ?? request.sourceUrl, meta.source === MANUAL_SOURCE ? MANUAL_SOURCE : null)])
    return res[0]?.meta.changes ? { status: 'reconciled', key, size: existing.size, sha256: meta.sha256 } : { status: 'unchanged', key, reason: 'déjà enregistré' }
  }

  db.usage.httpRequests++
  const outcome = await fetchClassified(
    fetcher,
    request.sourceUrl,
    { method: 'GET', headers: { accept: source.accept, 'user-agent': SYMBOL_USER_AGENT }, cache: 'no-store', redirect: 'manual' },
    config.fetchTimeoutMs,
    now,
  )
  if (outcome.kind === 'not_found') return { status: 'source_not_found', reason: `HTTP ${outcome.status} à ${request.sourceUrl}`, httpStatus: outcome.status }
  if (outcome.kind === 'client_error') return { status: 'source_refused', reason: `HTTP ${outcome.status} à ${request.sourceUrl}`, httpStatus: outcome.status }
  if (outcome.kind !== 'ok') {
    const reason = outcome.kind === 'network_error' ? outcome.message : `HTTP ${outcome.status} à ${request.sourceUrl}`
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
  if (size.width < SYMBOL_MIN_SIDE || size.height < SYMBOL_MIN_SIDE) return { status: 'invalid', reason: `image trop petite (${size.width}×${size.height})` }
  const oversized = size.width > SYMBOL_MAX_SIDE || size.height > SYMBOL_MAX_SIDE
  const webp = png || oversized ? await images.toWebp(bytes) : bytes
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
        variant: 'symbol',
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
