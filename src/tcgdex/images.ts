/**
 * Images des cartes : URL candidates et validation des fichiers WebP.
 *
 * Règle de reconstruction (sondage du CDN du 13/11/2026, 0 écart sur 22 368 URL officielles) :
 *   https://assets.tcgdex.net/{langue}/{id de série}/{id d'extension}/{encodeURIComponent(localId)}
 * avec la casse exacte de l'API. Aucune variante (zéros de tête, casse, autre langue) n'a jamais
 * fonctionné : une seule URL reconstruite par carte, sans balayage du CDN.
 */

import type { Config, Language } from '../config'
import { toHex } from '../utils/hash'
import { ASSETS_BASE, ASSETS_HOST } from './client'

export type ImageSource = 'api' | 'cdn_reconstructed'

export interface ImageCandidate {
  url: string
  quality: string
  source: ImageSource
}

/**
 * Base d'URL officielle acceptable : https sur le CDN de TCGdex, sans requête ni fragment (même
 * vides), et dans la langue de la carte (une image d'une autre langue n'est jamais acceptée).
 */
export function isTrustedImageBase(base: string | null, language: Language): base is string {
  if (!base || base.includes('?') || base.includes('#')) return false
  try {
    const url = new URL(base)
    return url.protocol === 'https:' && url.host === ASSETS_HOST && url.pathname.split('/')[1] === language
  } catch {
    return false
  }
}

/** Base d'URL reconstruite, ou `null` si une information fiable manque. */
export function reconstructedBase(language: Language, seriesId: string | null, setId: string | null, localId: string | null): string | null {
  if (!seriesId || !setId || !localId) return null
  // Un segment « . » ou « .. » serait normalisé par fetch vers une autre ressource du CDN.
  if ([seriesId, setId, localId].some((part) => part === '.' || part === '..')) return null
  return [ASSETS_BASE, language, seriesId, setId, localId].map((part, i) => (i === 0 ? part : encodeURIComponent(part))).join('/')
}

/**
 * Id de série déduit de l'URL du logo ou du symbole d'une extension
 * (`https://assets.tcgdex.net/fr/swsh/swsh3/logo` → `swsh`), seulement si le segment suivant est
 * bien l'id de l'extension. Sert de secours quand le détail de l'extension est indisponible.
 */
export function seriesFromAssetUrl(url: string | null, setId: string): string | null {
  if (!url) return null
  try {
    const parsed = new URL(url)
    if (parsed.host !== ASSETS_HOST) return null
    const segments = parsed.pathname.split('/').map(decodeURIComponent)
    // ['', langue|univ, série, extension, logo|symbol]
    return segments.length >= 4 && segments[3] === setId && segments[2] ? segments[2] : null
  } catch {
    return null
  }
}

/**
 * URL à essayer, dans l'ordre : l'URL officielle en qualité principale puis de secours ; à défaut
 * d'URL officielle, l'URL reconstruite (qualité principale seulement, `maxReconstructionUrls`).
 */
export function imageCandidates(
  config: Config,
  card: { language: Language; api_image_url: string | null; series_id: string | null; set_tcgdex_id: string | null; local_id: string | null },
): ImageCandidate[] {
  const qualities = [config.imageQuality, ...(config.fallbackQuality ? [config.fallbackQuality] : [])]
  if (isTrustedImageBase(card.api_image_url, card.language)) {
    const base = card.api_image_url.replace(/\/+$/, '')
    return qualities.map((quality) => ({ url: `${base}/${quality}.webp`, quality, source: 'api' }))
  }
  const base = reconstructedBase(card.language, card.series_id, card.set_tcgdex_id, card.local_id)
  if (!base) return []
  return qualities.slice(0, config.maxReconstructionUrls).map((quality) => ({ url: `${base}/${quality}.webp`, quality, source: 'cdn_reconstructed' }))
}

/**
 * URL de la petite image d'une carte : la même base que l'image principale réellement copiée
 * (`image_source_url`), en qualité `low`. `null` si cette URL est absente ou n'est pas une URL
 * d'image fiable du CDN dans la langue de la carte (`…/high.webp` ou `…/low.webp`).
 */
export function lowImageUrl(sourceUrl: string | null, language: Language): string | null {
  const match = sourceUrl ? /^(.+)\/(high|low)\.webp$/.exec(sourceUrl) : null
  const base = match?.[1] ?? null
  return isTrustedImageBase(base, language) ? `${base}/low.webp` : null
}

/** L'URL source d'une image principale est-elle déjà en qualité `low` ? */
export const isLowQualitySource = (sourceUrl: string | null, quality: string | null): boolean =>
  quality === 'low' || (sourceUrl ?? '').endsWith('/low.webp')

/**
 * URL du logo d'une extension : `<logo>.webp`, seulement en https sur le CDN de TCGdex, sans requête
 * ni fragment, et dans la langue de l'extension ou dans `univ` (ressources communes) : un logo d'une
 * autre langue n'est jamais copié. `null` sinon.
 */
export function logoImageUrl(logoUrl: string | null, language: Language): string | null {
  if (!logoUrl || logoUrl.includes('?') || logoUrl.includes('#') || !logoUrl.startsWith(`${ASSETS_BASE}/`)) return null
  // Un segment « . » ou « .. » (même écrit %2e) serait normalisé par fetch vers une autre ressource.
  const segments = logoUrl.slice(ASSETS_BASE.length + 1).split('/')
  if (segments.some((part) => ['', '.', '..'].includes(part.replace(/%2e/gi, '.')))) return null
  if (segments[0] !== language && segments[0] !== 'univ') return null
  try {
    const url = new URL(logoUrl)
    if (url.protocol !== 'https:' || url.host !== ASSETS_HOST || url.username || url.password) return null
    return `${logoUrl}.webp`
  } catch {
    return null
  }
}

export type WebpCheck =
  | { ok: true; sha256: string; md5: string }
  /** `transient` : transfert probablement corrompu (à retenter) ; sinon fichier invalide. */
  | { ok: false; reason: string; transient: boolean }

const ascii = (bytes: Uint8Array, start: number, length: number) => String.fromCharCode(...bytes.subarray(start, start + length))

/**
 * Valide un fichier WebP téléchargé : type MIME, taille, signature `RIFF....WEBP`, taille RIFF
 * cohérente (fichier tronqué), longueur annoncée, et MD5 égal à l'ETag quand le CDN le fournit
 * (sur assets.tcgdex.net, l'ETag est le MD5 du contenu). Calcule le SHA-256 conservé en base.
 */
export async function checkWebp(bytes: Uint8Array, headers: Headers, config: Pick<Config, 'minImageBytes' | 'maxImageBytes'>): Promise<WebpCheck> {
  const contentType = (headers.get('content-type') ?? '').toLowerCase()
  if (!contentType.includes('image/webp')) return { ok: false, reason: `type MIME inattendu : ${contentType || 'absent'}`, transient: false }
  const declared = headers.get('content-length')
  // Avec un encodage de transfert (gzip…), la longueur annoncée est celle des octets compressés.
  const encoded = !['', 'identity'].includes((headers.get('content-encoding') ?? '').trim().toLowerCase())
  const lengthKnown = !encoded && declared !== null && /^\d+$/.test(declared)
  if (lengthKnown && Number(declared) !== bytes.byteLength) {
    return { ok: false, reason: `longueur reçue ${bytes.byteLength} ≠ annoncée ${declared}`, transient: true }
  }
  if (bytes.byteLength > config.maxImageBytes) return { ok: false, reason: `fichier trop volumineux (${bytes.byteLength} octets)`, transient: false }

  // Intégrité du transfert : MD5 du contenu reçu comparé à l'ETag du CDN (son MD5). Si elle est
  // prouvée, un fichier vide ou tronqué est un défaut du fichier source (définitif : on passe à la
  // qualité de secours) ; sinon, c'est probablement un transfert interrompu (à retenter).
  const [sha, md5] = await Promise.all([crypto.subtle.digest('SHA-256', bytes), crypto.subtle.digest('MD5', bytes)])
  const sha256 = toHex(sha)
  const md5Hex = toHex(md5)
  const etag = (headers.get('etag') ?? '').replace(/^W\//, '').replaceAll('"', '').toLowerCase()
  const etagIsMd5 = /^[0-9a-f]{32}$/.test(etag)
  if (etagIsMd5 && etag !== md5Hex) return { ok: false, reason: `MD5 ${md5Hex} ≠ ETag ${etag}`, transient: true }
  const transferProven = etagIsMd5 && lengthKnown

  if (bytes.byteLength === 0) return { ok: false, reason: 'contenu vide', transient: !transferProven }
  const signed = bytes.byteLength >= 12 && ascii(bytes, 0, 4) === 'RIFF' && ascii(bytes, 8, 4) === 'WEBP'
  // Un en-tête WebP qui annonce plus d'octets que reçus : fichier tronqué (passager si le transfert
  // n'est pas prouvé intègre), même s'il est aussi sous la taille minimale.
  if (signed) {
    const riffSize = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength).getUint32(4, true)
    if (riffSize + 8 > bytes.byteLength) {
      return { ok: false, reason: `fichier tronqué (RIFF ${riffSize + 8} > ${bytes.byteLength} octets)`, transient: !transferProven }
    }
  }
  if (bytes.byteLength < config.minImageBytes) return { ok: false, reason: `fichier trop petit (${bytes.byteLength} octets)`, transient: false }
  if (!signed) return { ok: false, reason: 'signature WebP absente (RIFF…WEBP)', transient: false }
  return { ok: true, sha256, md5: md5Hex }
}
