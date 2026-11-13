/** Résultat classé d'une requête HTTP vers TCGdex. */
export type HttpOutcome =
  | { kind: 'ok'; status: number; response: Response }
  | { kind: 'not_found'; status: number }
  | { kind: 'rate_limited'; status: number; retryAfterMs: number | null }
  | { kind: 'server_error'; status: number }
  | { kind: 'client_error'; status: number }
  | { kind: 'network_error'; status: null; message: string }

/** Plafond absolu d'une attente imposée par `Retry-After` (une valeur aberrante ne bloque pas tout). */
export const RETRY_AFTER_MAX_MS = 86_400_000

/** Date HTTP au format IMF-fixdate (RFC 9110), ex. `Wed, 21 Oct 2015 07:28:00 GMT`. */
const HTTP_DATE = /^[A-Z][a-z]{2}, \d{2} [A-Z][a-z]{2} \d{4} \d{2}:\d{2}:\d{2} GMT$/

/**
 * Durée d'attente imposée par `Retry-After` (secondes entières ou date HTTP), en millisecondes,
 * bornée à 24 h ; `null` si l'en-tête est absent ou illisible (`-1`, `1.5`, texte libre…).
 */
export function parseRetryAfter(value: string | null, nowMs: number): number | null {
  if (!value) return null
  const trimmed = value.trim()
  if (/^\d{1,10}$/.test(trimmed)) return Math.min(Number(trimmed) * 1000, RETRY_AFTER_MAX_MS)
  if (!HTTP_DATE.test(trimmed)) return null
  const date = Date.parse(trimmed)
  return Number.isNaN(date) ? null : Math.min(Math.max(0, date - nowMs), RETRY_AFTER_MAX_MS)
}

/** Classe une réponse HTTP. Le corps des réponses non `ok` est libéré. */
export async function classify(response: Response, nowMs: number): Promise<HttpOutcome> {
  const { status } = response
  if (status >= 200 && status < 300) return { kind: 'ok', status, response }
  await response.body?.cancel()
  if (status === 404 || status === 410) return { kind: 'not_found', status }
  if (status === 429) return { kind: 'rate_limited', status, retryAfterMs: parseRetryAfter(response.headers.get('retry-after'), nowMs) }
  // 408 (délai dépassé) et 425 sont temporaires : on les traite comme des erreurs serveur.
  if (status >= 500 || status === 408 || status === 425) return { kind: 'server_error', status }
  return { kind: 'client_error', status }
}

/** Requête `fetch` interrompue après `timeoutMs`, sans jamais lever : les erreurs réseau sont classées. */
export async function fetchClassified(
  fetcher: typeof fetch,
  url: string,
  init: RequestInit,
  timeoutMs: number,
  nowMs: () => number,
): Promise<HttpOutcome> {
  try {
    const response = await fetcher(url, { ...init, signal: AbortSignal.timeout(timeoutMs) })
    return await classify(response, nowMs())
  } catch (error) {
    return { kind: 'network_error', status: null, message: error instanceof Error ? `${error.name}: ${error.message}` : String(error) }
  }
}

/**
 * Lit un corps de réponse en flux, sans jamais dépasser `maxBytes` en mémoire : au-delà, la
 * lecture s'arrête et lève une erreur. Rend les octets contigus.
 */
export async function readCapped(response: Response, maxBytes: number): Promise<Uint8Array> {
  const declared = Number(response.headers.get('content-length') ?? Number.NaN)
  if (Number.isFinite(declared) && declared > maxBytes) {
    await response.body?.cancel()
    throw new BodyTooLargeError(declared, maxBytes)
  }
  if (!response.body) return new Uint8Array()
  const reader = response.body.getReader()
  // Longueur annoncée (sans encodage de transfert) : un seul tampon, sans recopie finale.
  const encoded = !['', 'identity'].includes((response.headers.get('content-encoding') ?? '').trim().toLowerCase())
  if (Number.isFinite(declared) && declared >= 0 && !encoded) {
    const out = new Uint8Array(declared)
    let offset = 0
    for (;;) {
      const { done, value } = await reader.read()
      if (done) break
      if (offset + value.byteLength > declared) {
        // Plus d'octets qu'annoncé : on bascule sur la lecture générale, toujours plafonnée.
        const total = offset + value.byteLength
        if (total > maxBytes) {
          await reader.cancel()
          throw new BodyTooLargeError(total, maxBytes)
        }
        const rest = await readRest(reader, maxBytes, total)
        const merged = new Uint8Array(total + rest.byteLength)
        merged.set(out.subarray(0, offset), 0)
        merged.set(value, offset)
        merged.set(rest, total)
        return merged
      }
      out.set(value, offset)
      offset += value.byteLength
    }
    return offset === declared ? out : out.slice(0, offset)
  }
  return readRest(reader, maxBytes, 0)
}

/** Lit la suite d'un flux en morceaux, sans dépasser `maxBytes` au total (`already` déjà lus). */
async function readRest(reader: ReadableStreamDefaultReader<Uint8Array>, maxBytes: number, already: number): Promise<Uint8Array> {
  const chunks: Uint8Array[] = []
  let total = 0
  for (;;) {
    const { done, value } = await reader.read()
    if (done) break
    total += value.byteLength
    if (already + total > maxBytes) {
      await reader.cancel()
      throw new BodyTooLargeError(already + total, maxBytes)
    }
    chunks.push(value)
  }
  const out = new Uint8Array(total)
  let offset = 0
  for (const chunk of chunks) {
    out.set(chunk, offset)
    offset += chunk.byteLength
  }
  return out
}

export class BodyTooLargeError extends Error {
  constructor(size: number, max: number) {
    super(`corps de réponse trop volumineux : ${size} octets (maximum ${max})`)
    this.name = 'BodyTooLargeError'
  }
}
