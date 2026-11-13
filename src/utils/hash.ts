const HEX = Array.from({ length: 256 }, (_, i) => i.toString(16).padStart(2, '0'))

/** Représentation hexadécimale d'octets. */
export function toHex(bytes: ArrayBuffer | Uint8Array): string {
  const view = bytes instanceof Uint8Array ? bytes : new Uint8Array(bytes)
  let out = ''
  for (const byte of view) out += HEX[byte]
  return out
}

/** SHA-256 (hexadécimal) d'une chaîne UTF-8 ou d'octets. */
export async function sha256Hex(data: string | ArrayBuffer | Uint8Array): Promise<string> {
  const bytes = typeof data === 'string' ? new TextEncoder().encode(data) : data
  return toHex(await crypto.subtle.digest('SHA-256', bytes))
}

/** JSON canonique (clés triées) : deux objets égaux donnent la même chaîne, donc le même hash. */
export function canonicalJson(value: unknown): string {
  return JSON.stringify(value, (_key, inner: unknown) => {
    if (inner && typeof inner === 'object' && !Array.isArray(inner)) {
      // Objet sans prototype : une clé JSON `__proto__` reste une propriété ordinaire.
      const sorted = Object.create(null) as Record<string, unknown>
      for (const key of Object.keys(inner).sort()) sorted[key] = (inner as Record<string, unknown>)[key]
      return sorted
    }
    return inner
  })
}

/** Empreinte d'un objet JSON, indépendante de l'ordre de ses clés. */
export const contentHash = (value: unknown): Promise<string> => sha256Hex(canonicalJson(value))
