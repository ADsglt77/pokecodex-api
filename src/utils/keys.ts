import type { Language } from '../config'

/**
 * Encode un segment de clé R2 de façon stable, sûre et réversible : seuls `A-Z a-z 0-9 . -`
 * restent tels quels ; tout autre octet UTF-8 (dont `_`, `/`, `%`, `?`) devient `_XX`.
 * Ex. `exu-%3F` → `exu-_253F`. Deux identifiants différents donnent toujours deux clés différentes,
 * et un segment encodé ne contient jamais `/` ni `..` seul.
 */
export function encodeKeySegment(value: string): string {
  if (value === '') throw new Error('segment de clé vide')
  let out = ''
  for (const char of value) {
    if (/^[A-Za-z0-9.-]$/.test(char)) out += char
    else for (const byte of new TextEncoder().encode(char)) out += `_${byte.toString(16).toUpperCase().padStart(2, '0')}`
  }
  // `.` et `..` seuls sont ambigus dans un chemin : on les encode.
  if (out === '.' || out === '..') out = out.replaceAll('.', '_2E')
  return out
}

/** Clé R2 de l'image d'une carte : `cards/<langue>/<extension>/<carte>.webp`. */
export const cardImageKey = (language: Language, setTcgdexId: string, cardTcgdexId: string): string =>
  `cards/${language}/${encodeKeySegment(setTcgdexId)}/${encodeKeySegment(cardTcgdexId)}.webp`

/** Identifiant interne d'une ligne : `<langue>:<id TCGdex>`. */
export const internalId = (language: Language, tcgdexId: string): string => `${language}:${tcgdexId}`
