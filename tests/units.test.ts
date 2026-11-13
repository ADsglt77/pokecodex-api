/**
 * Tests unitaires des fonctions pures et des parseurs : cron, clés R2, CSV, empreintes,
 * configuration, HTTP, limiteur de débit, URL et validation des images, réponses TCGdex, délai des
 * nouvelles tentatives. Aucun appel réseau : les réponses sont construites localement.
 */

import { env } from 'cloudflare:workers'
import { beforeEach, describe, expect, it } from 'vitest'
import { parseConfig, SCHEDULED_MAX_MS, type Config, type ConfigSource } from '../src/config'
import { retryDelayMs } from '../src/repositories/jobs'
import { checkWebp, imageCandidates, isTrustedImageBase, reconstructedBase, seriesFromAssetUrl } from '../src/tcgdex/images'
import { parseCardDetail, parseCardList, parseSeriesList, parseSetDetail, parseSetList, stripPricing } from '../src/tcgdex/types'
import { nextCronTime } from '../src/utils/cron'
import { csvCell, csvRow } from '../src/utils/csv'
import { canonicalJson, contentHash, sha256Hex, toHex } from '../src/utils/hash'
import { BodyTooLargeError, classify, fetchClassified, parseRetryAfter, readCapped, RETRY_AFTER_MAX_MS } from '../src/utils/http'
import { cardImageKey, encodeKeySegment, internalId } from '../src/utils/keys'
import { RateLimiter } from '../src/utils/limiter'
import { ASSETS, Clock, md5Hex, resetAll, sha256Hex as sha256Bytes, testConfig, webp } from './helpers'

beforeEach(resetAll)

// ——— Outils locaux ———

/** Prochaine occurrence en ISO, ou `null`. */
function next(expression: string, after: string): string | null {
  const result = nextCronTime(expression, Date.parse(after))
  return result === null ? null : new Date(result).toISOString()
}

/** Décodage d'un segment de clé (inverse de `encodeKeySegment`), pour vérifier la réversibilité. */
function decodeKeySegment(segment: string): string {
  const bytes: number[] = []
  for (let i = 0; i < segment.length; ) {
    if (segment[i] === '_') {
      bytes.push(Number.parseInt(segment.slice(i + 1, i + 3), 16))
      i += 3
    } else {
      bytes.push(segment.charCodeAt(i))
      i++
    }
  }
  return new TextDecoder('utf-8', { fatal: true, ignoreBOM: false }).decode(new Uint8Array(bytes))
}

/** En-têtes d'une image servie par le CDN : type WebP, longueur exacte, ETag = MD5. */
async function cdnHeaders(bytes: Uint8Array, extra: Record<string, string> = {}): Promise<Headers> {
  return new Headers({ 'content-type': 'image/webp', 'content-length': String(bytes.byteLength), etag: `"${await md5Hex(bytes)}"`, ...extra })
}

const LIMITS = { minImageBytes: 100, maxImageBytes: 8_000_000 }

/** Flux d'octets produit à la demande, qui compte ses lectures et son annulation. */
function countingStream(chunkSize: number, chunks: number) {
  const state = { pulls: 0, cancelled: false }
  const stream = new ReadableStream<Uint8Array>({
    pull(controller) {
      if (state.pulls >= chunks) return controller.close()
      state.pulls++
      controller.enqueue(new Uint8Array(chunkSize).fill(state.pulls))
    },
    cancel() {
      state.cancelled = true
    },
  })
  return { stream, state }
}

const NOW = Date.parse('2026-11-13T10:00:00.000Z')

/** Dépassement possible après l'échéance d'une exécution, tel que `parseConfig` le calcule. */
const overrunMs = (config: Config): number => 2 * config.fetchTimeoutMs + config.maxConcurrency * config.maxRequestIntervalMs + 60_000

/** Générateur pseudo-aléatoire déterministe (LCG), pour des tests de propriétés reproductibles. */
function seeded(seed: number): () => number {
  let state = seed >>> 0
  return () => {
    state = (Math.imul(state, 1_664_525) + 1_013_904_223) >>> 0
    return state / 2 ** 32
  }
}

const DEFAULT_API = 'https://api.tcgdex.net/v2'

// ——— src/utils/cron.ts ———

describe('nextCronTime', () => {
  it('« */5 * * * * » : prochaine minute multiple de 5, strictement après l’instant donné', () => {
    expect(next('*/5 * * * *', '2026-11-13T10:00:00.000Z')).toBe('2026-11-13T10:05:00.000Z')
    expect(next('*/5 * * * *', '2026-11-13T10:03:27.123Z')).toBe('2026-11-13T10:05:00.000Z')
    expect(next('*/5 * * * *', '2026-11-13T10:04:59.999Z')).toBe('2026-11-13T10:05:00.000Z')
    expect(next('*/5 * * * *', '2026-11-13T10:55:00.000Z')).toBe('2026-11-13T11:00:00.000Z')
    expect(next('*/5 * * * *', '2026-11-13T23:58:00.000Z')).toBe('2026-11-14T00:00:00.000Z')
  })

  it('« 0 3 * * MON » : le lundi suivant à 03:00 UTC (abréviation insensible à la casse)', () => {
    // Le 13/11/2026 est un vendredi.
    expect(new Date(Date.parse('2026-11-13T10:00:00Z')).getUTCDay()).toBe(5)
    expect(next('0 3 * * MON', '2026-11-13T10:00:00.000Z')).toBe('2026-11-16T03:00:00.000Z')
    expect(next('0 3 * * mon', '2026-11-13T10:00:00.000Z')).toBe('2026-11-16T03:00:00.000Z')
    expect(next('0 3 * * MON', '2026-11-16T02:59:59.000Z')).toBe('2026-11-16T03:00:00.000Z')
    // À 03:00 pile, l'occurrence suivante est la semaine d'après.
    expect(next('0 3 * * MON', '2026-11-16T03:00:00.000Z')).toBe('2026-11-23T03:00:00.000Z')
    expect(next('0 3 * * MON', '2026-11-16T03:00:30.000Z')).toBe('2026-11-23T03:00:00.000Z')
  })

  it('numérotation Cloudflare : 1 = dimanche, 2 = lundi … 7 = samedi', () => {
    const sunday = next('0 17 * * 1', '2026-11-13T10:00:00.000Z')
    expect(sunday).toBe('2026-11-15T17:00:00.000Z')
    expect(new Date(sunday as string).getUTCDay()).toBe(0)
    expect(next('0 0 * * 2', '2026-11-13T10:00:00.000Z')).toBe(next('0 0 * * MON', '2026-11-13T10:00:00.000Z'))
    expect(next('0 0 * * SUN', '2026-11-13T10:00:00.000Z')).toBe(next('0 0 * * 1', '2026-11-13T10:00:00.000Z'))
    const saturday = next('0 0 * * 7', '2026-11-13T10:00:00.000Z')
    expect(saturday).toBe('2026-11-14T00:00:00.000Z')
    expect(new Date(saturday as string).getUTCDay()).toBe(6)
    expect(next('0 0 * * SAT', '2026-11-13T10:00:00.000Z')).toBe(saturday)
    expect(next('30 12 * * 6', '2026-11-13T10:00:00.000Z')).toBe('2026-11-13T12:30:00.000Z') // 6 = vendredi
  })

  it('listes de valeurs (minutes, heures, jours de semaine)', () => {
    expect(next('0,30 * * * *', '2026-11-13T10:10:00.000Z')).toBe('2026-11-13T10:30:00.000Z')
    expect(next('0,30 * * * *', '2026-11-13T10:30:00.000Z')).toBe('2026-11-13T11:00:00.000Z')
    expect(next('15 8,20 * * *', '2026-11-13T10:00:00.000Z')).toBe('2026-11-13T20:15:00.000Z')
    expect(next('15 8,20 * * *', '2026-11-13T20:15:00.000Z')).toBe('2026-11-14T08:15:00.000Z')
    expect(next('0 12 * * MON,FRI', '2026-11-13T13:00:00.000Z')).toBe('2026-11-16T12:00:00.000Z')
    expect(next('0 12 * * MON,FRI', '2026-11-13T11:00:00.000Z')).toBe('2026-11-13T12:00:00.000Z')
  })

  it('plages et pas (a-b, a-b/n, a/n, noms de jours)', () => {
    expect(next('0 9-17 * * *', '2026-11-13T18:00:00.000Z')).toBe('2026-11-14T09:00:00.000Z')
    expect(next('0 9-17 * * *', '2026-11-13T12:10:00.000Z')).toBe('2026-11-13T13:00:00.000Z')
    expect(next('0 9 * * MON-FRI', '2026-11-13T10:00:00.000Z')).toBe('2026-11-16T09:00:00.000Z')
    expect(next('0 9 * * 2-6', '2026-11-13T10:00:00.000Z')).toBe('2026-11-16T09:00:00.000Z')
    expect(next('0-10/5 * * * *', '2026-11-13T10:06:00.000Z')).toBe('2026-11-13T10:10:00.000Z')
    expect(next('0-10/5 * * * *', '2026-11-13T10:11:00.000Z')).toBe('2026-11-13T11:00:00.000Z')
    expect(next('5/20 * * * *', '2026-11-13T10:06:00.000Z')).toBe('2026-11-13T10:25:00.000Z')
    expect(next('5/20 * * * *', '2026-11-13T10:46:00.000Z')).toBe('2026-11-13T11:05:00.000Z')
  })

  it('passage de mois et d’année, mois courts et 29 février', () => {
    expect(next('0 0 1 * *', '2026-11-13T10:00:00.000Z')).toBe('2026-12-01T00:00:00.000Z')
    expect(next('0 0 1 1 *', '2026-11-13T10:00:00.000Z')).toBe('2027-01-01T00:00:00.000Z')
    expect(next('0 0 1 JAN *', '2026-11-13T10:00:00.000Z')).toBe('2027-01-01T00:00:00.000Z')
    expect(next('0 0 * * *', '2026-12-31T23:59:30.000Z')).toBe('2027-01-01T00:00:00.000Z')
    expect(next('59 23 31 12 *', '2026-12-31T23:59:00.000Z')).toBe('2027-12-31T23:59:00.000Z')
    // Novembre n'a que 30 jours : le 31 suivant est en décembre.
    expect(next('0 0 31 * *', '2026-11-13T10:00:00.000Z')).toBe('2026-12-31T00:00:00.000Z')
    expect(next('0 0 29 2 *', '2026-11-13T10:00:00.000Z')).toBe('2028-02-29T00:00:00.000Z')
  })

  it('le résultat est toujours une minute entière strictement postérieure', () => {
    for (const expression of ['*/5 * * * *', '0 3 * * MON', '7 */2 * * *', '0 0 1 * *']) {
      for (const after of [NOW, NOW + 1, NOW + 59_999, NOW + 123_456_789]) {
        const result = nextCronTime(expression, after)
        expect(result).not.toBeNull()
        expect(result as number).toBeGreaterThan(after)
        expect((result as number) % 60_000).toBe(0)
      }
    }
  })

  it.each([
    [''],
    ['* * * *'],
    ['* * * * * *'],
    ['60 * * * *'],
    ['* 24 * * *'],
    ['* * 0 * *'],
    ['* * 32 * *'],
    ['* * * 0 *'],
    ['* * * 13 *'],
    ['* * * * 0'],
    ['* * * * 8'],
    ['*/0 * * * *'],
    ['5-2 * * * *'],
    ['-1 * * * *'],
    ['1,,2 * * * *'],
    ['0 0 L * *'],
    ['0 0 15W * *'],
    ['0 0 * * 5L'],
    ['0 0 * * MON#2'],
    ['0 0 * * FOO'],
    ['0 0 ? * MON'],
    ['a b c d e'],
  ])('expression invalide « %s » → null', (expression) => {
    expect(nextCronTime(expression, NOW)).toBeNull()
  })

  it('date impossible (30 février) → null, sans boucle infinie', () => {
    expect(nextCronTime('0 0 30 2 *', NOW)).toBeNull()
  })
})

// ——— src/utils/keys.ts ———

describe('encodeKeySegment et cardImageKey', () => {
  it('laisse inchangés les caractères sûrs A-Z a-z 0-9 . -', () => {
    for (const value of ['swsh3-136', 'SV1a', 'AZaz09.-', 'base1-4', 'sv03.5-001']) expect(encodeKeySegment(value)).toBe(value)
  })

  it('encode « % », « + », « _ », « / », « ? » et l’espace en _XX (hexadécimal majuscule)', () => {
    expect(encodeKeySegment('exu-%3F')).toBe('exu-_253F')
    expect(encodeKeySegment('SM1+')).toBe('SM1_2B')
    expect(encodeKeySegment('SM1+-001')).toBe('SM1_2B-001')
    expect(encodeKeySegment('_')).toBe('_5F')
    expect(encodeKeySegment('a_b')).toBe('a_5Fb')
    expect(encodeKeySegment('a/b')).toBe('a_2Fb')
    expect(encodeKeySegment('?')).toBe('_3F')
    expect(encodeKeySegment('a b')).toBe('a_20b')
    expect(encodeKeySegment('a\\b')).toBe('a_5Cb')
  })

  it('encode le japonais et les accents octet par octet en UTF-8', () => {
    expect(encodeKeySegment('ア')).toBe('_E3_82_A2')
    expect(encodeKeySegment('é')).toBe('_C3_A9')
    expect(encodeKeySegment('🃏')).toBe('_F0_9F_83_8F')
    const encoded = encodeKeySegment('トロピウス')
    expect(encoded).toMatch(/^(_[0-9A-F]{2})+$/)
    expect(encoded.length).toBe(5 * 3 * 3)
  })

  it('encode « . » et « .. » seuls, ambigus dans un chemin', () => {
    expect(encodeKeySegment('.')).toBe('_2E')
    expect(encodeKeySegment('..')).toBe('_2E_2E')
    expect(encodeKeySegment('...')).toBe('...')
  })

  it('refuse un segment vide', () => {
    expect(() => encodeKeySegment('')).toThrow()
  })

  it('est injectif et réversible sur un échantillon piégeux, sans jamais produire « / »', () => {
    const sample = [
      'swsh3-136', 'exu-%3F', 'exu-?', 'exu-_3F', 'exu-_253F', 'SM1+', 'SM1_2B', 'SM1 2B', 'SM1%2B',
      'a/b', 'a_2Fb', 'a%2Fb', 'a\\b', '.', '..', '...', '_2E', '%2E', '_2E_2E', 'ア', '_E3_82_A2',
      'トロピウス', 'SV1a-001', 'sv1a-001', 'é', 'é', '🃏', 'a_b', 'a__b', '_', '__', '_5F', 'A', 'a',
      '../../etc/passwd', 'x\u0000y', 'x\ny',
    ]
    const encoded = sample.map(encodeKeySegment)
    expect(new Set(encoded).size).toBe(sample.length)
    for (const [i, key] of encoded.entries()) {
      expect(key).toMatch(/^[A-Za-z0-9._-]+$/)
      expect(key).not.toContain('/')
      expect(['.', '..']).not.toContain(key)
      expect(decodeKeySegment(key)).toBe(sample[i])
    }
  })

  it('cardImageKey range l’image sous « cards/<langue>/<extension>/<carte>.webp »', () => {
    expect(cardImageKey('fr', 'swsh3', 'swsh3-136')).toBe('cards/fr/swsh3/swsh3-136.webp')
    expect(cardImageKey('fr', 'exu', 'exu-%3F')).toBe('cards/fr/exu/exu-_253F.webp')
    expect(cardImageKey('ja', 'SM1+', 'SM1+-001')).toBe('cards/ja/SM1_2B/SM1_2B-001.webp')
    for (const [setId, cardId] of [['../..', '../../reports/latest/summary'], ['a/b', 'c/d'], ['.', '..']] as const) {
      const key = cardImageKey('ja', setId, cardId)
      expect(key.startsWith('cards/ja/')).toBe(true)
      const segments = key.split('/')
      expect(segments).toHaveLength(4)
      expect(segments.some((segment) => segment === '.' || segment === '..')).toBe(false)
    }
  })

  it('internalId : « <langue>:<id TCGdex> »', () => {
    expect(internalId('fr', 'swsh3-136')).toBe('fr:swsh3-136')
    expect(internalId('ja', 'SM1+-001')).toBe('ja:SM1+-001')
  })
})

// ——— src/utils/csv.ts ———

describe('csvCell et csvRow', () => {
  it('rend les valeurs simples telles quelles, null et undefined vides', () => {
    expect(csvCell('abc')).toBe('abc')
    expect(csvCell('Ténèbres Embrasées')).toBe('Ténèbres Embrasées')
    expect(csvCell(42)).toBe('42')
    expect(csvCell(0)).toBe('0')
    expect(csvCell(false)).toBe('false')
    expect(csvCell(null)).toBe('')
    expect(csvCell(undefined)).toBe('')
    expect(csvCell('')).toBe('')
    expect(csvCell('a=b')).toBe('a=b')
  })

  it('met entre guillemets les champs avec virgule, point-virgule, guillemet ou retour à la ligne', () => {
    expect(csvCell('a,b')).toBe('"a,b"')
    expect(csvCell('a;b')).toBe('"a;b"')
    expect(csvCell('il a dit "bonjour"')).toBe('"il a dit ""bonjour"""')
    expect(csvCell('"')).toBe('""""')
    expect(csvCell('ligne 1\nligne 2')).toBe('"ligne 1\nligne 2"')
    expect(csvCell('ligne 1\r\nligne 2')).toBe('"ligne 1\r\nligne 2"')
  })

  it('préfixe d’une apostrophe les chaînes commençant par = + - @ tabulation ou CR (injection de formule)', () => {
    expect(csvCell('=1+1')).toBe("'=1+1")
    expect(csvCell('+33 6 00 00 00 00')).toBe("'+33 6 00 00 00 00")
    expect(csvCell('@SUM(A1:A2)')).toBe("'@SUM(A1:A2)")
    expect(csvCell('\t=1')).toBe("'\t=1")
    expect(csvCell('=HYPERLINK("http://x","a")')).toBe(`"'=HYPERLINK(""http://x"",""a"")"`)
    expect(csvCell('=1,2')).toBe(`"'=1,2"`)
    // « - » aussi : -2+3 ou -cmd|… sont des formules pour un tableur.
    expect(csvCell('-1')).toBe("'-1")
    expect(csvCell('-')).toBe("'-")
    expect(csvCell('-2+3')).toBe("'-2+3")
    expect(csvCell("-cmd|' /C calc'!A0")).toBe("'-cmd|' /C calc'!A0")
    expect(csvCell('-1,5')).toBe(`"'-1,5"`)
    expect(csvCell('\r=1')).toBe(`"'\r=1"`)
  })

  it('un « - » ailleurs qu’en tête, et les nombres négatifs, restent intacts (seules les chaînes sont neutralisées)', () => {
    expect(csvCell('a-b')).toBe('a-b')
    expect(csvCell('swsh3-136')).toBe('swsh3-136')
    expect(csvCell(' -1')).toBe(' -1')
    expect(csvCell(-1)).toBe('-1')
    expect(csvCell(-0.5)).toBe('-0.5')
    expect(csvCell(-12n)).toBe('-12')
    expect(csvCell(-Infinity)).toBe('-Infinity')
    expect(csvRow([-3, '-3', 'x-y', 0])).toBe(`-3,'-3,x-y,0\r\n`)
  })

  it('csvRow joint les cellules par des virgules et termine par CRLF', () => {
    expect(csvRow(['a', null, 'b,c', 3, undefined, '=x'])).toBe(`a,,"b,c",3,,'=x\r\n`)
    expect(csvRow([])).toBe('\r\n')
  })
})

// ——— src/utils/hash.ts ———

describe('canonicalJson, sha256Hex et contentHash', () => {
  it('canonicalJson ne dépend pas de l’ordre des clés, à toute profondeur', () => {
    const a = { b: 1, a: { d: [1, { z: 1, y: 2 }], c: 'x' }, e: null }
    const b = { e: null, a: { c: 'x', d: [1, { y: 2, z: 1 }] }, b: 1 }
    expect(canonicalJson(a)).toBe(canonicalJson(b))
    expect(canonicalJson(a)).toBe('{"a":{"c":"x","d":[1,{"y":2,"z":1}]},"b":1,"e":null}')
  })

  it('canonicalJson garde l’ordre des tableaux et sérialise les valeurs simples', () => {
    expect(canonicalJson([2, 1])).toBe('[2,1]')
    expect(canonicalJson([2, 1])).not.toBe(canonicalJson([1, 2]))
    expect(canonicalJson('x')).toBe('"x"')
    expect(canonicalJson(null)).toBe('null')
    expect(canonicalJson(3.5)).toBe('3.5')
  })

  it('sha256Hex donne les empreintes connues (chaîne, octets, ArrayBuffer)', async () => {
    expect(await sha256Hex('abc')).toBe('ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad')
    expect(await sha256Hex('')).toBe('e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855')
    expect(await sha256Hex(new Uint8Array([0x61, 0x62, 0x63]))).toBe('ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad')
    expect(await sha256Hex(new Uint8Array([0x61, 0x62, 0x63]).buffer)).toBe('ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad')
    // Les chaînes sont hachées en UTF-8.
    expect(await sha256Hex('é')).toBe(await sha256Hex(new Uint8Array([0xc3, 0xa9])))
  })

  it('toHex : deux chiffres minuscules par octet', () => {
    expect(toHex(new Uint8Array([0, 15, 16, 255]))).toBe('000f10ff')
    expect(toHex(new ArrayBuffer(0))).toBe('')
  })

  it('contentHash : même empreinte quel que soit l’ordre des clés, différente si une valeur change', async () => {
    const hash = await contentHash({ b: 1, a: { y: 2, x: 1 } })
    expect(hash).toBe(await contentHash({ a: { x: 1, y: 2 }, b: 1 }))
    expect(hash).toBe(await sha256Hex('{"a":{"x":1,"y":2},"b":1}'))
    expect(hash).not.toBe(await contentHash({ a: { x: 1, y: 3 }, b: 1 }))
    expect(hash).toMatch(/^[0-9a-f]{64}$/)
  })

  it('canonicalJson conserve une clé « __proto__ » venue d’un JSON externe', async () => {
    const withProto = JSON.parse('{"__proto__":{"x":1},"a":1}') as unknown
    expect(canonicalJson(withProto)).toBe('{"__proto__":{"x":1},"a":1}')
    expect(await contentHash(withProto)).not.toBe(await contentHash({ a: 1 }))
  })

  it('canonicalJson : « __proto__ » imbriquée, triée comme les autres clés, sans pollution du prototype', async () => {
    const a = JSON.parse('{"z":{"__proto__":{"polluted":true},"b":2},"__proto__":[1],"a":1}') as unknown
    const b = JSON.parse('{"a":1,"__proto__":[1],"z":{"b":2,"__proto__":{"polluted":true}}}') as unknown
    expect(canonicalJson(a)).toBe('{"__proto__":[1],"a":1,"z":{"__proto__":{"polluted":true},"b":2}}')
    expect(canonicalJson(a)).toBe(canonicalJson(b))
    expect(await contentHash(a)).toBe(await contentHash(b))
    // La valeur de la clé compte dans l'empreinte.
    expect(await contentHash(a)).not.toBe(await contentHash(JSON.parse('{"__proto__":[2],"a":1,"z":{"__proto__":{"polluted":true},"b":2}}')))
    // Aucun objet n'hérite de la valeur sérialisée.
    expect(({} as Record<string, unknown>).polluted).toBeUndefined()
    expect(Object.getPrototypeOf({})).toBe(Object.prototype)
    // L'entrée n'est pas modifiée.
    expect(Object.keys(a as object)).toEqual(['z', '__proto__', 'a'])
  })
})

// ——— src/config.ts ———

describe('parseConfig', () => {
  it('valeurs par défaut quand aucune variable n’est fournie', () => {
    expect(parseConfig({})).toEqual({
      processCron: '*/5 * * * *',
      syncCron: '7 3 * * MON',
      languages: ['fr', 'ja'],
      apiBase: 'https://api.tcgdex.net/v2',
      userAgent: 'tcgdex-r2-importer/1.0',
      maxConcurrency: 2,
      minRequestIntervalMs: 250,
      maxRequestIntervalMs: 5_000,
      runTimeBudgetMs: 240_000,
      maxJobsPerRun: 900,
      claimBatchSize: 10,
      d1QueryBudget: 850,
      subrequestBudget: 5_000,
      // 360 s demandées, mais au moins budget + dépassement possible (240 s + 130 s).
      runnerLeaseMs: 370_000,
      jobLeaseMs: 600_000,
      maxAttempts: 5,
      maxReleases: 48,
      retryBaseMs: 60_000,
      retryMaxMs: 21_600_000,
      fetchTimeoutMs: 30_000,
      rateLimitBackoffMs: 120_000,
      rateLimitMaxBackoffMs: 3_600_000,
      errorStreakLimit: 10,
      errorStreakBackoffMs: 900_000,
      imageQuality: 'high',
      fallbackQuality: 'low',
      maxReconstructionUrls: 1,
      minImageBytes: 100,
      maxImageBytes: 8_000_000,
      maxJsonBytes: 8_000_000,
      maxStoredBytes: 20_000_000_000,
      maxDailyOperations: 150_000,
      detailRefreshDays: 90,
      missingRecheckDays: 30,
    } satisfies Config)
  })

  it('SYNC_CRON par défaut « 7 3 * * MON » : lundi 03:07 UTC, hors des multiples de 5 du traitement', () => {
    const config = parseConfig({})
    expect(config.syncCron).toBe('7 3 * * MON')
    expect(next(config.syncCron, '2026-11-13T10:00:00.000Z')).toBe('2026-11-16T03:07:00.000Z')
    expect(next(config.syncCron, '2026-11-16T03:07:00.000Z')).toBe('2026-11-23T03:07:00.000Z')
    // Jamais au même instant qu'une exécution « */5 » du traitement.
    const sync = nextCronTime(config.syncCron, NOW) as number
    expect(nextCronTime(config.processCron, sync - 1)).not.toBe(sync)
  })

  it('wrangler.jsonc : les vars reprennent les nouveaux défauts (SYNC_CRON, MAX_JSON_BYTES, MAX_RELEASES)', () => {
    expect(env.SYNC_CRON).toBe('7 3 * * MON')
    expect(env.PROCESS_CRON).toBe('*/5 * * * *')
    // Le nombre de voies (4 en production, 2 par défaut dans le code) entre dans le calcul des verrous.
    const defaults = parseConfig({ MAX_CONCURRENCY: env.MAX_CONCURRENCY })
    expect(testConfig()).toMatchObject({
      syncCron: defaults.syncCron,
      processCron: defaults.processCron,
      maxJsonBytes: defaults.maxJsonBytes,
      maxReleases: defaults.maxReleases,
      runTimeBudgetMs: defaults.runTimeBudgetMs,
      runnerLeaseMs: defaults.runnerLeaseMs,
      jobLeaseMs: defaults.jobLeaseMs,
    })
  })

  it('SCHEDULED_MAX_MS : 15 min, durée maximale d’une invocation planifiée', () => {
    expect(SCHEDULED_MAX_MS).toBe(900_000)
  })

  it('entiers : ramenés dans leurs bornes, arrondis vers le bas, valeur par défaut si illisibles', () => {
    const concurrency = (raw: string) => parseConfig({ MAX_CONCURRENCY: raw }).maxConcurrency
    expect(concurrency('99')).toBe(4)
    expect(concurrency('0')).toBe(1)
    expect(concurrency('-5')).toBe(1)
    expect(concurrency('3.9')).toBe(3)
    expect(concurrency('abc')).toBe(2)
    expect(concurrency('')).toBe(2)
    expect(concurrency('   ')).toBe(2)
    expect(concurrency('Infinity')).toBe(2)
    expect(parseConfig({ MIN_REQUEST_INTERVAL_MS: '1' }).minRequestIntervalMs).toBe(50)
    expect(parseConfig({ MIN_REQUEST_INTERVAL_MS: '999999' }).minRequestIntervalMs).toBe(60_000)
    expect(parseConfig({ D1_QUERY_BUDGET: '5000' }).d1QueryBudget).toBe(950)
    expect(parseConfig({ D1_QUERY_BUDGET: '1' }).d1QueryBudget).toBe(50)
    expect(parseConfig({ SUBREQUEST_BUDGET: '20000' }).subrequestBudget).toBe(9_500)
    // Défaut : dépassement possible = 2 × 30 s + 2 × 5 s + 60 s = 130 s, donc budget ≤ 900 − 130 s.
    expect(parseConfig({ RUN_TIME_BUDGET_MS: '9999999' }).runTimeBudgetMs).toBe(770_000)
    expect(parseConfig({ MAX_ATTEMPTS: '0' }).maxAttempts).toBe(1)
    expect(parseConfig({ MIN_IMAGE_BYTES: '1' }).minImageBytes).toBe(12)
    expect(parseConfig({ MAX_RECONSTRUCTION_URLS: '9' }).maxReconstructionUrls).toBe(5)
    expect(parseConfig({ MAX_RECONSTRUCTION_URLS: '0' }).maxReconstructionUrls).toBe(0)
  })

  it('D1_QUERY_BUDGET au moins 50, SUBREQUEST_BUDGET au moins 200', () => {
    const d1 = (raw: string) => parseConfig({ D1_QUERY_BUDGET: raw }).d1QueryBudget
    expect(d1('0')).toBe(50)
    expect(d1('20')).toBe(50)
    expect(d1('49')).toBe(50)
    expect(d1('50')).toBe(50)
    expect(d1('51')).toBe(51)
    expect(d1('950')).toBe(950)
    expect(d1('951')).toBe(950)
    const subrequests = (raw: string) => parseConfig({ SUBREQUEST_BUDGET: raw }).subrequestBudget
    expect(subrequests('1')).toBe(200)
    expect(subrequests('100')).toBe(200)
    expect(subrequests('199')).toBe(200)
    expect(subrequests('200')).toBe(200)
    expect(subrequests('201')).toBe(201)
    expect(subrequests('9501')).toBe(9_500)
  })

  it('MAX_JSON_BYTES : 8 000 000 par défaut, 12 000 000 au plus, 100 000 au moins', () => {
    const json = (raw: string | undefined) => parseConfig({ MAX_JSON_BYTES: raw }).maxJsonBytes
    expect(json(undefined)).toBe(8_000_000)
    expect(json('abc')).toBe(8_000_000)
    expect(json('30000000')).toBe(12_000_000)
    expect(json('12000001')).toBe(12_000_000)
    expect(json('12000000')).toBe(12_000_000)
    expect(json('11999999')).toBe(11_999_999)
    expect(json('1')).toBe(100_000)
  })

  it('MAX_RELEASES : 48 par défaut, borné à 1..1000, arrondi vers le bas', () => {
    const releases = (raw: string | undefined) => parseConfig({ MAX_RELEASES: raw }).maxReleases
    expect(releases(undefined)).toBe(48)
    expect(releases('')).toBe(48)
    expect(releases('abc')).toBe(48)
    expect(releases('0')).toBe(1)
    expect(releases('-3')).toBe(1)
    expect(releases('1')).toBe(1)
    expect(releases('7.9')).toBe(7)
    expect(releases('1000')).toBe(1_000)
    expect(releases('1001')).toBe(1_000)
    expect(releases('99999')).toBe(1_000)
  })

  it('RUN_TIME_BUDGET_MS : jamais au-delà de 15 min moins le dépassement possible', () => {
    expect(parseConfig({ RUN_TIME_BUDGET_MS: '770000' }).runTimeBudgetMs).toBe(770_000)
    expect(parseConfig({ RUN_TIME_BUDGET_MS: '770001' }).runTimeBudgetMs).toBe(770_000)
    expect(parseConfig({ RUN_TIME_BUDGET_MS: '600000' }).runTimeBudgetMs).toBe(600_000)
    expect(parseConfig({ RUN_TIME_BUDGET_MS: '1' }).runTimeBudgetMs).toBe(5_000)
    // Le dépassement grandit avec le délai d'expiration, la concurrence et l'intervalle maximal.
    const slow = parseConfig({ RUN_TIME_BUDGET_MS: '840000', FETCH_TIMEOUT_MS: '60000' })
    expect(overrunMs(slow)).toBe(190_000)
    expect(slow.runTimeBudgetMs).toBe(710_000)
    // Pire cas : 2 × 120 s + 4 × 120 s + 60 s = 780 s → 120 s de budget au plus.
    const worst = parseConfig({ RUN_TIME_BUDGET_MS: '840000', FETCH_TIMEOUT_MS: '120000', MAX_CONCURRENCY: '4', MAX_REQUEST_INTERVAL_MS: '120000' })
    expect(overrunMs(worst)).toBe(780_000)
    expect(worst).toMatchObject({ runTimeBudgetMs: 120_000, runnerLeaseMs: 900_000, jobLeaseMs: 900_000 })
    // L'intervalle maximal effectif (relevé à l'intervalle minimal) entre dans le calcul.
    const raised = parseConfig({ RUN_TIME_BUDGET_MS: '840000', MIN_REQUEST_INTERVAL_MS: '60000', MAX_REQUEST_INTERVAL_MS: '50' })
    expect(raised.maxRequestIntervalMs).toBe(60_000)
    expect(raised.runTimeBudgetMs).toBe(SCHEDULED_MAX_MS - (2 * 30_000 + 2 * 60_000 + 60_000))
  })

  it('verrous : exécuteur ≥ budget + dépassement, tâche ≥ exécuteur', () => {
    expect(parseConfig({ RUNNER_LEASE_MS: '30000' }).runnerLeaseMs).toBe(370_000)
    expect(parseConfig({ RUNNER_LEASE_MS: '369999' }).runnerLeaseMs).toBe(370_000)
    expect(parseConfig({ RUNNER_LEASE_MS: '400000' }).runnerLeaseMs).toBe(400_000)
    // Un verrou d'exécuteur plus long que celui des tâches relève ce dernier.
    expect(parseConfig({ RUNNER_LEASE_MS: '1000000' })).toMatchObject({ runnerLeaseMs: 1_000_000, jobLeaseMs: 1_000_000 })
    expect(parseConfig({ JOB_LEASE_MS: '60000' }).jobLeaseMs).toBe(370_000)
    expect(parseConfig({ JOB_LEASE_MS: '7200000' }).jobLeaseMs).toBe(7_200_000)
    // Un budget plus long allonge les deux verrous.
    expect(parseConfig({ RUN_TIME_BUDGET_MS: '600000' })).toMatchObject({ runTimeBudgetMs: 600_000, runnerLeaseMs: 730_000, jobLeaseMs: 730_000 })
    expect(parseConfig({ RUN_TIME_BUDGET_MS: '9999999', RUNNER_LEASE_MS: '1', JOB_LEASE_MS: '1' })).toMatchObject({
      runTimeBudgetMs: 770_000,
      runnerLeaseMs: 900_000,
      jobLeaseMs: 900_000,
    })
  })

  it('propriété : budget + dépassement ≤ 15 min, verrou de tâche ≥ verrou de l’exécuteur ≥ budget + dépassement', () => {
    const random = seeded(20_261_113)
    const pick = (): string => {
      if (random() < 0.1) return ['', 'abc', '-5', 'Infinity', '1e9'][Math.floor(random() * 5)] as string
      return String(Math.floor(random() * 10 ** (1 + Math.floor(random() * 7))))
    }
    const keys = ['RUN_TIME_BUDGET_MS', 'FETCH_TIMEOUT_MS', 'MAX_CONCURRENCY', 'MIN_REQUEST_INTERVAL_MS', 'MAX_REQUEST_INTERVAL_MS', 'RUNNER_LEASE_MS', 'JOB_LEASE_MS']
    for (let i = 0; i < 500; i++) {
      const source: ConfigSource = {}
      for (const key of keys) if (random() < 0.8) source[key] = pick()
      const config = parseConfig(source)
      const overrun = overrunMs(config)
      expect(config.runTimeBudgetMs).toBeGreaterThanOrEqual(5_000)
      expect(config.runTimeBudgetMs + overrun).toBeLessThanOrEqual(SCHEDULED_MAX_MS)
      expect(config.runnerLeaseMs).toBeGreaterThanOrEqual(config.runTimeBudgetMs + overrun)
      expect(config.jobLeaseMs).toBeGreaterThanOrEqual(config.runnerLeaseMs)
      expect(config.maxRequestIntervalMs).toBeGreaterThanOrEqual(config.minRequestIntervalMs)
    }
  })

  it('l’intervalle maximal entre requêtes n’est jamais inférieur à l’intervalle minimal', () => {
    const config = parseConfig({ MIN_REQUEST_INTERVAL_MS: '1000', MAX_REQUEST_INTERVAL_MS: '500' })
    expect(config.minRequestIntervalMs).toBe(1_000)
    expect(config.maxRequestIntervalMs).toBe(1_000)
  })

  it('textes : espaces retirés, valeur par défaut si vides, barres obliques finales de l’API ôtées', () => {
    expect(parseConfig({ PROCESS_CRON: '  */10 * * * *  ' }).processCron).toBe('*/10 * * * *')
    expect(parseConfig({ PROCESS_CRON: '   ' }).processCron).toBe('*/5 * * * *')
    expect(parseConfig({ TCGDEX_API_BASE: 'https://api.tcgdex.net/v2///' }).apiBase).toBe(DEFAULT_API)
    expect(parseConfig({ TCGDEX_API_BASE: '  https://api.tcgdex.net/v2/  ' }).apiBase).toBe(DEFAULT_API)
    expect(parseConfig({ TCGDEX_API_BASE: '   ' }).apiBase).toBe(DEFAULT_API)
  })

  it('TCGDEX_API_BASE : seulement https://api.tcgdex.net…, sinon la valeur par défaut', () => {
    const base = (raw: string) => parseConfig({ TCGDEX_API_BASE: raw }).apiBase
    expect(base('https://api.tcgdex.net/v2')).toBe(DEFAULT_API)
    expect(base('https://api.tcgdex.net/v3')).toBe('https://api.tcgdex.net/v3')
    expect(base('https://api.tcgdex.net')).toBe('https://api.tcgdex.net')
    for (const raw of [
      'https://exemple.test/v2///',
      'https://exemple.test/v2',
      'http://api.tcgdex.net/v2',
      'ftp://api.tcgdex.net/v2',
      'https://api.tcgdex.net.evil.example/v2',
      'https://evil.example/api.tcgdex.net/v2',
      'https://api.tcgdex.net@evil.example/v2',
      'https://api.tcgdex.net:8443/v2',
      'https://assets.tcgdex.net/v2',
      'https://tcgdex.net/v2',
      'https://api.tcgdex.net/v2?x=1',
      'https://api.tcgdex.net/v2#x',
      'api.tcgdex.net/v2',
      'pas une url',
    ]) {
      expect(base(raw), raw).toBe(DEFAULT_API)
    }
  })

  it('TCGDEX_API_BASE avec identifiants (user:pass@) → valeur par défaut', () => {
    expect(parseConfig({ TCGDEX_API_BASE: 'https://user:pass@api.tcgdex.net/v2' }).apiBase).toBe(DEFAULT_API)
  })

  it('TCGDEX_API_BASE terminée par « ? » ou « # » vides → jamais de « ? » ni de « # » dans la base', () => {
    const bases = ['https://api.tcgdex.net/v2?', 'https://api.tcgdex.net/v2#'].map((raw) => parseConfig({ TCGDEX_API_BASE: raw }).apiBase)
    expect(bases).toEqual([DEFAULT_API, DEFAULT_API])
  })

  it('FALLBACK_QUALITY : vide → null, égale à la qualité principale → null, invalide → null', () => {
    expect(parseConfig({ FALLBACK_QUALITY: '' }).fallbackQuality).toBeNull()
    expect(parseConfig({ FALLBACK_QUALITY: '   ' }).fallbackQuality).toBeNull()
    expect(parseConfig({ FALLBACK_QUALITY: 'high' }).fallbackQuality).toBeNull()
    expect(parseConfig({ IMAGE_QUALITY: 'low' }).fallbackQuality).toBeNull()
    expect(parseConfig({ FALLBACK_QUALITY: 'medium' }).fallbackQuality).toBeNull()
    expect(parseConfig({ IMAGE_QUALITY: 'low', FALLBACK_QUALITY: 'high' })).toMatchObject({ imageQuality: 'low', fallbackQuality: 'high' })
    expect(parseConfig({ FALLBACK_QUALITY: ' low ' }).fallbackQuality).toBe('low')
    expect(parseConfig({ IMAGE_QUALITY: 'ultra' }).imageQuality).toBe('high')
  })

  it('FALLBACK_QUALITY égale à la qualité principale EFFECTIVE (IMAGE_QUALITY invalide) → null', () => {
    const config = parseConfig({ IMAGE_QUALITY: 'ultra', FALLBACK_QUALITY: 'high' })
    expect(config.imageQuality).toBe('high')
    expect(config.fallbackQuality).toBeNull()
    // Une même URL n'est donc jamais demandée deux fois au CDN.
    const card = { language: 'fr' as const, api_image_url: `${ASSETS}/fr/swsh/swsh3/136`, series_id: 'swsh', set_tcgdex_id: 'swsh3', local_id: '136' }
    expect(imageCandidates(config, card).map((candidate) => candidate.url)).toEqual([`${ASSETS}/fr/swsh/swsh3/136/high.webp`])
  })

  it('FALLBACK_QUALITY différente de la qualité principale effective → gardée', () => {
    expect(parseConfig({ IMAGE_QUALITY: 'ultra', FALLBACK_QUALITY: 'low' })).toMatchObject({ imageQuality: 'high', fallbackQuality: 'low' })
    expect(parseConfig({ IMAGE_QUALITY: '', FALLBACK_QUALITY: 'low' })).toMatchObject({ imageQuality: 'high', fallbackQuality: 'low' })
    expect(parseConfig({ IMAGE_QUALITY: ' low ', FALLBACK_QUALITY: 'low' })).toMatchObject({ imageQuality: 'low', fallbackQuality: null })
    expect(parseConfig({ IMAGE_QUALITY: ' low ', FALLBACK_QUALITY: 'high' })).toMatchObject({ imageQuality: 'low', fallbackQuality: 'high' })
  })

  it('LANGUAGES : langues inconnues filtrées, doublons retirés, défaut si aucune valide', () => {
    expect(parseConfig({ LANGUAGES: 'fr,en,ja' }).languages).toEqual(['fr', 'ja'])
    expect(parseConfig({ LANGUAGES: 'ja' }).languages).toEqual(['ja'])
    expect(parseConfig({ LANGUAGES: ' ja , fr ' }).languages).toEqual(['ja', 'fr'])
    expect(parseConfig({ LANGUAGES: 'ja,ja' }).languages).toEqual(['ja'])
    expect(parseConfig({ LANGUAGES: 'en,de' }).languages).toEqual(['fr', 'ja'])
    expect(parseConfig({ LANGUAGES: 'FR' }).languages).toEqual(['fr', 'ja'])
    expect(parseConfig({ LANGUAGES: '' }).languages).toEqual(['fr', 'ja'])
  })
})

// ——— src/utils/http.ts ———

describe('parseRetryAfter', () => {
  it('en secondes', () => {
    expect(parseRetryAfter('120', NOW)).toBe(120_000)
    expect(parseRetryAfter(' 7 ', NOW)).toBe(7_000)
    expect(parseRetryAfter('0', NOW)).toBe(0)
  })

  it('en date HTTP (future → durée restante, passée → 0)', () => {
    expect(parseRetryAfter(new Date(NOW + 90_000).toUTCString(), NOW)).toBe(90_000)
    expect(parseRetryAfter('Fri, 13 Nov 2026 10:02:00 GMT', NOW)).toBe(120_000)
    expect(parseRetryAfter('Wed, 21 Oct 2015 07:28:00 GMT', NOW)).toBe(0)
  })

  it('absent ou illisible → null', () => {
    expect(parseRetryAfter(null, NOW)).toBeNull()
    expect(parseRetryAfter('', NOW)).toBeNull()
    expect(parseRetryAfter('   ', NOW)).toBeNull()
    expect(parseRetryAfter('bientôt', NOW)).toBeNull()
  })

  it('valeurs illisibles qui ressemblent à des nombres → null (jamais 0)', () => {
    expect(parseRetryAfter('-1', NOW)).toBeNull()
    expect(parseRetryAfter('foo 12', NOW)).toBeNull()
    expect(parseRetryAfter('1.5', NOW)).toBeNull()
  })

  it('secondes : seulement des entiers de 1 à 10 chiffres ASCII', () => {
    expect(parseRetryAfter('5', NOW)).toBe(5_000)
    expect(parseRetryAfter('0000000005', NOW)).toBe(5_000)
    for (const value of ['12345678901', '+5', '5s', '1e3', '0x10', '1 2', '1_000', '1,5', '٣', '５', 'Infinity', 'NaN', '-0']) {
      expect(parseRetryAfter(value, NOW), value).toBeNull()
    }
  })

  it('bornée à RETRY_AFTER_MAX_MS = 24 h (secondes ou date lointaine)', () => {
    expect(RETRY_AFTER_MAX_MS).toBe(86_400_000)
    expect(parseRetryAfter('86399', NOW)).toBe(86_399_000)
    expect(parseRetryAfter('86400', NOW)).toBe(86_400_000)
    expect(parseRetryAfter('86401', NOW)).toBe(86_400_000)
    expect(parseRetryAfter('9999999999', NOW)).toBe(86_400_000)
    expect(parseRetryAfter(new Date(NOW + 86_400_000).toUTCString(), NOW)).toBe(86_400_000)
    expect(parseRetryAfter(new Date(NOW + 86_401_000).toUTCString(), NOW)).toBe(86_400_000)
    expect(parseRetryAfter('Sat, 13 Nov 2027 10:00:00 GMT', NOW)).toBe(86_400_000)
  })

  it('date : seulement au format IMF-fixdate (« Fri, 13 Nov 2026 10:02:00 GMT »)', () => {
    expect(parseRetryAfter('  Fri, 13 Nov 2026 10:02:00 GMT  ', NOW)).toBe(120_000)
    for (const value of [
      'Friday, 13-Nov-26 10:02:00 GMT', // RFC 850
      'Fri Nov 13 10:02:00 2026', // asctime
      '2026-11-13T10:02:00Z', // ISO 8601
      'Fri, 13 Nov 2026 10:02:00 UTC',
      'Fri, 13 Nov 2026 10:02:00 +0000',
      'Fri, 13 Nov 2026 10:02:00',
      'fri, 13 nov 2026 10:02:00 gmt',
      'Fri, 3 Nov 2026 10:02:00 GMT',
      'Fri,13 Nov 2026 10:02:00 GMT',
      '13 Nov 2026 10:02:00 GMT',
      'Fri, 13 Nov 2026 10:02 GMT',
      'Fri, 32 Nov 2026 10:02:00 GMT',
    ]) {
      expect(parseRetryAfter(value, NOW), value).toBeNull()
    }
  })
})

describe('classify', () => {
  const cases: [number, string][] = [
    [200, 'ok'],
    [204, 'ok'],
    [404, 'not_found'],
    [410, 'not_found'],
    [429, 'rate_limited'],
    [500, 'server_error'],
    [503, 'server_error'],
    [408, 'server_error'],
    [400, 'client_error'],
    [403, 'client_error'],
  ]
  it.each(cases)('HTTP %i → %s', async (status, kind) => {
    const outcome = await classify(new Response(status === 204 ? null : 'corps', { status }), NOW)
    expect(outcome).toMatchObject({ kind, status })
  })

  it('une réponse ok garde son corps lisible ; les autres sont libérées', async () => {
    const ok = await classify(new Response('corps', { status: 200 }), NOW)
    expect(ok.kind).toBe('ok')
    if (ok.kind === 'ok') expect(await ok.response.text()).toBe('corps')
    const notFound = new Response('page 404', { status: 404 })
    await classify(notFound, NOW)
    expect(notFound.bodyUsed).toBe(true)
  })

  it('429 : lit Retry-After (secondes ou date), null s’il est absent', async () => {
    expect(await classify(new Response(null, { status: 429, headers: { 'retry-after': '30' } }), NOW)).toEqual({ kind: 'rate_limited', status: 429, retryAfterMs: 30_000 })
    expect(await classify(new Response(null, { status: 429, headers: { 'retry-after': new Date(NOW + 5_000).toUTCString() } }), NOW)).toEqual({ kind: 'rate_limited', status: 429, retryAfterMs: 5_000 })
    expect(await classify(new Response(null, { status: 429 }), NOW)).toEqual({ kind: 'rate_limited', status: 429, retryAfterMs: null })
  })

  it('429 : Retry-After illisible → null (l’attente par défaut s’applique), aberrant → borné à 24 h', async () => {
    for (const value of ['-1', '1.5', 'foo 12', '2026-11-13T12:00:00Z']) {
      expect(await classify(new Response(null, { status: 429, headers: { 'retry-after': value } }), NOW), value).toEqual({ kind: 'rate_limited', status: 429, retryAfterMs: null })
    }
    expect(await classify(new Response(null, { status: 429, headers: { 'retry-after': '99999999' } }), NOW)).toEqual({ kind: 'rate_limited', status: 429, retryAfterMs: RETRY_AFTER_MAX_MS })
  })
})

describe('fetchClassified', () => {
  it('une erreur réseau est classée, jamais levée', async () => {
    const fetcher = (async () => {
      throw new TypeError('connexion refusée')
    }) as typeof fetch
    expect(await fetchClassified(fetcher, 'https://api.tcgdex.net/v2/fr/sets', {}, 1_000, () => NOW)).toEqual({
      kind: 'network_error',
      status: null,
      message: 'TypeError: connexion refusée',
    })
  })

  it('transmet la requête avec un signal d’expiration et classe la réponse', async () => {
    const seen: { url: string; method: string | undefined; signal: boolean }[] = []
    const fetcher = (async (input: RequestInfo | URL, init?: RequestInit) => {
      seen.push({ url: String(input), method: init?.method, signal: init?.signal instanceof AbortSignal })
      return new Response('indisponible', { status: 503 })
    }) as typeof fetch
    expect(await fetchClassified(fetcher, 'https://api.tcgdex.net/v2/fr/sets', { method: 'GET' }, 1_000, () => NOW)).toEqual({ kind: 'server_error', status: 503 })
    expect(seen).toEqual([{ url: 'https://api.tcgdex.net/v2/fr/sets', method: 'GET', signal: true }])
  })

  it('une requête trop longue est interrompue et classée en erreur réseau', async () => {
    const fetcher = ((_input: RequestInfo | URL, init?: RequestInit) =>
      new Promise<Response>((_resolve, reject) => {
        init?.signal?.addEventListener('abort', () => reject(init.signal?.reason))
      })) as typeof fetch
    const outcome = await fetchClassified(fetcher, 'https://api.tcgdex.net/v2/fr/sets', {}, 20, () => NOW)
    expect(outcome.kind).toBe('network_error')
    if (outcome.kind === 'network_error') expect(outcome.message).toMatch(/timeout|abort/i)
  })
})

describe('readCapped', () => {
  it('rend les octets d’un corps sous la limite (ou exactement à la limite)', async () => {
    expect(await readCapped(new Response(new Uint8Array([1, 2, 3])), 10)).toEqual(new Uint8Array([1, 2, 3]))
    expect(await readCapped(new Response(new Uint8Array([1, 2, 3])), 3)).toEqual(new Uint8Array([1, 2, 3]))
    const { stream } = countingStream(40, 3)
    const bytes = await readCapped(new Response(stream), 120)
    expect(bytes.byteLength).toBe(120)
    expect([bytes[0], bytes[40], bytes[119]]).toEqual([1, 2, 3])
  })

  it('un corps absent donne zéro octet', async () => {
    expect(await readCapped(new Response(null), 10)).toEqual(new Uint8Array())
  })

  it('Content-Length au-dessus de la limite → BodyTooLargeError, corps annulé sans être lu', async () => {
    const { stream, state } = countingStream(50, 100)
    const response = new Response(stream, { headers: { 'content-length': '5000' } })
    const error = await readCapped(response, 100).catch((caught: unknown) => caught)
    expect(error).toBeInstanceOf(BodyTooLargeError)
    expect((error as Error).name).toBe('BodyTooLargeError')
    expect((error as Error).message).toContain('5000')
    await new Promise((resolve) => setTimeout(resolve, 5))
    expect(state.cancelled).toBe(true)
    expect(state.pulls).toBeLessThanOrEqual(2)
  })

  it('flux sans Content-Length au-dessus de la limite → BodyTooLargeError, lecture arrêtée tôt', async () => {
    const { stream, state } = countingStream(50, 1_000)
    const response = new Response(stream)
    expect(response.headers.get('content-length')).toBeNull()
    await expect(readCapped(response, 120)).rejects.toBeInstanceOf(BodyTooLargeError)
    expect(state.cancelled).toBe(true)
    expect(state.pulls).toBeLessThan(10)
  })

  it('un Content-Length mensonger (trop petit) n’empêche pas la coupure en flux', async () => {
    const { stream } = countingStream(50, 100)
    await expect(readCapped(new Response(stream, { headers: { 'content-length': '10' } }), 120)).rejects.toBeInstanceOf(BodyTooLargeError)
  })

  it('Content-Length exact, sans encodage : un seul tampon de la taille annoncée, octets intacts', async () => {
    const { stream } = countingStream(40, 3)
    const bytes = await readCapped(new Response(stream, { headers: { 'content-length': '120' } }), 1_000)
    expect(bytes.byteLength).toBe(120)
    expect(bytes.buffer.byteLength).toBe(120)
    expect([bytes[0], bytes[39], bytes[40], bytes[79], bytes[80], bytes[119]]).toEqual([1, 1, 2, 2, 3, 3])
    // Exactement à la limite.
    const atLimit = countingStream(40, 3)
    expect((await readCapped(new Response(atLimit.stream, { headers: { 'content-length': '120' } }), 120)).byteLength).toBe(120)
  })

  it('Content-Length plus grand que le corps reçu : seuls les octets reçus sont rendus', async () => {
    const { stream } = countingStream(40, 2)
    const bytes = await readCapped(new Response(stream, { headers: { 'content-length': '200' } }), 1_000)
    expect(bytes.byteLength).toBe(80)
    expect(bytes.buffer.byteLength).toBe(80)
    expect([bytes[0], bytes[39], bytes[40], bytes[79]]).toEqual([1, 1, 2, 2])
    expect(await readCapped(new Response(countingStream(40, 0).stream, { headers: { 'content-length': '50' } }), 1_000)).toEqual(new Uint8Array())
  })

  it('Content-Length plus petit que le corps (sous la limite) : tous les octets, dans l’ordre', async () => {
    for (const declared of ['70', '50', '0', '1']) {
      const { stream } = countingStream(50, 3)
      const bytes = await readCapped(new Response(stream, { headers: { 'content-length': declared } }), 1_000)
      expect(bytes.byteLength, declared).toBe(150)
      expect([bytes[0], bytes[49], bytes[50], bytes[99], bytes[100], bytes[149]], declared).toEqual([1, 1, 2, 2, 3, 3])
    }
  })

  it('Content-Length trop petit et premier morceau en excès au-delà de la limite → BodyTooLargeError, flux annulé', async () => {
    const { stream, state } = countingStream(200, 50)
    const error = await readCapped(new Response(stream, { headers: { 'content-length': '10' } }), 120).catch((caught: unknown) => caught)
    expect(error).toBeInstanceOf(BodyTooLargeError)
    expect((error as Error).message).toContain('200')
    expect(state.cancelled).toBe(true)
    expect(state.pulls).toBeLessThanOrEqual(2)
  })

  it('Content-Length trop petit, dépassement plus loin dans le flux → BodyTooLargeError, flux annulé', async () => {
    const { stream, state } = countingStream(50, 1_000)
    await expect(readCapped(new Response(stream, { headers: { 'content-length': '60' } }), 120)).rejects.toBeInstanceOf(BodyTooLargeError)
    expect(state.cancelled).toBe(true)
    expect(state.pulls).toBeLessThan(10)
  })

  it('avec Content-Encoding (gzip…), la longueur annoncée ne dimensionne pas le tampon', async () => {
    for (const encoding of ['gzip', 'BR', ' deflate ']) {
      const { stream } = countingStream(50, 3)
      const bytes = await readCapped(new Response(stream, { headers: { 'content-length': '20', 'content-encoding': encoding } }), 1_000)
      expect(bytes.byteLength, encoding).toBe(150)
      expect(bytes.buffer.byteLength, encoding).toBe(150)
      expect([bytes[0], bytes[50], bytes[149]], encoding).toEqual([1, 2, 3])
    }
    // « identity » n'est pas un encodage : la longueur annoncée reste utilisable.
    const identity = countingStream(40, 3)
    expect((await readCapped(new Response(identity.stream, { headers: { 'content-length': '120', 'content-encoding': 'identity' } }), 1_000)).byteLength).toBe(120)
  })

  it('Content-Length illisible ou négatif : lecture générale, toujours plafonnée', async () => {
    for (const declared of ['abc', '-5']) {
      const { stream } = countingStream(40, 3)
      expect((await readCapped(new Response(stream, { headers: { 'content-length': declared } }), 1_000)).byteLength, declared).toBe(120)
      const big = countingStream(50, 100)
      await expect(readCapped(new Response(big.stream, { headers: { 'content-length': declared } }), 120), declared).rejects.toBeInstanceOf(BodyTooLargeError)
    }
  })
})

// ——— src/utils/limiter.ts ———

describe('RateLimiter', () => {
  it('espace les acquisitions successives d’au moins intervalMs (horloge simulée)', async () => {
    const clock = new Clock()
    const limiter = new RateLimiter(250, clock.now, clock.wait)
    const times: number[] = []
    for (let i = 0; i < 4; i++) {
      await limiter.acquire()
      times.push(clock.now())
    }
    const start = times[0] as number
    expect(times.map((t) => t - start)).toEqual([0, 250, 500, 750])
  })

  it('des acquisitions simultanées reçoivent des créneaux échelonnés', async () => {
    const clock = new Clock()
    const waits: number[] = []
    const limiter = new RateLimiter(250, clock.now, async (ms) => {
      waits.push(ms)
    })
    await Promise.all([limiter.acquire(), limiter.acquire(), limiter.acquire(), limiter.acquire()])
    expect(waits).toEqual([0, 250, 500, 750])
  })

  it('aucune attente après une période d’inactivité', async () => {
    const clock = new Clock()
    const waits: number[] = []
    const limiter = new RateLimiter(250, clock.now, async (ms) => {
      waits.push(ms)
      clock.advance(ms)
    })
    await limiter.acquire()
    clock.advance(10_000)
    await limiter.acquire()
    expect(waits).toEqual([0, 0])
  })

  it('slowDown multiplie l’intervalle sans jamais dépasser le plafond', async () => {
    const clock = new Clock()
    const limiter = new RateLimiter(250, clock.now, clock.wait)
    limiter.slowDown(1.5, 5_000)
    expect(limiter.interval).toBe(375)
    limiter.slowDown(1.5, 5_000)
    expect(limiter.interval).toBe(563)
    for (let i = 0; i < 20; i++) limiter.slowDown(1.5, 5_000)
    expect(limiter.interval).toBe(5_000)
    // Le nouvel intervalle s'applique aux acquisitions suivantes.
    await limiter.acquire()
    const first = clock.now()
    await limiter.acquire()
    expect(clock.now() - first).toBe(5_000)
    const capped = new RateLimiter(250, clock.now, clock.wait)
    capped.slowDown(2, 300)
    expect(capped.interval).toBe(300)
  })

  it('speedUp ramène progressivement l’intervalle au minimum, jamais en dessous', () => {
    const clock = new Clock()
    const limiter = new RateLimiter(250, clock.now, clock.wait)
    for (let i = 0; i < 20; i++) limiter.slowDown(1.5, 5_000)
    expect(limiter.interval).toBe(5_000)
    limiter.speedUp(0.8, 250)
    expect(limiter.interval).toBe(4_000)
    // Une quinzaine de réponses normales suffisent à revenir au débit nominal.
    for (let i = 0; i < 15; i++) limiter.speedUp(0.8, 250)
    expect(limiter.interval).toBe(250)
    limiter.speedUp(0.8, 250)
    expect(limiter.interval).toBe(250)
  })
})

// ——— src/tcgdex/images.ts ———

describe('URL des images', () => {
  it('isTrustedImageBase n’accepte que https://assets.tcgdex.net sans requête ni fragment', () => {
    expect(isTrustedImageBase(`${ASSETS}/fr/swsh/swsh3/136`, 'fr')).toBe(true)
    expect(isTrustedImageBase(`${ASSETS}/ja/SV/SV1a/001`, 'ja')).toBe(true)
    expect(isTrustedImageBase(`${ASSETS}/fr/swsh/swsh3/136/`, 'fr')).toBe(true)
    expect(isTrustedImageBase('https://ASSETS.tcgdex.net/fr/swsh/swsh3/136', 'fr')).toBe(true)
    for (const base of [
      null,
      '',
      'http://assets.tcgdex.net/fr/swsh/swsh3/136',
      'https://evil.example/fr/swsh/swsh3/136',
      'https://assets.tcgdex.net.evil.example/fr/swsh/swsh3/136',
      'https://assets.tcgdex.net:8443/fr/swsh/swsh3/136',
      'https://api.tcgdex.net/v2/fr/cards/swsh3-136',
      `${ASSETS}/fr/swsh/swsh3/136?x=1`,
      `${ASSETS}/fr/swsh/swsh3/136#frag`,
      'pas une url',
    ]) {
      expect(isTrustedImageBase(base, 'fr'), String(base)).toBe(false)
    }
  })

  it('isTrustedImageBase refuse « ? » ou « # » n’importe où, même vides', () => {
    for (const base of [
      `${ASSETS}/fr/swsh/swsh3/136?`,
      `${ASSETS}/fr/swsh/swsh3/136#`,
      `${ASSETS}/fr/swsh/swsh3/136/?`,
      `${ASSETS}/fr/swsh/swsh3/136?#`,
      `${ASSETS}/fr/swsh?/swsh3/136`,
      `${ASSETS}/fr/swsh#/swsh3/136`,
      `${ASSETS}/fr?/swsh/swsh3/136`,
      `${ASSETS}?/fr/swsh/swsh3/136`,
      `${ASSETS}#/fr/swsh/swsh3/136`,
    ]) {
      expect(isTrustedImageBase(base, 'fr'), base).toBe(false)
    }
  })

  it('isTrustedImageBase exige la langue de la carte comme premier segment du chemin', () => {
    expect(isTrustedImageBase(`${ASSETS}/ja/SV/SV1a/001`, 'fr')).toBe(false)
    expect(isTrustedImageBase(`${ASSETS}/fr/swsh/swsh3/136`, 'ja')).toBe(false)
    for (const base of [
      `${ASSETS}/univ/swsh/swsh3/136`,
      `${ASSETS}/en/swsh/swsh3/136`,
      `${ASSETS}/FR/swsh/swsh3/136`,
      `${ASSETS}/french/swsh/swsh3/136`,
      `${ASSETS}/fra/swsh/swsh3/136`,
      `${ASSETS}/swsh/fr/swsh3/136`,
      `${ASSETS}//fr/swsh/swsh3/136`,
      `${ASSETS}/%66r/swsh/swsh3/136`,
      `${ASSETS}/`,
      ASSETS,
      // Chemin normalisé : « .. » ne permet pas de changer de langue.
      `${ASSETS}/fr/../ja/SV/SV1a/001`,
      `${ASSETS}/fr/%2e%2e/ja/SV/SV1a/001`,
      `${ASSETS}/fr/..`,
    ]) {
      expect(isTrustedImageBase(base, 'fr'), base).toBe(false)
    }
    expect(isTrustedImageBase(`${ASSETS}/ja/../fr/swsh/swsh3/136`, 'ja')).toBe(false)
  })

  it('une base contenant « ? » ou « # » ne produit jamais d’URL hors du chemin de l’image', () => {
    const config = parseConfig({})
    for (const base of [`${ASSETS}/fr/swsh/swsh3/136?`, `${ASSETS}/fr/swsh/swsh3/136#`, `${ASSETS}/fr/swsh?/swsh3/136`, `${ASSETS}/fr/swsh/swsh3/136?x=1`]) {
      const candidates = imageCandidates(config, { language: 'fr', api_image_url: base, series_id: 'swsh', set_tcgdex_id: 'swsh3', local_id: '136' })
      expect(candidates, base).toEqual([{ url: `${ASSETS}/fr/swsh/swsh3/136/high.webp`, quality: 'high', source: 'cdn_reconstructed' }])
      for (const candidate of candidates) {
        const url = new URL(candidate.url)
        expect(url.search).toBe('')
        expect(url.hash).toBe('')
        expect(url.pathname).toMatch(/\/(high|low)\.webp$/)
      }
    }
  })

  it('imageCandidates : une URL officielle d’une autre langue est ignorée au profit de la reconstruction', () => {
    const config = parseConfig({})
    const fr = { language: 'fr' as const, api_image_url: `${ASSETS}/ja/SV/SV1a/001`, series_id: 'swsh', set_tcgdex_id: 'swsh3', local_id: '136' }
    expect(imageCandidates(config, fr)).toEqual([{ url: `${ASSETS}/fr/swsh/swsh3/136/high.webp`, quality: 'high', source: 'cdn_reconstructed' }])
    const ja = { language: 'ja' as const, api_image_url: `${ASSETS}/fr/swsh/swsh3/136`, series_id: 'SV', set_tcgdex_id: 'SV1a', local_id: '001' }
    expect(imageCandidates(config, ja)).toEqual([{ url: `${ASSETS}/ja/SV/SV1a/001/high.webp`, quality: 'high', source: 'cdn_reconstructed' }])
    // Sans reconstruction possible : aucune URL, jamais l'image de l'autre langue.
    expect(imageCandidates(config, { ...fr, series_id: null })).toEqual([])
    expect(imageCandidates(config, { ...ja, local_id: null })).toEqual([])
    // Dans la bonne langue, l'URL officielle reste prioritaire.
    expect(imageCandidates(config, { ...ja, api_image_url: `${ASSETS}/ja/SV/SV1a/001` }).map((candidate) => candidate.source)).toEqual(['api', 'api'])
  })

  it('reconstructedBase : localId « . » ou « .. » → null, jamais une URL hors du dossier de l’extension', () => {
    const config = parseConfig({})
    const results = ['.', '..'].map((localId) => ({
      base: reconstructedBase('fr', 'swsh', 'swsh3', localId),
      candidates: imageCandidates(config, { language: 'fr', api_image_url: null, series_id: 'swsh', set_tcgdex_id: 'swsh3', local_id: localId }),
    }))
    expect(results).toEqual([
      { base: null, candidates: [] },
      { base: null, candidates: [] },
    ])
  })

  it('reconstructedBase encode chaque segment, null si une information manque', () => {
    expect(reconstructedBase('fr', 'swsh', 'swsh3', '136')).toBe(`${ASSETS}/fr/swsh/swsh3/136`)
    expect(reconstructedBase('fr', 'ex', 'exu', '%3F')).toBe(`${ASSETS}/fr/ex/exu/%253F`)
    expect(reconstructedBase('ja', 'SM', 'SM1+', '001')).toBe(`${ASSETS}/ja/SM/SM1%2B/001`)
    expect(reconstructedBase('ja', 'SV', 'SV1a', 'ア/1')).toBe(`${ASSETS}/ja/SV/SV1a/%E3%82%A2%2F1`)
    expect(reconstructedBase('fr', null, 'swsh3', '136')).toBeNull()
    expect(reconstructedBase('fr', 'swsh', null, '136')).toBeNull()
    expect(reconstructedBase('fr', 'swsh', 'swsh3', null)).toBeNull()
    expect(reconstructedBase('fr', '', 'swsh3', '136')).toBeNull()
  })

  it('seriesFromAssetUrl lit la série dans l’URL du logo, seulement si l’extension correspond', () => {
    expect(seriesFromAssetUrl(`${ASSETS}/fr/swsh/swsh3/logo`, 'swsh3')).toBe('swsh')
    expect(seriesFromAssetUrl(`${ASSETS}/univ/swsh/swsh3/symbol`, 'swsh3')).toBe('swsh')
    expect(seriesFromAssetUrl(`${ASSETS}/ja/SM/SM1%2B/symbol`, 'SM1+')).toBe('SM')
    expect(seriesFromAssetUrl(`${ASSETS}/fr/swsh/swsh3/136`, 'swsh3')).toBe('swsh')
    expect(seriesFromAssetUrl(`${ASSETS}/fr/swsh/swsh4/logo`, 'swsh3')).toBeNull()
    expect(seriesFromAssetUrl('https://evil.example/fr/swsh/swsh3/logo', 'swsh3')).toBeNull()
    expect(seriesFromAssetUrl(`${ASSETS}/fr/swsh`, 'swsh3')).toBeNull()
    expect(seriesFromAssetUrl(`${ASSETS}/fr//swsh3/logo`, 'swsh3')).toBeNull()
    expect(seriesFromAssetUrl(`${ASSETS}/fr/%E0%A4%A/swsh3/logo`, 'swsh3')).toBeNull()
    expect(seriesFromAssetUrl(null, 'swsh3')).toBeNull()
    expect(seriesFromAssetUrl('pas une url', 'swsh3')).toBeNull()
  })

  it('imageCandidates : URL officielle en qualité principale puis de secours', () => {
    const config = parseConfig({})
    const card = { language: 'fr' as const, api_image_url: `${ASSETS}/fr/swsh/swsh3/136`, series_id: 'swsh', set_tcgdex_id: 'swsh3', local_id: '136' }
    expect(imageCandidates(config, card)).toEqual([
      { url: `${ASSETS}/fr/swsh/swsh3/136/high.webp`, quality: 'high', source: 'api' },
      { url: `${ASSETS}/fr/swsh/swsh3/136/low.webp`, quality: 'low', source: 'api' },
    ])
    expect(imageCandidates(config, { ...card, api_image_url: `${ASSETS}/fr/swsh/swsh3/136/` })[0]?.url).toBe(`${ASSETS}/fr/swsh/swsh3/136/high.webp`)
    expect(imageCandidates(parseConfig({ FALLBACK_QUALITY: '' }), card)).toEqual([{ url: `${ASSETS}/fr/swsh/swsh3/136/high.webp`, quality: 'high', source: 'api' }])
  })

  it('imageCandidates : sans URL officielle fiable, une seule URL reconstruite en qualité principale', () => {
    const config = parseConfig({})
    const card = { language: 'fr' as const, api_image_url: null, series_id: 'ex', set_tcgdex_id: 'exu', local_id: '%3F' }
    expect(imageCandidates(config, card)).toEqual([{ url: `${ASSETS}/fr/ex/exu/%253F/high.webp`, quality: 'high', source: 'cdn_reconstructed' }])
    // Une URL officielle non fiable (http, autre hôte) est ignorée au profit de la reconstruction.
    expect(imageCandidates(config, { ...card, api_image_url: 'http://assets.tcgdex.net/fr/ex/exu/x' })).toEqual([
      { url: `${ASSETS}/fr/ex/exu/%253F/high.webp`, quality: 'high', source: 'cdn_reconstructed' },
    ])
    expect(imageCandidates(parseConfig({ MAX_RECONSTRUCTION_URLS: '0' }), card)).toEqual([])
    expect(imageCandidates(parseConfig({ MAX_RECONSTRUCTION_URLS: '2' }), card).map((c) => c.quality)).toEqual(['high', 'low'])
  })

  it('imageCandidates : aucune URL si la série (ou l’extension, ou le numéro) manque', () => {
    const config = parseConfig({})
    const card = { language: 'ja' as const, api_image_url: null, series_id: 'SV', set_tcgdex_id: 'SV1a', local_id: '002' }
    expect(imageCandidates(config, { ...card, series_id: null })).toEqual([])
    expect(imageCandidates(config, { ...card, set_tcgdex_id: null })).toEqual([])
    expect(imageCandidates(config, { ...card, local_id: null })).toEqual([])
    expect(imageCandidates(config, { ...card, series_id: null, api_image_url: 'https://evil.example/x' })).toEqual([])
  })
})

describe('checkWebp', () => {
  it('fichier valide → SHA-256 et MD5 du contenu', async () => {
    const bytes = webp(256, 7)
    expect(await checkWebp(bytes, await cdnHeaders(bytes), LIMITS)).toEqual({ ok: true, sha256: await sha256Bytes(bytes), md5: await md5Hex(bytes) })
    // Sans Content-Length ni ETag : rien à comparer, le fichier reste valide.
    expect((await checkWebp(bytes, new Headers({ 'content-type': 'IMAGE/WEBP' }), LIMITS)).ok).toBe(true)
  })

  it('type MIME text/html ou absent → invalide, non passager', async () => {
    const bytes = webp(256)
    const html = await checkWebp(bytes, await cdnHeaders(bytes, { 'content-type': 'text/html' }), LIMITS)
    expect(html).toMatchObject({ ok: false, transient: false })
    const absent = await checkWebp(bytes, new Headers({ 'content-length': '256' }), LIMITS)
    expect(absent).toMatchObject({ ok: false, transient: false })
  })

  it('signature RIFF…WEBP absente → invalide, non passager', async () => {
    const zeros = new Uint8Array(256)
    expect(await checkWebp(zeros, await cdnHeaders(zeros), LIMITS)).toMatchObject({ ok: false, transient: false, reason: expect.stringContaining('signature') })
    const html = new TextEncoder().encode(`<html><body>${'x'.repeat(200)}</body></html>`)
    expect(await checkWebp(html, await cdnHeaders(html), LIMITS)).toMatchObject({ ok: false, transient: false })
    const wave = webp(256)
    wave.set(new TextEncoder().encode('WAVE'), 8)
    expect(await checkWebp(wave, await cdnHeaders(wave), LIMITS)).toMatchObject({ ok: false, transient: false })
  })

  it('trop petit ou trop gros → invalide, non passager', async () => {
    const small = webp(50)
    expect(await checkWebp(small, await cdnHeaders(small), LIMITS)).toMatchObject({ ok: false, transient: false, reason: expect.stringContaining('petit') })
    const big = webp(2_000)
    expect(await checkWebp(big, await cdnHeaders(big), { minImageBytes: 100, maxImageBytes: 1_000 })).toMatchObject({ ok: false, transient: false, reason: expect.stringContaining('volumineux') })
  })

  it('fichier vide sans preuve d’intégrité → passager', async () => {
    expect(await checkWebp(new Uint8Array(), new Headers({ 'content-type': 'image/webp' }), LIMITS)).toMatchObject({ ok: false, transient: true })
    const empty = new Uint8Array()
    const md5 = await md5Hex(empty)
    // ETag = MD5 du vide, mais longueur inconnue (absente, ou avec un encodage) : pas de preuve.
    expect(await checkWebp(empty, new Headers({ 'content-type': 'image/webp', etag: `"${md5}"` }), LIMITS)).toMatchObject({ ok: false, transient: true, reason: expect.stringContaining('vide') })
    expect(await checkWebp(empty, new Headers({ 'content-type': 'image/webp', etag: `"${md5}"`, 'content-length': '0', 'content-encoding': 'gzip' }), LIMITS)).toMatchObject({ ok: false, transient: true })
    // Longueur connue mais ETag qui n'est pas un MD5 : pas de preuve non plus.
    expect(await checkWebp(empty, new Headers({ 'content-type': 'image/webp', etag: '"5f2b1a-0"', 'content-length': '0' }), LIMITS)).toMatchObject({ ok: false, transient: true })
  })

  it('fichier vide dont l’ETag (MD5) et la longueur annoncée correspondent → défaut du fichier, non passager', async () => {
    const empty = new Uint8Array()
    const md5 = await md5Hex(empty)
    expect(md5).toBe('d41d8cd98f00b204e9800998ecf8427e')
    expect(await checkWebp(empty, await cdnHeaders(empty), LIMITS)).toEqual({ ok: false, transient: false, reason: 'contenu vide' })
    expect(await checkWebp(empty, await cdnHeaders(empty, { etag: `W/"${md5.toUpperCase()}"`, 'content-encoding': 'identity' }), LIMITS)).toMatchObject({ ok: false, transient: false })
  })

  it('fichier vide dont l’ETag ne correspond pas → passager, motif ETag (contrôlé avant le contenu vide)', async () => {
    const empty = new Uint8Array()
    expect(await checkWebp(empty, await cdnHeaders(empty, { etag: '"0123456789abcdef0123456789abcdef"' }), LIMITS)).toMatchObject({ ok: false, transient: true, reason: expect.stringContaining('ETag') })
  })

  it('RIFF tronqué sans preuve d’intégrité → passager', async () => {
    const truncated = webp(300).slice(0, 200)
    const md5 = await md5Hex(truncated)
    const expected = { ok: false, transient: true, reason: expect.stringContaining('tronqué') }
    // Ni ETag ni longueur.
    expect(await checkWebp(truncated, new Headers({ 'content-type': 'image/webp' }), LIMITS)).toMatchObject(expected)
    // Longueur seule.
    expect(await checkWebp(truncated, new Headers({ 'content-type': 'image/webp', 'content-length': '200' }), LIMITS)).toMatchObject(expected)
    // ETag seul.
    expect(await checkWebp(truncated, new Headers({ 'content-type': 'image/webp', etag: `"${md5}"` }), LIMITS)).toMatchObject(expected)
    // ETag et longueur, mais avec un encodage : la longueur annoncée ne prouve rien.
    expect(await checkWebp(truncated, await cdnHeaders(truncated, { 'content-length': '90', 'content-encoding': 'gzip' }), LIMITS)).toMatchObject(expected)
    // ETag qui n'est pas un MD5.
    expect(await checkWebp(truncated, await cdnHeaders(truncated, { etag: '"5f2b1a-c8"' }), LIMITS)).toMatchObject(expected)
  })

  it('RIFF tronqué dont l’ETag (MD5) et la longueur annoncée correspondent → défaut du fichier, non passager', async () => {
    const truncated = webp(300).slice(0, 200)
    expect(await checkWebp(truncated, await cdnHeaders(truncated), LIMITS)).toMatchObject({ ok: false, transient: false, reason: expect.stringContaining('tronqué') })
    const md5 = await md5Hex(truncated)
    expect(await checkWebp(truncated, await cdnHeaders(truncated, { etag: `W/"${md5}"` }), LIMITS)).toMatchObject({ ok: false, transient: false })
    expect(await checkWebp(truncated, await cdnHeaders(truncated, { 'content-encoding': 'identity' }), LIMITS)).toMatchObject({ ok: false, transient: false })
  })

  it('RIFF tronqué dont l’ETag ne correspond pas → passager, motif ETag (contrôlé avant la troncature)', async () => {
    const truncated = webp(300).slice(0, 200)
    expect(await checkWebp(truncated, await cdnHeaders(truncated, { etag: '"0123456789abcdef0123456789abcdef"' }), LIMITS)).toMatchObject({ ok: false, transient: true, reason: expect.stringContaining('ETag') })
  })

  it('Content-Length ≠ octets reçus → passager, sauf avec Content-Encoding (gzip…)', async () => {
    const bytes = webp(256)
    expect(await checkWebp(bytes, await cdnHeaders(bytes, { 'content-length': '300' }), LIMITS)).toMatchObject({ ok: false, transient: true })
    expect(await checkWebp(bytes, await cdnHeaders(bytes, { 'content-length': '300', 'content-encoding': 'identity' }), LIMITS)).toMatchObject({ ok: false, transient: true })
    expect((await checkWebp(bytes, await cdnHeaders(bytes, { 'content-length': '120', 'content-encoding': 'gzip' }), LIMITS)).ok).toBe(true)
    expect((await checkWebp(bytes, await cdnHeaders(bytes, { 'content-length': '120', 'content-encoding': 'BR' }), LIMITS)).ok).toBe(true)
  })

  it('MD5 ≠ ETag → passager ; ETag faible ou en majuscules accepté s’il correspond', async () => {
    const bytes = webp(256, 3)
    const md5 = await md5Hex(bytes)
    expect(await checkWebp(bytes, await cdnHeaders(bytes, { etag: '"0123456789abcdef0123456789abcdef"' }), LIMITS)).toMatchObject({ ok: false, transient: true, reason: expect.stringContaining('ETag') })
    expect((await checkWebp(bytes, await cdnHeaders(bytes, { etag: `W/"${md5}"` }), LIMITS)).ok).toBe(true)
    expect((await checkWebp(bytes, await cdnHeaders(bytes, { etag: `"${md5.toUpperCase()}"` }), LIMITS)).ok).toBe(true)
  })

  it('un ETag qui n’est pas un MD5 est ignoré', async () => {
    const bytes = webp(256, 5)
    for (const etag of ['"5f2b1a-3c"', '"abc"', `"${'a'.repeat(64)}"`, '"multipart-0123456789abcdef0123456789abcdef-2"']) {
      expect((await checkWebp(bytes, await cdnHeaders(bytes, { etag }), LIMITS)).ok).toBe(true)
    }
  })
})

// ——— src/tcgdex/types.ts ———

describe('parseurs des réponses TCGdex', () => {
  it('stripPricing retire récursivement « pricing » et garde « thirdParty », sans modifier l’entrée', () => {
    const input = {
      id: 'swsh3-136',
      pricing: { cardmarket: { avg: 1 } },
      variants_detailed: [{ type: 'Normal', thirdParty: { cardmarket: 123 }, pricing: { tcgplayer: 2 } }],
      nested: { deeper: [[{ pricing: 1, keep: true }]] },
      count: 3,
      empty: null,
    }
    const copy = structuredClone(input)
    expect(stripPricing(input)).toEqual({
      id: 'swsh3-136',
      variants_detailed: [{ type: 'Normal', thirdParty: { cardmarket: 123 } }],
      nested: { deeper: [[{ keep: true }]] },
      count: 3,
      empty: null,
    })
    expect(input).toEqual(copy)
    expect(stripPricing('texte')).toBe('texte')
    expect(stripPricing(null)).toBeNull()
    expect(stripPricing([{ pricing: 1 }])).toEqual([{}])
  })

  it('parseSeriesList ignore les éléments invalides (id vide, « / », non objet)', () => {
    const parsed = parseSeriesList([
      { id: 'swsh', name: 'Épée et Bouclier', logo: `${ASSETS}/fr/swsh/swsh1/logo` },
      { id: '', name: 'vide' },
      { id: '   ', name: 'blanc' },
      { id: 'a/b', name: 'barre oblique' },
      { id: 'a\\b' },
      { id: 'a\nb' },
      { id: 'x'.repeat(201) },
      { name: 'sans id' },
      { id: 42 },
      null,
      'swsh',
      [],
      { id: 'ex' },
    ])
    expect(parsed).toEqual([
      { id: 'swsh', name: 'Épée et Bouclier', logo: `${ASSETS}/fr/swsh/swsh1/logo` },
      { id: 'ex', name: 'ex', logo: null },
    ])
    expect(() => parseSeriesList({ id: 'swsh' })).toThrow(TypeError)
  })

  it('parseSetList ignore les éléments invalides et lit cardCount', () => {
    const parsed = parseSetList([
      { id: 'swsh3', name: 'Ténèbres Embrasées', logo: 'L', symbol: 'S', cardCount: { total: 201, official: '189' } },
      { id: '', name: 'vide' },
      { id: 'sv/1', name: 'barre oblique' },
      { id: 'SM1+' },
    ])
    expect(parsed).toEqual([
      { id: 'swsh3', name: 'Ténèbres Embrasées', logo: 'L', symbol: 'S', cardCount: { total: 201, official: null } },
      { id: 'SM1+', name: 'SM1+', logo: null, symbol: null, cardCount: { total: null, official: null } },
    ])
    expect(() => parseSetList(null)).toThrow(TypeError)
  })

  it('parseCardList ignore les cartes sans id valide ou sans localId', () => {
    const parsed = parseCardList([
      { id: 'swsh3-136', localId: '136', name: 'Fouinar', image: `${ASSETS}/fr/swsh/swsh3/136` },
      { id: 'swsh3-137', name: 'sans localId' },
      { id: '', localId: '1' },
      { id: 'a/b', localId: '1' },
      { id: 'exu-%3F', localId: '%3F' },
    ])
    expect(parsed).toEqual([
      { id: 'swsh3-136', localId: '136', name: 'Fouinar', image: `${ASSETS}/fr/swsh/swsh3/136` },
      { id: 'exu-%3F', localId: '%3F', name: '', image: null },
    ])
    expect(() => parseCardList('[]')).toThrow(TypeError)
  })

  it('parseSetDetail lit la série, filtre les cartes et retire cards et pricing du raw', () => {
    const detail = parseSetDetail({
      id: 'swsh3',
      name: 'Ténèbres Embrasées',
      logo: `${ASSETS}/fr/swsh/swsh3/logo`,
      releaseDate: '2020-08-14',
      cardCount: { total: 201, official: 189 },
      serie: { id: 'swsh', name: 'Épée et Bouclier' },
      legal: { standard: false, expanded: true },
      pricing: { cardmarket: 1 },
      cards: [{ id: 'swsh3-1', localId: '1', name: 'Bulbizarre' }, { id: '', localId: '2' }, { id: 'swsh3-3' }],
    })
    expect(detail).toMatchObject({
      id: 'swsh3',
      name: 'Ténèbres Embrasées',
      releaseDate: '2020-08-14',
      serie: { id: 'swsh', name: 'Épée et Bouclier' },
      cardCount: { total: 201, official: 189 },
      cards: [{ id: 'swsh3-1', localId: '1', name: 'Bulbizarre', image: null }],
    })
    expect(detail.raw).not.toHaveProperty('cards')
    expect(detail.raw).not.toHaveProperty('pricing')
    expect(detail.raw).toMatchObject({ id: 'swsh3', serie: { id: 'swsh' }, legal: { standard: false, expanded: true } })
  })

  it('parseSetDetail : série sans nom → nom = id ; série invalide ou absente → null', () => {
    expect(parseSetDetail({ id: 'SV1a', serie: { id: 'SV' } }).serie).toEqual({ id: 'SV', name: 'SV' })
    expect(parseSetDetail({ id: 'SV1a', serie: { id: '' } }).serie).toBeNull()
    expect(parseSetDetail({ id: 'SV1a', serie: { id: 'S/V' } }).serie).toBeNull()
    expect(parseSetDetail({ id: 'SV1a', serie: 'SV' }).serie).toBeNull()
    expect(parseSetDetail({ id: 'SV1a' })).toMatchObject({ serie: null, cards: [], releaseDate: null })
  })

  it('parseSetDetail : données non objet ou sans id → TypeError', () => {
    for (const data of [null, 'swsh3', 42, [], { name: 'sans id' }, { id: 'a/b' }]) expect(() => parseSetDetail(data)).toThrow(TypeError)
  })

  it('parseCardDetail : rarity « None » → null, set sans name → name = id, prix retirés', () => {
    const detail = parseCardDetail({
      id: 'SV1a-001',
      localId: '001',
      name: 'トロピウス',
      image: `${ASSETS}/ja/SV/SV1a/001`,
      category: 'Pokemon',
      rarity: 'None',
      illustrator: 'Illustrateur',
      set: { id: 'SV1a', cardCount: { total: 73, official: 73 } },
      variants_detailed: [{ type: 'Normal', thirdParty: { cardmarket: 123 }, pricing: { cardmarket: { avg: 1 } } }],
      pricing: { cardmarket: { avg: 0.5 } },
    })
    expect(detail).toMatchObject({
      id: 'SV1a-001',
      localId: '001',
      name: 'トロピウス',
      image: `${ASSETS}/ja/SV/SV1a/001`,
      category: 'Pokemon',
      rarity: null,
      illustrator: 'Illustrateur',
      set: { id: 'SV1a', name: 'SV1a', logo: null, symbol: null, cardCount: { total: 73, official: 73 } },
    })
    expect(JSON.stringify(detail.raw)).not.toContain('pricing')
    expect(JSON.stringify(detail.raw)).toContain('"thirdParty":{"cardmarket":123}')
  })

  it('parseCardDetail : rareté réelle conservée, vide ou absente → null, set invalide → null', () => {
    expect(parseCardDetail({ id: 'swsh3-136', localId: '136', rarity: 'Rare Holo' }).rarity).toBe('Rare Holo')
    expect(parseCardDetail({ id: 'swsh3-136', localId: '136', rarity: '' }).rarity).toBeNull()
    expect(parseCardDetail({ id: 'swsh3-136', localId: '136' })).toMatchObject({ rarity: null, set: null, category: null, name: '' })
    expect(parseCardDetail({ id: 'swsh3-136', localId: '136', set: { id: '' } }).set).toBeNull()
  })

  it('parseCardDetail : données non objet, sans id ou sans localId → TypeError', () => {
    for (const data of [null, undefined, 'texte', 42, [], { id: 'swsh3-136' }, { localId: '136' }, { id: 'a/b', localId: '1' }]) {
      expect(() => parseCardDetail(data)).toThrow(TypeError)
    }
  })

  it('les ids « . » et « .. » sont refusés par tous les parseurs (« ... », « .5 » ou « sv3.5 » restent valides)', () => {
    expect(parseSeriesList([{ id: '.' }, { id: '..' }, { id: '...' }, { id: 'swsh' }]).map((series) => series.id)).toEqual(['...', 'swsh'])
    expect(parseSetList([{ id: '.' }, { id: '..' }, { id: 'sv3.5' }, { id: '.5' }]).map((set) => set.id)).toEqual(['sv3.5', '.5'])
    expect(parseCardList([{ id: '.', localId: '1' }, { id: '..', localId: '2' }, { id: '...', localId: '3' }, { id: 'sv3.5-1', localId: '1' }]).map((card) => card.id)).toEqual([
      '...',
      'sv3.5-1',
    ])
    for (const id of ['.', '..']) {
      expect(() => parseSetDetail({ id, name: 'point' }), id).toThrow(TypeError)
      expect(() => parseCardDetail({ id, localId: '1' }), id).toThrow(TypeError)
      expect(parseSetDetail({ id: 'swsh3', serie: { id } }).serie, id).toBeNull()
      expect(parseSetDetail({ id: 'swsh3', cards: [{ id, localId: '1' }, { id: 'swsh3-2', localId: '2' }] }).cards.map((card) => card.id), id).toEqual(['swsh3-2'])
      expect(parseCardDetail({ id: 'swsh3-1', localId: '1', set: { id } }).set, id).toBeNull()
    }
    expect(parseSetDetail({ id: '...' }).id).toBe('...')
    expect(parseCardDetail({ id: '...', localId: '1' }).id).toBe('...')
  })
})

// ——— src/repositories/jobs.ts ———

describe('retryDelayMs', () => {
  const BASE = 60_000
  const MAX = 21_600_000

  it('croît exponentiellement (base × 2^(tentative − 1))', () => {
    const delays = [1, 2, 3, 4, 5].map((attempts) => retryDelayMs(attempts, BASE, MAX, () => 0.5))
    expect(delays).toEqual([60_000, 120_000, 240_000, 480_000, 960_000])
    expect(retryDelayMs(0, BASE, MAX, () => 0.5)).toBe(60_000)
  })

  it('est plafonné à retryMaxMs (sans Infinity ni NaN pour un grand nombre de tentatives)', () => {
    expect(retryDelayMs(30, BASE, MAX, () => 0.5)).toBe(MAX)
    expect(retryDelayMs(5_000, BASE, MAX, () => 0.5)).toBe(MAX)
    expect(retryDelayMs(3, BASE, 100_000, () => 0.5)).toBe(100_000)
  })

  it('l’aléa reste dans ±20 % autour du délai (plafonné)', () => {
    expect(retryDelayMs(3, BASE, MAX, () => 0)).toBe(192_000)
    expect(retryDelayMs(3, BASE, MAX, () => 0.999_999)).toBeLessThanOrEqual(288_000)
    expect(retryDelayMs(3, BASE, MAX, () => 0.999_999)).toBeGreaterThan(287_000)
    for (let i = 0; i < 500; i++) {
      const attempts = 1 + (i % 12)
      const expected = Math.min(MAX, BASE * 2 ** (attempts - 1))
      const delay = retryDelayMs(attempts, BASE, MAX)
      expect(delay).toBeGreaterThanOrEqual(Math.floor(expected * 0.8))
      expect(delay).toBeLessThanOrEqual(Math.ceil(expected * 1.2))
    }
  })
})
