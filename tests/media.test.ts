/**
 * Copies secondaires pour l'affichage du site (src/services/media.ts) : petite image `low.webp` de
 * chaque carte à image valide (clé `….low.webp`), logo (`_logo.webp`) et symbole (`_symbol.webp`)
 * de chaque extension ; création des tâches par la tâche `set` et par
 * `POST /admin/sync/backfill-media`, priorités, idempotence, plafonds de coût, statistiques et audit.
 *
 * Aucun appel réseau : TCGdex est simulé (FakeTcgdex), l'horloge aussi (Clock).
 */

import { env } from 'cloudflare:workers'
import { beforeEach, describe, expect, it } from 'vitest'
import { handleAdmin } from '../src/admin/router'
import type { Language } from '../src/config'
import { Db } from '../src/db'
import { claimJobs, enqueueMediaStmts, enqueueStmts, JOB_PRIORITY, MEDIA_PRIORITY, mediaKind } from '../src/repositories/jobs'
import { storedBytes } from '../src/repositories/runs'
import { imageJob, logoJob, lowImageJob, symbolJob } from '../src/services/catalog'
import { runTick, type RunnerDeps, type TickResult } from '../src/services/runner'
import type { Stats } from '../src/services/stats'
import { startSync } from '../src/services/sync'
import { checkWebp, isLowQualitySource, logoImageUrl, lowImageUrl, symbolImageUrl } from '../src/tcgdex/images'
import { cardImageKey, encodeKeySegment, lowImageKey, setLogoKey, setSymbolKey } from '../src/utils/keys'
import { iso } from '../src/utils/time'
import { ASSETS, Clock, drain, FakeTcgdex, makeDeps, md5Hex, one, resetAll, rows, sampleCatalog, sha256Hex, webp } from './helpers'

beforeEach(resetAll)

const DAY = 86_400_000
const MIN = 60_000

// ——— Données de test ———

const BASE_136 = `${ASSETS}/fr/swsh/swsh3/136`
const HIGH_136 = `${BASE_136}/high.webp`
const LOW_136 = `${BASE_136}/low.webp`
const KEY_136 = 'cards/fr/swsh3/swsh3-136.webp'
const LOW_KEY_136 = 'cards/fr/swsh3/swsh3-136.low.webp'
const LOGO = `${ASSETS}/fr/swsh/swsh3/logo`
const LOGO_KEY = 'cards/fr/swsh3/_logo.webp'
// Les symboles sont souvent sous `univ` (ex. réel : https://assets.tcgdex.net/univ/sv/sv03.5/symbol).
const SYMBOL = `${ASSETS}/univ/swsh/swsh3/symbol`
const SYMBOL_KEY = 'cards/fr/swsh3/_symbol.webp'
const SYMBOL_JA = `${ASSETS}/ja/SV/SV1a/symbol`
const SYMBOL_KEY_JA = 'cards/ja/SV1a/_symbol.webp'

const db = () => new Db(env.DB)
const tick = (deps: RunnerDeps): Promise<TickResult> => runTick(deps, { trigger: 'test' })

interface CardSeed {
  language?: Language
  set?: string
  tcgdexId?: string
  /** Statut de l'image principale (valide par défaut). */
  mainStatus?: string
  mainKey?: string | null
  sourceUrl?: string | null
  quality?: string | null
  size?: number | null
  sha256?: string | null
  lowStatus?: string | null
  lowKey?: string | null
  lowSize?: number | null
  lowCheckedAt?: string | null
  removedAt?: string | null
}

/** Insère l'extension (si besoin) et une carte, par défaut fr:swsh3-136 dont l'image principale est copiée. */
async function seedCard(clock: Clock, seed: CardSeed = {}): Promise<{ cardId: string; tcgdexId: string; language: Language }> {
  const language = seed.language ?? 'fr'
  const set = seed.set ?? 'swsh3'
  const tcgdexId = seed.tcgdexId ?? 'swsh3-136'
  const cardId = `${language}:${tcgdexId}`
  const now = iso(clock.now())
  await env.DB.batch([
    env.DB.prepare(`INSERT OR IGNORE INTO sets (id, tcgdex_id, language, name, created_at, updated_at) VALUES (?1, ?2, ?3, ?4, ?5, ?5)`).bind(
      `${language}:${set}`,
      set,
      language,
      `Extension ${set}`,
      now,
    ),
    env.DB.prepare(
      `INSERT INTO cards (id, tcgdex_id, language, name, local_id, set_id, image_status, image_r2_key, image_source_url, image_source,
         image_quality, image_file_size, image_sha256, image_low_status, image_low_r2_key, image_low_file_size, image_low_checked_at,
         removed_at, created_at, updated_at)
       VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, 'api', ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?18)`,
    ).bind(
      cardId,
      tcgdexId,
      language,
      `Carte ${tcgdexId}`,
      tcgdexId.split('-').at(-1) ?? '1',
      `${language}:${set}`,
      seed.mainStatus ?? 'downloaded',
      seed.mainKey === undefined ? KEY_136 : seed.mainKey,
      seed.sourceUrl === undefined ? HIGH_136 : seed.sourceUrl,
      seed.quality === undefined ? 'high' : seed.quality,
      seed.size === undefined ? 300 : seed.size,
      seed.sha256 === undefined ? 'a'.repeat(64) : seed.sha256,
      seed.lowStatus ?? null,
      seed.lowKey ?? null,
      seed.lowSize ?? null,
      seed.lowCheckedAt ?? null,
      seed.removedAt ?? null,
      now,
    ),
  ])
  return { cardId, tcgdexId, language }
}

/** Carte fr de swsh3 dont l'image principale n'est pas encore copiée (seule son URL officielle est connue). */
async function seedPendingMain(clock: Clock, tcgdexId = 'swsh3-136'): Promise<{ cardId: string }> {
  const { cardId } = await seedCard(clock, { tcgdexId, mainStatus: 'pending', mainKey: null, sourceUrl: null, quality: null, size: null, sha256: null })
  await env.DB.prepare(`UPDATE cards SET api_image_url = ?1 WHERE id = ?2`).bind(`${ASSETS}/fr/swsh/swsh3/${tcgdexId.split('-').at(-1)}`, cardId).run()
  return { cardId }
}

interface SetSeed {
  logoStatus?: string
  logoKey?: string
  logoSize?: number
  logoCheckedAt?: string
  logoSourceUrl?: string
  symbolUrl?: string
  symbolStatus?: string
  symbolKey?: string
  symbolSize?: number
  symbolCheckedAt?: string
  symbolSourceUrl?: string
}

/** Insère une extension, avec un logo et un symbole éventuels. */
async function seedSet(clock: Clock, language: Language, set: string, logoUrl: string | null, extra: SetSeed = {}): Promise<void> {
  await env.DB.prepare(
    `INSERT INTO sets (id, tcgdex_id, language, name, logo_url, logo_status, logo_r2_key, logo_file_size, logo_checked_at, logo_source_url,
       symbol_url, symbol_status, symbol_r2_key, symbol_file_size, symbol_checked_at, symbol_source_url, created_at, updated_at)
     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?17)`,
  )
    .bind(
      `${language}:${set}`,
      set,
      language,
      `Extension ${set}`,
      logoUrl,
      extra.logoStatus ?? null,
      extra.logoKey ?? null,
      extra.logoSize ?? null,
      extra.logoCheckedAt ?? null,
      extra.logoSourceUrl ?? null,
      extra.symbolUrl ?? null,
      extra.symbolStatus ?? null,
      extra.symbolKey ?? null,
      extra.symbolSize ?? null,
      extra.symbolCheckedAt ?? null,
      extra.symbolSourceUrl ?? null,
      iso(clock.now()),
    )
    .run()
}

async function enqueueLow(clock: Clock, language: Language, tcgdexId: string, maxAttempts = 5): Promise<void> {
  const database = db()
  await database.batch(enqueueMediaStmts(database, language, [lowImageJob(language, tcgdexId)], { runId: null, maxAttempts, nowMs: clock.now() }))
}

async function enqueueLogo(clock: Clock, language: Language, set: string, maxAttempts = 5): Promise<void> {
  const database = db()
  await database.batch(enqueueMediaStmts(database, language, [logoJob(language, set)], { runId: null, maxAttempts, nowMs: clock.now() }))
}

async function enqueueSymbol(clock: Clock, language: Language, set: string, maxAttempts = 5): Promise<void> {
  const database = db()
  await database.batch(enqueueMediaStmts(database, language, [symbolJob(language, set)], { runId: null, maxAttempts, nowMs: clock.now() }))
}

/** Symbole servi comme par le vrai CDN : 200, `application/octet-stream`, ETag = MD5 (sondé le 14/11/2026). */
function addSymbol(fake: FakeTcgdex, url: string, body: Uint8Array): void {
  fake.addImage(url, body, { headers: { 'content-type': 'application/octet-stream' } })
}

interface LowRow {
  image_low_status: string | null
  image_low_r2_key: string | null
  image_low_file_size: number | null
  image_low_sha256: string | null
  image_low_checked_at: string | null
  image_low_failure_reason: string | null
}

const lowRow = (cardId: string) =>
  one<LowRow>(
    `SELECT image_low_status, image_low_r2_key, image_low_file_size, image_low_sha256, image_low_checked_at, image_low_failure_reason FROM cards WHERE id = ?1`,
    cardId,
  )

interface LogoRow {
  logo_status: string | null
  logo_r2_key: string | null
  logo_source_url: string | null
  logo_file_size: number | null
  logo_sha256: string | null
  logo_failure_reason: string | null
}

const logoRow = (setId: string) =>
  one<LogoRow>(`SELECT logo_status, logo_r2_key, logo_source_url, logo_file_size, logo_sha256, logo_failure_reason FROM sets WHERE id = ?1`, setId)

interface SymbolRow {
  symbol_status: string | null
  symbol_r2_key: string | null
  symbol_source_url: string | null
  symbol_file_size: number | null
  symbol_sha256: string | null
  symbol_checked_at: string | null
  symbol_failure_reason: string | null
}

const symbolRow = (setId: string) =>
  one<SymbolRow>(
    `SELECT symbol_status, symbol_r2_key, symbol_source_url, symbol_file_size, symbol_sha256, symbol_checked_at, symbol_failure_reason FROM sets WHERE id = ?1`,
    setId,
  )

const jobByKey = (dedupeKey: string) =>
  one<{ job_type: string; status: string; attempts: number; releases: number; priority: number; run_id: number | null; payload_json: string | null; last_error: string | null }>(
    `SELECT job_type, status, attempts, releases, priority, run_id, payload_json, last_error FROM import_jobs WHERE dedupe_key = ?1`,
    dedupeKey,
  )

async function r2Bytes(key: string): Promise<Uint8Array | null> {
  const object = await env.BUCKET.get(key)
  return object ? new Uint8Array(await object.arrayBuffer()) : null
}

async function r2Keys(prefix: string): Promise<string[]> {
  return (await env.BUCKET.list({ prefix })).objects.map((object) => object.key).sort()
}

async function imageResponse(body: Uint8Array, headers: Record<string, string> = {}): Promise<Response> {
  return new Response(body, {
    status: 200,
    headers: { 'content-type': 'image/webp', 'content-length': String(body.byteLength), etag: `"${await md5Hex(body)}"`, ...headers },
  })
}

function admin(deps: RunnerDeps, fake: FakeTcgdex, method: string, path: string): Promise<Response> {
  return handleAdmin(new Request(`https://importateur.test${path}`, { method, headers: { authorization: `Bearer ${env.ADMIN_TOKEN}` } }), {
    config: deps.config,
    d1: env.DB,
    bucket: env.BUCKET,
    fetcher: fake.fetcher,
    adminToken: env.ADMIN_TOKEN,
    now: deps.now ?? Date.now,
  })
}

/** Catalogue de référence avec les petites images des 4 cartes à image valide et le logo de swsh3. */
function catalogWithMedia(): FakeTcgdex {
  const fake = sampleCatalog()
  fake.addImage(LOW_136, webp(120, 21))
  fake.addImage(`${ASSETS}/fr/swsh/swsh3/137/low.webp`, webp(130, 22))
  fake.addImage(`${ASSETS}/ja/SV/SV1a/001/low.webp`, webp(140, 23))
  fake.addImage(`${ASSETS}/ja/SV/SV1a/002/low.webp`, webp(150, 24))
  fake.addImage(`${LOGO}.webp`, webp(500, 25))
  return fake
}

/** Le même catalogue, avec un symbole pour swsh3 (sous `univ`) et pour SV1a (JA), servis comme par le vrai CDN. */
function catalogWithSymbols(): FakeTcgdex {
  const fake = catalogWithMedia()
  const swsh3 = fake.sets.fr.find((set) => set.id === 'swsh3')
  const sv1a = fake.sets.ja.find((set) => set.id === 'SV1a')
  if (!swsh3 || !sv1a) throw new Error('catalogue de référence inattendu')
  swsh3.symbol = SYMBOL
  sv1a.symbol = SYMBOL_JA
  addSymbol(fake, `${SYMBOL}.webp`, webp(352, 41))
  addSymbol(fake, `${SYMBOL_JA}.webp`, webp(360, 42))
  return fake
}

// ——— Clés et URL ———

describe('clés R2 et URL des copies secondaires', () => {
  it('lowImageKey : la clé de l’image principale, « .webp » remplacé par « .low.webp »', () => {
    expect(lowImageKey('cards/fr/swsh3/swsh3-136.webp')).toBe('cards/fr/swsh3/swsh3-136.low.webp')
    expect(lowImageKey(cardImageKey('fr', 'exu', 'exu-%3F'))).toBe('cards/fr/exu/exu-_253F.low.webp')
    expect(lowImageKey(cardImageKey('ja', 'SM1+', 'SM1+-001'))).toBe('cards/ja/SM1_2B/SM1_2B-001.low.webp')
    expect(lowImageKey(cardImageKey('fr', 'sans-extension', 'xy-2'))).toBe('cards/fr/sans-extension/xy-2.low.webp')
    // Jamais hors de cards/, jamais à partir d'autre chose qu'une image WebP.
    for (const key of ['reports/latest/summary.webp', 'images/x.webp', 'cards/fr/swsh3/swsh3-136.png', 'cards/.webp', '']) expect(lowImageKey(key), key).toBeNull()
  })

  it('setLogoKey : « cards/<langue>/<extension encodée>/_logo.webp », sans collision possible avec une carte', () => {
    expect(setLogoKey('fr', 'swsh3')).toBe('cards/fr/swsh3/_logo.webp')
    expect(setLogoKey('ja', 'SM1+')).toBe('cards/ja/SM1_2B/_logo.webp')
    expect(setLogoKey('fr', 'sv03.5')).toBe('cards/fr/sv03.5/_logo.webp')
    // Un segment encodé n'écrit « _ » que suivi de deux chiffres hexadécimaux majuscules : jamais « _logo ».
    for (const id of ['_logo', '_logo.webp', 'logo', '_l', 'swsh3-_logo', '_LOGO']) {
      const encoded = encodeKeySegment(id)
      expect(encoded, id).toMatch(/^(?:[A-Za-z0-9.-]|_[0-9A-F]{2})+$/)
      expect(cardImageKey('fr', 'swsh3', id), id).not.toBe(setLogoKey('fr', 'swsh3'))
      expect(lowImageKey(cardImageKey('fr', 'swsh3', id)), id).not.toBe(setLogoKey('fr', 'swsh3'))
    }
  })

  it('lowImageUrl : même base que l’image principale copiée, qualité low ; seulement une URL d’image fiable de la langue', () => {
    expect(lowImageUrl(HIGH_136, 'fr')).toBe(LOW_136)
    expect(lowImageUrl(LOW_136, 'fr')).toBe(LOW_136)
    expect(lowImageUrl(`${ASSETS}/fr/ex/exu/%253F/high.webp`, 'fr')).toBe(`${ASSETS}/fr/ex/exu/%253F/low.webp`)
    expect(lowImageUrl(`${ASSETS}/ja/SM/SM1%2B/001/high.webp`, 'ja')).toBe(`${ASSETS}/ja/SM/SM1%2B/001/low.webp`)
    for (const [url, language] of [
      [null, 'fr'],
      [HIGH_136, 'ja'],
      ['https://evil.example.com/fr/swsh/swsh3/136/high.webp', 'fr'],
      ['http://assets.tcgdex.net/fr/swsh/swsh3/136/high.webp', 'fr'],
      [`${BASE_136}/high.png`, 'fr'],
      [`${BASE_136}/high.webp?x=1`, 'fr'],
      [`${BASE_136}`, 'fr'],
    ] as const) {
      expect(lowImageUrl(url, language), String(url)).toBeNull()
    }
    expect(isLowQualitySource(LOW_136, 'low')).toBe(true)
    expect(isLowQualitySource(LOW_136, null)).toBe(true)
    expect(isLowQualitySource(HIGH_136, 'high')).toBe(false)
  })

  it('logoImageUrl : « <logo>.webp » sur le CDN, dans la langue de l’extension ou univ ; sinon refusée', () => {
    expect(logoImageUrl(LOGO, 'fr')).toBe(`${LOGO}.webp`)
    expect(logoImageUrl(`${ASSETS}/univ/swsh/swsh3/logo`, 'fr')).toBe(`${ASSETS}/univ/swsh/swsh3/logo.webp`)
    for (const [url, language] of [
      [null, 'fr'],
      [LOGO, 'ja'],
      [`${ASSETS}/en/swsh/swsh3/logo`, 'fr'],
      ['https://evil.example.com/fr/swsh/swsh3/logo', 'fr'],
      ['http://assets.tcgdex.net/fr/swsh/swsh3/logo', 'fr'],
      [`${LOGO}?v=2`, 'fr'],
      [`${LOGO}#x`, 'fr'],
      [`${ASSETS}/fr/../ja/swsh3/logo`, 'fr'],
      [`${ASSETS}/fr/%2e%2e/en/logo`, 'fr'],
      [`${ASSETS}/fr/swsh//logo`, 'fr'],
      [`${ASSETS}/fr/swsh/swsh3/logo/`, 'fr'],
      ['https://assets.tcgdex.net.evil.example/fr/logo', 'fr'],
    ] as const) {
      expect(logoImageUrl(url, language), String(url)).toBeNull()
    }
  })

  it('mediaKind : seules les tâches image avec { media: low | logo | symbol } sont des copies secondaires', () => {
    expect(mediaKind({ job_type: 'image', payload_json: '{"media":"low"}' })).toBe('low')
    expect(mediaKind({ job_type: 'image', payload_json: '{"media":"logo"}' })).toBe('logo')
    expect(mediaKind({ job_type: 'image', payload_json: '{"media":"symbol"}' })).toBe('symbol')
    expect(mediaKind({ job_type: 'image', payload_json: null })).toBeNull()
    expect(mediaKind({ job_type: 'image', payload_json: '{"media":"autre"}' })).toBeNull()
    expect(mediaKind({ job_type: 'image', payload_json: '{"media":"Symbol"}' })).toBeNull()
    expect(mediaKind({ job_type: 'image', payload_json: 'pas du json' })).toBeNull()
    expect(mediaKind({ job_type: 'card', payload_json: '{"media":"low"}' })).toBeNull()
    expect(mediaKind({ job_type: 'set', payload_json: '{"media":"symbol"}' })).toBeNull()
    expect(lowImageJob('fr', 'swsh3-136')).toEqual({ target: 'swsh3-136', cardId: 'fr:swsh3-136', dedupeKey: 'image_low:fr:swsh3-136', payload: { media: 'low' } })
    expect(logoJob('ja', 'SM1+')).toEqual({ target: 'SM1+', cardId: null, dedupeKey: 'logo:ja:SM1+', payload: { media: 'logo' } })
    expect(symbolJob('fr', 'sv03.5')).toEqual({ target: 'sv03.5', cardId: null, dedupeKey: 'symbol:fr:sv03.5', payload: { media: 'symbol' } })
  })

  it('setSymbolKey : « cards/<langue>/<extension encodée>/_symbol.webp », sans collision avec une carte ni avec le logo', () => {
    expect(setSymbolKey('fr', 'swsh3')).toBe(SYMBOL_KEY)
    expect(setSymbolKey('fr', 'sv03.5')).toBe('cards/fr/sv03.5/_symbol.webp')
    expect(setSymbolKey('ja', 'SM1+')).toBe('cards/ja/SM1_2B/_symbol.webp')
    expect(setSymbolKey('fr', 'swsh3')).not.toBe(setLogoKey('fr', 'swsh3'))
    // `s` n'est pas un chiffre hexadécimal : aucun segment encodé ne peut commencer par « _symbol ».
    for (const id of ['_symbol', '_symbol.webp', 'symbol', '_s', '_SYMBOL', 'swsh3-_symbol']) {
      expect(encodeKeySegment(id), id).toMatch(/^(?:[A-Za-z0-9.-]|_[0-9A-F]{2})+$/)
      expect(cardImageKey('fr', 'swsh3', id), id).not.toBe(setSymbolKey('fr', 'swsh3'))
      expect(lowImageKey(cardImageKey('fr', 'swsh3', id)), id).not.toBe(setSymbolKey('fr', 'swsh3'))
    }
  })

  it('symbolImageUrl : « <symbol>.webp » sur le CDN, dans la langue de l’extension ou univ ; sinon refusée', () => {
    expect(symbolImageUrl(`${ASSETS}/univ/sv/sv03.5/symbol`, 'fr')).toBe(`${ASSETS}/univ/sv/sv03.5/symbol.webp`)
    expect(symbolImageUrl(`${ASSETS}/univ/sv/sv03.5/symbol`, 'ja')).toBe(`${ASSETS}/univ/sv/sv03.5/symbol.webp`)
    expect(symbolImageUrl(SYMBOL_JA, 'ja')).toBe(`${SYMBOL_JA}.webp`)
    for (const [url, language] of [
      [null, 'fr'],
      [SYMBOL_JA, 'fr'],
      [`${ASSETS}/en/swsh/swsh3/symbol`, 'fr'],
      ['https://evil.example.com/univ/swsh/swsh3/symbol', 'fr'],
      ['http://assets.tcgdex.net/univ/swsh/swsh3/symbol', 'fr'],
      [`${SYMBOL}?v=2`, 'fr'],
      [`${SYMBOL}#x`, 'fr'],
      [`${ASSETS}/univ/../ja/SV1a/symbol`, 'fr'],
      [`${ASSETS}/univ/%2E%2E/en/symbol`, 'fr'],
      [`${ASSETS}/univ/swsh//symbol`, 'fr'],
      [`${SYMBOL}/`, 'fr'],
      ['https://assets.tcgdex.net.evil.example/univ/symbol', 'fr'],
    ] as const) {
      expect(symbolImageUrl(url, language), String(url)).toBeNull()
    }
  })

  it('checkWebp : « application/octet-stream » accepté seulement sur demande (symboles), signature, tailles et MD5 toujours exigés', async () => {
    const limits = { minImageBytes: 100, maxImageBytes: 8_000_000 }
    const headers = async (body: Uint8Array, contentType: string, etag?: string) =>
      new Headers({ 'content-type': contentType, 'content-length': String(body.byteLength), etag: etag ?? `"${await md5Hex(body)}"` })
    const body = webp(352, 41)
    const octet = await headers(body, 'application/octet-stream')

    // Par défaut (images principales, petites images, logos) : refusé.
    expect(await checkWebp(body, octet, limits)).toEqual({ ok: false, reason: 'type MIME inattendu : application/octet-stream', transient: false })
    expect(await checkWebp(body, octet, limits, { allowOctetStream: false })).toMatchObject({ ok: false })
    // Symboles : accepté, avec les mêmes empreintes que pour image/webp.
    const accepted = await checkWebp(body, octet, limits, { allowOctetStream: true })
    expect(accepted).toEqual({ ok: true, sha256: await sha256Hex(body), md5: await md5Hex(body) })
    expect(await checkWebp(body, await headers(body, 'Application/Octet-Stream; charset=binary'), limits, { allowOctetStream: true })).toMatchObject({ ok: true })
    expect(await checkWebp(body, await headers(body, 'image/webp'), limits, { allowOctetStream: true })).toMatchObject({ ok: true })

    // Aucun autre type n'est toléré, même pour un symbole.
    for (const type of ['text/html', 'application/json', 'application/octet-stream, text/html', 'image/png']) {
      expect(await checkWebp(body, await headers(body, type), limits, { allowOctetStream: true }), type).toMatchObject({ ok: false, transient: false, reason: expect.stringContaining('type MIME inattendu') })
    }
    // Le contenu reste vérifié : signature, taille minimale, MD5 = ETag, longueur annoncée.
    const png = new Uint8Array(300)
    png.set([0x89, 0x50, 0x4e, 0x47])
    expect(await checkWebp(png, await headers(png, 'application/octet-stream'), limits, { allowOctetStream: true })).toMatchObject({ ok: false, transient: false, reason: expect.stringContaining('signature WebP absente') })
    const tiny = webp(60, 1)
    expect(await checkWebp(tiny, await headers(tiny, 'application/octet-stream'), limits, { allowOctetStream: true })).toMatchObject({ ok: false, transient: false, reason: expect.stringContaining('petit') })
    expect(await checkWebp(body, await headers(body, 'application/octet-stream', '"00112233445566778899aabbccddeeff"'), limits, { allowOctetStream: true })).toMatchObject({ ok: false, transient: true, reason: expect.stringContaining('ETag') })
    const truncated = new Headers(octet)
    truncated.set('content-length', '400')
    expect(await checkWebp(body, truncated, limits, { allowOctetStream: true })).toMatchObject({ ok: false, transient: true, reason: expect.stringContaining('annoncée') })
  })
})

// ——— Petite image ———

describe('petite image (low.webp)', () => {
  it('copie : objet R2 « ….low.webp » identique, validé, métadonnées ; carte « downloaded » ; image principale intacte', async () => {
    const fake = new FakeTcgdex()
    const body = webp(120, 21)
    fake.addImage(LOW_136, body)
    const clock = new Clock()
    const deps = makeDeps(fake, clock)
    const { cardId } = await seedCard(clock)
    const mainBefore = await one(`SELECT image_status, image_r2_key, image_source_url, image_quality, image_file_size, image_sha256 FROM cards WHERE id = ?1`, cardId)
    await enqueueLow(clock, 'fr', 'swsh3-136')

    const result = await tick(deps)
    expect(result).toMatchObject({ status: 'ran', claimed: 1, done: 1, httpRequests: 1, bytesStored: 120 })
    expect(fake.requests.map((request) => request.url)).toEqual([LOW_136])

    expect(await r2Bytes(LOW_KEY_136)).toEqual(body)
    const head = await env.BUCKET.head(LOW_KEY_136)
    const sha256 = await sha256Hex(body)
    expect(head?.httpMetadata?.contentType).toBe('image/webp')
    expect(head?.customMetadata).toEqual({ variant: 'low', quality: 'low', language: 'fr', tcgdexId: 'swsh3-136', sourceUrl: LOW_136, sha256, md5: await md5Hex(body) })
    expect(await lowRow(cardId)).toEqual({
      image_low_status: 'downloaded',
      image_low_r2_key: LOW_KEY_136,
      image_low_file_size: 120,
      image_low_sha256: sha256,
      image_low_checked_at: iso(clock.now()),
      image_low_failure_reason: null,
    })
    expect(await one(`SELECT image_status, image_r2_key, image_source_url, image_quality, image_file_size, image_sha256 FROM cards WHERE id = ?1`, cardId)).toEqual(mainBefore)
    expect(await jobByKey('image_low:fr:swsh3-136')).toMatchObject({ job_type: 'image', status: 'done', attempts: 1, priority: MEDIA_PRIORITY })
    // Seule la petite image est sous cards/ (l'objet principal n'est pas touché ni recréé).
    expect(await r2Keys('cards/')).toEqual([LOW_KEY_136])
  })

  it('404 : « not_found_on_cdn » avec l’URL testée, aucune nouvelle tentative, aucun objet', async () => {
    const fake = new FakeTcgdex()
    const clock = new Clock()
    const deps = makeDeps(fake, clock)
    const { cardId } = await seedCard(clock)
    await enqueueLow(clock, 'fr', 'swsh3-136')

    await drain(deps, clock)
    const row = await lowRow(cardId)
    expect(row).toMatchObject({ image_low_status: 'not_found_on_cdn', image_low_r2_key: null, image_low_file_size: null })
    expect(row.image_low_failure_reason).toContain('introuvable sur le CDN (HTTP 404)')
    expect(row.image_low_failure_reason).toContain(LOW_136)
    expect(fake.requests).toHaveLength(1)
    expect(await jobByKey('image_low:fr:swsh3-136')).toMatchObject({ status: 'done', attempts: 1 })
    expect(await env.BUCKET.head(LOW_KEY_136)).toBeNull()
    // L'image principale n'est jamais marquée manquante pour autant.
    expect(await one(`SELECT image_status FROM cards WHERE id = ?1`, cardId)).toEqual({ image_status: 'downloaded' })
  })

  it('fichier invalide (HTML servi en 200) : « invalid_image », définitif, rien n’est écrit dans R2', async () => {
    const fake = new FakeTcgdex()
    fake.queue(LOW_136, () => imageResponse(new TextEncoder().encode(`<html>${'x'.repeat(300)}</html>`), { 'content-type': 'text/html' }))
    const clock = new Clock()
    const deps = makeDeps(fake, clock)
    const { cardId } = await seedCard(clock)
    await enqueueLow(clock, 'fr', 'swsh3-136')

    await drain(deps, clock)
    const row = await lowRow(cardId)
    expect(row.image_low_status).toBe('invalid_image')
    expect(row.image_low_failure_reason).toContain('type MIME inattendu')
    expect(fake.requests).toHaveLength(1)
    expect(await env.BUCKET.head(LOW_KEY_136)).toBeNull()
  })

  it('transfert corrompu (MD5 ≠ ETag) : nouvelle tentative, puis « download_error » après MAX_ATTEMPTS, jamais d’objet', async () => {
    const fake = new FakeTcgdex()
    const corrupt = () => imageResponse(webp(120, 21), { etag: '"00112233445566778899aabbccddeeff"' })
    fake.queue(LOW_136, corrupt, corrupt)
    const clock = new Clock()
    const deps = makeDeps(fake, clock, { MAX_ATTEMPTS: '2' })
    const { cardId } = await seedCard(clock)
    await enqueueLow(clock, 'fr', 'swsh3-136', 2)

    expect(await tick(deps)).toMatchObject({ status: 'ran', retried: 1, done: 0 })
    expect(await jobByKey('image_low:fr:swsh3-136')).toMatchObject({ status: 'pending', attempts: 1 })
    expect((await lowRow(cardId)).image_low_status).toBeNull()

    await drain(deps, clock)
    expect(await jobByKey('image_low:fr:swsh3-136')).toMatchObject({ status: 'failed', attempts: 2 })
    const row = await lowRow(cardId)
    expect(row.image_low_status).toBe('download_error')
    expect(row.image_low_failure_reason).toContain('ETag')
    expect(fake.requests).toHaveLength(2)
    expect(await env.BUCKET.head(LOW_KEY_136)).toBeNull()
  })

  it('HTTP 429 : tâche rendue sans tentative consommée, attente imposée', async () => {
    const fake = new FakeTcgdex()
    fake.addImage(LOW_136, webp(120, 21))
    fake.queue(LOW_136, () => new Response(null, { status: 429, headers: { 'retry-after': '120' } }))
    const clock = new Clock()
    const deps = makeDeps(fake, clock)
    await seedCard(clock)
    await enqueueLow(clock, 'fr', 'swsh3-136')

    expect(await tick(deps)).toMatchObject({ status: 'ran', released: 1, retried: 0, done: 0 })
    expect(await jobByKey('image_low:fr:swsh3-136')).toMatchObject({ status: 'pending', attempts: 0, releases: 1 })
    expect(await one(`SELECT value IS NOT NULL AS present FROM runtime_state WHERE key = 'backoff_until'`)).toEqual({ present: 1 })
    await drain(deps, clock)
    expect((await lowRow('fr:swsh3-136')).image_low_status).toBe('downloaded')
  })

  it('image principale déjà en qualité low : enregistrée comme petite image (même clé), sans téléchargement ni double comptage', async () => {
    const fake = new FakeTcgdex()
    const clock = new Clock()
    const deps = makeDeps(fake, clock)
    const { cardId } = await seedCard(clock, { sourceUrl: LOW_136, quality: 'low', size: 200, sha256: 'b'.repeat(64) })
    await enqueueLow(clock, 'fr', 'swsh3-136')

    expect(await tick(deps)).toMatchObject({ status: 'ran', done: 1, httpRequests: 0, bytesStored: 0 })
    expect(fake.requests).toHaveLength(0)
    expect(await lowRow(cardId)).toMatchObject({ image_low_status: 'downloaded', image_low_r2_key: KEY_136, image_low_file_size: null, image_low_sha256: 'b'.repeat(64) })
    expect(await r2Keys('cards/')).toEqual([])
    expect(await storedBytes(db())).toBe(200)
  })

  it('objet déjà présent dans R2 (copie faite, D1 pas à jour) : réconcilié en « downloaded » sans téléchargement ni écriture', async () => {
    const fake = new FakeTcgdex()
    fake.addImage(LOW_136, webp(999, 9))
    const clock = new Clock()
    const deps = makeDeps(fake, clock)
    const { cardId } = await seedCard(clock)
    const body = webp(120, 21)
    const sha256 = await sha256Hex(body)
    await env.BUCKET.put(LOW_KEY_136, body, { customMetadata: { sha256, tcgdexId: 'swsh3-136', variant: 'low' } })
    const uploaded = (await env.BUCKET.head(LOW_KEY_136))?.uploaded.toISOString()
    await enqueueLow(clock, 'fr', 'swsh3-136')

    expect(await tick(deps)).toMatchObject({ status: 'ran', done: 1, httpRequests: 0, bytesStored: 0 })
    expect(fake.requests).toHaveLength(0)
    expect(await lowRow(cardId)).toMatchObject({ image_low_status: 'downloaded', image_low_r2_key: LOW_KEY_136, image_low_file_size: 120, image_low_sha256: sha256 })
    expect(await r2Bytes(LOW_KEY_136)).toEqual(body)
    expect((await env.BUCKET.head(LOW_KEY_136))?.uploaded.toISOString()).toBe(uploaded)
  })

  it('objet valide d’une autre carte à la même clé : jamais écrasé, échec enregistré', async () => {
    const fake = new FakeTcgdex()
    fake.addImage(LOW_136, webp(120, 21))
    const clock = new Clock()
    const deps = makeDeps(fake, clock)
    const { cardId } = await seedCard(clock)
    const foreign = webp(180, 4)
    await env.BUCKET.put(LOW_KEY_136, foreign, { customMetadata: { sha256: await sha256Hex(foreign), tcgdexId: 'autre-carte' } })
    await enqueueLow(clock, 'fr', 'swsh3-136')

    expect(await tick(deps)).toMatchObject({ status: 'ran', done: 1, httpRequests: 0 })
    const row = await lowRow(cardId)
    expect(row.image_low_status).toBe('download_error')
    expect(row.image_low_failure_reason).toContain('jamais écrasé')
    expect(await r2Bytes(LOW_KEY_136)).toEqual(foreign)
  })

  it('petite image déjà copiée : aucune requête, rien de réécrit (idempotence)', async () => {
    const fake = new FakeTcgdex()
    fake.addImage(LOW_136, webp(120, 21))
    const clock = new Clock()
    const deps = makeDeps(fake, clock)
    const { cardId } = await seedCard(clock, { lowStatus: 'downloaded', lowKey: LOW_KEY_136, lowSize: 120, lowCheckedAt: iso(clock.now()) })
    const before = await one(`SELECT * FROM cards WHERE id = ?1`, cardId)
    await enqueueLow(clock, 'fr', 'swsh3-136')
    clock.advance(MIN)

    expect(await tick(deps)).toMatchObject({ status: 'ran', done: 1, httpRequests: 0 })
    expect(fake.requests).toHaveLength(0)
    expect(await one(`SELECT * FROM cards WHERE id = ?1`, cardId)).toEqual(before)
  })

  it('URL reconstruite (identifiant « % ») : petite image à la même base, clé encodée', async () => {
    const fake = new FakeTcgdex()
    const url = `${ASSETS}/fr/ex/exu/%253F/low.webp`
    fake.addImage(url, webp(110, 3))
    const clock = new Clock()
    const deps = makeDeps(fake, clock)
    const { cardId } = await seedCard(clock, { set: 'exu', tcgdexId: 'exu-%3F', mainKey: 'cards/fr/exu/exu-_253F.webp', sourceUrl: `${ASSETS}/fr/ex/exu/%253F/high.webp` })
    await enqueueLow(clock, 'fr', 'exu-%3F')

    await tick(deps)
    expect(fake.requests.map((request) => request.url)).toEqual([url])
    expect(await lowRow(cardId)).toMatchObject({ image_low_status: 'downloaded', image_low_r2_key: 'cards/fr/exu/exu-_253F.low.webp' })
  })

  it('URL source de l’image principale inconnue ou non reconnue : « missing_api_url », aucune requête', async () => {
    const fake = new FakeTcgdex()
    const clock = new Clock()
    const deps = makeDeps(fake, clock)
    await seedCard(clock, { sourceUrl: null })
    await seedCard(clock, { tcgdexId: 'swsh3-137', mainKey: 'cards/fr/swsh3/swsh3-137.webp', sourceUrl: 'https://evil.example.com/fr/swsh/swsh3/137/high.webp' })
    await enqueueLow(clock, 'fr', 'swsh3-136')
    await enqueueLow(clock, 'fr', 'swsh3-137')

    await drain(deps, clock)
    for (const id of ['fr:swsh3-136', 'fr:swsh3-137']) {
      const row = await lowRow(id)
      expect(row.image_low_status, id).toBe('missing_api_url')
      expect(row.image_low_failure_reason, id).toContain('URL source')
    }
    expect(fake.requests).toHaveLength(0)
  })

  it('image principale pas encore copiée : la petite image se termine sans l’attendre (aucun rendu), puis est copiée après elle', async () => {
    const fake = new FakeTcgdex()
    fake.addImage(HIGH_136, webp(300, 1))
    fake.addImage(LOW_136, webp(120, 21))
    const clock = new Clock()
    const deps = makeDeps(fake, clock)
    const { cardId } = await seedPendingMain(clock)
    const database = db()
    // Image principale prête dans une heure seulement ; la petite image tout de suite.
    await database.batch(enqueueStmts(database, 'image', 'fr', [imageJob('fr', 'swsh3-136')], { runId: null, maxAttempts: 5, nowMs: clock.now(), notBeforeMs: clock.now() + 60 * MIN }))
    await enqueueLow(clock, 'fr', 'swsh3-136')

    // Aucun rendu (qui userait MAX_RELEASES pendant une longue file d'images principales) : terminée.
    expect(await tick(deps)).toMatchObject({ status: 'ran', claimed: 1, done: 1, released: 0, httpRequests: 0 })
    expect(await jobByKey('image_low:fr:swsh3-136')).toMatchObject({ status: 'done', attempts: 1, releases: 0, last_error: null })

    await drain(deps, clock)
    expect(await one(`SELECT image_status FROM cards WHERE id = ?1`, cardId)).toEqual({ image_status: 'downloaded' })
    expect((await lowRow(cardId)).image_low_status).toBe('downloaded')
    expect(fake.requests.map((request) => request.url)).toEqual([HIGH_136, LOW_136])
    expect(await jobByKey('image_low:fr:swsh3-136')).toMatchObject({ status: 'done', releases: 0 })
  })

  it('petite image passée avant son image principale : terminée sans requête ni écriture, remise en attente par la copie de la principale, puis copiée ; aucune tâche créée pour une carte qui n’en avait pas', async () => {
    const fake = new FakeTcgdex()
    const HIGH_137 = `${ASSETS}/fr/swsh/swsh3/137/high.webp`
    fake.addImage(HIGH_136, webp(300, 1))
    fake.addImage(HIGH_137, webp(310, 2))
    fake.addImage(LOW_136, webp(120, 21))
    fake.addImage(`${ASSETS}/fr/swsh/swsh3/137/low.webp`, webp(130, 22))
    const clock = new Clock()
    const deps = makeDeps(fake, clock)
    const { cardId } = await seedPendingMain(clock)
    await seedPendingMain(clock, 'swsh3-137')
    const database = db()
    // Images principales de 136 et 137 prêtes dans une heure ; seule 136 a une tâche de petite image, prête tout de suite.
    const later = { runId: null, maxAttempts: 5, nowMs: clock.now(), notBeforeMs: clock.now() + 60 * MIN }
    await database.batch(enqueueStmts(database, 'image', 'fr', [imageJob('fr', 'swsh3-136'), imageJob('fr', 'swsh3-137')], later))
    await enqueueLow(clock, 'fr', 'swsh3-136')
    const cardBefore = await one(`SELECT * FROM cards WHERE id = ?1`, cardId)

    // 1. La petite image passe seule : terminée, sans requête, sans écriture (carte, R2), sans rendu.
    expect(await tick(deps)).toMatchObject({ status: 'ran', claimed: 1, done: 1, released: 0, httpRequests: 0, bytesStored: 0 })
    expect(fake.requests).toHaveLength(0)
    expect(await one(`SELECT * FROM cards WHERE id = ?1`, cardId)).toEqual(cardBefore)
    expect(await r2Keys('cards/')).toEqual([])
    expect(await jobByKey('image_low:fr:swsh3-136')).toMatchObject({ status: 'done', attempts: 1, releases: 0, last_error: null })

    // 2. Les images principales seules (maxJobs : 2) : leur copie remet la petite image de 136 en attente,
    //    tentatives et rendus à zéro ; 137, qui n'avait pas de tâche de petite image, n'en reçoit aucune.
    clock.advance(60 * MIN)
    expect(await runTick(deps, { trigger: 'test', maxJobs: 2 })).toMatchObject({ status: 'ran', claimed: 2, done: 2, httpRequests: 2 })
    expect(fake.requests.map((request) => request.url).sort()).toEqual([HIGH_136, HIGH_137])
    expect(await rows(`SELECT id, image_status, image_low_status FROM cards ORDER BY id`)).toEqual([
      { id: cardId, image_status: 'downloaded', image_low_status: null },
      { id: 'fr:swsh3-137', image_status: 'downloaded', image_low_status: null },
    ])
    const revived = `SELECT status, attempts, releases, last_error, completed_at, next_attempt_at <= ?1 AS ready FROM import_jobs WHERE dedupe_key = 'image_low:fr:swsh3-136'`
    expect(await one(revived, iso(clock.now()))).toEqual({ status: 'pending', attempts: 0, releases: 0, last_error: null, completed_at: null, ready: 1 })
    expect(await rows(`SELECT dedupe_key FROM import_jobs WHERE dedupe_key LIKE 'image_low:%'`)).toEqual([{ dedupe_key: 'image_low:fr:swsh3-136' }])

    // 3. Exécution suivante : la petite image de 136 est copiée ; celle de 137 n'est jamais demandée.
    expect(await tick(deps)).toMatchObject({ status: 'ran', claimed: 1, done: 1, httpRequests: 1, bytesStored: 120 })
    expect(await lowRow(cardId)).toMatchObject({ image_low_status: 'downloaded', image_low_r2_key: LOW_KEY_136, image_low_file_size: 120 })
    expect(await r2Bytes(LOW_KEY_136)).toEqual(webp(120, 21))
    expect(await jobByKey('image_low:fr:swsh3-136')).toMatchObject({ status: 'done', attempts: 1, releases: 0 })
    expect(fake.requests.at(-1)?.url).toBe(LOW_136)
    expect(fake.count((url) => url.endsWith('/low.webp'))).toBe(1)
  })

  it('petite image réservée dans le même lot que son image principale : rendue une fois (jamais perdue), puis copiée', async () => {
    const fake = new FakeTcgdex()
    fake.addImage(HIGH_136, webp(300, 1))
    fake.addImage(LOW_136, webp(120, 21))
    const clock = new Clock()
    const deps = makeDeps(fake, clock)
    const { cardId } = await seedPendingMain(clock)
    const database = db()
    await database.batch(enqueueStmts(database, 'image', 'fr', [imageJob('fr', 'swsh3-136')], { runId: null, maxAttempts: 5, nowMs: clock.now() }))
    await enqueueLow(clock, 'fr', 'swsh3-136')

    // Même lot : la remise en file écrite par l'image principale passerait avant la clôture de la
    // petite image (encore en cours) ; celle-ci est donc rendue au lieu d'être terminée sans rien copier.
    expect(await tick(deps)).toMatchObject({ status: 'ran', claimed: 2, done: 1, released: 1, httpRequests: 1 })
    expect(await one(`SELECT image_status FROM cards WHERE id = ?1`, cardId)).toEqual({ image_status: 'downloaded' })
    expect(await jobByKey('image_low:fr:swsh3-136')).toMatchObject({ status: 'pending', attempts: 0, releases: 1, last_error: 'petite image : image principale en cours de copie' })

    await drain(deps, clock)
    expect(await lowRow(cardId)).toMatchObject({ image_low_status: 'downloaded', image_low_r2_key: LOW_KEY_136 })
    expect(fake.requests.map((request) => request.url)).toEqual([HIGH_136, LOW_136])
    expect(await jobByKey('image_low:fr:swsh3-136')).toMatchObject({ status: 'done', releases: 1 })
  })

  it('image principale absente sans tâche active : rien à faire, aucune écriture (la carte sera reprise plus tard)', async () => {
    const fake = new FakeTcgdex()
    const clock = new Clock()
    const deps = makeDeps(fake, clock)
    const { cardId } = await seedCard(clock, { mainStatus: 'not_found_on_cdn', mainKey: null, sourceUrl: null })
    await enqueueLow(clock, 'fr', 'swsh3-136')

    expect(await tick(deps)).toMatchObject({ status: 'ran', done: 1, httpRequests: 0 })
    expect(await lowRow(cardId)).toMatchObject({ image_low_status: null, image_low_checked_at: null })
  })

  it('verrou expiré après la dernière tentative : petite image en « download_error », image principale intacte', async () => {
    const fake = new FakeTcgdex()
    const clock = new Clock()
    const deps = makeDeps(fake, clock, { MAX_ATTEMPTS: '1' })
    const { cardId } = await seedCard(clock)
    await enqueueLow(clock, 'fr', 'swsh3-136', 1)
    await enqueueLogo(clock, 'fr', 'swsh3', 1)
    await env.DB.prepare(`UPDATE sets SET logo_url = ?1 WHERE id = 'fr:swsh3'`).bind(LOGO).run()
    const claimed = await claimJobs(db(), { limit: 10, token: 'execution-morte', nowMs: clock.now(), leaseMs: deps.config.jobLeaseMs })
    expect(claimed).toHaveLength(2)

    clock.advance(deps.config.jobLeaseMs + 1)
    await tick(deps)
    const reason = 'verrou expiré : nombre maximal de tentatives atteint'
    expect(await lowRow(cardId)).toMatchObject({ image_low_status: 'download_error', image_low_failure_reason: reason })
    expect(await logoRow('fr:swsh3')).toMatchObject({ logo_status: 'download_error', logo_failure_reason: reason })
    expect(await one(`SELECT image_status, image_failure_reason FROM cards WHERE id = ?1`, cardId)).toEqual({ image_status: 'downloaded', image_failure_reason: null })
    expect(fake.requests).toHaveLength(0)
  })
})

// ——— Logo ———

describe('logo d’extension', () => {
  it('copie : « cards/fr/swsh3/_logo.webp », validé, métadonnées ; extension « downloaded »', async () => {
    const fake = new FakeTcgdex()
    const body = webp(500, 25)
    fake.addImage(`${LOGO}.webp`, body)
    const clock = new Clock()
    const deps = makeDeps(fake, clock)
    await seedSet(clock, 'fr', 'swsh3', LOGO)
    await enqueueLogo(clock, 'fr', 'swsh3')

    expect(await tick(deps)).toMatchObject({ status: 'ran', done: 1, httpRequests: 1, bytesStored: 500 })
    expect(await r2Bytes(LOGO_KEY)).toEqual(body)
    const sha256 = await sha256Hex(body)
    expect((await env.BUCKET.head(LOGO_KEY))?.customMetadata).toEqual({ variant: 'logo', language: 'fr', setId: 'swsh3', sourceUrl: `${LOGO}.webp`, sha256, md5: await md5Hex(body) })
    expect(await logoRow('fr:swsh3')).toEqual({
      logo_status: 'downloaded',
      logo_r2_key: LOGO_KEY,
      logo_source_url: `${LOGO}.webp`,
      logo_file_size: 500,
      logo_sha256: sha256,
      logo_failure_reason: null,
    })
    expect(await jobByKey('logo:fr:swsh3')).toMatchObject({ job_type: 'image', status: 'done', priority: MEDIA_PRIORITY, payload_json: '{"media":"logo"}' })
  })

  it('404 : « not_found_on_cdn » avec l’URL testée, sans nouvelle tentative', async () => {
    const fake = new FakeTcgdex()
    const clock = new Clock()
    const deps = makeDeps(fake, clock)
    await seedSet(clock, 'fr', 'swsh3', LOGO)
    await enqueueLogo(clock, 'fr', 'swsh3')
    await drain(deps, clock)
    expect(await logoRow('fr:swsh3')).toMatchObject({ logo_status: 'not_found_on_cdn', logo_source_url: `${LOGO}.webp`, logo_r2_key: null })
    expect(fake.requests).toHaveLength(1)
  })

  it('fichier non WebP : « invalid_image » ; logo d’une autre langue ou hors du CDN : « missing_api_url » sans requête', async () => {
    const fake = new FakeTcgdex()
    const png = new Uint8Array(300)
    png.set([0x89, 0x50, 0x4e, 0x47])
    fake.addImage(`${LOGO}.webp`, png)
    const clock = new Clock()
    const deps = makeDeps(fake, clock)
    await seedSet(clock, 'fr', 'swsh3', LOGO)
    await seedSet(clock, 'fr', 'swsh4', `${ASSETS}/en/swsh/swsh4/logo`)
    await seedSet(clock, 'fr', 'swsh5', 'https://evil.example.com/fr/swsh/swsh5/logo')
    for (const set of ['swsh3', 'swsh4', 'swsh5']) await enqueueLogo(clock, 'fr', set)

    await drain(deps, clock)
    expect(await logoRow('fr:swsh3')).toMatchObject({ logo_status: 'invalid_image', logo_r2_key: null })
    expect((await logoRow('fr:swsh3')).logo_failure_reason).toContain('signature WebP absente')
    for (const set of ['swsh4', 'swsh5']) {
      const row = await logoRow(`fr:${set}`)
      expect(row.logo_status, set).toBe('missing_api_url')
      expect(row.logo_failure_reason, set).toContain('URL de logo refusée')
    }
    expect(fake.requests.map((request) => request.url)).toEqual([`${LOGO}.webp`])
    expect(await r2Keys('cards/')).toEqual([])
  })

  it('logo déjà présent dans R2 : réconcilié sans téléchargement ; déjà enregistré : aucune requête', async () => {
    const fake = new FakeTcgdex()
    fake.addImage(`${LOGO}.webp`, webp(500, 25))
    const clock = new Clock()
    const deps = makeDeps(fake, clock)
    await seedSet(clock, 'fr', 'swsh3', LOGO)
    const body = webp(450, 26)
    await env.BUCKET.put(LOGO_KEY, body, { customMetadata: { sha256: await sha256Hex(body), setId: 'swsh3', variant: 'logo' } })
    await enqueueLogo(clock, 'fr', 'swsh3')

    expect(await tick(deps)).toMatchObject({ done: 1, httpRequests: 0 })
    expect(await logoRow('fr:swsh3')).toMatchObject({ logo_status: 'downloaded', logo_r2_key: LOGO_KEY, logo_file_size: 450 })
    expect(await r2Bytes(LOGO_KEY)).toEqual(body)

    // Relancée : rien à faire.
    clock.advance(MIN)
    await enqueueLogo(clock, 'fr', 'swsh3')
    expect(await tick(deps)).toMatchObject({ done: 1, httpRequests: 0 })
    expect(fake.requests).toHaveLength(0)
  })
})

// ——— Symbole ———

describe('symbole d’extension', () => {
  it('copie (servi en application/octet-stream, comme le vrai CDN) : « cards/fr/swsh3/_symbol.webp », validé, métadonnées ; logo intact', async () => {
    const fake = new FakeTcgdex()
    const body = webp(352, 41)
    addSymbol(fake, `${SYMBOL}.webp`, body)
    const clock = new Clock()
    const deps = makeDeps(fake, clock)
    await seedSet(clock, 'fr', 'swsh3', LOGO, { symbolUrl: SYMBOL })
    await enqueueSymbol(clock, 'fr', 'swsh3')

    expect(await tick(deps)).toMatchObject({ status: 'ran', done: 1, httpRequests: 1, bytesStored: 352 })
    expect(fake.requests.map((request) => request.url)).toEqual([`${SYMBOL}.webp`])
    expect(await r2Bytes(SYMBOL_KEY)).toEqual(body)
    const head = await env.BUCKET.head(SYMBOL_KEY)
    const sha256 = await sha256Hex(body)
    // Stocké en image/webp (signature vérifiée), quel que soit le type générique servi par le CDN.
    expect(head?.httpMetadata?.contentType).toBe('image/webp')
    expect(head?.customMetadata).toEqual({ variant: 'symbol', language: 'fr', setId: 'swsh3', sourceUrl: `${SYMBOL}.webp`, sha256, md5: await md5Hex(body) })
    expect(await symbolRow('fr:swsh3')).toEqual({
      symbol_status: 'downloaded',
      symbol_r2_key: SYMBOL_KEY,
      symbol_source_url: `${SYMBOL}.webp`,
      symbol_file_size: 352,
      symbol_sha256: sha256,
      symbol_checked_at: iso(clock.now()),
      symbol_failure_reason: null,
    })
    expect(await jobByKey('symbol:fr:swsh3')).toMatchObject({ job_type: 'image', status: 'done', attempts: 1, priority: MEDIA_PRIORITY, payload_json: '{"media":"symbol"}' })
    // Le logo de l'extension n'est pas touché ; seul le symbole est sous cards/.
    expect(await logoRow('fr:swsh3')).toMatchObject({ logo_status: null, logo_r2_key: null })
    expect(await r2Keys('cards/')).toEqual([SYMBOL_KEY])
  })

  it('404 : « not_found_on_cdn » avec l’URL testée, sans nouvelle tentative, aucun objet', async () => {
    const fake = new FakeTcgdex()
    const clock = new Clock()
    const deps = makeDeps(fake, clock)
    await seedSet(clock, 'fr', 'swsh3', null, { symbolUrl: SYMBOL })
    await enqueueSymbol(clock, 'fr', 'swsh3')
    await drain(deps, clock)

    const row = await symbolRow('fr:swsh3')
    expect(row).toMatchObject({ symbol_status: 'not_found_on_cdn', symbol_source_url: `${SYMBOL}.webp`, symbol_r2_key: null, symbol_file_size: null })
    expect(row.symbol_failure_reason).toContain('symbole fr:swsh3 introuvable sur le CDN (HTTP 404)')
    expect(fake.requests).toHaveLength(1)
    expect(await jobByKey('symbol:fr:swsh3')).toMatchObject({ status: 'done', attempts: 1 })
    expect(await r2Keys('cards/')).toEqual([])
  })

  it('fichier invalide : « invalid_image » (non WebP, ou type MIME autre que webp/octet-stream) ; symbole absent, d’une autre langue ou hors du CDN : « missing_api_url » sans requête', async () => {
    const fake = new FakeTcgdex()
    const png = new Uint8Array(300)
    png.set([0x89, 0x50, 0x4e, 0x47])
    addSymbol(fake, `${SYMBOL}.webp`, png)
    const html = `${ASSETS}/univ/swsh/swsh4/symbol`
    fake.addImage(`${html}.webp`, webp(352, 43), { headers: { 'content-type': 'text/html' } })
    const clock = new Clock()
    const deps = makeDeps(fake, clock)
    await seedSet(clock, 'fr', 'swsh3', null, { symbolUrl: SYMBOL })
    await seedSet(clock, 'fr', 'swsh4', null, { symbolUrl: html })
    await seedSet(clock, 'fr', 'swsh5', null, { symbolUrl: `${ASSETS}/en/swsh/swsh5/symbol` })
    await seedSet(clock, 'fr', 'swsh6', null, { symbolUrl: 'https://evil.example.com/univ/swsh/swsh6/symbol' })
    await seedSet(clock, 'fr', 'swsh7', null)
    for (const set of ['swsh3', 'swsh4', 'swsh5', 'swsh6', 'swsh7']) await enqueueSymbol(clock, 'fr', set)

    await drain(deps, clock)
    expect(await symbolRow('fr:swsh3')).toMatchObject({ symbol_status: 'invalid_image', symbol_r2_key: null })
    expect((await symbolRow('fr:swsh3')).symbol_failure_reason).toContain('signature WebP absente')
    expect(await symbolRow('fr:swsh4')).toMatchObject({ symbol_status: 'invalid_image', symbol_r2_key: null })
    expect((await symbolRow('fr:swsh4')).symbol_failure_reason).toContain('type MIME inattendu : text/html')
    for (const set of ['swsh5', 'swsh6']) {
      const row = await symbolRow(`fr:${set}`)
      expect(row.symbol_status, set).toBe('missing_api_url')
      expect(row.symbol_failure_reason, set).toContain('URL de symbole refusée')
    }
    expect(await symbolRow('fr:swsh7')).toMatchObject({ symbol_status: 'missing_api_url', symbol_failure_reason: 'aucun symbole donné par l’API' })
    expect(fake.requests.map((request) => request.url).sort()).toEqual([`${SYMBOL}.webp`, `${html}.webp`])
    expect(await r2Keys('cards/')).toEqual([])
  })

  it('« application/octet-stream » accepté pour le symbole seulement : le même fichier refusé pour un logo et une petite image', async () => {
    const fake = new FakeTcgdex()
    addSymbol(fake, `${SYMBOL}.webp`, webp(352, 41))
    addSymbol(fake, `${LOGO}.webp`, webp(500, 25))
    addSymbol(fake, LOW_136, webp(120, 21))
    const clock = new Clock()
    const deps = makeDeps(fake, clock)
    const { cardId } = await seedCard(clock)
    await env.DB.prepare(`UPDATE sets SET logo_url = ?1, symbol_url = ?2 WHERE id = 'fr:swsh3'`).bind(LOGO, SYMBOL).run()
    await enqueueSymbol(clock, 'fr', 'swsh3')
    await enqueueLogo(clock, 'fr', 'swsh3')
    await enqueueLow(clock, 'fr', 'swsh3-136')

    await drain(deps, clock)
    expect(await symbolRow('fr:swsh3')).toMatchObject({ symbol_status: 'downloaded', symbol_r2_key: SYMBOL_KEY })
    const logo = await logoRow('fr:swsh3')
    expect(logo.logo_status).toBe('invalid_image')
    expect(logo.logo_failure_reason).toContain('type MIME inattendu : application/octet-stream')
    const low = await lowRow(cardId)
    expect(low.image_low_status).toBe('invalid_image')
    expect(low.image_low_failure_reason).toContain('type MIME inattendu : application/octet-stream')
    expect(await r2Keys('cards/')).toEqual([SYMBOL_KEY])
    expect(fake.requests).toHaveLength(3)
  })

  it('transfert corrompu (MD5 ≠ ETag) malgré le type générique : nouvelle tentative, puis « download_error », jamais d’objet', async () => {
    const fake = new FakeTcgdex()
    const corrupt = () => imageResponse(webp(352, 41), { 'content-type': 'application/octet-stream', etag: '"00112233445566778899aabbccddeeff"' })
    fake.queue(`${SYMBOL}.webp`, corrupt, corrupt)
    const clock = new Clock()
    const deps = makeDeps(fake, clock, { MAX_ATTEMPTS: '2' })
    await seedSet(clock, 'fr', 'swsh3', null, { symbolUrl: SYMBOL })
    await enqueueSymbol(clock, 'fr', 'swsh3', 2)

    expect(await tick(deps)).toMatchObject({ status: 'ran', retried: 1, done: 0 })
    expect(await jobByKey('symbol:fr:swsh3')).toMatchObject({ status: 'pending', attempts: 1 })
    expect((await symbolRow('fr:swsh3')).symbol_status).toBeNull()

    await drain(deps, clock)
    expect(await jobByKey('symbol:fr:swsh3')).toMatchObject({ status: 'failed', attempts: 2 })
    const row = await symbolRow('fr:swsh3')
    expect(row.symbol_status).toBe('download_error')
    expect(row.symbol_failure_reason).toContain('ETag')
    expect(fake.requests).toHaveLength(2)
    expect(await env.BUCKET.head(SYMBOL_KEY)).toBeNull()
  })

  it('symbole déjà présent dans R2 : réconcilié sans téléchargement ; déjà enregistré : aucune requête, rien de réécrit (idempotence)', async () => {
    const fake = new FakeTcgdex()
    addSymbol(fake, `${SYMBOL}.webp`, webp(352, 41))
    const clock = new Clock()
    const deps = makeDeps(fake, clock)
    await seedSet(clock, 'fr', 'swsh3', null, { symbolUrl: SYMBOL })
    const body = webp(340, 44)
    const sha256 = await sha256Hex(body)
    await env.BUCKET.put(SYMBOL_KEY, body, { customMetadata: { sha256, setId: 'swsh3', variant: 'symbol' } })
    const uploaded = (await env.BUCKET.head(SYMBOL_KEY))?.uploaded.toISOString()
    await enqueueSymbol(clock, 'fr', 'swsh3')

    expect(await tick(deps)).toMatchObject({ done: 1, httpRequests: 0, bytesStored: 0 })
    expect(await symbolRow('fr:swsh3')).toMatchObject({ symbol_status: 'downloaded', symbol_r2_key: SYMBOL_KEY, symbol_file_size: 340, symbol_sha256: sha256 })
    expect(await r2Bytes(SYMBOL_KEY)).toEqual(body)
    expect((await env.BUCKET.head(SYMBOL_KEY))?.uploaded.toISOString()).toBe(uploaded)

    // Relancée : rien à faire, la ligne de l'extension n'est pas réécrite.
    const before = await one(`SELECT * FROM sets WHERE id = 'fr:swsh3'`)
    clock.advance(MIN)
    await enqueueSymbol(clock, 'fr', 'swsh3')
    expect(await tick(deps)).toMatchObject({ done: 1, httpRequests: 0 })
    expect(await one(`SELECT * FROM sets WHERE id = 'fr:swsh3'`)).toEqual(before)
    expect(fake.requests).toHaveLength(0)
  })

  it('objet valide d’une autre extension à la même clé : jamais écrasé, échec enregistré, aucune requête', async () => {
    const fake = new FakeTcgdex()
    addSymbol(fake, `${SYMBOL}.webp`, webp(352, 41))
    const clock = new Clock()
    const deps = makeDeps(fake, clock)
    await seedSet(clock, 'fr', 'swsh3', null, { symbolUrl: SYMBOL })
    const foreign = webp(380, 45)
    await env.BUCKET.put(SYMBOL_KEY, foreign, { customMetadata: { sha256: await sha256Hex(foreign), setId: 'autre-extension', variant: 'symbol' } })
    await enqueueSymbol(clock, 'fr', 'swsh3')

    expect(await tick(deps)).toMatchObject({ status: 'ran', done: 1, httpRequests: 0 })
    const row = await symbolRow('fr:swsh3')
    expect(row).toMatchObject({ symbol_status: 'download_error', symbol_r2_key: null })
    expect(row.symbol_failure_reason).toContain('jamais écrasé')
    expect(await r2Bytes(SYMBOL_KEY)).toEqual(foreign)
    expect(fake.requests).toHaveLength(0)
  })

  it('verrou expiré après la dernière tentative : symbole en « download_error », logo intact', async () => {
    const fake = new FakeTcgdex()
    const clock = new Clock()
    const deps = makeDeps(fake, clock, { MAX_ATTEMPTS: '1' })
    await seedSet(clock, 'fr', 'swsh3', LOGO, { symbolUrl: SYMBOL, logoStatus: 'downloaded', logoKey: LOGO_KEY, logoSize: 500 })
    await enqueueSymbol(clock, 'fr', 'swsh3', 1)
    expect(await claimJobs(db(), { limit: 10, token: 'execution-morte', nowMs: clock.now(), leaseMs: deps.config.jobLeaseMs })).toHaveLength(1)

    clock.advance(deps.config.jobLeaseMs + 1)
    await tick(deps)
    expect(await symbolRow('fr:swsh3')).toMatchObject({ symbol_status: 'download_error', symbol_failure_reason: 'verrou expiré : nombre maximal de tentatives atteint' })
    expect(await logoRow('fr:swsh3')).toMatchObject({ logo_status: 'downloaded', logo_r2_key: LOGO_KEY, logo_failure_reason: null })
    expect(await jobByKey('symbol:fr:swsh3')).toMatchObject({ status: 'failed' })
    expect(fake.requests).toHaveLength(0)
  })
})

// ——— Création des tâches par la synchronisation ———

describe('tâches créées par la tâche « set »', () => {
  it('import complet : petites images et logo copiés en dernière priorité, rattachés à la synchronisation', async () => {
    const fake = catalogWithMedia()
    const clock = new Clock()
    const deps = makeDeps(fake, clock)
    const started = await startSync(db(), deps.config, clock.now())
    const runIds = Object.fromEntries(started.started.map((run) => [run.language, run.runId]))
    const ticks = await drain(deps, clock)
    expect(ticks.at(-1)?.status).toBe('idle')

    expect(await r2Keys('cards/')).toEqual([
      LOGO_KEY,
      LOW_KEY_136,
      KEY_136,
      'cards/fr/swsh3/swsh3-137.low.webp',
      'cards/fr/swsh3/swsh3-137.webp',
      'cards/ja/SV1a/SV1a-001.low.webp',
      'cards/ja/SV1a/SV1a-001.webp',
      'cards/ja/SV1a/SV1a-002.low.webp',
      'cards/ja/SV1a/SV1a-002.webp',
    ])
    expect(await r2Bytes(LOW_KEY_136)).toEqual(webp(120, 21))
    const cards = await rows<{ id: string; image_status: string; image_low_status: string | null; image_low_r2_key: string | null }>(
      `SELECT id, image_status, image_low_status, image_low_r2_key FROM cards ORDER BY id`,
    )
    expect(cards).toEqual([
      { id: 'fr:exu-%3F', image_status: 'not_found_on_cdn', image_low_status: null, image_low_r2_key: null },
      { id: 'fr:swsh3-136', image_status: 'downloaded', image_low_status: 'downloaded', image_low_r2_key: LOW_KEY_136 },
      { id: 'fr:swsh3-137', image_status: 'recovered_from_cdn', image_low_status: 'downloaded', image_low_r2_key: 'cards/fr/swsh3/swsh3-137.low.webp' },
      { id: 'fr:swsh3-138', image_status: 'not_found_on_cdn', image_low_status: null, image_low_r2_key: null },
      { id: 'ja:SM1+-001', image_status: 'not_found_on_cdn', image_low_status: null, image_low_r2_key: null },
      { id: 'ja:SV1a-001', image_status: 'downloaded', image_low_status: 'downloaded', image_low_r2_key: 'cards/ja/SV1a/SV1a-001.low.webp' },
      { id: 'ja:SV1a-002', image_status: 'recovered_from_cdn', image_low_status: 'downloaded', image_low_r2_key: 'cards/ja/SV1a/SV1a-002.low.webp' },
    ])
    expect(await rows(`SELECT id, logo_status, logo_r2_key FROM sets ORDER BY id`)).toEqual([
      { id: 'fr:exu', logo_status: null, logo_r2_key: null },
      { id: 'fr:swsh3', logo_status: 'downloaded', logo_r2_key: LOGO_KEY },
      { id: 'ja:SM1+', logo_status: null, logo_r2_key: null },
      { id: 'ja:SV1a', logo_status: null, logo_r2_key: null },
    ])

    // Une tâche de petite image par carte nouvelle, un logo pour la seule extension qui en a un ;
    // priorité la plus basse, rattachées à la synchronisation de leur langue ; tout est terminé.
    const media = await rows<{ dedupe_key: string; language: string; priority: number; run_id: number; status: string }>(
      `SELECT dedupe_key, language, priority, run_id, status FROM import_jobs WHERE priority = ?1 ORDER BY dedupe_key`,
      MEDIA_PRIORITY,
    )
    expect(media.map((job) => job.dedupe_key)).toEqual([
      'image_low:fr:exu-%3F',
      'image_low:fr:swsh3-136',
      'image_low:fr:swsh3-137',
      'image_low:fr:swsh3-138',
      'image_low:ja:SM1+-001',
      'image_low:ja:SV1a-001',
      'image_low:ja:SV1a-002',
      'logo:fr:swsh3',
    ])
    for (const job of media) expect(job, job.dedupe_key).toMatchObject({ status: 'done', run_id: runIds[job.language] })
    expect(await rows(`SELECT language, status FROM sync_runs ORDER BY language`)).toEqual([
      { language: 'fr', status: 'completed' },
      { language: 'ja', status: 'completed' },
    ])
    // Aucune requête vers une petite image de carte sans image principale.
    expect(fake.count((url) => url.endsWith('/low.webp'))).toBe(4)
    expect(fake.count((url) => url === `${LOGO}.webp`)).toBe(1)

    // L'audit du rapport de fin connaît ces objets.
    const summary = JSON.parse(await ((await env.BUCKET.get('reports/latest/summary.json')) as R2ObjectBody).text()) as { audit: Record<string, number>; status: string }
    expect(summary.status).toBe('TERMINÉ')
    expect(summary.audit).toMatchObject({ r2Objects: 9, unknownObjects: 0, lowImageObjects: 4, logoObjects: 1, mediaReconciled: 0, mediaMissingObjects: 0 })
  })

  it('synchronisation suivante : rien de retéléchargé, aucune nouvelle tâche, cartes et extensions inchangées', async () => {
    const fake = catalogWithMedia()
    const clock = new Clock()
    const deps = makeDeps(fake, clock)
    await startSync(db(), deps.config, clock.now())
    await drain(deps, clock)
    const cardsBefore = await rows(`SELECT * FROM cards ORDER BY id`)
    const logosBefore = await rows(`SELECT id, logo_status, logo_r2_key, logo_checked_at, logo_sha256 FROM sets ORDER BY id`)
    const jobsBefore = (await one<{ n: number }>(`SELECT COUNT(*) AS n FROM import_jobs`)).n
    const webpRequests = fake.count((url) => url.endsWith('.webp'))

    clock.advance(7 * DAY)
    await startSync(db(), deps.config, clock.now(), { kind: 'weekly' })
    await drain(deps, clock)
    expect(fake.count((url) => url.endsWith('.webp'))).toBe(webpRequests)
    expect((await one<{ n: number }>(`SELECT COUNT(*) AS n FROM import_jobs`)).n).toBe(jobsBefore)
    expect(await rows(`SELECT * FROM cards ORDER BY id`)).toEqual(cardsBefore)
    expect(await rows(`SELECT id, logo_status, logo_r2_key, logo_checked_at, logo_sha256 FROM sets ORDER BY id`)).toEqual(logosBefore)
  })

  it('petite image absente revérifiée seulement après MISSING_RECHECK_DAYS ; carte dont la petite image manque encore relancée par la synchro', async () => {
    const fake = catalogWithMedia()
    fake.images.delete(LOW_136)
    const clock = new Clock()
    const deps = makeDeps(fake, clock)
    await startSync(db(), deps.config, clock.now(), { languages: ['fr'] })
    await drain(deps, clock)
    expect((await lowRow('fr:swsh3-136')).image_low_status).toBe('not_found_on_cdn')
    expect(fake.count((url) => url === LOW_136)).toBe(1)
    fake.addImage(LOW_136, webp(120, 21))

    const weekly = async (days: number) => {
      clock.advance(days * DAY)
      await startSync(db(), deps.config, clock.now(), { kind: 'weekly', languages: ['fr'] })
      await drain(deps, clock)
    }
    await weekly(7)
    await weekly(21)
    expect(fake.count((url) => url === LOW_136)).toBe(1)
    await weekly(7)
    expect(fake.count((url) => url === LOW_136)).toBe(2)
    expect(await lowRow('fr:swsh3-136')).toMatchObject({ image_low_status: 'downloaded', image_low_r2_key: LOW_KEY_136 })
    // La petite image déjà copiée de 137 n'a jamais été redemandée.
    expect(fake.count((url) => url === `${ASSETS}/fr/swsh/swsh3/137/low.webp`)).toBe(1)
  })

  it('carte dont l’image principale devient valide plus tard : petite image créée à la synchronisation suivante', async () => {
    const fake = catalogWithMedia()
    const clock = new Clock()
    const deps = makeDeps(fake, clock)
    await startSync(db(), deps.config, clock.now(), { languages: ['fr'] })
    await drain(deps, clock)
    // Image principale de 138 récupérée hors synchronisation (relance manuelle) : pas encore de petite image.
    await env.DB.prepare(
      `UPDATE cards SET image_status = 'recovered_from_cdn', image_r2_key = 'cards/fr/swsh3/swsh3-138.webp',
         image_source_url = ?1, image_quality = 'high', image_file_size = 350 WHERE id = 'fr:swsh3-138'`,
    )
      .bind(`${ASSETS}/fr/swsh/swsh3/138/high.webp`)
      .run()
    fake.addImage(`${ASSETS}/fr/swsh/swsh3/138/low.webp`, webp(160, 27))

    clock.advance(7 * DAY)
    await startSync(db(), deps.config, clock.now(), { kind: 'weekly', languages: ['fr'] })
    await drain(deps, clock)
    expect(await lowRow('fr:swsh3-138')).toMatchObject({ image_low_status: 'downloaded', image_low_r2_key: 'cards/fr/swsh3/swsh3-138.low.webp' })
  })

  it('logo absent : URL changée par l’API → revérifié tout de suite ; extensions sans logo (JA) : aucune tâche', async () => {
    const fake = sampleCatalog()
    const clock = new Clock()
    const deps = makeDeps(fake, clock)
    await startSync(db(), deps.config, clock.now())
    await drain(deps, clock)
    expect(await logoRow('fr:swsh3')).toMatchObject({ logo_status: 'not_found_on_cdn', logo_source_url: `${LOGO}.webp` })
    expect(await rows(`SELECT dedupe_key FROM import_jobs WHERE dedupe_key LIKE 'logo:%'`)).toEqual([{ dedupe_key: 'logo:fr:swsh3' }])

    // Même URL une semaine plus tard : pas de nouvelle requête.
    clock.advance(7 * DAY)
    await startSync(db(), deps.config, clock.now(), { kind: 'weekly', languages: ['fr'] })
    await drain(deps, clock)
    expect(fake.count((url) => url === `${LOGO}.webp`)).toBe(1)

    // Nouvelle URL de logo : revérifiée sans attendre 30 jours.
    const swsh3 = fake.sets.fr.find((set) => set.id === 'swsh3')
    if (!swsh3) throw new Error('catalogue de référence inattendu')
    swsh3.logo = `${ASSETS}/fr/swsh/swsh3/logo-v2`
    fake.addImage(`${ASSETS}/fr/swsh/swsh3/logo-v2.webp`, webp(480, 28))
    clock.advance(7 * DAY)
    await startSync(db(), deps.config, clock.now(), { kind: 'weekly', languages: ['fr'] })
    await drain(deps, clock)
    expect(await logoRow('fr:swsh3')).toMatchObject({ logo_status: 'downloaded', logo_r2_key: LOGO_KEY, logo_source_url: `${ASSETS}/fr/swsh/swsh3/logo-v2.webp`, logo_file_size: 480 })

    // Logo copié : une URL encore différente ne le remplace jamais.
    swsh3.logo = `${ASSETS}/fr/swsh/swsh3/logo-v3`
    fake.addImage(`${ASSETS}/fr/swsh/swsh3/logo-v3.webp`, webp(470, 29))
    clock.advance(7 * DAY)
    await startSync(db(), deps.config, clock.now(), { kind: 'weekly', languages: ['fr'] })
    await drain(deps, clock)
    expect(fake.count((url) => url.endsWith('logo-v3.webp'))).toBe(0)
    expect(await r2Bytes(LOGO_KEY)).toEqual(webp(480, 28))
  })

  it('symboles : copiés à l’import (FR sous univ, JA) en dernière priorité, rattachés à la synchronisation ; la synchronisation suivante ne recrée ni ne retélécharge rien', async () => {
    const fake = catalogWithSymbols()
    const clock = new Clock()
    const deps = makeDeps(fake, clock)
    const started = await startSync(db(), deps.config, clock.now())
    const runIds = Object.fromEntries(started.started.map((run) => [run.language, run.runId]))
    expect((await drain(deps, clock)).at(-1)?.status).toBe('idle')

    expect(await r2Bytes(SYMBOL_KEY)).toEqual(webp(352, 41))
    expect(await r2Bytes(SYMBOL_KEY_JA)).toEqual(webp(360, 42))
    expect(await rows(`SELECT id, symbol_status, symbol_r2_key, symbol_source_url FROM sets ORDER BY id`)).toEqual([
      { id: 'fr:exu', symbol_status: null, symbol_r2_key: null, symbol_source_url: null },
      { id: 'fr:swsh3', symbol_status: 'downloaded', symbol_r2_key: SYMBOL_KEY, symbol_source_url: `${SYMBOL}.webp` },
      { id: 'ja:SM1+', symbol_status: null, symbol_r2_key: null, symbol_source_url: null },
      { id: 'ja:SV1a', symbol_status: 'downloaded', symbol_r2_key: SYMBOL_KEY_JA, symbol_source_url: `${SYMBOL_JA}.webp` },
    ])
    // Logo et petites images inchangés par l'ajout des symboles.
    expect(await logoRow('fr:swsh3')).toMatchObject({ logo_status: 'downloaded', logo_r2_key: LOGO_KEY })
    expect((await one<{ n: number }>(`SELECT COUNT(*) AS n FROM cards WHERE image_low_status = 'downloaded'`)).n).toBe(4)

    // Une tâche par extension qui a un symbole, et seulement elles ; priorité la plus basse, synchronisation de sa langue.
    const jobs = await rows<{ dedupe_key: string; language: string; job_type: string; priority: number; run_id: number; status: string; payload_json: string }>(
      `SELECT dedupe_key, language, job_type, priority, run_id, status, payload_json FROM import_jobs WHERE dedupe_key LIKE 'symbol:%' ORDER BY dedupe_key`,
    )
    expect(jobs.map((job) => job.dedupe_key)).toEqual(['symbol:fr:swsh3', 'symbol:ja:SV1a'])
    for (const job of jobs) {
      expect(job, job.dedupe_key).toMatchObject({ job_type: 'image', priority: MEDIA_PRIORITY, status: 'done', run_id: runIds[job.language], payload_json: '{"media":"symbol"}' })
    }
    expect(fake.count((url) => url.endsWith('/symbol.webp'))).toBe(2)
    const summary = JSON.parse(await ((await env.BUCKET.get('reports/latest/summary.json')) as R2ObjectBody).text()) as { audit: Record<string, number> }
    expect(summary.audit).toMatchObject({ r2Objects: 11, unknownObjects: 0, lowImageObjects: 4, logoObjects: 1, symbolObjects: 2, mediaReconciled: 0, mediaMissingObjects: 0 })

    // Synchronisation hebdomadaire : rien de retéléchargé, aucune nouvelle tâche, extensions inchangées.
    const setsBefore = await rows(`SELECT * FROM sets ORDER BY id`)
    const jobsBefore = (await one<{ n: number }>(`SELECT COUNT(*) AS n FROM import_jobs`)).n
    const webpRequests = fake.count((url) => url.endsWith('.webp'))
    clock.advance(7 * DAY)
    await startSync(db(), deps.config, clock.now(), { kind: 'weekly' })
    await drain(deps, clock)
    expect(fake.count((url) => url.endsWith('.webp'))).toBe(webpRequests)
    expect((await one<{ n: number }>(`SELECT COUNT(*) AS n FROM import_jobs`)).n).toBe(jobsBefore)
    expect(await rows(`SELECT * FROM sets ORDER BY id`)).toEqual(setsBefore)
    expect(await rows(`SELECT status FROM import_jobs WHERE dedupe_key LIKE 'symbol:%'`)).toEqual([{ status: 'done' }, { status: 'done' }])
  })

  it('symbole absent : revérifié seulement après MISSING_RECHECK_DAYS, tout de suite si l’URL change ; un symbole copié n’est jamais remplacé', async () => {
    const fake = sampleCatalog()
    const swsh3 = fake.sets.fr.find((set) => set.id === 'swsh3')
    if (!swsh3) throw new Error('catalogue de référence inattendu')
    swsh3.symbol = SYMBOL
    const clock = new Clock()
    const deps = makeDeps(fake, clock)
    const weekly = async (days: number) => {
      clock.advance(days * DAY)
      await startSync(db(), deps.config, clock.now(), { kind: 'weekly', languages: ['fr'] })
      await drain(deps, clock)
    }
    await startSync(db(), deps.config, clock.now(), { languages: ['fr'] })
    await drain(deps, clock)
    expect(await symbolRow('fr:swsh3')).toMatchObject({ symbol_status: 'not_found_on_cdn', symbol_source_url: `${SYMBOL}.webp` })
    expect(fake.count((url) => url === `${SYMBOL}.webp`)).toBe(1)

    // Même URL : pas de nouvelle requête avant 30 jours, puis une seule.
    await weekly(7)
    await weekly(21)
    expect(fake.count((url) => url === `${SYMBOL}.webp`)).toBe(1)
    await weekly(7)
    expect(fake.count((url) => url === `${SYMBOL}.webp`)).toBe(2)
    expect((await symbolRow('fr:swsh3')).symbol_status).toBe('not_found_on_cdn')

    // Nouvelle URL de symbole : revérifiée sans attendre 30 jours.
    swsh3.symbol = `${ASSETS}/univ/swsh/swsh3/symbol-v2`
    addSymbol(fake, `${swsh3.symbol}.webp`, webp(352, 46))
    await weekly(7)
    expect(await symbolRow('fr:swsh3')).toMatchObject({ symbol_status: 'downloaded', symbol_r2_key: SYMBOL_KEY, symbol_source_url: `${ASSETS}/univ/swsh/swsh3/symbol-v2.webp`, symbol_file_size: 352 })

    // Symbole copié : une URL encore différente ne le remplace jamais.
    swsh3.symbol = `${ASSETS}/univ/swsh/swsh3/symbol-v3`
    addSymbol(fake, `${swsh3.symbol}.webp`, webp(360, 47))
    await weekly(7)
    expect(fake.count((url) => url.endsWith('symbol-v3.webp'))).toBe(0)
    expect(await r2Bytes(SYMBOL_KEY)).toEqual(webp(352, 46))
    expect(await rows(`SELECT dedupe_key FROM import_jobs WHERE dedupe_key LIKE 'symbol:%'`)).toEqual([{ dedupe_key: 'symbol:fr:swsh3' }])
  })
})

// ——— Rattrapage par l'administration ———

describe('POST /admin/sync/backfill-media', () => {
  it('crée une tâche par petite image et par logo manquants, ignore le reste ; rappeler la route ne crée rien de plus', async () => {
    const fake = new FakeTcgdex()
    const clock = new Clock()
    const deps = makeDeps(fake, clock)
    const old = iso(clock.now() - 31 * DAY)
    const recent = iso(clock.now() - 2 * DAY)
    const card = (id: string, seed: CardSeed = {}) => seedCard(clock, { tcgdexId: id, mainKey: `cards/fr/swsh3/${id}.webp`, sourceUrl: `${ASSETS}/fr/swsh/swsh3/${id.split('-')[1]}/high.webp`, ...seed })
    await seedSet(clock, 'fr', 'swsh3', LOGO)
    await card('swsh3-1') // jamais traitée → oui
    await card('swsh3-2', { lowStatus: 'downloaded', lowKey: 'cards/fr/swsh3/swsh3-2.low.webp', lowCheckedAt: recent }) // copiée → non
    await card('swsh3-3', { lowStatus: 'not_found_on_cdn', lowCheckedAt: recent }) // absente récemment → non
    await card('swsh3-4', { lowStatus: 'not_found_on_cdn', lowCheckedAt: old }) // absente depuis > 30 j → oui
    await card('swsh3-5', { lowStatus: 'download_error', lowCheckedAt: old }) // en erreur depuis > 30 j → oui
    await card('swsh3-6', { mainStatus: 'not_found_on_cdn', mainKey: null }) // pas d'image principale → non
    await card('swsh3-7', { removedAt: recent }) // retirée → non
    await card('swsh3-8') // déjà une tâche active → non
    await enqueueLow(clock, 'fr', 'swsh3-8')
    await seedSet(clock, 'fr', 'swsh4', `${ASSETS}/fr/swsh/swsh4/logo`, { logoStatus: 'downloaded', logoKey: 'cards/fr/swsh4/_logo.webp', logoCheckedAt: recent }) // non
    await seedSet(clock, 'fr', 'swsh5', `${ASSETS}/fr/swsh/swsh5/logo`, { logoStatus: 'not_found_on_cdn', logoCheckedAt: recent, logoSourceUrl: `${ASSETS}/fr/swsh/swsh5/logo.webp` }) // non
    await seedSet(clock, 'fr', 'swsh6', `${ASSETS}/fr/swsh/swsh6/logo-v2`, { logoStatus: 'not_found_on_cdn', logoCheckedAt: recent, logoSourceUrl: `${ASSETS}/fr/swsh/swsh6/logo.webp` }) // URL changée → oui
    await seedSet(clock, 'ja', 'SV1a', null) // pas de logo → non
    await seedCard(clock, { language: 'ja', set: 'SV1a', tcgdexId: 'SV1a-001', mainKey: 'cards/ja/SV1a/SV1a-001.webp', sourceUrl: `${ASSETS}/ja/SV/SV1a/001/high.webp` }) // oui

    expect((await admin(deps, fake, 'POST', '/admin/sync/backfill-media?language=de')).status).toBe(400)
    expect((await admin(deps, fake, 'GET', '/admin/sync/backfill-media')).status).toBe(405)

    const fr = await admin(deps, fake, 'POST', '/admin/sync/backfill-media?language=fr')
    expect(fr.status).toBe(200)
    expect(await fr.json()).toMatchObject({ lowImages: 3, logos: 2, complete: true })
    const created = await rows<{ dedupe_key: string; job_type: string; priority: number; run_id: number | null; payload_json: string; status: string }>(
      `SELECT dedupe_key, job_type, priority, run_id, payload_json, status FROM import_jobs ORDER BY dedupe_key`,
    )
    expect(created.map((job) => job.dedupe_key)).toEqual([
      'image_low:fr:swsh3-1',
      'image_low:fr:swsh3-4',
      'image_low:fr:swsh3-5',
      'image_low:fr:swsh3-8',
      'logo:fr:swsh3',
      'logo:fr:swsh6',
    ])
    for (const job of created) {
      expect(job, job.dedupe_key).toMatchObject({ job_type: 'image', priority: MEDIA_PRIORITY, run_id: null, status: 'pending' })
      expect(JSON.parse(job.payload_json), job.dedupe_key).toEqual({ media: job.dedupe_key.startsWith('logo:') ? 'logo' : 'low' })
    }

    // Toutes langues (par défaut) : seule la carte japonaise reste ; puis plus rien.
    expect(await (await admin(deps, fake, 'POST', '/admin/sync/backfill-media')).json()).toMatchObject({ lowImages: 1, logos: 0, complete: true })
    expect(await (await admin(deps, fake, 'POST', '/admin/sync/backfill-media')).json()).toMatchObject({ lowImages: 0, logos: 0, complete: true })
    expect((await one<{ n: number }>(`SELECT COUNT(*) AS n FROM import_jobs`)).n).toBe(7)
  })

  it('symboles : une tâche par symbole manquant (même règle que les logos), « symbols » dans la réponse ; rappeler la route ne crée rien de plus ; puis copiés', async () => {
    const fake = new FakeTcgdex()
    const clock = new Clock()
    const deps = makeDeps(fake, clock)
    const old = iso(clock.now() - 31 * DAY)
    const recent = iso(clock.now() - 2 * DAY)
    const url = (set: string, name = 'symbol') => `${ASSETS}/univ/swsh/${set}/${name}`
    await seedSet(clock, 'fr', 'swsh1', null, { symbolUrl: url('swsh1') }) // jamais traité → oui
    await seedSet(clock, 'fr', 'swsh2', null, { symbolUrl: url('swsh2'), symbolStatus: 'downloaded', symbolKey: 'cards/fr/swsh2/_symbol.webp', symbolCheckedAt: recent }) // copié → non
    await seedSet(clock, 'fr', 'swsh3', null, { symbolUrl: url('swsh3'), symbolStatus: 'not_found_on_cdn', symbolCheckedAt: recent, symbolSourceUrl: `${url('swsh3')}.webp` }) // absent récemment → non
    await seedSet(clock, 'fr', 'swsh4', null, { symbolUrl: url('swsh4'), symbolStatus: 'not_found_on_cdn', symbolCheckedAt: old, symbolSourceUrl: `${url('swsh4')}.webp` }) // absent depuis > 30 j → oui
    await seedSet(clock, 'fr', 'swsh5', null, { symbolUrl: url('swsh5'), symbolStatus: 'download_error', symbolCheckedAt: old, symbolSourceUrl: `${url('swsh5')}.webp` }) // en erreur depuis > 30 j → oui
    await seedSet(clock, 'fr', 'swsh6', null, { symbolUrl: url('swsh6', 'symbol-v2'), symbolStatus: 'not_found_on_cdn', symbolCheckedAt: recent, symbolSourceUrl: `${url('swsh6')}.webp` }) // URL changée → oui
    await seedSet(clock, 'fr', 'swsh7', null) // pas de symbole → non
    await seedSet(clock, 'fr', 'swsh8', null, { symbolUrl: url('swsh8') }) // déjà une tâche active → non
    await enqueueSymbol(clock, 'fr', 'swsh8')
    await seedSet(clock, 'ja', 'SV1a', null, { symbolUrl: SYMBOL_JA }) // oui (JA)

    const fr = await admin(deps, fake, 'POST', '/admin/sync/backfill-media?language=fr')
    expect(fr.status).toBe(200)
    expect(await fr.json()).toMatchObject({ lowImages: 0, logos: 0, symbols: 4, complete: true })
    const created = await rows<{ dedupe_key: string; job_type: string; priority: number; run_id: number | null; payload_json: string; status: string }>(
      `SELECT dedupe_key, job_type, priority, run_id, payload_json, status FROM import_jobs ORDER BY dedupe_key`,
    )
    expect(created.map((job) => job.dedupe_key)).toEqual(['symbol:fr:swsh1', 'symbol:fr:swsh4', 'symbol:fr:swsh5', 'symbol:fr:swsh6', 'symbol:fr:swsh8'])
    for (const job of created) {
      expect(job, job.dedupe_key).toMatchObject({ job_type: 'image', priority: MEDIA_PRIORITY, run_id: null, payload_json: '{"media":"symbol"}', status: 'pending' })
    }

    // Toutes langues (par défaut) : seule l'extension japonaise reste ; puis plus rien.
    expect(await (await admin(deps, fake, 'POST', '/admin/sync/backfill-media')).json()).toMatchObject({ lowImages: 0, logos: 0, symbols: 1, complete: true })
    expect(await (await admin(deps, fake, 'POST', '/admin/sync/backfill-media')).json()).toMatchObject({ lowImages: 0, logos: 0, symbols: 0, complete: true })
    expect((await one<{ n: number }>(`SELECT COUNT(*) AS n FROM import_jobs`)).n).toBe(6)

    // Exécution : symboles présents sur le CDN copiés (servis en octet-stream), les autres introuvables.
    addSymbol(fake, `${url('swsh1')}.webp`, webp(352, 48))
    addSymbol(fake, `${SYMBOL_JA}.webp`, webp(360, 42))
    await drain(deps, clock)
    expect(await rows(`SELECT id, symbol_status FROM sets ORDER BY id`)).toEqual([
      { id: 'fr:swsh1', symbol_status: 'downloaded' },
      { id: 'fr:swsh2', symbol_status: 'downloaded' },
      { id: 'fr:swsh3', symbol_status: 'not_found_on_cdn' },
      { id: 'fr:swsh4', symbol_status: 'not_found_on_cdn' },
      { id: 'fr:swsh5', symbol_status: 'not_found_on_cdn' },
      { id: 'fr:swsh6', symbol_status: 'not_found_on_cdn' },
      { id: 'fr:swsh7', symbol_status: null },
      { id: 'fr:swsh8', symbol_status: 'not_found_on_cdn' },
      { id: 'ja:SV1a', symbol_status: 'downloaded' },
    ])
    expect(await r2Keys('cards/')).toEqual(['cards/fr/swsh1/_symbol.webp', SYMBOL_KEY_JA])
    expect(fake.count((request) => request.endsWith('.webp'))).toBe(6)
    expect(await (await admin(deps, fake, 'POST', '/admin/sync/backfill-media')).json()).toMatchObject({ lowImages: 0, logos: 0, symbols: 0, complete: true })
  })

  it('rattrapage par pages (plus de 500 cartes), puis exécution : tout est copié, et la route ne trouve plus rien', async () => {
    const fake = new FakeTcgdex()
    const clock = new Clock()
    const deps = makeDeps(fake, clock, { MIN_REQUEST_INTERVAL_MS: '50' })
    await seedSet(clock, 'fr', 'bulk', `${ASSETS}/fr/swsh/bulk/logo`)
    fake.addImage(`${ASSETS}/fr/swsh/bulk/logo.webp`, webp(400, 30))
    const n = 1_201
    const ids = Array.from({ length: n }, (_, i) => `bulk-${i + 1}`)
    await env.DB.prepare(
      `INSERT INTO cards (id, tcgdex_id, language, name, local_id, set_id, image_status, image_r2_key, image_source_url, image_quality, image_file_size, created_at, updated_at)
       SELECT 'fr:' || value, value, 'fr', value, value, 'fr:bulk', 'downloaded', 'cards/fr/bulk/' || value || '.webp',
         ?1 || '/' || value || '/high.webp', 'high', 300, ?2, ?2
       FROM json_each(?3)`,
    )
      .bind(`${ASSETS}/fr/swsh/bulk`, iso(clock.now()), JSON.stringify(ids))
      .run()

    expect(await (await admin(deps, fake, 'POST', '/admin/sync/backfill-media')).json()).toMatchObject({ lowImages: n, logos: 1, complete: true })
    expect((await one<{ n: number }>(`SELECT COUNT(*) AS n FROM import_jobs WHERE dedupe_key LIKE 'image_low:fr:bulk-%' AND priority = ?1`, MEDIA_PRIORITY)).n).toBe(n)

    // Quelques cartes seulement sur le CDN : les autres sont introuvables (404), sans nouvelle tentative.
    for (const id of ids.slice(0, 3)) fake.addImage(`${ASSETS}/fr/swsh/bulk/${id}/low.webp`, webp(100 + Number(id.split('-')[1]), 31))
    await drain(deps, clock, 200)
    expect(await rows(`SELECT image_low_status, COUNT(*) AS n FROM cards GROUP BY image_low_status ORDER BY image_low_status`)).toEqual([
      { image_low_status: 'downloaded', n: 3 },
      { image_low_status: 'not_found_on_cdn', n: n - 3 },
    ])
    expect((await logoRow('fr:bulk')).logo_status).toBe('downloaded')
    expect(fake.count((url) => url.endsWith('/low.webp'))).toBe(n)
    expect(await (await admin(deps, fake, 'POST', '/admin/sync/backfill-media')).json()).toMatchObject({ lowImages: 0, logos: 0, complete: true })
  }, 120_000)
})

// ——— Priorités ———

describe('priorités', () => {
  it('petites images, logos et symboles passent après catalogue, extensions, index, rapport, images principales et détails', async () => {
    expect(MEDIA_PRIORITY).toBeGreaterThan(Math.max(...Object.values(JOB_PRIORITY)))
    const clock = new Clock()
    const database = db()
    const opts = { runId: null, maxAttempts: 5, nowMs: clock.now() }
    // Créées dans l'ordre inverse de leur priorité.
    await database.batch([
      ...enqueueMediaStmts(database, 'fr', [logoJob('fr', 'swsh3'), lowImageJob('fr', 'swsh3-136'), symbolJob('fr', 'swsh3')], opts),
      ...enqueueStmts(database, 'card', 'fr', [{ target: 'swsh3-136', cardId: 'fr:swsh3-136', dedupeKey: 'card:fr:swsh3-136' }], opts),
      ...enqueueStmts(database, 'image', 'fr', [imageJob('fr', 'swsh3-136')], opts),
      ...enqueueStmts(database, 'report', null, [{ target: null, cardId: null, dedupeKey: 'report:*:*' }], opts),
      ...enqueueStmts(database, 'card_index', 'fr', [{ target: null, cardId: null, dedupeKey: 'card_index:fr:*' }], opts),
      ...enqueueStmts(database, 'set', 'fr', [{ target: 'swsh3', cardId: null, dedupeKey: 'set:fr:swsh3' }], opts),
      ...enqueueStmts(database, 'catalog', 'fr', [{ target: null, cardId: null, dedupeKey: 'catalog:fr:*' }], opts),
    ])
    const order: string[] = []
    for (let i = 0; i < 10; i++) {
      const [claimed] = await claimJobs(database, { limit: 1, token: `t${i}`, nowMs: clock.now(), leaseMs: MIN })
      if (!claimed) break
      order.push(`${claimed.job_type}:${claimed.priority}:${mediaKind(claimed) ?? '-'}`)
    }
    expect(order).toEqual(['catalog:0:-', 'set:1:-', 'card_index:2:-', 'report:3:-', 'image:4:-', 'card:5:-', 'image:6:logo', 'image:6:low', 'image:6:symbol'])
  })
})

// ——— Plafonds de coût ———

describe('plafonds de coût', () => {
  it('storedBytes compte images principales, petites images propres et logos (sans double comptage), sur index couvrants', async () => {
    const clock = new Clock()
    await seedCard(clock, { size: 300, lowStatus: 'downloaded', lowKey: LOW_KEY_136, lowSize: 120 })
    await seedCard(clock, { tcgdexId: 'swsh3-137', mainKey: 'cards/fr/swsh3/swsh3-137.webp', size: 200, lowStatus: 'downloaded', lowKey: 'cards/fr/swsh3/swsh3-137.webp', lowSize: null })
    await seedCard(clock, { tcgdexId: 'swsh3-138', mainKey: 'cards/fr/swsh3/swsh3-138.webp', size: 100, lowStatus: 'not_found_on_cdn', lowSize: 999 })
    await seedSet(clock, 'fr', 'swsh4', LOGO, { logoStatus: 'downloaded', logoKey: 'cards/fr/swsh4/_logo.webp', logoSize: 500 })
    await seedSet(clock, 'fr', 'swsh5', LOGO, { logoStatus: 'not_found_on_cdn', logoSize: 777 })
    expect(await storedBytes(db())).toBe(300 + 120 + 200 + 100 + 500)

    const plan = await rows<{ detail: string }>(`EXPLAIN QUERY PLAN SELECT COALESCE(SUM(image_low_file_size), 0) FROM cards WHERE image_low_status = 'downloaded'`)
    expect(plan.map((step) => step.detail).join('\n')).toMatch(/USING COVERING INDEX cards_low_stored/)
  })

  it('MAX_STORED_BYTES atteint par les petites images : pause automatique, plus aucune requête', async () => {
    const fake = new FakeTcgdex()
    fake.addImage(`${ASSETS}/fr/swsh/swsh3/137/low.webp`, webp(120, 22))
    const clock = new Clock()
    const deps = makeDeps(fake, clock, { MAX_STORED_BYTES: '1000000' })
    await seedCard(clock, { size: 600_000, lowStatus: 'downloaded', lowKey: LOW_KEY_136, lowSize: 450_000 })
    await seedCard(clock, { tcgdexId: 'swsh3-137', mainKey: 'cards/fr/swsh3/swsh3-137.webp', sourceUrl: `${ASSETS}/fr/swsh/swsh3/137/high.webp`, size: 300 })
    await enqueueLow(clock, 'fr', 'swsh3-137')

    const result = await tick(deps)
    expect(result).toMatchObject({ status: 'skipped', reason: 'plafond de stockage atteint (1050300 octets) : pause automatique', claimed: 0 })
    expect(fake.requests).toHaveLength(0)
    expect(await one(`SELECT value FROM runtime_state WHERE key = 'paused'`)).toEqual({ value: '1' })
  })

  it('opérations du jour : la requête et l’écriture R2 d’une petite image et d’un logo sont comptées', async () => {
    const fake = new FakeTcgdex()
    fake.addImage(LOW_136, webp(120, 21))
    fake.addImage(`${LOGO}.webp`, webp(500, 25))
    const clock = new Clock()
    const deps = makeDeps(fake, clock)
    await seedCard(clock)
    await env.DB.prepare(`UPDATE sets SET logo_url = ?1 WHERE id = 'fr:swsh3'`).bind(LOGO).run()
    await enqueueLow(clock, 'fr', 'swsh3-136')
    await enqueueLogo(clock, 'fr', 'swsh3')

    expect(await tick(deps)).toMatchObject({ done: 2, httpRequests: 2, bytesStored: 620 })
    expect(await one(`SELECT value FROM runtime_state WHERE key = 'ops:2026-11-13'`)).toEqual({ value: '4' })
    expect(await one(`SELECT http_requests, bytes_stored FROM tick_runs`)).toEqual({ http_requests: 2, bytes_stored: 620 })
  })

  it('plafond quotidien atteint : aucune copie secondaire ne part', async () => {
    const fake = new FakeTcgdex()
    fake.addImage(LOW_136, webp(120, 21))
    const clock = new Clock()
    const deps = makeDeps(fake, clock, { MAX_DAILY_OPERATIONS: '100' })
    await seedCard(clock)
    await enqueueLow(clock, 'fr', 'swsh3-136')
    await env.DB.prepare(`INSERT INTO runtime_state (key, value, updated_at) VALUES ('ops:2026-11-13', '100', ?1)`).bind(iso(clock.now())).run()
    expect(await tick(deps)).toMatchObject({ status: 'skipped', claimed: 0 })
    expect(fake.requests).toHaveLength(0)
  })

  it('symboles : comptés dans storedBytes (copiés seulement) ; MAX_STORED_BYTES atteint par eux : pause automatique, aucune requête', async () => {
    const fake = new FakeTcgdex()
    addSymbol(fake, `${SYMBOL}.webp`, webp(352, 41))
    const clock = new Clock()
    await seedSet(clock, 'fr', 'swsh4', LOGO, { logoStatus: 'downloaded', logoKey: 'cards/fr/swsh4/_logo.webp', logoSize: 500, symbolUrl: SYMBOL, symbolStatus: 'downloaded', symbolKey: 'cards/fr/swsh4/_symbol.webp', symbolSize: 352 })
    await seedSet(clock, 'fr', 'swsh5', null, { symbolUrl: SYMBOL, symbolStatus: 'not_found_on_cdn', symbolSize: 999 })
    expect(await storedBytes(db())).toBe(500 + 352)

    await seedSet(clock, 'fr', 'big', null, { symbolUrl: SYMBOL, symbolStatus: 'downloaded', symbolKey: 'cards/fr/big/_symbol.webp', symbolSize: 999_148 })
    await seedSet(clock, 'fr', 'swsh3', null, { symbolUrl: SYMBOL })
    await enqueueSymbol(clock, 'fr', 'swsh3')
    const deps = makeDeps(fake, clock, { MAX_STORED_BYTES: '1000000' })
    expect(await tick(deps)).toMatchObject({ status: 'skipped', reason: 'plafond de stockage atteint (1000000 octets) : pause automatique', claimed: 0 })
    expect(fake.requests).toHaveLength(0)
    expect(await one(`SELECT value FROM runtime_state WHERE key = 'paused'`)).toEqual({ value: '1' })
  })

  it('symboles : la requête et l’écriture R2 comptent dans les opérations du jour ; plafond atteint : aucun symbole ne part', async () => {
    const fake = new FakeTcgdex()
    addSymbol(fake, `${SYMBOL}.webp`, webp(352, 41))
    const clock = new Clock()
    const deps = makeDeps(fake, clock)
    await seedSet(clock, 'fr', 'swsh3', null, { symbolUrl: SYMBOL })
    await enqueueSymbol(clock, 'fr', 'swsh3')
    expect(await tick(deps)).toMatchObject({ done: 1, httpRequests: 1, bytesStored: 352 })
    expect(await one(`SELECT value FROM runtime_state WHERE key = 'ops:2026-11-13'`)).toEqual({ value: '2' })
    expect(await one(`SELECT http_requests, bytes_stored FROM tick_runs`)).toEqual({ http_requests: 1, bytes_stored: 352 })

    const other = `${ASSETS}/univ/swsh/swsh4/symbol`
    addSymbol(fake, `${other}.webp`, webp(352, 49))
    await seedSet(clock, 'fr', 'swsh4', null, { symbolUrl: other })
    await enqueueSymbol(clock, 'fr', 'swsh4')
    await env.DB.prepare(`UPDATE runtime_state SET value = '100' WHERE key = 'ops:2026-11-13'`).run()
    clock.advance(5 * MIN)
    expect(await tick(makeDeps(fake, clock, { MAX_DAILY_OPERATIONS: '100' }))).toMatchObject({ status: 'skipped', claimed: 0 })
    expect(fake.requests).toHaveLength(1)
    expect((await symbolRow('fr:swsh4')).symbol_status).toBeNull()
  })
})

// ——— Statut, erreurs et audit ———

describe('statut, erreurs et audit', () => {
  it('GET /admin/status (texte et JSON) : petites images et logos par langue et au total', async () => {
    const fake = catalogWithMedia()
    fake.images.delete(`${ASSETS}/ja/SV/SV1a/002/low.webp`)
    const clock = new Clock()
    const deps = makeDeps(fake, clock)
    await startSync(db(), deps.config, clock.now())
    await drain(deps, clock)

    const stats = (await (await admin(deps, fake, 'GET', '/admin/status?format=json')).json()) as Stats
    expect(stats.languages[0]).toMatchObject({ language: 'fr', lowImagesStored: 2, lowImagesMissing: 0, lowImagesErrors: 0, lowImagesPending: 0, lowBytes: 250, setsWithLogo: 1, logosStored: 1, logosPending: 0, logoBytes: 500 })
    expect(stats.languages[1]).toMatchObject({ language: 'ja', lowImagesStored: 1, lowImagesMissing: 1, lowBytes: 140, setsWithLogo: 0, logosStored: 0 })
    expect(stats.total).toMatchObject({ bytes: 1260, lowImagesStored: 3, logosStored: 1, mediaBytes: 250 + 140 + 500, storedBytes: 1260 + 250 + 140 + 500 })
    // Les tâches des copies secondaires sont comptées à part.
    expect(stats.jobs.filter((job) => job.job_type === 'image_low' || job.job_type === 'logo')).toEqual([
      { job_type: 'image_low', status: 'done', n: 7 },
      { job_type: 'logo', status: 'done', n: 1 },
    ])
    expect(stats.status).toBe('TERMINÉ')

    const text = await (await admin(deps, fake, 'GET', '/admin/status')).text()
    const sections = new Map(text.trimEnd().split('\n\n').map((block) => [block.split('\n')[0] ?? '', block]))
    expect(sections.get('FRANÇAIS')).toMatch(/^Petites images \(low\)\s*: 2 \/ 2 \(introuvables 0, erreurs 0, à faire 0\)$/m)
    expect(sections.get('FRANÇAIS')).toMatch(/^Logos d’extension\s*: 1 \/ 1 \(introuvables 0, erreurs 0, à faire 0\)$/m)
    expect(sections.get('JAPONAIS')).toMatch(/^Petites images \(low\)\s*: 1 \/ 2 \(introuvables 1, erreurs 0, à faire 0\)$/m)
    expect(sections.get('JAPONAIS')).toMatch(/^Logos d’extension\s*: 0 \/ 0 /m)
    expect(sections.get('TOTAL')).toMatch(/^Petites images R2\s*: 3$/m)
    expect(sections.get('TOTAL')).toMatch(/^Logos R2\s*: 1$/m)
    expect(sections.get('TOTAL')).toMatch(/^Volume stocké\s*: 0,000 Go$/m)
  })

  it('GET /admin/errors : une tâche de petite image en échec est signalée comme « image_low »', async () => {
    const fake = new FakeTcgdex()
    fake.images.set(LOW_136, { status: 500 })
    const clock = new Clock()
    const deps = makeDeps(fake, clock, { MAX_ATTEMPTS: '1' })
    await seedCard(clock)
    await enqueueLow(clock, 'fr', 'swsh3-136', 1)
    await tick(deps)
    const errors = (await (await admin(deps, fake, 'GET', '/admin/errors')).json()) as { failedJobs: { job_type: string; target_id: string; last_error: string }[] }
    expect(errors.failedJobs).toEqual([expect.objectContaining({ job_type: 'image_low', target_id: 'swsh3-136' })])
    expect(errors.failedJobs[0]?.last_error).toContain('HTTP 500')
    expect(await lowRow('fr:swsh3-136')).toMatchObject({ image_low_status: 'download_error' })
  })

  it('audit : objet de petite image ou de logo présent mais non enregistré → réconcilié ; enregistré mais disparu → recopié', async () => {
    const fake = catalogWithMedia()
    const clock = new Clock()
    const deps = makeDeps(fake, clock, { MIN_REQUEST_INTERVAL_MS: '50' })
    await startSync(db(), deps.config, clock.now())
    await drain(deps, clock)
    // La petite image de 137 et le logo ont disparu de R2 ; D1 a « oublié » celle de 136 (objet présent).
    await env.BUCKET.delete(['cards/fr/swsh3/swsh3-137.low.webp', LOGO_KEY])
    await env.DB.prepare(`UPDATE cards SET image_low_status = NULL, image_low_r2_key = NULL WHERE id = 'fr:swsh3-136'`).run()
    const requests = fake.count((url) => url.endsWith('.webp'))

    clock.advance(MIN)
    expect((await admin(deps, fake, 'POST', '/admin/reports/generate')).status).toBe(200)
    await drain(deps, clock)
    const summary = JSON.parse(await ((await env.BUCKET.get('reports/latest/summary.json')) as R2ObjectBody).text()) as { audit: Record<string, number> }
    expect(summary.audit).toMatchObject({ unknownObjects: 0, lowImageObjects: 3, logoObjects: 0, mediaReconciled: 1, mediaMissingObjects: 2 })

    expect(await lowRow('fr:swsh3-136')).toMatchObject({ image_low_status: 'downloaded', image_low_r2_key: LOW_KEY_136 })
    expect(await lowRow('fr:swsh3-137')).toMatchObject({ image_low_status: 'downloaded', image_low_r2_key: 'cards/fr/swsh3/swsh3-137.low.webp' })
    expect(await logoRow('fr:swsh3')).toMatchObject({ logo_status: 'downloaded', logo_r2_key: LOGO_KEY })
    expect(await env.BUCKET.head('cards/fr/swsh3/swsh3-137.low.webp')).not.toBeNull()
    expect(await env.BUCKET.head(LOGO_KEY)).not.toBeNull()
    // Seuls les deux objets disparus ont été retéléchargés (136 réconciliée sans requête).
    expect(fake.count((url) => url.endsWith('.webp'))).toBe(requests + 2)
  })

  it('GET /admin/status (texte et JSON) : symboles par langue et au total, comptés dans le volume ; tâches « symbol » à part', async () => {
    const fake = catalogWithSymbols()
    fake.images.delete(`${SYMBOL_JA}.webp`)
    const clock = new Clock()
    const deps = makeDeps(fake, clock)
    await startSync(db(), deps.config, clock.now())
    await drain(deps, clock)

    const stats = (await (await admin(deps, fake, 'GET', '/admin/status?format=json')).json()) as Stats
    expect(stats.languages[0]).toMatchObject({ language: 'fr', setsWithSymbol: 1, symbolsStored: 1, symbolsMissing: 0, symbolsErrors: 0, symbolsPending: 0, symbolBytes: 352 })
    expect(stats.languages[1]).toMatchObject({ language: 'ja', setsWithSymbol: 1, symbolsStored: 0, symbolsMissing: 1, symbolsErrors: 0, symbolsPending: 0, symbolBytes: 0 })
    // Images principales 1 260 ; petites images 250 (FR) + 290 (JA) ; logo 500 ; symbole 352.
    expect(stats.total).toMatchObject({ bytes: 1260, symbolsStored: 1, mediaBytes: 250 + 290 + 500 + 352, storedBytes: 1260 + 250 + 290 + 500 + 352 })
    expect(stats.total.storedBytes).toBe(await storedBytes(db()))
    expect(stats.jobs.filter((job) => job.job_type === 'symbol')).toEqual([{ job_type: 'symbol', status: 'done', n: 2 }])

    const text = await (await admin(deps, fake, 'GET', '/admin/status')).text()
    const sections = new Map(text.trimEnd().split('\n\n').map((block) => [block.split('\n')[0] ?? '', block]))
    // Symboles copiés / toutes les extensions de la langue (2 de chaque côté, une seule avec un symbole).
    expect(sections.get('FRANÇAIS')).toMatch(/^Symboles d’extension\s*: 1 \/ 2 \(introuvables 0, erreurs 0, à faire 0\)$/m)
    expect(sections.get('JAPONAIS')).toMatch(/^Symboles d’extension\s*: 0 \/ 2 \(introuvables 1, erreurs 0, à faire 0\)$/m)
    expect(sections.get('TOTAL')).toMatch(/^Symboles R2\s*: 1$/m)
  })

  it('GET /admin/errors : une tâche de symbole en échec est signalée comme « symbol »', async () => {
    const fake = new FakeTcgdex()
    fake.images.set(`${SYMBOL}.webp`, { status: 500 })
    const clock = new Clock()
    const deps = makeDeps(fake, clock, { MAX_ATTEMPTS: '1' })
    await seedSet(clock, 'fr', 'swsh3', null, { symbolUrl: SYMBOL })
    await enqueueSymbol(clock, 'fr', 'swsh3', 1)
    await tick(deps)
    const errors = (await (await admin(deps, fake, 'GET', '/admin/errors')).json()) as { failedJobs: { job_type: string; target_id: string; last_error: string }[] }
    expect(errors.failedJobs).toEqual([expect.objectContaining({ job_type: 'symbol', target_id: 'swsh3' })])
    expect(errors.failedJobs[0]?.last_error).toContain('HTTP 500')
    expect(await symbolRow('fr:swsh3')).toMatchObject({ symbol_status: 'download_error', symbol_r2_key: null })
  })

  it('audit : objet de symbole présent mais non enregistré → réconcilié sans requête ; enregistré mais disparu → recopié', async () => {
    const fake = catalogWithSymbols()
    const clock = new Clock()
    const deps = makeDeps(fake, clock, { MIN_REQUEST_INTERVAL_MS: '50' })
    await startSync(db(), deps.config, clock.now())
    await drain(deps, clock)
    // Le symbole FR a disparu de R2 ; D1 a « oublié » le symbole JA (objet présent).
    await env.BUCKET.delete(SYMBOL_KEY)
    await env.DB.prepare(`UPDATE sets SET symbol_status = NULL, symbol_r2_key = NULL WHERE id = 'ja:SV1a'`).run()
    const requests = fake.count((url) => url.endsWith('/symbol.webp'))

    clock.advance(MIN)
    expect((await admin(deps, fake, 'POST', '/admin/reports/generate')).status).toBe(200)
    await drain(deps, clock)
    const summary = JSON.parse(await ((await env.BUCKET.get('reports/latest/summary.json')) as R2ObjectBody).text()) as { audit: Record<string, number> }
    expect(summary.audit).toMatchObject({ unknownObjects: 0, lowImageObjects: 4, logoObjects: 1, symbolObjects: 1, mediaReconciled: 1, mediaMissingObjects: 1 })

    expect(await symbolRow('fr:swsh3')).toMatchObject({ symbol_status: 'downloaded', symbol_r2_key: SYMBOL_KEY, symbol_source_url: `${SYMBOL}.webp` })
    expect(await symbolRow('ja:SV1a')).toMatchObject({ symbol_status: 'downloaded', symbol_r2_key: SYMBOL_KEY_JA, symbol_source_url: `${SYMBOL_JA}.webp` })
    expect(await r2Bytes(SYMBOL_KEY)).toEqual(webp(352, 41))
    // Seul le symbole disparu a été retéléchargé (le japonais réconcilié sans requête).
    expect(fake.count((url) => url.endsWith('/symbol.webp'))).toBe(requests + 1)
  })
})

// ——— Schéma ———

describe('schéma (migration 0002)', () => {
  it('colonnes ajoutées, nulles par défaut ; seuls les statuts du vocabulaire sont acceptés (« downloaded » = présent)', async () => {
    const clock = new Clock()
    const { cardId } = await seedCard(clock)
    expect(await lowRow(cardId)).toEqual({ image_low_status: null, image_low_r2_key: null, image_low_file_size: null, image_low_sha256: null, image_low_checked_at: null, image_low_failure_reason: null })
    expect(await logoRow('fr:swsh3')).toEqual({ logo_status: null, logo_r2_key: null, logo_source_url: null, logo_file_size: null, logo_sha256: null, logo_failure_reason: null })
    for (const status of ['downloaded', 'not_found_on_cdn', 'missing_api_url', 'download_error', 'invalid_image']) {
      await env.DB.prepare(`UPDATE cards SET image_low_status = ?1 WHERE id = ?2`).bind(status, cardId).run()
      await env.DB.prepare(`UPDATE sets SET logo_status = ?1 WHERE id = 'fr:swsh3'`).bind(status).run()
    }
    for (const status of ['already_present', 'recovered_from_cdn', 'pending', 'présente']) {
      await expect(env.DB.prepare(`UPDATE cards SET image_low_status = ?1 WHERE id = ?2`).bind(status, cardId).run(), status).rejects.toThrow(/CHECK constraint failed/)
      await expect(env.DB.prepare(`UPDATE sets SET logo_status = ?1 WHERE id = 'fr:swsh3'`).bind(status).run(), status).rejects.toThrow(/CHECK constraint failed/)
    }
    // La contrainte CHECK de import_jobs.job_type est intacte (aucun nouveau type).
    await expect(
      env.DB.prepare(`INSERT INTO import_jobs (job_type, dedupe_key, priority, max_attempts, next_attempt_at, created_at, updated_at) VALUES ('logo', 'x', 6, 1, ?1, ?1, ?1)`)
        .bind(iso(clock.now()))
        .run(),
    ).rejects.toThrow(/CHECK constraint failed/)
  })
})

describe('schéma (migration 0003)', () => {
  it('colonnes symbol_* ajoutées à sets, nulles par défaut ; même vocabulaire que logo_status ; import_jobs.job_type intact', async () => {
    const clock = new Clock()
    await seedSet(clock, 'fr', 'swsh3', LOGO, { symbolUrl: SYMBOL })
    expect(await symbolRow('fr:swsh3')).toEqual({
      symbol_status: null,
      symbol_r2_key: null,
      symbol_source_url: null,
      symbol_file_size: null,
      symbol_sha256: null,
      symbol_checked_at: null,
      symbol_failure_reason: null,
    })
    type Column = { name: string; type: string; notnull: number; dflt_value: string | null }
    const added = (await rows<Column>(`PRAGMA table_info(sets)`)).filter((column) => column.name.startsWith('symbol_') && column.name !== 'symbol_url')
    expect(added.map((column) => [column.name, column.type, column.notnull, column.dflt_value])).toEqual([
      ['symbol_status', 'TEXT', 0, null],
      ['symbol_r2_key', 'TEXT', 0, null],
      ['symbol_checked_at', 'TEXT', 0, null],
      ['symbol_source_url', 'TEXT', 0, null],
      ['symbol_file_size', 'INTEGER', 0, null],
      ['symbol_sha256', 'TEXT', 0, null],
      ['symbol_failure_reason', 'TEXT', 0, null],
      // Ajoutée par 0005_set_symbol_provider.sql (symboles copiés par l'API d'import).
      ['symbol_provider', 'TEXT', 0, null],
    ])
    for (const status of ['downloaded', 'not_found_on_cdn', 'missing_api_url', 'download_error', 'invalid_image', null]) {
      await env.DB.prepare(`UPDATE sets SET symbol_status = ?1 WHERE id = 'fr:swsh3'`).bind(status).run()
    }
    for (const status of ['already_present', 'recovered_from_cdn', 'pending', 'présent']) {
      await expect(env.DB.prepare(`UPDATE sets SET symbol_status = ?1 WHERE id = 'fr:swsh3'`).bind(status).run(), status).rejects.toThrow(/CHECK constraint failed/)
    }
    // Aucun nouveau type de tâche : les symboles sont des tâches `image`.
    await expect(
      env.DB.prepare(`INSERT INTO import_jobs (job_type, dedupe_key, priority, max_attempts, next_attempt_at, created_at, updated_at) VALUES ('symbol', 'x', 6, 1, ?1, ?1, ?1)`)
        .bind(iso(clock.now()))
        .run(),
    ).rejects.toThrow(/CHECK constraint failed/)
  })
})
