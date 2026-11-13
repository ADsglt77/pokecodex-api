/**
 * Copie des images de cartes (TCGdex → Worker → R2) et gestion des erreurs HTTP :
 * `src/services/downloader.ts` et `src/tcgdex/client.ts`, au travers de l'exécuteur (`runTick`).
 *
 * Les cartes sont soit créées par une synchronisation sur le faux TCGdex, soit insérées directement
 * en base (série, extension, carte) avec leur tâche `image`. Aucun appel réseau réel : le faux
 * TCGdex (`FakeTcgdex`) ou un fetch simulé local répond à tout, et l'horloge est simulée.
 */

import { env } from 'cloudflare:workers'
import { beforeEach, describe, expect, it } from 'vitest'
import type { ConfigSource, Language } from '../src/config'
import { Db, Usage } from '../src/db'
import { enqueueStmts } from '../src/repositories/jobs'
import { imageJob, setJob } from '../src/services/catalog'
import { runTick, type RunnerDeps, type TickResult } from '../src/services/runner'
import { startSync } from '../src/services/sync'
import { Health, TcgdexClient } from '../src/tcgdex/client'
import { RateLimiter } from '../src/utils/limiter'
import { iso } from '../src/utils/time'
import { API, ASSETS, Clock, drain, FakeTcgdex, jsonResponse, makeDeps, md5Hex, one, resetAll, rows, sampleCatalog, sha256Hex, testConfig, webp } from './helpers'

beforeEach(resetAll)

// ——— Données de test ———

/** Base officielle (sans qualité) de la carte fr/swsh3-136, ses deux URL d'image et sa clé R2. */
const OFFICIAL = `${ASSETS}/fr/swsh/swsh3/136`
const HIGH = `${OFFICIAL}/high.webp`
const LOW = `${OFFICIAL}/low.webp`
const KEY = 'cards/fr/swsh3/swsh3-136.webp'
/** URL reconstruite et clé R2 de la carte fr/swsh3-137 (sans URL officielle). */
const REBUILT = `${ASSETS}/fr/swsh/swsh3/137/high.webp`
const REBUILT_KEY = 'cards/fr/swsh3/swsh3-137.webp'
/** Hôte que l'importateur n'a pas le droit de contacter. */
const EVIL_HOST = 'evil.example.com'

interface Seed {
  language?: Language
  /** Id TCGdex de la série de l'extension ; `null` : série inconnue. */
  series?: string | null
  /** Id TCGdex de l'extension. */
  set?: string
  localId?: string
  /** Base d'URL officielle donnée par l'API ; `null` ou absente : aucune. */
  image?: string | null
  imageStatus?: string
  imageR2Key?: string | null
}

/** Insère directement la série (si connue), l'extension et la carte. */
async function seedCard(clock: Clock, seed: Seed = {}): Promise<{ cardId: string; tcgdexId: string; language: Language }> {
  const language = seed.language ?? 'fr'
  const series = seed.series === undefined ? 'swsh' : seed.series
  const set = seed.set ?? 'swsh3'
  const localId = seed.localId ?? '136'
  const tcgdexId = `${set}-${localId}`
  const cardId = `${language}:${tcgdexId}`
  const now = iso(clock.now())
  const statements: D1PreparedStatement[] = []
  if (series) {
    statements.push(
      env.DB.prepare(`INSERT OR IGNORE INTO series (id, tcgdex_id, language, name, created_at, updated_at) VALUES (?1, ?2, ?3, ?4, ?5, ?5)`).bind(
        `${language}:${series}`,
        series,
        language,
        `Série ${series}`,
        now,
      ),
    )
  }
  statements.push(
    env.DB.prepare(`INSERT OR IGNORE INTO sets (id, tcgdex_id, series_id, language, name, created_at, updated_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?6)`).bind(
      `${language}:${set}`,
      set,
      series ? `${language}:${series}` : null,
      language,
      `Extension ${set}`,
      now,
    ),
    env.DB.prepare(
      `INSERT INTO cards (id, tcgdex_id, language, name, local_id, set_id, api_image_url, image_status, image_r2_key, created_at, updated_at)
       VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?10)`,
    ).bind(cardId, tcgdexId, language, `Carte ${tcgdexId}`, localId, `${language}:${set}`, seed.image ?? null, seed.imageStatus ?? 'pending', seed.imageR2Key ?? null, now),
  )
  await env.DB.batch(statements)
  return { cardId, tcgdexId, language }
}

/** Crée la tâche `image` d'une carte, prête tout de suite. */
async function enqueueImage(deps: RunnerDeps, clock: Clock, language: Language, tcgdexId: string): Promise<void> {
  const db = new Db(env.DB)
  await db.batch(enqueueStmts(db, 'image', language, [imageJob(language, tcgdexId)], { runId: null, maxAttempts: deps.config.maxAttempts, nowMs: clock.now() }))
}

/** Carte en base et sa tâche `image` prête. */
async function prepare(deps: RunnerDeps, clock: Clock, seed: Seed = {}): Promise<{ cardId: string; tcgdexId: string; language: Language }> {
  const card = await seedCard(clock, seed)
  await enqueueImage(deps, clock, card.language, card.tcgdexId)
  return card
}

// ——— Lectures ———

interface CardRow {
  image_status: string
  image_source: string | null
  image_source_url: string | null
  image_r2_key: string | null
  image_quality: string | null
  image_file_size: number | null
  image_sha256: string | null
  image_tested_urls: string | null
  image_http_status: number | null
  image_failure_reason: string | null
}

const cardRow = (cardId: string): Promise<CardRow> =>
  one<CardRow>(
    `SELECT image_status, image_source, image_source_url, image_r2_key, image_quality, image_file_size, image_sha256,
       image_tested_urls, image_http_status, image_failure_reason
     FROM cards WHERE id = ?1`,
    cardId,
  )

interface JobRow {
  status: string
  attempts: number
  max_attempts: number
  next_attempt_at: string
  last_error: string | null
  last_http_status: number | null
}

const jobRow = (language: Language, tcgdexId: string): Promise<JobRow> =>
  one<JobRow>(`SELECT status, attempts, max_attempts, next_attempt_at, last_error, last_http_status FROM import_jobs WHERE dedupe_key = ?1`, `image:${language}:${tcgdexId}`)

const testedUrls = (row: CardRow): unknown => JSON.parse(row.image_tested_urls ?? 'null')

async function stateValue(key: string): Promise<string | null> {
  return (await rows<{ value: string | null }>(`SELECT value FROM runtime_state WHERE key = ?1`, key))[0]?.value ?? null
}

async function r2Bytes(key: string): Promise<Uint8Array | null> {
  const object = await env.BUCKET.get(key)
  return object ? new Uint8Array(await object.arrayBuffer()) : null
}

const tick = (deps: RunnerDeps, budgetMs?: number): Promise<TickResult> => runTick(deps, budgetMs === undefined ? { trigger: 'test' } : { trigger: 'test', budgetMs })

// ——— Réponses et fetch simulés ———

const urlOf = (input: RequestInfo | URL): string => (typeof input === 'string' ? input : input instanceof URL ? input.href : input.url)

/**
 * Réponse d'image comme celles du CDN : type, longueur et ETag (= MD5) cohérents, sauf en-têtes
 * imposés (`null` : en-tête retiré).
 */
async function imageResponse(body: Uint8Array, headers: Record<string, string | null> = {}): Promise<Response> {
  const all = new Headers({ 'content-type': 'image/webp', 'content-length': String(body.byteLength), etag: `"${await md5Hex(body)}"` })
  for (const [name, value] of Object.entries(headers)) {
    if (value === null) all.delete(name)
    else all.set(name, value)
  }
  return new Response(body, { status: 200, headers: all })
}

/** Corps lu en flux, par morceaux, sans `Content-Length`. */
const streamOf = (bytes: Uint8Array): ReadableStream<Uint8Array> =>
  new ReadableStream<Uint8Array>({
    start(controller) {
      for (let i = 0; i < bytes.byteLength; i += 500) controller.enqueue(bytes.slice(i, i + 500))
      controller.close()
    },
  })

/** Enregistre chaque appel (URL et options) avant de le passer à `inner`. */
function spyFetcher(inner: typeof fetch): { calls: { url: string; init: Parameters<typeof fetch>[1] }[]; fetcher: typeof fetch } {
  const calls: { url: string; init: Parameters<typeof fetch>[1] }[] = []
  const fetcher: typeof fetch = (input, init) => {
    calls.push({ url: urlOf(input), init })
    return inner(input, init)
  }
  return { calls, fetcher }
}

/**
 * Fetch simulé qui suit les redirections comme le `fetch` de workerd quand `redirect` vaut
 * `follow` (valeur par défaut) : sans aucun contrôle de l'hôte de destination.
 */
function redirectingFetcher(routes: Map<string, () => Promise<Response>>, calls: string[]): typeof fetch {
  return async (input, init) => {
    let url = urlOf(input)
    for (let hop = 0; hop <= 20; hop++) {
      calls.push(url)
      const response = (await routes.get(url)?.()) ?? new Response('absent', { status: 404 })
      const location = response.headers.get('location')
      if (response.status >= 300 && response.status < 400 && location && (init?.redirect ?? 'follow') === 'follow') {
        url = new URL(location, url).href
        continue
      }
      return response
    }
    throw new TypeError('trop de redirections')
  }
}

/** Client TCGdex isolé (hors exécuteur), sur une horloge simulée. */
function directClient(fetcher: typeof fetch, overrides: ConfigSource = {}, quickRetryDelaysMs: number[] = []): { client: TcgdexClient; usage: Usage } {
  const clock = new Clock()
  const config = testConfig(overrides)
  const usage = new Usage()
  const client = new TcgdexClient({
    config,
    fetcher,
    limiter: new RateLimiter(config.minRequestIntervalMs, clock.now, clock.wait),
    usage,
    health: new Health(),
    now: clock.now,
    deadlineMs: clock.now() + config.runTimeBudgetMs,
    wait: clock.wait,
    quickRetryDelaysMs,
  })
  return { client, usage }
}

/** Bucket R2 de test dont `head` est remplacé (lecture qui lève, objet illisible…). */
function bucketWithHead(head: (key: string) => Promise<unknown>): R2Bucket {
  return {
    head,
    put: (key: string, value: Uint8Array, options?: R2PutOptions) => env.BUCKET.put(key, value, options),
  } as unknown as R2Bucket
}

// ——— Téléchargements corrompus ou invalides ———

interface Corruption {
  name: string
  /** Transfert corrompu (nouvelle tentative) ou fichier invalide (définitif). */
  transient: boolean
  /** Extrait attendu du motif enregistré. */
  reason: string
  overrides?: ConfigSource
  respond: () => Promise<Response>
}

function truncatedWebp(): Uint8Array {
  const bytes = webp(300, 5)
  new DataView(bytes.buffer).setUint32(4, 1_000, true) // taille RIFF annoncée > taille reçue
  return bytes
}

/** MD5 d'un contenu vide (ETag que donnerait le CDN pour un fichier source vide). */
const EMPTY_MD5 = 'd41d8cd98f00b204e9800998ecf8427e'
const WRONG_ETAG = '"00112233445566778899aabbccddeeff"'

function pngBytes(): Uint8Array {
  const bytes = new Uint8Array(300)
  bytes.set([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a])
  return bytes
}

const CORRUPTIONS: Corruption[] = [
  {
    name: 'réponse 200 en text/html',
    transient: false,
    reason: 'type MIME inattendu',
    respond: () => imageResponse(new TextEncoder().encode(`<html><body>${'x'.repeat(300)}</body></html>`), { 'content-type': 'text/html; charset=utf-8' }),
  },
  { name: 'signature non WebP (PNG servi en image/webp)', transient: false, reason: 'signature WebP absente', respond: () => imageResponse(pngBytes()) },
  { name: 'fichier trop petit', transient: false, reason: 'trop petit', respond: () => imageResponse(webp(50)) },
  {
    name: 'fichier trop gros (Content-Length annoncé)',
    transient: false,
    reason: 'trop volumineux',
    overrides: { MAX_IMAGE_BYTES: '1000' },
    respond: () => imageResponse(webp(2_000)),
  },
  {
    name: 'fichier trop gros (lu en flux, sans Content-Length)',
    transient: false,
    reason: 'trop volumineux',
    overrides: { MAX_IMAGE_BYTES: '1000' },
    respond: async () => new Response(streamOf(webp(2_000)), { status: 200, headers: { 'content-type': 'image/webp' } }),
  },
  // Intégrité du transfert prouvée (ETag = MD5 du contenu reçu, longueur annoncée exacte, sans
  // encodage) : le fichier source lui-même est vide ou tronqué, inutile de le retélécharger.
  { name: 'RIFF tronqué, transfert intègre (ETag = MD5, Content-Length exact)', transient: false, reason: 'tronqué', respond: () => imageResponse(truncatedWebp()) },
  {
    name: 'contenu vide, transfert intègre (ETag = MD5 du vide, Content-Length: 0)',
    transient: false,
    reason: 'contenu vide',
    respond: async () => {
      const response = await imageResponse(new Uint8Array(0))
      expect(response.headers.get('etag')).toBe(`"${EMPTY_MD5}"`)
      return response
    },
  },
  // Intégrité non prouvée : transfert probablement interrompu, à retenter.
  { name: 'RIFF tronqué sans ETag', transient: true, reason: 'tronqué', respond: () => imageResponse(truncatedWebp(), { etag: null }) },
  { name: 'RIFF tronqué, ETag qui n’est pas un MD5', transient: true, reason: 'tronqué', respond: () => imageResponse(truncatedWebp(), { etag: '"5f2b1c-1a2b"' }) },
  {
    name: 'RIFF tronqué, ETag = MD5 mais longueur inconnue (flux sans Content-Length)',
    transient: true,
    reason: 'tronqué',
    respond: async () => new Response(streamOf(truncatedWebp()), { status: 200, headers: { 'content-type': 'image/webp', etag: `"${await md5Hex(truncatedWebp())}"` } }),
  },
  { name: 'RIFF tronqué, ETag = MD5 mais Content-Encoding: gzip', transient: true, reason: 'tronqué', respond: () => imageResponse(truncatedWebp(), { 'content-encoding': 'gzip' }) },
  { name: 'contenu vide sans ETag', transient: true, reason: 'contenu vide', respond: () => imageResponse(new Uint8Array(0), { etag: null }) },
  { name: 'MD5 ≠ ETag', transient: true, reason: 'ETag', respond: () => imageResponse(webp(300, 5), { etag: WRONG_ETAG }) },
  // L'ETag est comparé avant les contrôles « vide » et « tronqué ».
  { name: 'RIFF tronqué et MD5 ≠ ETag (ETag contrôlé d’abord)', transient: true, reason: 'ETag', respond: () => imageResponse(truncatedWebp(), { etag: WRONG_ETAG }) },
  { name: 'contenu vide et MD5 ≠ ETag (ETag contrôlé d’abord)', transient: true, reason: 'ETag', respond: () => imageResponse(new Uint8Array(0), { etag: WRONG_ETAG }) },
]

// ——— Tests ———

describe('copie d’une image officielle', () => {
  it('image valide : objet R2 identique, métadonnées R2 et carte « downloaded »', async () => {
    const fake = new FakeTcgdex()
    const body = webp(300, 1)
    fake.addImage(HIGH, body)
    const clock = new Clock()
    const deps = makeDeps(fake, clock)
    const { cardId, tcgdexId } = await prepare(deps, clock, { image: OFFICIAL })

    const result = await tick(deps)
    expect(result).toMatchObject({ status: 'ran', done: 1, retried: 0, failed: 0, released: 0, bytesStored: 300 })

    const object = await env.BUCKET.get(KEY)
    expect(object).not.toBeNull()
    const stored = object as R2ObjectBody
    expect(new Uint8Array(await stored.arrayBuffer())).toEqual(body)
    expect(stored.httpMetadata?.contentType).toBe('image/webp')
    const sha256 = await sha256Hex(body)
    expect(stored.customMetadata).toMatchObject({ source: 'api', sourceUrl: HIGH, quality: 'high', sha256, md5: await md5Hex(body) })

    expect(await cardRow(cardId)).toEqual({
      image_status: 'downloaded',
      image_source: 'api',
      image_source_url: HIGH,
      image_r2_key: KEY,
      image_quality: 'high',
      image_file_size: 300,
      image_sha256: sha256,
      image_tested_urls: JSON.stringify([{ url: HIGH, status: 200 }]),
      image_http_status: 200,
      image_failure_reason: null,
    })
    expect(await jobRow('fr', tcgdexId)).toMatchObject({ status: 'done', attempts: 1, last_http_status: 200 })
    // La qualité de secours n'est pas demandée quand la principale existe.
    expect(fake.count((url) => url === LOW)).toBe(0)
  })

  it('high.webp absente (404) puis low.webp présente : « downloaded » en qualité low, les deux URL testées', async () => {
    const fake = new FakeTcgdex()
    const body = webp(200, 2)
    fake.addImage(LOW, body)
    const clock = new Clock()
    const deps = makeDeps(fake, clock)
    const { cardId } = await prepare(deps, clock, { image: OFFICIAL })

    await tick(deps)

    const card = await cardRow(cardId)
    expect(card).toMatchObject({ image_status: 'downloaded', image_source: 'api', image_source_url: LOW, image_quality: 'low', image_file_size: 200, image_r2_key: KEY })
    expect(testedUrls(card)).toEqual([
      { url: HIGH, status: 404 },
      { url: LOW, status: 200 },
    ])
    expect(await r2Bytes(KEY)).toEqual(body)
    expect((await env.BUCKET.head(KEY))?.customMetadata?.quality).toBe('low')
  })

  it('URL officielle absente en high et en low : « not_found_on_cdn », motif de l’URL officielle, sans nouvelle tentative', async () => {
    const fake = new FakeTcgdex()
    const clock = new Clock()
    const deps = makeDeps(fake, clock)
    const { cardId, tcgdexId } = await prepare(deps, clock, { image: OFFICIAL })

    await drain(deps, clock)

    const card = await cardRow(cardId)
    expect(card).toMatchObject({ image_status: 'not_found_on_cdn', image_http_status: 404, image_r2_key: null })
    expect(card.image_failure_reason).toContain('URL officielle')
    expect(testedUrls(card)).toEqual([
      { url: HIGH, status: 404 },
      { url: LOW, status: 404 },
    ])
    expect(fake.requests.map((request) => request.url)).toEqual([HIGH, LOW])
    expect(await jobRow('fr', tcgdexId)).toMatchObject({ status: 'done', attempts: 1 })
  })
})

describe('URL reconstruite et informations manquantes', () => {
  it('URL reconstruite trouvée sur le CDN : « recovered_from_cdn »', async () => {
    const fake = new FakeTcgdex()
    const body = webp(310, 2)
    fake.addImage(REBUILT, body)
    const clock = new Clock()
    const deps = makeDeps(fake, clock)
    const { cardId } = await prepare(deps, clock, { localId: '137' })

    await tick(deps)

    expect(await cardRow(cardId)).toMatchObject({
      image_status: 'recovered_from_cdn',
      image_source: 'cdn_reconstructed',
      image_source_url: REBUILT,
      image_r2_key: REBUILT_KEY,
      image_quality: 'high',
      image_file_size: 310,
      image_sha256: await sha256Hex(body),
    })
    expect(await r2Bytes(REBUILT_KEY)).toEqual(body)
    expect((await env.BUCKET.head(REBUILT_KEY))?.customMetadata).toMatchObject({ source: 'cdn_reconstructed', sourceUrl: REBUILT })
    expect(fake.requests.map((request) => request.url)).toEqual([REBUILT])
  })

  it('URL reconstruite introuvable (404) : « not_found_on_cdn », une seule requête, motif et code 404 enregistrés', async () => {
    const fake = new FakeTcgdex()
    const clock = new Clock()
    const deps = makeDeps(fake, clock)
    const { cardId, tcgdexId } = await prepare(deps, clock, { localId: '137' })

    // Comme le Cron : plusieurs exécutions, aucune ne doit retenter l'URL.
    const ticks = await drain(deps, clock)
    expect(ticks.at(-1)?.status).toBe('idle')

    const card = await cardRow(cardId)
    expect(card).toMatchObject({ image_status: 'not_found_on_cdn', image_http_status: 404, image_r2_key: null, image_sha256: null })
    expect(card.image_failure_reason).toBe('URL reconstruite introuvable sur le CDN (404)')
    expect(testedUrls(card)).toEqual([{ url: REBUILT, status: 404 }])
    expect(fake.requests).toHaveLength(1)
    expect(await jobRow('fr', tcgdexId)).toMatchObject({ status: 'done', attempts: 1, last_http_status: 404 })
    expect(await env.BUCKET.head(REBUILT_KEY)).toBeNull()
  })

  it('ni URL officielle ni série connue : « missing_api_url » sans aucune requête', async () => {
    const fake = new FakeTcgdex()
    const clock = new Clock()
    const deps = makeDeps(fake, clock)
    const { cardId, tcgdexId } = await prepare(deps, clock, { series: null, localId: '137' })

    await drain(deps, clock)

    const card = await cardRow(cardId)
    expect(card).toMatchObject({ image_status: 'missing_api_url', image_r2_key: null, image_http_status: null })
    expect(card.image_failure_reason).toContain('reconstruction impossible')
    // Aucune URL testée : l'historique (vide ici) est conservé tel quel, sans liste vide écrite.
    expect(card.image_tested_urls).toBeNull()
    expect(fake.requests).toHaveLength(0)
    expect(await jobRow('fr', tcgdexId)).toMatchObject({ status: 'done', attempts: 1 })
  })

  it('série inconnue mais tâche « set » de l’extension en attente : tâche rendue sans consommer de tentative, puis copie une fois la série connue', async () => {
    const fake = new FakeTcgdex()
    const body = webp(310, 2)
    fake.addImage(REBUILT, body)
    const clock = new Clock()
    const deps = makeDeps(fake, clock)
    const { cardId, tcgdexId } = await prepare(deps, clock, { series: null, localId: '137' })
    // Détail de l'extension attendu (tâche en attente, pas encore prête).
    const db = new Db(env.DB)
    await db.batch(enqueueStmts(db, 'set', 'fr', [setJob('fr', 'swsh3')], { runId: null, maxAttempts: deps.config.maxAttempts, nowMs: clock.now(), notBeforeMs: clock.now() + 3_600_000 }))
    const t0 = clock.now()

    const result = await tick(deps)
    expect(result).toMatchObject({ status: 'ran', claimed: 1, released: 1, done: 0, retried: 0 })
    const job = await jobRow('fr', tcgdexId)
    expect(job).toMatchObject({ status: 'pending', attempts: 0 })
    expect(Date.parse(job.next_attempt_at)).toBeGreaterThan(t0)
    expect((await cardRow(cardId)).image_status).toBe('pending')
    expect(fake.requests).toHaveLength(0)

    // Avant l'heure de reprise : rien à faire.
    expect((await tick(deps)).status).toBe('idle')

    // Le détail de l'extension est arrivé : la série est connue, l'URL reconstruite est essayée.
    await env.DB.batch([
      env.DB.prepare(`INSERT INTO series (id, tcgdex_id, language, name, created_at, updated_at) VALUES ('fr:swsh', 'swsh', 'fr', 'Épée et Bouclier', ?1, ?1)`).bind(iso(t0)),
      env.DB.prepare(`UPDATE sets SET series_id = 'fr:swsh' WHERE id = 'fr:swsh3'`),
      env.DB.prepare(`UPDATE import_jobs SET status = 'done' WHERE dedupe_key = 'set:fr:swsh3'`),
    ])
    clock.t = Date.parse(job.next_attempt_at) + 1_000
    await tick(deps)
    expect(await cardRow(cardId)).toMatchObject({ image_status: 'recovered_from_cdn', image_r2_key: REBUILT_KEY })
    expect(await jobRow('fr', tcgdexId)).toMatchObject({ status: 'done', attempts: 1 })
  })
})

describe('HTTP 429', () => {
  it('429 avec Retry-After: 120 : tâche rendue sans tentative consommée, attente imposée, exécution suivante sautée, puis reprise', async () => {
    const fake = new FakeTcgdex()
    const body = webp(300, 1)
    fake.addImage(HIGH, body)
    fake.queue(HIGH, () => new Response(null, { status: 429, headers: { 'retry-after': '120' } }))
    const clock = new Clock()
    const deps = makeDeps(fake, clock)
    const { cardId, tcgdexId } = await prepare(deps, clock, { image: OFFICIAL })
    const t0 = clock.now()

    const first = await tick(deps)
    expect(first).toMatchObject({ status: 'ran', released: 1, retried: 0, failed: 0, done: 0 })
    expect(first.reason).toContain('429')
    const job = await jobRow('fr', tcgdexId)
    expect(job).toMatchObject({ status: 'pending', attempts: 0, last_http_status: 429 })
    expect(Date.parse(job.next_attempt_at) - t0).toBeGreaterThanOrEqual(120_000)
    expect((await cardRow(cardId)).image_status).toBe('pending')
    // Ni la qualité de secours ni une nouvelle requête après un 429.
    expect(fake.requests.map((request) => request.url)).toEqual([HIGH])

    const backoffUntil = Date.parse((await stateValue('backoff_until')) ?? '')
    expect(backoffUntil - t0).toBeGreaterThanOrEqual(120_000)
    expect(backoffUntil - t0).toBeLessThan(125_000)
    expect(await stateValue('backoff_reason')).toContain('429')

    // Avant la fin de l'attente : exécution sautée, aucune requête.
    clock.t = t0 + 60_000
    const second = await tick(deps)
    expect(second.status).toBe('skipped')
    expect(second.reason).toMatch(/^attente imposée/)
    expect(second.httpRequests).toBe(0)
    expect(fake.requests).toHaveLength(1)

    // Après l'attente : tout reprend.
    clock.t = Math.max(backoffUntil, Date.parse(job.next_attempt_at)) + 1_000
    const third = await tick(deps)
    expect(third).toMatchObject({ status: 'ran', done: 1 })
    expect(await cardRow(cardId)).toMatchObject({ image_status: 'downloaded', image_quality: 'high' })
    expect(await r2Bytes(KEY)).toEqual(body)
    expect(await stateValue('consecutive_429')).toBeNull()
  })

  it('Retry-After: 86400 (24 h) respecté au-delà de RATE_LIMIT_MAX_BACKOFF_MS : attente globale et reprise de la tâche alignées', async () => {
    const fake = new FakeTcgdex()
    const body = webp(300, 1)
    fake.addImage(HIGH, body)
    fake.queue(HIGH, () => new Response(null, { status: 429, headers: { 'retry-after': '86400' } }))
    const clock = new Clock()
    const deps = makeDeps(fake, clock)
    expect(deps.config.rateLimitMaxBackoffMs).toBeLessThan(86_400_000)
    const { cardId, tcgdexId } = await prepare(deps, clock, { image: OFFICIAL })
    const t0 = clock.now()

    expect(await tick(deps)).toMatchObject({ status: 'ran', released: 1, retried: 0, failed: 0 })
    const backoffUntil = Date.parse((await stateValue('backoff_until')) ?? '')
    expect(backoffUntil - t0).toBeGreaterThanOrEqual(86_400_000)
    expect(backoffUntil - t0).toBeLessThan(86_400_000 + 5_000)
    const job = await jobRow('fr', tcgdexId)
    expect(job).toMatchObject({ status: 'pending', attempts: 0, last_http_status: 429 })
    // La tâche reprend en même temps que l'importation, ni avant ni bien après.
    expect(Math.abs(Date.parse(job.next_attempt_at) - backoffUntil)).toBeLessThanOrEqual(1_000)

    // Passé l'ancien plafond (RATE_LIMIT_MAX_BACKOFF_MS) : toujours en attente, aucune requête.
    clock.t = t0 + deps.config.rateLimitMaxBackoffMs + 60_000
    expect((await tick(deps)).status).toBe('skipped')
    expect(fake.requests).toHaveLength(1)

    clock.t = Math.max(backoffUntil, Date.parse(job.next_attempt_at)) + 1_000
    expect(await tick(deps)).toMatchObject({ status: 'ran', done: 1 })
    expect(await cardRow(cardId)).toMatchObject({ image_status: 'downloaded', image_quality: 'high' })
    expect(await r2Bytes(KEY)).toEqual(body)
  })

  // Horloge simulée : vendredi 13 novembre 2026, 10:00:00 UTC. Délai `null` : Retry-After illisible,
  // attente par défaut (RATE_LIMIT_BACKOFF_MS, ici 300 s). RATE_LIMIT_MAX_BACKOFF_MS (400 s) ne
  // plafonne que l'attente calculée sans en-tête, jamais un Retry-After lisible.
  it.each([
    { header: '600', delayMs: 600_000, why: 'secondes entières, au-delà de RATE_LIMIT_MAX_BACKOFF_MS' },
    { header: '9999999999', delayMs: 86_400_000, why: '10 chiffres, borné à 24 h' },
    { header: 'Fri, 13 Nov 2026 10:10:00 GMT', delayMs: 600_000, why: 'date IMF-fixdate' },
    { header: 'Sat, 13 Nov 2027 10:00:00 GMT', delayMs: 86_400_000, why: 'date lointaine, bornée à 24 h' },
    { header: 'Fri, 13 Nov 2026 09:00:00 GMT', delayMs: 0, why: 'date passée' },
    { header: '12345678901', delayMs: null, why: '11 chiffres' },
    { header: '1.5', delayMs: null, why: 'secondes non entières' },
    { header: '-1', delayMs: null, why: 'négatif' },
    { header: '+60', delayMs: null, why: 'signe' },
    { header: '1e3', delayMs: null, why: 'notation scientifique' },
    { header: 'demain', delayMs: null, why: 'texte libre' },
    { header: '2026-11-13T10:10:00Z', delayMs: null, why: 'date ISO 8601, pas une date HTTP' },
    { header: 'Friday, 13-Nov-26 10:10:00 GMT', delayMs: null, why: 'ancien format RFC 850' },
  ])('429 avec Retry-After « $header » ($why) : attente globale et reprise de la tâche', async ({ header, delayMs }) => {
    const fake = new FakeTcgdex()
    fake.addImage(HIGH, webp(300, 1))
    fake.queue(HIGH, () => new Response(null, { status: 429, headers: { 'retry-after': header } }))
    const clock = new Clock()
    const deps = makeDeps(fake, clock, { RATE_LIMIT_BACKOFF_MS: '300000', RATE_LIMIT_MAX_BACKOFF_MS: '400000' })
    const { cardId, tcgdexId } = await prepare(deps, clock, { image: OFFICIAL })
    const t0 = clock.now()
    expect(t0).toBe(Date.parse('Fri, 13 Nov 2026 10:00:00 GMT'))
    const expected = delayMs ?? 300_000

    expect(await tick(deps)).toMatchObject({ status: 'ran', released: 1, retried: 0, failed: 0, done: 0 })
    const backoffUntil = Date.parse((await stateValue('backoff_until')) ?? '')
    expect(backoffUntil - t0).toBeGreaterThanOrEqual(expected)
    expect(backoffUntil - t0).toBeLessThan(expected + 5_000)
    const job = await jobRow('fr', tcgdexId)
    expect(job).toMatchObject({ status: 'pending', attempts: 0, last_http_status: 429 })
    expect(Date.parse(job.next_attempt_at) - t0).toBeGreaterThanOrEqual(expected)
    expect(Date.parse(job.next_attempt_at) - t0).toBeLessThan(expected + 5_000)
    expect((await cardRow(cardId)).image_status).toBe('pending')
    expect(fake.requests.map((request) => request.url)).toEqual([HIGH])
  })
})

describe('erreurs passagères et nouvelles tentatives', () => {
  it('HTTP 503 répété : nouvelle tentative différée, puis « failed » et carte « download_error » après MAX_ATTEMPTS = 2', async () => {
    const fake = new FakeTcgdex()
    fake.images.set(HIGH, { status: 503 })
    const clock = new Clock()
    const deps = makeDeps(fake, clock, { MAX_ATTEMPTS: '2' })
    const { cardId, tcgdexId } = await prepare(deps, clock, { image: OFFICIAL })
    const t0 = clock.now()

    expect(await tick(deps)).toMatchObject({ status: 'ran', retried: 1, failed: 0 })
    const job = await jobRow('fr', tcgdexId)
    expect(job).toMatchObject({ status: 'pending', attempts: 1, max_attempts: 2, last_http_status: 503 })
    expect(job.last_error).toContain('HTTP 503')
    expect(Date.parse(job.next_attempt_at)).toBeGreaterThan(t0)
    expect((await cardRow(cardId)).image_status).toBe('pending')

    // Tentative différée : rien avant son heure.
    expect((await tick(deps)).status).toBe('idle')
    expect(fake.requests).toHaveLength(1)

    clock.t = Date.parse(job.next_attempt_at) + 1_000
    expect(await tick(deps)).toMatchObject({ status: 'ran', failed: 1, retried: 0 })
    const failed = await jobRow('fr', tcgdexId)
    expect(failed).toMatchObject({ status: 'failed', attempts: 2, last_http_status: 503 })
    expect(failed.last_error).toBe('image fr:swsh3-136 : HTTP 503 (après 2 tentative(s))')
    const card = await cardRow(cardId)
    expect(card).toMatchObject({ image_status: 'download_error', image_http_status: 503, image_failure_reason: 'HTTP 503', image_r2_key: null })
    expect(testedUrls(card)).toEqual([{ url: HIGH, status: 503, error: 'HTTP 503' }])
    // Une erreur serveur n'est pas une absence : la qualité de secours n'est jamais demandée.
    expect(fake.requests.map((request) => request.url)).toEqual([HIGH, HIGH])
    expect(await env.BUCKET.head(KEY)).toBeNull()

    // Plus rien ensuite.
    clock.advance(86_400_000)
    expect((await tick(deps)).status).toBe('idle')
    expect(fake.requests).toHaveLength(2)
  })

  it('HTTP 503 répété : délais des nouvelles tentatives croissants (next_attempt_at croissant)', async () => {
    const fake = new FakeTcgdex()
    fake.images.set(HIGH, { status: 503 })
    const clock = new Clock()
    const deps = makeDeps(fake, clock, { MAX_ATTEMPTS: '3' })
    const { cardId, tcgdexId } = await prepare(deps, clock, { image: OFFICIAL })

    const delays: number[] = []
    const nexts: number[] = []
    for (let i = 0; i < 2; i++) {
      const at = clock.now()
      expect(await tick(deps)).toMatchObject({ retried: 1 })
      const job = await jobRow('fr', tcgdexId)
      expect(job).toMatchObject({ status: 'pending', attempts: i + 1 })
      const next = Date.parse(job.next_attempt_at)
      delays.push(next - at)
      nexts.push(next)
      clock.t = next + 1_000
    }
    const [d1, d2] = delays as [number, number]
    const [n1, n2] = nexts as [number, number]
    expect(n2).toBeGreaterThan(n1)
    expect(d1).toBeGreaterThan(0)
    expect(d2).toBeGreaterThan(d1)

    expect(await tick(deps)).toMatchObject({ failed: 1 })
    expect((await jobRow('fr', tcgdexId)).status).toBe('failed')
    expect((await cardRow(cardId)).image_status).toBe('download_error')
  })

  it('erreur réseau (fetch qui lève) : nouvelle tentative, jamais « not_found_on_cdn »', async () => {
    const fake = new FakeTcgdex()
    const fail = (): Response => {
      throw new TypeError('connexion réinitialisée')
    }
    fake.queue(REBUILT, fail, fail)
    const clock = new Clock()
    const deps = makeDeps(fake, clock, { MAX_ATTEMPTS: '2' })
    const { cardId, tcgdexId } = await prepare(deps, clock, { localId: '137' })

    expect(await tick(deps)).toMatchObject({ retried: 1 })
    const job = await jobRow('fr', tcgdexId)
    expect(job).toMatchObject({ status: 'pending', attempts: 1, last_http_status: null })
    expect(job.last_error).toContain('erreur réseau')
    expect((await cardRow(cardId)).image_status).toBe('pending')

    clock.t = Date.parse(job.next_attempt_at) + 1_000
    expect(await tick(deps)).toMatchObject({ failed: 1 })
    const card = await cardRow(cardId)
    expect(card.image_status).toBe('download_error')
    expect(card.image_status).not.toBe('not_found_on_cdn')
    expect(card.image_http_status).toBeNull()
    expect(card.image_failure_reason).toContain('erreur réseau')
    expect(fake.count((url) => url === REBUILT)).toBe(2)
  })

  it('erreur réseau puis image disponible : copiée à la tentative suivante', async () => {
    const fake = new FakeTcgdex()
    const body = webp(310, 2)
    fake.addImage(REBUILT, body)
    fake.queue(REBUILT, () => {
      throw new TypeError('délai dépassé')
    })
    const clock = new Clock()
    const deps = makeDeps(fake, clock)
    const { cardId, tcgdexId } = await prepare(deps, clock, { localId: '137' })

    await tick(deps)
    expect((await cardRow(cardId)).image_status).toBe('pending')
    clock.t = Date.parse((await jobRow('fr', tcgdexId)).next_attempt_at) + 1_000
    await tick(deps)
    expect(await cardRow(cardId)).toMatchObject({ image_status: 'recovered_from_cdn', image_r2_key: REBUILT_KEY })
    expect(await r2Bytes(REBUILT_KEY)).toEqual(body)
  })

  it('série d’erreurs serveur (ERROR_STREAK_LIMIT) : exécution arrêtée, tâches restantes rendues, pause automatique', async () => {
    const fake = new FakeTcgdex()
    const clock = new Clock()
    const deps = makeDeps(fake, clock, { ERROR_STREAK_LIMIT: '3', MAX_CONCURRENCY: '1' })
    for (const localId of ['101', '102', '103', '104', '105']) {
      fake.images.set(`${ASSETS}/fr/swsh/swsh3/${localId}/high.webp`, { status: 503 })
      await prepare(deps, clock, { localId, image: `${ASSETS}/fr/swsh/swsh3/${localId}` })
    }

    const result = await tick(deps)
    expect(result).toMatchObject({ status: 'ran', claimed: 5, retried: 3, released: 2 })
    expect(fake.requests).toHaveLength(3)
    const backoffUntil = Date.parse((await stateValue('backoff_until')) ?? '')
    expect(backoffUntil - clock.now()).toBeGreaterThan(deps.config.errorStreakBackoffMs - 10_000)
    expect(backoffUntil - clock.now()).toBeLessThanOrEqual(deps.config.errorStreakBackoffMs)
    expect(await stateValue('backoff_reason')).toContain('erreurs serveur')
    const released = await rows<{ attempts: number }>(`SELECT attempts FROM import_jobs WHERE status = 'pending' AND attempts = 0`)
    expect(released).toHaveLength(2)

    expect((await tick(deps)).status).toBe('skipped')
    expect(fake.requests).toHaveLength(3)
  })
})

describe('relances courtes du client', () => {
  it('quickRetryDelaysMs: [10] : un 503 unique suivi d’un 200 aboutit dans la même exécution', async () => {
    const fake = new FakeTcgdex()
    const body = webp(300, 1)
    fake.addImage(HIGH, body)
    fake.queue(HIGH, () => new Response('indisponible', { status: 503 }))
    const clock = new Clock()
    const deps: RunnerDeps = { ...makeDeps(fake, clock), quickRetryDelaysMs: [10] }
    const { cardId, tcgdexId } = await prepare(deps, clock, { image: OFFICIAL })

    expect(await tick(deps)).toMatchObject({ status: 'ran', done: 1, retried: 0 })
    expect(fake.requests.map((request) => request.url)).toEqual([HIGH, HIGH])
    expect(await cardRow(cardId)).toMatchObject({ image_status: 'downloaded', image_quality: 'high' })
    expect(await jobRow('fr', tcgdexId)).toMatchObject({ status: 'done', attempts: 1 })
    expect(await r2Bytes(KEY)).toEqual(body)
  })

  it('quickRetryDelaysMs: [10] : une erreur réseau unique suivie d’un 200 aboutit dans la même exécution', async () => {
    const fake = new FakeTcgdex()
    fake.addImage(HIGH, webp(300, 1))
    fake.queue(HIGH, () => {
      throw new TypeError('connexion réinitialisée')
    })
    const clock = new Clock()
    const deps: RunnerDeps = { ...makeDeps(fake, clock), quickRetryDelaysMs: [10] }
    const { cardId } = await prepare(deps, clock, { image: OFFICIAL })

    expect(await tick(deps)).toMatchObject({ done: 1, retried: 0 })
    expect(fake.count((url) => url === HIGH)).toBe(2)
    expect((await cardRow(cardId)).image_status).toBe('downloaded')
  })

  it.each([404, 429])('relance courte jamais déclenchée pour un HTTP %i', async (status) => {
    const fake = new FakeTcgdex()
    fake.addImage(HIGH, webp(300, 1))
    fake.queue(HIGH, () => new Response(null, { status }))
    const clock = new Clock()
    const deps: RunnerDeps = { ...makeDeps(fake, clock), quickRetryDelaysMs: [10, 10] }
    await prepare(deps, clock, { image: OFFICIAL })

    await tick(deps)
    expect(fake.count((url) => url === HIGH)).toBe(1)
  })

  it('relance courte abandonnée quand le budget de l’exécution ne laisse pas le temps d’une requête', async () => {
    const fake = new FakeTcgdex()
    fake.addImage(HIGH, webp(300, 1))
    fake.queue(HIGH, () => new Response(null, { status: 503 }))
    const clock = new Clock()
    const deps: RunnerDeps = { ...makeDeps(fake, clock, { FETCH_TIMEOUT_MS: '30000' }), quickRetryDelaysMs: [10] }
    const { tcgdexId } = await prepare(deps, clock, { image: OFFICIAL })

    // Budget de 10 s : délai (10 ms) + délai d'expiration (30 s) dépassent l'échéance.
    expect(await tick(deps, 10_000)).toMatchObject({ retried: 1, done: 0 })
    expect(fake.count((url) => url === HIGH)).toBe(1)
    expect(await jobRow('fr', tcgdexId)).toMatchObject({ status: 'pending', attempts: 1, last_http_status: 503 })
  })
})

describe('validation des fichiers téléchargés', () => {
  it.each(CORRUPTIONS.filter((c) => !c.transient))('$name : « invalid_image », aucune nouvelle tentative, rien dans R2', async (corruption) => {
    const fake = new FakeTcgdex()
    fake.queue(HIGH, corruption.respond)
    const clock = new Clock()
    const deps = makeDeps(fake, clock, corruption.overrides ?? {})
    const { cardId, tcgdexId } = await prepare(deps, clock, { image: OFFICIAL })

    await drain(deps, clock)

    const card = await cardRow(cardId)
    expect(card).toMatchObject({ image_status: 'invalid_image', image_r2_key: null, image_sha256: null })
    expect(card.image_failure_reason).toContain('image invalide')
    expect(card.image_failure_reason).toContain(corruption.reason)
    const urls = testedUrls(card) as { url: string; status: number | null; error?: string }[]
    expect(urls[0]).toMatchObject({ url: HIGH, status: 200 })
    expect(urls[0]?.error).toContain(corruption.reason)
    // Fichier invalide : la qualité de secours est essayée (absente ici).
    expect(urls.slice(1)).toEqual([{ url: LOW, status: 404 }])
    expect(await jobRow('fr', tcgdexId)).toMatchObject({ status: 'done', attempts: 1 })
    expect(fake.count((url) => url === HIGH)).toBe(1)
    expect(await env.BUCKET.head(KEY)).toBeNull()
  })

  it.each(CORRUPTIONS.filter((c) => !c.transient))('$name en high.webp, low.webp valide : copie de low.webp dans la même tentative', async (corruption) => {
    const fake = new FakeTcgdex()
    const low = webp(200, 3)
    fake.queue(HIGH, corruption.respond)
    fake.addImage(LOW, low)
    const clock = new Clock()
    const deps = makeDeps(fake, clock, corruption.overrides ?? {})
    const { cardId, tcgdexId } = await prepare(deps, clock, { image: OFFICIAL })

    expect(await tick(deps)).toMatchObject({ status: 'ran', done: 1, retried: 0, failed: 0 })
    const card = await cardRow(cardId)
    expect(card).toMatchObject({
      image_status: 'downloaded',
      image_source: 'api',
      image_source_url: LOW,
      image_quality: 'low',
      image_file_size: 200,
      image_r2_key: KEY,
      image_sha256: await sha256Hex(low),
      image_http_status: 200,
      image_failure_reason: null,
    })
    const urls = testedUrls(card) as { url: string; status: number | null; error?: string }[]
    expect(urls).toHaveLength(2)
    expect(urls[0]).toMatchObject({ url: HIGH, status: 200 })
    expect(urls[0]?.error).toContain(corruption.reason)
    expect(urls[1]).toEqual({ url: LOW, status: 200 })
    expect(fake.requests.map((request) => request.url)).toEqual([HIGH, LOW])
    expect(await r2Bytes(KEY)).toEqual(low)
    expect((await env.BUCKET.head(KEY))?.customMetadata?.quality).toBe('low')
    expect(await jobRow('fr', tcgdexId)).toMatchObject({ status: 'done', attempts: 1 })
  })

  it('RIFF tronqué en high.webp et en low.webp, transfert intègre : « invalid_image » après les deux URL', async () => {
    const fake = new FakeTcgdex()
    fake.queue(HIGH, () => imageResponse(truncatedWebp()))
    fake.queue(LOW, () => imageResponse(truncatedWebp()))
    const clock = new Clock()
    const deps = makeDeps(fake, clock)
    const { cardId, tcgdexId } = await prepare(deps, clock, { image: OFFICIAL })

    expect(await tick(deps)).toMatchObject({ done: 1, retried: 0 })
    const card = await cardRow(cardId)
    expect(card).toMatchObject({ image_status: 'invalid_image', image_r2_key: null, image_http_status: 200 })
    expect(card.image_failure_reason).toContain('tronqué')
    expect((testedUrls(card) as { url: string }[]).map((tested) => tested.url)).toEqual([HIGH, LOW])
    expect(await jobRow('fr', tcgdexId)).toMatchObject({ status: 'done', attempts: 1 })
    expect(await env.BUCKET.head(KEY)).toBeNull()
  })

  it('RIFF tronqué sous MIN_IMAGE_BYTES, intégrité non prouvée (flux sans Content-Length ni ETag) : transfert corrompu, nouvelle tentative', async () => {
    const fake = new FakeTcgdex()
    const body = webp(300, 1)
    fake.addImage(HIGH, body)
    const cut = webp(300, 1).slice(0, 60) // RIFF annonce 300 octets, 60 reçus
    fake.queue(HIGH, async () => new Response(streamOf(cut), { status: 200, headers: { 'content-type': 'image/webp' } }))
    const clock = new Clock()
    const deps = makeDeps(fake, clock)
    const { cardId, tcgdexId } = await prepare(deps, clock, { image: OFFICIAL })

    expect(await tick(deps)).toMatchObject({ retried: 1, done: 0 })
    expect((await jobRow('fr', tcgdexId)).last_error).toContain('tronqué')
    expect((await cardRow(cardId)).image_status).toBe('pending')
    expect(fake.count((url) => url === LOW)).toBe(0)
  })

  it.each(CORRUPTIONS.filter((c) => c.transient))('$name : transfert corrompu, nouvelle tentative, puis copie du bon fichier', async (corruption) => {
    const fake = new FakeTcgdex()
    const body = webp(300, 1)
    fake.addImage(HIGH, body)
    fake.queue(HIGH, corruption.respond)
    const clock = new Clock()
    const deps = makeDeps(fake, clock, corruption.overrides ?? {})
    const { cardId, tcgdexId } = await prepare(deps, clock, { image: OFFICIAL })

    expect(await tick(deps)).toMatchObject({ retried: 1, done: 0 })
    const job = await jobRow('fr', tcgdexId)
    expect(job).toMatchObject({ status: 'pending', attempts: 1, last_http_status: 200 })
    expect(job.last_error).toContain(corruption.reason)
    expect((await cardRow(cardId)).image_status).toBe('pending')
    expect(await env.BUCKET.head(KEY)).toBeNull()
    // Une tentative corrompue n'essaie pas la qualité de secours.
    expect(fake.count((url) => url === LOW)).toBe(0)

    clock.t = Date.parse(job.next_attempt_at) + 1_000
    expect(await tick(deps)).toMatchObject({ done: 1 })
    expect(await cardRow(cardId)).toMatchObject({ image_status: 'downloaded', image_sha256: await sha256Hex(body) })
    expect(await r2Bytes(KEY)).toEqual(body)
  })

  it.each(CORRUPTIONS)('$name : n’écrase jamais un objet R2 existant valide', async (corruption) => {
    const fake = new FakeTcgdex()
    fake.queue(HIGH, corruption.respond)
    const clock = new Clock()
    const deps = makeDeps(fake, clock, corruption.overrides ?? {})
    // Copie antérieure valide, mais sans SHA-256 dans ses métadonnées : elle n'est pas réconciliée.
    const previous = webp(400, 9)
    await env.BUCKET.put(KEY, previous, { httpMetadata: { contentType: 'image/webp' }, customMetadata: { source: 'copie-antérieure' } })
    const before = await env.BUCKET.head(KEY)
    const { cardId } = await prepare(deps, clock, { image: OFFICIAL })

    await tick(deps)

    expect(fake.count((url) => url === HIGH)).toBe(1)
    expect(await r2Bytes(KEY)).toEqual(previous)
    const after = await env.BUCKET.head(KEY)
    expect(after?.etag).toBe(before?.etag)
    expect(after?.customMetadata).toEqual({ source: 'copie-antérieure' })
    expect((await cardRow(cardId)).image_status).not.toBe('downloaded')
  })

  it.each([
    {
      name: 'écriture R2 qui lève',
      reason: 'écriture R2 échouée',
      put: async (): Promise<R2Object | null> => {
        throw new Error('R2 indisponible')
      },
    },
    { name: 'écriture R2 sans objet rendu', reason: 'écriture R2 non confirmée', put: async (): Promise<R2Object | null> => null },
  ])('$name : nouvelle tentative, D1 non mis à jour', async ({ reason, put }) => {
    const fake = new FakeTcgdex()
    fake.addImage(HIGH, webp(300, 1))
    const clock = new Clock()
    const bucket = { head: (key: string) => env.BUCKET.head(key), put } as unknown as R2Bucket
    const deps: RunnerDeps = { ...makeDeps(fake, clock), bucket }
    const { cardId, tcgdexId } = await prepare(deps, clock, { image: OFFICIAL })

    expect(await tick(deps)).toMatchObject({ retried: 1, done: 0 })
    const job = await jobRow('fr', tcgdexId)
    expect(job).toMatchObject({ status: 'pending', attempts: 1 })
    expect(job.last_error).toContain(reason)
    expect(await cardRow(cardId)).toMatchObject({ image_status: 'pending', image_r2_key: null, image_sha256: null })
  })
})

describe('réconciliation et idempotence', () => {
  it('objet déjà présent dans R2 avec customMetadata.sha256 : « already_present » sans aucune requête vers le CDN', async () => {
    const fake = new FakeTcgdex()
    fake.addImage(HIGH, webp(300, 1)) // image du CDN : ne doit pas être demandée
    const clock = new Clock()
    const deps = makeDeps(fake, clock)
    const body = webp(500, 7)
    const sha256 = await sha256Hex(body)
    // Copie faite par une exécution précédente, interrompue avant la mise à jour de D1.
    await env.BUCKET.put(KEY, body, {
      httpMetadata: { contentType: 'image/webp' },
      customMetadata: { source: 'api', sourceUrl: HIGH, quality: 'high', sha256, md5: await md5Hex(body) },
    })
    const { cardId, tcgdexId } = await prepare(deps, clock, { image: OFFICIAL })

    expect(await tick(deps)).toMatchObject({ status: 'ran', done: 1, httpRequests: 0 })

    expect(fake.requests).toHaveLength(0)
    expect(await cardRow(cardId)).toMatchObject({
      image_status: 'already_present',
      image_source: 'api',
      image_source_url: HIGH,
      image_r2_key: KEY,
      image_quality: 'high',
      image_file_size: 500,
      image_sha256: sha256,
    })
    expect(await r2Bytes(KEY)).toEqual(body)
    expect(await jobRow('fr', tcgdexId)).toMatchObject({ status: 'done' })
  })

  it('carte déjà « downloaded » : la tâche ne fait rien (ni requête, ni écriture R2, ni modification de la carte)', async () => {
    const fake = new FakeTcgdex()
    fake.addImage(HIGH, webp(300, 1))
    const clock = new Clock()
    const deps = makeDeps(fake, clock)
    const { cardId, tcgdexId } = await prepare(deps, clock, { image: OFFICIAL, imageStatus: 'downloaded', imageR2Key: KEY })
    const before = await one(`SELECT * FROM cards WHERE id = ?1`, cardId)

    clock.advance(60_000)
    expect(await tick(deps)).toMatchObject({ status: 'ran', done: 1, httpRequests: 0 })

    expect(fake.requests).toHaveLength(0)
    expect(await env.BUCKET.head(KEY)).toBeNull()
    expect(await one(`SELECT * FROM cards WHERE id = ?1`, cardId)).toEqual(before)
    expect(await jobRow('fr', tcgdexId)).toMatchObject({ status: 'done' })
  })
})

describe('lecture R2 en échec et exceptions', () => {
  it('lecture R2 (bucket.head) qui lève : nouvelle tentative sans requête vers le CDN, puis « download_error » à la dernière tentative', async () => {
    const fake = new FakeTcgdex()
    fake.addImage(HIGH, webp(300, 1))
    const clock = new Clock()
    const bucket = bucketWithHead(async () => {
      throw new Error('R2 indisponible')
    })
    const deps: RunnerDeps = { ...makeDeps(fake, clock, { MAX_ATTEMPTS: '2' }), bucket }
    const { cardId, tcgdexId } = await prepare(deps, clock, { image: OFFICIAL })

    expect(await tick(deps)).toMatchObject({ status: 'ran', retried: 1, failed: 0, done: 0, httpRequests: 0 })
    const job = await jobRow('fr', tcgdexId)
    expect(job).toMatchObject({ status: 'pending', attempts: 1, last_http_status: null })
    expect(job.last_error).toBe('image fr:swsh3-136 : lecture R2 échouée : Error: R2 indisponible')
    expect((await cardRow(cardId)).image_status).toBe('pending')

    clock.t = Date.parse(job.next_attempt_at) + 1_000
    expect(await tick(deps)).toMatchObject({ status: 'ran', failed: 1, retried: 0, done: 0, httpRequests: 0 })
    const failed = await jobRow('fr', tcgdexId)
    expect(failed).toMatchObject({ status: 'failed', attempts: 2, last_http_status: null })
    expect(failed.last_error).toContain('lecture R2 échouée')
    expect(failed.last_error).toContain('après 2 tentative(s)')
    expect(await cardRow(cardId)).toMatchObject({
      image_status: 'download_error',
      image_failure_reason: 'lecture R2 échouée : Error: R2 indisponible',
      image_http_status: null,
      image_tested_urls: null,
      image_r2_key: null,
      image_sha256: null,
    })
    expect(fake.requests).toHaveLength(0)
    expect(await env.BUCKET.head(KEY)).toBeNull()
  })

  it('lecture R2 qui lève à la dernière tentative d’une carte déjà vérifiée : URL testées et code HTTP précédents conservés', async () => {
    const fake = new FakeTcgdex()
    const clock = new Clock()
    const bucket = bucketWithHead(async () => {
      throw new Error('R2 indisponible')
    })
    const deps: RunnerDeps = { ...makeDeps(fake, clock, { MAX_ATTEMPTS: '1' }), bucket }
    // Carte vérifiée par une synchronisation précédente, puis remise en attente.
    const { cardId } = await prepare(deps, clock, { image: OFFICIAL, imageStatus: 'not_found_on_cdn' })
    const history = JSON.stringify([
      { url: HIGH, status: 404 },
      { url: LOW, status: 404 },
    ])
    await env.DB.prepare(`UPDATE cards SET image_tested_urls = ?2, image_http_status = 404 WHERE id = ?1`).bind(cardId, history).run()

    expect(await tick(deps)).toMatchObject({ failed: 1, httpRequests: 0 })
    expect(await cardRow(cardId)).toMatchObject({
      image_status: 'download_error',
      image_failure_reason: 'lecture R2 échouée : Error: R2 indisponible',
      image_tested_urls: history,
      image_http_status: 404,
    })
  })

  it('exception pendant la tâche image (objet R2 illisible) : nouvelle tentative, puis carte « download_error » à la dernière tentative', async () => {
    const fake = new FakeTcgdex()
    fake.addImage(HIGH, webp(300, 1))
    const clock = new Clock()
    const unreadable = {
      get size(): number {
        throw new Error('objet R2 illisible')
      },
    }
    const bucket = bucketWithHead(async () => unreadable)
    const deps: RunnerDeps = { ...makeDeps(fake, clock, { MAX_ATTEMPTS: '2' }), bucket }
    const { cardId, tcgdexId } = await prepare(deps, clock, { image: OFFICIAL })

    expect(await tick(deps)).toMatchObject({ status: 'ran', retried: 1, failed: 0, done: 0 })
    const job = await jobRow('fr', tcgdexId)
    expect(job).toMatchObject({ status: 'pending', attempts: 1, last_http_status: null })
    expect(job.last_error).toBe('exception : Error: objet R2 illisible')
    expect((await cardRow(cardId)).image_status).toBe('pending')

    clock.t = Date.parse(job.next_attempt_at) + 1_000
    expect(await tick(deps)).toMatchObject({ status: 'ran', failed: 1, retried: 0, done: 0 })
    expect(await jobRow('fr', tcgdexId)).toMatchObject({ status: 'failed', attempts: 2, last_error: 'exception : Error: objet R2 illisible (après 2 tentative(s))' })
    // La carte n'est jamais laissée « pending » derrière une tâche en échec définitif.
    expect(await cardRow(cardId)).toMatchObject({
      image_status: 'download_error',
      image_failure_reason: 'exception : Error: objet R2 illisible (après 2 tentative(s))',
      image_http_status: null,
      image_r2_key: null,
    })
    expect(fake.requests).toHaveLength(0)
    expect(await env.BUCKET.head(KEY)).toBeNull()
  })
})

describe('client TCGdex', () => {
  it('requêtes vers le CDN et l’API : cache « no-store », agent utilisateur configuré et type attendu', async () => {
    const fake = sampleCatalog()
    const spy = spyFetcher(fake.fetcher)
    const clock = new Clock()
    const deps: RunnerDeps = { ...makeDeps(fake, clock, { USER_AGENT: 'agent-de-test/2.0' }), fetcher: spy.fetcher }
    await startSync(new Db(env.DB), deps.config, clock.now())
    await drain(deps, clock)

    const cdn = spy.calls.filter((call) => call.url.startsWith(`${ASSETS}/`))
    const api = spy.calls.filter((call) => call.url.startsWith(`${API}/`))
    expect(cdn.length).toBeGreaterThan(0)
    expect(api.length).toBeGreaterThan(0)
    expect(cdn.length + api.length).toBe(spy.calls.length)
    for (const call of spy.calls) {
      expect(call.init?.cache, call.url).toBe('no-store')
      // Aucune redirection n'est jamais suivie par fetch.
      expect(call.init?.redirect, call.url).toBe('manual')
      expect(new Headers(call.init?.headers).get('user-agent'), call.url).toBe('agent-de-test/2.0')
    }
    for (const call of cdn) expect(new Headers(call.init?.headers).get('accept'), call.url).toBe('image/webp')
    for (const call of api) expect(new Headers(call.init?.headers).get('accept'), call.url).toBe('application/json')
  })

  it('URL de l’API : identifiants encodés (exu-%3F → exu-%253F, SM1+ → SM1%2B)', async () => {
    const fake = sampleCatalog()
    const { client } = directClient(fake.fetcher)
    expect(client.apiUrl('fr', 'cards', 'exu-%3F')).toBe(`${API}/fr/cards/exu-%253F`)
    expect(client.apiUrl('ja', 'sets', 'SM1+')).toBe(`${API}/ja/sets/SM1%2B`)
    expect(client.apiUrl('ja', 'cards', 'SM1+-001')).toBe(`${API}/ja/cards/SM1%2B-001`)

    // Pendant une synchronisation réelle sur le faux TCGdex.
    const clock = new Clock()
    const deps = makeDeps(fake, clock)
    await startSync(new Db(env.DB), deps.config, clock.now())
    await drain(deps, clock)
    const urls = fake.requests.map((request) => request.url)
    expect(urls).toContain(`${API}/fr/cards/exu-%253F`)
    expect(urls).toContain(`${API}/ja/sets/SM1%2B`)
    expect(urls).toContain(`${API}/ja/cards/SM1%2B-001`)
    expect(urls).not.toContain(`${API}/fr/cards/exu-%3F`)
    expect(urls).not.toContain(`${API}/ja/sets/SM1+`)
    // URL reconstruite du CDN, encodée elle aussi.
    expect(urls).toContain(`${ASSETS}/ja/SM/SM1%2B/001/high.webp`)
    const details = await rows<{ id: string; detail_status: string }>(`SELECT id, detail_status FROM cards WHERE id IN ('fr:exu-%3F', 'ja:SM1+-001') ORDER BY id`)
    expect(details).toEqual([
      { id: 'fr:exu-%3F', detail_status: 'fetched' },
      { id: 'ja:SM1+-001', detail_status: 'fetched' },
    ])
  })

  it('hôte non autorisé ou schéma non https : refusé par le client, sans aucune requête', async () => {
    const calls: string[] = []
    const fetcher: typeof fetch = async (input) => {
      calls.push(urlOf(input))
      return imageResponse(webp(300, 1))
    }
    const { client, usage } = directClient(fetcher)
    for (const url of [
      `https://${EVIL_HOST}/fr/swsh/swsh3/136/high.webp`,
      'http://assets.tcgdex.net/fr/swsh/swsh3/136/high.webp',
      `https://assets.tcgdex.net.${EVIL_HOST}/fr/swsh/swsh3/136/high.webp`,
      `https://assets.tcgdex.net@${EVIL_HOST}/fr/swsh/swsh3/136/high.webp`,
      'http://api.tcgdex.net/v2/fr/sets',
      // Hôte autorisé, mais avec des identifiants dans l'URL.
      'https://utilisateur:secret@assets.tcgdex.net/fr/swsh/swsh3/136/high.webp',
      'https://utilisateur@api.tcgdex.net/v2/fr/sets',
      'https://:secret@api.tcgdex.net/v2/fr/sets',
      'pas une URL',
    ]) {
      expect(await client.get(url, 'image/webp'), url).toEqual({ kind: 'client_error', status: 0 })
    }
    expect(calls).toEqual([])
    expect(usage.httpRequests).toBe(0)

    // Les hôtes de TCGdex restent joignables.
    expect((await client.get(HIGH, 'image/webp')).kind).toBe('ok')
    expect((await client.get(`${API}/fr/sets`, 'application/json')).kind).toBe('ok')
    expect(calls).toEqual([HIGH, `${API}/fr/sets`])
  })

  it.each([`https://${EVIL_HOST}/v2`, 'http://api.tcgdex.net/v2', `https://api.tcgdex.net.${EVIL_HOST}/v2`])(
    'TCGDEX_API_BASE non fiable (%s) : base par défaut, et aucune exception du client pour cette base',
    async (base) => {
      const calls: string[] = []
      const fetcher: typeof fetch = async (input) => {
        calls.push(urlOf(input))
        return jsonResponse([])
      }
      const { client, usage } = directClient(fetcher, { TCGDEX_API_BASE: base })
      expect(client.apiUrl('fr', 'sets')).toBe(`${API}/fr/sets`)
      expect(await client.get(`${base}/fr/sets`, 'application/json')).toEqual({ kind: 'client_error', status: 0 })
      expect(await client.getJson(`${base}/fr/sets`)).toEqual({ kind: 'client_error', status: 0 })
      expect(calls).toEqual([])
      expect(usage.httpRequests).toBe(0)
    },
  )

  it.each([
    `https://${EVIL_HOST}/fr/swsh/swsh3/136`,
    'http://assets.tcgdex.net/fr/swsh/swsh3/136',
    `https://assets.tcgdex.net.${EVIL_HOST}/fr/swsh/swsh3/136`,
    `https://assets.tcgdex.net@${EVIL_HOST}/fr/swsh/swsh3/136`,
    // Requête ou fragment, même vides, n'importe où dans l'URL.
    `${OFFICIAL}?`,
    `${OFFICIAL}#`,
    `${OFFICIAL}?v=2`,
    `${ASSETS}/fr/swsh?/swsh3/136`,
    `${ASSETS}/fr/swsh#/swsh3/136`,
    // Premier segment du chemin autre que la langue de la carte (fr).
    `${ASSETS}/ja/swsh/swsh3/136`,
    `${ASSETS}/swsh/swsh3/136`,
    `${ASSETS}/fr/../ja/swsh/swsh3/136`,
  ])('URL d’image officielle non fiable (%s) : aucune requête vers elle, carte non marquée téléchargée', async (untrusted) => {
    // Série inconnue : aucune reconstruction possible non plus.
    const fake = new FakeTcgdex()
    const clock = new Clock()
    const deps = makeDeps(fake, clock)
    const { cardId } = await prepare(deps, clock, { series: null, image: untrusted })

    await drain(deps, clock)
    expect(fake.requests).toHaveLength(0)
    expect(await cardRow(cardId)).toMatchObject({ image_status: 'missing_api_url', image_r2_key: null })
    expect(await env.BUCKET.head(KEY)).toBeNull()
  })

  it('URL d’image officielle sur un autre domaine mais série connue : seule l’URL reconstruite du CDN est demandée', async () => {
    const fake = new FakeTcgdex()
    const body = webp(300, 1)
    fake.addImage(HIGH, body)
    const clock = new Clock()
    const deps = makeDeps(fake, clock)
    const { cardId } = await prepare(deps, clock, { image: `https://${EVIL_HOST}/fr/swsh/swsh3/136` })

    await tick(deps)
    expect(fake.requests.every((request) => new URL(request.url).host === 'assets.tcgdex.net')).toBe(true)
    expect(fake.requests.map((request) => request.url)).toEqual([HIGH])
    const card = await cardRow(cardId)
    expect(card.image_status).not.toBe('downloaded')
    expect(card).toMatchObject({ image_status: 'recovered_from_cdn', image_source: 'cdn_reconstructed', image_source_url: HIGH })
  })

  it.each([
    { language: 'fr' as Language, series: 'swsh', set: 'swsh3', localId: '136', foreign: `${ASSETS}/ja/swsh/swsh3/136`, expected: HIGH, key: KEY },
    {
      language: 'ja' as Language,
      series: 'SV',
      set: 'SV1a',
      localId: '001',
      foreign: `${ASSETS}/fr/SV/SV1a/001`,
      expected: `${ASSETS}/ja/SV/SV1a/001/high.webp`,
      key: 'cards/ja/SV1a/SV1a-001.webp',
    },
  ])('carte $language avec une URL officielle d’une autre langue ($foreign) : jamais demandée, URL reconstruite dans la langue de la carte', async (c) => {
    const fake = new FakeTcgdex()
    const body = webp(320, 3)
    // L'image de l'autre langue existe bien sur le CDN : elle ne doit pas être copiée.
    fake.addImage(`${c.foreign}/high.webp`, webp(330, 4))
    fake.addImage(`${c.foreign}/low.webp`, webp(330, 4))
    fake.addImage(c.expected, body)
    const clock = new Clock()
    const deps = makeDeps(fake, clock)
    const { cardId } = await prepare(deps, clock, { language: c.language, series: c.series, set: c.set, localId: c.localId, image: c.foreign })

    expect(await tick(deps)).toMatchObject({ status: 'ran', done: 1 })
    expect(fake.requests.map((request) => request.url)).toEqual([c.expected])
    expect(await cardRow(cardId)).toMatchObject({
      image_status: 'recovered_from_cdn',
      image_source: 'cdn_reconstructed',
      image_source_url: c.expected,
      image_r2_key: c.key,
      image_sha256: await sha256Hex(body),
    })
    expect(await r2Bytes(c.key)).toEqual(body)
  })

  it('redirection du CDN vers un hôte non autorisé : aucune requête vers cet hôte, carte non copiée', async () => {
    const elsewhere = `https://${EVIL_HOST}/fr/swsh/swsh3/136/high.webp`
    const calls: string[] = []
    const routes = new Map<string, () => Promise<Response>>([
      [HIGH, async () => new Response(null, { status: 302, headers: { location: elsewhere } })],
      [elsewhere, () => imageResponse(webp(300, 1))],
    ])
    const clock = new Clock()
    const deps: RunnerDeps = { ...makeDeps(new FakeTcgdex(), clock), fetcher: redirectingFetcher(routes, calls) }
    const { cardId } = await prepare(deps, clock, { image: OFFICIAL })

    await tick(deps)
    expect(calls.filter((url) => new URL(url).host === EVIL_HOST)).toEqual([])
    expect((await cardRow(cardId)).image_status).not.toBe('downloaded')
    expect(await env.BUCKET.head(KEY)).toBeNull()
  })

  it.each([
    { status: 302, location: `https://${EVIL_HOST}/fr/swsh/swsh3/136/high.webp`, what: 'un autre hôte' },
    { status: 301, location: 'http://assets.tcgdex.net/fr/swsh/swsh3/136/high.webp', what: 'http' },
    { status: 307, location: LOW, what: 'le même hôte (qualité de secours)' },
    { status: 308, location: `${OFFICIAL}/high.webp?v=2`, what: 'le même hôte (autre URL)' },
  ])('redirection HTTP $status du CDN vers $what : jamais suivie, nouvelle tentative puis « download_error »', async ({ status, location }) => {
    const calls: string[] = []
    const routes = new Map<string, () => Promise<Response>>([
      [HIGH, async () => new Response(null, { status, headers: { location } })],
      [location, () => imageResponse(webp(300, 1))],
    ])
    const clock = new Clock()
    const deps: RunnerDeps = { ...makeDeps(new FakeTcgdex(), clock, { MAX_ATTEMPTS: '2' }), fetcher: redirectingFetcher(routes, calls), quickRetryDelaysMs: [10] }
    const { cardId, tcgdexId } = await prepare(deps, clock, { image: OFFICIAL })

    // Erreur client : ni relance courte, ni qualité de secours, ni requête vers la destination.
    expect(await tick(deps)).toMatchObject({ status: 'ran', retried: 1, done: 0, failed: 0 })
    expect(calls).toEqual([HIGH])
    const job = await jobRow('fr', tcgdexId)
    expect(job).toMatchObject({ status: 'pending', attempts: 1, last_http_status: status, last_error: `image fr:swsh3-136 : HTTP ${status}` })
    expect((await cardRow(cardId)).image_status).toBe('pending')

    clock.t = Date.parse(job.next_attempt_at) + 1_000
    expect(await tick(deps)).toMatchObject({ status: 'ran', failed: 1, retried: 0, done: 0 })
    expect(calls).toEqual([HIGH, HIGH])
    expect(await jobRow('fr', tcgdexId)).toMatchObject({ status: 'failed', attempts: 2, last_http_status: status })
    const card = await cardRow(cardId)
    expect(card).toMatchObject({ image_status: 'download_error', image_http_status: status, image_failure_reason: `HTTP ${status}`, image_r2_key: null })
    expect(testedUrls(card)).toEqual([{ url: HIGH, status, error: `HTTP ${status}` }])
    expect(await env.BUCKET.head(KEY)).toBeNull()
  })

  it.each([301, 302, 303, 307, 308])('API : redirection HTTP %i classée en erreur client, jamais suivie ni relancée', async (status) => {
    const calls: string[] = []
    const routes = new Map<string, () => Promise<Response>>([
      [`${API}/fr/sets`, async () => new Response(null, { status, headers: { location: `https://${EVIL_HOST}/v2/fr/sets` } })],
      [`https://${EVIL_HOST}/v2/fr/sets`, async () => jsonResponse([{ id: 'piege', name: 'Piège' }])],
    ])
    const { client, usage } = directClient(redirectingFetcher(routes, calls), {}, [10, 10])

    expect(await client.get(`${API}/fr/sets`, 'application/json')).toEqual({ kind: 'client_error', status })
    expect(await client.getJson(`${API}/fr/sets`)).toEqual({ kind: 'client_error', status })
    expect(calls).toEqual([`${API}/fr/sets`, `${API}/fr/sets`])
    expect(usage.httpRequests).toBe(2)
  })
})
