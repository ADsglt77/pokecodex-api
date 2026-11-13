/**
 * Outils partagés des tests : faux TCGdex (API et CDN) en mémoire, horloge simulée, fichiers WebP
 * synthétiques, remise à zéro de D1 et R2. Aucun appel réseau réel, aucune image réelle.
 */

import { env } from 'cloudflare:workers'
import { parseConfig, type Config, type ConfigSource, type Language } from '../src/config'
import { runTick, type RunnerDeps, type TickResult } from '../src/services/runner'
import { toHex } from '../src/utils/hash'

export const API = 'https://api.tcgdex.net/v2'
export const ASSETS = 'https://assets.tcgdex.net'

// ——— Fichiers WebP synthétiques ———

/** Un fichier WebP minimal mais structurellement valide (RIFF…WEBP, taille RIFF exacte). */
export function webp(size = 256, seed = 1): Uint8Array {
  const bytes = new Uint8Array(size)
  const view = new DataView(bytes.buffer)
  bytes.set([0x52, 0x49, 0x46, 0x46], 0) // RIFF
  view.setUint32(4, size - 8, true)
  bytes.set([0x57, 0x45, 0x42, 0x50], 8) // WEBP
  bytes.set([0x56, 0x50, 0x38, 0x4c], 12) // VP8L
  view.setUint32(16, size - 20, true)
  for (let i = 20; i < size; i++) bytes[i] = (i * seed + seed) & 0xff
  return bytes
}

export async function md5Hex(bytes: Uint8Array): Promise<string> {
  return toHex(await crypto.subtle.digest('MD5', bytes))
}

export async function sha256Hex(bytes: Uint8Array): Promise<string> {
  return toHex(await crypto.subtle.digest('SHA-256', bytes))
}

// ——— Horloge simulée ———

export class Clock {
  t: number
  constructor(start = '2026-11-13T10:00:00.000Z') {
    this.t = Date.parse(start)
  }
  now = (): number => this.t
  /** Les attentes (limiteur de débit, relances) avancent l'horloge sans attendre réellement. */
  wait = async (ms: number): Promise<void> => {
    this.t += Math.max(0, ms)
  }
  advance(ms: number): void {
    this.t += ms
  }
}

// ——— Faux TCGdex ———

export interface FakeCard {
  id: string
  localId: string
  name: string
  /** Base d'URL officielle (sans qualité ni extension), ou rien. */
  image?: string | null
  category?: string
  rarity?: string
  illustrator?: string
  /** Extension annoncée par le détail de la carte, si différente de celle qui la liste. */
  detailSetId?: string
}

export interface FakeSet {
  id: string
  name: string
  serie: { id: string; name: string } | null
  logo?: string
  symbol?: string
  releaseDate?: string
  cards: FakeCard[]
  /** Présente dans la liste `/sets` (vrai par défaut). */
  listed?: boolean
  /** Statut forcé du détail de l'extension (ex. 404). */
  detailStatus?: number
}

export interface FakeImage {
  status: number
  body?: Uint8Array
  headers?: Record<string, string>
  /** Ajoute l'ETag = MD5 du contenu (comme assets.tcgdex.net), vrai par défaut. */
  etag?: boolean
}

type Responder = (url: string, init?: RequestInit) => Response | Promise<Response>

export class FakeTcgdex {
  series: Record<Language, { id: string; name: string; logo?: string }[]> = { fr: [], ja: [] }
  sets: Record<Language, FakeSet[]> = { fr: [], ja: [] }
  /** Cartes de `/cards` absentes du détail de toute extension. */
  orphanCards: Record<Language, (FakeCard & { setId: string })[]> = { fr: [], ja: [] }
  images = new Map<string, FakeImage>()
  /** Réponses imposées, consommées dans l'ordre (injection de pannes), par URL exacte. */
  queued = new Map<string, Responder[]>()
  requests: { url: string; method: string; headers: Record<string, string> }[] = []

  /** Ajoute une image valide servie à `url`. */
  addImage(url: string, body: Uint8Array = webp(), extra: Partial<FakeImage> = {}): void {
    this.images.set(url, { status: 200, body, ...extra })
  }

  /** Impose les prochaines réponses d'une URL (après épuisement, comportement normal). */
  queue(url: string, ...responders: Responder[]): void {
    this.queued.set(url, [...(this.queued.get(url) ?? []), ...responders])
  }

  count(predicate: (url: string) => boolean): number {
    return this.requests.filter((request) => predicate(request.url)).length
  }

  fetcher: typeof fetch = async (input, init) => {
    const url = typeof input === 'string' ? input : input instanceof URL ? input.toString() : input.url
    const headers: Record<string, string> = {}
    new Headers(init?.headers).forEach((value, key) => (headers[key] = value))
    this.requests.push({ url, method: init?.method ?? 'GET', headers })
    const queue = this.queued.get(url)
    if (queue?.length) return (queue.shift() as Responder)(url, init)
    if (url.startsWith(`${API}/`)) return this.api(url)
    if (url.startsWith(`${ASSETS}/`)) return this.asset(url)
    throw new TypeError(`réseau simulé : hôte inconnu ${url}`)
  }

  private api(url: string): Response {
    const [language, kind, id] = url.slice(API.length + 1).split('/').map(decodeURIComponent) as [Language, string, string | undefined]
    const sets = this.sets[language] ?? []
    if (kind === 'series' && id === undefined) return jsonResponse(this.series[language] ?? [])
    if (kind === 'sets' && id === undefined) {
      return jsonResponse(sets.filter((set) => set.listed !== false).map((set) => ({ id: set.id, name: set.name, logo: set.logo, symbol: set.symbol, cardCount: { total: set.cards.length, official: set.cards.length } })))
    }
    if (kind === 'sets' && id !== undefined) {
      const set = sets.find((item) => item.id === id)
      if (!set || set.detailStatus === 404) return notFoundJson(url)
      if (set.detailStatus) return new Response('erreur', { status: set.detailStatus })
      return jsonResponse({
        id: set.id,
        name: set.name,
        logo: set.logo,
        symbol: set.symbol,
        releaseDate: set.releaseDate ?? '2020-01-01',
        serie: set.serie ?? undefined,
        cardCount: { total: set.cards.length, official: set.cards.length },
        legal: { standard: false, expanded: true },
        cards: set.cards.map(({ id: cardId, localId, name, image }) => ({ id: cardId, localId, name, ...(image ? { image } : {}) })),
      })
    }
    const allCards = [...sets.flatMap((set) => set.cards.map((card) => ({ ...card, setId: card.detailSetId ?? set.id }))), ...(this.orphanCards[language] ?? [])]
    if (kind === 'cards' && id === undefined) {
      return jsonResponse(allCards.map(({ id: cardId, localId, name, image }) => ({ id: cardId, localId, name, ...(image ? { image } : {}) })))
    }
    if (kind === 'cards' && id !== undefined) {
      const card = allCards.find((item) => item.id === id)
      if (!card) return notFoundJson(url)
      const set = sets.find((item) => item.id === card.setId)
      return jsonResponse({
        id: card.id,
        localId: card.localId,
        name: card.name,
        ...(card.image ? { image: card.image } : {}),
        category: card.category ?? 'Pokémon',
        rarity: card.rarity ?? 'Commune',
        illustrator: card.illustrator ?? 'Illustrateur',
        set: { id: card.setId, name: set?.name, logo: set?.logo, symbol: set?.symbol, cardCount: { total: set?.cards.length ?? 0, official: set?.cards.length ?? 0 } },
        variants: { normal: true },
        variants_detailed: [{ type: 'Normal', thirdParty: { cardmarket: 123 }, pricing: { cardmarket: { avg: 1.23 } } }],
        updated: '2026-10-19T09:46:55+01:00',
        pricing: { cardmarket: { avg: 0.5, unit: 'EUR' } },
      })
    }
    return notFoundJson(url)
  }

  private async asset(url: string): Promise<Response> {
    const image = this.images.get(url)
    if (!image) {
      // Comme le CDN réel : 404 nginx, avec un double Content-Type et un cache « immutable ».
      const headers = new Headers({ 'cache-control': 'public, max-age=31536000, immutable' })
      headers.append('content-type', 'text/html')
      headers.append('content-type', 'image/webp')
      return new Response('<html><body><h1>404 Not Found</h1></body></html>', { status: 404, headers })
    }
    const headers = new Headers({ 'content-type': 'image/webp', ...(image.headers ?? {}) })
    if (image.body && !headers.has('content-length')) headers.set('content-length', String(image.body.byteLength))
    if (image.body && image.etag !== false && !headers.has('etag')) headers.set('etag', `"${await md5Hex(image.body)}"`)
    return new Response(image.body ?? null, { status: image.status, headers })
  }
}

export const jsonResponse = (body: unknown, status = 200): Response =>
  new Response(JSON.stringify(body), { status, headers: { 'content-type': 'application/json; charset=utf-8' } })

const notFoundJson = (url: string) => jsonResponse({ type: 'https://tcgdex.dev/errors/not-found', status: 404, endpoint: url }, 404)

/**
 * Petit catalogue de référence :
 *   fr / swsh3 (série swsh) : 136 avec image, 137 sans image mais présente sur le CDN (reconstruite),
 *                             138 sans image et absente du CDN ;
 *   fr / exu   (série ex)   : `exu-%3F` (id avec « % »), sans image ;
 *   ja / SV1a  (série SV)   : 001 avec image, 002 sans image mais sur le CDN ;
 *   ja / SM1+  (série SM)   : 001 sans image et absente du CDN (id avec « + »).
 */
export function sampleCatalog(): FakeTcgdex {
  const fake = new FakeTcgdex()
  fake.series.fr = [
    { id: 'swsh', name: 'Épée et Bouclier', logo: `${ASSETS}/fr/swsh/swsh1/logo` },
    { id: 'ex', name: 'EX' },
  ]
  fake.series.ja = [
    { id: 'SV', name: 'スカーレット&バイオレット' },
    { id: 'SM', name: 'サン&ムーン' },
  ]
  fake.sets.fr = [
    {
      id: 'swsh3',
      name: 'Ténèbres Embrasées',
      serie: { id: 'swsh', name: 'Épée et Bouclier' },
      logo: `${ASSETS}/fr/swsh/swsh3/logo`,
      cards: [
        { id: 'swsh3-136', localId: '136', name: 'Fouinar', image: `${ASSETS}/fr/swsh/swsh3/136` },
        { id: 'swsh3-137', localId: '137', name: 'Carte reconstruite' },
        { id: 'swsh3-138', localId: '138', name: 'Carte introuvable' },
      ],
    },
    { id: 'exu', name: 'Unseen Forces', serie: { id: 'ex', name: 'EX' }, cards: [{ id: 'exu-%3F', localId: '%3F', name: 'Zarbi' }] },
  ]
  fake.sets.ja = [
    {
      id: 'SV1a',
      name: 'トリプレットビート',
      serie: { id: 'SV', name: 'スカーレット&バイオレット' },
      cards: [
        { id: 'SV1a-001', localId: '001', name: 'トロピウス', image: `${ASSETS}/ja/SV/SV1a/001` },
        { id: 'SV1a-002', localId: '002', name: 'タマゲタケ' },
      ],
    },
    { id: 'SM1+', name: 'サン&ムーン強化', serie: { id: 'SM', name: 'サン&ムーン' }, cards: [{ id: 'SM1+-001', localId: '001', name: 'テスト' }] },
  ]
  fake.addImage(`${ASSETS}/fr/swsh/swsh3/136/high.webp`, webp(300, 1))
  fake.addImage(`${ASSETS}/fr/swsh/swsh3/137/high.webp`, webp(310, 2))
  fake.addImage(`${ASSETS}/ja/SV/SV1a/001/high.webp`, webp(320, 3))
  fake.addImage(`${ASSETS}/ja/SV/SV1a/002/high.webp`, webp(330, 4))
  return fake
}

// ——— Configuration, dépendances, exécution ———

/** Configuration de test : celle de `wrangler.jsonc`, modifiable. */
export function testConfig(overrides: ConfigSource = {}): Config {
  const source: ConfigSource = {}
  for (const [key, value] of Object.entries(env)) if (typeof value === 'string') source[key] = value
  return parseConfig({ ...source, ...overrides })
}

export function makeDeps(fake: FakeTcgdex, clock = new Clock(), overrides: ConfigSource = {}): RunnerDeps {
  return {
    config: testConfig(overrides),
    d1: env.DB,
    bucket: env.BUCKET,
    fetcher: fake.fetcher,
    now: clock.now,
    wait: clock.wait,
    random: () => 0.5,
    quickRetryDelaysMs: [],
  }
}

/**
 * Enchaîne les exécutions comme le ferait le Cron (une toutes les 5 minutes, en sautant les attentes
 * imposées) jusqu'à ce qu'il n'y ait plus rien à faire, ou `maxTicks`.
 */
export async function drain(deps: RunnerDeps, clock: Clock, maxTicks = 50): Promise<TickResult[]> {
  const results: TickResult[] = []
  for (let i = 0; i < maxTicks; i++) {
    const result = await runTick(deps, { trigger: 'test' })
    results.push(result)
    if (result.status === 'idle') {
      // Des tâches différées (nouvelle tentative, attente d'une dépendance) : le Cron les reprendra.
      const next = await env.DB.prepare(`SELECT MIN(next_attempt_at) AS at FROM import_jobs WHERE status = 'pending'`).first<{ at: string | null }>()
      if (!next?.at) break
      clock.t = Math.max(clock.t, Date.parse(next.at)) + 1_000
      continue
    }
    clock.advance(5 * 60_000)
    if (result.status === 'skipped' && result.reason.startsWith('attente imposée')) clock.advance(60 * 60_000)
    if (result.status === 'skipped' && result.reason.startsWith('en pause')) break
  }
  return results
}

// ——— D1 et R2 ———

export async function rows<T = Record<string, unknown>>(sql: string, ...params: (string | number | null)[]): Promise<T[]> {
  return (await env.DB.prepare(sql).bind(...params).all<T>()).results
}

export async function one<T = Record<string, unknown>>(sql: string, ...params: (string | number | null)[]): Promise<T> {
  const row = await env.DB.prepare(sql).bind(...params).first<T>()
  if (row === null) throw new Error(`aucune ligne : ${sql}`)
  return row
}

/** Vide toutes les tables et le R2 de test (stockage propre à ce fichier de test). */
export async function resetAll(): Promise<void> {
  await env.DB.batch(
    ['card_marketplace_links', 'import_jobs', 'cards', 'sets', 'series', 'sync_runs', 'tick_runs', 'runtime_state'].map((table) => env.DB.prepare(`DELETE FROM ${table}`)),
  )
  for (let listed = await env.BUCKET.list(); listed.objects.length; listed = await env.BUCKET.list()) {
    await env.BUCKET.delete(listed.objects.map((object) => object.key))
  }
}
