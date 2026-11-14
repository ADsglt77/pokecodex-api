/**
 * Vérifie les hypothèses de l'importateur sur le vrai TCGdex (formes des réponses, encodage des
 * identifiants, images, reconstruction d'URL). Aucune écriture : ni D1, ni R2.
 */

import { env } from 'cloudflare:workers'
import { describe, expect, it } from 'vitest'
import { parseConfig, type ConfigSource } from '../src/config'
import { Usage } from '../src/db'
import { Health, TcgdexClient } from '../src/tcgdex/client'
import { checkWebp, imageCandidates, logoImageUrl, lowImageUrl, symbolImageUrl } from '../src/tcgdex/images'
import { parseCardDetail, parseCardList, parseSeriesList, parseSetDetail, parseSetList } from '../src/tcgdex/types'
import { readCapped } from '../src/utils/http'
import { RateLimiter } from '../src/utils/limiter'

const source: ConfigSource = {}
for (const [key, value] of Object.entries(env)) if (typeof value === 'string') source[key] = value
const config = parseConfig(source)
const client = new TcgdexClient({
  config,
  fetcher: (input, init) => fetch(input, init),
  limiter: new RateLimiter(400),
  usage: new Usage(),
  health: new Health(),
  now: Date.now,
  deadlineMs: Date.now() + 10 * 60_000,
})

async function json(url: string): Promise<unknown> {
  const outcome = await client.getJson(url)
  if (outcome.kind !== 'ok') throw new Error(`${url} → ${outcome.kind} ${outcome.status}`)
  return outcome.data
}

async function image(url: string) {
  const outcome = await client.get(url, 'image/webp')
  if (outcome.kind !== 'ok') return { status: outcome.status, check: null }
  const bytes = await readCapped(outcome.response, config.maxImageBytes)
  return { status: outcome.status, size: bytes.byteLength, check: await checkWebp(bytes, outcome.response.headers, config) }
}

describe('TCGdex réel', () => {
  it('séries et extensions FR et JP', async () => {
    for (const language of ['fr', 'ja'] as const) {
      const series = parseSeriesList(await json(client.apiUrl(language, 'series')))
      const sets = parseSetList(await json(client.apiUrl(language, 'sets')))
      expect(series.length).toBeGreaterThan(10)
      expect(sets.length).toBeGreaterThan(150)
    }
  })

  it('catalogues complets des cartes FR et JP', async () => {
    const fr = parseCardList(await json(client.apiUrl('fr', 'cards')))
    const ja = parseCardList(await json(client.apiUrl('ja', 'cards')))
    expect(fr.length).toBeGreaterThan(20_000)
    expect(ja.length).toBeGreaterThan(10_000)
    expect(new Set(fr.map((card) => card.id)).size).toBe(fr.length)
  })

  it('détail d’extension et de carte, prix retirés', async () => {
    const set = parseSetDetail(await json(client.apiUrl('fr', 'sets', 'swsh3')))
    expect(set.serie?.id).toBe('swsh')
    expect(set.cards.length).toBeGreaterThan(150)
    const card = parseCardDetail(await json(client.apiUrl('fr', 'cards', 'swsh3-136')))
    expect(card).toMatchObject({ id: 'swsh3-136', localId: '136', name: 'Fouinar', set: { id: 'swsh3' } })
    expect(card.rarity).toBeTruthy()
    expect(JSON.stringify(card.raw)).not.toContain('"pricing"')
    const ja = parseSetDetail(await json(client.apiUrl('ja', 'sets', 'SV1a')))
    expect(ja.serie?.id).toBe('SV')
  })

  it('identifiants à encoder : « % » et « + »', async () => {
    expect(parseCardDetail(await json(client.apiUrl('fr', 'cards', 'exu-%3F'))).id).toBe('exu-%3F')
    expect(parseSetDetail(await json(client.apiUrl('ja', 'sets', 'SM1+'))).id).toBe('SM1+')
  })

  it('image officielle valide, URL reconstruite trouvée et 404 confirmé', async () => {
    const official = await image('https://assets.tcgdex.net/fr/swsh/swsh3/136/high.webp')
    expect(official.status).toBe(200)
    expect(official.check?.ok).toBe(true)
    // Carte japonaise sans champ image dans l'API, mais présente sur le CDN (sondage du 13/11/2026).
    const [candidate] = imageCandidates(config, { language: 'ja', api_image_url: null, series_id: 'SM', set_tcgdex_id: 'SM1M', local_id: '001' })
    expect(candidate?.url).toBe('https://assets.tcgdex.net/ja/SM/SM1M/001/high.webp')
    const recovered = await image(candidate?.url ?? '')
    expect(recovered.status).toBe(200)
    expect(recovered.check?.ok).toBe(true)
    const missing = await image('https://assets.tcgdex.net/fr/swsh/swsh3/9999/high.webp')
    expect(missing.status).toBe(404)
  })

  it('petite image (low.webp, même base que high.webp) et logo d’extension (<logo>.webp)', async () => {
    const low = await image(lowImageUrl('https://assets.tcgdex.net/fr/swsh/swsh3/136/high.webp', 'fr') ?? '')
    expect(low.status).toBe(200)
    expect(low.check?.ok).toBe(true)
    const set = parseSetDetail(await json(client.apiUrl('fr', 'sets', 'swsh3')))
    const logoUrl = logoImageUrl(set.logo, 'fr')
    expect(logoUrl).toBe('https://assets.tcgdex.net/fr/swsh/swsh3/logo.webp')
    const logo = await image(logoUrl ?? '')
    expect(logo.status).toBe(200)
    expect(logo.check?.ok).toBe(true)
  })

  it('symbole d’extension (<symbol>.webp sous univ, servi en application/octet-stream)', async () => {
    const url = symbolImageUrl('https://assets.tcgdex.net/univ/sv/sv03.5/symbol', 'fr')
    expect(url).toBe('https://assets.tcgdex.net/univ/sv/sv03.5/symbol.webp')
    const outcome = await client.get(url ?? '', 'image/webp')
    if (outcome.kind !== 'ok') throw new Error(`${url} → ${outcome.kind} ${outcome.status}`)
    // Si TCGdex sert un jour image/webp, l'exception `allowOctetStream` des symboles pourra être retirée.
    expect(outcome.response.headers.get('content-type')).toBe('application/octet-stream')
    const bytes = await readCapped(outcome.response, config.maxImageBytes)
    expect((await checkWebp(bytes, outcome.response.headers, config, { allowOctetStream: true })).ok).toBe(true)
    expect((await checkWebp(bytes, outcome.response.headers, config)).ok).toBe(false)
  })
})
