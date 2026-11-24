/**
 * Import manuel (src/services/manual-import.ts, routes `/admin/import/*`) : ajouts d'une autre source
 * que TCGdex, sans jamais écraser une ligne, une image ou un objet R2 existants, et cohabitation avec
 * la synchronisation TCGdex (jamais de retrait, reprise si TCGdex liste la carte un jour).
 */

import { env } from 'cloudflare:workers'
import { beforeEach, describe, expect, it } from 'vitest'
import { handleAdmin, type AdminDeps } from '../src/admin/router'
import { Db } from '../src/db'
import { importCatalog, InputError, numberKey, parseCatalogBatch, setKey } from '../src/services/manual-import'
import { retryMissing, startSync } from '../src/services/sync'
import { Clock, drain, makeDeps, one, resetAll, rows, sampleCatalog, sha256Hex, webp, type FakeTcgdex } from './helpers'

const TOKEN = env.ADMIN_TOKEN
const ORIGIN = 'https://importateur.test'
const SOURCE = 'https://exemple.test/scan.jpg'

let fake: FakeTcgdex
let clock: Clock

beforeEach(async () => {
  await resetAll()
  fake = sampleCatalog()
  clock = new Clock()
  const deps = makeDeps(fake, clock)
  await startSync(new Db(env.DB), deps.config, clock.now())
  const ticks = await drain(deps, clock)
  expect(ticks.at(-1)?.status).toBe('idle')
})

function admin(method: string, path: string, body?: BodyInit, authorization = `Bearer ${TOKEN}`): Promise<Response> {
  const deps = makeDeps(fake, clock)
  const adminDeps: AdminDeps = { config: deps.config, d1: env.DB, bucket: env.BUCKET, fetcher: fake.fetcher, adminToken: TOKEN, now: clock.now }
  return handleAdmin(new Request(`${ORIGIN}${path}`, { method, headers: { authorization }, body }), adminDeps)
}

const catalog = (body: unknown) => admin('POST', '/admin/import/catalog', JSON.stringify(body))
const image = (query: string, bytes: Uint8Array) => admin('POST', `/admin/import/image?${query}&source=${encodeURIComponent(SOURCE)}`, bytes)

/** Un lot type : une nouvelle extension japonaise (série SV existante) et une carte ajoutée à SV1a. */
const sampleBatch = () => ({
  language: 'ja',
  sets: [{ id: 'VM1', name: 'Vending Machine', series: 'SV', releaseDate: '1998-03-23', officialCards: 2, metadata: { block: 'Vending' } }],
  cards: [
    { id: 'VM1-1', set: 'VM1', localId: '1', name: 'フシギダネ', rarity: 'Common', category: 'Pokemon', illustrator: 'Ken Sugimori', metadata: { dexId: [1], types: ['Grass'], variants_detailed: [{ type: 'normal', thirdParty: { cardmarket: 12 } }] } },
    { id: 'VM1-2', set: 'VM1', localId: '2', name: 'ミュウ', rarity: null, category: 'Pokemon', illustrator: null, metadata: {} },
    { id: 'SV1a-099', set: 'SV1a', localId: '099', name: '追加', rarity: null, category: 'Trainer', illustrator: null },
  ],
})

describe('schéma (migration 0004)', () => {
  it('ajoute data_source / logo_provider / image_provider, NULL par défaut et limités à pokecardex', async () => {
    expect(await one(`SELECT COUNT(*) AS n FROM cards WHERE data_source IS NOT NULL OR image_provider IS NOT NULL`)).toEqual({ n: 0 })
    expect(await one(`SELECT COUNT(*) AS n FROM sets WHERE data_source IS NOT NULL OR logo_provider IS NOT NULL`)).toEqual({ n: 0 })
    await env.DB.prepare(`UPDATE cards SET data_source = 'pokecardex', image_provider = 'pokecardex' WHERE id = 'fr:swsh3-138'`).run()
    await expect(env.DB.prepare(`UPDATE cards SET data_source = 'autre' WHERE id = 'fr:swsh3-138'`).run()).rejects.toThrow(/CHECK constraint failed/)
    await expect(env.DB.prepare(`UPDATE sets SET logo_provider = 'tcgdex' WHERE id = 'fr:swsh3'`).run()).rejects.toThrow(/CHECK constraint failed/)
  })
})

describe('clés de comparaison du site', () => {
  it('setKey et numberKey', () => {
    expect(setKey('M-P')).toBe(setKey('mp'))
    expect(numberKey('017')).toBe(numberKey('17'))
    expect(numberKey('SV001')).toBe('SV1')
    expect(numberKey('0')).toBe('0')
    expect(numberKey('a12')).toBe('A12')
  })
})

describe('POST /admin/import/catalog', () => {
  it('ajoute extension et cartes, marquées pokecardex, sans brief_hash, rattachées à la série', async () => {
    const res = await catalog(sampleBatch())
    expect(res.status).toBe(200)
    expect(await res.json()).toEqual({ series: { inserted: 0, existing: 0 }, sets: { inserted: 1, existing: 0, rejected: [] }, cards: { inserted: 3, existing: 0, rejected: [] } })
    expect(await one(`SELECT tcgdex_id, series_id, name, release_date, official_cards, data_source, metadata_json FROM sets WHERE id = 'ja:VM1'`)).toEqual({
      tcgdex_id: 'VM1',
      series_id: 'ja:SV',
      name: 'Vending Machine',
      release_date: '1998-03-23',
      official_cards: 2,
      data_source: 'pokecardex',
      metadata_json: '{"block":"Vending"}',
    })
    const card = await one<Record<string, unknown>>(`SELECT * FROM cards WHERE id = 'ja:VM1-1'`)
    expect(card).toMatchObject({ tcgdex_id: 'VM1-1', set_id: 'ja:VM1', series_id: 'SV', local_id: '1', rarity: 'Common', category: 'Pokemon', brief_hash: null, data_source: 'pokecardex', image_status: 'pending', removed_at: null })
    // Le site lit ces chemins JSON : ils doivent être des tableaux.
    expect(await one(`SELECT json_extract(metadata_json, '$.dexId') AS dex, json_extract(metadata_json, '$.variants_detailed') AS v FROM cards WHERE id = 'ja:VM1-1'`)).toEqual({
      dex: '[1]',
      v: '[{"type":"normal","thirdParty":{"cardmarket":12}}]',
    })
    expect(await one(`SELECT metadata_json FROM cards WHERE id = 'ja:SV1a-099'`)).toEqual({ metadata_json: '{}' })
  })

  it('est idempotent et ne modifie jamais une ligne existante (TCGdex ou import précédent)', async () => {
    const before = await one(`SELECT * FROM cards WHERE id = 'ja:SV1a-001'`)
    await catalog(sampleBatch())
    const again = sampleBatch()
    again.sets[0]!.name = 'Autre nom'
    again.cards[0]!.name = 'Autre nom'
    again.cards.push({ id: 'SV1a-001', set: 'SV1a', localId: '777', name: 'Écrasement ?', rarity: null, category: 'Trainer', illustrator: null })
    const res = await catalog(again)
    expect(await res.json()).toEqual({ series: { inserted: 0, existing: 0 }, sets: { inserted: 0, existing: 1, rejected: [] }, cards: { inserted: 0, existing: 4, rejected: [] } })
    expect(await one(`SELECT name FROM sets WHERE id = 'ja:VM1'`)).toEqual({ name: 'Vending Machine' })
    expect(await one(`SELECT name FROM cards WHERE id = 'ja:VM1-1'`)).toEqual({ name: 'フシギダネ' })
    expect(await one(`SELECT * FROM cards WHERE id = 'ja:SV1a-001'`)).toEqual(before)
  })

  it('crée une série absente (jamais tcgp, jamais une série existante modifiée) et y rattache une extension', async () => {
    const res = await catalog({
      language: 'ja',
      series: [{ id: 'DPt', name: 'プラチナ' }, { id: 'SV', name: 'Autre nom' }, { id: 'DPt', name: 'doublon' }],
      sets: [{ id: 'PT1', name: 'Galactic Conquest', series: 'DPt', releaseDate: '2008-09-01' }],
    })
    expect(await res.json()).toMatchObject({ series: { inserted: 1, existing: 2 }, sets: { inserted: 1 } })
    expect(await one(`SELECT tcgdex_id, name, data_source FROM series WHERE id = 'ja:DPt'`)).toEqual({ tcgdex_id: 'DPt', name: 'プラチナ', data_source: 'pokecardex' })
    expect(await one(`SELECT name, data_source FROM series WHERE id = 'ja:SV'`)).toEqual({ name: 'スカーレット&バイオレット', data_source: null })
    expect(await one(`SELECT series_id FROM sets WHERE id = 'ja:PT1'`)).toEqual({ series_id: 'ja:DPt' })
    expect((await catalog({ language: 'ja', series: [{ id: 'tcgp', name: 'Pocket' }] })).status).toBe(400)
  })

  it('refuse un code d’extension confondu par le site, une série inconnue, un numéro déjà pris', async () => {
    const res = await catalog({
      language: 'ja',
      sets: [
        { id: 'SV-1A', name: 'Confondue avec SV1a', series: 'SV' },
        { id: 'XX', name: 'Série inconnue', series: 'nope' },
        { id: 'OK', name: 'Sans série', series: null, releaseDate: '2999-01-01' },
      ],
      cards: [
        { id: 'SV1a-1', set: 'SV1a', localId: '1', name: 'Même numéro que 001' },
        { id: 'OK-1', set: 'OK', localId: '1', name: 'Bonne' },
        { id: 'OK-01', set: 'OK', localId: '01', name: 'Doublon dans le lot' },
        { id: 'XX-1', set: 'XX', localId: '1', name: 'Extension refusée' },
      ],
    })
    const out = (await res.json()) as { sets: { inserted: number; rejected: { id: string; reason: string }[] }; cards: { inserted: number; rejected: { id: string; reason: string }[] } }
    expect(out.sets.inserted).toBe(1)
    expect(out.sets.rejected.map((r) => r.id)).toEqual(['SV-1A', 'XX'])
    expect(out.cards.inserted).toBe(1)
    expect(out.cards.rejected.map((r) => r.id)).toEqual(['SV1a-1', 'OK-01', 'XX-1'])
    // Une date à venir n'est pas enregistrée (elle ferait de l'extension la page par défaut du site).
    expect(await one(`SELECT release_date, series_id FROM sets WHERE id = 'ja:OK'`)).toEqual({ release_date: null, series_id: null })
  })

  it('refuse les formes qui casseraient la tâche du site (400, rien d’écrit)', async () => {
    const bad = [
      { language: 'en', sets: [], cards: [] },
      { language: 'fr', sets: [{ id: 'A+B', name: 'plus interdit' }] },
      { language: 'fr', cards: [{ id: 'swsh3-x', set: 'swsh3', localId: 'X-1', name: 'numéro illisible' }] },
      { language: 'fr', cards: [{ id: 'swsh3-500', set: 'swsh3', localId: '500', name: 'cat', category: 'Pokemon' }] },
      { language: 'fr', cards: [{ id: 'swsh3-500', set: 'swsh3', localId: '500', name: 'méta', metadata: { dexId: '25' } }] },
      { language: 'fr', cards: [{ id: 'swsh3-500', set: 'swsh3', localId: '500', name: 'méta', metadata: { variants: [] } }] },
      { language: 'fr', cards: [{ id: 'swsh3-500', set: 'swsh3', localId: '500', name: ' espace' }] },
    ]
    for (const body of bad) {
      const res = await catalog(body)
      expect(res.status, JSON.stringify(body)).toBe(400)
    }
    expect(await one(`SELECT COUNT(*) AS n FROM cards WHERE data_source IS NOT NULL`)).toEqual({ n: 0 })
    expect((await admin('POST', '/admin/import/catalog', '{pas du json')).status).toBe(400)
    expect(() => parseCatalogBatch({ language: 'fr', cards: [{ id: 'a b', set: 'x', localId: '1', name: 'n' }] }, '2026-11-24')).toThrow(InputError)
  })

  it('exige le jeton', async () => {
    expect((await admin('POST', '/admin/import/catalog', JSON.stringify(sampleBatch()), 'Bearer faux')).status).toBe(401)
    expect(await one(`SELECT COUNT(*) AS n FROM sets WHERE data_source IS NOT NULL`)).toEqual({ n: 0 })
  })
})

describe('POST /admin/import/image', () => {
  beforeEach(async () => {
    await importCatalog(new Db(env.DB), sampleBatch(), clock.now())
  })

  it('image principale : écrite à la clé de la carte, D1 mis à jour, jamais réécrite', async () => {
    const bytes = webp(400, 7)
    const res = await image('kind=main&language=ja&target=VM1-1', bytes)
    expect(res.status).toBe(201)
    const sha = await sha256Hex(bytes)
    expect(await res.json()).toEqual({ status: 'stored', key: 'cards/ja/VM1/VM1-1.webp', size: 400, sha256: sha })
    const object = await env.BUCKET.head('cards/ja/VM1/VM1-1.webp')
    expect(object?.customMetadata).toMatchObject({ source: 'pokecardex', sourceUrl: SOURCE, sha256: sha, tcgdexId: 'VM1-1' })
    expect(object?.httpMetadata?.contentType).toBe('image/webp')
    expect(await one(`SELECT image_status, image_source, image_source_url, image_r2_key, image_file_size, image_sha256, image_provider FROM cards WHERE id = 'ja:VM1-1'`)).toEqual({
      image_status: 'downloaded',
      image_source: null,
      image_source_url: SOURCE,
      image_r2_key: 'cards/ja/VM1/VM1-1.webp',
      image_file_size: 400,
      image_sha256: sha,
      image_provider: 'pokecardex',
    })
    // Un second envoi (autre contenu) ne change rien.
    const again = await image('kind=main&language=ja&target=VM1-1', webp(500, 9))
    expect(again.status).toBe(200)
    expect(await again.json()).toMatchObject({ status: 'unchanged' })
    expect((await env.BUCKET.head('cards/ja/VM1/VM1-1.webp'))?.size).toBe(400)
  })

  it('ne touche jamais une carte qui a déjà une image TCGdex', async () => {
    const before = await one(`SELECT * FROM cards WHERE id = 'ja:SV1a-001'`)
    const object = await env.BUCKET.head('cards/ja/SV1a/SV1a-001.webp')
    const res = await image('kind=main&language=ja&target=SV1a-001', webp(400, 7))
    expect(await res.json()).toMatchObject({ status: 'unchanged' })
    expect(await one(`SELECT * FROM cards WHERE id = 'ja:SV1a-001'`)).toEqual(before)
    expect((await env.BUCKET.head('cards/ja/SV1a/SV1a-001.webp'))?.etag).toBe(object?.etag)
    // La petite image d'une image TCGdex n'est pas acceptée non plus.
    expect((await image('kind=low&language=ja&target=SV1a-001', webp(200, 7))).status).toBe(409)
  })

  it('complète une carte TCGdex sans image (introuvable sur le CDN)', async () => {
    expect(await one(`SELECT image_status FROM cards WHERE id = 'fr:swsh3-138'`)).toEqual({ image_status: 'not_found_on_cdn' })
    const res = await image('kind=main&language=fr&target=swsh3-138', webp(400, 7))
    expect(res.status).toBe(201)
    expect(await one(`SELECT image_status, image_provider, data_source, image_failure_reason FROM cards WHERE id = 'fr:swsh3-138'`)).toEqual({
      image_status: 'downloaded',
      image_provider: 'pokecardex',
      data_source: null,
      image_failure_reason: null,
    })
    // Relancer les images manquantes ne la touche plus.
    const retry = await retryMissing(new Db(env.DB), makeDeps(fake, clock).config, clock.now(), ['fr'])
    expect(retry.images).toBe(1) // exu-%3F seulement
  })

  it('objet R2 déjà présent : jamais écrasé ; même contenu : réconcilié', async () => {
    await env.BUCKET.put('cards/ja/VM1/VM1-2.webp', webp(300, 3), { customMetadata: { sha256: 'autre' } })
    const conflict = await image('kind=main&language=ja&target=VM1-2', webp(400, 7))
    expect(conflict.status).toBe(409)
    expect((await env.BUCKET.head('cards/ja/VM1/VM1-2.webp'))?.size).toBe(300)
    expect(await one(`SELECT image_status FROM cards WHERE id = 'ja:VM1-2'`)).toEqual({ image_status: 'pending' })

    const bytes = webp(400, 7)
    await env.BUCKET.put('cards/ja/SV1a/SV1a-099.webp', bytes, { customMetadata: { sha256: await sha256Hex(bytes) } })
    const same = await image('kind=main&language=ja&target=SV1a-099', bytes)
    expect(same.status).toBe(200)
    expect(await same.json()).toMatchObject({ status: 'reconciled', key: 'cards/ja/SV1a/SV1a-099.webp' })
    expect(await one(`SELECT image_status, image_provider FROM cards WHERE id = 'ja:SV1a-099'`)).toEqual({ image_status: 'downloaded', image_provider: 'pokecardex' })
  })

  it('petite image : seulement après l’image principale de la même source, à la clé .low.webp', async () => {
    expect((await image('kind=low&language=ja&target=VM1-1', webp(200, 2))).status).toBe(409)
    await image('kind=main&language=ja&target=VM1-1', webp(400, 7))
    const res = await image('kind=low&language=ja&target=VM1-1', webp(200, 2))
    expect(res.status).toBe(201)
    expect(await one(`SELECT image_low_status, image_low_r2_key, image_low_file_size FROM cards WHERE id = 'ja:VM1-1'`)).toEqual({
      image_low_status: 'downloaded',
      image_low_r2_key: 'cards/ja/VM1/VM1-1.low.webp',
      image_low_file_size: 200,
    })
    expect(await image('kind=low&language=ja&target=VM1-1', webp(210, 2)).then((r) => r.json())).toMatchObject({ status: 'unchanged' })
  })

  it('logo : à la clé de l’extension, reconnu par la tâche de logo, jamais remplacé', async () => {
    const bytes = webp(600, 5)
    const res = await image('kind=logo&language=ja&target=VM1', bytes)
    expect(res.status).toBe(201)
    expect(await one(`SELECT logo_status, logo_r2_key, logo_source_url, logo_file_size, logo_provider FROM sets WHERE id = 'ja:VM1'`)).toEqual({
      logo_status: 'downloaded',
      logo_r2_key: 'cards/ja/VM1/_logo.webp',
      logo_source_url: SOURCE,
      logo_file_size: 600,
      logo_provider: 'pokecardex',
    })
    expect((await env.BUCKET.head('cards/ja/VM1/_logo.webp'))?.customMetadata).toMatchObject({ setId: 'VM1', sha256: await sha256Hex(bytes) })
    expect(await image('kind=logo&language=ja&target=VM1', webp(700, 5)).then((r) => r.json())).toMatchObject({ status: 'unchanged' })
    // Un logo TCGdex déjà copié n'est jamais remplacé.
    await env.DB.prepare(`UPDATE sets SET logo_status = 'downloaded', logo_r2_key = 'cards/fr/swsh3/_logo.webp' WHERE id = 'fr:swsh3'`).run()
    expect(await image('kind=logo&language=fr&target=swsh3', webp(700, 5)).then((r) => r.json())).toMatchObject({ status: 'unchanged' })
    expect(await env.BUCKET.head('cards/fr/swsh3/_logo.webp')).toBeNull()
  })

  it('refuse un fichier qui n’est pas un WebP, une cible inconnue, des paramètres invalides', async () => {
    const png = new Uint8Array(400).fill(7)
    expect((await image('kind=main&language=ja&target=VM1-1', png)).status).toBe(422)
    expect((await image('kind=main&language=ja&target=inconnue', webp(400, 1))).status).toBe(404)
    expect((await image('kind=autre&language=ja&target=VM1-1', webp(400, 1))).status).toBe(400)
    expect((await admin('POST', '/admin/import/image?kind=main&language=ja&target=VM1-1&source=ftp://x', webp(400, 1))).status).toBe(400)
    expect((await image('kind=main&language=ja&target=VM1-1', new Uint8Array(0))).status).toBe(413)
    expect(await one(`SELECT image_status FROM cards WHERE id = 'ja:VM1-1'`)).toEqual({ image_status: 'pending' })
    expect((await env.BUCKET.list({ prefix: 'cards/ja/VM1/' })).objects).toEqual([])
  })
})

describe('cohabitation avec la synchronisation TCGdex', () => {
  it('une carte ajoutée n’est jamais retirée ; si TCGdex la liste ensuite, il la reprend', async () => {
    await importCatalog(new Db(env.DB), sampleBatch(), clock.now())
    await image('kind=main&language=ja&target=SV1a-099', webp(400, 7))
    const deps = makeDeps(fake, clock)
    clock.advance(8 * 86_400_000)
    await startSync(new Db(env.DB), deps.config, clock.now(), { languages: ['ja'], scope: {} })
    await drain(deps, clock)
    expect(await one(`SELECT removed_at, data_source, image_status FROM cards WHERE id = 'ja:SV1a-099'`)).toEqual({ removed_at: null, data_source: 'pokecardex', image_status: 'downloaded' })
    expect(await one(`SELECT removed_at FROM cards WHERE id = 'ja:VM1-1'`)).toEqual({ removed_at: null })

    // TCGdex publie la carte sous le même id : la ligne devient TCGdex, l'image déjà copiée reste.
    fake.sets.ja[0]!.cards.push({ id: 'SV1a-099', localId: '099', name: 'カード公式' })
    clock.advance(8 * 86_400_000)
    await startSync(new Db(env.DB), deps.config, clock.now(), { languages: ['ja'], scope: {} })
    await drain(deps, clock)
    expect(await one(`SELECT name, data_source, image_status, image_provider, brief_hash IS NOT NULL AS listed FROM cards WHERE id = 'ja:SV1a-099'`)).toEqual({
      name: 'カード公式',
      data_source: null,
      image_status: 'downloaded',
      image_provider: 'pokecardex',
      listed: 1,
    })
    expect(await rows(`SELECT key FROM import_jobs WHERE status = 'failed'`).catch(() => [])).toEqual([])
  })
})
