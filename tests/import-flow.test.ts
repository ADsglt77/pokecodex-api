/**
 * Parcours complet sur le petit catalogue de référence : synchronisation FR + JP, détails, images
 * (officielles, reconstruites, absentes), rapport, puis idempotence d'une seconde synchronisation.
 */

import { env } from 'cloudflare:workers'
import { beforeEach, describe, expect, it } from 'vitest'
import { Db } from '../src/db'
import { startSync } from '../src/services/sync'
import { ASSETS, Clock, drain, makeDeps, one, resetAll, rows, sampleCatalog, sha256Hex, webp } from './helpers'

beforeEach(resetAll)

describe('importation complète', () => {
  it('importe séries, extensions, cartes et images FR et JP', async () => {
    const fake = sampleCatalog()
    const clock = new Clock()
    const deps = makeDeps(fake, clock)
    const started = await startSync(new Db(env.DB), deps.config, clock.now())
    expect(started.started.map((run) => [run.language, run.kind])).toEqual([
      ['fr', 'initial'],
      ['ja', 'initial'],
    ])

    const ticks = await drain(deps, clock)
    expect(ticks.at(-1)?.status).toBe('idle')

    const cards = await rows<{ id: string; image_status: string; image_source: string | null; image_r2_key: string | null; detail_status: string; rarity: string | null; metadata_json: string }>(
      `SELECT id, image_status, image_source, image_r2_key, detail_status, rarity, metadata_json FROM cards ORDER BY id`,
    )
    const byId = Object.fromEntries(cards.map((card) => [card.id, card]))
    expect(Object.keys(byId).sort()).toEqual(['fr:exu-%3F', 'fr:swsh3-136', 'fr:swsh3-137', 'fr:swsh3-138', 'ja:SM1+-001', 'ja:SV1a-001', 'ja:SV1a-002'])
    expect(byId['fr:swsh3-136']).toMatchObject({ image_status: 'downloaded', image_source: 'api', image_r2_key: 'cards/fr/swsh3/swsh3-136.webp', detail_status: 'fetched', rarity: 'Commune' })
    expect(byId['fr:swsh3-137']).toMatchObject({ image_status: 'recovered_from_cdn', image_source: 'cdn_reconstructed' })
    expect(byId['fr:swsh3-138']?.image_status).toBe('not_found_on_cdn')
    expect(byId['fr:exu-%3F']?.image_status).toBe('not_found_on_cdn')
    expect(byId['ja:SV1a-001']?.image_status).toBe('downloaded')
    expect(byId['ja:SV1a-002']?.image_status).toBe('recovered_from_cdn')
    expect(byId['ja:SM1+-001']?.image_status).toBe('not_found_on_cdn')
    // Les prix (données de marché volatiles) ne sont jamais conservés ; les identifiants produits oui.
    expect(byId['fr:swsh3-136']?.metadata_json).not.toContain('pricing')
    expect(byId['fr:swsh3-136']?.metadata_json).toContain('"cardmarket":123')

    // L'image est réellement dans R2, intacte, avec son SHA-256 en base.
    const object = await env.BUCKET.get('cards/fr/swsh3/swsh3-136.webp')
    expect(object).not.toBeNull()
    const stored = new Uint8Array(await (object as R2ObjectBody).arrayBuffer())
    expect(stored).toEqual(webp(300, 1))
    const card = await one<{ image_sha256: string; image_file_size: number; image_quality: string }>(`SELECT image_sha256, image_file_size, image_quality FROM cards WHERE id = 'fr:swsh3-136'`)
    expect(card).toEqual({ image_sha256: await sha256Hex(webp(300, 1)), image_file_size: 300, image_quality: 'high' })

    // Clés R2 sûres pour les identifiants à caractères spéciaux.
    const missing = await one<{ image_tested_urls: string }>(`SELECT image_tested_urls FROM cards WHERE id = 'fr:exu-%3F'`)
    expect(JSON.parse(missing.image_tested_urls)).toEqual([{ url: `${ASSETS}/fr/ex/exu/%253F/high.webp`, status: 404 }])

    // Synchronisations closes avec leur bilan, rapport produit.
    const runs = await rows<{ language: string; status: string; cards_found: number; images_downloaded: number; images_recovered: number; images_missing: number }>(
      `SELECT language, status, cards_found, images_downloaded, images_recovered, images_missing FROM sync_runs ORDER BY language`,
    )
    expect(runs).toEqual([
      { language: 'fr', status: 'completed', cards_found: 4, images_downloaded: 1, images_recovered: 1, images_missing: 2 },
      { language: 'ja', status: 'completed', cards_found: 3, images_downloaded: 1, images_recovered: 1, images_missing: 1 },
    ])
    expect(await env.BUCKET.head('reports/latest/summary.json')).not.toBeNull()
    expect(await env.BUCKET.head('reports/latest/missing_fr.csv')).not.toBeNull()

    // Aucune tâche active ni en échec.
    const jobs = await rows<{ status: string; n: number }>(`SELECT status, COUNT(*) AS n FROM import_jobs GROUP BY status`)
    expect(jobs).toEqual([{ status: 'done', n: expect.any(Number) }])
  })

  it('une seconde synchronisation ne retélécharge rien et ne crée aucun doublon', async () => {
    const fake = sampleCatalog()
    const clock = new Clock()
    const deps = makeDeps(fake, clock)
    await startSync(new Db(env.DB), deps.config, clock.now())
    await drain(deps, clock)
    const before = await rows(`SELECT id, image_sha256, updated_at FROM cards ORDER BY id`)
    const jobsBefore = (await one<{ n: number }>(`SELECT COUNT(*) AS n FROM import_jobs`)).n
    const imageRequests = fake.count((url) => url.endsWith('.webp'))

    clock.advance(7 * 86_400_000)
    await startSync(new Db(env.DB), deps.config, clock.now(), { kind: 'weekly' })
    await drain(deps, clock)

    expect(await rows(`SELECT id, image_sha256, updated_at FROM cards ORDER BY id`)).toEqual(before)
    expect((await one<{ n: number }>(`SELECT COUNT(*) AS n FROM import_jobs`)).n).toBe(jobsBefore)
    // Aucune image valide retéléchargée, aucune image absente revérifiée avant 30 jours.
    expect(fake.count((url) => url.endsWith('.webp'))).toBe(imageRequests)
    expect(fake.count((url) => url.includes('/cards/'))).toBe(fake.count((url) => url.includes('/cards/')))
  })
})
