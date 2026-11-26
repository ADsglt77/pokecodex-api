#!/usr/bin/env node
/**
 * Envoie à l'importateur des lignes et des images préparées hors TCGdex (`POST /admin/import/*`).
 * Reprise possible à tout moment : chaque réponse est consignée dans un journal, et l'API ne
 * remplace jamais rien (un envoi déjà fait répond « unchanged »).
 *
 *   node scripts/import-manual.mjs catalog <dossier des lots>        lots catalog-*.json, dans l'ordre
 *   node scripts/import-manual.mjs logos <logos.json>                [{ language, set, file, source }]
 *   node scripts/import-manual.mjs images <images.json>              [{ language, card, hd, md, source }]
 *   node scripts/import-manual.mjs symbols <plan.json>               { extensions: [{ id, source, url }] } :
 *        le Worker télécharge lui-même chaque symbole (aucun fichier ne passe par ce poste), une adresse
 *        à la fois, 3 s d'écart chez Pokécardex et 0,3 s chez TCGdex
 *   node scripts/import-manual.mjs logo-urls <plan.json>             même format et même rythme, pour les
 *        logos (`POST /admin/import/logo`)
 *
 * Jeton et adresse : comme scripts/admin.mjs (`.secrets/`, jamais affichés).
 */

import { appendFileSync, existsSync, readdirSync, readFileSync } from 'node:fs'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

const root = fileURLToPath(new URL('..', import.meta.url))
const secret = (name) => {
  try {
    return readFileSync(`${root}.secrets/${name}`, 'utf8').trim()
  } catch {
    return ''
  }
}
const token = process.env.ADMIN_TOKEN?.trim() || secret('admin-token.txt')
const base = (process.env.IMPORTER_URL?.trim() || secret('importer-url.txt')).replace(/\/+$/, '')
if (!token || !base) throw new Error('jeton ou adresse absents (.secrets/)')

const [mode, input] = process.argv.slice(2)
const CONCURRENCY = Number(process.env.CONCURRENCY ?? 4)
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms))

async function post(route, body, contentType, maxAttempts = 6) {
  for (let attempt = 1; ; attempt++) {
    try {
      const res = await fetch(`${base}${route}`, { method: 'POST', headers: { authorization: `Bearer ${token}`, 'content-type': contentType }, body })
      const text = await res.text()
      let json
      try {
        json = JSON.parse(text)
      } catch {
        json = { error: text.slice(0, 300) }
      }
      // Erreurs passagères (5xx, limite) : nouvelle tentative avec attente croissante.
      if ((res.status >= 500 || res.status === 429) && attempt < maxAttempts) {
        await sleep(2_000 * attempt)
        continue
      }
      return { status: res.status, json }
    } catch (error) {
      if (attempt >= maxAttempts) return { status: 0, json: { error: String(error) } }
      await sleep(2_000 * attempt)
    }
  }
}

/** Journal de reprise : une ligne JSON par élément traité ; un élément « fini » n'est pas renvoyé. */
function journal(file) {
  const done = new Set()
  if (existsSync(file)) {
    for (const line of readFileSync(file, 'utf8').split('\n')) {
      if (!line) continue
      const entry = JSON.parse(line)
      if (entry.final) done.add(entry.id)
    }
  }
  return { done, write: (entry) => appendFileSync(file, `${JSON.stringify(entry)}\n`) }
}

async function pool(items, worker) {
  let next = 0
  await Promise.all(
    Array.from({ length: CONCURRENCY }, async () => {
      while (next < items.length) await worker(items[next++])
    }),
  )
}

const FINAL = new Set(['stored', 'reconciled', 'unchanged', 'conflict', 'not_found', 'invalid'])

async function sendImage(log, counts, id, query, file) {
  const bytes = readFileSync(file)
  const { status, json } = await post(`/admin/import/image?${query}`, bytes, 'image/webp')
  const final = FINAL.has(json.status)
  log.write({ id, status, result: json.status ?? null, reason: json.reason ?? json.error ?? null, key: json.key ?? null, final })
  counts[json.status ?? `http_${status}`] = (counts[json.status ?? `http_${status}`] ?? 0) + 1
  return json.status
}

if (mode === 'catalog') {
  const files = readdirSync(input).filter((name) => /^catalog-.*\.json$/.test(name)).sort()
  const log = journal(path.join(input, 'journal-catalogue.jsonl'))
  const total = { series: 0, sets: 0, cards: 0, existing: 0, rejected: 0 }
  for (const name of files) {
    if (log.done.has(name)) continue
    const { status, json } = await post('/admin/import/catalog', readFileSync(path.join(input, name)), 'application/json')
    if (status !== 200) {
      log.write({ id: name, status, error: json.error, final: false })
      throw new Error(`${name} : HTTP ${status} — ${json.error}`)
    }
    log.write({ id: name, status, result: json, final: true })
    total.series += json.series.inserted
    total.sets += json.sets.inserted
    total.cards += json.cards.inserted
    total.existing += json.sets.existing + json.cards.existing
    total.rejected += json.sets.rejected.length + json.cards.rejected.length
    console.log(`${name} : ${json.sets.inserted} extensions, ${json.cards.inserted} cartes ajoutées, ${json.sets.rejected.length + json.cards.rejected.length} refusées`)
  }
  console.log(JSON.stringify(total))
} else if (mode === 'logos') {
  const items = JSON.parse(readFileSync(input, 'utf8'))
  const log = journal(input.replace(/\.json$/, '.journal.jsonl'))
  const counts = {}
  await pool(
    items.filter((item) => !log.done.has(`${item.language}:${item.set}`)),
    (item) => sendImage(log, counts, `${item.language}:${item.set}`, new URLSearchParams({ kind: 'logo', language: item.language, target: item.set, source: item.source }), item.file),
  )
  console.log(JSON.stringify(counts))
} else if (mode === 'images') {
  const items = JSON.parse(readFileSync(input, 'utf8'))
  const log = journal(input.replace(/\.json$/, '.journal.jsonl'))
  const counts = {}
  let seen = 0
  await pool(items, async (item) => {
    const id = `${item.language}:${item.card}`
    if (!log.done.has(`${id}:main`)) {
      const result = await sendImage(log, counts, `${id}:main`, new URLSearchParams({ kind: 'main', language: item.language, target: item.card, source: item.source }), item.hd)
      if (!['stored', 'reconciled', 'unchanged'].includes(result)) return
    }
    if (item.md && !log.done.has(`${id}:low`)) {
      await sendImage(log, counts, `${id}:low`, new URLSearchParams({ kind: 'low', language: item.language, target: item.card, source: item.source }), item.md)
    }
    if (++seen % 500 === 0) console.log(`${seen} / ${items.length}`, JSON.stringify(counts))
  })
  console.log(JSON.stringify(counts))
} else if (mode === 'symbols' || mode === 'logo-urls' || mode === 'card-urls') {
  // Seules les sources acceptées par le Worker ; une adresse à la fois, au rythme de chaque hôte.
  const route = { symbols: '/admin/import/symbol', 'logo-urls': '/admin/import/logo', 'card-urls': '/admin/import/card' }[mode]
  const noun = { symbols: 'symboles', 'logo-urls': 'logos', 'card-urls': 'images de cartes' }[mode]
  const DELAY_MS = { 'pokecardex.b-cdn.net': 3_000, 'assets.tcgdex.net': 300, 'www.pokepedia.fr': 700, 'assets.pokemon.com': 500, 'media.52poke.com': 1_000, 'archives.bulbagarden.net': 1_000 }
  const plan = JSON.parse(readFileSync(input, 'utf8')).extensions
  const items = plan.filter((item) => item.url && new URL(item.url).host in DELAY_MS)
  console.log(`${items.length} ${noun} à demander ; ${plan.length - items.length} entrées du plan écartées (sans adresse ou autre hôte)`)
  // Journal : dernier résultat de chaque extension. Une extension réussie n'est jamais redemandée ;
  // un échec définitif (fichier absent, invalide) seulement tant que l'adresse du plan est la même.
  const logFile = input.replace(/\.json$/, '.journal.jsonl')
  const last = new Map()
  if (existsSync(logFile)) for (const line of readFileSync(logFile, 'utf8').split('\n')) if (line) last.set(JSON.parse(line).id, JSON.parse(line))
  // `not_found` (extension pas encore créée) n'est pas définitif : redemandé au prochain lancement.
  const SUCCESS = ['stored', 'reconciled', 'unchanged', 'conflict']
  const counts = {}
  let seen = 0
  let refusals = 0
  for (const item of items) {
    const previous = last.get(item.id)
    if (previous?.final && (SUCCESS.includes(previous.result) || previous.url === item.url)) continue
    const split = item.id.indexOf(':')
    const query = new URLSearchParams({ language: item.id.slice(0, split), target: item.id.slice(split + 1), source: item.url })
    // Une seule tentative : une panne (502) reste « non finie » dans le journal et sera reprise au
    // prochain lancement, sans jamais relancer la source plus vite que son rythme.
    const { status, json } = await post(`${route}?${query}`, undefined, 'application/json', 1)
    // Refus (403…) et pannes ne sont jamais définitifs : ils seront redemandés au prochain lancement.
    const final = [...SUCCESS, 'invalid', 'source_not_found'].includes(json.status)
    appendFileSync(logFile, `${JSON.stringify({ id: item.id, url: item.url, status, result: json.status ?? null, reason: json.reason ?? json.error ?? null, key: json.key ?? null, final })}\n`)
    const key = json.status ?? `http_${status}`
    counts[key] = (counts[key] ?? 0) + 1
    if (++seen % 50 === 0) console.log(`${seen} / ${items.length}`, JSON.stringify(counts))
    // Source qui refuse, limite ou tombe : arrêt après 3 réponses de suite, ou dès un 429.
    refusals = ['source_refused', 'source_error'].includes(json.status) || status >= 500 ? refusals + 1 : 0
    if (json.httpStatus === 429 || refusals >= 3) {
      console.log(`arrêt : la source refuse ou ne répond plus (${json.reason ?? json.error ?? `HTTP ${status}`})`)
      break
    }
    // Attente pleine après chaque réponse qui a pu contacter la source (jamais moins que son rythme).
    if (!['unchanged', 'reconciled', 'conflict', 'not_found'].includes(json.status)) await sleep(DELAY_MS[new URL(item.url).host])
  }
  console.log(JSON.stringify(counts))
} else if (mode === 'card-overlays') {
  // Images reconstituées : { extensions: [{ id, origin, x, y, w, file }] } (calque PNG + position relative).
  const items = JSON.parse(readFileSync(input, 'utf8')).extensions
  const log = journal(input.replace(/\.json$/, '.journal.jsonl'))
  const counts = {}
  await pool(
    items.filter((item) => !log.done.has(item.id)),
    async (item) => {
      const split = item.id.indexOf(':')
      const query = new URLSearchParams({ language: item.id.slice(0, split), target: item.id.slice(split + 1), origin: item.origin, x: String(item.x), y: String(item.y), w: String(item.w) })
      const { status, json } = await post(`/admin/import/card-overlay?${query}`, readFileSync(item.file), 'image/png')
      log.write({ id: item.id, status, result: json.status ?? null, reason: json.reason ?? json.error ?? null, key: json.key ?? null, final: FINAL.has(json.status) })
      counts[json.status ?? `http_${status}`] = (counts[json.status ?? `http_${status}`] ?? 0) + 1
    },
  )
  console.log(JSON.stringify(counts))
} else {
  throw new Error('mode : catalog, logos, images, symbols, logo-urls, card-urls ou card-overlays')
}
