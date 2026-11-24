#!/usr/bin/env node
/**
 * Prépare l'import des cartes absentes de TCGdex depuis la copie locale Pokécardex du 28/10/2026
 * (base D1 et R2 locaux de pokedex-data, faits sous l'ancien accord). Lecture seule de la copie ;
 * aucun accès réseau. Écrit dans le dossier de sortie :
 *
 *   catalog-<langue>-<n>.json  lots pour POST /admin/import/catalog (extensions d'abord, puis cartes)
 *   images.json                images candidates (fichier local, carte cible), à vérifier avant envoi
 *   logos.json                 logos candidats (fichier local, extension cible)
 *   rapport.json               ce qui est écarté, et pourquoi
 *
 *   node scripts/pokecardex/prepare.mjs <base D1 locale .sqlite> <dossier R2 local> <dossier des entrées> <dossier de sortie>
 *
 * Entrées (dossier des entrées) : rapprochement.json et rapprochement-cartes.json (extensions et
 * cartes Pokécardex ↔ TCGdex), noms-japonais-cartes.json (noms japonais retrouvés), verdicts.json
 * (langue réelle des cartes de chaque extension, recensement visuel), production-sets.json et
 * production-cards.json (état de pokecodex-cards le 23/11).
 *
 * Règles du contrat du site (pokedex-data) : id d'extension `[A-Za-z0-9._-]`, numéro
 * `^[a-z]*\d+[a-z]*$` unique dans l'extension (numberKey), catégories et raretés dans le vocabulaire
 * TCGdex de la langue, métadonnées au format TCGdex (dexId, types, trainerType, variants_detailed avec
 * l'id Cardmarket), aucun nom de source dans les ids et les clés.
 */

import fs from 'node:fs'
import path from 'node:path'
import { DatabaseSync } from 'node:sqlite'

const [dbPath, r2Dir, inDir, outDir] = process.argv.slice(2)
if (!outDir) throw new Error('usage : prepare.mjs <base .sqlite> <dossier R2 local> <dossier des entrées> <dossier de sortie>')
fs.mkdirSync(outDir, { recursive: true })
const read = (name) => JSON.parse(fs.readFileSync(path.join(inDir, name), 'utf8'))

const pcx = new DatabaseSync(dbPath, { readOnly: true })
const r2Sqlite = fs.readdirSync(path.join(r2Dir, 'miniflare-R2BucketObject')).find((f) => f.endsWith('.sqlite'))
const r2 = new DatabaseSync(path.join(r2Dir, 'miniflare-R2BucketObject', r2Sqlite), { readOnly: true })
const blobs = new Map(r2.prepare(`SELECT key, blob_id FROM _mf_objects`).all().map((row) => [row.key, path.join(r2Dir, 'pokedex-data', 'blobs', row.blob_id)]))
const blob = (key) => {
  const file = blobs.get(key)
  return file && fs.existsSync(file) ? file : null
}

const rapprochement = read('rapprochement.json')
const cardsMatch = new Map(read('rapprochement-cartes.json').map((r) => [`${r.lang}:${r.pcx_set}`, r]))
const jaNames = new Map(read('noms-japonais-cartes.json').map((r) => [r.pokecardex_id, r]))
const verdicts = new Map(read('verdicts.json').map((v) => [v.key, v]))
const prodSets = read('production-sets.json')
const prodCards = read('production-cards.json')

const LANG = { fr: 'fr', ja: 'ja' } // langue Pokécardex → langue PokéCodex
const today = new Date().toISOString().slice(0, 10)
const report = { excludedSets: [], skippedCards: [], renamedSets: [], notes: [] }

// ——— Vocabulaire ———

const setKey = (id) => id.toUpperCase().replaceAll('-', '')
const numberKey = (n) => n.toUpperCase().replace(/^([A-Z]*)0+(?=\d)/, '$1')
const LOCAL_ID = /^[A-Za-z]*\d+[A-Za-z]*$/
const SET_ID = /^[A-Za-z0-9._-]{1,32}$/

const POKEMON_TYPES = {
  1: ['Plante', 'Grass'],
  2: ['Eau', 'Water'],
  3: ['Feu', 'Fire'],
  4: ['Électrique', 'Lightning'],
  5: ['Psy', 'Psychic'],
  6: ['Obscurité', 'Darkness'],
  7: ['Métal', 'Metal'],
  8: ['Combat', 'Fighting'],
  9: ['Dragon', 'Dragon'],
  10: ['Incolore', 'Colorless'],
  11: ['Fée', 'Fairy'],
}
const TRAINER_TYPES = { 12: ['Stade', 'Stadium'], 14: ['Supporter', 'Supporter'], 15: ['Objet', 'Item'], 41: ['Outil', 'Tool'] }
const CATEGORY = { pokemon: ['Pokémon', 'Pokemon'], trainer: ['Dresseur', 'Trainer'], energy: ['Énergie', 'Energy'] }
const li = (lang) => (lang === 'fr' ? 0 : 1)

/** Catégorie et champs de type, d'après le type Pokécardex (1-11 et 18-40 : Pokémon). */
function typing(lang, type) {
  const i = li(lang)
  if (POKEMON_TYPES[type]) return { category: CATEGORY.pokemon[i], meta: { types: [POKEMON_TYPES[type][i]] } }
  if (TRAINER_TYPES[type]) return { category: CATEGORY.trainer[i], meta: { trainerType: TRAINER_TYPES[type][i] } }
  if (type === 13) return { category: CATEGORY.trainer[i], meta: {} }
  if (type === 16 || type === 17) return { category: CATEGORY.energy[i], meta: {} }
  if (type >= 18 && type <= 40) return { category: CATEGORY.pokemon[i], meta: {} }
  return { category: null, meta: {} }
}

// Raretés Pokécardex (nom de la table `rarities`) → orthographe TCGdex de la langue (export de production).
const RARITY = {
  fr: {
    Commune: 'Commune', Unco: 'Peu Commune', Rare: 'Rare', Holographique: 'Rare Holo', 'Ultra-rare': 'Ultra Rare', Promo: 'Promo',
    'Sans rareté': 'Sans Rareté', 'Double Rare': 'Double rare', 'Illustration Rare': 'Illustration rare', 'Ultra Rare': 'Ultra Rare',
    'Illustration Spéciale Rare': 'Illustration spéciale rare', 'Hyper Rare': 'Hyper rare', 'Illustration Rare Chromatique': 'Shiny rare',
    'Illustration Spéciale Chromatique': 'Chromatique ultra rare', 'HIGH-TECH rare': 'HIGH-TECH rare', 'Noir Blanc Rare': 'Rare Noir Blanc',
    'Mega Hyper Rare': 'Méga Hyper Rare', 'Méga Attaque Rare': 'Mega Attack Rare', 'Futuriste Rare': 'Futuristic Rare', RGB: 'RGB Rare',
  },
  ja: {
    Commune: 'Common', Unco: 'Uncommon', Rare: 'Rare', Holographique: 'Holo Rare', 'Ultra-rare': 'Ultra Rare', Secrète: 'Secret Rare', Promo: 'Promo',
    C: 'Common', U: 'Uncommon', R: 'Rare', RR: 'Double rare', RRR: 'Triple Rare', SR: 'Ultra Rare', HR: 'Hyper rare', UR: 'Hyper rare',
    S: 'Shiny rare', SSR: 'Shiny Ultra Rare', PROMO: 'Promo', PR: 'Promo', CHR: 'Character Rare', CSR: 'Character Super Rare', K: 'Radiant Rare',
    AR: 'Illustration rare', SAR: 'Special illustration rare', ACE: 'ACE SPEC Rare', BWR: 'Black White Rare', MUR: 'Mega Hyper Rare',
    'Mega Hyper Rare': 'Mega Hyper Rare', FUR: 'Futuristic Rare', RGB: 'RGB Rare', Shiny: 'Shiny rare',
  },
}
const rarityNames = new Map(pcx.prepare(`SELECT lang, id, name FROM rarities`).all().map((r) => [`${r.lang}:${r.id}`, r.name]))
function rarity(lang, id) {
  const name = rarityNames.get(`${lang}:${id}`)
  if (!name || name === 'Sans rareté') return lang === 'fr' && name ? 'Sans Rareté' : null
  return RARITY[lang][name] ?? name
}

/** Versions Pokécardex → `variants_detailed` TCGdex (la première version normale porte l'id Cardmarket). */
function variants(lang, versions, cardmarket) {
  const first = lang === 'fr' ? '1re Édition' : '1st Edition'
  const out = []
  for (const raw of versions) {
    let v
    if (raw === 'Standard') v = { type: 'normal' }
    else if (raw === 'Édition 1') v = { type: 'normal', stamp: [first] }
    else if (/^Édition 1 \(Reverse\)$/.test(raw)) v = { type: 'reverse', stamp: [first] }
    else if (/^Édition 1 \((.+)\)$/.test(raw)) v = { type: 'normal', stamp: [first], foil: raw.slice(11, -1) }
    else if (/^Reverse\b/.test(raw)) v = raw === 'Reverse' ? { type: 'reverse' } : { type: 'reverse', foil: raw.replace(/^Reverse\s*/, '').replace(/^\((.*)\)$/, '$1') }
    else if (/^Non Holo\b/.test(raw)) v = raw === 'Non Holo' ? { type: 'normal' } : { type: 'normal', foil: raw }
    else if (/^Holo\b/.test(raw)) v = raw === 'Holo' ? { type: 'holo' } : { type: 'holo', foil: raw.replace(/^Holo\s*/, '').replace(/^\((.*)\)$/, '$1') }
    else if (/^Tampon \((.+)\)$/.test(raw)) v = { type: 'normal', stamp: [raw.slice(8, -1)] }
    else if (/^Staff \((.+)\)$/.test(raw)) v = { type: 'normal', stamp: ['Staff', raw.slice(7, -1)] }
    else v = { type: raw }
    if (!out.some((o) => JSON.stringify(o) === JSON.stringify(v))) out.push(v)
  }
  if (!out.length) out.push({ type: 'normal' })
  if (cardmarket > 0) {
    const target = out.find((v) => v.type === 'normal' && !v.stamp) ?? out.find((v) => v.type === 'normal') ?? out[0]
    target.thirdParty = { cardmarket }
  }
  return out
}

// ——— Lecture de la copie ———

const pcxSets = new Map(pcx.prepare(`SELECT * FROM sets`).all().map((s) => [`${s.lang}:${s.id}`, s]))
const pcxCards = new Map()
for (const c of pcx.prepare(`SELECT * FROM cards ORDER BY lang, set_id, sort`).all()) {
  const key = `${c.lang}:${c.set_id}`
  if (!pcxCards.has(key)) pcxCards.set(key, [])
  pcxCards.get(key).push(c)
}
const cardById = new Map([...pcxCards.values()].flat().map((c) => [`${c.lang}:${c.id}`, c]))
const species = new Map()
for (const s of pcx.prepare(`SELECT lang, card, dex_id FROM card_species ORDER BY lang, card, rank`).all()) {
  const key = `${s.lang}:${s.card}`
  if (!species.has(key)) species.set(key, [])
  if (!species.get(key).includes(s.dex_id)) species.get(key).push(s.dex_id)
}

// État de production : extensions, séries, cartes (numéros et images).
const prodSetIds = { fr: new Map(), ja: new Map() }
for (const s of prodSets) prodSetIds[s.language].set(s.set_id, s)
const prodByCard = new Map(prodCards.map((c) => [`${c.language}:${c.tcgdex_id}`, c]))
const prodNumbers = new Map()
for (const c of prodCards) {
  const key = `${c.language}:${c.set_id}`
  if (!prodNumbers.has(key)) prodNumbers.set(key, [])
  prodNumbers.get(key).push(c.local_id)
}
const VALID = new Set(['downloaded', 'recovered_from_cdn', 'already_present'])

// Bloc Pokécardex → série TCGdex : vote des extensions rapprochées du même bloc.
const votes = new Map()
for (const r of rapprochement) {
  if (!r.tcgdex_set) continue
  const lang = LANG[r.lang]
  const block = pcxSets.get(`${r.lang}:${r.pcx_set}`)?.block
  const serie = prodSetIds[lang].get(r.tcgdex_set)?.serie_id
  if (!block || !serie || serie === 'tcgp') continue
  const key = `${lang}:${block}`
  if (!votes.has(key)) votes.set(key, new Map())
  votes.get(key).set(serie, (votes.get(key).get(serie) ?? 0) + 1)
}
// Séries absentes de TCGdex (le japonais n'a ni DP ni Platine) : créées par le premier lot.
const NEW_SERIES = { fr: [], ja: [{ id: 'DP', name: 'ダイヤモンド&パール' }, { id: 'DPt', name: 'プラチナ' }] }
const EXPLICIT = { fr: {}, ja: { 'Diamond & Pearl Era': 'DP', 'Platinum Era': 'DPt' } }
// Blocs qui couvrent plusieurs époques : rattachés à la série de l'époque de leur date de sortie.
const BY_DATE = { fr: ['Promos', 'Ligue Pokémon', 'Divers'], ja: ['Promotionals', 'Movies Commemoration'] }
// Séries thématiques, jamais choisies par date.
const THEMATIC = { fr: ['tcgp', 'mc', 'pop', 'tk', 'misc', 'col'], ja: ['web', 'VS'] }
const eras = { fr: new Map(), ja: new Map() }
const widen = (lang, serie, date) => {
  if (!date || THEMATIC[lang].includes(serie)) return
  const e = eras[lang].get(serie) ?? { min: date, max: date }
  eras[lang].set(serie, { min: date < e.min ? date : e.min, max: date > e.max ? date : e.max })
}
for (const s of prodSets) widen(s.language, s.serie_id, (s.release_date || '').slice(0, 10))
for (const s of pcxSets.values()) {
  const serie = EXPLICIT[LANG[s.lang]]?.[s.block]
  if (serie && /^\d{4}-\d{2}-\d{2}$/.test(s.released ?? '') && s.released > '1990') widen(LANG[s.lang], serie, s.released)
}
function seriesOf(lang, block, date) {
  if (EXPLICIT[lang][block]) return EXPLICIT[lang][block]
  if (BY_DATE[lang].includes(block)) {
    if (!date) return null
    let best = null
    for (const [serie, e] of eras[lang]) if (e.min <= date && (!best || e.min > best.min)) best = { serie, min: e.min }
    return best?.serie ?? null
  }
  const v = votes.get(`${lang}:${block}`)
  return v ? [...v].sort((a, b) => b[1] - a[1])[0][0] : null
}

// ——— Construction ———

const out = { fr: { sets: [], cards: [] }, ja: { sets: [], cards: [] } }
const images = []
const logos = []
const usedSetKeys = { fr: new Map(), ja: new Map() }
for (const lang of ['fr', 'ja']) for (const id of prodSetIds[lang].keys()) usedSetKeys[lang].set(setKey(id), id)

function cardMeta(lang, c, extra = {}) {
  const t = typing(lang, c.type)
  const meta = { source: 'pokecardex', ...t.meta }
  const dex = species.get(`${c.lang}:${c.id}`)
  if (dex?.length) meta.dexId = dex
  meta.variants_detailed = variants(lang, JSON.parse(c.versions || '[]'), c.cardmarket || 0)
  meta.pokecardex = { id: c.id, set: c.set_id, sort: c.sort, number: c.number || null, printed: c.printed || null, name_fr: c.name, name_en: c.name_en || null, name_de: c.name_de || null }
  return { category: t.category, meta: { ...meta, ...extra } }
}

function cardName(lang, c) {
  if (lang === 'fr') return { name: c.name.trim(), extra: {} }
  const ja = jaNames.get(c.id)
  if (ja?.name) return { name: ja.name.trim(), extra: { name_ja: { status: ja.name_ja?.status ?? null, method: ja.name_ja?.method ?? null } } }
  return { name: c.name.trim(), extra: { name_ja: { status: 'fallback', method: 'nom français' } } }
}

/** Numéro au format de l'extension TCGdex (zéros de tête si elle en met). */
function tcgdexNumber(lang, setId, number) {
  if (!number || !LOCAL_ID.test(number)) return null
  const existing = prodNumbers.get(`${lang}:${lang}:${setId}`) ?? prodNumbers.get(`${lang}:${setId}`) ?? []
  const padded = existing.find((n) => /^0\d/.test(n))
  if (/^\d+$/.test(number)) {
    const bare = String(Number(number))
    return padded ? bare.padStart(padded.length, '0') : bare
  }
  return number
}

const imageCandidate = (lang, c, setId, cardId, kind) => {
  const prefix = lang === 'fr' ? `images/cards/fr/${c.set_id}` : `images/cards/${c.set_id}`
  const hd = blob(`${prefix}/${c.sort}-hd.webp`)
  const md = blob(`${prefix}/${c.sort}-md.webp`)
  if (hd) images.push({ language: lang, pcxSet: c.set_id, pcxId: c.id, sort: c.sort, set: setId, card: cardId, kind, hd, md, source: lang === 'fr' ? `https://pokecardex-scans.b-cdn.net/sets/${c.set_id}/FR/${c.sort}.jpg` : `https://pokecardex-scans.b-cdn.net/sets_jp/${c.set_id}/${c.sort}.jpg` })
}

for (const r of rapprochement) {
  const lang = LANG[r.lang]
  const key = `${r.lang}:${r.pcx_set}`
  const v = verdicts.get(key)
  if (['hors_perimetre', 'vide'].includes(r.category)) {
    report.excludedSets.push({ key, reason: r.category })
    continue
  }
  if (v?.card_language !== lang) {
    report.excludedSets.push({ key, reason: `langue réelle des cartes : ${v?.card_language ?? 'inconnue'}` })
    continue
  }
  const source = pcxSets.get(key)
  const all = pcxCards.get(key) ?? []
  const logoKey = lang === 'fr' ? `images/sets/fr/${r.pcx_set}-hd.webp` : `images/sets/${r.pcx_set}-hd.webp`

  if (r.tcgdex_set) {
    // Extension TCGdex existante : cartes manquantes ajoutées sous ses ids, images manquantes complétées.
    const match = cardsMatch.get(key)
    if (!match) continue
    if (match.tcgdex_sets.length !== 1) {
      if (match.pcx_only.length) report.notes.push({ key, note: `${match.pcx_only.length} carte(s) non ajoutées : plusieurs extensions TCGdex (${match.tcgdex_sets.join(', ')})` })
    } else {
      const setId = match.tcgdex_sets[0]
      for (const [pcxId, number] of match.pcx_only) {
        const c = cardById.get(`${r.lang}:${pcxId}`)
        const localId = tcgdexNumber(lang, setId, number)
        if (!c || !localId) {
          report.skippedCards.push({ key, pcxId, reason: 'sans numéro exploitable dans une extension TCGdex' })
          continue
        }
        const id = `${setId}-${localId}`
        const { name, extra } = cardName(lang, c)
        const { category, meta } = cardMeta(lang, c, extra)
        out[lang].cards.push({ id, set: setId, localId, name, rarity: rarity(r.lang, c.rarity), category, illustrator: c.illustrator?.trim() || null, metadata: meta })
        imageCandidate(lang, c, setId, id, 'nouvelle')
      }
    }
    for (const [pcxId, tcgdexId] of match.pairs) {
      const prod = prodByCard.get(`${lang}:${tcgdexId}`)
      const c = cardById.get(`${r.lang}:${pcxId}`)
      if (!c || !prod || VALID.has(prod.image_status) || prod.removed_at) continue
      imageCandidate(lang, c, prod.set_id.slice(lang.length + 1), tcgdexId, 'complément')
    }
    const prodSet = prodSetIds[lang].get(r.tcgdex_set)
    if (prodSet && prodSet.logo_status !== 'downloaded' && match.tcgdex_sets.length === 1) {
      const file = blob(logoKey)
      if (file) logos.push({ language: lang, set: r.tcgdex_set, pcxSet: r.pcx_set, file, source: `https://pokecardex.b-cdn.net/assets/images/${lang === 'fr' ? 'logos' : 'logos_jp'}/${r.pcx_set}.png` })
    }
    continue
  }

  // Nouvelle extension : id = code d'origine (neutre), sauf collision pour le site.
  let setId = r.pcx_set
  if (!SET_ID.test(setId) || usedSetKeys[lang].has(setKey(setId))) {
    const base = setId.replace(/[^A-Za-z0-9._-]/g, '')
    let n = 2
    while (usedSetKeys[lang].has(setKey(`${base}${n}`))) n++
    report.renamedSets.push({ key, from: setId, to: `${base}${n}`, clash: usedSetKeys[lang].get(setKey(setId)) ?? null })
    setId = `${base}${n}`
  }
  usedSetKeys[lang].set(setKey(setId), setId)
  const totals = new Map()
  for (const c of all) {
    const m = /^\s*\S+\s*\/\s*(\d+)\s*$/.exec(c.printed || '')
    if (m) totals.set(Number(m[1]), (totals.get(Number(m[1])) ?? 0) + 1)
  }
  const official = [...totals].sort((a, b) => b[1] - a[1])[0]?.[0] ?? null
  const released = /^\d{4}-\d{2}-\d{2}$/.test(source?.released ?? '') && source.released > '1990' && source.released <= today ? source.released : null
  out[lang].sets.push({
    id: setId,
    name: (source?.name ?? r.pcx_name).trim(),
    series: seriesOf(lang, source?.block, released),
    releaseDate: released,
    officialCards: official,
    totalCards: all.length,
    metadata: { source: 'pokecardex', pokecardex: { code: r.pcx_set, block: source?.block ?? null, deck: !!r.deck, category: r.category } },
  })
  const used = new Set()
  for (const c of all) {
    let localId = c.number && LOCAL_ID.test(c.number) ? c.number : null
    if (!localId || used.has(numberKey(localId))) localId = String(c.sort)
    if (used.has(numberKey(localId))) {
      report.skippedCards.push({ key, pcxId: c.id, reason: `numéro en double (${c.number || c.sort})` })
      continue
    }
    used.add(numberKey(localId))
    const id = `${setId}-${localId}`
    const { name, extra } = cardName(lang, c)
    const { category, meta } = cardMeta(lang, c, extra)
    out[lang].cards.push({ id, set: setId, localId, name, rarity: rarity(r.lang, c.rarity), category, illustrator: c.illustrator?.trim() || null, metadata: meta })
    imageCandidate(lang, c, setId, id, 'nouvelle')
  }
  const file = blob(logoKey)
  if (file) logos.push({ language: lang, set: setId, pcxSet: r.pcx_set, file, source: `https://pokecardex.b-cdn.net/assets/images/${lang === 'fr' ? 'logos' : 'logos_jp'}/${r.pcx_set}.png` })
}

// ——— Écriture ———

let lots = 0
for (const lang of ['fr', 'ja']) {
  const { sets, cards } = out[lang]
  for (let i = 0; i < sets.length; i += 400) {
    const series = i === 0 ? NEW_SERIES[lang] : []
    fs.writeFileSync(path.join(outDir, `catalog-${lang}-${String(++lots).padStart(3, '0')}.json`), JSON.stringify({ language: lang, series, sets: sets.slice(i, i + 400), cards: [] }))
  }
  for (let i = 0; i < cards.length; i += 1000) {
    fs.writeFileSync(path.join(outDir, `catalog-${lang}-${String(++lots).padStart(3, '0')}.json`), JSON.stringify({ language: lang, sets: [], cards: cards.slice(i, i + 1000) }))
  }
}
fs.writeFileSync(path.join(outDir, 'images.json'), JSON.stringify(images, null, 1))
fs.writeFileSync(path.join(outDir, 'logos.json'), JSON.stringify(logos, null, 1))
fs.writeFileSync(path.join(outDir, 'rapport.json'), JSON.stringify(report, null, 1))
const count = (list, f) => list.filter(f).length
console.log(
  JSON.stringify(
    {
      fr: { sets: out.fr.sets.length, cards: out.fr.cards.length, images: count(images, (x) => x.language === 'fr'), logos: count(logos, (x) => x.language === 'fr') },
      ja: { sets: out.ja.sets.length, cards: out.ja.cards.length, images: count(images, (x) => x.language === 'ja'), logos: count(logos, (x) => x.language === 'ja') },
      lots,
      excludedSets: report.excludedSets.length,
      skippedCards: report.skippedCards.length,
      renamedSets: report.renamedSets,
      seriesNull: [...out.fr.sets, ...out.ja.sets].filter((s) => !s.series).length,
    },
    null,
    1,
  ),
)
