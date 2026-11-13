/**
 * Réponses de l'API TCGdex v2 (https://api.tcgdex.net/v2), validées avant usage : une réponse
 * inattendue ne doit jamais écrire de données incohérentes en base.
 *
 * Formes mesurées le 13/11/2026 :
 *   GET /{lang}/series        → [{ id, name, logo? }]
 *   GET /{lang}/sets          → [{ id, name, logo?, symbol?, cardCount: { total, official } }]
 *   GET /{lang}/sets/{id}     → { id, name, logo?, symbol?, cardCount, releaseDate?, serie: { id, name },
 *                                 legal?, cards: [{ id, localId, name, image? }] }
 *   GET /{lang}/cards         → [{ id, localId, name, image? }]
 *   GET /{lang}/cards/{id}    → { id, localId, name, image?, category, rarity?, illustrator?,
 *                                 set: { id, name, logo?, symbol?, cardCount }, variants, updated, … }
 * `image` est une base d'URL sans qualité ni extension (ex. https://assets.tcgdex.net/fr/swsh/swsh3/136).
 */

export interface SeriesBrief {
  id: string
  name: string
  logo: string | null
}

export interface CardCount {
  total: number | null
  official: number | null
}

export interface SetBrief {
  id: string
  name: string
  logo: string | null
  symbol: string | null
  cardCount: CardCount
}

export interface CardBrief {
  id: string
  localId: string
  name: string
  image: string | null
}

export interface SetDetail extends SetBrief {
  releaseDate: string | null
  serie: { id: string; name: string } | null
  cards: CardBrief[]
  /** Réponse brute (sans la liste des cartes ni les prix), conservée en base. */
  raw: Record<string, unknown>
}

export interface CardDetail extends CardBrief {
  category: string | null
  rarity: string | null
  illustrator: string | null
  set: SetBrief | null
  /** Réponse brute sans les prix, conservée en base. */
  raw: Record<string, unknown>
}

type Json = Record<string, unknown>

const isObject = (value: unknown): value is Json => typeof value === 'object' && value !== null && !Array.isArray(value)

const str = (value: unknown): string | null => (typeof value === 'string' && value.trim() !== '' ? value : null)

const num = (value: unknown): number | null => (typeof value === 'number' && Number.isFinite(value) ? value : null)

/** Identifiant TCGdex acceptable : non vide, sans caractère de contrôle ni barre oblique. */
const isValidId = (value: string | null): value is string =>
  value !== null && value.length <= 200 && value !== '.' && value !== '..' && !/[\u0000-\u001f\u007f/\\]/.test(value)

/**
 * Retire récursivement les objets `pricing` (prix Cardmarket/TCGplayer quotidiens) : données de
 * marché volatiles, hors du périmètre de cette version. Les identifiants produits (`thirdParty`)
 * sont gardés pour la future intégration des marketplaces.
 */
export function stripPricing<T>(value: T): T {
  if (Array.isArray(value)) return value.map(stripPricing) as T
  if (!isObject(value)) return value
  const out: Json = {}
  for (const [key, inner] of Object.entries(value)) if (key !== 'pricing') out[key] = stripPricing(inner)
  return out as T
}

function parseCardCount(value: unknown): CardCount {
  const count = isObject(value) ? value : {}
  return { total: num(count.total), official: num(count.official) }
}

export function parseSeriesList(data: unknown): SeriesBrief[] {
  if (!Array.isArray(data)) throw new TypeError('liste des séries : tableau attendu')
  return data.flatMap((item) => {
    if (!isObject(item)) return []
    const id = str(item.id)
    if (!isValidId(id)) return []
    return [{ id, name: str(item.name) ?? id, logo: str(item.logo) }]
  })
}

function parseSetBrief(item: unknown): SetBrief | null {
  if (!isObject(item)) return null
  const id = str(item.id)
  if (!isValidId(id)) return null
  return { id, name: str(item.name) ?? id, logo: str(item.logo), symbol: str(item.symbol), cardCount: parseCardCount(item.cardCount) }
}

export function parseSetList(data: unknown): SetBrief[] {
  if (!Array.isArray(data)) throw new TypeError('liste des extensions : tableau attendu')
  return data.map(parseSetBrief).filter((set): set is SetBrief => set !== null)
}

export function parseCardBrief(item: unknown): CardBrief | null {
  if (!isObject(item)) return null
  const id = str(item.id)
  const localId = str(item.localId)
  if (!isValidId(id) || localId === null) return null
  return { id, localId, name: str(item.name) ?? '', image: str(item.image) }
}

export function parseCardList(data: unknown): CardBrief[] {
  if (!Array.isArray(data)) throw new TypeError('liste des cartes : tableau attendu')
  return data.map(parseCardBrief).filter((card): card is CardBrief => card !== null)
}

export function parseSetDetail(data: unknown): SetDetail {
  const brief = parseSetBrief(data)
  if (!brief || !isObject(data)) throw new TypeError('détail d’extension : objet avec un id attendu')
  const serie = isObject(data.serie) && isValidId(str(data.serie.id))
    ? { id: str(data.serie.id) as string, name: str(data.serie.name) ?? (str(data.serie.id) as string) }
    : null
  const cards = Array.isArray(data.cards) ? data.cards.map(parseCardBrief).filter((card): card is CardBrief => card !== null) : []
  const { cards: _cards, ...rest } = data
  return { ...brief, releaseDate: str(data.releaseDate), serie, cards, raw: stripPricing(rest) }
}

export function parseCardDetail(data: unknown): CardDetail {
  const brief = parseCardBrief(data)
  if (!brief || !isObject(data)) throw new TypeError('détail de carte : objet avec id et localId attendu')
  return {
    ...brief,
    category: str(data.category),
    // L'API japonaise écrit parfois la chaîne littérale "None" : rareté inconnue.
    rarity: str(data.rarity) === 'None' ? null : str(data.rarity),
    illustrator: str(data.illustrator),
    set: parseSetBrief(data.set),
    raw: stripPricing(data),
  }
}
