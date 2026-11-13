/**
 * Prochaine occurrence d'une expression cron à 5 champs (minute heure jour mois jour-de-semaine),
 * en UTC et avec la numérotation des Cron Triggers de Cloudflare : jours de semaine de 1 = dimanche
 * à 7 = samedi, abréviations anglaises acceptées (`MON`, `jan`…). Gère `*`, `*\/n`, `a-b`, `a-b/n`
 * et les listes. Les syntaxes avancées (L, W, #) ne sont pas gérées : la fonction rend alors `null`.
 * Sert uniquement à afficher « Prochaine exécution ».
 */

const MONTHS = ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC']
const WEEKDAYS = ['SUN', 'MON', 'TUE', 'WED', 'THU', 'FRI', 'SAT']

/** Remplace les abréviations par leur numéro Cloudflare (mois 1-12, jours 1-7). */
function replaceNames(text: string, names: string[]): string {
  return text.toUpperCase().replace(/[A-Z]{3}/g, (name) => {
    const index = names.indexOf(name)
    return index === -1 ? name : String(index + 1)
  })
}

function parseField(text: string, min: number, max: number): Set<number> | null {
  const values = new Set<number>()
  for (const part of text.split(',')) {
    const match = /^(\*|\d+(?:-\d+)?)(?:\/(\d+))?$/.exec(part)
    if (!match) return null
    const [, range, stepText] = match
    const step = stepText ? Number(stepText) : 1
    if (!range || step < 1) return null
    let start: number
    let end: number
    if (range === '*') {
      start = min
      end = max
    } else if (range.includes('-')) {
      const [a, b] = range.split('-').map(Number)
      if (a === undefined || b === undefined) return null
      start = a
      end = b
    } else {
      start = Number(range)
      end = stepText ? max : start
    }
    if (start < min || end > max || start > end) return null
    for (let value = start; value <= end; value += step) values.add(value)
  }
  return values
}

export function nextCronTime(expression: string, afterMs: number): number | null {
  const fields = expression.trim().split(/\s+/)
  if (fields.length !== 5) return null
  const [mi, ho, dm, mo, dw] = fields as [string, string, string, string, string]
  const minutes = parseField(mi, 0, 59)
  const hours = parseField(ho, 0, 23)
  const days = parseField(dm, 1, 31)
  const months = parseField(replaceNames(mo, MONTHS), 1, 12)
  const cfWeekdays = parseField(replaceNames(dw, WEEKDAYS), 1, 7)
  if (!minutes || !hours || !days || !months || !cfWeekdays) return null
  // Cloudflare : 1 = dimanche … 7 = samedi ; JavaScript : 0 = dimanche … 6 = samedi.
  const weekdays = new Set([...cfWeekdays].map((day) => day - 1))
  const domRestricted = dm !== '*'
  const dowRestricted = dw !== '*'

  // Minute entière suivante, puis recherche bornée à un peu plus d'un an.
  const date = new Date(Math.floor(afterMs / 60_000) * 60_000 + 60_000)
  for (let i = 0; i < 60 * 24 * 370; i++) {
    const dayMatch =
      domRestricted && dowRestricted
        ? days.has(date.getUTCDate()) || weekdays.has(date.getUTCDay())
        : days.has(date.getUTCDate()) && weekdays.has(date.getUTCDay())
    if (!months.has(date.getUTCMonth() + 1) || !dayMatch) {
      date.setUTCHours(0, 0, 0, 0)
      date.setUTCDate(date.getUTCDate() + 1)
      continue
    }
    if (!hours.has(date.getUTCHours())) {
      date.setUTCMinutes(0, 0, 0)
      date.setUTCHours(date.getUTCHours() + 1)
      continue
    }
    if (minutes.has(date.getUTCMinutes())) return date.getTime()
    date.setUTCMinutes(date.getUTCMinutes() + 1)
  }
  return null
}
