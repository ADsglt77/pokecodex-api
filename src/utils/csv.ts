/**
 * Une cellule CSV (RFC 4180) : guillemets doublés, champ entre guillemets si nécessaire. Les
 * chaînes qui commencent par `=`, `+`, `-`, `@`, tabulation ou retour chariot sont préfixées d'une
 * apostrophe pour qu'un tableur ne les interprète pas comme des formules (les nombres restent intacts).
 */
export function csvCell(value: unknown): string {
  if (value === null || value === undefined) return ''
  let text = typeof value === 'string' ? value : String(value)
  if (typeof value === 'string' && /^[=+\-@\t\r]/.test(text)) text = `'${text}`
  return /[",\r\n;]/.test(text) ? `"${text.replaceAll('"', '""')}"` : text
}

/** Une ligne CSV terminée par CRLF. */
export const csvRow = (values: readonly unknown[]): string => `${values.map(csvCell).join(',')}\r\n`
