/** Journal structuré (JSON), lisible et filtrable dans Workers Logs. */
export function log(event: string, data: Record<string, unknown> = {}): void {
  console.log(JSON.stringify({ event, ...data }))
}

export function logError(event: string, error: unknown, data: Record<string, unknown> = {}): void {
  console.error(JSON.stringify({ event, error: errorMessage(error), ...data }))
}

/** Message lisible d'une erreur quelconque, tronqué pour la base. */
export function errorMessage(error: unknown, max = 500): string {
  const text = error instanceof Error ? `${error.name}: ${error.message}` : String(error)
  return text.length > max ? `${text.slice(0, max)}…` : text
}
