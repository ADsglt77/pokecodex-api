/** Date ISO 8601 UTC au format fixe (comparable par ordre lexicographique en SQL). */
export const iso = (ms: number): string => new Date(ms).toISOString()

/** Attend `ms` millisecondes (les minuteries avancent l'horloge des Workers). */
export const sleep = (ms: number): Promise<void> =>
  ms > 0 ? new Promise((resolve) => setTimeout(resolve, ms)) : Promise.resolve()

/** Jour UTC `YYYY-MM-DD` d'un instant. */
export const utcDay = (ms: number): string => iso(ms).slice(0, 10)
