/**
 * Accès à D1 et compteurs d'usage d'une exécution.
 *
 * Cloudflare limite une invocation à 1 000 requêtes D1 (un `batch()` compte pour une) et à 10 000
 * sous-requêtes par défaut (fetch, R2 et D1 confondus). Tout passe donc par `Db` et `Usage`, qui
 * comptent, pour que l'exécuteur s'arrête avant ces limites.
 */

export class Usage {
  d1Queries = 0
  httpRequests = 0
  r2Reads = 0
  r2Writes = 0
  bytesStored = 0

  /** Sous-requêtes au sens de Cloudflare : fetch, R2 et D1. */
  get subrequests(): number {
    return this.d1Queries + this.httpRequests + this.r2Reads + this.r2Writes
  }

  /** Opérations comptées dans le plafond quotidien : requêtes vers TCGdex et écritures R2. */
  get operations(): number {
    return this.httpRequests + this.r2Writes
  }
}

export type SqlValue = string | number | null

export class Db {
  constructor(
    readonly d1: D1Database,
    readonly usage: Usage = new Usage(),
  ) {}

  /** Requête préparée (non exécutée), pour un `batch`. */
  stmt(sql: string, ...params: SqlValue[]): D1PreparedStatement {
    return this.d1.prepare(sql).bind(...params)
  }

  async all<T>(sql: string, ...params: SqlValue[]): Promise<T[]> {
    this.usage.d1Queries++
    return (await this.d1.prepare(sql).bind(...params).all<T>()).results
  }

  async first<T>(sql: string, ...params: SqlValue[]): Promise<T | null> {
    this.usage.d1Queries++
    return await this.d1.prepare(sql).bind(...params).first<T>()
  }

  async run(sql: string, ...params: SqlValue[]): Promise<D1Result> {
    this.usage.d1Queries++
    return await this.d1.prepare(sql).bind(...params).run()
  }

  /** Exécute des requêtes dans une seule transaction D1 (tout ou rien) ; compte pour une requête. */
  async batch(statements: D1PreparedStatement[]): Promise<D1Result[]> {
    if (!statements.length) return []
    this.usage.d1Queries++
    return await this.d1.batch(statements)
  }
}
