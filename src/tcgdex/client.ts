/**
 * Client HTTP de TCGdex (API et CDN d'images) : débit limité, délai d'expiration, relances courtes
 * des pannes passagères, et classement de chaque réponse.
 *
 * Le sondage du 13/11/2026 a montré que l'API renvoie parfois des 503 pendant quelques secondes
 * (« no available server ») : une requête en erreur serveur ou réseau est donc relancée deux fois
 * dans la même exécution (après 2 s puis 8 s), dans la limite du temps restant, avant que la tâche
 * ne soit remise à plus tard. Les 404 et 429 ne sont jamais relancés ici.
 */

import type { Config, Language } from '../config'
import type { Usage } from '../db'
import { fetchClassified, readCapped, type HttpOutcome } from '../utils/http'
import type { RateLimiter } from '../utils/limiter'
import { sleep } from '../utils/time'

/** Hôtes que l'importateur a le droit de contacter. */
export const API_HOST = 'api.tcgdex.net'
export const ASSETS_HOST = 'assets.tcgdex.net'
export const ASSETS_BASE = `https://${ASSETS_HOST}`

/** État de santé partagé par une exécution : série d'erreurs serveur ou réseau consécutives. */
export class Health {
  errorStreak = 0
  rateLimited: { retryAfterMs: number | null } | null = null
}

export type JsonOutcome<T> =
  | { kind: 'ok'; status: number; data: T }
  | Exclude<HttpOutcome, { kind: 'ok' }>
  | { kind: 'invalid'; status: number; message: string }

export interface ClientDeps {
  config: Config
  fetcher: typeof fetch
  limiter: RateLimiter
  usage: Usage
  health: Health
  now: () => number
  /** Heure au-delà de laquelle on ne relance plus (fin du budget de l'exécution). */
  deadlineMs: number
  wait?: (ms: number) => Promise<void>
  quickRetryDelaysMs?: number[]
}

export class TcgdexClient {
  private readonly wait: (ms: number) => Promise<void>
  private readonly quickRetryDelaysMs: number[]

  constructor(private readonly deps: ClientDeps) {
    this.wait = deps.wait ?? sleep
    this.quickRetryDelaysMs = deps.quickRetryDelaysMs ?? [2_000, 8_000]
  }

  /** URL de l'API, chaque segment encodé (`exu-%3F` → `exu-%253F`, comme l'exige TCGdex). */
  apiUrl(language: Language, ...segments: string[]): string {
    return [this.deps.config.apiBase, language, ...segments.map(encodeURIComponent)].join('/')
  }

  /** GET classé, avec débit limité et relances courtes des erreurs passagères. */
  async get(url: string, accept: string): Promise<HttpOutcome> {
    // Seuls api.tcgdex.net et assets.tcgdex.net, en https, sont contactés (jamais un autre hôte).
    let parsed: URL
    try {
      parsed = new URL(url)
    } catch {
      return { kind: 'client_error', status: 0 }
    }
    if (parsed.protocol !== 'https:' || (parsed.host !== API_HOST && parsed.host !== ASSETS_HOST) || parsed.username || parsed.password) {
      return { kind: 'client_error', status: 0 }
    }
    for (let attempt = 0; ; attempt++) {
      await this.deps.limiter.acquire()
      this.deps.usage.httpRequests++
      const outcome = await fetchClassified(
        this.deps.fetcher,
        url,
        {
          method: 'GET',
          headers: { accept, 'user-agent': this.deps.config.userAgent },
          // Jamais de réponse en cache : le CDN marque même ses 404 « immutable » pour un an.
          cache: 'no-store',
          // Aucune redirection suivie : une redirection (vers un autre hôte, ou en http) est
          // classée en erreur et jamais suivie. TCGdex ne redirige pas en fonctionnement normal.
          redirect: 'manual',
        },
        this.deps.config.fetchTimeoutMs,
        this.deps.now,
      )
      this.record(outcome)
      const transient = outcome.kind === 'server_error' || outcome.kind === 'network_error'
      const delay = this.quickRetryDelaysMs[attempt]
      if (!transient || delay === undefined || this.deps.now() + delay + this.deps.config.fetchTimeoutMs > this.deps.deadlineMs) {
        return outcome
      }
      await this.wait(delay)
    }
  }

  /** GET d'un document JSON, lu dans la limite de `maxJsonBytes`. */
  async getJson<T = unknown>(url: string): Promise<JsonOutcome<T>> {
    const outcome = await this.get(url, 'application/json')
    if (outcome.kind !== 'ok') return outcome
    try {
      const bytes = await readCapped(outcome.response, this.deps.config.maxJsonBytes)
      return { kind: 'ok', status: outcome.status, data: JSON.parse(new TextDecoder().decode(bytes)) as T }
    } catch (error) {
      return { kind: 'invalid', status: outcome.status, message: error instanceof Error ? error.message : String(error) }
    }
  }

  private record(outcome: HttpOutcome): void {
    const { health, limiter, config } = this.deps
    if (outcome.kind === 'server_error' || outcome.kind === 'network_error') {
      health.errorStreak++
      // Ralentit le débit tant que le serveur peine.
      limiter.slowDown(1.5, config.maxRequestIntervalMs)
    } else if (outcome.kind === 'rate_limited') {
      health.rateLimited = { retryAfterMs: outcome.retryAfterMs }
      limiter.slowDown(2, config.maxRequestIntervalMs)
    } else {
      health.errorStreak = 0
      // Le serveur répond normalement : retour progressif au débit nominal.
      limiter.speedUp(0.8, config.minRequestIntervalMs)
    }
  }
}
