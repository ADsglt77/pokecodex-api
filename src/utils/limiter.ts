import { sleep } from './time'

/**
 * Espacement global des requêtes sortantes vers TCGdex, partagé par toutes les voies de
 * téléchargement d'une exécution : deux requêtes ne partent jamais à moins de `intervalMs`
 * d'intervalle. Comme une seule exécution travaille à la fois (verrou de l'exécuteur), c'est la
 * limite de débit globale du système.
 */
export class RateLimiter {
  private nextSlot = 0

  constructor(
    private intervalMs: number,
    private readonly now: () => number = Date.now,
    private readonly wait: (ms: number) => Promise<void> = sleep,
  ) {}

  /** Attend son tour, puis réserve le créneau suivant. */
  async acquire(): Promise<void> {
    const now = this.now()
    const slot = Math.max(now, this.nextSlot)
    this.nextSlot = slot + this.intervalMs
    await this.wait(slot - now)
  }

  /** Ralentit (après des erreurs serveur répétées), sans jamais dépasser `maxMs`. */
  slowDown(factor: number, maxMs: number): void {
    this.intervalMs = Math.min(maxMs, Math.ceil(this.intervalMs * factor))
  }

  /**
   * Réaccélère progressivement après une réponse normale, sans jamais descendre sous `minMs` :
   * quelques erreurs passagères ne bloquent plus le débit pour tout le reste de l'exécution.
   */
  speedUp(factor: number, minMs: number): void {
    this.intervalMs = Math.max(minMs, Math.floor(this.intervalMs * factor))
  }

  get interval(): number {
    return this.intervalMs
  }
}
