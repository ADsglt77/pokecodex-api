/**
 * État d'exécution (table `runtime_state`) : verrou de l'exécuteur, pause, attente imposée,
 * compteurs quotidiens, dernière exécution.
 */

import type { Db } from '../db'
import { iso, utcDay } from '../utils/time'

export const STATE = {
  runner: 'runner_lock',
  paused: 'paused',
  pauseReason: 'pause_reason',
  backoffUntil: 'backoff_until',
  backoffReason: 'backoff_reason',
  consecutive429: 'consecutive_429',
  lastTick: 'last_tick',
  lastActiveTick: 'last_active_tick',
} as const

const opsKey = (nowMs: number) => `ops:${utcDay(nowMs)}`

export interface StateRow {
  key: string
  value: string | null
  expires_at: string | null
  updated_at: string
}

/**
 * Prend le verrou de l'exécuteur s'il est libre ou expiré. Une seule exécution travaille à la fois :
 * les exécutions qui se chevauchent (Cron en retard, doublon, déclenchement manuel) s'arrêtent.
 */
export async function acquireRunnerLease(db: Db, token: string, nowMs: number, leaseMs: number): Promise<boolean> {
  const row = await db.first<{ value: string }>(
    `INSERT INTO runtime_state (key, value, expires_at, updated_at) VALUES (?1, ?2, ?3, ?4)
     ON CONFLICT (key) DO UPDATE SET value = excluded.value, expires_at = excluded.expires_at,
       updated_at = excluded.updated_at
     WHERE runtime_state.expires_at IS NULL OR runtime_state.expires_at <= ?4
     RETURNING value`,
    STATE.runner,
    token,
    iso(nowMs + leaseMs),
    iso(nowMs),
  )
  return row?.value === token
}

/** Libère le verrou de l'exécuteur, seulement s'il est encore à nous. */
export function releaseRunnerLeaseStmt(db: Db, token: string, nowMs: number): D1PreparedStatement {
  return db.stmt(
    `UPDATE runtime_state SET expires_at = ?3, updated_at = ?3 WHERE key = ?1 AND value = ?2`,
    STATE.runner,
    token,
    iso(nowMs),
  )
}

/**
 * Prolonge le verrou de l'exécuteur, seulement s'il est encore à nous (`meta.changes` vaut 0
 * sinon : l'exécution doit s'arrêter).
 */
export function renewRunnerLeaseStmt(db: Db, token: string, nowMs: number, leaseMs: number): D1PreparedStatement {
  return db.stmt(
    `UPDATE runtime_state SET expires_at = ?3, updated_at = ?4 WHERE key = ?1 AND value = ?2`,
    STATE.runner,
    token,
    iso(nowMs + leaseMs),
    iso(nowMs),
  )
}

/** Écrit une valeur (et son expiration éventuelle). */
export function setStateStmt(db: Db, key: string, value: string | null, nowMs: number, expiresAt: string | null = null): D1PreparedStatement {
  return db.stmt(
    `INSERT INTO runtime_state (key, value, expires_at, updated_at) VALUES (?1, ?2, ?3, ?4)
     ON CONFLICT (key) DO UPDATE SET value = excluded.value, expires_at = excluded.expires_at,
       updated_at = excluded.updated_at`,
    key,
    value,
    expiresAt,
    iso(nowMs),
  )
}

/** Ajoute `count` au compteur d'opérations du jour UTC. */
export function addDailyOperationsStmt(db: Db, count: number, nowMs: number): D1PreparedStatement {
  return db.stmt(
    `INSERT INTO runtime_state (key, value, updated_at) VALUES (?1, ?2, ?3)
     ON CONFLICT (key) DO UPDATE SET value = CAST(CAST(runtime_state.value AS INTEGER) + ?2 AS TEXT),
       updated_at = excluded.updated_at`,
    opsKey(nowMs),
    String(count),
    iso(nowMs),
  )
}

/** Tout l'état utile au démarrage d'une exécution, en une requête. */
export async function readState(db: Db, nowMs: number): Promise<Map<string, StateRow>> {
  const rows = await db.all<StateRow>(
    `SELECT key, value, expires_at, updated_at FROM runtime_state WHERE key IN (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)`,
    STATE.runner,
    STATE.paused,
    STATE.pauseReason,
    STATE.backoffUntil,
    STATE.backoffReason,
    STATE.consecutive429,
    STATE.lastTick,
    STATE.lastActiveTick,
    opsKey(nowMs),
  )
  return new Map(rows.map((row) => [row.key, row]))
}

export const dailyOperations = (state: Map<string, StateRow>, nowMs: number): number =>
  Number(state.get(opsKey(nowMs))?.value ?? 0) || 0

export const isPaused = (state: Map<string, StateRow>): boolean => state.get(STATE.paused)?.value === '1'

/** Fin de l'attente imposée (429 ou série d'erreurs), en ms, ou 0. */
export const backoffUntilMs = (state: Map<string, StateRow>): number => {
  const value = state.get(STATE.backoffUntil)?.value
  const ms = value ? Date.parse(value) : 0
  return Number.isNaN(ms) ? 0 : ms
}
