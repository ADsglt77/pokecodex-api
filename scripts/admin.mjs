#!/usr/bin/env node
/**
 * Petit client de l'API d'administration, sans afficher le jeton.
 *
 *   node scripts/admin.mjs GET admin/status
 *   node scripts/admin.mjs POST admin/sync/start
 *   node scripts/admin.mjs POST "admin/tick?max_jobs=20"
 *
 * Jeton : variable ADMIN_TOKEN, sinon `.secrets/admin-token.txt` (ignoré par Git).
 * Adresse : variable IMPORTER_URL, sinon `.secrets/importer-url.txt`.
 */

import { readFileSync } from 'node:fs'
import { fileURLToPath } from 'node:url'

const root = fileURLToPath(new URL('..', import.meta.url))
const read = (name) => {
  try {
    return readFileSync(`${root}.secrets/${name}`, 'utf8').trim()
  } catch {
    return ''
  }
}

const [method = 'GET', rawPath = 'admin/status'] = process.argv.slice(2)
// Git Bash convertit un argument « /admin/… » en chemin Windows (« C:/Program Files/Git/admin/… ») :
// on ne garde que la partie à partir de « admin », avec ou sans barre initiale.
const start = rawPath.search(/\/?admin(\/|$|\?)|\/?health$/)
const path = `/${(start >= 0 ? rawPath.slice(start) : rawPath).replace(/^\/+/, '')}`
const token = process.env.ADMIN_TOKEN?.trim() || read('admin-token.txt')
const base = (process.env.IMPORTER_URL?.trim() || read('importer-url.txt')).replace(/\/+$/, '')
if (!token) throw new Error('jeton absent : ADMIN_TOKEN ou .secrets/admin-token.txt')
if (!base) throw new Error('adresse absente : IMPORTER_URL ou .secrets/importer-url.txt')

const response = await fetch(`${base}${path}`, { method: method.toUpperCase(), headers: { authorization: `Bearer ${token}` } })
process.stdout.write(await response.text())
if (!response.ok) {
  process.stderr.write(`\nHTTP ${response.status}\n`)
  process.exitCode = 1
}
