/**
 * Authentification de l'API d'administration : `Authorization: Bearer <ADMIN_TOKEN>`.
 *
 * Le jeton est un secret du Worker (`wrangler secret put ADMIN_TOKEN`), jamais dans Git. Sans jeton
 * configuré (ou trop court), toute l'administration est refusée : on échoue fermé. La comparaison se
 * fait sur les empreintes SHA-256, en temps constant.
 */

export const MIN_TOKEN_LENGTH = 32

export type AuthResult = { ok: true } | { ok: false; status: 401 | 503; message: string }

async function digest(value: string): Promise<ArrayBuffer> {
  return crypto.subtle.digest('SHA-256', new TextEncoder().encode(value))
}

export async function checkAdminAuth(request: Request, adminToken: string | undefined): Promise<AuthResult> {
  if (!adminToken || adminToken.length < MIN_TOKEN_LENGTH) {
    return { ok: false, status: 503, message: 'administration désactivée : secret ADMIN_TOKEN absent ou trop court' }
  }
  const header = request.headers.get('authorization') ?? ''
  const match = /^Bearer\s+(.+)$/i.exec(header.trim())
  if (!match?.[1]) return { ok: false, status: 401, message: 'authentification requise' }
  const [given, expected] = await Promise.all([digest(match[1]), digest(adminToken)])
  if (!crypto.subtle.timingSafeEqual(given, expected)) return { ok: false, status: 401, message: 'jeton invalide' }
  return { ok: true }
}
