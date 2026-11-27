/**
 * Tâche `image` : copie de l'image d'une carte de TCGdex vers R2 (TCGdex → Worker → R2), sans
 * jamais passer par un ordinateur local.
 *
 * 1. Carte déjà dotée d'une image valide : rien à faire.
 * 2. Objet déjà présent dans R2 (copie faite mais D1 pas mis à jour après une panne) : réconcilié
 *    en `already_present`, sans nouveau téléchargement.
 * 3. URL officielle de l'API (qualité principale, puis de secours), sinon une URL reconstruite.
 * 4. Fichier lu en flux dans la limite de `maxImageBytes`, validé (type, taille, signature WebP,
 *    taille RIFF, MD5 = ETag), puis écrit dans R2 avec contrôle SHA-256 côté R2. Le fichier est
 *    gardé en mémoire le temps de la validation (≈ 90 Ko en moyenne) : c'est ce qui garantit
 *    qu'aucun téléchargement corrompu n'écrase jamais un objet R2.
 * 5. D1 n'est mis à jour qu'une fois l'écriture R2 confirmée (taille relue dans la réponse de R2).
 *
 * Un 404 est une absence confirmée pour l'URL testée (pas de nouvelle tentative) ; un 429 rend la
 * tâche ; une erreur serveur, réseau ou un transfert corrompu sont retentés plus tard.
 */

import { IMAGES_PREFIX } from '../config'
import { imageFailureStmt, imageSuccessStmt, isValidImageStatus, type CardForJob, type ImageStatus } from '../repositories/catalog'
import { reviveLowImageStmt, type Job } from '../repositories/jobs'
import { checkWebp, imageCandidates } from '../tcgdex/images'
import { BodyTooLargeError, readCapped } from '../utils/http'
import { cardImageKey } from '../utils/keys'
import { errorMessage } from '../utils/log'
import { jobLanguage, rateLimitDelayMs, type JobContext, type JobResult } from './context'

interface Tested {
  url: string
  status: number | null
  error?: string
}

const NO_SET = 'sans-extension'

export async function runImageJob(ctx: JobContext, job: Job, card: CardForJob | undefined): Promise<JobResult> {
  const language = jobLanguage(job)
  const { db, bucket, client, config } = ctx
  if (!card) return { kind: 'failed', error: `carte ${job.card_id ?? job.target_id} inconnue en base`, httpStatus: null }
  if (isValidImageStatus(card.image_status) && card.image_r2_key) return { kind: 'done', statements: [] }
  // Image retirée à la main (règles de qualité) : la carte reste sans image.
  if (card.image_provider !== null) return { kind: 'done', statements: [] }

  const key = cardImageKey(language, card.set_tcgdex_id ?? NO_SET, card.tcgdex_id)
  /** La petite image de la carte, remise en file avec chaque réussite (`reviveLowImageStmt`). */
  const lowImageAfter = () => [reviveLowImageStmt(db, language, card.tcgdex_id, ctx.now())]
  if (!key.startsWith(IMAGES_PREFIX)) throw new Error(`clé R2 hors du préfixe ${IMAGES_PREFIX} : ${key}`)
  const usage = db.usage

  const exhaustedWith = (reason: string, httpStatus: number | null, tested = '[]') => [
    imageFailureStmt(db, card.id, { status: 'download_error', reason, testedUrls: tested, httpStatus }, ctx.now()),
  ]

  // Réconciliation : l'objet est déjà dans R2.
  usage.r2Reads++
  let existing: R2Object | null
  try {
    existing = await bucket.head(key)
  } catch (error) {
    const message = `lecture R2 échouée : ${errorMessage(error)}`
    return { kind: 'retry', error: `image ${card.id} : ${message}`, httpStatus: null, exhausted: exhaustedWith(message, null) }
  }
  if (existing && existing.size >= config.minImageBytes && existing.customMetadata?.sha256) {
    const meta = existing.customMetadata
    const source = meta.source === 'api' || meta.source === 'cdn_reconstructed' ? meta.source : null
    return {
      kind: 'done',
      statements: [
        imageSuccessStmt(
          db,
          card.id,
          {
            status: 'already_present',
            source,
            sourceUrl: meta.sourceUrl ?? null,
            key,
            quality: meta.quality ?? null,
            size: existing.size,
            sha256: meta.sha256 ?? null,
            testedUrls: '[]',
            httpStatus: null,
          },
          ctx.now(),
        ),
        ...lowImageAfter(),
      ],
    }
  }

  const candidates = imageCandidates(config, { language, api_image_url: card.api_image_url, series_id: card.series_id, set_tcgdex_id: card.set_tcgdex_id, local_id: card.local_id })
  if (!candidates.length) {
    // Pas d'URL officielle ni de série connue : si le détail de l'extension est encore attendu, on
    // patiente ; sinon la reconstruction est impossible faute d'informations fiables.
    if (card.set_tcgdex_id && !card.series_id) {
      const pending = await db.first<{ n: number }>(
        `SELECT COUNT(*) AS n FROM import_jobs WHERE dedupe_key = ?1 AND status IN ('pending', 'running')`,
        `set:${language}:${card.set_tcgdex_id}`,
      )
      if ((pending?.n ?? 0) > 0) return { kind: 'release', reason: 'série inconnue : détail de l’extension attendu', httpStatus: null, delayMs: 10 * 60_000 }
    }
    return {
      kind: 'done',
      statements: [
        imageFailureStmt(db, card.id, { status: 'missing_api_url', reason: 'aucune URL dans l’API et série ou numéro inconnus : reconstruction impossible', testedUrls: '[]', httpStatus: null }, ctx.now()),
      ],
    }
  }

  const tested: Tested[] = []
  let invalid: string | null = null
  const exhausted = (reason: string, httpStatus: number | null) => exhaustedWith(reason, httpStatus, JSON.stringify(tested))

  for (const candidate of candidates) {
    const outcome = await client.get(candidate.url, 'image/webp')
    if (outcome.kind === 'not_found') {
      tested.push({ url: candidate.url, status: outcome.status })
      continue
    }
    if (outcome.kind === 'rate_limited') {
      return { kind: 'release', reason: 'CDN : HTTP 429', httpStatus: 429, delayMs: rateLimitDelayMs(outcome.retryAfterMs, config) }
    }
    if (outcome.kind !== 'ok') {
      const status = outcome.kind === 'network_error' ? null : outcome.status
      const error = outcome.kind === 'network_error' ? `erreur réseau : ${outcome.message}` : `HTTP ${outcome.status}`
      tested.push({ url: candidate.url, status, error })
      return { kind: 'retry', error: `image ${card.id} : ${error}`, httpStatus: status, exhausted: exhausted(error, status) }
    }

    let bytes: Uint8Array
    try {
      bytes = await readCapped(outcome.response, config.maxImageBytes)
    } catch (error) {
      if (error instanceof BodyTooLargeError) {
        invalid = error.message
        tested.push({ url: candidate.url, status: outcome.status, error: invalid })
        continue
      }
      const message = `lecture interrompue : ${errorMessage(error)}`
      tested.push({ url: candidate.url, status: outcome.status, error: message })
      return { kind: 'retry', error: `image ${card.id} : ${message}`, httpStatus: outcome.status, exhausted: exhausted(message, outcome.status) }
    }

    const check = await checkWebp(bytes, outcome.response.headers, config)
    if (!check.ok) {
      tested.push({ url: candidate.url, status: outcome.status, error: check.reason })
      if (check.transient) {
        return { kind: 'retry', error: `image ${card.id} : ${check.reason}`, httpStatus: outcome.status, exhausted: exhausted(check.reason, outcome.status) }
      }
      invalid = check.reason
      continue
    }
    tested.push({ url: candidate.url, status: outcome.status })

    // Écriture R2 : le contrôle SHA-256 est vérifié par R2 lui-même à la réception.
    let stored: R2Object | null
    try {
      usage.r2Writes++
      stored = await bucket.put(key, bytes, {
        sha256: check.sha256,
        httpMetadata: { contentType: 'image/webp', cacheControl: 'public, max-age=31536000, immutable' },
        customMetadata: {
          source: candidate.source,
          sourceUrl: candidate.url,
          quality: candidate.quality,
          sha256: check.sha256,
          md5: check.md5,
          language,
          tcgdexId: card.tcgdex_id,
        },
      })
    } catch (error) {
      const message = `écriture R2 échouée : ${errorMessage(error)}`
      return { kind: 'retry', error: `image ${card.id} : ${message}`, httpStatus: null, exhausted: exhausted(message, null) }
    }
    if (!stored || stored.size !== bytes.byteLength) {
      const message = `écriture R2 non confirmée (${stored?.size ?? 'aucun objet'} ≠ ${bytes.byteLength} octets)`
      return { kind: 'retry', error: `image ${card.id} : ${message}`, httpStatus: null, exhausted: exhausted(message, null) }
    }
    usage.bytesStored += bytes.byteLength
    return {
      kind: 'done',
      httpStatus: outcome.status,
      statements: [
        imageSuccessStmt(
          db,
          card.id,
          {
            status: candidate.source === 'api' ? 'downloaded' : 'recovered_from_cdn',
            source: candidate.source,
            sourceUrl: candidate.url,
            key,
            quality: candidate.quality,
            size: bytes.byteLength,
            sha256: check.sha256,
            testedUrls: JSON.stringify(tested),
            httpStatus: outcome.status,
          },
          ctx.now(),
        ),
        ...lowImageAfter(),
      ],
    }
  }

  // Aucune URL n'a donné d'image valide : absence confirmée (404) ou fichier invalide.
  const official = candidates[0]?.source === 'api'
  const status: ImageStatus = invalid ? 'invalid_image' : 'not_found_on_cdn'
  const reason = invalid
    ? `image invalide : ${invalid}`
    : official
      ? 'URL officielle de l’API introuvable sur le CDN (404)'
      : 'URL reconstruite introuvable sur le CDN (404)'
  const last = tested.at(-1)
  return {
    kind: 'done',
    httpStatus: last?.status ?? null,
    statements: [imageFailureStmt(db, card.id, { status, reason, testedUrls: JSON.stringify(tested), httpStatus: last?.status ?? null }, ctx.now())],
  }
}
