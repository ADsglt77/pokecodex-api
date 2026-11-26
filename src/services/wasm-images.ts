/**
 * Conversion d'images dans le Worker lui-même (codecs WebAssembly jSquash, ceux de Squoosh), sans
 * la liaison Cloudflare Images : décodage PNG / JPEG / WebP, redimensionnement (lanczos3) et
 * encodage WebP. Même interface que l'adaptateur Images (`SetMediaImages`) ; aucun quota, seulement
 * du temps CPU du Worker.
 */

import decodeJpeg, { init as initJpegDecode } from '@jsquash/jpeg/decode'
import decodePng, { init as initPngDecode } from '@jsquash/png/decode'
import resize, { initResize } from '@jsquash/resize'
import decodeWebp, { init as initWebpDecode } from '@jsquash/webp/decode'
import encodeWebp, { init as initWebpEncode } from '@jsquash/webp/encode'
import JPEG_DEC from '@jsquash/jpeg/codec/dec/mozjpeg_dec.wasm'
import PNG_DEC from '@jsquash/png/codec/pkg/squoosh_png_bg.wasm?module'
import RESIZE from '@jsquash/resize/lib/resize/pkg/squoosh_resize_bg.wasm?module'
import WEBP_DEC from '@jsquash/webp/codec/dec/webp_dec.wasm'
import WEBP_ENC from '@jsquash/webp/codec/enc/webp_enc_simd.wasm'
import type { SetMediaImages } from './set-media-import'

/** Image décodée (pixels RGBA) telle que la rendent les codecs. */
type ImageData = Awaited<ReturnType<typeof decodePng>>

// Les codecs construisent des `ImageData`, absent des Workers : équivalent minimal.
class ImageDataShim {
  readonly colorSpace = 'srgb'
  readonly data: Uint8ClampedArray
  readonly width: number
  readonly height: number
  constructor(a: Uint8ClampedArray | number, b: number, c?: number) {
    if (typeof a === 'number') {
      this.width = a
      this.height = b
      this.data = new Uint8ClampedArray(a * b * 4)
    } else {
      this.data = a
      this.width = b
      this.height = c ?? a.length / 4 / b
    }
  }
}
const g = globalThis as { ImageData?: unknown }
if (typeof g.ImageData === 'undefined') g.ImageData = ImageDataShim

let ready: Promise<unknown> | null = null
const initCodecs = () =>
  (ready ??= Promise.all([initPngDecode(PNG_DEC), initJpegDecode(JPEG_DEC), initWebpDecode(WEBP_DEC), initWebpEncode(WEBP_ENC), initResize(RESIZE)]))

const isPng = (b: Uint8Array) => b[0] === 0x89 && b[1] === 0x50 && b[2] === 0x4e && b[3] === 0x47
const isJpeg = (b: Uint8Array) => b[0] === 0xff && b[1] === 0xd8 && b[2] === 0xff
const isWebp = (b: Uint8Array) => String.fromCharCode(...b.subarray(0, 4)) === 'RIFF' && String.fromCharCode(...b.subarray(8, 12)) === 'WEBP'
const buffer = (b: Uint8Array): ArrayBuffer => b.buffer.slice(b.byteOffset, b.byteOffset + b.byteLength) as ArrayBuffer

/**
 * Mémoire : un isolat n'a que 128 Mo, et la mémoire d'un codec WebAssembly ne rétrécit jamais. Un
 * grand scan (ex. 1385×1953) la faisait déborder : le codec plantait (`RuntimeError: unreachable`)
 * et restait inutilisable pour toutes les images suivantes de l'isolat. D'où : aucune image de plus
 * de `MAX_PIXELS` décodée (dimensions lues dans l'en-tête), et toute image décodée de plus de
 * `PRE_REDUCE_SIDE` px de côté réduite aussitôt en JavaScript (moyenne par zone) avant les codecs.
 */
const MAX_PIXELS = 6_000_000
const PRE_REDUCE_SIDE = 1024

/** Dimensions lues dans l'en-tête (PNG : IHDR ; JPEG : segment SOF) ; `null` si introuvables. */
function headerSize(b: Uint8Array): { width: number; height: number } | null {
  if (isPng(b) && b.byteLength >= 24) return { width: ((b[16]! << 24) | (b[17]! << 16) | (b[18]! << 8) | b[19]!) >>> 0, height: ((b[20]! << 24) | (b[21]! << 16) | (b[22]! << 8) | b[23]!) >>> 0 }
  if (isJpeg(b)) {
    let i = 2
    while (i + 9 < b.byteLength && b[i] === 0xff) {
      const marker = b[i + 1]!
      if (marker >= 0xc0 && marker <= 0xcf && ![0xc4, 0xc8, 0xcc].includes(marker)) return { height: (b[i + 5]! << 8) | b[i + 6]!, width: (b[i + 7]! << 8) | b[i + 8]! }
      i += 2 + ((b[i + 2]! << 8) | b[i + 3]!)
    }
  }
  return null
}

/** Réduction par moyenne de zone (pondérée par l'alpha), côté long ramené à `maxSide`. */
function areaShrink(src: ImageData, maxSide: number): ImageData {
  const s = maxSide / Math.max(src.width, src.height)
  const w = Math.max(1, Math.round(src.width * s))
  const h = Math.max(1, Math.round(src.height * s))
  const out = new Uint8ClampedArray(w * h * 4)
  const fx = src.width / w
  const fy = src.height / h
  const d = src.data
  for (let y = 0; y < h; y++) {
    const y0 = y * fy
    const y1 = y0 + fy
    for (let x = 0; x < w; x++) {
      const x0 = x * fx
      const x1 = x0 + fx
      let r = 0, g = 0, bl = 0, a = 0, wt = 0
      for (let sy = Math.floor(y0); sy < Math.min(src.height, Math.ceil(y1)); sy++) {
        const wy = Math.min(y1, sy + 1) - Math.max(y0, sy)
        for (let sx = Math.floor(x0); sx < Math.min(src.width, Math.ceil(x1)); sx++) {
          const k = (Math.min(x1, sx + 1) - Math.max(x0, sx)) * wy
          const i = (sy * src.width + sx) * 4
          const al = d[i + 3]! * k
          r += d[i]! * al
          g += d[i + 1]! * al
          bl += d[i + 2]! * al
          a += al
          wt += k
        }
      }
      const o = (y * w + x) * 4
      if (a > 0) {
        out[o] = r / a
        out[o + 1] = g / a
        out[o + 2] = bl / a
      }
      out[o + 3] = a / wt
    }
  }
  return new ImageDataShim(out, w, h) as unknown as ImageData
}

/** Dernière image décodée : `info` puis deux `toWebp` sur les mêmes octets ne décodent qu'une fois. */
let last: { bytes: Uint8Array; image: ImageData; width: number; height: number } | null = null
async function decodeSized(bytes: Uint8Array): Promise<{ image: ImageData; width: number; height: number }> {
  if (last?.bytes === bytes) return last
  const header = headerSize(bytes)
  if (header && header.width * header.height > MAX_PIXELS) throw new Error(`image trop grande pour la conversion (${header.width}×${header.height})`)
  await initCodecs()
  let image: ImageData
  if (isPng(bytes)) image = await decodePng(buffer(bytes))
  else if (isJpeg(bytes)) image = await decodeJpeg(buffer(bytes))
  else if (isWebp(bytes)) image = await decodeWebp(buffer(bytes))
  else throw new Error('format non pris en charge (ni PNG, ni JPEG, ni WebP)')
  const { width, height } = image
  if (Math.max(width, height) > PRE_REDUCE_SIDE) image = areaShrink(image, PRE_REDUCE_SIDE)
  last = { bytes, image, width, height }
  return last
}
const decode = async (bytes: Uint8Array): Promise<ImageData> => (await decodeSized(bytes)).image

/** Image réduite (jamais agrandie) à `maxSide`, encodée en WebP (sans perte si `quality` ≥ 100). */
async function encodeSized(image: ImageData, maxSide: number, quality: number): Promise<Uint8Array> {
  const scale = Math.min(1, maxSide / Math.max(image.width, image.height))
  const sized =
    scale < 1
      ? await resize(image, { width: Math.max(1, Math.round(image.width * scale)), height: Math.max(1, Math.round(image.height * scale)), method: 'lanczos3', fitMethod: 'stretch', premultiply: true, linearRGB: true })
      : image
  const out = await encodeWebp(sized, quality >= 100 ? { lossless: 1, quality: 100 } : { quality })
  return new Uint8Array(out)
}

export function wasmImagesAdapter(): SetMediaImages {
  return {
    async info(bytes) {
      const { width, height } = await decodeSized(bytes)
      return { format: isPng(bytes) ? 'image/png' : isJpeg(bytes) ? 'image/jpeg' : 'image/webp', width, height }
    },
    async toWebp(bytes, maxSide, quality = 100) {
      return encodeSized(await decode(bytes), maxSide, quality)
    },
  }
}

/** Position d'un calque sur une carte, relative à ses dimensions (coin haut gauche, largeur). */
export interface OverlayPlace {
  x: number
  y: number
  w: number
}

/**
 * Colle `overlay` (PNG à fond transparent, ex. tampon de deck) sur l'image `base` à la position
 * relative `place` (hauteur proportionnelle), puis encode le résultat en WebP pour chaque sortie.
 */
export async function composeOverlay(base: Uint8Array, overlay: Uint8Array, place: OverlayPlace, outputs: readonly { maxSide: number; quality: number }[]): Promise<Uint8Array[]> {
  await initCodecs()
  const card = await decode(base)
  const over = isPng(overlay) ? await decodePng(buffer(overlay)) : await decode(overlay)
  const ow = Math.max(1, Math.round(place.w * card.width))
  const oh = Math.max(1, Math.round((over.height * ow) / over.width))
  const layer = ow === over.width && oh === over.height ? over : await resize(over, { width: ow, height: oh, method: 'lanczos3', fitMethod: 'stretch', premultiply: true, linearRGB: false })
  const ox = Math.round(place.x * card.width)
  const oy = Math.round(place.y * card.height)
  const out = new Uint8ClampedArray(card.data)
  for (let y = 0; y < oh; y++) {
    const ty = oy + y
    if (ty < 0 || ty >= card.height) continue
    for (let x = 0; x < ow; x++) {
      const tx = ox + x
      if (tx < 0 || tx >= card.width) continue
      const i = (y * ow + x) * 4
      const j = (ty * card.width + tx) * 4
      const a = (layer.data[i + 3] ?? 0) / 255
      if (!a) continue
      for (let k = 0; k < 3; k++) out[j + k] = Math.round((layer.data[i + k] ?? 0) * a + (out[j + k] ?? 0) * (1 - a))
      out[j + 3] = Math.max(out[j + 3] ?? 0, layer.data[i + 3] ?? 0)
    }
  }
  const composed = new ImageDataShim(out, card.width, card.height) as unknown as ImageData
  return Promise.all(outputs.map((o) => encodeSized(composed, o.maxSide, o.quality)))
}
