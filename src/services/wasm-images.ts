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

/** Dernière image décodée : `info` puis deux `toWebp` sur les mêmes octets ne décodent qu'une fois. */
let last: { bytes: Uint8Array; image: ImageData } | null = null
async function decode(bytes: Uint8Array): Promise<ImageData> {
  if (last?.bytes === bytes) return last.image
  await initCodecs()
  let image: ImageData
  if (isPng(bytes)) image = await decodePng(buffer(bytes))
  else if (isJpeg(bytes)) image = await decodeJpeg(buffer(bytes))
  else if (isWebp(bytes)) image = await decodeWebp(buffer(bytes))
  else throw new Error('format non pris en charge (ni PNG, ni JPEG, ni WebP)')
  last = { bytes, image }
  return image
}

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
      const image = await decode(bytes)
      return { format: isPng(bytes) ? 'image/png' : isJpeg(bytes) ? 'image/jpeg' : 'image/webp', width: image.width, height: image.height }
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
