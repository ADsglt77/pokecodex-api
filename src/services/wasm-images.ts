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

export function wasmImagesAdapter(): SetMediaImages {
  return {
    async info(bytes) {
      const image = await decode(bytes)
      return { format: isPng(bytes) ? 'image/png' : isJpeg(bytes) ? 'image/jpeg' : 'image/webp', width: image.width, height: image.height }
    },
    async toWebp(bytes, maxSide, quality = 100) {
      const image = await decode(bytes)
      const scale = Math.min(1, maxSide / Math.max(image.width, image.height))
      const sized =
        scale < 1
          ? await resize(image, { width: Math.max(1, Math.round(image.width * scale)), height: Math.max(1, Math.round(image.height * scale)), method: 'lanczos3', fitMethod: 'stretch', premultiply: true, linearRGB: true })
          : image
      const out = await encodeWebp(sized, quality >= 100 ? { lossless: 1, quality: 100 } : { quality })
      return new Uint8Array(out)
    },
  }
}
