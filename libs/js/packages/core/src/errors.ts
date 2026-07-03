// The transport-neutral error model (SPEC §9.1). Shared by every transport and
// surfaced in the public API as a string code plus optional message.

// KpsErrorCode is the canonical reset/cancel code set, used in the public API as
// a string; the WebRTC framing maps it to a wire uint32 (see ./framing).
export type KpsErrorCode =
  | 'cancelled'
  | 'closed'
  | 'reset'
  | 'timeout'
  | 'network-error'
  | 'protocol-error'
  | 'unsupported'
  | 'too-large'
  | 'queue-full'
  | 'permission-denied'
  | 'internal-error'

export interface KpsReason {
  code?: KpsErrorCode
  message?: string
}

// Build an Error carrying a KpsReason's code, for surfacing through stream/
// connection rejections.
export function streamError(reason: KpsReason): Error {
  const e = new Error(reason.message ?? `kps: stream ${reason.code ?? 'reset'}`)
  ;(e as unknown as { code?: KpsErrorCode }).code = reason.code
  return e
}

// Coerce an arbitrary throwable / abort reason into a KpsReason.
export function reasonFrom(x: unknown): KpsReason | undefined {
  if (x == null) return undefined
  if (typeof x === 'object' && ('code' in x || 'message' in x)) return x as KpsReason
  return { message: String((x as { message?: unknown })?.message ?? x) }
}
