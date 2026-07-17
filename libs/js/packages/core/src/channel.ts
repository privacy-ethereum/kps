// ChannelLike — the minimal data-channel surface the shared WebRTC protocol
// engine (stream-core / conn-core) is written against. @kpstreams/webrtc-client
// adapts the browser RTCDataChannel; @kpstreams/server adapts node-datachannel's
// DataChannel. Keeping one engine over this interface is what stops the two
// implementations drifting apart.
//
// Registration is single-handler (node-datachannel style): each on* replaces
// the previous handler. Adapters must deliver binary message payloads as
// Uint8Array views.

export interface ChannelLike {
  isOpen(): boolean
  /** Hand one message to the transport; throws if the channel is not open. */
  send(data: Uint8Array): void
  bufferedAmount(): number
  setBufferedAmountLowThreshold(bytes: number): void
  onBufferedAmountLow(cb: () => void): void
  onMessage(cb: (data: Uint8Array) => void): void
  onOpen(cb: () => void): void
  onClose(cb: () => void): void
  onError(cb: (message: string) => void): void
  close(): void
}
