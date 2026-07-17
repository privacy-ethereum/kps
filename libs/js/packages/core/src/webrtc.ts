// @kpstreams/core/webrtc — the WebRTC wire protocol (wire version 1): the §6.2
// datachannel framing, the §6.5 flow-control engine, the §8 typed control
// channel, the shared stream/connection engines over ChannelLike, and the
// certhash→SDP fingerprint + SDP/ICE synthesis. Shared by
// @kpstreams/webrtc-client and @kpstreams/server (both speak this to each
// other); QUIC needs none of it (native lifecycle + flow control + datagrams).

export {
  FRAME_DATA, FRAME_FIN, FRAME_RESET, FRAME_STOP_SENDING, FRAME_MAX_STREAM_DATA,
  MAX_FRAME_PAYLOAD, MAX_WEBRTC_FRAME_SIZE, MAX_OFFSET,
  ProtocolViolation,
  encodeData, encodeFin, encodeCode, encodeMaxStreamData, parseFrame,
  codeToNum, numToCode,
  type ParsedFrame,
} from './framing.js'
export {
  WIRE_VERSION,
  CTRL_CONNECTION_CLOSE, CTRL_HELLO, CTRL_MAX_DATA, CTRL_MAX_STREAMS,
  encodeConnClose, encodeHello, encodeMaxData, encodeMaxStreams, decodeControl,
  type ControlMsg, type HelloLimits,
} from './control.js'
export {
  ConnFlow, StreamFlow, resolveLimits,
  DEFAULT_INITIAL_MAX_STREAM_DATA, DEFAULT_INITIAL_MAX_DATA, DEFAULT_INITIAL_MAX_STREAMS,
  type FlowLimits, type ConnFlowSink,
} from './flow.js'
export type { ChannelLike } from './channel.js'
export { KpsStream, type StreamHooks } from './stream-core.js'
export {
  ConnCore,
  WEBRTC_MAX_DATAGRAM, CONTROL_LABEL, CONTROL_ID, DATAGRAM_LABEL, DATAGRAM_ID,
  type ConnCoreHost,
} from './conn-core.js'
export { digestToSdpFingerprint } from './certhash.js'
export {
  extractUfragFromLocalOffer, generateUfrag, deriveICEPwd,
  rewriteOfferUfrag, synthesizeAnswer, buildClientOffer,
} from './sdp.js'
