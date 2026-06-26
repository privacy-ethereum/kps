// @kpstreams/core/webrtc — the WebRTC wire protocol: the §6.2 datachannel
// framing, the certhash→SDP fingerprint, and the SDP/ICE synthesis. Shared by
// @kpstreams/webrtc-client and @kpstreams/server (both speak this to each
// other); QUIC needs none of it (it has FIN/RESET/STOP_SENDING + datagrams
// natively).

export {
  FRAME_DATA, FRAME_FIN, FRAME_RESET, FRAME_STOP_SENDING, MAX_FRAME_PAYLOAD,
  encodeData, encodeFin, encodeCode, decodeFrame, codeToNum, numToCode,
  type Frame,
} from './framing.js'
export { digestToSdpFingerprint } from './certhash.js'
export {
  extractUfragFromLocalOffer, generateUfrag, deriveICEPwd,
  rewriteOfferUfrag, synthesizeAnswer,
} from './sdp.js'
