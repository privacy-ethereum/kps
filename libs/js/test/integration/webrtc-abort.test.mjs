// webrtc-client abort handling that needs no peer. A pre-aborted dial must
// reject up front (before any RTCPeerConnection / SDP work), matching the QUIC
// client. Runs headless in Node — dial() throws before touching the browser
// RTCPeerConnection global, so no polyfill or server is required.

import { test } from 'node:test'
import assert from 'node:assert/strict'
import { dial } from '@kpstreams/webrtc-client'

const ADDR = '203.0.113.5:41108:uEiCkUUDgDV1i0X-h7AS3yMiv_aVZV2C0vige93oBFa2l6Q'

test('webrtc dial rejects a pre-aborted signal', async () => {
  await assert.rejects(() => dial(ADDR, { signal: AbortSignal.abort() }), /dial aborted/)
})
