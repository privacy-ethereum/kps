// Unit tests for the wire-version-1 protocol engine (framing strictness, the
// flow-control credit engine, and the shared stream/connection engines) run
// entirely over in-memory mock channels — no WebRTC. Two linked ConnCores
// exercise the conforming paths; a "raw peer" harness (direct access to the
// remote channel ends) exercises the adversarial ones.
import { test } from 'node:test'
import assert from 'node:assert/strict'
import {
  // framing
  parseFrame, encodeData, encodeFin, encodeCode, encodeMaxStreamData,
  FRAME_DATA, FRAME_FIN, FRAME_RESET, FRAME_STOP_SENDING,
  MAX_FRAME_PAYLOAD, MAX_WEBRTC_FRAME_SIZE, MAX_OFFSET, ProtocolViolation,
  // control
  WIRE_VERSION, encodeHello, encodeConnClose, encodeMaxData, encodeMaxStreams, decodeControl,
  // flow
  ConnFlow, resolveLimits,
  // engine
  ConnCore,
} from '../dist/webrtc.js'

const tick = () => new Promise(r => setTimeout(r, 0))
async function settle(times = 5) {
  for (let i = 0; i < times; i++) await tick()
}

// ---------- framing / control codec strictness ----------

test('parseFrame rejects wire-rule violations', () => {
  const cases = [
    ['empty message', new Uint8Array(0)],
    ['empty DATA', new Uint8Array([FRAME_DATA])],
    ['oversized frame', new Uint8Array(MAX_WEBRTC_FRAME_SIZE + 1)],
    ['FIN with payload', new Uint8Array([FRAME_FIN, 1])],
    ['short RESET', new Uint8Array([FRAME_RESET, 0, 0])],
    ['long STOP_SENDING', new Uint8Array([FRAME_STOP_SENDING, 0, 0, 0, 0, 0])],
    ['short MAX_STREAM_DATA', new Uint8Array([0x04, 1, 2, 3])],
    ['unknown type', new Uint8Array([0x05, 1])],
  ]
  for (const [name, bytes] of cases) {
    assert.throws(() => parseFrame(bytes), ProtocolViolation, name)
  }
  // MAX_STREAM_DATA above MAX_OFFSET
  const over = new Uint8Array(9)
  over[0] = 0x04
  new DataView(over.buffer).setBigUint64(1, MAX_OFFSET + 1n, false)
  assert.throws(() => parseFrame(over), ProtocolViolation, 'MAX_STREAM_DATA above ceiling')
  // boundary value is fine
  assert.equal(parseFrame(encodeMaxStreamData(MAX_OFFSET)).value, MAX_OFFSET)
  // max-size DATA is fine
  assert.equal(parseFrame(encodeData(new Uint8Array(MAX_FRAME_PAYLOAD))).payload.length, MAX_FRAME_PAYLOAD)
})

test('control codec round-trips and rejects violations', () => {
  const limits = { initialMaxStreamData: 1n << 20n, initialMaxData: 8n << 20n, initialMaxStreams: 100n }
  const hello = decodeControl(encodeHello(limits))
  assert.equal(hello.t, 'hello')
  assert.equal(hello.version, WIRE_VERSION)
  assert.deepEqual(hello.limits, limits)

  assert.deepEqual(decodeControl(encodeConnClose('reset')), { t: 'close', code: 3 })
  assert.deepEqual(decodeControl(encodeMaxData(42n)), { t: 'max-data', value: 42n })
  assert.deepEqual(decodeControl(encodeMaxStreams(7n)), { t: 'max-streams', value: 7n })

  assert.throws(() => decodeControl(new Uint8Array(0)), ProtocolViolation, 'empty')
  assert.throws(() => decodeControl(new Uint8Array([0x00, 0, 0, 0])), ProtocolViolation, 'short close')
  assert.throws(() => decodeControl(new Uint8Array([0x01, 1, 0, 0])), ProtocolViolation, 'short hello')
  assert.throws(() => decodeControl(new Uint8Array([0x07, 0])), ProtocolViolation, 'unknown type')
  const overMax = encodeMaxData(0n)
  new DataView(overMax.buffer).setBigUint64(1, MAX_OFFSET + 1n, false)
  assert.throws(() => decodeControl(overMax), ProtocolViolation, 'MAX_DATA above ceiling')
})

// ---------- ConnFlow / StreamFlow (pure credit engine) ----------

function makeFlow(localOverrides = {}, sink = {}) {
  const sent = { maxData: [], maxStreams: [] }
  const flow = new ConnFlow(resolveLimits(localOverrides), {
    sendMaxData: v => { sent.maxData.push(v); sink.sendMaxData?.(v) },
    sendMaxStreams: v => { sent.maxStreams.push(v); sink.sendMaxStreams?.(v) }
  })
  return { flow, sent }
}

test('flow: reservations block until peer HELLO grants credit', async () => {
  const { flow } = makeFlow()
  const streamAdverts = []
  const sf = flow.newStream(v => streamAdverts.push(v))

  let reserved = false
  const p = sf.reserve(10).then(() => { reserved = true })
  await settle()
  assert.equal(reserved, false, 'no credit before HELLO')

  flow.onPeerHello({ initialMaxStreamData: 100n, initialMaxData: 1000n, initialMaxStreams: 5n })
  await p
  assert.equal(reserved, true)
  sf.commit(10)
})

test('flow: stream window blocks and MAX_STREAM_DATA resumes', async () => {
  const { flow } = makeFlow()
  const sf = flow.newStream(() => {})
  flow.onPeerHello({ initialMaxStreamData: 10n, initialMaxData: 1000n, initialMaxStreams: 5n })

  await sf.reserve(10)
  sf.commit(10)
  let second = false
  const p = sf.reserve(1).then(() => { second = true })
  await settle()
  assert.equal(second, false, 'stream window exhausted')

  sf.onPeerMaxStreamData(5n) // lower than effective limit: ignored
  await settle()
  assert.equal(second, false)

  sf.onPeerMaxStreamData(11n)
  await p
  assert.equal(second, true)
})

test('flow: connection window is shared across streams and atomic', async () => {
  const { flow } = makeFlow()
  const a = flow.newStream(() => {})
  const b = flow.newStream(() => {})
  flow.onPeerHello({ initialMaxStreamData: 100n, initialMaxData: 15n, initialMaxStreams: 5n })

  assert.equal(await a.reserve(10), 10)
  // Only 5 bytes of connection credit remain: partial grant.
  assert.equal(await b.reserve(10), 5)
  let bMore = 0
  const p = b.reserve(10).then(m => { bMore = m })
  await settle()
  assert.equal(bMore, 0, 'conn window cannot be double-spent')

  a.release(10) // discarded unsent → releases connection credit
  await p
  assert.equal(bMore, 10)
})

test('flow: receiver enforcement throws on over-credit DATA', () => {
  const { flow } = makeFlow({ initialMaxStreamData: 10n, initialMaxData: 15n })
  const sf = flow.newStream(() => {})
  sf.onDataReceived(10)
  assert.throws(() => sf.onDataReceived(1), ProtocolViolation, 'stream window exceeded')

  const sf2 = flow.newStream(() => {})
  sf2.onDataReceived(5)
  assert.throws(() => sf2.onDataReceived(1), ProtocolViolation, 'conn window exceeded')
})

test('flow: consumption advertises past half the window', () => {
  const { flow, sent } = makeFlow({ initialMaxStreamData: 10n, initialMaxData: 100n })
  const adverts = []
  const sf = flow.newStream(v => adverts.push(v))
  sf.onDataReceived(10)
  sf.onConsumed(4)
  assert.equal(adverts.length, 0, 'below half window: batched')
  sf.onConsumed(1)
  assert.deepEqual(adverts, [15n], 'consumed(5)+window(10)')
  // after cancelRead, stream credit stops but connection credit still flows
  const sf2 = flow.newStream(v => adverts.push(v))
  sf2.markCancelled()
  sf2.onDataReceived(10)
  sf2.onConsumed(10)
  assert.equal(adverts.length, 1, 'no stream credit after cancel')
  // conn: 15 consumed of 100-window → below half, no MAX_DATA yet
  assert.equal(sent.maxData.length, 0)
})

test('flow: stream slots reserve/commit and MAX_STREAMS grants', async () => {
  const { flow, sent } = makeFlow({ initialMaxStreams: 2n })
  flow.onPeerHello({ initialMaxStreamData: 10n, initialMaxData: 100n, initialMaxStreams: 1n })

  await flow.reserveStreamSlot()
  flow.commitStreamSlot()
  let second = false
  const p = flow.reserveStreamSlot().then(() => { second = true })
  await settle()
  assert.equal(second, false, 'peer limit of 1 stream')
  flow.onPeerMaxStreams(2n)
  await p
  assert.equal(second, true)

  // receiver side: observed peer streams against our limit of 2
  flow.peerStreamOpened()
  flow.peerStreamOpened()
  assert.throws(() => flow.peerStreamOpened(), ProtocolViolation, 'third stream over limit')
  flow.peerStreamRetired()
  assert.deepEqual(sent.maxStreams, [3n], 'retirement grants a replacement slot')
  flow.peerStreamOpened() // now allowed
})

// ---------- mock channels + linked engines ----------

class MockChannel {
  #h = {}
  opened = false
  closed = false
  peer = null

  static pair() {
    const a = new MockChannel()
    const b = new MockChannel()
    a.peer = b
    b.peer = a
    return [a, b]
  }

  isOpen() { return this.opened && !this.closed }
  send(data) {
    if (!this.isOpen()) throw new Error('mock channel not open')
    const copy = data.slice()
    const peer = this.peer
    queueMicrotask(() => { if (!peer.closed) peer.#h.message?.(copy) })
  }
  bufferedAmount() { return 0 }
  setBufferedAmountLowThreshold() {}
  onBufferedAmountLow(cb) { this.#h.bal = cb }
  onMessage(cb) { this.#h.message = cb }
  onOpen(cb) { this.#h.open = cb }
  onClose(cb) { this.#h.close = cb }
  onError(cb) { this.#h.error = cb }
  doOpen() {
    if (this.opened) return
    this.opened = true
    this.#h.open?.()
  }
  close() {
    if (this.closed) return
    this.closed = true
    const peer = this.peer
    queueMicrotask(() => this.#h.close?.())
    queueMicrotask(() => peer?.closeFromPeer())
  }
  closeFromPeer() {
    if (this.closed) return
    this.closed = true
    this.#h.close?.()
  }
}

// Two conforming engines linked by mock channels.
function linkedCores(limitsA = {}, limitsB = {}) {
  const [ctrlA, ctrlB] = MockChannel.pair()
  const [dgA, dgB] = MockChannel.pair()
  /* eslint-disable prefer-const */
  let coreA, coreB
  const openToward = (getPeerCore) => (label) => {
    const [mine, theirs] = MockChannel.pair()
    queueMicrotask(() => {
      getPeerCore().handleIncomingChannel(theirs)
      theirs.doOpen()
      mine.doOpen()
    })
    return mine
  }
  coreA = new ConnCore({
    control: ctrlA, datagram: dgA, limits: limitsA,
    openChannel: openToward(() => coreB),
    closeTransport() {}
  })
  coreB = new ConnCore({
    control: ctrlB, datagram: dgB, limits: limitsB,
    openChannel: openToward(() => coreA),
    closeTransport() {}
  })
  ctrlA.doOpen(); ctrlB.doOpen(); dgA.doOpen(); dgB.doOpen()
  return { coreA, coreB }
}

// One real engine against raw channel ends the test scripts directly.
function rawPeer(limits = {}) {
  const [ctrlA, ctrlB] = MockChannel.pair()
  const [dgA, dgB] = MockChannel.pair()
  const raw = {
    control: ctrlB,
    datagram: dgB,
    controlLog: [],
    openedByCore: [],
    openChannelToCore(core) {
      const [mine, theirs] = MockChannel.pair()
      core.handleIncomingChannel(theirs)
      theirs.doOpen()
      mine.doOpen()
      return mine
    }
  }
  ctrlB.onMessage(d => raw.controlLog.push(decodeControl(d)))
  const core = new ConnCore({
    control: ctrlA, datagram: dgA, limits,
    openChannel() {
      const [mine, theirs] = MockChannel.pair()
      raw.openedByCore.push(theirs)
      queueMicrotask(() => { theirs.doOpen(); mine.doOpen() })
      return mine
    },
    closeTransport() {}
  })
  ctrlA.doOpen(); ctrlB.doOpen(); dgA.doOpen(); dgB.doOpen()
  return { core, raw }
}

const enc = s => new TextEncoder().encode(s)
const dec = b => new TextDecoder().decode(b)

async function readAll(stream) {
  const reader = stream.readable.getReader()
  const parts = []
  for (;;) {
    const { value, done } = await reader.read()
    if (done) break
    parts.push(value)
  }
  const out = new Uint8Array(parts.reduce((n, p) => n + p.length, 0))
  let off = 0
  for (const p of parts) { out.set(p, off); off += p.length }
  return out
}

// ---------- engine end-to-end (conforming peers) ----------

test('engine: mutual HELLO establishes; echo round-trip', async () => {
  const { coreA, coreB } = linkedCores()
  await coreA.established
  await coreB.established

  const [sA, sB] = await Promise.all([coreA.openStream(), coreB.acceptStream()])
  const w = sA.writable.getWriter()
  await w.write(enc('hello kps'))
  await w.close()
  assert.equal(dec(await readAll(sB)), 'hello kps')
})

test('engine: a never-reading receiver blocks the sender at its window', async () => {
  // B grants tiny windows; A tries to send more.
  const { coreA, coreB } = linkedCores({}, { initialMaxStreamData: 64n, initialMaxData: 1024n })
  await coreA.established
  const [sA, sB] = await Promise.all([coreA.openStream(), coreB.acceptStream()])

  const w = sA.writable.getWriter()
  let wrote = false
  const p = w.write(new Uint8Array(65)).then(() => { wrote = true })
  await settle(10)
  assert.equal(wrote, false, '65th byte must wait for credit')

  // Reading on B fulfills consumption → credit returns → the write completes.
  const reader = sB.readable.getReader()
  const got = []
  while (got.reduce((n, c) => n + c.length, 0) < 65) {
    const { value, done } = await reader.read()
    if (done) break
    got.push(value)
  }
  await p
  assert.equal(wrote, true)
  assert.equal(got.reduce((n, c) => n + c.length, 0), 65)
})

test('engine: connection window is shared across streams', async () => {
  const { coreA, coreB } = linkedCores({}, { initialMaxStreamData: 1024n, initialMaxData: 100n })
  await coreA.established
  const s1 = await coreA.openStream()
  const s2 = await coreA.openStream()

  const w1 = s1.writable.getWriter()
  const w2 = s2.writable.getWriter()
  await w1.write(new Uint8Array(100)) // consumes the whole connection window
  let second = false
  const p = w2.write(new Uint8Array(1)).then(() => { second = true })
  await settle(10)
  assert.equal(second, false, 'second stream starved by conn window')

  // B reads stream 1 → MAX_DATA credit returns → stream 2 unblocks.
  const b1 = await coreB.acceptStream()
  await readAllUpTo(b1, 100)
  await p
  assert.equal(second, true)
})

async function readAllUpTo(stream, n) {
  const reader = stream.readable.getReader()
  let got = 0
  while (got < n) {
    const { value, done } = await reader.read()
    if (done) break
    got += value.length
  }
  reader.releaseLock()
  return got
}

test('engine: MAX_STREAMS blocks the opener until a stream retires', async () => {
  const { coreA, coreB } = linkedCores({}, { initialMaxStreams: 1n })
  await coreA.established

  const s1 = await coreA.openStream()
  let opened = false
  const p = coreA.openStream().then(s => { opened = true; return s })
  await settle(10)
  assert.equal(opened, false, 'second open waits at the limit')

  // Retire stream 1: both halves terminal + drained on both sides.
  const b1 = await coreB.acceptStream()
  await s1.close()      // FIN + STOP_SENDING from A
  await b1.close()      // FIN (auto-RESET may already have fired) from B
  await readAll(b1).catch(() => {})
  await settle(20)
  assert.equal(opened, true, 'retirement returned a slot')
  await p
})

test('engine: retirement credit is withheld while data is unread', async () => {
  const { core, raw } = rawPeer({ initialMaxStreams: 5n })
  raw.control.send(encodeHello(resolveLimits()))
  await core.established

  // Raw peer opens a stream, sends 1 byte + FIN.
  const ch = raw.openChannelToCore(core)
  ch.send(encodeData(enc('x')))
  ch.send(encodeFin())
  await settle()

  const s = await core.acceptStream()
  await s.closeWrite() // local terminal → wire-complete, but 1 byte unread
  await settle(10)
  assert.ok(
    !raw.controlLog.some(m => m.t === 'max-streams'),
    'no MAX_STREAMS while a byte is unread'
  )

  assert.equal(dec(await readAll(s)), 'x') // drain → retire → channel closes → credit
  await settle(10)
  assert.ok(raw.controlLog.some(m => m.t === 'max-streams'), 'MAX_STREAMS after drain')
})

test('engine: datagrams round-trip and are bounded', async () => {
  const { coreA, coreB } = linkedCores()
  await coreA.established
  await coreB.established
  await coreA.sendDatagram(enc('dg'))
  assert.equal(dec(await coreB.receiveDatagram()), 'dg')
  await assert.rejects(coreA.sendDatagram(new Uint8Array(1300)), /too-large|exceeds/)
})

// ---------- adversarial (raw peer) ----------

test('engine: unsupported wire version → close(unsupported)', async () => {
  const { core, raw } = rawPeer()
  raw.control.send(encodeHello(resolveLimits(), 2))
  const info = await core.closed
  assert.equal(info.ok, false)
  assert.equal(info.reason?.code, 'unsupported')
  await settle()
  assert.ok(raw.controlLog.some(m => m.t === 'close' && m.code === 7), 'peer told: unsupported')
  await assert.rejects(core.established)
})

test('engine: CONNECTION_CLOSE before HELLO is a clean rejection', async () => {
  const { core, raw } = rawPeer()
  raw.control.send(encodeConnClose('permission-denied'))
  const info = await core.closed
  assert.equal(info.ok, false)
  assert.equal(info.reason?.code, 'permission-denied')
  await assert.rejects(core.established)
})

test('engine: duplicate HELLO and pre-HELLO credit are protocol errors', async () => {
  {
    const { core, raw } = rawPeer()
    raw.control.send(encodeHello(resolveLimits()))
    raw.control.send(encodeHello(resolveLimits()))
    const info = await core.closed
    assert.equal(info.reason?.code, 'protocol-error')
  }
  {
    const { core, raw } = rawPeer()
    raw.control.send(encodeMaxData(1n)) // before HELLO
    const info = await core.closed
    assert.equal(info.reason?.code, 'protocol-error')
  }
})

test('engine: malformed control message is fatal, never a clean close', async () => {
  const { core, raw } = rawPeer()
  raw.control.send(new Uint8Array([0x00, 0, 0])) // truncated CONNECTION_CLOSE
  const info = await core.closed
  assert.equal(info.ok, false)
  assert.equal(info.reason?.code, 'protocol-error')
})

test('engine: streams observed before peer HELLO are staged, not surfaced', async () => {
  const { core, raw } = rawPeer()
  const ch = raw.openChannelToCore(core)
  ch.send(encodeData(enc('early')))
  await settle()

  let accepted = false
  const p = core.acceptStream().then(s => { accepted = true; return s })
  await settle()
  assert.equal(accepted, false, 'staged until mutual HELLO')

  raw.control.send(encodeHello(resolveLimits()))
  const s = await p
  assert.equal(accepted, true)
  const reader = s.readable.getReader()
  const { value } = await reader.read()
  assert.equal(dec(value), 'early')
})

test('engine: poison frames are connection-fatal', async () => {
  const poisons = [
    ['empty DATA', ch => ch.send(new Uint8Array([FRAME_DATA]))],
    ['unknown frame type', ch => ch.send(new Uint8Array([0x09, 1]))],
    ['DATA after FIN', ch => { ch.send(encodeFin()); ch.send(encodeData(enc('x'))) }],
    ['second terminal', ch => { ch.send(encodeFin()); ch.send(encodeCode(FRAME_RESET, 3)) }],
    ['short RESET', ch => ch.send(new Uint8Array([FRAME_RESET, 0]))],
  ]
  for (const [name, poison] of poisons) {
    const { core, raw } = rawPeer()
    raw.control.send(encodeHello(resolveLimits()))
    await core.established
    const ch = raw.openChannelToCore(core)
    poison(ch)
    const info = await core.closed
    assert.equal(info.reason?.code, 'protocol-error', name)
  }
})

test('engine: DATA beyond the advertised stream window is fatal', async () => {
  const { core, raw } = rawPeer({ initialMaxStreamData: 4n })
  raw.control.send(encodeHello(resolveLimits()))
  await core.established
  const ch = raw.openChannelToCore(core)
  ch.send(encodeData(enc('12345'))) // 5 > 4
  const info = await core.closed
  assert.equal(info.reason?.code, 'protocol-error')
})

test('engine: opening streams beyond MAX_STREAMS is fatal', async () => {
  const { core, raw } = rawPeer({ initialMaxStreams: 1n })
  raw.control.send(encodeHello(resolveLimits()))
  await core.established
  raw.openChannelToCore(core)
  raw.openChannelToCore(core) // second: over the limit
  const info = await core.closed
  assert.equal(info.reason?.code, 'protocol-error')
})

test('engine: unexpected mid-stream channel close is fatal', async () => {
  const { core, raw } = rawPeer()
  raw.control.send(encodeHello(resolveLimits()))
  await core.established
  const ch = raw.openChannelToCore(core)
  ch.send(encodeData(enc('hi')))
  await settle()
  ch.close() // no terminal frames: not wire-complete
  const info = await core.closed
  assert.equal(info.reason?.code, 'protocol-error')
})

test('engine: reserved channel loss is fatal', async () => {
  const { core, raw } = rawPeer()
  raw.control.send(encodeHello(resolveLimits()))
  await core.established
  raw.datagram.close()
  const info = await core.closed
  assert.equal(info.reason?.code, 'protocol-error')
})

test('engine: STOP_SENDING triggers auto-RESET and fails writers', async () => {
  const { core, raw } = rawPeer()
  raw.control.send(encodeHello(resolveLimits()))
  await core.established
  const s = await core.openStream()
  await settle()
  const ch = raw.openedByCore.at(-1)
  const frames = []
  ch.onMessage(d => frames.push(parseFrame(d)))
  ch.send(encodeCode(FRAME_STOP_SENDING, 1))
  await settle()
  assert.ok(frames.some(f => f.type === 'reset'), 'auto-RESET reply')
  const w = s.writable.getWriter()
  await assert.rejects(w.write(enc('x')), /cancelled|reset/)
})

test('engine: cancelRead discards, sends STOP_SENDING, withholds stream credit', async () => {
  const { core, raw } = rawPeer({ initialMaxStreamData: 8n, initialMaxData: 64n })
  raw.control.send(encodeHello(resolveLimits()))
  await core.established
  const ch = raw.openChannelToCore(core)
  const frames = []
  ch.onMessage(d => frames.push(parseFrame(d)))
  ch.send(encodeData(enc('12345678'))) // fills the 8-byte stream window
  await settle()

  const s = await core.acceptStream()
  await s.cancelRead({ code: 'cancelled' })
  await settle()
  assert.ok(frames.some(f => f.type === 'stop-sending'), 'STOP_SENDING sent')
  assert.ok(!frames.some(f => f.type === 'max-stream-data'), 'no stream credit after cancel')
  // The discarded 8 bytes must still release connection credit eventually
  // (below the half-window threshold here, so just assert no violation killed
  // the connection and the peer can finish the stream).
  ch.send(encodeCode(FRAME_RESET, 1)) // conforming reply to STOP_SENDING
  await s.closeWrite()
  await settle(10)
  assert.notEqual(core.state, 'closed')
})

test('engine: graceful close sends CONNECTION_CLOSE with the code', async () => {
  const { core, raw } = rawPeer()
  raw.control.send(encodeHello(resolveLimits()))
  await core.established
  core.close({ code: 'closed' })
  await settle()
  assert.ok(raw.controlLog.some(m => m.t === 'close' && m.code === 2))
  const info = await core.closed
  assert.equal(info.ok, true)
})

// Park a read on `stream` (no inbound bytes) and return a promise that settles
// to {done} | {err} — handler attached synchronously so a rejection is handled.
function parkRead(stream) {
  return stream.readable.getReader().read().then(
    v => ({ done: v.done }),
    e => ({ err: e }),
  )
}

test('engine: §9.2 — local read termination errors a parked read, never EOF', async () => {
  // (a) local stream.close() → read rejects with 'closed'
  {
    const { coreA, coreB } = linkedCores()
    await coreA.established
    const a = await coreA.openStream()
    await coreB.acceptStream()
    const settled = parkRead(a)
    await settle()
    await a.close()
    const r = await settled
    assert.equal(r.done, undefined, 'must not resolve as EOF')
    assert.equal(r.err?.code, 'closed')
  }
  // (b) local cancelRead(code) → read rejects with that code
  {
    const { coreA, coreB } = linkedCores()
    await coreA.established
    const a = await coreA.openStream()
    await coreB.acceptStream()
    const settled = parkRead(a)
    await settle()
    await a.cancelRead({ code: 'cancelled' })
    const r = await settled
    assert.equal(r.err?.code, 'cancelled')
  }
  // (c) connection close (no reason) → read on a still-open stream rejects 'closed'
  {
    const { coreA, coreB } = linkedCores()
    await coreA.established
    const a = await coreA.openStream()
    await coreB.acceptStream()
    const settled = parkRead(a)
    await settle()
    coreA.close() // no reason → orderly local teardown
    const r = await settled
    assert.equal(r.done, undefined, 'connection close must error the read, not EOF')
    assert.equal(r.err?.code, 'closed')
  }
})

test('engine: §9.2 — peer FIN still yields clean EOF (contrast)', async () => {
  const { coreA, coreB } = linkedCores()
  await coreA.established
  const a = await coreA.openStream()
  const b = await coreB.acceptStream()
  const settled = parkRead(a)
  await settle()
  await b.closeWrite() // peer's write half finishes → FIN
  const r = await settled
  assert.deepEqual(r, { done: true }, 'peer FIN → EOF, no error')
})
