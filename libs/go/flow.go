package kps

import (
	"context"
	"sync"
)

// End-to-end flow control for the WebRTC mapping (SPEC §6.5): the credit
// engine. One connFlow per connection tracks both directions of
// connection-level credit plus stream-count credit; one streamFlow per stream
// tracks that stream's credit. All streamFlow state is guarded by the owning
// connFlow's mutex (one lock per connection — reservations must be atomic
// across streams anyway).
//
// Sender side: `sent + reserved + n ≤ peerMax` at both levels, reserved
// atomically BEFORE a DATA frame may be sent; a blocked reservation waits for
// peer credit. Grants may be partial (a write larger than the whole window
// splits at the window boundary, like a QUIC sender). Receiver side:
// `received + n ≤ localMax` enforced before buffering; consumption (read
// fulfilled or explicit discard) advances counters and re-advertises credit
// once half a window is consumed-but-unadvertised.

// flowLimits are a receiver's initial windows, announced in HELLO.
type flowLimits struct {
	maxStreamData uint64
	maxData       uint64
	maxStreams    uint64
}

// Recommended initial windows (SPEC §6.5) — receiver policy, not protocol
// constants.
var defaultFlowLimits = flowLimits{
	maxStreamData: 1 << 20, // 1 MiB
	maxData:       8 << 20, // 8 MiB
	maxStreams:    100,
}

func satOffset(v uint64) uint64 {
	if v > maxOffset {
		return maxOffset
	}
	return v
}

type connFlow struct {
	mu     sync.Mutex
	wakeCh chan struct{} // closed+replaced on any credit grant/release/failure
	failed error

	local flowLimits

	// sender side (peer-granted; zero until the peer's HELLO)
	peerInitStreamData uint64
	peerMaxData        uint64
	peerMaxStreams     uint64
	connSent           uint64
	connReserved       uint64
	streamsOpened      uint64
	streamsReserved    uint64

	// receiver side
	localMaxData         uint64 // enforcement limit (advances at commit-to-send)
	connReceived         uint64
	connConsumed         uint64
	connAdvertisedAt     uint64
	peerOpenedStreams    uint64
	peerRetiredStreams   uint64
	advertisedMaxStreams uint64

	// Advertisement senders, wired by the conn; called OUTSIDE the mutex
	// (dc.Send may block on the transport buffer).
	sendMaxData    func(uint64)
	sendMaxStreams func(uint64)
}

func newConnFlow(local flowLimits) *connFlow {
	return &connFlow{
		wakeCh:               make(chan struct{}),
		local:                local,
		localMaxData:         local.maxData,
		advertisedMaxStreams: local.maxStreams,
	}
}

func (f *connFlow) wakeLocked() {
	close(f.wakeCh)
	f.wakeCh = make(chan struct{})
}

// fail rejects every pending and future credit wait (connection teardown).
func (f *connFlow) fail(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failed == nil {
		f.failed = err
		f.wakeLocked()
	}
}

// onPeerHello seeds every send-side limit from the peer's HELLO.
func (f *connFlow) onPeerHello(l flowLimits) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.peerInitStreamData = l.maxStreamData
	f.peerMaxData = l.maxData
	f.peerMaxStreams = l.maxStreams
	f.wakeLocked()
}

// onPeerMaxData / onPeerMaxStreams raise limits; decreases are ignored.
func (f *connFlow) onPeerMaxData(v uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if v > f.peerMaxData {
		f.peerMaxData = v
		f.wakeLocked()
	}
}

func (f *connFlow) onPeerMaxStreams(v uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if v > f.peerMaxStreams {
		f.peerMaxStreams = v
		f.wakeLocked()
	}
}

func (f *connFlow) newStream(sendMaxStreamData func(uint64)) *streamFlow {
	return &streamFlow{
		conn:              f,
		localMax:          f.local.maxStreamData,
		sendMaxStreamData: sendMaxStreamData,
	}
}

// reserveData reserves up to n DATA payload bytes at both levels, blocking
// until at least one byte of credit is available. Returns the granted amount
// (1..n). Fails when the stream's write half fails (STOP_SENDING, reset,
// close) or the connection fails. Blocking without a context mirrors
// io.Writer's contract (like the QUIC stream's Write); connection teardown
// unblocks it.
func (f *connFlow) reserveData(sf *streamFlow, n uint64) (uint64, error) {
	for {
		f.mu.Lock()
		if f.failed != nil {
			err := f.failed
			f.mu.Unlock()
			return 0, err
		}
		if sf.sendFailure != nil {
			err := sf.sendFailure
			f.mu.Unlock()
			return 0, err
		}
		grant := min(sf.peerMaxLocked()-sf.sendSent-sf.sendReserved, f.peerMaxData-f.connSent-f.connReserved, n)
		if grant >= 1 {
			sf.sendReserved += grant
			f.connReserved += grant
			f.mu.Unlock()
			return grant, nil
		}
		ch := f.wakeCh
		f.mu.Unlock()
		<-ch
	}
}

// commitData: bytes passed to the transport (reserved → sent, both levels).
func (f *connFlow) commitData(sf *streamFlow, n uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	sf.sendReserved -= n
	sf.sendSent += n
	f.connReserved -= n
	f.connSent += n
}

// releaseData: a reserved-but-unsent frame was discarded.
func (f *connFlow) releaseData(sf *streamFlow, n uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	sf.sendReserved -= n
	f.connReserved -= n
	f.wakeLocked()
}

// reserveStreamSlot reserves a slot to open one stream, waiting at the limit.
func (f *connFlow) reserveStreamSlot(ctx context.Context) error {
	for {
		f.mu.Lock()
		if f.failed != nil {
			err := f.failed
			f.mu.Unlock()
			return err
		}
		if f.streamsOpened+f.streamsReserved < f.peerMaxStreams {
			f.streamsReserved++
			f.mu.Unlock()
			return nil
		}
		ch := f.wakeCh
		f.mu.Unlock()
		select {
		case <-ch:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// commitStreamSlot: channel creation succeeded (the count never decreases).
func (f *connFlow) commitStreamSlot() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.streamsReserved--
	f.streamsOpened++
}

// releaseStreamSlot: channel creation failed synchronously.
func (f *connFlow) releaseStreamSlot() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.streamsReserved--
	f.wakeLocked()
}

// peerStreamOpened records an observed peer-initiated stream (it consumes a
// slot immediately, even unaccepted or pre-HELLO). Errors when the peer
// exceeds MAX_STREAMS.
func (f *connFlow) peerStreamOpened() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.peerOpenedStreams >= f.advertisedMaxStreams {
		return errProtocol("peer exceeded MAX_STREAMS")
	}
	f.peerOpenedStreams++
	return nil
}

// peerStreamRetired grants a replacement slot for a retired peer-initiated
// stream (sends MAX_STREAMS outside the lock).
func (f *connFlow) peerStreamRetired() {
	f.mu.Lock()
	f.peerRetiredStreams++
	f.advertisedMaxStreams = satOffset(f.local.maxStreams + f.peerRetiredStreams)
	v, send := f.advertisedMaxStreams, f.sendMaxStreams
	f.mu.Unlock()
	if send != nil {
		send(v)
	}
}

type streamFlow struct {
	conn *connFlow

	// sender side (guarded by conn.mu)
	sendSent        uint64
	sendReserved    uint64
	peerMaxExplicit uint64 // largest MAX_STREAM_DATA received on this stream
	sendFailure     error

	// receiver side (guarded by conn.mu)
	localMax     uint64 // enforcement limit
	received     uint64
	consumed     uint64
	advertisedAt uint64
	cancelled    bool // local cancelRead: no further stream credit

	// Called OUTSIDE the mutex.
	sendMaxStreamData func(uint64)
}

// peerMaxLocked: effective peer window — explicit updates never lower it below
// the HELLO initial (streams staged before the peer's HELLO start at 0 and see
// the window the moment it arrives). Caller holds conn.mu.
func (sf *streamFlow) peerMaxLocked() uint64 {
	return max(sf.peerMaxExplicit, sf.conn.peerInitStreamData)
}

func (sf *streamFlow) reserve(n uint64) (uint64, error) { return sf.conn.reserveData(sf, n) }
func (sf *streamFlow) commit(n uint64)                  { sf.conn.commitData(sf, n) }
func (sf *streamFlow) release(n uint64)                 { sf.conn.releaseData(sf, n) }

// failSend fails pending and future reservations (STOP_SENDING, reset, close).
func (sf *streamFlow) failSend(err error) {
	f := sf.conn
	f.mu.Lock()
	defer f.mu.Unlock()
	if sf.sendFailure == nil {
		sf.sendFailure = err
		f.wakeLocked() // conn-wide wake; waiters re-check their own state
	}
}

// onPeerMaxStreamData: MAX_STREAM_DATA from the peer; decreases are ignored.
func (sf *streamFlow) onPeerMaxStreamData(v uint64) {
	f := sf.conn
	f.mu.Lock()
	defer f.mu.Unlock()
	if v > sf.peerMaxExplicit {
		sf.peerMaxExplicit = v
		f.wakeLocked()
	}
}

// onDataReceived enforces both receive windows atomically before n inbound
// payload bytes may be buffered. Errors when the peer exceeds either window.
func (sf *streamFlow) onDataReceived(n uint64) error {
	f := sf.conn
	f.mu.Lock()
	defer f.mu.Unlock()
	if sf.received+n > sf.localMax {
		return errProtocol("peer exceeded MAX_STREAM_DATA")
	}
	if f.connReceived+n > f.localMaxData {
		return errProtocol("peer exceeded MAX_DATA")
	}
	sf.received += n
	f.connReceived += n
	return nil
}

// onConsumed records n consumed bytes (read-fulfilled to the application or
// explicitly discarded) and advertises replacement credit past the half-window
// threshold. Stream credit is withheld after cancelRead; connection credit
// always flows so a discarded stream cannot starve unrelated streams. The
// local enforcement limits advance here — at commit-to-send — not when the
// peer acknowledges (§6.5).
func (sf *streamFlow) onConsumed(n uint64) {
	f := sf.conn
	f.mu.Lock()
	sf.consumed += n
	var streamAdv uint64
	if !sf.cancelled && sf.consumed-sf.advertisedAt >= f.local.maxStreamData/2 {
		sf.advertisedAt = sf.consumed
		sf.localMax = satOffset(sf.consumed + f.local.maxStreamData)
		streamAdv = sf.localMax
	}
	f.connConsumed += n
	var connAdv uint64
	if f.connConsumed-f.connAdvertisedAt >= f.local.maxData/2 {
		f.connAdvertisedAt = f.connConsumed
		f.localMaxData = satOffset(f.connConsumed + f.local.maxData)
		connAdv = f.localMaxData
	}
	sendStream, sendConn := sf.sendMaxStreamData, f.sendMaxData
	f.mu.Unlock()
	if streamAdv > 0 && sendStream != nil {
		sendStream(streamAdv)
	}
	if connAdv > 0 && sendConn != nil {
		sendConn(connAdv)
	}
}

// markCancelled: local cancelRead — stop granting stream credit; discards
// still free MAX_DATA.
func (sf *streamFlow) markCancelled() {
	f := sf.conn
	f.mu.Lock()
	defer f.mu.Unlock()
	sf.cancelled = true
}
