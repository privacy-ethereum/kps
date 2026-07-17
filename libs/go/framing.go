package kps

import (
	"encoding/binary"
	"fmt"
)

// Internal stream framing (SPEC §6.2, wire version 1). Each WebRTC data-channel
// message is exactly one frame: a 1-byte type, then a type-specific payload.
// This makes a reliable+ordered, message-oriented SCTP data channel present as
// a byte stream with QUIC-like lifecycle (graceful FIN, write reset, read stop)
// plus per-stream flow-control credit (§6.5). The framing is internal to KPS —
// applications never see it.
type frameType byte

const (
	frameData          frameType = 0x00 // payload = stream bytes (1..maxFramePayload)
	frameFin           frameType = 0x01 // no payload — local write half finished
	frameReset         frameType = 0x02 // payload = uint32 code — write half aborted
	frameStopSending   frameType = 0x03 // payload = uint32 code — peer cancelled its read
	frameMaxStreamData frameType = 0x04 // payload = uint64 absolute credit (§6.5)
)

// A frame (type byte + payload) never exceeds maxWebRTCFrameSize: an SCTP user
// message is reassembled before KPS can inspect it, so credit alone cannot
// bound one message. DATA therefore carries 1..maxFramePayload bytes; larger
// Writes are split, empty writes produce no frame.
const (
	maxWebRTCFrameSize = 16384
	maxFramePayload    = maxWebRTCFrameSize - 1
)

// maxOffset is the ceiling for every offset, limit and count (QUIC's integer
// range, SPEC §6.5).
const maxOffset = uint64(1)<<62 - 1

// errProtocol wraps a wire-rule violation by the peer: the connection is closed
// with CodeProtocolError. Within a wire version, malformed input is never
// tolerated or read as something weaker.
func errProtocol(format string, args ...any) error {
	return fmt.Errorf("kps: protocol violation: "+format, args...)
}

// ErrorCode is the application-level reset/cancel code carried in RESET and
// STOP_SENDING frames. The values are the canonical registry from SPEC §9.1 and
// are shared with the QUIC transport's stream error codes.
type ErrorCode uint32

const (
	CodeNone             ErrorCode = 0
	CodeCancelled        ErrorCode = 1
	CodeClosed           ErrorCode = 2
	CodeReset            ErrorCode = 3
	CodeTimeout          ErrorCode = 4
	CodeNetworkError     ErrorCode = 5
	CodeProtocolError    ErrorCode = 6
	CodeUnsupported      ErrorCode = 7
	CodeTooLarge         ErrorCode = 8
	CodeQueueFull        ErrorCode = 9
	CodePermissionDenied ErrorCode = 10
	CodeInternalError    ErrorCode = 11
)

func encodeData(p []byte) []byte {
	out := make([]byte, 1+len(p))
	out[0] = byte(frameData)
	copy(out[1:], p)
	return out
}

func encodeFin() []byte {
	return []byte{byte(frameFin)}
}

func encodeCode(t frameType, code ErrorCode) []byte {
	out := make([]byte, 5)
	out[0] = byte(t)
	binary.BigEndian.PutUint32(out[1:], uint32(code))
	return out
}

func encodeMaxStreamData(v uint64) []byte {
	out := make([]byte, 9)
	out[0] = byte(frameMaxStreamData)
	binary.BigEndian.PutUint64(out[1:], v)
	return out
}

// parsedFrame is one strictly-validated §6.2 frame.
type parsedFrame struct {
	typ     frameType
	payload []byte // DATA only
	code    ErrorCode
	credit  uint64 // MAX_STREAM_DATA only
}

// parseFrame validates one data-channel message against the wire-version-1
// rules: unknown types, wrong payload lengths, empty or oversized DATA, and
// out-of-range credit are all protocol violations (connection-fatal).
func parseFrame(data []byte) (parsedFrame, error) {
	if len(data) == 0 {
		return parsedFrame{}, errProtocol("empty data-channel message")
	}
	if len(data) > maxWebRTCFrameSize {
		return parsedFrame{}, errProtocol("frame exceeds %d bytes (%d)", maxWebRTCFrameSize, len(data))
	}
	t := frameType(data[0])
	payload := data[1:]
	switch t {
	case frameData:
		if len(payload) == 0 {
			return parsedFrame{}, errProtocol("empty DATA frame")
		}
		return parsedFrame{typ: t, payload: payload}, nil
	case frameFin:
		if len(payload) != 0 {
			return parsedFrame{}, errProtocol("FIN with payload")
		}
		return parsedFrame{typ: t}, nil
	case frameReset, frameStopSending:
		if len(payload) != 4 {
			return parsedFrame{}, errProtocol("RESET/STOP_SENDING payload must be 4 bytes")
		}
		return parsedFrame{typ: t, code: ErrorCode(binary.BigEndian.Uint32(payload))}, nil
	case frameMaxStreamData:
		if len(payload) != 8 {
			return parsedFrame{}, errProtocol("MAX_STREAM_DATA payload must be 8 bytes")
		}
		v := binary.BigEndian.Uint64(payload)
		if v > maxOffset {
			return parsedFrame{}, errProtocol("MAX_STREAM_DATA above MAX_OFFSET")
		}
		return parsedFrame{typ: t, credit: v}, nil
	default:
		return parsedFrame{}, errProtocol("unknown frame type 0x%02x", byte(t))
	}
}

// --- reserved control channel messages (SPEC §8, wire version 1) ---
//
//	0x00 CONNECTION_CLOSE  uint32 code                          5 bytes
//	0x01 HELLO             uint8 version + 3× uint64 credits   26 bytes
//	0x02 MAX_DATA          uint64 absolute limit                9 bytes
//	0x03 MAX_STREAMS       uint64 absolute limit                9 bytes

const (
	ctrlConnClose  byte = 0x00
	ctrlHello      byte = 0x01
	ctrlMaxData    byte = 0x02
	ctrlMaxStreams byte = 0x03
)

const wireVersion = 1

func encodeConnClose(code ErrorCode) []byte {
	out := make([]byte, 5)
	out[0] = ctrlConnClose
	binary.BigEndian.PutUint32(out[1:], uint32(code))
	return out
}

func encodeHello(l flowLimits) []byte {
	out := make([]byte, 26)
	out[0] = ctrlHello
	out[1] = wireVersion
	binary.BigEndian.PutUint64(out[2:], l.maxStreamData)
	binary.BigEndian.PutUint64(out[10:], l.maxData)
	binary.BigEndian.PutUint64(out[18:], l.maxStreams)
	return out
}

func encodeMaxData(v uint64) []byte {
	out := make([]byte, 9)
	out[0] = ctrlMaxData
	binary.BigEndian.PutUint64(out[1:], v)
	return out
}

func encodeMaxStreams(v uint64) []byte {
	out := make([]byte, 9)
	out[0] = ctrlMaxStreams
	binary.BigEndian.PutUint64(out[1:], v)
	return out
}

type controlMsg struct {
	typ     byte
	code    ErrorCode  // CONNECTION_CLOSE
	version byte       // HELLO
	limits  flowLimits // HELLO
	value   uint64     // MAX_DATA / MAX_STREAMS
}

func decodeControl(data []byte) (controlMsg, error) {
	if len(data) == 0 {
		return controlMsg{}, errProtocol("empty control message")
	}
	switch data[0] {
	case ctrlConnClose:
		if len(data) != 5 {
			return controlMsg{}, errProtocol("CONNECTION_CLOSE must be 5 bytes")
		}
		return controlMsg{typ: ctrlConnClose, code: ErrorCode(binary.BigEndian.Uint32(data[1:]))}, nil
	case ctrlHello:
		if len(data) != 26 {
			return controlMsg{}, errProtocol("HELLO must be 26 bytes")
		}
		l := flowLimits{
			maxStreamData: binary.BigEndian.Uint64(data[2:]),
			maxData:       binary.BigEndian.Uint64(data[10:]),
			maxStreams:    binary.BigEndian.Uint64(data[18:]),
		}
		if l.maxStreamData > maxOffset || l.maxData > maxOffset || l.maxStreams > maxOffset {
			return controlMsg{}, errProtocol("HELLO credit above MAX_OFFSET")
		}
		return controlMsg{typ: ctrlHello, version: data[1], limits: l}, nil
	case ctrlMaxData, ctrlMaxStreams:
		if len(data) != 9 {
			return controlMsg{}, errProtocol("MAX_DATA/MAX_STREAMS must be 9 bytes")
		}
		v := binary.BigEndian.Uint64(data[1:])
		if v > maxOffset {
			return controlMsg{}, errProtocol("credit above MAX_OFFSET")
		}
		return controlMsg{typ: data[0], value: v}, nil
	default:
		return controlMsg{}, errProtocol("unknown control message type 0x%02x", data[0])
	}
}
