package kps

import "encoding/binary"

// Internal stream framing (SPEC §6.2). Each WebRTC data-channel message is
// exactly one frame: a 1-byte type, then a type-specific payload. This makes a
// reliable+ordered, message-oriented SCTP data channel present as a byte stream
// with QUIC-like lifecycle (graceful FIN, write reset, read stop). The framing
// is internal to KPS — applications never see it.
type frameType byte

const (
	frameData        frameType = 0x00 // payload = stream bytes
	frameFin         frameType = 0x01 // no payload — local write half finished
	frameReset       frameType = 0x02 // payload = uint32 code — write half aborted
	frameStopSending frameType = 0x03 // payload = uint32 code — peer cancelled its read
)

// maxFramePayload bounds the stream bytes carried in a single DATA frame so we
// stay well under the negotiated SCTP max-message-size; larger Writes are split.
const maxFramePayload = 16 << 10 // 16 KiB

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

// decodeCode reads the uint32 error code from a RESET/STOP_SENDING payload,
// defaulting to CodeNone when absent or short.
func decodeCode(payload []byte) ErrorCode {
	if len(payload) < 4 {
		return CodeNone
	}
	return ErrorCode(binary.BigEndian.Uint32(payload))
}
