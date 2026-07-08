//! Internal stream framing (SPEC §6.2). Each WebRTC data-channel message is
//! exactly one frame: a 1-byte type, then a type-specific payload. This makes a
//! reliable+ordered, message-oriented SCTP data channel present as a byte
//! stream with QUIC-like lifecycle (graceful FIN, write reset, read stop). The
//! framing is internal to KPS — applications never see it.

use crate::error::ErrorCode;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FrameType {
    Data = 0x00,        // payload = stream bytes
    Fin = 0x01,         // no payload — local write half finished
    Reset = 0x02,       // payload = u32 code — write half aborted
    StopSending = 0x03, // payload = u32 code — peer cancelled its read
}

impl FrameType {
    pub(crate) fn from_byte(b: u8) -> Option<Self> {
        match b {
            0x00 => Some(Self::Data),
            0x01 => Some(Self::Fin),
            0x02 => Some(Self::Reset),
            0x03 => Some(Self::StopSending),
            _ => None,
        }
    }
}

/// Bounds the stream bytes carried in a single DATA frame so we stay well
/// under the negotiated SCTP max-message-size; larger writes are split.
pub(crate) const MAX_FRAME_PAYLOAD: usize = 16 << 10; // 16 KiB

pub(crate) fn encode_data(p: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(1 + p.len());
    out.push(FrameType::Data as u8);
    out.extend_from_slice(p);
    out
}

pub(crate) fn encode_fin() -> Vec<u8> {
    vec![FrameType::Fin as u8]
}

pub(crate) fn encode_code(t: FrameType, code: ErrorCode) -> Vec<u8> {
    let mut out = Vec::with_capacity(5);
    out.push(t as u8);
    out.extend_from_slice(&code.to_wire().to_be_bytes());
    out
}

/// Reads the u32 error code from a RESET/STOP_SENDING payload, defaulting to
/// `None` when absent or short.
pub(crate) fn decode_code(payload: &[u8]) -> ErrorCode {
    if payload.len() < 4 {
        return ErrorCode::None;
    }
    ErrorCode::from_wire(u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]))
}

/// Builds the WebRTC CONNECTION_CLOSE control message carried on the control
/// channel (SPEC §8): a bare big-endian u32 application error code, the WebRTC
/// analogue of QUIC CONNECTION_CLOSE. Decode with `decode_code`.
pub(crate) fn encode_conn_close(code: ErrorCode) -> Vec<u8> {
    code.to_wire().to_be_bytes().to_vec()
}

#[cfg(test)]
mod tests {
    use super::*;

    // Mirrors libs/go framing_test.go: round-trips and defaults.
    #[test]
    fn code_round_trip() {
        for code in [
            ErrorCode::None,
            ErrorCode::Cancelled,
            ErrorCode::Closed,
            ErrorCode::Reset,
            ErrorCode::Timeout,
            ErrorCode::NetworkError,
            ErrorCode::ProtocolError,
            ErrorCode::Unsupported,
            ErrorCode::TooLarge,
            ErrorCode::QueueFull,
            ErrorCode::PermissionDenied,
            ErrorCode::InternalError,
        ] {
            let f = encode_code(FrameType::Reset, code);
            assert_eq!(f[0], FrameType::Reset as u8);
            assert_eq!(decode_code(&f[1..]), code);
        }
    }

    #[test]
    fn registry_wire_values() {
        // SPEC §9.1: values are load-bearing wire constants.
        assert_eq!(ErrorCode::Cancelled.to_wire(), 1);
        assert_eq!(ErrorCode::Closed.to_wire(), 2);
        assert_eq!(ErrorCode::Reset.to_wire(), 3);
        assert_eq!(ErrorCode::Timeout.to_wire(), 4);
        assert_eq!(ErrorCode::NetworkError.to_wire(), 5);
        assert_eq!(ErrorCode::ProtocolError.to_wire(), 6);
        assert_eq!(ErrorCode::Unsupported.to_wire(), 7);
        assert_eq!(ErrorCode::TooLarge.to_wire(), 8);
        assert_eq!(ErrorCode::QueueFull.to_wire(), 9);
        assert_eq!(ErrorCode::PermissionDenied.to_wire(), 10);
        assert_eq!(ErrorCode::InternalError.to_wire(), 11);
    }

    #[test]
    fn unknown_code_sinks_to_internal_error() {
        assert_eq!(ErrorCode::from_wire(9999), ErrorCode::InternalError);
    }

    #[test]
    fn short_payload_is_none() {
        assert_eq!(decode_code(&[]), ErrorCode::None);
        assert_eq!(decode_code(&[0, 0, 1]), ErrorCode::None);
    }

    #[test]
    fn data_and_fin_shapes() {
        assert_eq!(encode_data(b"abc"), vec![0x00, b'a', b'b', b'c']);
        assert_eq!(encode_fin(), vec![0x01]);
        assert_eq!(encode_conn_close(ErrorCode::ProtocolError), vec![0, 0, 0, 6]);
    }
}
