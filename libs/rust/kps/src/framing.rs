//! Internal stream framing (SPEC §6.2, wire version 1) and the reserved
//! control channel's typed messages (SPEC §8). Each WebRTC data-channel
//! message is exactly one frame: a 1-byte type, then a type-specific payload.
//! This makes a reliable+ordered, message-oriented SCTP data channel present
//! as a byte stream with QUIC-like lifecycle plus per-stream flow-control
//! credit (§6.5). The framing is internal to KPS — applications never see it.

use crate::error::ErrorCode;
use crate::flow::FlowLimits;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FrameType {
    Data = 0x00,          // payload = stream bytes (1..=MAX_FRAME_PAYLOAD)
    Fin = 0x01,           // no payload — local write half finished
    Reset = 0x02,         // payload = u32 code — write half aborted
    StopSending = 0x03,   // payload = u32 code — peer cancelled its read
    MaxStreamData = 0x04, // payload = u64 absolute credit (§6.5)
}

/// A frame (type byte + payload) never exceeds `MAX_WEBRTC_FRAME_SIZE`: an
/// SCTP user message is reassembled before KPS can inspect it, so credit alone
/// cannot bound one message. DATA therefore carries 1..=MAX_FRAME_PAYLOAD
/// bytes; larger writes are split, empty writes produce no frame.
pub(crate) const MAX_WEBRTC_FRAME_SIZE: usize = 16_384;
pub(crate) const MAX_FRAME_PAYLOAD: usize = MAX_WEBRTC_FRAME_SIZE - 1;

/// Ceiling for every offset, limit and count (QUIC's integer range, §6.5).
pub(crate) const MAX_OFFSET: u64 = (1 << 62) - 1;

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

pub(crate) fn encode_max_stream_data(v: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(9);
    out.push(FrameType::MaxStreamData as u8);
    out.extend_from_slice(&v.to_be_bytes());
    out
}

/// One strictly-validated §6.2 frame.
#[derive(Debug)]
pub(crate) enum ParsedFrame<'a> {
    Data(&'a [u8]),
    Fin,
    Reset(ErrorCode),
    StopSending(ErrorCode),
    MaxStreamData(u64),
}

/// Validates one data-channel message against the wire-version-1 rules:
/// unknown types, wrong payload lengths, empty or oversized DATA, and
/// out-of-range credit are all protocol violations (connection-fatal). The Err
/// string describes the violation.
pub(crate) fn parse_frame(data: &[u8]) -> Result<ParsedFrame<'_>, String> {
    if data.is_empty() {
        return Err("empty data-channel message".into());
    }
    if data.len() > MAX_WEBRTC_FRAME_SIZE {
        return Err(format!("frame exceeds {} bytes ({})", MAX_WEBRTC_FRAME_SIZE, data.len()));
    }
    let payload = &data[1..];
    match data[0] {
        0x00 => {
            if payload.is_empty() {
                return Err("empty DATA frame".into());
            }
            Ok(ParsedFrame::Data(payload))
        }
        0x01 => {
            if !payload.is_empty() {
                return Err("FIN with payload".into());
            }
            Ok(ParsedFrame::Fin)
        }
        t @ (0x02 | 0x03) => {
            if payload.len() != 4 {
                return Err("RESET/STOP_SENDING payload must be 4 bytes".into());
            }
            let code = ErrorCode::from_wire(u32::from_be_bytes(payload.try_into().unwrap()));
            if t == 0x02 {
                Ok(ParsedFrame::Reset(code))
            } else {
                Ok(ParsedFrame::StopSending(code))
            }
        }
        0x04 => {
            if payload.len() != 8 {
                return Err("MAX_STREAM_DATA payload must be 8 bytes".into());
            }
            let v = u64::from_be_bytes(payload.try_into().unwrap());
            if v > MAX_OFFSET {
                return Err("MAX_STREAM_DATA above MAX_OFFSET".into());
            }
            Ok(ParsedFrame::MaxStreamData(v))
        }
        t => Err(format!("unknown frame type 0x{t:02x}")),
    }
}

// --- reserved control channel messages (SPEC §8, wire version 1) ---
//
//   0x00 CONNECTION_CLOSE  u32 code                        5 bytes
//   0x01 HELLO             u8 version + 3× u64 credits    26 bytes
//   0x02 MAX_DATA          u64 absolute limit              9 bytes
//   0x03 MAX_STREAMS       u64 absolute limit              9 bytes

pub(crate) const WIRE_VERSION: u8 = 1;

pub(crate) fn encode_conn_close(code: ErrorCode) -> Vec<u8> {
    let mut out = Vec::with_capacity(5);
    out.push(0x00);
    out.extend_from_slice(&code.to_wire().to_be_bytes());
    out
}

pub(crate) fn encode_hello(l: &FlowLimits) -> Vec<u8> {
    let mut out = Vec::with_capacity(26);
    out.push(0x01);
    out.push(WIRE_VERSION);
    out.extend_from_slice(&l.max_stream_data.to_be_bytes());
    out.extend_from_slice(&l.max_data.to_be_bytes());
    out.extend_from_slice(&l.max_streams.to_be_bytes());
    out
}

pub(crate) fn encode_max_data(v: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(9);
    out.push(0x02);
    out.extend_from_slice(&v.to_be_bytes());
    out
}

pub(crate) fn encode_max_streams(v: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(9);
    out.push(0x03);
    out.extend_from_slice(&v.to_be_bytes());
    out
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ControlMsg {
    Close(ErrorCode),
    Hello { version: u8, limits: FlowLimits },
    MaxData(u64),
    MaxStreams(u64),
}

pub(crate) fn decode_control(data: &[u8]) -> Result<ControlMsg, String> {
    if data.is_empty() {
        return Err("empty control message".into());
    }
    match data[0] {
        0x00 => {
            if data.len() != 5 {
                return Err("CONNECTION_CLOSE must be 5 bytes".into());
            }
            Ok(ControlMsg::Close(ErrorCode::from_wire(u32::from_be_bytes(
                data[1..5].try_into().unwrap(),
            ))))
        }
        0x01 => {
            if data.len() != 26 {
                return Err("HELLO must be 26 bytes".into());
            }
            let limits = FlowLimits {
                max_stream_data: u64::from_be_bytes(data[2..10].try_into().unwrap()),
                max_data: u64::from_be_bytes(data[10..18].try_into().unwrap()),
                max_streams: u64::from_be_bytes(data[18..26].try_into().unwrap()),
            };
            if limits.max_stream_data > MAX_OFFSET
                || limits.max_data > MAX_OFFSET
                || limits.max_streams > MAX_OFFSET
            {
                return Err("HELLO credit above MAX_OFFSET".into());
            }
            Ok(ControlMsg::Hello { version: data[1], limits })
        }
        t @ (0x02 | 0x03) => {
            if data.len() != 9 {
                return Err("MAX_DATA/MAX_STREAMS must be 9 bytes".into());
            }
            let v = u64::from_be_bytes(data[1..9].try_into().unwrap());
            if v > MAX_OFFSET {
                return Err("credit above MAX_OFFSET".into());
            }
            if t == 0x02 {
                Ok(ControlMsg::MaxData(v))
            } else {
                Ok(ControlMsg::MaxStreams(v))
            }
        }
        t => Err(format!("unknown control message type 0x{t:02x}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn data_and_fin_shapes() {
        assert_eq!(encode_data(b"abc"), vec![0x00, b'a', b'b', b'c']);
        assert_eq!(encode_fin(), vec![0x01]);
        assert_eq!(encode_conn_close(ErrorCode::ProtocolError), vec![0x00, 0, 0, 0, 6]);
    }

    #[test]
    fn parse_frame_round_trips() {
        match parse_frame(&encode_data(b"hi")).unwrap() {
            ParsedFrame::Data(p) => assert_eq!(p, b"hi"),
            f => panic!("unexpected {f:?}"),
        }
        assert!(matches!(parse_frame(&encode_fin()).unwrap(), ParsedFrame::Fin));
        match parse_frame(&encode_code(FrameType::Reset, ErrorCode::Reset)).unwrap() {
            ParsedFrame::Reset(c) => assert_eq!(c, ErrorCode::Reset),
            f => panic!("unexpected {f:?}"),
        }
        match parse_frame(&encode_max_stream_data(MAX_OFFSET)).unwrap() {
            ParsedFrame::MaxStreamData(v) => assert_eq!(v, MAX_OFFSET),
            f => panic!("unexpected {f:?}"),
        }
    }

    #[test]
    fn parse_frame_rejects_violations() {
        // Wire-rule violations (SPEC §6.2, wire version 1) are errors.
        for (name, bad) in [
            ("empty message", vec![]),
            ("empty DATA", vec![0x00]),
            ("FIN with payload", vec![0x01, 1]),
            ("short RESET", vec![0x02, 0, 0]),
            ("long STOP_SENDING", vec![0x03, 0, 0, 0, 0, 0]),
            ("short MAX_STREAM_DATA", vec![0x04, 1, 2, 3]),
            ("unknown type", vec![0x05, 1]),
            ("oversized frame", vec![0u8; MAX_WEBRTC_FRAME_SIZE + 1]),
        ] {
            assert!(parse_frame(&bad).is_err(), "{name} should fail");
        }
    }

    #[test]
    fn control_codec() {
        let l = FlowLimits { max_stream_data: 1 << 20, max_data: 8 << 20, max_streams: 100 };
        let hello = encode_hello(&l);
        assert_eq!(hello.len(), 26);
        assert_eq!(
            decode_control(&hello).unwrap(),
            ControlMsg::Hello { version: WIRE_VERSION, limits: l }
        );
        assert_eq!(
            decode_control(&encode_conn_close(ErrorCode::Reset)).unwrap(),
            ControlMsg::Close(ErrorCode::Reset)
        );
        assert_eq!(decode_control(&encode_max_data(42)).unwrap(), ControlMsg::MaxData(42));
        assert_eq!(decode_control(&encode_max_streams(7)).unwrap(), ControlMsg::MaxStreams(7));
        for (name, bad) in [
            ("empty", vec![]),
            ("short close", vec![0x00, 0, 0, 0]),
            ("short hello", vec![0x01, 1, 0]),
            ("unknown type", vec![0x07, 0]),
        ] {
            assert!(decode_control(&bad).is_err(), "{name} should fail");
        }
    }
}
