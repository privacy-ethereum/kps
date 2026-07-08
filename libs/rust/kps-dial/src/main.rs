// Command kps-dial is a minimal KPS echo client used by the integration tests
// to exercise cross-implementation paths: it dials a server over the chosen
// transport, opens one stream, writes a message, half-closes, reads the echo
// back, and exits 0 iff the echo matches (non-zero otherwise). The echoed text
// is printed to stdout so the caller can assert on it too. Mirrors
// libs/go/cmd/dial.

use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::timeout;

struct Flags {
    addr: String,
    transport: String,
    message: String,
    timeout: Duration,
    close_code: u32,
    datagram: bool,
}

fn parse_flags() -> Flags {
    let mut flags = Flags {
        addr: String::new(),
        transport: "quic".into(),
        message: "hello-kps".into(),
        timeout: Duration::from_secs(15),
        close_code: 0,
        datagram: false,
    };
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut i = 0;
    while i < args.len() {
        let (name, inline) = match args[i].trim_start_matches('-').split_once('=') {
            Some((n, v)) => (n.to_string(), Some(v.to_string())),
            None => (args[i].trim_start_matches('-').to_string(), None),
        };
        let mut take = |i: &mut usize| -> String {
            if let Some(v) = &inline {
                v.clone()
            } else {
                *i += 1;
                args.get(*i).cloned().unwrap_or_else(|| {
                    eprintln!("dial: -{name} requires a value");
                    std::process::exit(2);
                })
            }
        };
        match name.as_str() {
            "addr" => flags.addr = take(&mut i),
            "transport" => flags.transport = take(&mut i),
            "message" => flags.message = take(&mut i),
            "timeout" => {
                let v = take(&mut i);
                let secs: f64 = v.trim_end_matches('s').parse().unwrap_or(15.0);
                flags.timeout = Duration::from_secs_f64(secs);
            }
            "closecode" => flags.close_code = take(&mut i).parse().unwrap_or(0),
            "datagram" => {
                if let Some(v) = &inline {
                    flags.datagram = v == "true";
                } else {
                    flags.datagram = true;
                }
            }
            other => {
                eprintln!("dial: unknown flag -{other}");
                std::process::exit(2);
            }
        }
        i += 1;
    }
    flags
}

fn code_from_u32(v: u32) -> kps::ErrorCode {
    kps::ErrorCode::from_wire(v)
}

#[tokio::main]
async fn main() {
    let flags = parse_flags();
    if flags.addr.is_empty() {
        eprintln!("dial: -addr is required");
        std::process::exit(2);
    }

    let dial_result = match flags.transport.as_str() {
        "quic" => timeout(flags.timeout, kps::dial(&flags.addr)).await,
        "webrtc" => timeout(flags.timeout, kps::dial_webrtc(&flags.addr)).await,
        other => {
            eprintln!("dial: unknown transport {other:?}");
            std::process::exit(2);
        }
    };
    let conn = match dial_result {
        Ok(Ok(c)) => c,
        Ok(Err(e)) => {
            eprintln!("dial: {e}");
            std::process::exit(1);
        }
        Err(_) => {
            eprintln!("dial: timed out");
            std::process::exit(1);
        }
    };

    // Datagram mode: send + read the echo back. Datagrams are unreliable, so
    // retry the send until the echo arrives or the deadline passes.
    if flags.datagram {
        let deadline = tokio::time::Instant::now() + flags.timeout;
        while tokio::time::Instant::now() < deadline {
            // The datagram channel opens shortly after connect; a send may fail
            // until then. Treat send errors as transient.
            if conn.send_datagram(flags.message.as_bytes()).await.is_err() {
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue;
            }
            match timeout(Duration::from_millis(300), conn.receive_datagram()).await {
                Ok(Ok(got)) => {
                    print!("{}", String::from_utf8_lossy(&got));
                    if got != flags.message.as_bytes() {
                        eprintln!("\ndatagram echo mismatch");
                        std::process::exit(1);
                    }
                    finish(&*conn, flags.close_code).await;
                    return;
                }
                _ => continue,
            }
        }
        eprintln!("datagram: no echo within timeout");
        std::process::exit(1);
    }

    let mut stream = match timeout(flags.timeout, conn.open_stream()).await {
        Ok(Ok(s)) => s,
        Ok(Err(e)) => {
            eprintln!("open stream: {e}");
            std::process::exit(1);
        }
        Err(_) => {
            eprintln!("open stream: timed out");
            std::process::exit(1);
        }
    };

    if let Err(e) = stream.write_all(flags.message.as_bytes()).await {
        eprintln!("write: {e}");
        std::process::exit(1);
    }
    // Finish our write half so the echo server sees EOF, mirrors the bytes
    // back, and closes its own write half.
    if let Err(e) = stream.close_write().await {
        eprintln!("close write: {e}");
        std::process::exit(1);
    }

    let mut echoed = Vec::new();
    match timeout(flags.timeout, stream.read_to_end(&mut echoed)).await {
        Ok(Ok(_)) => {}
        Ok(Err(e)) => {
            eprintln!("read: {e}");
            std::process::exit(1);
        }
        Err(_) => {
            eprintln!("read: timed out");
            std::process::exit(1);
        }
    }
    let _ = stream.close().await;

    print!("{}", String::from_utf8_lossy(&echoed));
    if echoed != flags.message.as_bytes() {
        eprintln!("\ndial: echo mismatch");
        std::process::exit(1);
    }
    finish(&*conn, flags.close_code).await;
}

async fn finish(conn: &dyn kps::Conn, close_code: u32) {
    if close_code > 0 {
        let _ = conn.close_with_error(code_from_u32(close_code)).await;
        // Give the best-effort CONNECTION_CLOSE a moment to flush.
        tokio::time::sleep(Duration::from_millis(100)).await;
    } else {
        let _ = conn.close().await;
    }
}
