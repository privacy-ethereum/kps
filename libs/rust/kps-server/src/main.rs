// Command kps-server is the KPS echo server used by the integration tests: it
// binds one UDP port, prints the dialable address, and echoes every stream
// (and datagram) each peer opens. Mirrors libs/go/cmd/server.

use std::sync::Arc;

use tokio::io::AsyncWriteExt;

struct Flags {
    listen: String,
    key: String,
    ip: String,
}

fn parse_flags() -> Flags {
    let mut flags =
        Flags { listen: ":0".into(), key: "kps.key".into(), ip: String::new() };
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut i = 0;
    while i < args.len() {
        let (name, inline) = match args[i].trim_start_matches('-').split_once('=') {
            Some((n, v)) => (n.to_string(), Some(v.to_string())),
            None => (args[i].trim_start_matches('-').to_string(), None),
        };
        let take = |i: &mut usize| -> String {
            if let Some(v) = &inline {
                v.clone()
            } else {
                *i += 1;
                args.get(*i).cloned().unwrap_or_else(|| {
                    eprintln!("server: -{name} requires a value");
                    std::process::exit(2);
                })
            }
        };
        match name.as_str() {
            "listen" => flags.listen = take(&mut i),
            "key" => flags.key = take(&mut i),
            "ip" => flags.ip = take(&mut i),
            other => {
                eprintln!("server: unknown flag -{other}");
                std::process::exit(2);
            }
        }
        i += 1;
    }
    flags
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();
    let flags = parse_flags();

    let listener = kps::listen(
        &flags.listen,
        kps::ListenOptions { key_file: Some(flags.key.into()), ..Default::default() },
    )
    .await?;

    // The same line format as the Go server; harnesses regex the address out.
    println!("listening; dial with kps.dial(\"{}\")", listener.address(&flags.ip));

    loop {
        let conn = match listener.accept().await {
            Ok(c) => c,
            Err(_) => break,
        };
        tokio::spawn(handle_conn(Arc::from(conn)));
    }
    Ok(())
}

/// Echoes every stream the peer opens (copy bytes back until the peer finishes
/// its write half, then finish ours) and echoes datagrams.
async fn handle_conn(conn: Arc<dyn kps::Conn>) {
    let c = conn.clone();
    tokio::spawn(async move {
        while let Ok(d) = c.receive_datagram().await {
            let _ = c.send_datagram(&d).await;
        }
    });
    while let Ok(stream) = conn.accept_stream().await {
        tokio::spawn(async move {
            eprintln!("[echo] new stream");
            let (mut rd, mut wr) = tokio::io::split(stream);
            if let Err(e) = tokio::io::copy(&mut rd, &mut wr).await {
                eprintln!("[echo] copy: {e}");
            }
            let _ = wr.shutdown().await; // close_write: peer sees EOF
        });
    }
}
