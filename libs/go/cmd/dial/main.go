// Command dial is a minimal KPS echo client used by the JS integration tests to
// exercise cross-implementation paths: it dials a server over the chosen
// transport, opens one stream, writes a message, half-closes, reads the echo
// back, and exits 0 iff the echo matches (non-zero otherwise). The echoed text
// is printed to stdout so the caller can assert on it too.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	kps "github.com/privacy-ethereum/kps/libs/go"
)

func main() {
	addr := flag.String("addr", "", "server address ip:port:certhash")
	transport := flag.String("transport", "quic", "transport: quic | webrtc")
	message := flag.String("message", "hello-kps", "message to echo")
	timeout := flag.Duration("timeout", 15*time.Second, "dial+echo timeout")
	closeCode := flag.Uint("closecode", 0, "if >0, close the connection with this error code (CloseWithError)")
	datagram := flag.Bool("datagram", false, "echo via a datagram round-trip instead of a stream")
	flag.Parse()

	if *addr == "" {
		fmt.Fprintln(os.Stderr, "dial: -addr is required")
		os.Exit(2)
	}

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	dial := kps.Dial
	switch *transport {
	case "quic":
		dial = kps.Dial
	case "webrtc":
		dial = kps.DialWebRTC
	default:
		fmt.Fprintf(os.Stderr, "dial: unknown transport %q\n", *transport)
		os.Exit(2)
	}

	conn, err := dial(ctx, *addr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "dial: %v\n", err)
		os.Exit(1)
	}
	if *closeCode > 0 {
		defer conn.CloseWithError(kps.ErrorCode(*closeCode))
	} else {
		defer conn.Close()
	}

	// Datagram mode: send + read the echo back. Datagrams are unreliable, so
	// retry the send until the echo arrives or the deadline passes.
	if *datagram {
		msg := []byte(*message)
		deadline := time.Now().Add(*timeout)
		for time.Now().Before(deadline) {
			// The datagram channel opens shortly after connect; a send may fail
			// with "not open" until then. Treat send errors as transient.
			if err := conn.SendDatagram(msg); err != nil {
				time.Sleep(50 * time.Millisecond)
				continue
			}
			rctx, rcancel := context.WithTimeout(ctx, 300*time.Millisecond)
			got, rerr := conn.ReceiveDatagram(rctx)
			rcancel()
			if rerr == nil {
				fmt.Print(string(got))
				if string(got) != *message {
					fmt.Fprintf(os.Stderr, "\ndatagram echo mismatch: sent %q got %q\n", *message, string(got))
					os.Exit(1)
				}
				return
			}
		}
		fmt.Fprintln(os.Stderr, "datagram: no echo within timeout")
		os.Exit(1)
	}

	s, err := conn.OpenStream(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open stream: %v\n", err)
		os.Exit(1)
	}

	if _, err := s.Write([]byte(*message)); err != nil {
		fmt.Fprintf(os.Stderr, "write: %v\n", err)
		os.Exit(1)
	}
	// Finish our write half so the echo server's io.Copy sees EOF and mirrors
	// the bytes back, then closes its own write half.
	if err := s.CloseWrite(); err != nil {
		fmt.Fprintf(os.Stderr, "close write: %v\n", err)
		os.Exit(1)
	}

	echoed, err := io.ReadAll(s)
	if err != nil {
		fmt.Fprintf(os.Stderr, "read: %v\n", err)
		os.Exit(1)
	}
	_ = s.Close()

	fmt.Print(string(echoed))
	if string(echoed) != *message {
		fmt.Fprintf(os.Stderr, "\ndial: echo mismatch: sent %q got %q\n", *message, string(echoed))
		os.Exit(1)
	}
}
