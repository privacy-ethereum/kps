package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"syscall"

	kps "github.com/privacy-ethereum/kps/libs/go"
)

func main() {
	listenFlag := flag.String("listen", ":0", "host:port to bind UDP socket")
	keyFlag := flag.String("key", "kps.key", "path to persistent server key (created if absent)")
	ipFlag := flag.String("ip", "", "ip to advertise in printed address (default: auto)")
	flag.Parse()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	listener, err := kps.Listen(ctx, *listenFlag, kps.Options{KeyFile: *keyFlag})
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	defer listener.Close()

	go func() {
		for {
			conn, err := listener.Accept(ctx)
			if err != nil {
				return
			}
			go handleConn(ctx, conn)
		}
	}()

	fmt.Printf("listening; dial with kps.dial(\"%s\")\n", listener.Address(*ipFlag))

	<-ctx.Done()
}

// handleConn echoes every stream the peer opens: it copies the stream's bytes
// straight back until the peer finishes its write half (EOF).
func handleConn(ctx context.Context, conn kps.Conn) {
	for {
		s, err := conn.AcceptStream(ctx)
		if err != nil {
			return
		}
		go func() {
			log.Printf("[echo] new stream")
			if _, err := io.Copy(s, s); err != nil {
				log.Printf("[echo] copy: %v", err)
			}
			_ = s.CloseWrite()
			_ = s.Close()
		}()
	}
}
