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

	kps "github.com/voltrevo/kps/libs/go"
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

	listener.Handle("echo", func(s *kps.Stream) {
		log.Printf("[echo] new stream")
		for {
			buf, err := s.Recv()
			if err != nil {
				if err != io.EOF {
					log.Printf("[echo] recv: %v", err)
				}
				return
			}
			if err := s.Send(buf); err != nil {
				log.Printf("[echo] send: %v", err)
				return
			}
		}
	})

	fmt.Printf("listening; dial with kps.dial(\"%s\")\n", listener.Address(*ipFlag))

	<-ctx.Done()
}
