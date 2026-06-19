// kps-demo-server — chat + eth-rpc proxy over kps streams.
//
// Two stream protocols:
//   "chat"     — line-delimited JSON: hello / bulletin / dm / ack / dm-fail / roster
//   "eth-rpc"  — line-delimited JSON: { network, req: <jsonrpc-2.0> } -> upstream response
//
// Identity is app-level: each client generates an Ed25519 keypair and sends
// its raw public key in `hello`. The server uses base64(idPublicKey) as the
// peerId for routing; other peers verify dmSignature against it.

package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"sync"
	"syscall"
	"time"

	kps "github.com/voltrevo/kps/libs/go"
)

const (
	rpcMaxBufBytes        = 1 << 20
	rpcUpstreamTimeout    = 3 * time.Second
)

// Curated free public JSON-RPC endpoints, mirrored from
// https://github.com/voltrevo/keynet/blob/main/src/meta-rpc-server.ts
var rpcUpstreams = map[string][]string{
	"ethereum": {
		"https://ethereum-rpc.publicnode.com",
		"https://eth.drpc.org",
		"https://ethereum.publicnode.com",
		"https://endpoints.omniatech.io/v1/eth/mainnet/public",
		"https://1rpc.io/eth",
	},
	"arbitrum": {
		"https://arb1.arbitrum.io/rpc",
		"https://arbitrum.drpc.org",
		"https://arbitrum-one.public.blastapi.io",
		"https://arbitrum.meowrpc.com",
		"https://arbitrum.public.blockpi.network/v1/rpc/public",
	},
	"optimism": {
		"https://optimism.public.blockpi.network/v1/rpc/public",
		"https://api.zan.top/opt-mainnet",
		"https://optimism-public.nodies.app",
		"https://optimism-rpc.publicnode.com",
		"https://1rpc.io/op",
	},
	"base": {
		"https://1rpc.io/base",
		"https://mainnet.base.org",
		"https://developer-access-mainnet.base.org",
		"https://base-public.nodies.app",
		"https://base.public.blockpi.network/v1/rpc/public",
	},
	"polygon": {
		"https://1rpc.io/matic",
		"https://polygon.drpc.org",
		"https://polygon-public.nodies.app",
		"https://api.zan.top/polygon-mainnet",
		"https://polygon-bor-rpc.publicnode.com",
	},
}

var (
	chainIDToNetwork = map[string]string{"1": "ethereum", "42161": "arbitrum", "10": "optimism", "8453": "base", "137": "polygon"}
	networkAliases   = map[string]string{"eth": "ethereum", "arb": "arbitrum", "op": "optimism", "poly": "polygon", "matic": "polygon"}
)

func resolveNetwork(input string) string {
	if _, ok := rpcUpstreams[input]; ok {
		return input
	}
	if v, ok := networkAliases[input]; ok {
		return v
	}
	if v, ok := chainIDToNetwork[input]; ok {
		return v
	}
	return ""
}

func proxyRPC(network string, req json.RawMessage, ctx string) (json.RawMessage, error) {
	list := rpcUpstreams[network]
	url := list[rand.Intn(len(list))]

	httpCtx, cancel := context.WithTimeout(context.Background(), rpcUpstreamTimeout)
	defer cancel()
	httpReq, err := http.NewRequestWithContext(httpCtx, "POST", url, bytes.NewReader(req))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("content-type", "application/json")
	started := time.Now()
	resp, err := http.DefaultClient.Do(httpReq)
	elapsed := time.Since(started)
	method := peekMethod(req)
	tag := "[rpc]"
	if ctx != "" {
		tag = "[rpc " + ctx + "]"
	}
	if err != nil {
		log.Printf("%s %s %s → %s FAIL (%dms): %v", tag, network, method, url, elapsed.Milliseconds(), err)
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	log.Printf("%s %s %s → %s %d (%dms)", tag, network, method, url, resp.StatusCode, elapsed.Milliseconds())
	if resp.StatusCode/100 != 2 {
		return nil, fmt.Errorf("upstream %s returned %d", url, resp.StatusCode)
	}
	return body, nil
}

func peekMethod(req json.RawMessage) string {
	var p struct {
		Method string `json:"method"`
	}
	_ = json.Unmarshal(req, &p)
	if p.Method == "" {
		return "?"
	}
	return p.Method
}

// ---- chat roster ----

type peerEntry struct {
	stream      *kps.Stream
	idPublicKey string // base64 raw Ed25519 pubkey; also the peerId
	dmPublicKey string // base64 raw P-256 pubkey
	dmSignature string // base64 sig over dm-key-payload
	name        string

	mu sync.Mutex // serialize writes per-peer
}

type rosterMember struct {
	PeerID      string `json:"peerId"`
	IDPublicKey string `json:"idPublicKey"`
	DMPublicKey string `json:"dmPublicKey"`
	DMSignature string `json:"dmSignature"`
	Name        string `json:"name"`
}

var (
	peersMu sync.RWMutex
	peers   = map[string]*peerEntry{}
)

func sanitizeName(n string) string {
	n = trim(n)
	if len(n) > 40 {
		n = n[:40]
	}
	return n
}

func trim(s string) string {
	for len(s) > 0 && (s[0] == ' ' || s[0] == '\t' || s[0] == '\n' || s[0] == '\r') {
		s = s[1:]
	}
	for len(s) > 0 {
		c := s[len(s)-1]
		if c != ' ' && c != '\t' && c != '\n' && c != '\r' {
			break
		}
		s = s[:len(s)-1]
	}
	return s
}

func sendObj(p *peerEntry, obj any) error {
	b, err := json.Marshal(obj)
	if err != nil {
		return err
	}
	b = append(b, '\n')
	p.mu.Lock()
	defer p.mu.Unlock()
	_, err = p.stream.Write(b)
	return err
}

func rosterSnapshot() []rosterMember {
	peersMu.RLock()
	defer peersMu.RUnlock()
	out := make([]rosterMember, 0, len(peers))
	for id, p := range peers {
		if p.dmPublicKey == "" || p.dmSignature == "" {
			continue
		}
		name := p.name
		if name == "" {
			name = shortID(id)
		}
		out = append(out, rosterMember{
			PeerID:      id,
			IDPublicKey: p.idPublicKey,
			DMPublicKey: p.dmPublicKey,
			DMSignature: p.dmSignature,
			Name:        name,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].PeerID < out[j].PeerID })
	return out
}

func logRoster() {
	snap := rosterSnapshot()
	parts := make([]string, 0, len(snap))
	for _, m := range snap {
		parts = append(parts, fmt.Sprintf("%s(%s)", m.Name, shortID(m.PeerID)))
	}
	if len(parts) == 0 {
		log.Printf("[roster] 0 peer(s): (none)")
	} else {
		log.Printf("[roster] %d peer(s): %s", len(parts), join(parts, ", "))
	}
}

func join(ss []string, sep string) string {
	if len(ss) == 0 {
		return ""
	}
	out := ss[0]
	for _, s := range ss[1:] {
		out += sep + s
	}
	return out
}

func shortID(id string) string {
	if len(id) <= 10 {
		return id
	}
	return "…" + id[len(id)-8:]
}

func broadcastRoster() {
	snap := rosterSnapshot()
	msg := map[string]any{"type": "roster", "peers": snap}
	peersMu.RLock()
	targets := make([]*peerEntry, 0, len(peers))
	for _, p := range peers {
		if p.dmPublicKey != "" {
			targets = append(targets, p)
		}
	}
	peersMu.RUnlock()
	for _, p := range targets {
		_ = sendObj(p, msg)
	}
}

func forward(targetPeerID string, msg any) bool {
	peersMu.RLock()
	target, ok := peers[targetPeerID]
	peersMu.RUnlock()
	if !ok || target.dmPublicKey == "" {
		return false
	}
	return sendObj(target, msg) == nil
}

// ---- handlers ----

// handleConn accepts the connection's streams and routes each one. KPS streams
// are unnamed, so the application does its own routing: a stream's first line
// is a protocol selector ("chat" or "eth-rpc"); the rest is that protocol's
// newline-delimited JSON.
func handleConn(conn *kps.Conn) {
	for {
		stream, err := conn.AcceptStream(context.Background())
		if err != nil {
			return
		}
		go routeStream(stream)
	}
}

func routeStream(stream *kps.Stream) {
	if err := stream.WaitOpen(); err != nil {
		return
	}
	r := bufio.NewReader(stream)
	sel, err := r.ReadBytes('\n')
	if err != nil {
		_ = stream.Close()
		return
	}
	switch string(bytes.TrimSpace(sel)) {
	case "chat":
		chatHandler(stream, r)
	case "eth-rpc":
		rpcHandler(stream, r)
	default:
		_ = stream.Close()
	}
}

const dmSigDomain = "kps-webrtc-dm-key-v1:"

func verifyDMSignature(idPubB64, dmPubB64, sigB64 string) bool {
	idPub, err := base64.StdEncoding.DecodeString(idPubB64)
	if err != nil || len(idPub) != ed25519.PublicKeySize {
		return false
	}
	rawDM, err := base64.StdEncoding.DecodeString(dmPubB64)
	if err != nil {
		return false
	}
	sig, err := base64.StdEncoding.DecodeString(sigB64)
	if err != nil {
		return false
	}
	payload := append([]byte(dmSigDomain), rawDM...)
	return ed25519.Verify(ed25519.PublicKey(idPub), payload, sig)
}

func chatHandler(stream *kps.Stream, r *bufio.Reader) {
	var entry *peerEntry
	var peerID string
	defer func() {
		if peerID == "" {
			return
		}
		peersMu.Lock()
		if peers[peerID] == entry {
			delete(peers, peerID)
		}
		peersMu.Unlock()
		log.Printf("[-] peer disconnected: %s", peerID)
		logRoster()
		broadcastRoster()
	}()

	for {
		// Byte stream: read one newline-delimited JSON message at a time.
		buf, err := r.ReadBytes('\n')
		if err != nil {
			return
		}
		lines := bytes.Split(buf, []byte{'\n'})
		for _, line := range lines {
			line = bytes.TrimSpace(line)
			if len(line) == 0 {
				continue
			}
			var raw map[string]json.RawMessage
			if err := json.Unmarshal(line, &raw); err != nil {
				continue
			}
			var typ string
			_ = json.Unmarshal(raw["type"], &typ)

			switch typ {
			case "hello":
				if entry != nil {
					continue
				}
				var msg struct {
					IDPublicKey string `json:"idPublicKey"`
					DMPublicKey string `json:"dmPublicKey"`
					DMSignature string `json:"dmSignature"`
					Name        string `json:"name"`
				}
				if err := json.Unmarshal(line, &msg); err != nil {
					continue
				}
				if msg.IDPublicKey == "" || msg.DMPublicKey == "" || msg.DMSignature == "" {
					continue
				}
				if !verifyDMSignature(msg.IDPublicKey, msg.DMPublicKey, msg.DMSignature) {
					log.Printf("[chat] rejecting hello: bad dm-key signature")
					return
				}
				entry = &peerEntry{
					stream:      stream,
					idPublicKey: msg.IDPublicKey,
					dmPublicKey: msg.DMPublicKey,
					dmSignature: msg.DMSignature,
					name:        sanitizeName(msg.Name),
				}
				peerID = msg.IDPublicKey
				peersMu.Lock()
				if old, ok := peers[peerID]; ok {
					_ = old.stream.Close()
				}
				peers[peerID] = entry
				peersMu.Unlock()
				log.Printf("[+] peer connected: %s", peerID)
				logRoster()
				broadcastRoster()

			case "dm":
				if entry == nil {
					continue
				}
				var msg struct {
					ID         json.RawMessage `json:"id"`
					To         string          `json:"to"`
					IV         string          `json:"iv"`
					Ciphertext string          `json:"ciphertext"`
				}
				if err := json.Unmarshal(line, &msg); err != nil {
					continue
				}
				delivered := forward(msg.To, map[string]any{
					"type":       "dm",
					"from":       peerID,
					"iv":         msg.IV,
					"ciphertext": msg.Ciphertext,
				})
				if delivered {
					_ = sendObj(entry, map[string]any{"type": "ack", "id": msg.ID})
				} else {
					_ = sendObj(entry, map[string]any{"type": "dm-fail", "id": msg.ID, "reason": "peer unreachable"})
				}

			case "bulletin":
				if entry == nil {
					continue
				}
				var msg struct {
					ID   json.RawMessage `json:"id"`
					Text string          `json:"text"`
				}
				if err := json.Unmarshal(line, &msg); err != nil {
					continue
				}
				if msg.Text == "" {
					continue
				}
				if len(msg.Text) > 4000 {
					msg.Text = msg.Text[:4000]
				}
				out := map[string]any{"type": "bulletin", "from": peerID, "text": msg.Text}
				peersMu.RLock()
				targets := make([]*peerEntry, 0, len(peers))
				for id, p := range peers {
					if id != peerID && p.dmPublicKey != "" {
						targets = append(targets, p)
					}
				}
				peersMu.RUnlock()
				for _, t := range targets {
					_ = sendObj(t, out)
				}
				_ = sendObj(entry, map[string]any{"type": "ack", "id": msg.ID})
			}
		}
	}
}

func rpcHandler(stream *kps.Stream, r *bufio.Reader) {
	tag := "?"
	log.Printf("[rpc+] %s", tag)
	defer log.Printf("[rpc-] %s", tag)

	var sendMu sync.Mutex
	send := func(obj any) {
		b, err := json.Marshal(obj)
		if err != nil {
			log.Printf("[rpc] marshal: %v", err)
			return
		}
		b = append(b, '\n')
		sendMu.Lock()
		defer sendMu.Unlock()
		if _, err := stream.Write(b); err != nil {
			log.Printf("[rpc] send (%d bytes): %v", len(b), err)
		}
	}

	var pending sync.WaitGroup
	defer pending.Wait()

	for {
		buf, err := r.ReadBytes('\n')
		if err != nil {
			return
		}
		if len(buf) > rpcMaxBufBytes {
			log.Printf("[rpc] oversized message, closing")
			return
		}
		lines := bytes.Split(buf, []byte{'\n'})
		for _, line := range lines {
			line = bytes.TrimSpace(line)
			if len(line) == 0 {
				continue
			}
			pending.Add(1)
			go func(line []byte) {
				defer pending.Done()
				var env struct {
					Network string          `json:"network"`
					Req     json.RawMessage `json:"req"`
				}
				if err := json.Unmarshal(line, &env); err != nil {
					send(map[string]any{
						"jsonrpc": "2.0", "id": nil,
						"error": map[string]any{"code": -32700, "message": "Parse error"},
					})
					return
				}
				var idHolder struct {
					ID json.RawMessage `json:"id"`
				}
				_ = json.Unmarshal(env.Req, &idHolder)
				network := resolveNetwork(env.Network)
				if network == "" {
					send(map[string]any{
						"jsonrpc": "2.0", "id": idHolder.ID,
						"error": map[string]any{"code": -32603, "message": "proxy error", "data": "unknown network: " + env.Network},
					})
					return
				}
				resp, err := proxyRPC(network, env.Req, tag)
				if err != nil {
					send(map[string]any{
						"jsonrpc": "2.0", "id": idHolder.ID,
						"error": map[string]any{"code": -32603, "message": "proxy error", "data": err.Error()},
					})
					return
				}
				// resp already shaped as a JSON-RPC response; forward verbatim.
				out := append(resp, '\n')
				sendMu.Lock()
				_, err = stream.Write(out)
				sendMu.Unlock()
				if err != nil {
					log.Printf("[rpc] send response (%d bytes, network=%s): %v", len(out), network, err)
				}
			}(line)
		}
	}
}

// ---- state ----

// state is persisted as JSON: the TLS cert + key (combined PEM) and the
// chosen UDP port live together so the printed address stays byte-stable
// across restarts.
type state struct {
	Port int    `json:"port"`
	TLS  string `json:"tls"` // combined PEM (PRIVATE KEY + CERTIFICATE)
}

func loadState(path string) (*state, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	var s state
	if err := json.Unmarshal(b, &s); err != nil {
		return nil, err
	}
	return &s, nil
}

func saveState(path string, s *state) error {
	b, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	return os.WriteFile(path, append(b, '\n'), 0o600)
}

func main() {
	listenFlag := flag.String("listen", "", "host:port to bind UDP socket (default: pick free port; persists in state.json)")
	stateFlag := flag.String("state", "./state.json", "path to persistent state (port + TLS cert)")
	ipFlag := flag.String("ip", "", "ip to advertise in printed address (default: auto)")
	flag.Parse()

	rand.Seed(time.Now().UnixNano())

	saved, err := loadState(*stateFlag)
	if err != nil {
		log.Fatalf("load state: %v", err)
	}

	var identity *kps.Identity
	if saved != nil && saved.TLS != "" {
		identity, err = kps.IdentityFromPEM(saved.TLS)
		if err != nil {
			log.Fatalf("load identity from %s: %v", *stateFlag, err)
		}
	} else {
		identity, err = kps.GenerateIdentity()
		if err != nil {
			log.Fatalf("generate identity: %v", err)
		}
	}

	listenAddr := *listenFlag
	if listenAddr == "" {
		port := 0
		if saved != nil {
			port = saved.Port
		}
		listenAddr = fmt.Sprintf(":%d", port)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	listener, err := kps.Listen(ctx, listenAddr, kps.Options{Identity: identity})
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	defer listener.Close()

	freshCert := saved == nil || saved.TLS == ""
	freshPort := *listenFlag == "" && (saved == nil || saved.Port == 0)
	if freshCert || freshPort {
		next := state{}
		if saved != nil {
			next = *saved
		}
		if freshCert {
			pemStr, err := identity.PEM()
			if err != nil {
				log.Fatalf("serialize identity: %v", err)
			}
			next.TLS = pemStr
		}
		if freshPort {
			next.Port = listener.Port()
		}
		if err := saveState(*stateFlag, &next); err != nil {
			log.Printf("warn: save state: %v", err)
		} else {
			log.Printf("saved state to %s; future starts will reuse this cert%s",
				*stateFlag,
				map[bool]string{true: " and port", false: ""}[freshPort])
		}
	}

	go func() {
		for {
			conn, err := listener.Accept(ctx)
			if err != nil {
				return
			}
			go handleConn(conn)
		}
	}()

	addr := listener.Address(*ipFlag)
	fmt.Printf("listening; dial from the browser:\n  %s\n", addr)

	<-ctx.Done()
}
