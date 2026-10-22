// Interop harness for the TURN server and client in rtc-turn, against pion's.
//
//	pion-turn --mode server --addr 127.0.0.1:34790 --user user=pass
//	pion-turn --mode client --addr 127.0.0.1:34790 --user user=pass
//
// The client allocates a relay, sends datagrams to a local echo peer through it and checks the
// echoes come back through it too, then prints "status=PASS".
package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/pion/turn/v5"
)

func main() {
	mode := flag.String("mode", "", "server or client")
	addr := flag.String("addr", "127.0.0.1:3478", "the TURN server's address")
	user := flag.String("user", "user=pass", "credentials, user=pass")
	realm := flag.String("realm", "webrtc.rs", "authentication realm")
	count := flag.Int("count", 100, "datagrams the client sends")
	flag.Parse()

	name, pass, ok := strings.Cut(*user, "=")
	if !ok {
		fmt.Fprintln(os.Stderr, "--user wants user=pass")
		os.Exit(2)
	}
	switch *mode {
	case "server":
		runServer(*addr, *realm, name, pass)
	case "client":
		os.Exit(runClient(*addr, *realm, name, pass, *count))
	default:
		fmt.Fprintln(os.Stderr, "--mode wants server or client")
		os.Exit(2)
	}
}

func runServer(addr, realm, user, pass string) {
	conn, err := net.ListenPacket("udp4", addr)
	if err != nil {
		fmt.Fprintln(os.Stderr, "listen:", err)
		os.Exit(1)
	}
	host, _, _ := net.SplitHostPort(addr)
	key := turn.GenerateAuthKey(user, realm, pass)
	server, err := turn.NewServer(turn.ServerConfig{
		Realm: realm,
		AuthHandler: func(ra *turn.RequestAttributes) (string, []byte, bool) {
			if ra.Username == user {
				return user, key, true
			}
			return "", nil, false
		},
		PacketConnConfigs: []turn.PacketConnConfig{{
			PacketConn: conn,
			RelayAddressGenerator: &turn.RelayAddressGeneratorStatic{
				RelayAddress: net.ParseIP(host),
				Address:      host,
			},
		}},
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "server:", err)
		os.Exit(1)
	}
	fmt.Println("pion TURN server listening on", addr)
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	<-sig
	_ = server.Close()
}

func runClient(addr, realm, user, pass string, count int) int {
	conn, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		fmt.Fprintln(os.Stderr, "listen:", err)
		return 1
	}
	defer conn.Close()

	client, err := turn.NewClient(&turn.ClientConfig{
		STUNServerAddr: addr,
		TURNServerAddr: addr,
		Conn:           conn,
		Username:       user,
		Password:       pass,
		Realm:          realm,
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "client:", err)
		return 1
	}
	defer client.Close()
	if err := client.Listen(); err != nil {
		fmt.Fprintln(os.Stderr, "listen:", err)
		return 1
	}
	relay, err := client.Allocate()
	if err != nil {
		fmt.Printf("PION_CLIENT result: echoed=0/%d mismatched=0 status=FAIL error=%v\n", count, err)
		return 1
	}
	defer relay.Close()

	// A peer that echoes whatever the relay forwards to it.
	peer, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		fmt.Fprintln(os.Stderr, "peer:", err)
		return 1
	}
	defer peer.Close()
	go func() {
		buf := make([]byte, 2048)
		for {
			n, from, err := peer.ReadFrom(buf)
			if err != nil {
				return
			}
			_, _ = peer.WriteTo(buf[:n], from)
		}
	}()

	echoed, mismatched := 0, 0
	buf := make([]byte, 2048)
	for i := 0; i < count; i++ {
		payload := fmt.Sprintf("ping-%d", i)
		if _, err := relay.WriteTo([]byte(payload), peer.LocalAddr()); err != nil {
			fmt.Printf("PION_CLIENT result: echoed=%d/%d mismatched=%d status=FAIL error=%v\n", echoed, count, mismatched, err)
			return 1
		}
		_ = relay.SetReadDeadline(time.Now().Add(5 * time.Second))
		n, from, err := relay.ReadFrom(buf)
		if err != nil {
			fmt.Printf("PION_CLIENT result: echoed=%d/%d mismatched=%d status=FAIL error=%v\n", echoed, count, mismatched, err)
			return 1
		}
		if from.String() == peer.LocalAddr().String() && string(buf[:n]) == payload {
			echoed++
		} else {
			mismatched++
		}
	}
	status := "PASS"
	if echoed != count || mismatched != 0 {
		status = "FAIL"
	}
	fmt.Printf("PION_CLIENT result: echoed=%d/%d mismatched=%d status=%s\n", echoed, count, mismatched, status)
	if status != "PASS" {
		return 1
	}
	return 0
}
