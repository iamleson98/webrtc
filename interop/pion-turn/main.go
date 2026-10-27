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
	"io"
	"net"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/pion/turn/v5"
)

func main() {
	mode := flag.String("mode", "", "server, client or tcp-client")
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
	case "tcp-client":
		os.Exit(runTCPClient(*addr, *realm, name, pass, *count))
	default:
		fmt.Fprintln(os.Stderr, "--mode wants server, client or tcp-client")
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

// runTCPClient exercises RFC 6062 against a TURN server over TCP: it allocates a TCP relay, dials a
// local echo peer through it (Connect, then ConnectionBind on a second connection to the server) and
// checks the echoes; then the peer connects in to the relayed address, which the server announces
// with a ConnectionAttempt, and the two exchange a message the other way.
func runTCPClient(addr, realm, user, pass string, count int) int {
	fail := func(format string, args ...any) int {
		fmt.Printf("PION_TCP_CLIENT result: status=FAIL error=%s\n", fmt.Sprintf(format, args...))
		return 1
	}

	control, err := net.Dial("tcp", addr)
	if err != nil {
		return fail("dial %s: %v", addr, err)
	}
	client, err := turn.NewClient(&turn.ClientConfig{
		STUNServerAddr: addr,
		TURNServerAddr: addr,
		Conn:           turn.NewSTUNConn(control),
		Username:       user,
		Password:       pass,
		Realm:          realm,
	})
	if err != nil {
		return fail("client: %v", err)
	}
	defer client.Close()
	if err := client.Listen(); err != nil {
		return fail("listen: %v", err)
	}
	allocation, err := client.AllocateTCP()
	if err != nil {
		return fail("allocate tcp: %v", err)
	}
	defer allocation.Close()

	// A peer that echoes every connection's bytes back.
	peer, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		return fail("peer: %v", err)
	}
	defer peer.Close()
	go func() {
		for {
			c, err := peer.Accept()
			if err != nil {
				return
			}
			go func() { defer c.Close(); _, _ = io.Copy(c, c) }()
		}
	}()

	// Outbound: the server connects to the peer on the client's behalf.
	out, err := allocation.Dial("tcp", peer.Addr().String())
	if err != nil {
		return fail("dial peer through the relay: %v", err)
	}
	defer out.Close()
	echoed, buf := 0, make([]byte, 64)
	for i := 0; i < count; i++ {
		payload := fmt.Sprintf("ping-%d", i)
		_ = out.SetDeadline(time.Now().Add(5 * time.Second))
		if _, err := out.Write([]byte(payload)); err != nil {
			return fail("write %d: %v", i, err)
		}
		got := make([]byte, 0, len(payload))
		for len(got) < len(payload) {
			n, err := out.Read(buf)
			if err != nil {
				return fail("read %d: %v", i, err)
			}
			got = append(got, buf[:n]...)
		}
		if string(got) != payload {
			return fail("echo %d: got %q want %q", i, got, payload)
		}
		echoed++
	}

	// Inbound: the peer connects to the relayed address. Dialing it above permitted its address.
	accepted := make(chan net.Conn, 1)
	go func() {
		c, err := allocation.Accept()
		if err != nil {
			accepted <- nil
			return
		}
		accepted <- c
	}()
	inbound, err := net.DialTimeout("tcp4", allocation.Addr().String(), 5*time.Second)
	if err != nil {
		return fail("peer dials the relay: %v", err)
	}
	defer inbound.Close()
	var in net.Conn
	select {
	case in = <-accepted:
	case <-time.After(10 * time.Second):
		return fail("no ConnectionAttempt for the peer's connection")
	}
	if in == nil {
		return fail("accepting the peer's connection failed")
	}
	defer in.Close()
	_ = in.SetDeadline(time.Now().Add(5 * time.Second))
	_ = inbound.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := inbound.Write([]byte("hello-from-peer")); err != nil {
		return fail("peer write: %v", err)
	}
	got := make([]byte, len("hello-from-peer"))
	if _, err := io.ReadFull(in, got); err != nil || string(got) != "hello-from-peer" {
		return fail("client read of the peer's bytes: %q %v", got, err)
	}
	if _, err := in.Write([]byte("hello-from-client")); err != nil {
		return fail("client write: %v", err)
	}
	back := make([]byte, len("hello-from-client"))
	if _, err := io.ReadFull(inbound, back); err != nil || string(back) != "hello-from-client" {
		return fail("peer read of the client's bytes: %q %v", back, err)
	}
	fmt.Printf("PION_TCP_CLIENT result: echoed=%d/%d inbound=ok status=PASS\n", echoed, count)
	return 0
}
