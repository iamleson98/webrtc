// pion-interop: cross-implementation data-channel echo, the pion (Go) half of the
// harness that proves pion and webrtc-rs interoperate over real sockets.
//
// Two modes, one binary — mirroring examples/pion-interop in the webrtc-rs repo:
//
//	go run . --mode server              # HTTP signaling at :9090, echoes data channels
//	go run . --mode client --url http://127.0.0.1:9090
//
// `run.sh` (one directory up) drives both cross directions.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/pion/webrtc/v4"
)

const msgPrefix = "interop"

func main() {
	mode := flag.String("mode", "", "server | client")
	httpPort := flag.Int("http_port", 9090, "server: signaling listen port")
	url := flag.String("url", "http://127.0.0.1:9090", "client: signaling endpoint")
	messages := flag.Int("messages", 100, "client: messages to send and expect echoes for")
	flag.Parse()

	switch *mode {
	case "server":
		serverMode(*httpPort)
	case "client":
		if err := clientMode(*url, *messages); err != nil {
			fmt.Fprintln(os.Stderr, "fatal:", err)
			os.Exit(1)
		}
	default:
		fmt.Fprintln(os.Stderr, "fatal: --mode must be server or client")
		os.Exit(1)
	}
}

func serverMode(port int) {
	http.HandleFunc("/doSignaling", func(w http.ResponseWriter, r *http.Request) {
		var offer webrtc.SessionDescription
		if err := json.NewDecoder(r.Body).Decode(&offer); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		pc, err := webrtc.NewPeerConnection(webrtc.Configuration{})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		pc.OnDataChannel(func(d *webrtc.DataChannel) {
			d.OnMessage(func(m webrtc.DataChannelMessage) {
				_ = d.Send(m.Data) // echo verbatim
			})
		})

		if err := pc.SetRemoteDescription(offer); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		answer, err := pc.CreateAnswer(nil)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		gatherComplete := webrtc.GatheringCompletePromise(pc)
		if err := pc.SetLocalDescription(answer); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		<-gatherComplete // non-trickle: candidates ride in the one response

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(*pc.LocalDescription())
	})

	addr := fmt.Sprintf(":%d", port)
	fmt.Printf("interop pion server: POST /doSignaling on http://%s\n", addr)
	panic(http.ListenAndServe(addr, nil))
}

func clientMode(baseURL string, messages int) error {
	pc, err := webrtc.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		return err
	}

	dc, err := pc.CreateDataChannel("interop", nil)
	if err != nil {
		return err
	}

	type result struct {
		payload string
		ok      bool
	}
	echoes := make(chan result, messages)

	open := make(chan struct{})
	dc.OnOpen(func() { close(open) })
	dc.OnMessage(func(m webrtc.DataChannelMessage) {
		echoes <- result{payload: string(m.Data), ok: true}
	})

	offer, err := pc.CreateOffer(nil)
	if err != nil {
		return err
	}
	gatherComplete := webrtc.GatheringCompletePromise(pc)
	if err := pc.SetLocalDescription(offer); err != nil {
		return err
	}
	<-gatherComplete

	body, err := json.Marshal(*pc.LocalDescription())
	if err != nil {
		return err
	}
	resp, err := http.Post(baseURL+"/doSignaling", "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("signaling failed: %s: %s", resp.Status, string(b))
	}

	var answer webrtc.SessionDescription
	if err := json.NewDecoder(resp.Body).Decode(&answer); err != nil {
		return err
	}
	if err := pc.SetRemoteDescription(answer); err != nil {
		return err
	}

	select {
	case <-open:
	case <-time.After(15 * time.Second):
		return fmt.Errorf("data channel never opened")
	}

	for i := 0; i < messages; i++ {
		if err := dc.SendText(fmt.Sprintf("%s-%d", msgPrefix, i)); err != nil {
			return err
		}
	}

	echoed, mismatched := 0, 0
	for i := 0; i < messages; i++ {
		select {
		case r := <-echoes:
			if r.payload == fmt.Sprintf("%s-%d", msgPrefix, i) {
				echoed++
			} else {
				mismatched++
			}
		case <-time.After(15 * time.Second):
			fmt.Printf("PION_CLIENT result: echoed=%d/%d mismatched=%d status=FAIL\n", echoed, messages, mismatched)
			return fmt.Errorf("echo %d never arrived", i)
		}
	}

	status := "PASS"
	if echoed != messages || mismatched != 0 {
		status = "FAIL"
	}
	fmt.Printf("PION_CLIENT result: echoed=%d/%d mismatched=%d status=%s\n", echoed, messages, mismatched, status)
	if status == "FAIL" {
		os.Exit(1)
	}
	return nil
}
