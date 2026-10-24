// sfu-load drives the `broadcast` example of either stack (pion/webrtc or webrtc-rs) as an SFU:
// one publisher sends synthetic RTP, N subscribers receive it. The server prints each answer
// (base64 JSON) on stdout, so the generator reads it from the server's log file.
//
//	sfu-load -log server.log -subs 50 -duration 30s -pid <server pid>
//
// It reports packets received per subscriber, loss against what the publisher sent, and the
// server process's CPU time and resident memory over the measured window.
package main

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
)

var (
	logPath  = flag.String("log", "", "server stdout log file")
	url      = flag.String("url", "http://127.0.0.1:8080/sdp", "server signaling URL")
	subs     = flag.Int("subs", 10, "subscribers")
	duration = flag.Duration("duration", 20*time.Second, "measurement window")
	pps      = flag.Int("pps", 120, "publisher packets per second")
	size     = flag.Int("size", 1100, "RTP payload bytes")
	pid      = flag.Int("pid", 0, "server pid to sample")
)

func main() {
	flag.Parse()
	if *logPath == "" {
		panic("-log required")
	}
	pub := newPeer(true)
	offer, _ := pub.CreateOffer(nil)
	gather := webrtc.GatheringCompletePromise(pub)
	must(pub.SetLocalDescription(offer))
	<-gather
	answer := signal(pub.LocalDescription())
	must(pub.SetRemoteDescription(answer))
	track := pubTrack
	connected := make(chan struct{})
	pub.OnConnectionStateChange(func(s webrtc.PeerConnectionState) {
		if s == webrtc.PeerConnectionStateConnected {
			close(connected)
		}
	})
	<-connected
	var sent atomic.Int64
	stop := make(chan struct{})
	go func() {
		t := time.NewTicker(time.Second / time.Duration(*pps))
		defer t.Stop()
		var seq uint16
		var ts uint32
		payload := make([]byte, *size)
		for {
			select {
			case <-stop:
				return
			case <-t.C:
				seq++
				ts += 90000 / uint32(*pps)
				_ = track.WriteRTP(&rtp.Packet{Header: rtp.Header{Version: 2, PayloadType: 96, SequenceNumber: seq, Timestamp: ts, SSRC: 1234}, Payload: payload})
				sent.Add(1)
			}
		}
	}()

	var got []*atomic.Int64
	var mu sync.Mutex
	var wg sync.WaitGroup
	for i := 0; i < *subs; i++ {
		c := new(atomic.Int64)
		got = append(got, c)
		wg.Add(1)
		go func() {
			defer wg.Done()
			pc := newPeer(false)
			pc.OnTrack(func(tr *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
				buf := make([]byte, 1500)
				for {
					if _, _, err := tr.Read(buf); err != nil {
						return
					}
					c.Add(1)
				}
			})
			if _, err := pc.AddTransceiverFromKind(webrtc.RTPCodecTypeVideo, webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionRecvonly}); err != nil {
				panic(err)
			}
			off, _ := pc.CreateOffer(nil)
			g := webrtc.GatheringCompletePromise(pc)
			must(pc.SetLocalDescription(off))
			<-g
			mu.Lock() // the example serves one signaling exchange at a time
			ans := signal(pc.LocalDescription())
			mu.Unlock()
			must(pc.SetRemoteDescription(ans))
		}()
	}
	wg.Wait()
	fmt.Printf("all %d subscribers signaled; letting media flow\n", *subs)
	time.Sleep(5 * time.Second)

	for _, c := range got {
		c.Store(0)
	}
	sent.Store(0)
	cpu0 := cpuSeconds()
	t0 := time.Now()
	time.Sleep(*duration)
	elapsed := time.Since(t0).Seconds()
	cpu1 := cpuSeconds()
	close(stop)

	var total, active int64
	var minGot int64 = 1 << 62
	for _, c := range got {
		v := c.Load()
		total += v
		if v > 0 {
			active++
		}
		if v < minGot {
			minGot = v
		}
	}
	expect := float64(sent.Load()) * float64(*subs)
	fmt.Printf("subs=%d active=%d sent=%d (%.0f pps) received=%d loss=%.2f%% min_per_sub=%d\n",
		*subs, active, sent.Load(), float64(sent.Load())/elapsed, total, 100*(1-float64(total)/expect), minGot)
	if *pid != 0 {
		fmt.Printf("server cpu=%.2fs over %.0fs (%.1f%% of one core) rss=%.0f MiB\n", cpu1-cpu0, elapsed, 100*(cpu1-cpu0)/elapsed, rssMiB())
	}
}

var pubTrack *webrtc.TrackLocalStaticRTP

func newPeer(publisher bool) *webrtc.PeerConnection {
	me := &webrtc.MediaEngine{}
	must(me.RegisterDefaultCodecs())
	pc, err := webrtc.NewAPI(webrtc.WithMediaEngine(me)).NewPeerConnection(webrtc.Configuration{})
	must(err)
	if publisher {
		t, err := webrtc.NewTrackLocalStaticRTP(webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeVP8}, "video", "load")
		must(err)
		pubTrack = t
		_, err = pc.AddTrack(t)
		must(err)
	}
	return pc
}

// signal posts an offer and returns the answer the server prints next.
func signal(d *webrtc.SessionDescription) webrtc.SessionDescription {
	before := lines()
	b, _ := json.Marshal(d)
	resp, err := http.Post(*url, "text/plain", strings.NewReader(base64.StdEncoding.EncodeToString(b)))
	must(err)
	resp.Body.Close()
	for i := 0; i < 600; i++ {
		ls := lines()
		for _, l := range ls[len(before):] {
			raw, err := base64.StdEncoding.DecodeString(strings.TrimSpace(l))
			if err != nil {
				continue
			}
			var a webrtc.SessionDescription
			if json.Unmarshal(raw, &a) == nil && a.Type == webrtc.SDPTypeAnswer {
				return a
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	panic("no answer in server log")
}

func lines() []string {
	f, err := os.Open(*logPath)
	must(err)
	defer f.Close()
	var out []string
	s := bufio.NewScanner(f)
	s.Buffer(make([]byte, 1<<20), 1<<20)
	for s.Scan() {
		out = append(out, s.Text())
	}
	return out
}

func ps(field string) string {
	out, _ := exec.Command("ps", "-o", field+"=", "-p", strconv.Itoa(*pid)).Output()
	return strings.TrimSpace(string(bytes.TrimSpace(out)))
}

// cpuSeconds parses ps's cputime ([[dd-]hh:]mm:ss.xx) into seconds.
func cpuSeconds() float64 {
	if *pid == 0 {
		return 0
	}
	s := ps("cputime")
	var secs float64
	for _, part := range strings.Split(s, ":") {
		v, _ := strconv.ParseFloat(part, 64)
		secs = secs*60 + v
	}
	return secs
}

func rssMiB() float64 {
	v, _ := strconv.ParseFloat(ps("rss"), 64)
	return v / 1024
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
