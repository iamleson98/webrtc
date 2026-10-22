// Go twin of the webrtc-rs end-to-end benches (benches/throughput.rs, memory.rs, latency.rs), on the published pion module.
package main

import (
	"flag"
	"fmt"
	"io"
	"net"
	"sort"
	"sync/atomic"
	"time"

	"github.com/pion/interceptor"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
)

const (
	totalBytes = 256 << 20
	chunkSize  = 16 << 10
	bufferHigh = 8 << 20
)

func newPC(detach bool) *webrtc.PeerConnection {
	se := webrtc.SettingEngine{}
	se.SetNetworkTypes([]webrtc.NetworkType{webrtc.NetworkTypeUDP4})
	se.SetIncludeLoopbackCandidate(true)
	se.SetInterfaceFilter(func(n string) bool { return n == "lo0" || n == "lo" })
	if detach {
		se.DetachDataChannels()
	}
	api := webrtc.NewAPI(webrtc.WithSettingEngine(se))
	pc, err := api.NewPeerConnection(webrtc.Configuration{})
	must(err)
	return pc
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func signal(o, a *webrtc.PeerConnection) {
	offer, err := o.CreateOffer(nil)
	must(err)
	g := webrtc.GatheringCompletePromise(o)
	must(o.SetLocalDescription(offer))
	<-g
	must(a.SetRemoteDescription(*o.LocalDescription()))
	ans, err := a.CreateAnswer(nil)
	must(err)
	g2 := webrtc.GatheringCompletePromise(a)
	must(a.SetLocalDescription(ans))
	<-g2
	must(o.SetRemoteDescription(*a.LocalDescription()))
}

func b1(detach bool) {
	o, a := newPC(detach), newPC(detach)
	done := make(chan struct{})
	var received atomic.Int64
	a.OnDataChannel(func(dc *webrtc.DataChannel) {
		if detach {
			dc.OnOpen(func() {
				raw, err := dc.Detach()
				must(err)
				go func() {
					buf := make([]byte, 64<<10)
					for {
						n, err := raw.Read(buf)
						if err != nil {
							return
						}
						if received.Add(int64(n)) >= totalBytes {
							close(done)
							return
						}
					}
				}()
			})
			return
		}
		dc.OnMessage(func(m webrtc.DataChannelMessage) {
			if received.Add(int64(len(m.Data))) >= totalBytes {
				select {
				case <-done:
				default:
					close(done)
				}
			}
		})
	})
	dc, err := o.CreateDataChannel("bench", nil)
	must(err)
	opened := make(chan struct{})
	var start time.Time
	payload := make([]byte, chunkSize)
	low := make(chan struct{}, 1)
	dc.SetBufferedAmountLowThreshold(bufferHigh / 2)
	dc.OnBufferedAmountLow(func() {
		select {
		case low <- struct{}{}:
		default:
		}
	})
	dc.OnOpen(func() {
		go func() {
			var w io.Writer = nil
			if detach {
				raw, err := dc.Detach()
				must(err)
				w = raw
			}
			close(opened)
			start = time.Now()
			sent := 0
			for sent < totalBytes {
				if dc.BufferedAmount() > bufferHigh {
					select {
					case <-low:
					case <-time.After(time.Millisecond):
					}
					continue
				}
				if w != nil {
					_, err = w.Write(payload)
				} else {
					err = dc.Send(payload)
				}
				must(err)
				sent += chunkSize
			}
		}()
	})
	signal(o, a)
	<-opened
	<-done
	el := time.Since(start)
	fmt.Printf("GO_B1 result: detach=%v bytes=%d elapsed_ms=%d throughput_mibs=%.2f\n", detach, totalBytes, el.Milliseconds(), float64(totalBytes)/(1<<20)/el.Seconds())
	o.Close()
	a.Close()
}

func b2(n int, detach bool) {
	var pcs []*webrtc.PeerConnection
	for i := 0; i < n; i++ {
		o, a := newPC(detach), newPC(detach)
		_, err := o.CreateDataChannel("keepalive", nil)
		must(err)
		signal(o, a)
		pcs = append(pcs, o, a)
	}
	time.Sleep(8 * time.Second)
	fmt.Printf("GO_B2 result: connections=%d\n", len(pcs))
	for _, p := range pcs {
		p.Close()
	}
}

func b3(rounds int, detach bool) {
	var s []time.Duration
	for i := 0; i < rounds; i++ {
		t0 := time.Now()
		o, a := newPC(detach), newPC(detach)
		dc, err := o.CreateDataChannel("latency", nil)
		must(err)
		open := make(chan struct{})
		dc.OnOpen(func() { close(open) })
		signal(o, a)
		<-open
		s = append(s, time.Since(t0))
		o.Close()
		a.Close()
	}
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	var sum time.Duration
	for _, d := range s {
		sum += d
	}
	fmt.Printf("GO_B3 result: rounds=%d median_ms=%d mean_ms=%.1f min_ms=%d max_ms=%d\n", rounds, s[len(s)/2].Milliseconds(), float64(sum.Milliseconds())/float64(len(s)), s[0].Milliseconds(), s[len(s)-1].Milliseconds())
}

var _ = net.IPv4

// Media forwarding (twin of benches/media.rs): 60,000 VP8-typed RTP packets of 1200 bytes, paced
// at 12 per millisecond, from one peer connection to another over loopback with the default
// codecs and interceptors.
const (
	mediaPackets = 60000
	mediaPerTick = 12
	mediaPayload = 1188
	mediaSSRC    = 0x12345678
)

func newMediaPC() *webrtc.PeerConnection {
	se := webrtc.SettingEngine{}
	se.SetNetworkTypes([]webrtc.NetworkType{webrtc.NetworkTypeUDP4})
	se.SetIncludeLoopbackCandidate(true)
	se.SetInterfaceFilter(func(n string) bool { return n == "lo0" || n == "lo" })
	m := &webrtc.MediaEngine{}
	must(m.RegisterDefaultCodecs())
	ir := &interceptor.Registry{}
	must(webrtc.RegisterDefaultInterceptors(m, ir))
	api := webrtc.NewAPI(webrtc.WithSettingEngine(se), webrtc.WithMediaEngine(m), webrtc.WithInterceptorRegistry(ir))
	pc, err := api.NewPeerConnection(webrtc.Configuration{})
	must(err)
	return pc
}

func b4() {
	o, a := newMediaPC(), newMediaPC()
	track, err := webrtc.NewTrackLocalStaticRTP(webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeVP8}, "bench-track", "bench-stream")
	must(err)
	sender, err := o.AddTrack(track)
	must(err)
	// Read the sender's RTCP, as pion's examples do, so its interceptors see receiver reports.
	go func() {
		buf := make([]byte, 1500)
		for {
			if _, _, err := sender.Read(buf); err != nil {
				return
			}
		}
	}()
	var received atomic.Int64
	a.OnTrack(func(tr *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
		for {
			if _, _, err := tr.ReadRTP(); err != nil {
				return
			}
			received.Add(1)
		}
	})
	connected := make(chan struct{})
	var once atomic.Bool
	o.OnConnectionStateChange(func(s webrtc.PeerConnectionState) {
		if s == webrtc.PeerConnectionStateConnected && once.CompareAndSwap(false, true) {
			close(connected)
		}
	})
	signal(o, a)
	<-connected

	payload := make([]byte, mediaPayload)
	pkt := &rtp.Packet{Header: rtp.Header{Version: 2, PayloadType: 96, SSRC: mediaSSRC}, Payload: payload}
	start := time.Now()
	sent := 0
	tick := 0
	for sent < mediaPackets {
		tick++
		if d := time.Until(start.Add(time.Duration(tick) * time.Millisecond)); d > 0 {
			time.Sleep(d)
		}
		for i := 0; i < mediaPerTick; i++ {
			pkt.SequenceNumber = uint16(sent)
			pkt.Timestamp = uint32(sent) * 3000
			must(track.WriteRTP(pkt))
			sent++
		}
	}
	last := received.Load()
	quiet := 0
	for quiet < 6 && int(last) < sent {
		time.Sleep(50 * time.Millisecond)
		now := received.Load()
		if now == last {
			quiet++
		} else {
			quiet = 0
		}
		last = now
	}
	fmt.Printf("GO_B4 result: sent=%d received=%d elapsed_ms=%d\n", sent, received.Load(), time.Since(start).Milliseconds())
	_ = o.Close()
	_ = a.Close()
}

func main() {
	mode := flag.String("mode", "b1", "b1|b2|b3|b4")
	n := flag.Int("n", 50, "pairs for b2 / rounds for b3")
	detach := flag.Bool("detach", false, "detached data channels")
	flag.Parse()
	switch *mode {
	case "b1":
		b1(*detach)
	case "b2":
		b2(*n, *detach)
	case "b3":
		b3(*n, *detach)
	case "b4":
		b4()
	}
}
