package bridge

import (
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bluenviron/gortsplib/v5"
	"github.com/bluenviron/gortsplib/v5/pkg/base"
	"github.com/bluenviron/gortsplib/v5/pkg/description"
	"github.com/bluenviron/gortsplib/v5/pkg/format"
	"github.com/bluenviron/gortsplib/v5/pkg/format/rtph265"
	"github.com/pion/rtp"
)

type fixturePublisher struct {
	videoPackets int
	audioPackets int
	publications int
}

func (p *fixturePublisher) Open(_ string, _ []byte, _ []byte, _ []byte, epochPTS uint64, _ *TalkTarget) (*Publication, error) {
	encoder := newH265EncoderForTest()
	p.publications++
	return &Publication{videoEnc: encoder, epochPTS: epochPTS, epochWall: time.Unix(0, 0), videoBase: 1000}, nil
}

func (p *fixturePublisher) ClosePublication(*Publication) {}

func (p *fixturePublisher) EncodeVideo(publication *Publication, nalus [][]byte) ([]*rtp.Packet, error) {
	return publication.EncodeVideo(nalus)
}

func (p *fixturePublisher) WriteVideo(_ *Publication, _ *rtp.Packet, _ time.Time) error {
	p.videoPackets++
	return nil
}

func (p *fixturePublisher) WriteAudio(_ *Publication, _ *rtp.Packet, _ time.Time) error {
	p.audioPackets++
	return nil
}

func newH265EncoderForTest() rtph265.Encoder {
	encoder := rtph265.Encoder{PayloadType: 96, PayloadMaxSize: 1200}
	if err := encoder.Init(); err != nil {
		panic(err)
	}
	return encoder
}

func TestPrivateC07Pipeline(t *testing.T) {
	root := os.Getenv("CATLINK_TESTDATA_DIR")
	manifestPath := os.Getenv("CATLINK_TESTDATA_MANIFEST")
	if root == "" || manifestPath == "" {
		t.Skip("private fixture gate not configured")
	}
	manifest, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatal("cannot read private fixture manifest")
	}
	count := 0
	for _, line := range strings.Split(string(manifest), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 2 || strings.HasPrefix(fields[0], "#") {
			continue
		}
		data, err := os.ReadFile(filepath.Join(root, fields[1]))
		if err != nil {
			t.Fatal("cannot read private fixture")
		}
		recorder := new(fixturePublisher)
		pipeline, err := NewPipeline("/fixture", recorder)
		if err != nil {
			t.Fatal("create pipeline")
		}
		for offset := 0; offset < len(data); {
			end := offset + 1388
			if end > len(data) {
				end = len(data)
			}
			if err := pipeline.Feed(data[offset:end]); err != nil {
				t.Fatalf("fixture %s pipeline failed: %v", fields[0], err)
			}
			offset = end
		}
		if len(fields) == 3 && fields[2] == "abrupt" {
			pipeline.CloseAbrupt()
		}
		if recorder.videoPackets == 0 || recorder.audioPackets == 0 || recorder.publications == 0 {
			t.Fatalf("fixture %s produced incomplete RTP counts", fields[0])
		}
		count++
	}
	if count == 0 {
		t.Fatal("private fixture manifest is empty")
	}
}

func TestPrivateC07RTSPLoopback(t *testing.T) {
	root := os.Getenv("CATLINK_TESTDATA_DIR")
	manifestPath := os.Getenv("CATLINK_TESTDATA_MANIFEST")
	if root == "" || manifestPath == "" {
		t.Skip("private fixture gate not configured")
	}
	manifest, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatal("cannot read private fixture manifest")
	}
	var fixture string
	for _, line := range strings.Split(string(manifest), "\n") {
		fields := strings.Fields(line)
		if len(fields) >= 2 && !strings.HasPrefix(fields[0], "#") {
			fixture = filepath.Join(root, fields[1])
			break
		}
	}
	if fixture == "" {
		t.Fatal("private fixture manifest is empty")
	}
	data, err := os.ReadFile(fixture)
	if err != nil {
		t.Fatal("cannot read private fixture")
	}

	publisher := NewRTSPPublisher("127.0.0.1:0")
	var listener net.Listener
	publisher.server.Listen = func(network, address string) (net.Listener, error) {
		var err error
		listener, err = net.Listen(network, address)
		return listener, err
	}
	if err := publisher.Start(); err != nil {
		t.Fatalf("start publisher: %v", err)
	}
	defer publisher.Shutdown()
	pipeline, err := NewPipeline("/fixture", publisher)
	if err != nil {
		t.Fatalf("create pipeline: %v", err)
	}
	feedDone := make(chan error, 1)
	go func() {
		for offset := 0; offset < len(data); {
			end := offset + 1388
			if end > len(data) {
				end = len(data)
			}
			if err := pipeline.Feed(data[offset:end]); err != nil {
				feedDone <- err
				return
			}
			offset = end
			time.Sleep(time.Millisecond)
		}
		feedDone <- nil
	}()

	serverURL, err := url.Parse("rtsp://" + listener.Addr().String() + "/fixture")
	if err != nil {
		t.Fatal(err)
	}
	var client *gortsplib.Client
	var desc *description.Session
	for attempt := 0; attempt < 100; attempt++ {
		candidate := &gortsplib.Client{Scheme: serverURL.Scheme, Host: serverURL.Host, ReadTimeout: 5 * time.Second, WriteTimeout: 5 * time.Second}
		protocol := gortsplib.ProtocolTCP
		candidate.Protocol = &protocol
		if err := candidate.Start(); err == nil {
			desc, _, err = candidate.Describe(&base.URL{Scheme: serverURL.Scheme, Host: serverURL.Host, Path: serverURL.Path})
			if err == nil {
				client = candidate
				break
			}
			candidate.Close()
		}
		time.Sleep(10 * time.Millisecond)
	}
	if client == nil || desc == nil {
		t.Fatal("RTSP publication did not become available")
	}
	defer client.Close()
	if len(desc.Medias) != 2 {
		t.Fatalf("description medias=%d", len(desc.Medias))
	}
	if err := client.SetupAll(desc.BaseURL, desc.Medias); err != nil {
		t.Fatalf("setup: %v", err)
	}
	videoPackets := make(chan struct{}, 1)
	audioPackets := make(chan struct{}, 1)
	client.OnPacketRTPAny(func(_ *description.Media, forma format.Format, _ *rtp.Packet) {
		switch forma.PayloadType() {
		case 96:
			select {
			case videoPackets <- struct{}{}:
			default:
			}
		case 8:
			select {
			case audioPackets <- struct{}{}:
			default:
			}
		}
	})
	if _, err := client.Play(nil); err != nil {
		t.Fatalf("play: %v", err)
	}
	videoSeen, audioSeen := false, false
	deadline := time.After(3 * time.Second)
	for !(videoSeen && audioSeen) {
		select {
		case <-videoPackets:
			videoSeen = true
		case <-audioPackets:
			audioSeen = true
		case err := <-feedDone:
			if err != nil {
				t.Fatalf("fixture feed: %v", err)
			}
			feedDone = nil
		case <-deadline:
			t.Fatalf("RTP not received video=%t audio=%t", videoSeen, audioSeen)
		}
	}
	select {
	case err := <-feedDone:
		if err != nil {
			t.Fatalf("fixture feed: %v", err)
		}
	default:
	}
}
