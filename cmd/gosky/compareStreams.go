package main

import (
	"context"
	"fmt"
	comatproto "github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/events"
	"github.com/bluesky-social/indigo/events/schedulers/sequential"
	lexutil "github.com/bluesky-social/indigo/lex/util"
	"github.com/gorilla/websocket"
	"github.com/urfave/cli/v2"
	"io"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

type compareStreamsSession struct {
	streams []compareStreamsStream

	// time between seeing a matched event on one side and the other
	spreadWindowedAverage durationWindowedAverage

	matches uint64
}

func (ses *compareStreamsSession) printCurrentDelta(out io.Writer) (int, error) {
	var sb strings.Builder
	for i, stm := range ses.streams {
		fmt.Fprintf(&sb, "[%d] %d pending", i, stm.pendingCount)
		if i > 0 {
			sb.WriteString(", ")
		}
	}
	sb.WriteRune('\n')
	return out.Write([]byte(sb.String()))
}
func (ses *compareStreamsSession) printDetailedDelta(out io.Writer) {
	for did, sl := range ses.streams[0].buffer {
		osl := ses.streams[1].buffer[did]
		if len(osl) > 0 && len(sl) > 0 {
			fmt.Fprintf(out, "%s had mismatched events on both streams (%d, %d)\n", did, len(sl), len(osl))
		}
	}
}

func (ses *compareStreamsSession) run(url1, url2 string) error {
	d := websocket.DefaultDialer
	events := make(chan indexedEvent, 10)
	// Create two goroutines for reading events from two URLs
	for i, url := range []string{url1, url2} {
		ses := newCompareStreamsStream(url, i, events)
		go ses.run(d)
	}
	logPeriod := time.Second
	out := os.Stdout

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGTERM, syscall.SIGINT)

	totalEventCount := uint64(0)
	nextLog := time.Now().Add(logPeriod)
	// Compare events from the two URLs
	for {
		select {
		case event := <-events:
			totalEventCount++
			source := event.sourceId
			other := source ^ 1
			found, dt, err := ses.streams[other].findMatchAndRemove(event)
			if err != nil {
				fmt.Println("checking for match failed: ", err)
				continue
			}
			if !found {
				ses.streams[source].add(event)
			} else {
				// the good case
				//fmt.Println("Match found")
				ses.spreadWindowedAverage.add(dt)
				ses.matches++
			}
		case <-ch:
			// shutdown
			ses.printDetailedDelta(out)
			return nil
		}

		if totalEventCount%100 == 0 {
			now := time.Now()
			if now.After(nextLog) {
				ses.printCurrentDelta(out)
				nextLog = now.Add(logPeriod)
				for now.After(nextLog) {
					nextLog = now.Add(logPeriod)
				}
			}
		}
	}
}

type durationWindowedAverage struct {
	WindowSize int

	times []time.Duration
	nextt int
	sum   time.Duration
}

func (dwa *durationWindowedAverage) add(dt time.Duration) {
	if len(dwa.times) < dwa.WindowSize {
		dwa.times = append(dwa.times, dt)
		dwa.sum += dt
		return
	}
	dwa.sum -= dwa.times[dwa.nextt]
	dwa.times[dwa.nextt] = dt
	dwa.sum += dt
	dwa.nextt = (dwa.nextt + 1) % len(dwa.times)
}

func (dwa *durationWindowedAverage) mean() time.Duration {
	return dwa.sum / time.Duration(len(dwa.times))
}

// just the bits we need from comatproto.SyncSubscribeRepos_Commit
type minCommit struct {
	Commit lexutil.LexLink  `json:"commit" cborgen:"commit"`
	Prev   *lexutil.LexLink `json:"prev" cborgen:"prev"`
	Repo   string           `json:"repo" cborgen:"repo"`
	Seq    int64            `json:"seq" cborgen:"seq"`
}

func commitToMinCommit(evt *comatproto.SyncSubscribeRepos_Commit) minCommit {
	return minCommit{
		Commit: evt.Commit,
		Prev:   evt.Prev,
		Repo:   evt.Repo,
		Seq:    evt.Seq,
	}
}

type indexedEvent struct {
	sourceId  int
	event     minCommit
	arrivedAt time.Time
}

type compareStreamsStream struct {
	url          string
	n            int
	pendingCount int
	events       chan<- indexedEvent
	buffer       map[string][]indexedEvent
	count        uint64
}

func newCompareStreamsStream(url string, n int, events chan<- indexedEvent) *compareStreamsStream {
	return &compareStreamsStream{
		url:    url,
		n:      n,
		events: events,
		buffer: make(map[string][]indexedEvent),
	}
}

func (cstream *compareStreamsStream) findMatchAndRemove(event indexedEvent) (found bool, dt time.Duration, err error) {
	buf := cstream.buffer
	slice, ok := buf[event.event.Repo]
	if !ok || len(slice) == 0 {
		return false, 0, nil
	}

	for i, ev := range slice {
		if ev.event.Commit == event.event.Commit {
			if llpEq(ev.event.Prev, event.event.Prev) {
				// same commit different prev??
				return false, 0, fmt.Errorf("matched event with same commit but different prev: (%d) %d - %d", cstream.n, ev.event.Seq, event.event.Seq)
			}
		}

		if i != 0 {
			fmt.Printf("detected skipped event: %d (%d)\n", slice[0].event.Seq, i)
		}

		dt := event.arrivedAt.Sub(ev.arrivedAt)
		slice = slice[i+1:]
		cstream.pendingCount--
		buf[event.event.Repo] = slice
		return true, dt, nil
	}

	return false, 0, fmt.Errorf("did not find matching event despite having events in buffer")
}

func (cstream *compareStreamsStream) add(event indexedEvent) {
	cstream.pendingCount++
	cstream.buffer[event.event.Repo] = append(cstream.buffer[event.event.Repo], event)
}

func (cstream *compareStreamsStream) run(d *websocket.Dialer) {
	con, _, err := d.Dial(cstream.url, http.Header{})
	i := cstream.n
	if err != nil {
		log.Error("Dial failure", "i", i, "url", cstream.url, "err", err)
		os.Exit(1)
	}

	ctx := context.TODO()
	rsc := &events.RepoStreamCallbacks{
		RepoCommit: func(evt *comatproto.SyncSubscribeRepos_Commit) error {
			cstream.count++
			minc := commitToMinCommit(evt)
			cstream.events <- indexedEvent{event: minc, sourceId: cstream.n, arrivedAt: time.Now()}
			return nil
		},
		// TODO: all the other Repo* event types
		Error: func(evt *events.ErrorFrame) error {
			return fmt.Errorf("%s: %s", evt.Error, evt.Message)
		},
	}
	seqScheduler := sequential.NewScheduler(fmt.Sprintf("debug-stream-%d", i+1), rsc.EventHandler)
	if err := events.HandleRepoStream(ctx, con, seqScheduler, nil); err != nil {
		log.Error("HandleRepoStream failure", "i", i, "url", cstream.url, "err", err)
		os.Exit(1)
	}
}

func llpEq(a, b *lexutil.LexLink) bool {
	if a == nil {
		return b == nil
	}
	if b == nil {
		return false
	}
	return *a == *b
}

func compareStreams(cctx *cli.Context) error {
	h1 := cctx.String("host1")
	h2 := cctx.String("host2")

	url1 := fmt.Sprintf("%s/xrpc/com.atproto.sync.subscribeRepos", h1)
	url2 := fmt.Sprintf("%s/xrpc/com.atproto.sync.subscribeRepos", h2)

	var ses compareStreamsSession
	return ses.run(url1, url2)
}

func compareStreamsOLD(cctx *cli.Context) error {
	h1 := cctx.String("host1")
	h2 := cctx.String("host2")

	url1 := fmt.Sprintf("%s/xrpc/com.atproto.sync.subscribeRepos", h1)
	url2 := fmt.Sprintf("%s/xrpc/com.atproto.sync.subscribeRepos", h2)

	d := websocket.DefaultDialer

	eventChans := []chan *comatproto.SyncSubscribeRepos_Commit{
		make(chan *comatproto.SyncSubscribeRepos_Commit, 2),
		make(chan *comatproto.SyncSubscribeRepos_Commit, 2),
	}

	buffers := []map[string][]*comatproto.SyncSubscribeRepos_Commit{
		make(map[string][]*comatproto.SyncSubscribeRepos_Commit),
		make(map[string][]*comatproto.SyncSubscribeRepos_Commit),
	}

	addToBuffer := func(n int, event *comatproto.SyncSubscribeRepos_Commit) {
		buffers[n][event.Repo] = append(buffers[n][event.Repo], event)
	}

	//pll := func(ll *lexutil.LexLink) string {
	//	if ll == nil {
	//		return "<nil>"
	//	}
	//	return ll.String()
	//}

	findMatchAndRemove := func(n int, event *comatproto.SyncSubscribeRepos_Commit) (*comatproto.SyncSubscribeRepos_Commit, error) {
		buf := buffers[n]
		slice, ok := buf[event.Repo]
		if !ok || len(slice) == 0 {
			return nil, nil
		}

		for i, ev := range slice {
			if ev.Commit == event.Commit {
				if llpEq(ev.Prev, event.Prev) {
					// same commit different prev??
					return nil, fmt.Errorf("matched event with same commit but different prev: (%d) %d - %d", n, ev.Seq, event.Seq)
				}
			}

			if i != 0 {
				fmt.Printf("detected skipped event: %d (%d)\n", slice[0].Seq, i)
			}

			slice = slice[i+1:]
			buf[event.Repo] = slice
			return ev, nil
		}

		return nil, fmt.Errorf("did not find matching event despite having events in buffer")
	}

	printCurrentDelta := func() {
		var a, b int
		for _, sl := range buffers[0] {
			a += len(sl)
		}
		for _, sl := range buffers[1] {
			b += len(sl)
		}

		fmt.Printf("%d %d\n", a, b)
	}

	printDetailedDelta := func() {
		for did, sl := range buffers[0] {
			osl := buffers[1][did]
			if len(osl) > 0 && len(sl) > 0 {
				fmt.Printf("%s had mismatched events on both streams (%d, %d)\n", did, len(sl), len(osl))
			}

		}
	}

	// Create two goroutines for reading events from two URLs
	for i, url := range []string{url1, url2} {
		go func(i int, url string) {
			con, _, err := d.Dial(url, http.Header{})
			if err != nil {
				log.Error("Dial failure", "i", i, "url", url, "err", err)
				os.Exit(1)
			}

			ctx := context.TODO()
			rsc := &events.RepoStreamCallbacks{
				RepoCommit: func(evt *comatproto.SyncSubscribeRepos_Commit) error {
					eventChans[i] <- evt
					return nil
				},
				// TODO: all the other Repo* event types
				Error: func(evt *events.ErrorFrame) error {
					return fmt.Errorf("%s: %s", evt.Error, evt.Message)
				},
			}
			seqScheduler := sequential.NewScheduler(fmt.Sprintf("debug-stream-%d", i+1), rsc.EventHandler)
			if err := events.HandleRepoStream(ctx, con, seqScheduler, nil); err != nil {
				log.Error("HandleRepoStream failure", "i", i, "url", url, "err", err)
				os.Exit(1)
			}
		}(i, url)
	}

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGTERM, syscall.SIGINT)

	// Compare events from the two URLs
	for {
		select {
		case event := <-eventChans[0]:
			partner, err := findMatchAndRemove(1, event)
			if err != nil {
				fmt.Println("checking for match failed: ", err)
				continue
			}
			if partner == nil {
				addToBuffer(0, event)
			} else {
				// the good case
				fmt.Println("Match found")
			}

		case event := <-eventChans[1]:
			partner, err := findMatchAndRemove(0, event)
			if err != nil {
				fmt.Println("checking for match failed: ", err)
				continue
			}
			if partner == nil {
				addToBuffer(1, event)
			} else {
				// the good case
				fmt.Println("Match found")
			}
		case <-ch:
			printDetailedDelta()
			/*
				b, err := json.Marshal(buffers)
				if err != nil {
					return err
				}

				fmt.Println(string(b))
			*/
			return nil
		}

		printCurrentDelta()
	}
}
