package main

import (
	"context"
	"fmt"
	"github.com/bluesky-social/indigo/events"
	"github.com/gorilla/websocket"
	"log/slog"
	"net/http"
	"time"
)

type Firehose struct {
	Log *slog.Logger

	Host string
	Seq  int64

	events chan<- *events.XRPCStreamEvent
}

func (fh *Firehose) subscribeWithRedialer(ctx context.Context, fhevents chan<- *events.XRPCStreamEvent) {
	d := websocket.Dialer{}

	protocol := "wss"
	fh.events = fhevents
	defer close(fhevents)

	var backoff int
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		header := http.Header{
			"User-Agent": []string{"bgs-rainbow-v0"},
		}

		var url string
		if fh.Seq < 0 {
			url = fmt.Sprintf("%s://%s/xrpc/com.atproto.sync.subscribeRepos", protocol, fh.Host)
		} else {
			url = fmt.Sprintf("%s://%s/xrpc/com.atproto.sync.subscribeRepos?cursor=%d", protocol, fh.Host, fh.Seq)
		}
		con, res, err := d.DialContext(ctx, url, header)
		if err != nil {
			fh.Log.Warn("dialing failed", "host", fh.Host, "err", err, "backoff", backoff)
			time.Sleep(5 * time.Second) // TODO: backoff strategy?
			backoff++

			continue
		}

		fh.Log.Info("event subscription response", "code", res.StatusCode)

		if err := fh.handleConnection(ctx, con); err != nil {
			fh.Log.Warn("connection failed", "host", fh.Host, "err", err)
		}
	}
}

func (fh *Firehose) handleConnection(ctx context.Context, con *websocket.Conn) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	return events.HandleRepoStream(ctx, con, fh, fh.Log)
}

// AddWork is part of events.Scheduler
func (fh *Firehose) AddWork(ctx context.Context, repo string, val *events.XRPCStreamEvent) error {
	tsv, ok := val.GetSequence()
	if ok {
		fh.Seq = tsv
	}
	fh.events <- val
	return nil
}

// Shutdown is part of events.Scheduler
func (fh *Firehose) Shutdown() {
	// unneeded in this usage
}
