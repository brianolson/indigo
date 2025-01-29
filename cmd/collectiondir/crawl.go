package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"

	"github.com/urfave/cli/v2"

	"github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/xrpc"
)

type DidCollection struct {
	Did        string `json:"d"`
	Collection string `json:"c"`
}

func DidCollectionsToCsv(out io.Writer, sources <-chan DidCollection) {
	writer := csv.NewWriter(out)
	defer writer.Flush()
	var row [2]string
	for dc := range sources {
		row[0] = dc.Did
		row[1] = dc.Collection
		writer.Write(row[:])
	}
}

var crawlCmd = &cli.Command{
	Name:  "crawl",
	Usage: "crawl a PDS",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "host",
			Usage: "hostname or URL of PDS",
		},
		&cli.StringFlag{
			Name:  "csv-out",
			Usage: "path for output or - for stdout",
		},
		&cli.StringFlag{
			Name:    "ratelimit-header",
			Usage:   "secret for friend PDSes",
			EnvVars: []string{"BSKY_SOCIAL_RATE_LIMIT_SKIP", "RATE_LIMIT_HEADER"},
		},
	},
	Action: func(cctx *cli.Context) error {
		log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		hostname := cctx.String("host")
		hosturl, err := url.Parse(hostname)
		if err != nil {
			hosturl = new(url.URL)
			hosturl.Scheme = "https"
			hosturl.Host = hostname
		}
		httpClient := http.Client{}
		rpcClient := xrpc.Client{
			Host:   hosturl.String(),
			Client: &httpClient,
		}
		if cctx.IsSet("ratelimit-header") {
			rpcClient.Headers = map[string]string{
				"x-ratelimit-bypass": cctx.String("ratelimit-header"),
			}
		}
		log.Info("will crawl", "url", rpcClient.Host)
		csvOutPath := cctx.String("csv-out")
		var fout io.Writer = os.Stdout
		if csvOutPath != "" {
			if csvOutPath == "-" {
				fout = os.Stdout
			} else {
				fout, err = os.Create(csvOutPath)
				if err != nil {
					return fmt.Errorf("%s: could not open for writing: %w", csvOutPath, err)
				}
			}
		}
		results := make(chan DidCollection, 100)
		defer close(results)
		go DidCollectionsToCsv(fout, results)
		var cursor string
		for true {
			repos, err := atproto.SyncListRepos(ctx, &rpcClient, cursor, 1000)
			if err != nil {
				// TODO: wait N seconds, retry M times
				return fmt.Errorf("%s: sync repos: %w", hostname, err)
			}
			log.Info("got repo list", "count", len(repos.Repos))
			for _, xr := range repos.Repos {
				// TODO: rate limit
				desc, err := atproto.RepoDescribeRepo(ctx, &rpcClient, xr.Did)
				if err != nil {
					log.Error("repo desc", "host", hostname, "did", xr.Did, "err", err)
					continue
				}
				for _, collection := range desc.Collections {
					results <- DidCollection{Did: xr.Did, Collection: collection}
				}
			}
			if repos.Cursor != nil {
				cursor = *repos.Cursor
			} else {
				break
			}
		}
		log.Info("done")

		return nil
	},
}
