package main

import (
	"context"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/urfave/cli/v2"
	"log/slog"
	"net"
	"net/http"
	"sort"
	"strconv"
	"sync"
	"time"
)

var serveCmd = &cli.Command{
	Name: "serve",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:    "api-listen",
			Value:   ":2510",
			EnvVars: []string{"RAINBOW_API_LISTEN"},
		},
		&cli.StringFlag{
			Name:    "metrics-listen",
			Value:   ":2511",
			EnvVars: []string{"RAINBOW_METRICS_LISTEN", "SPLITTER_METRICS_LISTEN"},
		},
	},
	Action: func(cctx *cli.Context) error {
		var server collectionServer
		return server.run(cctx)
	},
}

type collectionServer struct {
	pcd *PebbleCollectionDirectory

	statsCache        *CollectionStats
	statsCacheWhen    time.Time
	statsCacheLock    sync.Mutex
	statsCacheFresh   sync.Cond
	statsCachePending bool
}

func (cs *collectionServer) run(cctx *cli.Context) error {
	cs.statsCacheFresh.L = &cs.statsCacheLock
	return nil
}

func (cs *collectionServer) StartApiServer(ctx context.Context, addr string) error {
	var lc net.ListenConfig
	li, err := lc.Listen(ctx, "tcp", addr)
	if err != nil {
		return err
	}
	e := echo.New()
	e.HideBanner = true

	e.Use(middleware.CORSWithConfig(middleware.CORSConfig{
		AllowOrigins: []string{"*"},
		AllowHeaders: []string{echo.HeaderOrigin, echo.HeaderContentType, echo.HeaderAccept, echo.HeaderAuthorization},
	}))

	e.GET("/v1/getDidsForCollection", cs.getDidsForCollection)
	e.GET("/v1/listCollections", cs.listCollections)

	e.Listener = li
	srv := &http.Server{}
	return e.StartServer(srv)
}

const statsCacheDuration = time.Second * 300

type GetDidsForCollectionResponse struct {
	Dids   []string `json:"dids"`
	Cursor string   `json:"cursor"`
}

func getLimit(c echo.Context, min, defaultLim, max int) int {
	limstr := c.QueryParam("limit")
	if limstr == "" {
		return defaultLim
	}
	lvx, err := strconv.ParseInt(limstr, 10, 64)
	if err != nil {
		return defaultLim
	}
	lv := int(lvx)
	if lv < min {
		return min
	}
	if lv > max {
		return max
	}
	return lv
}

// /v1/getDidsForCollection?collection={}&cursor={}
//
// returns
// {"dids":["did:A", "..."], "cursor":"opaque text"}
func (cs *collectionServer) getDidsForCollection(c echo.Context) error {
	ctx := c.Request().Context()
	collection := c.QueryParam("collection")
	cursor := c.QueryParam("cursor")
	limit := getLimit(c, 50, 500, 1000)
	they, nextCursor, err := cs.pcd.ReadCollection(ctx, collection, cursor, limit)
	if err != nil {
		slog.Error("ReadCollection", "collection", collection, "cursor", cursor, "limit", limit, "err", err)
		return c.String(http.StatusInternalServerError, "oops")
	}
	var out GetDidsForCollectionResponse
	out.Dids = make([]string, len(they))
	for i, rec := range they {
		out.Dids[i] = rec.Did
	}
	out.Cursor = nextCursor
	return c.JSON(http.StatusOK, out)
}

func (cs *collectionServer) getStatsCache() (*CollectionStats, error) {
	var statsCache *CollectionStats
	var staleCache *CollectionStats
	var waiter *freshStatsWaiter
	cs.statsCacheLock.Lock()
	if cs.statsCache != nil {
		if time.Since(cs.statsCacheWhen) < statsCacheDuration {
			// has fresh!
			statsCache = cs.statsCache
		} else if !cs.statsCachePending {
			cs.statsCachePending = true
			go cs.statsBuilder()
			staleCache = cs.statsCache
		} else {
			staleCache = cs.statsCache
		}
		if staleCache != nil {
			waiter = &freshStatsWaiter{
				cs:         cs,
				freshCache: make(chan *CollectionStats),
			}
			go waiter.waiter()
		}
	} else if !cs.statsCachePending {
		cs.statsCachePending = true
		go cs.statsBuilder()
	}
	cs.statsCacheLock.Unlock()

	if statsCache != nil {
		// return fresh-enough data
		return statsCache, nil
	}

	if staleCache == nil {
		// block forever waiting for fresh data
		cs.statsCacheLock.Lock()
		for cs.statsCache == nil {
			cs.statsCacheFresh.Wait()
		}
		statsCache = cs.statsCache
		cs.statsCacheLock.Unlock()
		return statsCache, nil
	}

	// wait for up to a second for fresh data, on timeout return stale data
	timeout := time.NewTimer(time.Second)
	defer timeout.Stop()
	select {
	case <-timeout.C:
		cs.statsCacheLock.Lock()
		waiter.l.Lock()
		waiter.obsolete = true
		waiter.l.Unlock()
		cs.statsCacheLock.Unlock()
		return staleCache, nil
	case statsCache = <-waiter.freshCache:
		return statsCache, nil
	}
}

type freshStatsWaiter struct {
	cs         *collectionServer
	l          sync.Mutex
	obsolete   bool
	freshCache chan *CollectionStats
}

func (fsw *freshStatsWaiter) waiter() {
	fsw.cs.statsCacheLock.Lock()
	defer fsw.cs.statsCacheLock.Unlock()
	fsw.cs.statsCacheFresh.Wait()
	fsw.l.Lock()
	defer fsw.l.Unlock()
	if fsw.obsolete {
		close(fsw.freshCache)
	} else {
		fsw.freshCache <- fsw.cs.statsCache
	}
}

func (cs *collectionServer) statsBuilder() {
	for {
		stats, err := cs.pcd.GetCollectionStats()
		if err == nil {
			cs.statsCacheLock.Lock()
			cs.statsCache = &stats
			cs.statsCacheWhen = time.Now()
			cs.statsCacheFresh.Broadcast()
			cs.statsCachePending = false
			cs.statsCacheLock.Unlock()
			return
		} else {
			slog.Error("GetCollectionStats", "err", err)
			time.Sleep(2 * time.Second)
		}
	}
}

// /v1/listCollections?c={}&cursor={}&limit={50<=limit<=1000}
//
// returns
// {"collections":{"app.bsky.feed.post": 123456789, "some collection": 42}, "cursor":"opaque text"}
func (cs *collectionServer) listCollections(c echo.Context) error {
	cursor := c.QueryParam("cursor")
	collections, hasQueryCollections := c.QueryParams()["c"]
	stats, err := cs.getStatsCache()
	limit := getLimit(c, 50, 500, 1000)
	if err != nil {
		slog.Error("getStatsCache", "err", err)
		return c.String(http.StatusInternalServerError, "oops")
	}
	var out ListCollectionsResponse
	if hasQueryCollections {
		out.Collections = make(map[string]uint64, len(collections))
		for _, collection := range collections {
			count, ok := stats.CollectionCounts[collection]
			if ok {
				out.Collections[collection] = count
			}
		}
	} else {
		allCollections := make([]string, 0, len(stats.CollectionCounts))
		for collection := range stats.CollectionCounts {
			allCollections = append(allCollections, collection)
		}
		sort.Strings(allCollections)
		out.Collections = make(map[string]uint64, limit)
		count := 0
		for _, collection := range allCollections {
			if (cursor == "") || (collection > cursor) {
				out.Collections[collection] = stats.CollectionCounts[collection]
				count++
				if count >= limit {
					out.Cursor = collection
				}
			}
		}
	}
	return c.JSON(http.StatusOK, out)
}

type ListCollectionsResponse struct {
	Collections map[string]uint64 `json:"collections"`
	Cursor      string            `json:"cursor"`
}
