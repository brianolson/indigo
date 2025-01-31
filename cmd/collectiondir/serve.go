package main

import (
	"context"
	"fmt"
	"github.com/bluesky-social/indigo/xrpc"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/urfave/cli/v2"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
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
			EnvVars: []string{"COLLECTIONS_API_LISTEN"},
		},
		&cli.StringFlag{
			Name:    "metrics-listen",
			Value:   ":2511",
			EnvVars: []string{"COLLECTIONS_METRICS_LISTEN"},
		},
		&cli.StringFlag{
			Name:     "pebble",
			Usage:    "path to store pebble db",
			Required: true,
		},
		&cli.StringFlag{
			Name:    "admin-token",
			Usage:   "admin authentication",
			EnvVars: []string{"COLLECTIONS_ADMIN_TOKEN"},
		},
		&cli.Float64Flag{
			Name:  "crawl-qps",
			Usage: "per-PDS crawl queries-per-second limit",
			Value: 100,
		},
		&cli.BoolFlag{
			Name: "verbose",
		},
	},
	Action: func(cctx *cli.Context) error {
		var server collectionServer
		return server.run(cctx)
	},
}

type collectionServer struct {
	pcd *PebbleCollectionDirectory
	ctx context.Context

	statsCache        *CollectionStats
	statsCacheWhen    time.Time
	statsCacheLock    sync.Mutex
	statsCacheFresh   sync.Cond
	statsCachePending bool

	ingest chan DidCollection

	log *slog.Logger

	AdminToken         string
	ExepctedAuthHeader string
	PerPDSCrawlQPS     float64

	activeCrawlHosts map[string]time.Time
	activeCrawlsLock sync.Mutex
}

const defaultPerPDSCrawlQPS = 100

func (cs *collectionServer) run(cctx *cli.Context) error {
	level := slog.LevelInfo
	if cctx.Bool("verbose") {
		level = slog.LevelDebug
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))
	cs.log = log
	cs.ctx = cctx.Context
	cs.AdminToken = cctx.String("admin-token")
	cs.ExepctedAuthHeader = "Bearer " + cs.AdminToken
	pebblePath := cctx.String("pebble")
	cs.pcd = &PebbleCollectionDirectory{}
	err := cs.pcd.Open(pebblePath)
	if err != nil {
		return fmt.Errorf("%s: failed to open pebble db: %w", pebblePath, err)
	}
	cs.statsCacheFresh.L = &cs.statsCacheLock
	errchan := make(chan error, 3)
	apiAddr := cctx.String("api-listen")
	go func() {
		errchan <- cs.StartApiServer(cctx.Context, apiAddr)
	}()
	// TODO run metrics
	return <-errchan
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

	// admin auth heador required
	e.POST("/v1/crawlRequest", cs.crawlPds)
	e.GET("/v1/crawlStatus", cs.crawlStatus)

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

func (cs *collectionServer) ingestReceiver() {
	cs.pcd.SetFromResults(cs.ingest)
}

type CrawlRequest struct {
	Host  string   `json:"host,omitempty"`
	Hosts []string `json:"hosts,omitempty"`
}

type CrawlRequestResponse struct {
	Message string `json:"message,omitempty"`
	Error   string `json:"error,omitempty"`
}

func hostOrUrlToUrl(host string) string {
	xu, err := url.Parse(host)
	if err != nil {
		xu = new(url.URL)
		xu.Host = host
		xu.Scheme = "https"
		return xu.String()
	} else if xu.Scheme == "" {
		xu.Scheme = "https"
		return xu.String()
	}
	return host
}

// /v1/crawlRequest
// requires header `Authorization: Bearer {admin token}`
//
// POST {"host":"one hostname or URL", "hosts":["up to 1000 hosts", "..."]}
// OR
// POST /v1/crawlRequest?host={one host}
func (cs *collectionServer) crawlPds(c echo.Context) error {
	authHeader := c.Request().Header.Get("Authorization")
	if authHeader != cs.ExepctedAuthHeader {
		return c.JSON(http.StatusForbidden, CrawlRequestResponse{Error: "nope"})
	}
	hostQ := c.QueryParam("host")
	if hostQ != "" {
		go cs.crawlThread(hostQ)
		return c.JSON(http.StatusOK, CrawlRequestResponse{Message: "ok"})
	}

	var req CrawlRequest
	err := c.Bind(&req)
	if err != nil {
		return c.String(http.StatusBadRequest, err.Error())
	}
	if req.Host != "" {
		go cs.crawlThread(req.Host)
	}
	for _, host := range req.Hosts {
		go cs.crawlThread(host)
	}
	return c.JSON(http.StatusOK, CrawlRequestResponse{Message: "ok"})
}

func (cs *collectionServer) crawlThread(hostIn string) {
	host := hostOrUrlToUrl(hostIn)
	if host != hostIn {
		cs.log.Info("going to crawl", "in", hostIn, "as", host)
	}
	httpClient := http.Client{}
	rpcClient := xrpc.Client{
		Host:   host,
		Client: &httpClient,
	}
	crawler := Crawler{
		Ctx:       cs.ctx,
		RpcClient: &rpcClient,
		QPS:       cs.PerPDSCrawlQPS,
		Results:   cs.ingest,
		Log:       cs.log,
	}
	start := time.Now()
	cs.recordCrawlStart(host, start)
	cs.log.Info("crawling", "host", host)
	err := crawler.CrawlPDSRepoCollections()
	cs.clearActiveCrawl(host)
	if err != nil {
		cs.log.Warn("crawl err", "host", host, "err", err)
	} else {
		dt := time.Since(start)
		cs.log.Info("crawl done", "host", host, "dt", dt)
	}
}

func (cs *collectionServer) recordCrawlStart(host string, start time.Time) {
	cs.activeCrawlsLock.Lock()
	defer cs.activeCrawlsLock.Unlock()
	if cs.activeCrawlHosts == nil {
		cs.activeCrawlHosts = make(map[string]time.Time)
	}
	cs.activeCrawlHosts[host] = start
}

func (cs *collectionServer) clearActiveCrawl(host string) {
	cs.activeCrawlsLock.Lock()
	defer cs.activeCrawlsLock.Unlock()
	if cs.activeCrawlHosts == nil {
		return
	}
	delete(cs.activeCrawlHosts, host)
}

type CrawlStatusResponse struct {
	HostStarts map[string]string `json:"host_starts"`
}

// GET /v1/crawlStatus
func (cs *collectionServer) crawlStatus(c echo.Context) error {
	authHeader := c.Request().Header.Get("Authorization")
	if authHeader != cs.ExepctedAuthHeader {
		return c.JSON(http.StatusForbidden, CrawlRequestResponse{Error: "nope"})
	}
	var out CrawlStatusResponse
	out.HostStarts = make(map[string]string)
	cs.activeCrawlsLock.Lock()
	defer cs.activeCrawlsLock.Unlock()
	for host, start := range cs.activeCrawlHosts {
		out.HostStarts[host] = start.UTC().Format(time.RFC3339)
	}
	return c.JSON(http.StatusOK, out)
}
