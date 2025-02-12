package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	_ "net/http/pprof"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/bluesky-social/indigo/api"
	libbgs "github.com/bluesky-social/indigo/cmd/medsky/bgs"
	"github.com/bluesky-social/indigo/cmd/medsky/repomgr"
	"github.com/bluesky-social/indigo/did"
	"github.com/bluesky-social/indigo/events"
	"github.com/bluesky-social/indigo/plc"
	"github.com/bluesky-social/indigo/util"
	"github.com/bluesky-social/indigo/util/cliutil"
	"github.com/bluesky-social/indigo/xrpc"

	_ "github.com/joho/godotenv/autoload"
	_ "go.uber.org/automaxprocs"

	"github.com/carlmjohnson/versioninfo"
	"github.com/urfave/cli/v2"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/jaeger"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
	"gorm.io/plugin/opentelemetry/tracing"
)

var log = slog.Default().With("system", "bigsky")

func init() {
	// control log level using, eg, GOLOG_LOG_LEVEL=debug
	//logging.SetAllLoggers(logging.LevelDebug)
}

func main() {
	if err := run(os.Args); err != nil {
		slog.Error(err.Error())
		os.Exit(1)
	}
}

func run(args []string) error {

	app := cli.App{
		Name:    "bigsky",
		Usage:   "atproto Relay daemon",
		Version: versioninfo.Short(),
	}

	app.Flags = []cli.Flag{
		&cli.BoolFlag{
			Name: "jaeger",
		},
		&cli.StringFlag{
			Name:    "db-url",
			Usage:   "database connection string for BGS database",
			Value:   "sqlite://./data/bigsky/bgs.sqlite",
			EnvVars: []string{"DATABASE_URL"},
		},
		&cli.BoolFlag{
			Name: "db-tracing",
		},
		&cli.StringFlag{
			Name:    "data-dir",
			Usage:   "path of directory for CAR files and other data",
			Value:   "data/bigsky",
			EnvVars: []string{"RELAY_DATA_DIR", "DATA_DIR"},
		},
		&cli.StringFlag{
			Name:    "plc-host",
			Usage:   "method, hostname, and port of PLC registry",
			Value:   "https://plc.directory",
			EnvVars: []string{"ATP_PLC_HOST"},
		},
		&cli.BoolFlag{
			Name:  "crawl-insecure-ws",
			Usage: "when connecting to PDS instances, use ws:// instead of wss://",
		},
		&cli.StringFlag{
			Name:  "api-listen",
			Value: ":2470",
		},
		&cli.StringFlag{
			Name:    "metrics-listen",
			Value:   ":2471",
			EnvVars: []string{"RELAY_METRICS_LISTEN", "BGS_METRICS_LISTEN"},
		},
		&cli.StringFlag{
			Name:     "disk-persister-dir",
			Usage:    "set directory for disk persister (implicitly enables disk persister)",
			Required: true,
			EnvVars:  []string{"RELAY_PERSISTER_DIR"},
		},
		&cli.StringFlag{
			Name:    "admin-key",
			EnvVars: []string{"RELAY_ADMIN_KEY", "BGS_ADMIN_KEY"},
		},
		&cli.StringSliceFlag{
			Name:    "handle-resolver-hosts",
			EnvVars: []string{"HANDLE_RESOLVER_HOSTS"},
		},
		&cli.IntFlag{
			Name:    "max-metadb-connections",
			EnvVars: []string{"MAX_METADB_CONNECTIONS"},
			Value:   40,
		},
		&cli.StringFlag{
			Name:    "resolve-address",
			EnvVars: []string{"RESOLVE_ADDRESS"},
			Value:   "1.1.1.1:53",
		},
		&cli.BoolFlag{
			Name:    "force-dns-udp",
			EnvVars: []string{"FORCE_DNS_UDP"},
		},
		&cli.IntFlag{
			Name:    "max-fetch-concurrency",
			Value:   100,
			EnvVars: []string{"MAX_FETCH_CONCURRENCY"},
		},
		&cli.StringFlag{
			Name:    "env",
			Value:   "dev",
			EnvVars: []string{"ENVIRONMENT"},
			Usage:   "declared hosting environment (prod, qa, etc); used in metrics",
		},
		&cli.StringFlag{
			Name:    "otel-exporter-otlp-endpoint",
			EnvVars: []string{"OTEL_EXPORTER_OTLP_ENDPOINT"},
		},
		&cli.StringFlag{
			Name:    "bsky-social-rate-limit-skip",
			EnvVars: []string{"BSKY_SOCIAL_RATE_LIMIT_SKIP"},
			Usage:   "ratelimit bypass secret token for *.bsky.social domains",
		},
		&cli.IntFlag{
			Name:    "default-repo-limit",
			Value:   100,
			EnvVars: []string{"RELAY_DEFAULT_REPO_LIMIT"},
		},
		&cli.IntFlag{
			Name:    "concurrency-per-pds",
			EnvVars: []string{"RELAY_CONCURRENCY_PER_PDS"},
			Value:   100,
		},
		&cli.IntFlag{
			Name:    "max-queue-per-pds",
			EnvVars: []string{"RELAY_MAX_QUEUE_PER_PDS"},
			Value:   1_000,
		},
		&cli.IntFlag{
			Name:    "did-cache-size",
			Usage:   "in-process cache by number of Did documents. see also did-memcached",
			EnvVars: []string{"RELAY_DID_CACHE_SIZE"},
			Value:   5_000_000,
		},
		&cli.StringSliceFlag{
			Name:    "did-memcached",
			Usage:   "nearby memcached which is storing did documents. did-cache-size can be smaller if we have memcached",
			EnvVars: []string{"RELAY_DID_MEMCACHED"},
		},
		&cli.DurationFlag{
			Name:    "event-playback-ttl",
			Usage:   "time to live for event playback buffering (only applies to disk persister)",
			EnvVars: []string{"RELAY_EVENT_PLAYBACK_TTL"},
			Value:   72 * time.Hour,
		},
		&cli.StringSliceFlag{
			Name:    "next-crawler",
			Usage:   "forward POST requestCrawl to this url, should be machine root url and not xrpc/requestCrawl, comma separated list",
			EnvVars: []string{"RELAY_NEXT_CRAWLER"},
		},
	}

	app.Action = runBigsky
	return app.Run(os.Args)
}

func setupOTEL(cctx *cli.Context) error {

	env := cctx.String("env")
	if env == "" {
		env = "dev"
	}
	if cctx.Bool("jaeger") {
		jaegerUrl := "http://localhost:14268/api/traces"
		exp, err := jaeger.New(jaeger.WithCollectorEndpoint(jaeger.WithEndpoint(jaegerUrl)))
		if err != nil {
			return err
		}
		tp := tracesdk.NewTracerProvider(
			// Always be sure to batch in production.
			tracesdk.WithBatcher(exp),
			// Record information about this application in a Resource.
			tracesdk.WithResource(resource.NewWithAttributes(
				semconv.SchemaURL,
				semconv.ServiceNameKey.String("bgs"),
				attribute.String("env", env),         // DataDog
				attribute.String("environment", env), // Others
				attribute.Int64("ID", 1),
			)),
		)

		otel.SetTracerProvider(tp)
	}

	// Enable OTLP HTTP exporter
	// For relevant environment variables:
	// https://pkg.go.dev/go.opentelemetry.io/otel/exporters/otlp/otlptrace#readme-environment-variables
	// At a minimum, you need to set
	// OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318
	if ep := cctx.String("otel-exporter-otlp-endpoint"); ep != "" {
		slog.Info("setting up trace exporter", "endpoint", ep)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		exp, err := otlptracehttp.New(ctx)
		if err != nil {
			slog.Error("failed to create trace exporter", "error", err)
			os.Exit(1)
		}
		defer func() {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if err := exp.Shutdown(ctx); err != nil {
				slog.Error("failed to shutdown trace exporter", "error", err)
			}
		}()

		tp := tracesdk.NewTracerProvider(
			tracesdk.WithBatcher(exp),
			tracesdk.WithResource(resource.NewWithAttributes(
				semconv.SchemaURL,
				semconv.ServiceNameKey.String("bgs"),
				attribute.String("env", env),         // DataDog
				attribute.String("environment", env), // Others
				attribute.Int64("ID", 1),
			)),
		)
		otel.SetTracerProvider(tp)
	}

	return nil
}

func runBigsky(cctx *cli.Context) error {
	// Trap SIGINT to trigger a shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	_, err := cliutil.SetupSlog(cliutil.LogOptions{})
	if err != nil {
		return err
	}

	// start observability/tracing (OTEL and jaeger)
	if err := setupOTEL(cctx); err != nil {
		return err
	}

	// ensure data directory exists; won't error if it does
	datadir := cctx.String("data-dir")
	if err := os.MkdirAll(datadir, os.ModePerm); err != nil {
		return err
	}

	dburl := cctx.String("db-url")
	slog.Info("setting up main database", "url", dburl)
	db, err := cliutil.SetupDatabase(dburl, cctx.Int("max-metadb-connections"))
	if err != nil {
		return err
	}
	if cctx.Bool("db-tracing") {
		if err := db.Use(tracing.NewPlugin()); err != nil {
			return err
		}
	}

	if err != nil {
		return err
	}

	// DID RESOLUTION
	// 1. the outside world, PLCSerever or Web
	// 2. (maybe memcached)
	// 3. in-process cache
	var cachedidr did.Resolver
	{
		mr := did.NewMultiResolver()

		didr := &api.PLCServer{Host: cctx.String("plc-host")}
		mr.AddHandler("plc", didr)

		webr := did.WebResolver{}
		if cctx.Bool("crawl-insecure-ws") {
			webr.Insecure = true
		}
		mr.AddHandler("web", &webr)

		var prevResolver did.Resolver
		memcachedServers := cctx.StringSlice("did-memcached")
		if len(memcachedServers) > 0 {
			prevResolver = plc.NewMemcachedDidResolver(mr, time.Hour*24, memcachedServers)
		} else {
			prevResolver = mr
		}

		cachedidr = plc.NewCachingDidResolver(prevResolver, time.Hour*24, cctx.Int("did-cache-size"))
	}

	kmgr := repomgr.NewKeyManager(cachedidr)

	repoman := repomgr.NewRepoManager(kmgr)

	var persister events.EventPersistence

	dpd := cctx.String("disk-persister-dir")
	if dpd == "" {
		slog.Info("empty disk-persister-dir, use current working directory")
		cwd, err := os.Getwd()
		if err != nil {
			return err
		}
		dpd = filepath.Join(cwd, "medsky-persist")
	}
	slog.Info("setting up disk persister", "dir", dpd)

	pOpts := events.DefaultDiskPersistOptions()
	pOpts.Retention = cctx.Duration("event-playback-ttl")
	dp, err := events.NewDiskPersistence(dpd, "", db, pOpts)
	if err != nil {
		return fmt.Errorf("setting up disk persister: %w", err)
	}
	persister = dp

	evtman := events.NewEventManager(persister)

	rlskip := cctx.String("bsky-social-rate-limit-skip")
	// TODO: pass this into bgs somehow -- bolson 2025
	applyPDSClientSettings := func(c *xrpc.Client) {
		if c.Client == nil {
			c.Client = util.RobustHTTPClient()
		}
		if strings.HasSuffix(c.Host, ".bsky.network") {
			c.Client.Timeout = time.Minute * 30
			if rlskip != "" {
				c.Headers = map[string]string{
					"x-ratelimit-bypass": rlskip,
				}
			}
		} else {
			// Generic PDS timeout
			c.Client.Timeout = time.Minute * 1
		}
	}

	repoman.SetEventManager(evtman)

	prodHR, err := api.NewProdHandleResolver(100_000, cctx.String("resolve-address"), cctx.Bool("force-dns-udp"))
	if err != nil {
		return fmt.Errorf("failed to set up handle resolver: %w", err)
	}
	if rlskip != "" {
		prodHR.ReqMod = func(req *http.Request, host string) error {
			if strings.HasSuffix(host, ".bsky.social") {
				req.Header.Set("x-ratelimit-bypass", rlskip)
			}
			return nil
		}
	}

	var hr api.HandleResolver = prodHR
	if cctx.StringSlice("handle-resolver-hosts") != nil {
		hr = &api.TestHandleResolver{
			TrialHosts: cctx.StringSlice("handle-resolver-hosts"),
		}
	}

	slog.Info("constructing bgs")
	bgsConfig := libbgs.DefaultBGSConfig()
	bgsConfig.SSL = !cctx.Bool("crawl-insecure-ws")
	bgsConfig.ConcurrencyPerPDS = cctx.Int64("concurrency-per-pds")
	bgsConfig.MaxQueuePerPDS = cctx.Int64("max-queue-per-pds")
	bgsConfig.DefaultRepoLimit = cctx.Int64("default-repo-limit")
	bgsConfig.ApplyPDSClientSettings = applyPDSClientSettings
	nextCrawlers := cctx.StringSlice("next-crawler")
	if len(nextCrawlers) != 0 {
		nextCrawlerUrls := make([]*url.URL, len(nextCrawlers))
		for i, tu := range nextCrawlers {
			var err error
			nextCrawlerUrls[i], err = url.Parse(tu)
			if err != nil {
				return fmt.Errorf("failed to parse next-crawler url: %w", err)
			}
			slog.Info("configuring relay for requestCrawl", "host", nextCrawlerUrls[i])
		}
		bgsConfig.NextCrawlers = nextCrawlerUrls
	}
	bgs, err := libbgs.NewBGS(db, repoman, evtman, cachedidr, hr, bgsConfig)
	if err != nil {
		return err
	}

	if tok := cctx.String("admin-key"); tok != "" {
		if err := bgs.CreateAdminToken(tok); err != nil {
			return fmt.Errorf("failed to set up admin token: %w", err)
		}
	}

	// set up metrics endpoint
	go func() {
		if err := bgs.StartMetrics(cctx.String("metrics-listen")); err != nil {
			log.Error("failed to start metrics endpoint", "err", err)
			os.Exit(1)
		}
	}()

	bgsErr := make(chan error, 1)

	go func() {
		err := bgs.Start(cctx.String("api-listen"))
		bgsErr <- err
	}()

	slog.Info("startup complete")
	select {
	case <-signals:
		log.Info("received shutdown signal")
		errs := bgs.Shutdown()
		for err := range errs {
			slog.Error("error during BGS shutdown", "err", err)
		}
	case err := <-bgsErr:
		if err != nil {
			slog.Error("error during BGS startup", "err", err)
		}
		log.Info("shutting down")
		errs := bgs.Shutdown()
		for err := range errs {
			slog.Error("error during BGS shutdown", "err", err)
		}
	}

	log.Info("shutdown complete")

	return nil
}
