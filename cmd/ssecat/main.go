package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/flaviomartins/ssecat/pkg/client"
	"github.com/flaviomartins/ssecat/pkg/config"
	"github.com/flaviomartins/ssecat/pkg/output"
	"github.com/flaviomartins/ssecat/pkg/state"
	"github.com/flaviomartins/ssecat/pkg/version"
)

type headerFlags []string

func (h *headerFlags) String() string {
	return strings.Join(*h, ",")
}

func (h *headerFlags) Set(value string) error {
	*h = append(*h, value)
	return nil
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "ssecat: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	cfgPath, err := config.DefaultPath()
	if err != nil {
		return fmt.Errorf("resolve config path: %w", err)
	}

	fs := flag.NewFlagSet("ssecat", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	var (
		configPath = fs.String("config", cfgPath, "path to config file")
		stateDir   = fs.String("state-dir", "", "override state directory")
		noResume   = fs.Bool("no-resume", false, "disable Last-Event-ID resume")
		showVer    = fs.Bool("version", false, "show version")
		headers    headerFlags
	)
	fs.Var(&headers, "header", "extra request header in 'Name: Value' form (repeatable)")

	fs.Usage = func() {
		fmt.Fprintf(fs.Output(), "Usage: %s [flags] URL\n\n", fs.Name())
		fs.PrintDefaults()
	}

	if err := fs.Parse(os.Args[1:]); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}

	if *showVer {
		fmt.Fprintln(os.Stdout, version.String())
		return nil
	}

	if fs.NArg() != 1 {
		fs.Usage()
		return errors.New("exactly one URL argument is required")
	}

	url := fs.Arg(0)
	if _, err := config.ValidateURL(url); err != nil {
		return err
	}

	fileCfg, err := config.Load(*configPath)
	if err != nil {
		return err
	}

	retryDelay := fileCfg.RetryDelay
	if retryDelay <= 0 {
		retryDelay = 3 * time.Second
	}

	resume := fileCfg.Resume && !*noResume

	hdr := http.Header{}
	if fileCfg.Accept != "" {
		hdr.Set("Accept", fileCfg.Accept)
	}
	if fileCfg.UserAgent != "" {
		hdr.Set("User-Agent", fileCfg.UserAgent)
	}
	for _, raw := range headers {
		k, v, ok := strings.Cut(raw, ":")
		if !ok {
			return fmt.Errorf("invalid --header value %q, expected 'Name: Value'", raw)
		}
		name := strings.TrimSpace(k)
		value := strings.TrimSpace(v)
		if name == "" {
			return fmt.Errorf("invalid --header value %q: empty name", raw)
		}
		hdr.Add(name, value)
	}

	var st *state.Store
	if resume {
		st, err = state.New(*stateDir)
		if err != nil {
			return err
		}
	}

	httpClient := &http.Client{Timeout: 60 * time.Second}
	writer := output.NewPayloadWriter(os.Stdout)

	c := client.New(client.Options{
		URL:          url,
		HTTPClient:   httpClient,
		Headers:      hdr,
		Writer:       writer,
		State:        st,
		Retry:        fileCfg.Retry,
		InitialRetry: retryDelay,
	})

	ctx, stop := signalContext(context.Background())
	defer stop()

	if err := c.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	return nil
}
