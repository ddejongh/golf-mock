// Command vpadmin runs operational tasks against the VenuePulse database:
//
//	vpadmin migrate [up|down|status]
//	vpadmin seed -scenario scenarios/stadium-ingress.yaml
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/ddejongh/venuepulse/backend-go/internal/config"
	"github.com/ddejongh/venuepulse/backend-go/internal/logging"
	"github.com/ddejongh/venuepulse/backend-go/internal/simulator"
	"github.com/ddejongh/venuepulse/backend-go/internal/store"
)

const usage = `usage:
  vpadmin migrate [up|down|status]   apply, roll back one, or list migrations
  vpadmin seed -scenario FILE        upsert the scenario's venue and zones

env: VP_DATABASE_URL, VP_LOG_LEVEL`

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg := config.LoadAdmin(os.Getenv)
	log, err := logging.New(os.Stderr, "vpadmin", cfg.LogLevel)
	if err != nil {
		fmt.Fprintf(os.Stderr, "vpadmin: %v\n", err)
		os.Exit(2)
	}

	err = run(ctx, cfg, log, os.Args[1:])
	var uerr usageError
	switch {
	case errors.Is(err, flag.ErrHelp):
		fmt.Fprintln(os.Stderr, usage)
	case errors.As(err, &uerr):
		fmt.Fprintf(os.Stderr, "vpadmin: %v\n\n%s\n", err, usage)
		os.Exit(2)
	case err != nil:
		log.Error("vpadmin failed", slog.Any("error", err))
		os.Exit(1)
	}
}

type usageError struct{ msg string }

func (e usageError) Error() string { return e.msg }

func run(ctx context.Context, cfg config.Admin, log *slog.Logger, args []string) error {
	if len(args) == 0 {
		return usageError{"missing command"}
	}
	switch args[0] {
	case "migrate":
		return migrate(ctx, cfg, log, args[1:])
	case "seed":
		return seed(ctx, cfg, log, args[1:])
	case "-h", "--help", "help":
		return flag.ErrHelp
	default:
		return usageError{fmt.Sprintf("unknown command %q", args[0])}
	}
}

func migrate(ctx context.Context, cfg config.Admin, log *slog.Logger, args []string) error {
	action := "up"
	if len(args) > 0 {
		action = args[0]
	}
	if len(args) > 1 {
		return usageError{fmt.Sprintf("unexpected arguments: %v", args[1:])}
	}
	if action != "up" && action != "down" && action != "status" {
		return usageError{fmt.Sprintf("unknown migrate action %q", action)}
	}

	st, err := store.Open(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer st.Close()
	m, err := st.Migrator()
	if err != nil {
		return err
	}
	defer func() { _ = m.Close() }()

	switch action {
	case "up":
		results, err := m.Up(ctx)
		for _, r := range results {
			log.Info("migration applied", slog.String("source", r.Source.Path), slog.String("duration", r.Duration.String()))
		}
		if err != nil {
			return err
		}
		if len(results) == 0 {
			log.Info("database is up to date")
		}
	case "down":
		r, err := m.Down(ctx)
		if err != nil {
			return err
		}
		log.Info("migration rolled back", slog.String("source", r.Source.Path))
	case "status":
		statuses, err := m.Status(ctx)
		if err != nil {
			return err
		}
		for _, s := range statuses {
			fmt.Printf("%-8s %s\n", s.State, s.Source.Path)
		}
	}
	return nil
}

func seed(ctx context.Context, cfg config.Admin, log *slog.Logger, args []string) error {
	fs := flag.NewFlagSet("seed", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	path := fs.String("scenario", "", "scenario YAML whose venue and zones to upsert")
	if err := fs.Parse(args); err != nil {
		return fmt.Errorf("seed flags: %w", err)
	}
	if *path == "" {
		return usageError{"seed: -scenario is required"}
	}

	sc, err := simulator.LoadScenario(*path)
	if err != nil {
		return err
	}
	st, err := store.Open(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer st.Close()

	if err := st.UpsertVenue(ctx, venueSpec(sc)); err != nil {
		return err
	}
	log.Info("venue seeded", slog.String("venue_id", sc.Venue.ID), slog.Int("zones", len(sc.Zones)))
	return nil
}

func venueSpec(sc *simulator.Scenario) store.VenueSpec {
	v := store.VenueSpec{ID: sc.Venue.ID, Name: sc.Venue.Name, Kind: sc.Venue.Kind, Timezone: sc.Venue.Timezone}
	if v.Name == "" {
		v.Name = v.ID
	}
	for _, z := range sc.Zones {
		v.Zones = append(v.Zones, store.ZoneSpec{ID: z.ID, Name: z.Name, Kind: z.Kind, Capacity: z.Capacity})
	}
	return v
}
