// gatus-migrate-clickhouse imports an immutable Gatus SQLite snapshot into a
// dedicated ClickHouse database. It defaults to a local, read-only dry run.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/TwiN/gatus/v5/storage"
	"github.com/TwiN/gatus/v5/storage/store/clickhouse"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := run(ctx, os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, "Migration failed:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, out, stderr io.Writer) error {
	flags := flag.NewFlagSet("gatus-migrate-clickhouse", flag.ContinueOnError)
	flags.SetOutput(stderr)
	sourcePath := flags.String("sqlite", "", "SQLite snapshot path (opened read-only)")
	apply := flags.Bool("apply", false, "write to ClickHouse; without this flag only validate the SQLite source")
	dryRun := flags.Bool("dry-run", false, "explicitly validate locally without contacting ClickHouse (default behavior)")
	batchSize := flags.Int("batch-size", 1000, "maximum source rows per insert batch")
	address := flags.String("address", envOr("GATUS_CLICKHOUSE_ADDRESS", ""), "ClickHouse HTTP(S) URL (or GATUS_CLICKHOUSE_ADDRESS)")
	database := flags.String("database", envOr("GATUS_CLICKHOUSE_DATABASE", ""), "existing dedicated ClickHouse database (or GATUS_CLICKHOUSE_DATABASE; no default)")
	username := flags.String("username", envOr("GATUS_CLICKHOUSE_USERNAME", "default"), "ClickHouse username (or GATUS_CLICKHOUSE_USERNAME)")
	passwordEnv := flags.String("password-env", "GATUS_CLICKHOUSE_PASSWORD", "environment variable containing the ClickHouse password")
	timeout := flags.Duration("timeout", 30*time.Second, "timeout per ClickHouse HTTP request")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("unexpected positional arguments")
	}
	if *sourcePath == "" {
		return errors.New("--sqlite is required")
	}
	if *apply && *dryRun {
		return errors.New("--apply and --dry-run cannot be combined")
	}
	if *batchSize < 1 || *batchSize > 100000 {
		return errors.New("--batch-size must be between 1 and 100000")
	}
	source, err := openSource(ctx, *sourcePath)
	if err != nil {
		return err
	}
	defer source.close()
	migration, err := inspect(ctx, source, *batchSize)
	if err != nil {
		return fmt.Errorf("validate source: %w", err)
	}
	migration.printSummary(out)
	if !*apply {
		fmt.Fprintln(out, "Dry run complete. SQLite was read-only; ClickHouse was not contacted. Use --apply to import this snapshot.")
		return nil
	}
	if *passwordEnv == "" {
		return errors.New("--password-env cannot be empty")
	}
	cfg := &storage.ClickHouseConfig{Address: *address, Database: *database, Username: *username, Password: os.Getenv(*passwordEnv), Timeout: *timeout}
	if err = cfg.ValidateAndSetDefaults(); err != nil {
		return err
	}
	target, err := clickhouse.NewStore(cfg, storage.DefaultMaximumNumberOfResults, storage.DefaultMaximumNumberOfEvents)
	if err != nil {
		return fmt.Errorf("open ClickHouse target: %w", err)
	}
	defer target.Close()
	if err = migration.apply(ctx, source, target, *batchSize, out); err != nil {
		return err
	}
	fmt.Fprintln(out, "Import complete. Repeating this command with the same immutable snapshot is safe before Gatus starts using the target.")
	return nil
}

func envOr(name, fallback string) string {
	if value, ok := os.LookupEnv(name); ok {
		return value
	}
	return fallback
}
