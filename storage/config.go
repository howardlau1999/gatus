package storage

import (
	"errors"
	"fmt"
	"net/url"
	"time"
)

const (
	DefaultMaximumNumberOfResults = 100
	DefaultMaximumNumberOfEvents  = 50
)

var (
	ErrSQLStorageRequiresPath          = errors.New("sql storage requires a non-empty path to be defined")
	ErrMemoryStorageDoesNotSupportPath = errors.New("memory storage does not support persistence, use sqlite if you want persistence on file")
)

// Config is the configuration for storage
type Config struct {
	// Path is the path used by the store to achieve persistence
	// If blank, persistence is disabled.
	// Note that not all Type support persistence
	Path string `yaml:"path"`

	// Type of store
	// If blank, uses the default in-memory store
	Type Type `yaml:"type"`

	// Caching is whether to enable caching.
	// This is used to drastically decrease read latency by pre-emptively caching writes
	// as they happen, also known as the write-through caching strategy.
	// Does not apply if Config.Type is not TypePostgres or TypeSQLite.
	Caching bool `yaml:"caching,omitempty"`

	// MaximumNumberOfResults is the number of results each endpoint should be able to provide
	MaximumNumberOfResults int `yaml:"maximum-number-of-results,omitempty"`

	// MaximumNumberOfEvents is the number of events each endpoint should be able to provide
	MaximumNumberOfEvents int `yaml:"maximum-number-of-events,omitempty"`

	ClickHouse *ClickHouseConfig `yaml:"clickhouse,omitempty"`
}

// ClickHouseConfig configures ClickHouse's HTTP interface (normally port 8123,
// or 8443 for HTTPS), not the native TCP interface on port 9000.
type ClickHouseConfig struct {
	Address  string        `yaml:"address"`
	Database string        `yaml:"database"`
	Username string        `yaml:"username,omitempty"`
	Password string        `yaml:"password,omitempty"`
	Timeout  time.Duration `yaml:"timeout,omitempty"`
}

func (c *ClickHouseConfig) ValidateAndSetDefaults() error {
	if c == nil {
		return errors.New("clickhouse storage requires a clickhouse configuration")
	}
	u, err := url.Parse(c.Address)
	if err != nil || u == nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
		return errors.New("clickhouse address must be an HTTP(S) server URL without credentials, path or query")
	}
	if u.Port() == "9000" || u.Port() == "9440" {
		return errors.New("clickhouse address must use the HTTP interface, not native TCP port 9000/9440")
	}
	if c.Database == "" {
		return errors.New("clickhouse database must be specified")
	}
	if c.Username == "" {
		c.Username = "default"
	}
	if c.Timeout < 0 {
		return errors.New("clickhouse timeout must be positive")
	}
	if c.Timeout == 0 {
		c.Timeout = 30 * time.Second
	}
	return nil
}

// ValidateAndSetDefaults validates the configuration and sets the default values (if applicable)
func (c *Config) ValidateAndSetDefaults() error {
	if c.Type == "" {
		c.Type = TypeMemory
	}
	if (c.Type == TypePostgres || c.Type == TypeSQLite) && len(c.Path) == 0 {
		return ErrSQLStorageRequiresPath
	}
	if c.Type == TypeMemory && len(c.Path) > 0 {
		return ErrMemoryStorageDoesNotSupportPath
	}
	if c.Type == TypeClickHouse {
		if c.Path != "" {
			return errors.New("clickhouse storage uses storage.clickhouse, not storage.path")
		}
		if c.Caching {
			return errors.New("clickhouse storage does not support storage.caching")
		}
		if err := c.ClickHouse.ValidateAndSetDefaults(); err != nil {
			return fmt.Errorf("invalid clickhouse configuration: %w", err)
		}
	}
	if c.MaximumNumberOfResults <= 0 {
		c.MaximumNumberOfResults = DefaultMaximumNumberOfResults
	}
	if c.MaximumNumberOfEvents <= 0 {
		c.MaximumNumberOfEvents = DefaultMaximumNumberOfEvents
	}
	return nil
}
