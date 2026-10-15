package storage

import (
	"testing"
	"time"
)

func TestClickHouseConfig(t *testing.T) {
	t.Parallel()
	for _, address := range []string{"", "localhost:8123", "clickhouse://localhost:9000", "http://localhost:9000", "https://localhost:9440", "http://user:secret@localhost:8123", "http://localhost:8123/path", "http://localhost:8123?password=secret", "http://localhost:8123/#fragment"} {
		t.Run(address, func(t *testing.T) {
			cfg := &Config{Type: TypeClickHouse, ClickHouse: &ClickHouseConfig{Address: address, Database: "gatus"}}
			if cfg.ValidateAndSetDefaults() == nil {
				t.Fatal("accepted invalid address")
			}
		})
	}
	for _, address := range []string{"http://localhost:8123", "https://localhost:8443/", "http://[::1]:8123"} {
		cfg := &Config{Type: TypeClickHouse, ClickHouse: &ClickHouseConfig{Address: address, Database: "gatus"}}
		if err := cfg.ValidateAndSetDefaults(); err != nil {
			t.Fatal(err)
		}
		if cfg.ClickHouse.Username != "default" || cfg.ClickHouse.Timeout != 30*time.Second || cfg.MaximumNumberOfResults != 100 {
			t.Fatalf("defaults missing: %+v", cfg)
		}
	}
	for _, cfg := range []*Config{
		{Type: TypeClickHouse},
		{Type: TypeClickHouse, ClickHouse: &ClickHouseConfig{Address: "http://localhost:8123"}},
		{Type: TypeClickHouse, ClickHouse: &ClickHouseConfig{Address: "http://localhost:8123", Database: "gatus", Timeout: -time.Second}},
		{Type: TypeClickHouse, Path: "ignored", ClickHouse: &ClickHouseConfig{Address: "http://localhost:8123", Database: "gatus"}},
		{Type: TypeClickHouse, Caching: true, ClickHouse: &ClickHouseConfig{Address: "http://localhost:8123", Database: "gatus"}},
	} {
		if cfg.ValidateAndSetDefaults() == nil {
			t.Fatalf("accepted invalid configuration: %+v", cfg)
		}
	}
}
