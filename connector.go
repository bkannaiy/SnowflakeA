// Copyright (c) 2020 Snowflake Computing Inc. All right reserved.

package gosnowflake

import (
	"context"
	"database/sql/driver"
)

// snowflakeDriver is the interface for a Snowflake driver
type snowflakeDriver interface {
	Open(dsn string) (driver.Conn, error)
	OpenWithConfig(ctx context.Context, config Config) (driver.Conn, error)
}

// Connector creates connections using the Snowflake driver and config.
type Connector struct {
	driver snowflakeDriver
	cfg    Config
}

// NewConnector creates a new connector with the given driver and config.
func NewConnector(driver snowflakeDriver, config Config) Connector {
	return Connector{driver, config}
}

// Connect creates a new connection using the underlying driver.
func (t Connector) Connect(ctx context.Context) (driver.Conn, error) {
	cfg := t.cfg
	err := fillMissingConfigParameters(&cfg)
	if err != nil {
		return nil, err
	}
	return t.driver.OpenWithConfig(ctx, cfg)
}

// Driver returns the underlying driver.
func (t Connector) Driver() driver.Driver {
	return t.driver
}
