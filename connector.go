// Copyright (c) 2020 Snowflake Computing Inc. All right reserved.

package gosnowflake

import (
	"context"
	sqlDriver "database/sql/driver"
)

// driver is the interface for a Snowflake driver
type driver interface {
	Open(dsn string) (sqlDriver.Conn, error)
	OpenWithConfig(ctx context.Context, config Config) (sqlDriver.Conn, error)
}

// Connector creates connections using the Snowflake driver and config.
type Connector struct {
	driver driver
	cfg    Config
}

// NewConnector creates a new connector with the given driver and config.
func NewConnector(driver driver, config Config) Connector {
	return Connector{driver, config}
}

// Connect creates a new connection using the underlying driver.
func (t Connector) Connect(ctx context.Context) (sqlDriver.Conn, error) {
	cfg := t.cfg
	err := fillMissingConfigParameters(&cfg)
	if err != nil {
		return nil, err
	}
	return t.driver.OpenWithConfig(ctx, cfg)
}

// Driver returns the underlying driver.
func (t Connector) Driver() sqlDriver.Driver {
	return t.driver
}
