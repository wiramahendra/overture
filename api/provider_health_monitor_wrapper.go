//go:build ignore
// +build ignore

// Archived: scheduler provider health monitor wrapper (attic/scheduler). Not in wedge.

// Package api provides wrappers for scheduler components
package api

import (
	"database/sql"

	"github.com/wiramahendra/overture/scheduler"
	"github.com/wiramahendra/overture/security"
)

// NewProviderHealthMonitor creates a new provider health monitor
func NewProviderHealthMonitor(db *sql.DB, keyVault *security.KeyVault, config *scheduler.ProviderHealthMonitorConfig) *scheduler.ProviderHealthMonitor {
	return scheduler.NewProviderHealthMonitor(db, keyVault, config)
}
