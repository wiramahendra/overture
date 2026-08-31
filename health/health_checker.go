package health

import (
	"github.com/wiramahendra/overture/internal"

	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// HealthStatus represents the health status of a component
type HealthStatus string

const (
	StatusHealthy   HealthStatus = "healthy"
	StatusDegraded  HealthStatus = "degraded"
	StatusUnhealthy HealthStatus = "unhealthy"
)

// ComponentHealth represents the health of a single component
type ComponentHealth struct {
	Status      HealthStatus       `json:"status"`
	Message     string             `json:"message,omitempty"`
	Latency     string             `json:"latency,omitempty"`
	LastChecked time.Time          `json:"last_checked"`
	Details     map[string]interface{} `json:"details,omitempty"`
}

// OverallHealth represents the overall system health
type OverallHealth struct {
	Status     HealthStatus               `json:"status"`
	Version    string                     `json:"version"`
	Uptime     string                     `json:"uptime"`
	Timestamp  time.Time                  `json:"timestamp"`
	Components map[string]ComponentHealth `json:"components"`
}

// HealthChecker provides health check functionality
type HealthChecker struct {
	db          *sql.DB
	redis       *redis.Client
	startTime   time.Time
	version     string
	dbEnabled   bool
	redisEnabled bool
}

// NewHealthChecker creates a new health checker
func NewHealthChecker(version string) *HealthChecker {
	return &HealthChecker{
		startTime: time.Now(),
		version:   version,
	}
}

// SetDatabase configures database for health checks
func (hc *HealthChecker) SetDatabase(db *sql.DB, enabled bool) {
	hc.db = db
	hc.dbEnabled = enabled
}

// SetRedis configures Redis for health checks
func (hc *HealthChecker) SetRedis(client *redis.Client, enabled bool) {
	hc.redis = client
	hc.redisEnabled = enabled
}

// CheckHealth performs all health checks and returns overall status
func (hc *HealthChecker) CheckHealth(ctx context.Context) *OverallHealth {
	health := &OverallHealth{
		Status:     StatusHealthy,
		Version:    hc.version,
		Uptime:     time.Since(hc.startTime).String(),
		Timestamp:  time.Now(),
		Components: make(map[string]ComponentHealth),
	}

	// Check database
	if hc.dbEnabled && hc.db != nil {
		health.Components["database"] = hc.checkDatabase(ctx)
		health.Components["migrations"] = hc.checkMigrations(ctx)
		health.Components["signing_keys"] = hc.checkSigningKeys(ctx)
	}

	// Check Redis
	if hc.redisEnabled && hc.redis != nil {
		health.Components["redis"] = hc.checkRedis(ctx)
	}

	// Always check memory and basic health
	health.Components["application"] = hc.checkApplication()

	// Determine overall status
	for _, component := range health.Components {
		if component.Status == StatusUnhealthy {
			health.Status = StatusUnhealthy
			break
		} else if component.Status == StatusDegraded && health.Status == StatusHealthy {
			health.Status = StatusDegraded
		}
	}

	return health
}

// CheckLiveness performs a basic liveness check
func (hc *HealthChecker) CheckLiveness() bool {
	// Liveness: Is the application running?
	// This is always true if we can execute this code
	return true
}

// CheckReadiness performs a readiness check
func (hc *HealthChecker) CheckReadiness(ctx context.Context) bool {
	// Readiness: Can the application serve traffic?
	// Check critical dependencies

	// Check database if enabled
	if hc.dbEnabled && hc.db != nil {
		if err := hc.db.PingContext(ctx); err != nil {
			return false
		}
	}

	// Check Redis if enabled
	if hc.redisEnabled && hc.redis != nil {
		if err := hc.redis.Ping(ctx).Err(); err != nil {
			return false
		}
	}

	return true
}

// checkDatabase checks database connectivity and performance
func (hc *HealthChecker) checkDatabase(ctx context.Context) ComponentHealth {
	start := time.Now()
	health := ComponentHealth{
		LastChecked: time.Now(),
		Details:     make(map[string]interface{}),
	}

	// Ping database
	err := hc.db.PingContext(ctx)
	latency := time.Since(start)
	health.Latency = latency.String()

	if err != nil {
		health.Status = StatusUnhealthy
		health.Message = fmt.Sprintf("Database ping failed: %v", err)
		return health
	}

	// Check connection stats
	stats := hc.db.Stats()
	health.Details["open_connections"] = stats.OpenConnections
	health.Details["in_use"] = stats.InUse
	health.Details["idle"] = stats.Idle
	health.Details["max_open_connections"] = stats.MaxOpenConnections

	// Degraded if latency > 100ms
	if latency > 100*time.Millisecond {
		health.Status = StatusDegraded
		health.Message = "Database responding slowly"
	} else {
		health.Status = StatusHealthy
		health.Message = "Database connection healthy"
	}

	return health
}

// checkRedis checks Redis connectivity and performance
func (hc *HealthChecker) checkRedis(ctx context.Context) ComponentHealth {
	start := time.Now()
	health := ComponentHealth{
		LastChecked: time.Now(),
		Details:     make(map[string]interface{}),
	}

	// Ping Redis
	err := hc.redis.Ping(ctx).Err()
	latency := time.Since(start)
	health.Latency = latency.String()

	if err != nil {
		health.Status = StatusUnhealthy
		health.Message = fmt.Sprintf("Redis ping failed: %v", err)
		return health
	}

	// Get info
	if info, err := hc.redis.Info(ctx, "stats").Result(); err == nil {
		health.Details["redis_info_available"] = true
		// Could parse info here if needed
		_ = info
	}

	// Degraded if latency > 50ms
	if latency > 50*time.Millisecond {
		health.Status = StatusDegraded
		health.Message = "Redis responding slowly"
	} else {
		health.Status = StatusHealthy
		health.Message = "Redis connection healthy"
	}

	return health
}

// checkMigrations verifies required tables/columns for first-class execution
func (hc *HealthChecker) checkMigrations(ctx context.Context) ComponentHealth {
	health := ComponentHealth{
		LastChecked: time.Now(),
		Details:     make(map[string]interface{}),
	}
	// Required tables for durable execution
	requiredTables := []string{"task_records", "wal_checkpoints", "runtime_instances"}
	missing := []string{}
	for _, tbl := range requiredTables {
		var exists bool
		err := hc.db.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name=$1)`, tbl).Scan(&exists)
		if err != nil || !exists {
			missing = append(missing, tbl)
		}
	}
	if len(missing) > 0 {
		health.Status = StatusUnhealthy
		health.Message = fmt.Sprintf("Missing required tables: %s", strings.Join(missing, ", "))
		health.Details["missing_tables"] = missing
		return health
	}
	// Check DLQ migration 073
	var dlqExists bool
	_ = hc.db.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name='execution_dlq')`).Scan(&dlqExists)
	health.Details["execution_dlq"] = dlqExists
	var attemptColExists bool
	_ = hc.db.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='task_records' AND column_name='attempt_count')`).Scan(&attemptColExists)
	health.Details["attempt_count_column"] = attemptColExists
	if !dlqExists || !attemptColExists {
		health.Status = StatusDegraded
		health.Message = "DLQ migration 073 not applied — run psql database/migrations/073_execution_dlq.sql"
		return health
	}
	health.Status = StatusHealthy
	health.Message = "Migrations up-to-date"
	return health
}

// checkSigningKeys verifies signing key availability for runtime artifact verification
// In production, missing keys are Unhealthy (fail-closed); in dev, Degraded.
func (hc *HealthChecker) checkSigningKeys(ctx context.Context) ComponentHealth {
	health := ComponentHealth{
		LastChecked: time.Now(),
		Details:     make(map[string]interface{}),
	}
	overtureKey := os.Getenv("OVERTURE_SIGNING_KEY")
	if overtureKey == "" {
		overtureKey = internal.EnvOrLegacy("OVERTURE_OVERTURE_SIGNING_KEY", "IGRIS_OVERTURE_SIGNING_KEY")
	}
	runtimeKey := os.Getenv("OVERTURE_RUNTIME_PUBLIC_KEY")
	if runtimeKey == "" {
		runtimeKey = internal.EnvOrLegacy("OVERTURE_RUNTIME_PUBLIC_KEY", "IGRIS_RUNTIME_PUBLIC_KEY")
	}
	health.Details["overture_signing_key_present"] = overtureKey != ""
	health.Details["runtime_public_key_present"] = runtimeKey != ""
	isProd := os.Getenv("ENV") == "production" || os.Getenv("OVERTURE_ENV") == "production"
	if overtureKey == "" {
		if isProd {
			health.Status = StatusUnhealthy
			health.Message = "OVERTURE_SIGNING_KEY not set — production requires signing key (fail-closed)"
		} else {
			health.Status = StatusDegraded
			health.Message = "OVERTURE_SIGNING_KEY not set — governed policy decisions disabled"
		}
		return health
	}
	if runtimeKey == "" {
		if isProd {
			health.Status = StatusUnhealthy
			health.Message = "OVERTURE_RUNTIME_PUBLIC_KEY not set — production requires runtime public key (fail-closed)"
		} else {
			health.Status = StatusDegraded
			health.Message = "OVERTURE_RUNTIME_PUBLIC_KEY not set — artifact verification bypass possible"
		}
		return health
	}
	health.Status = StatusHealthy
	health.Message = "Signing keys present"
	return health
}

// checkApplication checks basic application health
func (hc *HealthChecker) checkApplication() ComponentHealth {
	health := ComponentHealth{
		Status:      StatusHealthy,
		Message:     "Application running",
		LastChecked: time.Now(),
		Details:     make(map[string]interface{}),
	}

	health.Details["uptime"] = time.Since(hc.startTime).String()
	health.Details["version"] = hc.version

	return health
}
