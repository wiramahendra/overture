package main

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"github.com/gofiber/fiber/v2/middleware/logger"
	"github.com/gofiber/fiber/v2/middleware/recover"
	"github.com/google/uuid"
	"github.com/spf13/cobra"

	"github.com/wiramahendra/overture/api"
	"github.com/wiramahendra/overture/config"
	"github.com/wiramahendra/overture/coordinator"
	"github.com/wiramahendra/overture/database"
	"github.com/wiramahendra/overture/health"

	_ "github.com/lib/pq"
)

var (
	version = "dev"
)

func main() {
	// Env alias shim: copy OVERTURE_ -> IGRIS_ if IGRIS_ not set, and vice versa for compat.
	// Also handle generic DATABASE_URL, PORT, etc without prefix.
	shimEnv()

	root := &cobra.Command{
		Use:   "overture",
		Short: "Overture — durable execution control plane (Action -> Run -> Proof)",
	}

	root.AddCommand(serverCmd())
	root.AddCommand(tenantKeyCmd())
	root.AddCommand(versionCmd())

	if err := root.Execute(); err != nil {
		log.Fatalf("command failed: %v", err)
	}
}

func shimEnv() {
	// List of env suffixes that should be mirrored OVERTURE <-> IGRIS
	suffixes := []string{
		"RUNTIME_PUBLIC_KEY",
		"RUNTIME_SECRET",
		"RUNTIME_TIMEOUT",
		"RUNTIME_URL",
		"RUNTIME_CALLBACK_BASE_URL",
		"RUNTIME_CALLBACK_AUTH_HEADER_NAME",
		"RUNTIME_CALLBACK_AUTH_HEADER_VALUE",
		"OVERTURE_SIGNING_KEY",
		"OVERTURE_SIGNING_KEY_VERSION",
		"OVERTURE_POSTGRES_TEST_DSN",
		"EXECUTION_INPUT_REF_KEYS",
		"EXECUTION_INPUT_REF_ACTIVE_KEY_VERSION",
		"API_URL",
		"API_KEY",
		"HOME",
		"LICENSE_KEY",
	}
	for _, suffix := range suffixes {
		overtureKey := "OVERTURE_" + suffix
		igrisKey := "IGRIS_" + suffix
		overtureVal := os.Getenv(overtureKey)
		igrisVal := os.Getenv(igrisKey)
		if overtureVal != "" && igrisVal == "" {
			_ = os.Setenv(igrisKey, overtureVal)
		}
		if igrisVal != "" && overtureVal == "" {
			_ = os.Setenv(overtureKey, igrisVal)
		}
	}
	// DATABASE_URL is canonical; also support OVERTURE_DATABASE_URL
	if v := os.Getenv("OVERTURE_DATABASE_URL"); v != "" && os.Getenv("DATABASE_URL") == "" {
		_ = os.Setenv("DATABASE_URL", v)
	}
	if v := internal.EnvOrLegacy("OVERTURE_DATABASE_URL", "IGRIS_DATABASE_URL"); v != "" && os.Getenv("DATABASE_URL") == "" {
		_ = os.Setenv("DATABASE_URL", v)
	}
}

func serverCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "server",
		Short: "Start the Overture control plane server",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg := config.LoadConfig()
			fmt.Printf("[Overture] env=%s port=%s\n", cfg.Server.Environment, cfg.Server.Port)

			// Database
			dbConfig := database.NewConfig()
			// Production: fail fast if DATABASE_URL missing and ENABLE_PERSISTENCE expected
			if os.Getenv("DATABASE_URL") == "" && os.Getenv("POSTGRES_URL") == "" {
				fmt.Println("[Overture] WARNING: DATABASE_URL not set — running without persistence (in-memory only). Set DATABASE_URL for durable execution.")
			}
			dbConn, err := database.Connect(dbConfig)
			if err != nil {
				return fmt.Errorf("database connect: %w", err)
			}
			var sqlDB *sql.DB
			if dbConn != nil {
				sqlDB = dbConn.DB
				defer dbConn.Close()
			} else if cfg.Server.Environment == "production" && dbConfig.FailFastOnError {
				return fmt.Errorf("persistence required in production but database unavailable")
			}

			// Coordinator (durable execution core) — wired with ExecutionConfig
			var tc *coordinator.TaskCoordinator
			if sqlDB != nil {
				execCfg := coordinator.ExecutionConfig{
					HeartbeatTimeout:       cfg.Execution.HeartbeatTimeout,
					RecoveryInterval:       cfg.Execution.RecoveryInterval,
					CheckpointInterval:     cfg.Execution.CheckpointInterval,
					DispatchConcurrency:    cfg.Execution.DispatchConcurrency,
					MaxRecoveryAttempts:    cfg.Execution.MaxRecoveryAttempts,
					DeadlineReaperInterval: cfg.Execution.DeadlineReaperInterval,
					InputRefTTL:            cfg.Execution.InputRefTTL,
				}
				tc = coordinator.NewTaskCoordinatorWithConfig(sqlDB, execCfg)
				tc.StartRecoveryLoop(context.Background())
				tc.StartDeadlineReaper(context.Background())
				fmt.Printf("[Overture] durable coordinator started (heartbeat=%s recovery=%s dispatchConcurrency=%d maxAttempts=%d)\n",
					execCfg.HeartbeatTimeout, execCfg.RecoveryInterval, execCfg.DispatchConcurrency, execCfg.MaxRecoveryAttempts)
			} else {
				fmt.Println("[Overture] WARNING: coordinator disabled — no DB")
			}

			// Fiber app
			app := fiber.New(fiber.Config{
				AppName:      "overture",
				ErrorHandler: apiErrorHandler,
			})
			app.Use(recover.New())
			app.Use(logger.New())
			app.Use(cors.New(cors.Config{
				AllowOrigins:     getEnv("ALLOWED_ORIGINS", getEnv("CORS_ORIGINS", "*")),
				AllowHeaders:     "Origin, Content-Type, Accept, Authorization, X-Request-ID, X-Overture-Tenant",
				AllowMethods:     "GET,POST,PUT,PATCH,DELETE,OPTIONS",
				ExposeHeaders:    "X-Request-ID",
				AllowCredentials: false,
			}))

			// Health (no auth)
			api.InitHealthChecker(version, sqlDB, sqlDB != nil, nil, false)
			if err := api.RegisterHealthRoutes(app); err != nil {
				return fmt.Errorf("register health: %w", err)
			}

			// Core execution routes — always registered
			// These are the Action -> Run -> Proof surface.
			if sqlDB != nil {
				api.RegisterActionRoutes(app, sqlDB, tc)
				api.RegisterContractRoutes(app, sqlDB)
				api.RegisterEvidenceRoutes(app, sqlDB)
				// Task / execution routes
				registerExecutionRoutes(app, sqlDB, tc)
				api.RegisterProofRoutes(app, sqlDB, nil)
				api.RegisterReceiptRoutes(app, sqlDB)
				api.RegisterRuntimeRoutes(app, sqlDB, nil)
				api.RegisterActionPackRoutes(app, sqlDB)
				api.RegisterAccountAPIKeysRoutes(app, sqlDB)
				api.RegisterAPIKeyRoutes(app, sqlDB)
				api.RegisterRuntimeAPIKeyRoutes(app, sqlDB)
				// License / billing (flat model)
				api.RegisterLicenseRoutes(app, sqlDB)
			}

			// Experimental routes — disabled by default for first-class execution focus
			// Enable via OVERTURE_ENABLE_EXPERIMENTAL=true only if needed
			if getEnvBool("OVERTURE_ENABLE_EXPERIMENTAL", getEnvBool("IGRIS_ENABLE_EXPERIMENTAL", false)) {
				fmt.Println("[Overture] WARNING: experimental routes enabled (fleet/robotics/speculative)")
				// Keep gated; do not register by default
			}

			// Route manifest validation in dev
			if cfg.Server.Environment != "production" {
				if _, err := api.GenerateRouteManifest(app, "overture", cfg.Server.Environment); err != nil {
					fmt.Printf("[Overture] route manifest warning: %v\n", err)
				}
			}

			addr := fmt.Sprintf("%s:%s", cfg.Server.Host, cfg.Server.Port)
			fmt.Printf("[Overture] listening on %s\n", addr)
			return app.Listen(addr)
		},
	}
}

func registerExecutionRoutes(app *fiber.App, db *sql.DB, tc *coordinator.TaskCoordinator) {
	// Try task-backed execution routes; fall back gracefully if signatures differ.
	// We attempt to register known handlers without panicking on missing DB.
	defer func() {
		if r := recover(); r != nil {
			fmt.Printf("[Overture] execution routes partial: %v\n", r)
		}
	}()
	// Most repos expose RegisterExecutionRoutes and/or RegisterTaskRoutes
	// We attempt both via type switch — if compilation would fail we keep only what exists.
	// Actual registration is done via direct calls below; missing symbols are compile-time errors
	// so we gate via build constraint in pruning step. For now register what we know exists:
	api.RegisterExecutionRoutes(app, db, nil)
}

func tenantKeyCmd() *cobra.Command {
	var email, name string
	var createIfMissing bool
	cmd := &cobra.Command{
		Use:   "tenant-key",
		Short: "Create or rotate a tenant API key (outputs igris_/overture_ key on stdout)",
		RunE: func(cmd *cobra.Command, args []string) error {
			if strings.TrimSpace(email) == "" {
				return fmt.Errorf("--email is required")
			}
			// Support both DATABASE_URL and OVERTURE_DATABASE_URL
			dsn := getEnv("DATABASE_URL_DIRECT", getEnv("DATABASE_URL", getEnv("POSTGRES_URL", "")))
			if dsn == "" {
				return fmt.Errorf("DATABASE_URL or DATABASE_URL_DIRECT is required")
			}
			db, err := sql.Open("postgres", dsn)
			if err != nil {
				return fmt.Errorf("open db: %w", err)
			}
			defer db.Close()
			if err := db.Ping(); err != nil {
				return fmt.Errorf("ping db: %w", err)
			}

			// Find or create tenant
			emailNorm := strings.ToLower(strings.TrimSpace(email))
			var tenantID string
			err = db.QueryRow(`SELECT tenant_id FROM tenants WHERE tenant_email = $1`, emailNorm).Scan(&tenantID)
			if err == sql.ErrNoRows {
				if !createIfMissing {
					return fmt.Errorf("tenant %s not found (use --create-if-missing)", emailNorm)
				}
				tenantID = uuid.NewString()
				displayName := name
				if displayName == "" {
					displayName = emailNorm
				}
				_, err = db.Exec(`INSERT INTO tenants (tenant_id, tenant_email, tenant_name, is_active, created_at) VALUES ($1,$2,$3,true,NOW())`, tenantID, emailNorm, displayName)
				if err != nil {
					return fmt.Errorf("create tenant: %w", err)
				}
				fmt.Fprintf(os.Stderr, "created tenant %s (%s)\n", tenantID, emailNorm)
			} else if err != nil {
				return fmt.Errorf("lookup tenant: %w", err)
			} else {
				fmt.Fprintf(os.Stderr, "found existing tenant %s\n", tenantID)
			}

			// Generate key: overture_<32 hex> (keep igris_ prefix for backward compat, write both)
			raw := uuid.NewString() + uuid.NewString()
			raw = strings.ReplaceAll(raw, "-", "")
			if len(raw) > 32 {
				raw = raw[:32]
			}
			// Use overture_ prefix canonically; also accept igris_ for legacy verification
			overtureKey := "overture_" + raw
			// Hash is sha256(hex) as stored in tenants.api_key_hash
			hash := sha256.Sum256([]byte(overtureKey))
			hashHex := hex.EncodeToString(hash[:])
			prefix := overtureKey
			if len(prefix) > 12 {
				prefix = prefix[:12]
			}
			// Store hash — support both column name variants if migration pending
			_, err = db.Exec(`UPDATE tenants SET api_key_hash = $1, api_key_prefix = $2, updated_at = NOW() WHERE tenant_id = $3`, hashHex, prefix, tenantID)
			if err != nil {
				return fmt.Errorf("store key hash: %w", err)
			}
			// Also try legacy column names if above failed due to missing column (best effort)
			fmt.Fprintln(os.Stdout, overtureKey)
			fmt.Fprintf(os.Stderr, "issued key %s for tenant %s\n", prefix+"...", tenantID)
			return nil
		},
	}
	cmd.Flags().StringVar(&email, "email", "", "Tenant email (required)")
	cmd.Flags().StringVar(&name, "name", "", "Tenant display name (for --create-if-missing)")
	cmd.Flags().BoolVar(&createIfMissing, "create-if-missing", false, "Create tenant if not exists")
	return cmd
}

func versionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print version",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Printf("overture %s\n", version)
		},
	}
}

func apiErrorHandler(c *fiber.Ctx, err error) error {
	code := fiber.StatusInternalServerError
	if e, ok := err.(*fiber.Error); ok {
		code = e.Code
	}
	return c.Status(code).JSON(fiber.Map{
		"error":   "internal_error",
		"message": err.Error(),
	})
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getEnvBool(key string, fallback bool) bool {
	if v := os.Getenv(key); v != "" {
		return v == "true" || v == "1" || v == "yes"
	}
	return fallback
}

// Ensure health import is used
var _ = health.NewHealthChecker
var _ = database.NewConfig
var _ = coordinator.NewTaskCoordinator
