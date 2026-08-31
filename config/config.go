package config

import (
	"os"
	"strconv"
	"time"
)

// Config holds all application configuration
type Config struct {
	Server          ServerConfig
	TLS             TLSConfig
	MLService       MLServiceConfig
	CircuitBreaker  CircuitBreakerConfig
	Validation      ValidationConfig
	Observability   ObservabilityConfig
	RateLimit       RateLimitConfig
	Security        SecurityConfig
	Persistence     PersistenceConfig // Phase 2: External state persistence
	Speculative     SpeculativeConfig // Speculative execution configuration (pruned — disabled by default)
	Execution       ExecutionConfig   // Durable execution tuning (first-class)
}

// ServerConfig holds server configuration
type ServerConfig struct {
	Port        string
	Host        string
	Environment string
}

// TLSConfig holds TLS configuration
type TLSConfig struct {
	Enabled  bool
	CertFile string
	KeyFile  string
}

// MLServiceConfig holds ML service configuration
type MLServiceConfig struct {
	Address    string
	Timeout    time.Duration
	MaxRetries int
	Enabled    bool // If false, skip semantic routing and use Thompson Sampling only
}

// CircuitBreakerConfig holds circuit breaker configuration
type CircuitBreakerConfig struct {
	Name        string
	MaxRequests uint32
	Interval    time.Duration
	Timeout     time.Duration
	Threshold   float64
}

// ValidationConfig holds validation configuration
type ValidationConfig struct {
	MaxFeatures    int
	RequireTraceID bool
}

// ObservabilityConfig holds observability configuration
type ObservabilityConfig struct {
	MetricsEnabled bool
	MetricsPort    string
	TracingEnabled bool
	TracingEndpoint string
	LogLevel       string
	LogFormat      string
}

// RateLimitConfig holds rate limiting configuration
type RateLimitConfig struct {
	Enabled            bool
	RequestsPerSecond  int
	Burst              int
}

// SecurityConfig holds security configuration
type SecurityConfig struct {
	JWTSecret    string
	CORSEnabled  bool
	CORSOrigins  string
	AllowedHosts string
}

// PersistenceConfig holds persistence layer configuration (Phase 2)
type PersistenceConfig struct {
	// Redis configuration
	UseRedis           bool   // Enable Redis for provider stats
	RedisURL           string // Redis connection URL
	RedisEnabled       bool   // Computed: true if UseRedis && RedisURL is set

	// PostgreSQL configuration
	UsePostgres        bool   // Enable Postgres for optimizer state
	PostgresURL        string // PostgreSQL connection URL
	PostgresEnabled    bool   // Computed: true if UsePostgres && PostgresURL is set

	// Distributed locking
	UseDistributedLock bool   // Enable distributed locking for multi-instance setups
}

// SpeculativeMode defines the optimization strategy for speculative execution
type SpeculativeMode string

const (
	SpeculativeModeOff      SpeculativeMode = "off"      // Disabled
	SpeculativeModeLatency  SpeculativeMode = "latency"  // Optimize for lowest first-token latency
	SpeculativeModeQuality  SpeculativeMode = "quality"  // Optimize for best early-token quality
	SpeculativeModeCost     SpeculativeMode = "cost"     // Optimize for lowest cost with acceptable latency
	SpeculativeModeBalanced SpeculativeMode = "balanced" // Balanced optimization across all criteria
)

// SpeculativeConfig holds configuration for speculative execution (pruned)
type SpeculativeConfig struct {
	Enabled               bool           `json:"enabled"`
	DefaultMode           SpeculativeMode `json:"default_mode"`
	MaxProviders          int            `json:"max_providers"`
	FirstTokenTimeout     time.Duration  `json:"first_token_timeout"`
	EarlyTokenCount       int            `json:"early_token_count"`
	CostMultiplier        float64        `json:"cost_multiplier"`
	WasteThreshold        float64        `json:"waste_threshold"`
	ChairmanProvider      string         `json:"chairman_provider"`
	UseONNXQualityScoring bool           `json:"use_onnx_quality_scoring"`
	ONNXModelPath         string         `json:"onnx_model_path"`
}

// ExecutionConfig holds durable execution tuning (first-class).
// Env canonical: OVERTURE_* with IGRIS_* fallback.
type ExecutionConfig struct {
	HeartbeatTimeout       time.Duration `json:"heartbeat_timeout"`        // how long before runtime declared dead (default 90s)
	RecoveryInterval       time.Duration `json:"recovery_interval"`        // how often to scan for dead runtimes (default 15s)
	DispatchConcurrency    int           `json:"dispatch_concurrency"`     // max concurrent dispatches (default 32)
	MaxRecoveryAttempts    int           `json:"max_recovery_attempts"`    // before DLQ (default 10)
	DeadlineReaperInterval time.Duration `json:"deadline_reaper_interval"` // sweep expired tasks (default 30s)
	InputRefTTL            time.Duration `json:"input_ref_ttl"`            // execution_input_refs expiry (default 24h)
	CheckpointInterval     int           `json:"checkpoint_interval"`      // forced checkpoint every N steps (default 5)
}

// LoadConfig loads configuration from environment variables
func LoadConfig() *Config {
	// Load persistence configuration
	useRedis := getEnvBool("USE_REDIS", false)
	redisURL := getEnv("REDIS_URL", "")

	usePostgres := getEnvBool("USE_PG_OPTIMIZER_STATE", false)
	postgresURL := getEnv("DATABASE_URL", "")
	if postgresURL == "" {
		postgresURL = getEnv("POSTGRES_URL", "")
	}

	return &Config{
		Server: ServerConfig{
			Port:        getEnv("PORT", "8080"),
			Host:        getEnv("HOST", "0.0.0.0"),
			Environment: getEnv("ENV", "development"),
		},
		TLS: TLSConfig{
			Enabled:  getEnvBool("TLS_ENABLED", false),
			CertFile: getEnv("TLS_CERT_FILE", ""),
			KeyFile:  getEnv("TLS_KEY_FILE", ""),
		},
		MLService: MLServiceConfig{
			Address:    getEnv("ML_SERVICE_ADDRESS", "python-ml:50051"),
			Timeout:    getEnvDuration("ML_SERVICE_TIMEOUT", 30*time.Second),
			MaxRetries: getEnvInt("ML_SERVICE_MAX_RETRIES", 3),
		},
		CircuitBreaker: CircuitBreakerConfig{
			Name:        getEnv("CIRCUIT_BREAKER_NAME", "ML-Service"),
			MaxRequests: uint32(getEnvInt("CIRCUIT_BREAKER_MAX_REQUESTS", 3)),
			Interval:    getEnvDuration("CIRCUIT_BREAKER_INTERVAL", 10*time.Second),
			Timeout:     getEnvDuration("CIRCUIT_BREAKER_TIMEOUT", 30*time.Second),
			Threshold:   getEnvFloat("CIRCUIT_BREAKER_THRESHOLD", 0.5),
		},
		Validation: ValidationConfig{
			MaxFeatures:    getEnvInt("VALIDATION_MAX_FEATURES", 10000),
			RequireTraceID: getEnvBool("VALIDATION_REQUIRE_TRACE_ID", true),
		},
		Observability: ObservabilityConfig{
			MetricsEnabled:  getEnvBool("METRICS_ENABLED", true),
			MetricsPort:     getEnv("METRICS_PORT", "9090"),
			TracingEnabled:  getEnvBool("TRACING_ENABLED", false),
			TracingEndpoint: getEnv("TRACING_ENDPOINT", ""),
			LogLevel:        getEnv("LOG_LEVEL", "info"),
			LogFormat:       getEnv("LOG_FORMAT", "json"),
		},
		RateLimit: RateLimitConfig{
			Enabled:           getEnvBool("RATE_LIMIT_ENABLED", true),
			RequestsPerSecond: getEnvInt("RATE_LIMIT_REQUESTS_PER_SECOND", 100),
			Burst:             getEnvInt("RATE_LIMIT_BURST", 200),
		},
		Security: SecurityConfig{
			JWTSecret:    getEnv("JWT_SECRET", "change-this-secret"),
			CORSEnabled:  getEnvBool("CORS_ENABLED", true),
			CORSOrigins:  getEnv("CORS_ORIGINS", "*"),
			AllowedHosts: getEnv("ALLOWED_HOSTS", "localhost"),
		},
		Persistence: PersistenceConfig{
			UseRedis:           useRedis,
			RedisURL:           redisURL,
			RedisEnabled:       useRedis && redisURL != "",
			UsePostgres:        usePostgres,
			PostgresURL:        postgresURL,
			PostgresEnabled:    usePostgres && postgresURL != "",
			UseDistributedLock: getEnvBool("USE_DISTRIBUTED_LOCK", false),
		},
		Speculative: SpeculativeConfig{
			Enabled:               getEnvBool("ENABLE_SPECULATIVE", false),
			DefaultMode:           SpeculativeMode(getEnv("SPECULATIVE_MODE", string(SpeculativeModeLatency))),
			MaxProviders:          getEnvInt("SPECULATIVE_MAX_PROVIDERS", 3),
			FirstTokenTimeout:     getEnvDuration("SPECULATIVE_FIRST_TOKEN_TIMEOUT", 5*time.Second),
			EarlyTokenCount:       getEnvInt("SPECULATIVE_EARLY_TOKEN_COUNT", 5),
			CostMultiplier:        getEnvFloat("SPECULATIVE_COST_MULTIPLIER", 1.5),
			WasteThreshold:        getEnvFloat("SPECULATIVE_WASTE_THRESHOLD", 0.3),
			ChairmanProvider:      getEnv("COUNCIL_CHAIRMAN_PROVIDER", "gpt-4"),
			UseONNXQualityScoring: getEnvBool("USE_ONNX_QUALITY_SCORING", false),
			ONNXModelPath:         getEnv("ONNX_QUALITY_MODEL_PATH", "/models/quality_scorer.onnx"),
		},
		Execution: ExecutionConfig{
			HeartbeatTimeout:       getEnvDurationWithFallback("OVERTURE_HEARTBEAT_TIMEOUT", "IGRIS_HEARTBEAT_TIMEOUT", 90*time.Second),
			RecoveryInterval:       getEnvDurationWithFallback("OVERTURE_RECOVERY_INTERVAL", "IGRIS_RECOVERY_INTERVAL", 15*time.Second),
			DispatchConcurrency:    getEnvIntWithFallback("OVERTURE_DISPATCH_CONCURRENCY", "IGRIS_DISPATCH_CONCURRENCY", 32),
			MaxRecoveryAttempts:    getEnvIntWithFallback("OVERTURE_MAX_RECOVERY_ATTEMPTS", "IGRIS_MAX_RECOVERY_ATTEMPTS", 10),
			DeadlineReaperInterval: getEnvDurationWithFallback("OVERTURE_DEADLINE_REAPER_INTERVAL", "IGRIS_DEADLINE_REAPER_INTERVAL", 30*time.Second),
			InputRefTTL:            getEnvDurationWithFallback("OVERTURE_INPUT_REF_TTL", "IGRIS_INPUT_REF_TTL", 24*time.Hour),
			CheckpointInterval:     getEnvIntWithFallback("OVERTURE_CHECKPOINT_INTERVAL", "IGRIS_CHECKPOINT_INTERVAL", 5),
		},
	}
}

// Helper functions for environment variable parsing

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getEnvWithFallback(primary, fallback, defaultValue string) string {
	if v := os.Getenv(primary); v != "" {
		return v
	}
	if v := os.Getenv(fallback); v != "" {
		return v
	}
	return defaultValue
}

func getEnvIntWithFallback(primary, fallback string, defaultValue int) int {
	if v := os.Getenv(primary); v != "" {
		if iv, err := strconv.Atoi(v); err == nil {
			return iv
		}
	}
	if v := os.Getenv(fallback); v != "" {
		if iv, err := strconv.Atoi(v); err == nil {
			return iv
		}
	}
	return defaultValue
}

func getEnvDurationWithFallback(primary, fallback string, defaultValue time.Duration) time.Duration {
	if v := os.Getenv(primary); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	if v := os.Getenv(fallback); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return defaultValue
}

func getEnvInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if intVal, err := strconv.Atoi(value); err == nil {
			return intVal
		}
	}
	return defaultValue
}

func getEnvBool(key string, defaultValue bool) bool {
	if value := os.Getenv(key); value != "" {
		if boolVal, err := strconv.ParseBool(value); err == nil {
			return boolVal
		}
	}
	return defaultValue
}

func getEnvFloat(key string, defaultValue float64) float64 {
	if value := os.Getenv(key); value != "" {
		if floatVal, err := strconv.ParseFloat(value, 64); err == nil {
			return floatVal
		}
	}
	return defaultValue
}

func getEnvDuration(key string, defaultValue time.Duration) time.Duration {
	if value := os.Getenv(key); value != "" {
		if duration, err := time.ParseDuration(value); err == nil {
			return duration
		}
	}
	return defaultValue
}
