package actionpack

import (
	"os"
	"strings"
)

// ListBuiltin returns all built-in Action Packs shipped with Overture.
func ListBuiltin() []Manifest {
	return []Manifest{StarterPack(), WedgePack()}
}

// GetBuiltin returns a built-in pack by name, or false when unknown.
func GetBuiltin(name string) (Manifest, bool) {
	for _, pack := range ListBuiltin() {
		if pack.Name == name {
			return pack, true
		}
	}
	return Manifest{}, false
}

// StarterPack is the safe first-run pack for agent onboarding.
func StarterPack() Manifest {
	irreversible := false
	approvalRequired := true
	return Manifest{
		SchemaVersion: SchemaVersion,
		Name:          "starter",
		DisplayName:   "Starter Pack",
		Description:   "Safe mock_demo actions for first-agent onboarding: echo, simulated failure, and approval gate.",
		Actions: []ActionEntry{
			{
				Name:         "demo.echo",
				DisplayName:  "Demo Echo",
				Description:  "Echo input through mock_demo — proves the happy path without external APIs.",
				TargetType:   "mock_demo",
				PolicyPreset: "Read-only",
				ReplayClass:  "read_only",
				TargetMetadata: map[string]interface{}{
					"demo_variant": "echo",
					"pack":         "starter",
				},
				InputSchema: map[string]interface{}{
					"type":     "object",
					"required": []interface{}{"message"},
					"properties": map[string]interface{}{
						"message": map[string]interface{}{"type": "string"},
					},
					"additionalProperties": false,
				},
				IdempotencyGuidance: "Use a stable key per logical echo; retries with the same key return the same run.",
				ExampleInput: map[string]interface{}{
					"message": "hello from starter pack",
				},
			},
			{
				Name:         "demo.fail_once",
				DisplayName:  "Demo Fail Once",
				Description:  "Simulates a failed first attempt for recovery visibility; retry with input.retry=true or a new idempotency key.",
				TargetType:   "mock_demo",
				PolicyPreset: "Safe automation",
				ReplayClass:  "retryable",
				TargetMetadata: map[string]interface{}{
					"demo_variant": "fail_once",
					"pack":         "starter",
				},
				InputSchema: map[string]interface{}{
					"type": "object",
					"properties": map[string]interface{}{
						"message": map[string]interface{}{"type": "string"},
						"retry":   map[string]interface{}{"type": "boolean"},
					},
					"additionalProperties": false,
				},
				IdempotencyGuidance: "First attempt fails by design. Retry with a new idempotency key and input.retry=true to complete.",
				ExampleInput: map[string]interface{}{
					"message": "show me failure visibility",
				},
			},
			{
				Name:             "demo.needs_approval",
				DisplayName:      "Demo Needs Approval",
				Description:      "Pauses at approval_required — proves human-gated policy before execution.",
				TargetType:       "mock_demo",
				PolicyPreset:     "Human-gated",
				ReplayClass:      "non_retryable",
				ApprovalRequired: &approvalRequired,
				Irreversible:     &irreversible,
				TargetMetadata: map[string]interface{}{
					"demo_variant": "needs_approval",
					"pack":         "starter",
				},
				InputSchema: map[string]interface{}{
					"type":     "object",
					"required": []interface{}{"message"},
					"properties": map[string]interface{}{
						"message": map[string]interface{}{"type": "string"},
					},
					"additionalProperties": false,
				},
				IdempotencyGuidance: "Use one key per approval request; do not reuse keys across distinct approval decisions.",
				ExampleInput: map[string]interface{}{
					"message": "request operator approval",
				},
			},
		},
	}
}

func wedgeBaseURL() string {
	base := strings.TrimSpace(os.Getenv("OVERTURE_WEDGE_TARGET_BASE_URL"))
	if base == "" {
		base = strings.TrimSpace(os.Getenv("IGRIS_WEDGE_TARGET_BASE_URL"))
	}
	if base == "" {
		base = "https://hooks.example.com"
	}
	base = strings.TrimRight(base, "/")
	if !strings.HasPrefix(strings.ToLower(base), "https://") {
		// Fallback to example if env is not https (fail-closed validation)
		base = "https://hooks.example.com"
	}
	return base
}

// WedgePack is the hosted-alpha wedge: deploy.staging, deploy.production, migrate.database, publish.package.
// These are hosted_api/webhook actions with explicit effect_class via ReplayClass/Irreversible and https target_url.
// Install with: POST /v1/action-packs/install {"name":"overture-wedge"}
// Target base URL is configurable via OVERTURE_WEDGE_TARGET_BASE_URL (default https://hooks.example.com).
func WedgePack() Manifest {
	irreversibleTrue := true
	irreversibleFalse := false
	approvalTrue := true
	approvalFalse := false
	base := wedgeBaseURL()
	return Manifest{
		SchemaVersion: SchemaVersion,
		Name:          "overture-wedge",
		DisplayName:   "Overture Wedge",
		Description:   "Hosted-alpha wedge for consequential delivery: deploy.staging, deploy.production, migrate.database, publish.package. Each is a durable hosted_api/webhook action with provider-agnostic target_url — configure your URL at install or via POST /v1/actions.",
		Actions: []ActionEntry{
			{
				Name:         "deploy.staging",
				DisplayName:  "Deploy to Staging",
				Description:  "Deploy service to staging via hosted webhook — retryable, safe automation.",
				TargetType:   "hosted_api",
				TargetURL:    base + "/deploy/staging",
				PolicyPreset: "Safe automation",
				ReplayClass:  "retryable",
				ApprovalRequired: &approvalFalse,
				Irreversible:     &irreversibleFalse,
				TargetMetadata: map[string]interface{}{"effect_class": "retryable", "pack": "overture-wedge"},
				InputSchema: map[string]interface{}{
					"type": "object",
					"required": []interface{}{"service", "commit"},
					"properties": map[string]interface{}{
						"service": map[string]interface{}{"type": "string", "description": "service name"},
						"commit":  map[string]interface{}{"type": "string", "description": "git SHA"},
						"env":     map[string]interface{}{"type": "string", "default": "staging"},
					},
					"additionalProperties": true,
				},
				IdempotencyGuidance: "Use deploy:staging:{service}:{commit} as idempotency_key; retries with same key are deduped.",
				ExampleInput: map[string]interface{}{"service": "api", "commit": "abc123", "env": "staging"},
			},
			{
				Name:         "deploy.production",
				DisplayName:  "Deploy to Production",
				Description:  "Deploy service to production — human-gated, irreversible, non-retryable.",
				TargetType:   "hosted_api",
				TargetURL:    base + "/deploy/production",
				PolicyPreset: "Human-gated",
				ReplayClass:  "non_retryable",
				ApprovalRequired: &approvalTrue,
				Irreversible:     &irreversibleTrue,
				TargetMetadata: map[string]interface{}{"effect_class": "irreversible", "pack": "overture-wedge"},
				InputSchema: map[string]interface{}{
					"type": "object",
					"required": []interface{}{"service", "commit"},
					"properties": map[string]interface{}{
						"service": map[string]interface{}{"type": "string"},
						"commit":  map[string]interface{}{"type": "string"},
						"env":     map[string]interface{}{"type": "string", "default": "production"},
					},
					"additionalProperties": true,
				},
				IdempotencyGuidance: "Human-gated. Use deploy:production:{service}:{commit}. Do not reuse keys across distinct approvals.",
				ExampleInput: map[string]interface{}{"service": "api", "commit": "abc123", "env": "production"},
			},
			{
				Name:         "migrate.database",
				DisplayName:  "Migrate Database",
				Description:  "Run database migration — human-gated, irreversible, non-retryable, effect journal before commit.",
				TargetType:   "webhook",
				TargetURL:    base + "/migrate/database",
				PolicyPreset: "Human-gated",
				ReplayClass:  "non_retryable",
				ApprovalRequired: &approvalTrue,
				Irreversible:     &irreversibleTrue,
				TargetMetadata: map[string]interface{}{"effect_class": "irreversible", "pack": "overture-wedge"},
				InputSchema: map[string]interface{}{
					"type": "object",
					"required": []interface{}{"migration"},
					"properties": map[string]interface{}{
						"migration": map[string]interface{}{"type": "string", "description": "migration name or file"},
						"database":  map[string]interface{}{"type": "string"},
					},
					"additionalProperties": true,
				},
				IdempotencyGuidance: "Use migrate:{database}:{migration} as idempotency_key. Irreversible — replay blocked after commit.",
				ExampleInput: map[string]interface{}{"migration": "20260601_add_users", "database": "primary"},
			},
			{
				Name:         "publish.package",
				DisplayName:  "Publish Package",
				Description:  "Publish package to registry — irreversible, safe automation with effect journal.",
				TargetType:   "webhook",
				TargetURL:    base + "/publish/package",
				PolicyPreset: "Safe automation",
				ReplayClass:  "non_retryable",
				ApprovalRequired: &approvalFalse,
				Irreversible:     &irreversibleTrue,
				TargetMetadata: map[string]interface{}{"effect_class": "irreversible", "pack": "overture-wedge"},
				InputSchema: map[string]interface{}{
					"type": "object",
					"required": []interface{}{"package", "version"},
					"properties": map[string]interface{}{
						"package": map[string]interface{}{"type": "string"},
						"version": map[string]interface{}{"type": "string"},
						"registry": map[string]interface{}{"type": "string"},
					},
					"additionalProperties": true,
				},
				IdempotencyGuidance: "Use publish:{package}:{version} as idempotency_key. Irreversible — do not retry with new key after commit.",
				ExampleInput: map[string]interface{}{"package": "overture-sdk", "version": "0.1.0", "registry": "https://registry.example"},
			},
		},
	}
}