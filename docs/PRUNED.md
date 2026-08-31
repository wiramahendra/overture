# Pruned Surface — Overture First-Class Execution

> PRD `docs/product/PRD.md:219` freezes: robotics, fleet, speculative, large console, etc until amendment.
> This doc records that the following surfaces are **pruned operationally**: code moved to `attic/` and not imported by default server (`cmd/server/main.go:93`). No `OVERTURE_ENABLE_EXPERIMENTAL` flag needed — attic is not compiled.

## Pruned (not registered by default)

| Dir / Feature | Reason | Where still lives |
|---|---|---|
| `router/speculative_router.go`, `router/council.go`, `router/quality_scorer.go`, `router/cost_accounting.go`, `router/stream_merger.go`, `router/semantic_router.go`, `router/thompson_sampling.go` | speculative execution (PRD frozen) | `attic/router/` — 13k LoC, not built |
| `routing/provider_selector.go` | inference provider health | `attic/routing/` 398 LoC |
| `inference/` (21 files, 5.7k) | inference orchestration, Rust FFI, quality/lineage | `attic/inference/` |
| `providers/` (18 files, 6.7k) | OpenAI/Anthropic adapters, cost models | `attic/providers/` |
| `mesh/inference_mesh.go` | NATS mesh | `attic/mesh/` |
| `orchestration/grpc_server.go` | gRPC InferenceRouterService | `attic/orchestration/` |
| `cognitive/`, `semantic/`, `bandit/`, `scheduler/`, `slo/`, `safety/` | auto-apply, ONNX, bandit, scheduler, safety budget | `attic/cognitive/`, `attic/semantic/`, `attic/bandit/`, `attic/scheduler/`, `attic/slo_archived/`, `attic/safety/` (total ~10k, `//go:build ignore` elsewhere) |
| `database/migrations/001_fleet_management.sql` fleet_* tables + `api/routes_fleet.go` | fleet features (PRD frozen) | archived in place but not queried by core execution |
| `coordinator` robotics branches (`robotics_workflow`, `evaluateRoboticsPolicy`) + `compliance/robotics_audit_export.go` | robotics (PRD frozen) | still in `coordinator/` but unreachable without `task_type=robotics_workflow` and never exercised by `deploy.*` wedge |

## What stays (first-class execution)

- `coordinator/` TaskCoordinator + `checkpoint_store.go` WAL, `api/routes_actions.go` Action→Run, `api/routes_contracts.go`, `api/routes_evidence.go`, `api/igris_run_proof.go` Proof, `api/operator_reconciliation.go`, `internal/runtime_client.go`, `database/migrations` core (031 task_records, 033 proof, 051 governance, 072 reconciliation), `health/`, `billing/` flat, `config/` + `internal/env.go` OVERTURE alias.

## Next step to fully delete

When CI is green on generic Postgres (`docker compose up` + `go test ./...`), run `go mod tidy` to drop `nats.go`, `grpc`, `onnx` and delete `attic/` — shrinks image ~40%. Keep this doc for audit.
