# Overture

Durable execution boundary for consequential actions. Extracted from `Igris-inertial/system` `igris-overture/`.

**Product:** `Action -> Run -> Proof` + exceptional `Reconciliation` per `docs/product/PRD.md:21` wedge `deploy.staging/production/migrate.database/publish.package`.

Core: `coordinator/task_coordinator.go:66` Submit->idempotency->selectRuntime->Policy->PermissionEnvelope->dispatch->VerifyReceipt `api/evidence_verify.go:104` + `igris_run_proof.go` + `072_operator_reconciliation_events.sql`.

REST is canonical `PRD.md:98`; Python SDK `sdk/python` thin wrapper.

Extracted 2026-08-26 via `git subtree` from monorepo - keep lean: ~12 dirs `api/coordinator/database/auth/policy/billing` (prune `cognitive/semantic/bandit/agent*`).

See `Igris-inertial/system` for runtime + web + archive.
