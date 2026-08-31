# Deploying Overture

Overture is the execution brain (action registry, policy, routing, recovery, proof, receipts, task state). It is the **only** persistence layer. Console and runtime both call into it; neither owns durable state.

> Deployment is provider-agnostic. Pick where you run Postgres + Redis + container — then decide. No Azure/Render/Neon lock-in.

## What you need (any provider)

| Concern | What | Example |
|---|---|---|
| Compute | Any container runtime | Docker, Fly, Render, Azure Container Apps, ECS, K8s |
| Database | Postgres 16+ (single URL) | Local Docker, Neon, Supabase, RDS, Cloud SQL, Azure Flexible Server |
| Cache | Redis 7+ (optional) | Local, Upstash, ElastiCache, Azure Cache — only for billing/offline grace |
| Runtime callers | Customer-installed runtime instances | — |
| Console callers | Your console | — |

One `DATABASE_URL` is canonical. `DATABASE_URL_DIRECT` is an optional alias if your provider needs a non-pooled URL (e.g. PgBouncer). For plain Postgres, just set `DATABASE_URL`.

## Auth surface (MVP)

Service API key via BetterAuth:

- Header: `Authorization: Bearer overture_<random>` (also accepts legacy `igris_` prefix)
- Lookup: `tenants.api_key_hash = sha256(key)` and `is_active = true`
- Tenant scope: every route filters by `tenant_id`

Tests in `api/auth_apikey_integration_test.go` and `api/auth_apikey_tenant_scoping_test.go` lock this contract.

### Issuing the console service key

**A. CLI — recommended for first deploy.** No console login required.

```bash
# Fresh DB — create tenant row and mint key in one shot:
./overture tenant-key \
  --email you@example.com \
  --name "Console Operator" \
  --create-if-missing

# Existing tenant — rotate key:
./overture tenant-key --email you@example.com
```

stdout = raw `overture_<…>` key (also valid as `igris_`). stderr = trace. Raw key is **never** logged — capture stdout and set as secret `OVERTURE_API_KEY` in your platform, then restart.

The CLI uses `DATABASE_URL_DIRECT` if set, otherwise `DATABASE_URL`. It stores `sha256(key)` same as `POST /v1/account/api-key`.

**B. HTTP rotation — once you have a valid session.**

```bash
curl -X POST https://api.example.com/v1/account/api-key \
  -H "Authorization: Bearer $EXISTING_KEY"
# → { "api_key": "overture_…", "prefix": "overture_xxxxxx", "created_at": "…" }
```

Either path revokes previous key. To revoke without replacement: `DELETE /v1/account/api-key` or
`psql "$DATABASE_URL" -c "UPDATE tenants SET api_key_hash=NULL, api_key_prefix=NULL WHERE tenant_email=lower('you@example.com')"`.

## Env vars (provider-agnostic)

| Var | Required | Purpose |
|---|---|---|
| `ENV` | yes | `production` flips safety paths |
| `PORT` | yes | Container port; default `8080` |
| `DATABASE_URL` | yes | Postgres connection string (`postgres://user:pass@host:5432/overture?sslmode=require` or `?sslmode=disable` for local) |
| `DATABASE_URL_DIRECT` | optional | Non-pooled Postgres URL if your provider uses PgBouncer; otherwise omit |
| `BETTER_AUTH_SECRET` | yes | HMAC for session-cookie verification |
| `OVERTURE_RUNTIME_PUBLIC_KEY` | as used | Ed25519 hex public key for runtime verify (also accepts `IGRIS_RUNTIME_PUBLIC_KEY`) |
| `OVERTURE_SIGNING_KEY` | as used | Ed25519 private key hex for governed decisions |
| `OVERTURE_EXECUTION_INPUT_REF_KEYS` | as used | AES-GCM keyring for encrypted input refs |
| `POLAR_WEBHOOK_SECRET` | as used | Polar.sh billing webhook signing |
| `RESEND_API_KEY` | as used | Outbound email for trial reminders |
| `ALLOWED_ORIGINS` | yes | Comma-separated console origin |
| `USE_REDIS` | optional | `true` enables PolarClient (billing). Without Redis, billing is offline |
| `REDIS_URL` | if above | Redis connection string (`redis://` or `rediss://`) |

Never commit real values. Set as secrets in your platform.

## Postgres setup (any provider)

1. Provision Postgres 16+ (Docker, Neon, Supabase, RDS, etc).
2. Single connection string → `DATABASE_URL`. Add `?sslmode=require` for managed hosts, `?sslmode=disable` for local Docker.
3. If your provider uses PgBouncer, add a direct (non-pooled) URL as `DATABASE_URL_DIRECT` — Overture’s advisory locks need it. If not, omit it.

### Running migrations

```bash
# With DATABASE_URL exported (or DATABASE_URL_DIRECT if you have it)
for f in $(ls -1 database/migrations/*.sql | sort); do
  echo ">> $f"
  psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f "$f"
done
```

Add new migrations in `database/migrations/` with incrementing prefixes. No in-app runner yet — apply SQL manually, then redeploy.

## Build & run

```bash
# Local dev (Postgres + Redis + Overture)
docker compose up --build

# Bare binary
go run ./cmd/server server
# or build
docker build -t overture:dev .
docker run -e DATABASE_URL=postgres://... -e BETTER_AUTH_SECRET=... -p 8080:8080 overture:dev
```

`Dockerfile` is generic multi-stage `golang:1.24` → `distroless/static` — push to any registry (`ghcr.io`, ECR, GCR, ACR) and run where you want.

## Health checks

| Endpoint | Auth | Purpose |
|---|---|---|
| `/healthz` | no | Liveness — 200 if process up |
| `/readyz` | no | Readiness — 200 only if DB ping succeeds |
| `/v1/health` | no | Detailed — sub-component status |

Wire your provider’s probes to `/healthz` (liveness) and `/readyz` (readiness).

## Smoke test (after deploy)

```bash
# liveness
curl -fsS https://api.example.com/healthz | jq

# auth gate (should 401 without a key)
curl -i https://api.example.com/v1/actions

# auth gate (should 200 with valid key)
curl -i https://api.example.com/v1/actions \
  -H "Authorization: Bearer $OVERTURE_API_KEY"

# tenant scoping (every row must belong to that tenant)
curl -fsS https://api.example.com/v1/actions \
  -H "Authorization: Bearer $OVERTURE_API_KEY" | jq '.actions | length'
```

## What stays out of Go

- UI rendering (console owns it).
- Per-user session UX (console forwards API key only).
- Local execution (runtime).

If a feature wants to live in Go, it must be one of: action storage, policy, routing, recovery, proof, receipts, runtime coordination, task state, billing webhooks, or operational metrics. Everything else is a red flag.
