# Overture

Safe execution boundary for consequential operations triggered by automated systems.

Overture lets you keep your existing agent, application, and tools while routing consequential work through a durable boundary:

**Action -> Run -> Proof**

- **Action** is the named, configured operation the operator allows.
- **Run** is one durable attempt to execute an Action, including idempotency, policy, approval, failure, and recovery.
- **Proof** is the inspectable record of what was authorized, dispatched, observed, and verified.

If the system cannot determine whether an external effect occurred, it enters an explicit uncertain state and requires exceptional **Reconciliation** - an authenticated, auditable human decision - instead of blindly replaying the effect.

## Wedge

Initial supported actions:

- `deploy.staging`
- `deploy.production`
- `migrate.database`
- `publish.package`

## How it works

```
Client -> POST /v1/actions/:name/run {idempotency_key} -> Overture -> dispatch to runtime -> callback -> Proof
```

- Tenant-scoped idempotency prevents duplicate submissions.
- Policy and permission envelope are signed and verified.
- Receipts are hash-chained and Ed25519-signed.
- Recovery is conservative - irreversible work is never blindly retried.

REST is the canonical interface. The Python SDK is a thin wrapper:

```python
from overture import Overture

client = Overture.from_env()
action = client.configure_action("deploy.staging", ...)
run = action.run(input={"service": "api", "commit": "abc123"}, idempotency_key="deploy:api:abc123")
run.wait()
proof = run.proof()
```

## Run locally

```bash
go run ./cmd/server
# Postgres + Redis required - see docs/product/PRD.md
```

## What this is not

Not an agent framework, workflow builder, or replacement for your tools. It is a safety boundary for the few operations that must be observable, reviewable, and recoverable.
