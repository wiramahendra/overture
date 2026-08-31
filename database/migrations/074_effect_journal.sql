-- 074_effect_journal: effect journal for exactly-once + dispatch queue with SKIP LOCKED
-- Fixes heuristic irreversible detection (containsIrreversibleActionToken) -> explicit effect_class

-- effect_journal records each external side-effect before it is performed.
-- committed = true means the external call succeeded; recovery must not replay it.
CREATE TABLE IF NOT EXISTS effect_journal (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id TEXT NOT NULL,
    task_id UUID NOT NULL REFERENCES task_records(task_id) ON DELETE CASCADE,
    node_id TEXT NOT NULL,
    step_index INT NOT NULL,
    effect_class TEXT NOT NULL CHECK (effect_class IN ('idempotent','irreversible','retryable')),
    external_call_id TEXT,
    status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending','committed','failed')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    committed_at TIMESTAMPTZ,
    UNIQUE (tenant_id, task_id, node_id)
);

CREATE INDEX IF NOT EXISTS idx_effect_journal_task ON effect_journal(tenant_id, task_id);
CREATE INDEX IF NOT EXISTS idx_effect_journal_committed ON effect_journal(tenant_id, task_id, effect_class) WHERE status = 'committed';

-- task_dispatch_queue provides bounded concurrency via SELECT FOR UPDATE SKIP LOCKED.
-- Tasks are enqueued at Submit and dequeued by dispatcher workers.
CREATE TABLE IF NOT EXISTS task_dispatch_queue (
    queue_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id TEXT NOT NULL,
    task_id UUID NOT NULL REFERENCES task_records(task_id) ON DELETE CASCADE,
    status TEXT NOT NULL DEFAULT 'queued' CHECK (status IN ('queued','dispatched','failed')),
    attempts INT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    dispatched_at TIMESTAMPTZ,
    UNIQUE (tenant_id, task_id)
);

CREATE INDEX IF NOT EXISTS idx_dispatch_queue_queued ON task_dispatch_queue(tenant_id, created_at) WHERE status = 'queued';

-- Add effect_class to action execution graph validation: no schema change needed for task_definition JSON,
-- but add column to task_records for fast filtering (optional, populated from graph nodes).
DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='task_records' AND column_name='has_irreversible_effect') THEN
    ALTER TABLE task_records ADD COLUMN has_irreversible_effect BOOLEAN NOT NULL DEFAULT FALSE;
  END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_task_records_irreversible ON task_records(tenant_id, has_irreversible_effect) WHERE has_irreversible_effect = true;
