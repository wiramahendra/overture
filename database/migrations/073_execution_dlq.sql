-- 073_execution_dlq: dead-letter queue for durable execution
-- First-class execution requires bounded retries.

CREATE TABLE IF NOT EXISTS execution_dlq (
    task_id UUID PRIMARY KEY REFERENCES task_records(task_id) ON DELETE CASCADE,
    tenant_id TEXT NOT NULL,
    attempts INT NOT NULL DEFAULT 0,
    last_error TEXT,
    last_error_details JSONB,
    enqueued_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    next_retry_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_execution_dlq_tenant_enqueued ON execution_dlq(tenant_id, enqueued_at DESC);
CREATE INDEX IF NOT EXISTS idx_execution_dlq_next_retry ON execution_dlq(next_retry_at) WHERE next_retry_at IS NOT NULL;

-- Add attempt tracking to task_records if not exists
DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='task_records' AND column_name='attempt_count') THEN
    ALTER TABLE task_records ADD COLUMN attempt_count INT NOT NULL DEFAULT 0;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='task_records' AND column_name='next_retry_at') THEN
    ALTER TABLE task_records ADD COLUMN next_retry_at TIMESTAMPTZ;
  END IF;
END $$;
