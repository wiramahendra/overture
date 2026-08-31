-- 075_fix_rls_session: make set_tenant_context session-level for pooled connections
-- Previous 004 used is_local=true (SET LOCAL) which is transaction-local and was lost after Exec's single-statement transaction.
-- Now is_local=false (SET) so the tenant sticks for the pooled connection's session until reset. Middleware resets after request.
CREATE OR REPLACE FUNCTION set_tenant_context(p_tenant_id TEXT)
RETURNS VOID AS $$
BEGIN
    PERFORM set_config('app.current_tenant', p_tenant_id, false);
END;
$$ LANGUAGE plpgsql;

-- Helper to clear tenant (called after request)
CREATE OR REPLACE FUNCTION clear_tenant_context()
RETURNS VOID AS $$
BEGIN
    PERFORM set_config('app.current_tenant', '', false);
END;
$$ LANGUAGE plpgsql;
