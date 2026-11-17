CREATE UNIQUE INDEX IF NOT EXISTS
    uq_reconciliation_run_layer
ON ops.reconciliation (run_id, layer_name);