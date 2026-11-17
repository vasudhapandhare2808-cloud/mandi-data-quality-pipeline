CREATE UNIQUE INDEX IF NOT EXISTS uq_dq_results_run_rule
ON ops.dq_results (run_id, rule_name);
