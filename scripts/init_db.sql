-- PostgreSQL initialization script
-- Creates additional databases if needed

-- Airbyte gets its own DB (managed by its own postgres container)
-- This script adds any shared metadata tables

CREATE TABLE IF NOT EXISTS pipeline_runs (
    id          SERIAL PRIMARY KEY,
    dag_id      VARCHAR(255) NOT NULL,
    run_id      VARCHAR(255) NOT NULL,
    status      VARCHAR(50)  NOT NULL DEFAULT 'running',
    started_at  TIMESTAMP    NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMP,
    rows_processed BIGINT,
    notes       TEXT
);

CREATE INDEX IF NOT EXISTS idx_pipeline_runs_dag_id ON pipeline_runs(dag_id);
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_started_at ON pipeline_runs(started_at DESC);

COMMENT ON TABLE pipeline_runs IS 'ETL pipeline run history';
