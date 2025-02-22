-- Initialize raw schema for 340B OPAIS data
-- File: sql/init.sql

-- Create raw schema
CREATE SCHEMA IF NOT EXISTS raw_340b;

-- Raw covered entities table - stores JSON data with minimal transformation
CREATE TABLE IF NOT EXISTS raw_340b.covered_entities (
    _loaded_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    ce_id INTEGER PRIMARY KEY,  -- Changed from composite key to single primary key
    id_340b TEXT,   -- kept as TEXT instead of VARCHAR to match original
    data JSONB NOT NULL,  -- kept JSONB and NOT NULL constraint
    data_hash VARCHAR(64),
    -- Removed old PRIMARY KEY (ce_id, _loaded_at) definition
);

-- Create indexes for efficient querying and CDC
CREATE INDEX IF NOT EXISTS idx_covered_entities_loaded_at ON raw_340b.covered_entities(_loaded_at);
CREATE INDEX IF NOT EXISTS idx_covered_entities_id340b ON raw_340b.covered_entities(id_340b);
CREATE INDEX IF NOT EXISTS idx_covered_entities_data ON raw_340b.covered_entities USING gin (data);

-- Create audit table for tracking loads
CREATE TABLE IF NOT EXISTS raw_340b.load_audit (
    load_id SERIAL PRIMARY KEY,
    start_time TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    end_time TIMESTAMP WITH TIME ZONE,
    records_processed INTEGER,
    status TEXT,
    error_message TEXT
);