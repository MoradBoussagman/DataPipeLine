-- ============================================================
-- 00_metabase_setup.sql
-- Runs automatically on first PostgreSQL initialization
-- Creates Metabase role and metadata database if missing
-- ============================================================

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'metabase') THEN
        CREATE ROLE metabase LOGIN PASSWORD 'mysecretpassword';
    END IF;
END
$$;

SELECT 'CREATE DATABASE metabaseappdb OWNER metabase'
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'metabaseappdb')\gexec

GRANT ALL PRIVILEGES ON DATABASE metabaseappdb TO metabase;
