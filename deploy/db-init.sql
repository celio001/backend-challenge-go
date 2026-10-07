-- Idempotent: creates the two application roles and hands the database to the migration owner.
-- wallet_owner owns the tables (runs migrations); wallet_app is what the replicas use and has no DDL rights.
-- Migration 00006 grants wallet_app its table privileges, and skips CREATE ROLE because the role exists here.
SELECT format('CREATE ROLE wallet_owner LOGIN PASSWORD %L', :'owner_password')
 WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'wallet_owner') \gexec
SELECT format('ALTER ROLE wallet_owner PASSWORD %L', :'owner_password') \gexec

SELECT format('CREATE ROLE wallet_app LOGIN PASSWORD %L', :'app_password')
 WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'wallet_app') \gexec
SELECT format('ALTER ROLE wallet_app PASSWORD %L', :'app_password') \gexec

SELECT format('ALTER DATABASE %I OWNER TO wallet_owner', current_database()) \gexec
