-- +goose Up
-- The role is cluster-wide, so it may already exist; its login credentials are provisioned outside the migrations.
-- +goose StatementBegin
DO $$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'wallet_app') THEN
    CREATE ROLE wallet_app LOGIN;
  END IF;
END $$;
-- +goose StatementEnd

REVOKE ALL ON ALL TABLES IN SCHEMA public FROM wallet_app;
GRANT SELECT, INSERT         ON wallet_ledger_entries, inbox_messages TO wallet_app;
GRANT SELECT, INSERT, UPDATE ON wallets, wager_transactions, outbox_events TO wallet_app;
GRANT UPDATE (processed_at)  ON inbox_messages TO wallet_app;

-- +goose Down
REVOKE ALL ON ALL TABLES IN SCHEMA public FROM wallet_app;
