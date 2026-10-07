-- +goose Up
CREATE TABLE wallet_ledger_entries (
  id                   uuid        PRIMARY KEY,
  wallet_id            uuid        NOT NULL,
  transaction_id       uuid        NOT NULL REFERENCES wager_transactions (id),
  wallet_version       bigint      NOT NULL CHECK (wallet_version >= 1),
  direction            text        NOT NULL CHECK (direction IN ('DEBIT','CREDIT')),
  amount_minor         bigint      NOT NULL CHECK (amount_minor > 0),
  currency             char(3)     NOT NULL,
  balance_before_minor bigint      NOT NULL CHECK (balance_before_minor >= 0),
  balance_after_minor  bigint      NOT NULL CHECK (balance_after_minor >= 0),
  created_at           timestamptz NOT NULL DEFAULT now(),
  FOREIGN KEY (wallet_id, currency) REFERENCES wallets (id, currency),
  UNIQUE (wallet_id, transaction_id),
  UNIQUE (wallet_id, wallet_version),
  CHECK (
    (direction = 'CREDIT' AND balance_after_minor = balance_before_minor + amount_minor) OR
    (direction = 'DEBIT'  AND balance_after_minor = balance_before_minor - amount_minor))
);

-- +goose StatementBegin
CREATE FUNCTION forbid_ledger_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'wallet_ledger_entries is append-only' USING ERRCODE = 'P0001';
END $$;
-- +goose StatementEnd

CREATE TRIGGER ledger_no_update_delete BEFORE UPDATE OR DELETE ON wallet_ledger_entries
  FOR EACH ROW EXECUTE FUNCTION forbid_ledger_mutation();
CREATE TRIGGER ledger_no_truncate BEFORE TRUNCATE ON wallet_ledger_entries
  FOR EACH STATEMENT EXECUTE FUNCTION forbid_ledger_mutation();

-- +goose Down
DROP TABLE wallet_ledger_entries;
DROP FUNCTION forbid_ledger_mutation();
