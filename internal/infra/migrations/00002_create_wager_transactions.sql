-- +goose Up
CREATE TABLE wager_transactions (
  id                                uuid    PRIMARY KEY,
  origin                            text    NOT NULL CHECK (origin IN ('INTERNAL','EXTERNAL')),
  kind                              text    NOT NULL CHECK (kind IN ('OPENING','BET','WIN','LOSS','REFUND','ROLLBACK')),
  status                            text    NOT NULL CHECK (status IN ('PENDING','PENDING_REFERENCE','PROCESSED','REJECTED','FAILED')),
  wallet_id                         uuid    NOT NULL,
  player_id                         uuid    NOT NULL,
  amount_minor                      bigint  NOT NULL CHECK (amount_minor >= 0),
  currency                          char(3) NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
  provider_id                       text,
  external_transaction_id           text,
  idempotency_key                   text,
  payload_hash                      bytea   CHECK (octet_length(payload_hash) = 32),
  round_id                          text,
  game_id                           text,
  reference_external_transaction_id text,
  reference_transaction_id          uuid    REFERENCES wager_transactions (id),
  failure_code                      text,
  result_balance_minor              bigint  CHECK (result_balance_minor >= 0),
  correlation_id                    text,
  attempts                          int     NOT NULL DEFAULT 0 CHECK (attempts >= 0),
  next_attempt_at                   timestamptz,
  expires_at                        timestamptz,
  locked_until                      timestamptz,
  created_at                        timestamptz NOT NULL,
  updated_at                        timestamptz NOT NULL,

  -- No currency here on purpose: a CURRENCY_MISMATCH rejection must be storable.
  FOREIGN KEY (wallet_id, player_id) REFERENCES wallets (id, player_id),

  CONSTRAINT origin_shape CHECK (
    (origin = 'INTERNAL' AND kind = 'OPENING'
       AND provider_id IS NULL AND external_transaction_id IS NULL AND idempotency_key IS NULL
       AND payload_hash IS NULL AND round_id IS NULL AND game_id IS NULL
       AND reference_external_transaction_id IS NULL)
    OR
    (origin = 'EXTERNAL' AND kind <> 'OPENING'
       AND provider_id IS NOT NULL AND external_transaction_id IS NOT NULL
       AND idempotency_key IS NOT NULL AND payload_hash IS NOT NULL
       AND round_id IS NOT NULL AND game_id IS NOT NULL)),
  CONSTRAINT zero_policy CHECK (
    (kind = 'LOSS' AND amount_minor = 0) OR (kind <> 'LOSS' AND amount_minor > 0)),
  CONSTRAINT reversal_needs_ref CHECK (
    kind NOT IN ('REFUND','ROLLBACK') OR reference_external_transaction_id IS NOT NULL),
  CONSTRAINT failure_iff_unsuccessful CHECK (
    (status IN ('REJECTED','FAILED')) = (failure_code IS NOT NULL)),
  CONSTRAINT processed_has_balance CHECK (
    status <> 'PROCESSED' OR result_balance_minor IS NOT NULL),
  CONSTRAINT pending_ref_scheduled CHECK (
    status <> 'PENDING_REFERENCE' OR (next_attempt_at IS NOT NULL AND expires_at IS NOT NULL))
);

CREATE UNIQUE INDEX ux_wtx_provider_key ON wager_transactions (provider_id, idempotency_key)
  WHERE origin = 'EXTERNAL';
CREATE UNIQUE INDEX ux_wtx_provider_ext ON wager_transactions (provider_id, external_transaction_id)
  WHERE origin = 'EXTERNAL';
CREATE UNIQUE INDEX ux_wtx_one_opening ON wager_transactions (wallet_id)
  WHERE kind = 'OPENING';
CREATE UNIQUE INDEX ux_wtx_one_reversal ON wager_transactions (reference_transaction_id)
  WHERE status = 'PROCESSED' AND kind IN ('REFUND','ROLLBACK');
CREATE INDEX ix_wtx_pending_ref_due ON wager_transactions (next_attempt_at)
  WHERE status = 'PENDING_REFERENCE';
CREATE INDEX ix_wtx_waiting_on ON wager_transactions (provider_id, reference_external_transaction_id)
  WHERE status = 'PENDING_REFERENCE';

-- +goose StatementBegin
CREATE FUNCTION forbid_terminal_transition() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP = 'DELETE' THEN
    RAISE EXCEPTION 'wager_transactions rows cannot be deleted' USING ERRCODE = 'P0001';
  END IF;
  IF OLD.status IN ('PROCESSED','REJECTED','FAILED') THEN
    RAISE EXCEPTION 'wager transaction % is terminal', OLD.id USING ERRCODE = 'P0001';
  END IF;
  RETURN NEW;
END $$;
-- +goose StatementEnd

CREATE TRIGGER wtx_terminal_guard BEFORE UPDATE OR DELETE ON wager_transactions
  FOR EACH ROW EXECUTE FUNCTION forbid_terminal_transition();

-- +goose Down
DROP TABLE wager_transactions;
DROP FUNCTION forbid_terminal_transition();
