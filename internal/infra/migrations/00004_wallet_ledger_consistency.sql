-- +goose Up
-- Runs at COMMIT: the wallet row is updated before its ledger entry is inserted.
-- +goose StatementBegin
CREATE FUNCTION assert_wallet_ledger_consistency() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  prev_balance bigint := CASE WHEN TG_OP = 'INSERT' THEN 0 ELSE OLD.balance_minor END;
  prev_version bigint := CASE WHEN TG_OP = 'INSERT' THEN 0 ELSE OLD.version END;
BEGIN
  IF TG_OP = 'INSERT' AND NEW.balance_minor = 0 THEN
    RETURN NULL;
  END IF;
  IF TG_OP = 'UPDATE' AND NEW.balance_minor = OLD.balance_minor THEN
    IF NEW.version <> OLD.version THEN
      RAISE EXCEPTION 'wallet % version changed without balance change', NEW.id USING ERRCODE = 'P0001';
    END IF;
    RETURN NULL;
  END IF;
  IF TG_OP = 'UPDATE' AND NEW.version <> prev_version + 1 THEN
    RAISE EXCEPTION 'wallet % version must increase by exactly 1', NEW.id USING ERRCODE = 'P0001';
  END IF;
  PERFORM 1 FROM wallet_ledger_entries
   WHERE wallet_id = NEW.id
     AND wallet_version = NEW.version
     AND balance_before_minor = prev_balance
     AND balance_after_minor  = NEW.balance_minor;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'wallet % balance change has no matching ledger entry', NEW.id USING ERRCODE = 'P0001';
  END IF;
  RETURN NULL;
END $$;
-- +goose StatementEnd

CREATE CONSTRAINT TRIGGER wallets_ledger_consistency
  AFTER INSERT OR UPDATE ON wallets
  DEFERRABLE INITIALLY DEFERRED
  FOR EACH ROW EXECUTE FUNCTION assert_wallet_ledger_consistency();

-- +goose Down
DROP TRIGGER wallets_ledger_consistency ON wallets;
DROP FUNCTION assert_wallet_ledger_consistency();
