-- +goose Up
CREATE TABLE inbox_messages (
  consumer_name text        NOT NULL,
  message_id    text        NOT NULL,
  payload_hash  bytea       NOT NULL,
  received_at   timestamptz NOT NULL,
  processed_at  timestamptz,
  PRIMARY KEY (consumer_name, message_id)
);

CREATE TABLE outbox_events (
  id              uuid        PRIMARY KEY,
  aggregate_type  text        NOT NULL,
  aggregate_id    uuid        NOT NULL,
  partition_key   uuid        NOT NULL,
  event_type      text        NOT NULL,
  event_version   int         NOT NULL,
  payload         jsonb       NOT NULL,
  occurred_at     timestamptz NOT NULL,
  attempts        int         NOT NULL DEFAULT 0 CHECK (attempts >= 0),
  next_attempt_at timestamptz NOT NULL DEFAULT now(),
  locked_by       text,
  locked_until    timestamptz,
  published_at    timestamptz,
  last_error      text
);

CREATE INDEX ix_outbox_due ON outbox_events (next_attempt_at) WHERE published_at IS NULL;

-- +goose StatementBegin
CREATE FUNCTION forbid_outbox_content_change() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP = 'DELETE'
     OR NEW.payload IS DISTINCT FROM OLD.payload
     OR NEW.event_type IS DISTINCT FROM OLD.event_type
     OR NEW.aggregate_id IS DISTINCT FROM OLD.aggregate_id
     OR NEW.occurred_at IS DISTINCT FROM OLD.occurred_at THEN
    RAISE EXCEPTION 'outbox event % content is immutable', OLD.id USING ERRCODE = 'P0001';
  END IF;
  RETURN NEW;
END $$;
-- +goose StatementEnd

CREATE TRIGGER outbox_content_guard BEFORE UPDATE OR DELETE ON outbox_events
  FOR EACH ROW EXECUTE FUNCTION forbid_outbox_content_change();

-- +goose Down
DROP TABLE outbox_events;
DROP FUNCTION forbid_outbox_content_change();
DROP TABLE inbox_messages;
