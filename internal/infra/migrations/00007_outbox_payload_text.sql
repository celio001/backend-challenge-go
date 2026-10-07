-- +goose Up
-- jsonb rewrites the document (key order, spacing); text keeps the frozen snapshot byte for byte.
ALTER TABLE outbox_events
  ALTER COLUMN payload TYPE text USING payload::text,
  ADD CONSTRAINT outbox_payload_is_json CHECK (payload::jsonb IS NOT NULL);

-- +goose Down
ALTER TABLE outbox_events
  DROP CONSTRAINT outbox_payload_is_json,
  ALTER COLUMN payload TYPE jsonb USING payload::jsonb;
