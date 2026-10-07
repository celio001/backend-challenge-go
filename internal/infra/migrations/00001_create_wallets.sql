-- +goose Up
CREATE TABLE wallets (
  id            uuid        PRIMARY KEY,
  player_id     uuid        NOT NULL,
  currency      char(3)     NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
  balance_minor bigint      NOT NULL CHECK (balance_minor >= 0),
  version       bigint      NOT NULL CHECK (version >= 1),
  created_at    timestamptz NOT NULL,
  updated_at    timestamptz NOT NULL,
  UNIQUE (player_id, currency),
  UNIQUE (id, player_id),
  UNIQUE (id, currency)
);

-- +goose Down
DROP TABLE wallets;
