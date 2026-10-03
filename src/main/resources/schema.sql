CREATE TABLE IF NOT EXISTS accounts (
    id            VARCHAR(64) PRIMARY KEY,
    balance_minor BIGINT NOT NULL
);

-- The PRIMARY KEY is doing the real work. Two consumers racing on the same event:
-- one insert lands, the other conflicts and backs out. Both commit consistently.
--
-- This table grows forever. In production, partition it by day or delete rows
-- older than your maximum redelivery window.
CREATE TABLE IF NOT EXISTS processed_events (
    event_id     VARCHAR(64) PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO accounts (id, balance_minor)
VALUES ('acct-1', 0)
ON CONFLICT (id) DO NOTHING;
