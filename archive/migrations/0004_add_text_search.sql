BEGIN;

CREATE TABLE IF NOT EXISTS message_ts (
	uuid uuid REFERENCES messages (uuid) ON DELETE CASCADE NOT NULL,
	text_data tsvector NOT NULL,
	text_fully_indexed boolean NOT NULL
);

CREATE INDEX IF NOT EXISTS message_ts_idx ON message_ts
USING GIN (text_data);

-- end the transaction
COMMIT;