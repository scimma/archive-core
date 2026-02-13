BEGIN;

CREATE TABLE IF NOT EXISTS messages (
	id BIGSERIAL PRIMARY KEY,
	topic text,
	timestamp bigint NOT NULL,
	uuid uuid,
	size integer,
	key text,
	bucket text,
	crc32 bigint,
	is_client_uuid boolean,
	public boolean,
	direct_upload boolean,
	message_crc32 bigint
);

CREATE INDEX IF NOT EXISTS messages_key_idx ON messages
USING btree (key);

CREATE INDEX IF NOT EXISTS messages_timestamp_id_idx ON messages
USING btree ("timestamp", id)
INCLUDE (topic, public);

-- CREATE INDEX IF NOT EXISTS messages_timestamp_id_idx2 ON messages
-- USING btree ("timestamp" DESC, id DESC)
-- INCLUDE (topic, public);

CREATE INDEX IF NOT EXISTS messages_timestamp_idx ON messages
USING btree ("timestamp");

CREATE INDEX IF NOT EXISTS messages_topic_idx ON messages
USING btree (topic);

CREATE INDEX IF NOT EXISTS messages_uuid_idx ON messages
USING btree (uuid);

-- end the transaction
COMMIT;