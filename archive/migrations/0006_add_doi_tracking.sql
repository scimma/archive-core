BEGIN;

CREATE TABLE IF NOT EXISTS dois (
	id BIGSERIAL PRIMARY KEY,
	doi text NOT NULL,
	record_url text,
	title text,
	created_by text,
	created_at timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS dois_doi_idx ON dois
USING btree (doi);

CREATE TABLE IF NOT EXISTS doi_messages (
	doi_id bigint NOT NULL REFERENCES dois (id) ON DELETE CASCADE,
	message_uuid uuid NOT NULL,
	PRIMARY KEY (doi_id, message_uuid)
);

CREATE INDEX IF NOT EXISTS doi_messages_message_uuid_idx ON doi_messages
USING btree (message_uuid);

-- end the transaction
COMMIT;
