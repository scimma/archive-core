BEGIN;

ALTER TABLE messages ALTER COLUMN topic SET NOT NULL;
ALTER TABLE messages ALTER COLUMN uuid SET NOT NULL;
ALTER TABLE messages ALTER COLUMN "size" SET NOT NULL;
ALTER TABLE messages ALTER COLUMN "key" SET NOT NULL;
ALTER TABLE messages ALTER COLUMN "bucket" SET NOT NULL;
ALTER TABLE messages ALTER COLUMN crc32 SET NOT NULL;
ALTER TABLE messages ALTER COLUMN is_client_uuid SET NOT NULL;
ALTER TABLE messages ALTER COLUMN public SET NOT NULL;
ALTER TABLE messages ALTER COLUMN direct_upload SET NOT NULL;
ALTER TABLE messages ALTER COLUMN message_crc32 SET NOT NULL;

DO LANGUAGE plpgsql $$ BEGIN
IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'messages_uuid_idx' AND contype = 'u')
THEN
	RAISE NOTICE 'constraint "messages_uuid_idx" does not exist';
	DROP INDEX IF EXISTS messages_uuid_idx;
	ALTER TABLE messages ADD CONSTRAINT messages_uuid_idx UNIQUE (uuid);
ELSE
	RAISE NOTICE 'constraint "messages_uuid_idx" already exists, skipping';
END IF;
END $$;

ALTER TABLE messages ADD COLUMN IF NOT EXISTS title text DEFAULT '' NOT NULL;
ALTER TABLE messages ADD COLUMN IF NOT EXISTS sender text DEFAULT '' NOT NULL;
ALTER TABLE messages ADD COLUMN IF NOT EXISTS media_type text DEFAULT '' NOT NULL;
ALTER TABLE messages ADD COLUMN IF NOT EXISTS file_name text DEFAULT '' NOT NULL;

COMMIT;