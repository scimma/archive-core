-- Use a transation to absolutely
-- no miss any inserts into messages
-- while making the function and trigger

BEGIN;


-- When making the table, import history
-- The production table has a unique contraint  
-- On topic as part of the plan to keep the number of rows
-- constrained to the number of topics

CREATE TABLE IF NOT EXISTS topics AS
SELECT DISTINCT ON (topic) topic, 
                   min(timestamp) e_timestamp, 
                   max(timestamp) l_timestamp, 
                   COUNT(*) n_messages 
FROM messages
group by topic ORDER BY topic;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'unique_topic'
    ) THEN
        ALTER TABLE topics
        ADD CONSTRAINT unique_topic UNIQUE (topic);
    END IF;
END
$$;


--
-- Insert or update topics from messages
-- Trigger on inserts
--
CREATE OR REPLACE FUNCTION update_topics()
RETURNS TRIGGER AS $$
BEGIN
  IF TG_OP = 'INSERT' THEN
    -- If a row with the same topic exists, update its timestamp
    UPDATE topics
    SET e_timestamp = LEAST(e_timestamp, NEW.timestamp), 
        l_timestamp = GREATEST(l_timestamp, NEW.timestamp), 
        n_messages = n_messages + 1
    WHERE topic = NEW.topic;
    
    -- If no row was updated, insert a new one
    IF NOT FOUND THEN
      INSERT INTO topics (topic, e_timestamp, l_timestamp, n_messages)
      VALUES (NEW.topic, NEW.timestamp, NEW.timestamp, 1);
    END IF;
    -- RAISE NOTICE 'Inserted topic: %, timestamp: %', NEW.topic, NEW.timestamp;
  ELSIF TG_OP = 'DELETE' THEN
    UPDATE topics
    -- We do not bother to update the earliest and latest timestamps, 
    -- as deletions should be rare, and having too wide a timestamp range
    -- should only cause minor inefficiency
    SET n_messages = n_messages - 1
    WHERE topic = OLD.topic;
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;


DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname = 'trigger_update_topics'
    ) THEN
       CREATE TRIGGER trigger_update_topics
       AFTER INSERT OR DELETE ON messages
       FOR EACH ROW
       EXECUTE FUNCTION update_topics();
    END IF;
END;
$$;


-- end the transaction

COMMIT;
