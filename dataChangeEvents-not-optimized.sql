CREATE TABLE IF NOT EXISTS eventlogs
(
  eventlogid    BIGSERIAL NOT NULL
    CONSTRAINT eventlogs_pkey
    PRIMARY KEY,
  tablename     VARCHAR(64),
  action        VARCHAR(10),
  data          TEXT,
  tablerecordid VARCHAR(100),
  createddate   TIMESTAMP
);
CREATE INDEX IF NOT EXISTS eventlogs_created_date_index
  ON eventlogs USING BTREE (createddate);

CREATE TABLE IF NOT EXISTS eventlogexecutor
(
  eventlogexecutorid BIGSERIAL PRIMARY KEY,
  processed          BOOLEAN,
  batchid            VARCHAR(100),
  eventlogid         BIGINT,
  createddate        TIMESTAMP,
  modifieddate       TIMESTAMP
);

CREATE INDEX IF NOT EXISTS eventlogexecutor_eventlogid_btree_index
  ON eventlogexecutor USING BTREE (eventlogid);

CREATE INDEX IF NOT EXISTS eventlogexecutor_eventlogid_hash_index
  ON eventlogexecutor USING HASH (eventlogid);


-- Trigger notification for messaging to PG Notify
CREATE OR REPLACE FUNCTION dataChangeEvents()
  RETURNS TRIGGER AS $dataChangeEventTrigger$


-- Declaring variables required to process the trigger.
-- Description:

DECLARE
  newrecord             RECORD; -- Variable to maintain newrecord (actual record with record data type)
  oldrecord             RECORD; -- Variable to maintain oldrecord (actual record with record data type)
  payload               TEXT; -- The final result, prepared as json, the channel is notified with same json and also in db the same json will be stored.
  column_name           TEXT; -- The name of the column.
  column_value          TEXT; -- The value of the column.
  new_payload_items     TEXT []; -- The payload which is prepared for the tracked fields for newly changed values. applies for insert and update.
  old_payload_items     TEXT []; -- The payload which is prepared for the tracked fields for old values. applies for update.
  track_changed_columns TEXT []; -- The array which keeps track of the column names whoes values got changed (which are getting tracked).
  recordid              CHARACTER VARYING(100); -- The record identifier.
  newjson               JSONB; -- json created using Json 'B' datatype and from new record value.
  oldjson               JSONB; -- json created using Json 'B' datatype and from old record value.
  changedColumns        JSONB; -- the subtraction of newjson and old json using jsonb_minus custom function.
BEGIN

  -- Set record row depending on operation
  CASE TG_OP

    WHEN 'INSERT'
    THEN
      recordid = NEW.gsid;
      newrecord := NEW;
      oldrecord := NULL;
      changedColumns := to_jsonb(NEW);

    WHEN 'UPDATE'
    THEN
      newrecord := NEW;
      oldrecord := old;
      recordid = NEW.gsid;
      newjson = to_jsonb(NEW);
      oldjson = to_jsonb(old);
      changedColumns = jsonb_minus(newjson, oldjson);

    WHEN 'DELETE'
    THEN
      recordid = NEW.gsid;
      newrecord := OLD;
      changedColumns = to_jsonb(old);
  ELSE
    RAISE EXCEPTION 'Unknown TG_OP: "%". Should not occur!', TG_OP;
  END CASE;

  -- Uncomment for testing
  -- RAISE NOTICE 'Test1 %', changed;


  -- Loop around the argument passed to the function, which are column names,
  -- use these column names to track if they exist in changedColumns jsonb variable.
  FOREACH COLUMN_NAME IN ARRAY TG_ARGV LOOP

    -- check if the column name getting tracked exists inside the new changed columns (record)
    IF changedColumns ? column_name
    THEN
      RAISE NOTICE '%', COLUMN_NAME;

      -- Maintain the column name which is getting tracked in the array.
      track_changed_columns := array_append(track_changed_columns, column_name);
      EXECUTE FORMAT('SELECT $1.%I::TEXT', COLUMN_NAME)
      INTO column_value
      USING newrecord;

      new_payload_items := array_append(new_payload_items,
                                        '"' || REPLACE(COLUMN_NAME, '"', '\"') || '":"' ||
                                        REPLACE(column_value, '"', '\"')
                                        ||
                                        '"');
    END IF;
  END LOOP;

  -- Uncomment for testing
  -- RAISE NOTICE 'Test2 %', new_payload_items;

  -- If track_changed_columns is null that means nothing got changed for the columns we are tracking.
  -- then it clearly means we should not be tracking old column values.
  -- Example personName was 'John', think we are tracking personName column.
  -- Now if i update personName with John again should i insert a record into db. Answer is No.
  -- In business sense actually nothing got changed.

  IF track_changed_columns IS NOT NULL
  THEN
    FOREACH COLUMN_NAME IN ARRAY track_changed_columns LOOP
      IF oldjson ? COLUMN_NAME
      THEN
        RAISE NOTICE '%', COLUMN_NAME;
        EXECUTE FORMAT('SELECT $1.%I::TEXT', COLUMN_NAME)
        INTO column_value
        USING oldrecord;
        old_payload_items := array_append(old_payload_items,
                                          '"' || REPLACE(COLUMN_NAME, '"', '\"') || '":"' ||
                                          REPLACE(column_value, '"', '\"')
                                          ||
                                          '"');
      END IF;
    END LOOP;
  END IF;

  --   uncomment for testing
  --   RAISE NOTICE 'Test3 %', old_payload_items;

  -- Build the payload checking.
  -- if old values are not there, we will not keep the oldValues key itself.
  IF new_payload_items IS NOT NULL
  THEN
    payload := ''
               || '{'
               || '"newValues":{' || array_to_string(new_payload_items, ',') || '}';
  END IF;

  IF old_payload_items IS NOT NULL
  THEN
    payload := ||'", oldValues":{' || array_to_string(old_payload_items, ',') || '}'
               || '}';

  END IF;

  --   uncomment for testing
  --   RAISE NOTICE 'Test4 %', payload;

  --Check if payload have anything to actually post to channel and insert into db.
  IF payload IS NOT NULL
  THEN

    -- Notify the channel
    PERFORM PG_NOTIFY('db_notifications', payload);

    INSERT INTO eventlogs (tableName, ACTION, DATA, tablerecordid, createddate)
    VALUES (TG_TABLE_NAME, TG_OP, payload, recordid, now());

  END IF;

  -- The trigger returns null
  RETURN NULL;
END;
$dataChangeEventTrigger$
LANGUAGE plpgsql;


-- Custom function to subtract one jsonb from another.
CREATE OR REPLACE FUNCTION jsonb_minus(arg1 JSONB, arg2 JSONB)
  RETURNS JSONB
AS $$

SELECT COALESCE(json_object_agg(key, value), '{}') :: JSONB
FROM
    jsonb_each(arg1)
WHERE
  arg1 -> key <> arg2 -> key
  OR arg2 -> key IS NULL

$$
LANGUAGE plpgsql;

CREATE TRIGGER deptTrigger
  AFTER INSERT OR UPDATE OR DELETE
  ON dept
  FOR EACH ROW EXECUTE PROCEDURE datachangeevents('deptno', 'dname', 'loc');

-- Bibliography
-- https://www.postgresql.org/docs/9.1/static/plpgsql- TRIGGER.html
-- http://coussej.github.io/2016/05/24/A- MINUS - OPERATOR - FOR -PostgreSQLs-JSONB/
-- https:// GIST.github.com/colophonemes/9701b906c5be572a40a84b08f4d2fa4e
-- https://dba.stackexchange.com/questions/156980/create-a-trigger-on-all-the-last-modified-columns-in-postgresql
-- https://stackoverflow.com/questions/39859642/how-to-print-new-value-with-trigger-postgres