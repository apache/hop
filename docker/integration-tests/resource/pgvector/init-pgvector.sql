/**
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
*/

-- Runs once, on first start of an empty data directory. The pgvector image ships the
-- extension but does not enable it, and a "vector" column cannot be created until it is.
CREATE EXTENSION IF NOT EXISTS vector;

-- A table where deleting one particular document fails, so an integration test can prove that
-- the rejected delete does not take the rest of the run down with it. PostgreSQL aborts the whole
-- transaction on a failed statement: without a savepoint, the row for doc-guard-b would be
-- rejected too, because its own delete would hit "current transaction is aborted".
CREATE TABLE hop_it_delete_guard (
  id TEXT PRIMARY KEY,
  document_id TEXT,
  chunk_index INTEGER,
  content TEXT,
  embedding vector(3)
);

CREATE FUNCTION hop_it_refuse_delete() RETURNS trigger AS $$
BEGIN
  IF OLD.document_id = 'doc-guard-a' THEN
    RAISE EXCEPTION 'deletes are refused for % by design', OLD.document_id;
  END IF;
  RETURN OLD;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER hop_it_refuse_delete
  BEFORE DELETE ON hop_it_delete_guard
  FOR EACH ROW EXECUTE FUNCTION hop_it_refuse_delete();

-- One row per document. The trigger is FOR EACH ROW, so it only fires when the delete actually
-- matches something: both documents need a seeded row for their delete to reach the trigger.
INSERT INTO hop_it_delete_guard (id, document_id, chunk_index, content, embedding) VALUES
  ('seed-a', 'doc-guard-a', 0, 'seeded a', '[1,0,0]'),
  ('seed-b', 'doc-guard-b', 0, 'seeded b', '[0,1,0]');
