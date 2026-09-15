/*
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

/*
DuckDB has a JSON and a UUID type of its own, and the driver reports both as Types.OTHER with only
the type name to tell them apart. Hop has a value type for each, so a column of either has to come
back as that type rather than as an anonymous string.

Every row carries the expected rendering of its own columns as plain text, so the pipeline compares
what Hop read with what DuckDB holds without hard coding a value in a transform.
*/

DROP TABLE IF EXISTS main.special_types;

CREATE TABLE main.special_types
(
  id     INTEGER
, label  VARCHAR(50)
, v_json JSON
, v_uuid UUID
, x_json VARCHAR(200)
, x_uuid VARCHAR(50)
);

/* A type that reads a null wrong is as broken as one that reads a value wrong. */
INSERT INTO main.special_types VALUES
  (1, 'all null', NULL, NULL, '<null>', '<null>');

INSERT INTO main.special_types VALUES
  (2, 'object'
    , '{"a":1}', '123e4567-e89b-12d3-a456-426614174000'
    , '{"a":1}', '123e4567-e89b-12d3-a456-426614174000');

INSERT INTO main.special_types VALUES
  (3, 'nested object'
    , '{"a":1,"b":[2,3]}', '00000000-0000-0000-0000-000000000000'
    , '{"a":1,"b":[2,3]}', '00000000-0000-0000-0000-000000000000');

INSERT INTO main.special_types VALUES
  (4, 'array'
    , '[1,2,3]', 'ffffffff-ffff-ffff-ffff-ffffffffffff'
    , '[1,2,3]', 'ffffffff-ffff-ffff-ffff-ffffffffffff');
