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

-- Dedicated fixture for the Table Input JDBC partitioned read (PR #9353).
-- Deliberately NOT part of script2.sql: other workflows consume those tables.
-- 12 rows exercising: NULL partition value, values below the lower bound, exact
-- lower/upper bound, partition boundaries with stride 20 (0..80 / 4 partitions),
-- values just before/after each boundary.
-- row_id is the stable unique key for golden ordering; partition_id is nullable
-- and payload is non-null.

DROP TABLE IF EXISTS table_input_partitioned;

CREATE TABLE table_input_partitioned (
  row_id        INTEGER     NOT NULL PRIMARY KEY,
  partition_id  INTEGER     NULL,
  payload       VARCHAR(40) NOT NULL
);

INSERT INTO table_input_partitioned (row_id, partition_id, payload) VALUES
  (1,  NULL, 'null-partition'),
  (2,  -10,  'below-lower'),
  (3,  0,    'lower-bound'),
  (4,  19,   'before-first'),
  (5,  20,   'first-boundary'),
  (6,  39,   'before-second'),
  (7,  40,   'second-boundary'),
  (8,  59,   'before-third'),
  (9,  60,   'third-boundary'),
  (10, 79,   'before-upper'),
  (11, 80,   'upper-bound'),
  (12, 100,  'above-upper');