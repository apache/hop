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
DuckDB's TIME carries a time of day and no date, which is the type issue #3744 could not read:
the driver cannot produce a Timestamp for it, and reading a date through getTimestamp is what Hop
does for every date column.

Every row carries the expected rendering of its own TIME column as plain text, so the pipeline
compares what Hop read with what DuckDB holds without hard coding a value in a transform.

Hop holds a date to the millisecond, so the fixtures stop there. DuckDB's TIME goes to the
microsecond; a value with more precision than Hop can hold would be testing rounding rather than
reading.
*/

DROP TABLE IF EXISTS main.time_samples;

CREATE TABLE main.time_samples
(
  id     INTEGER
, label  VARCHAR(50)
, t_time TIME
, x_time VARCHAR(20)
);

/* A type that reads a null wrong is as broken as one that reads a value wrong. */
INSERT INTO main.time_samples VALUES (1, 'all null', NULL, '<null>');
INSERT INTO main.time_samples VALUES (2, 'midnight', TIME '00:00:00', '00:00:00.000');
INSERT INTO main.time_samples VALUES (3, 'whole second', TIME '09:08:07', '09:08:07.000');
INSERT INTO main.time_samples VALUES (4, 'milliseconds', TIME '11:22:33.456', '11:22:33.456');
INSERT INTO main.time_samples VALUES (5, 'last millisecond of the day', TIME '23:59:59.999', '23:59:59.999');
