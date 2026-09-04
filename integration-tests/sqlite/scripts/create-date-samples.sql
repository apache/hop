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
SQLite has no date or time storage class. A date lives in a TEXT, REAL or INTEGER column and the
declared type only gives that column an affinity, so a single DATE column can hold any of the
formats SQLite accepts. The rows below cover the ones https://www.sqlite.org/lang_datefunc.html
lists as valid date/time values.

Every row carries the expected rendering of its own date columns as plain text, so a test can
compare what Hop read with what SQLite holds without hard coding anything in the pipeline.
*/

DROP TABLE IF EXISTS date_samples;

CREATE TABLE date_samples
(
  id          INTEGER PRIMARY KEY
, label       TEXT
, d_date      DATE
, d_datetime  DATETIME
, d_timestamp TIMESTAMP
, x_date      TEXT
, x_datetime  TEXT
, x_timestamp TEXT
);

/*
Row 1 is entirely NULL on purpose. The SQLite JDBC driver derives the type of an expression column
from the first row it sees, so a leading NULL is what makes STRFTIME()/DATE()/DATETIME() report
NUMERIC instead of TEXT.
*/
INSERT INTO date_samples VALUES
  (1, 'all null'
    , NULL, NULL, NULL
    , '<null>', '<null>', '<null>');

/* Row 2 holds the one format the SQLite JDBC driver parses by itself. */
INSERT INTO date_samples VALUES
  (2, 'full precision'
    , '2024-05-16 00:00:00', '2024-05-16 10:11:12', '2024-05-16 10:11:12.123'
    , '2024-05-16', '2024-05-16 10:11:12', '2024-05-16 10:11:12.123');

/*
Row 3 holds the plain YYYY-MM-DD form. It is what SQLite's own DATE() returns and what every other
client shows as a date.
*/
INSERT INTO date_samples VALUES
  (3, 'date only'
    , '2023-01-02', '2023-01-02', '2023-01-02'
    , '2023-01-02', '2023-01-02 00:00:00', '2023-01-02 00:00:00.000');

/* Row 4 holds the ISO 8601 form with a T between the date and the time. */
INSERT INTO date_samples VALUES
  (4, 'iso 8601 T'
    , '2022-03-04', '2022-03-04T05:06:07', '2022-03-04T05:06:07.089'
    , '2022-03-04', '2022-03-04 05:06:07', '2022-03-04 05:06:07.089');
