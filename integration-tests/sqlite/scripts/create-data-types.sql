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
One column per declared type a SQLite table is likely to carry, so that a change to how this
dialect reads any of them shows up as a failing test rather than as a surprise in a pipeline.

Each column is paired with the text it should render as once Hop has read it, which is what
0005-read-data-types.hpl compares against. Row 2 is entirely NULL: a type that reads a null wrong
is as broken as one that reads a value wrong.
*/

DROP TABLE IF EXISTS data_types;

CREATE TABLE data_types
(
  id           INTEGER PRIMARY KEY
, t_string     TEXT
, t_varchar    VARCHAR(50)
, t_char       CHAR(1)
, t_integer    INTEGER
, t_bigint     BIGINT
, t_number     REAL
, t_double     DOUBLE
, t_bignumber  DECIMAL(20,5)
, t_numeric    NUMERIC
, t_boolean    BOOLEAN
, t_date       DATE
, t_datetime   DATETIME
, t_timestamp  TIMESTAMP
, t_binary     BLOB
, x_string     TEXT
, x_varchar    TEXT
, x_char       TEXT
, x_integer    TEXT
, x_bigint     TEXT
, x_number     TEXT
, x_double     TEXT
, x_bignumber  TEXT
, x_numeric    TEXT
, x_boolean    TEXT
, x_date       TEXT
, x_datetime   TEXT
, x_timestamp  TEXT
, x_binary     TEXT
);

INSERT INTO data_types VALUES
  ( 1
  , 'text value', 'varchar value', 'Y'
  , 42, 9223372036854775807, 2.5, 3.25, 12345.67890, 7, 1
  , '2024-05-16', '2024-05-16 10:11:12', '2024-05-16 10:11:12.123'
  , X'48656C6C6F'
  , 'text value', 'varchar value', 'Y'
  , '42', '9223372036854775807', '2.5', '3.25', '12345.6789', '7', 'Y'
  , '2024-05-16', '2024-05-16 10:11:12', '2024-05-16 10:11:12.123'
  , 'Hello'
  );

INSERT INTO data_types VALUES
  ( 2
  , NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
  , '<null>', '<null>', '<null>', '<null>', '<null>', '<null>', '<null>'
  , '<null>', '<null>', '<null>', '<null>', '<null>', '<null>', '<null>'
  );
