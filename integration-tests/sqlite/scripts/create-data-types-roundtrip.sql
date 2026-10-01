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

/* The table Hop writes one value of every type into, so a later pipeline can read them back. */

DROP TABLE IF EXISTS data_types_written;

CREATE TABLE data_types_written
(
  id          INTEGER PRIMARY KEY
, w_string    TEXT
, w_integer   INTEGER
, w_number    REAL
, w_bignumber DECIMAL(20,5)
, w_boolean   CHAR(1)
, w_date      DATE
, w_timestamp TIMESTAMP
, w_binary    BLOB
, x_string    TEXT
, x_integer   TEXT
, x_number    TEXT
, x_bignumber TEXT
, x_boolean   TEXT
, x_date      TEXT
, x_timestamp TEXT
, x_binary    TEXT
);
