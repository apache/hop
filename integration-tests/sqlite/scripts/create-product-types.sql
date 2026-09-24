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
The hierarchy from issue #3633: ids in columns declared NUMERIC, holding integers, with the root's
parent NULL. The SQLite JDBC driver types such a column from the row it is on once a query runs:
INTEGER for an integer, NUMERIC for a NULL. Before the query runs it says NUMERIC for both. A
Database Join lookup starting at the root therefore sees id as INTEGER and parentId as NUMERIC,
and one starting further down sees both as INTEGER.

Each row carries the text its ids should render as once Hop has read them, which is what
0008-database-join-numeric-ids.hpl compares against.
*/

DROP TABLE IF EXISTS producttype;

CREATE TABLE producttype
(
  export_source TEXT
, id            NUMERIC
, parentId      NUMERIC
, x_id          TEXT
, x_parent_id   TEXT
);

INSERT INTO producttype VALUES
  ('ABCD', 100002348, NULL,      '100002348', '<null>')
, ('ABCD', 100002344, 100002348, '100002344', '100002348')
, ('ABCD', 100002340, 100002344, '100002340', '100002344')
, ('WXYZ', 100002348, NULL,      '100002348', '<null>')
;
