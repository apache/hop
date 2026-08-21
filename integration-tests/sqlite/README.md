<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# SQLite integration tests

Date handling against a SQLite database, reported as
[issue #3910](https://github.com/apache/hop/issues/3910), and one column of
every other type a SQLite table is likely to carry, so that the date handling
cannot cost any of them their reading.

SQLite is embedded, so there is no service to start: the workflows create
`${SQLITE_DB_FILE}` (`/tmp/hop-it-sqlite-3910.sqlite` by default, set in
`dev-env-config.json`) on first connect. The SQLite JDBC driver is Apache
licensed and ships in the client under `lib/jdbc`, so nothing has to be
downloaded either.

## What makes SQLite different

SQLite has no date or time storage class. A date lives in a `TEXT`, `REAL` or
`INTEGER` column, and a column declared `DATE` or `DATETIME` only gets a *type
affinity* from that name. The formats SQLite itself accepts as a date/time value
are listed in <https://www.sqlite.org/lang_datefunc.html> and include
`YYYY-MM-DD`, `YYYY-MM-DD HH:MM`, `YYYY-MM-DDTHH:MM:SS` and
`YYYY-MM-DD HH:MM:SS.SSS`. Every date fixture row here is one of those.

## Tests

| Workflow | What it checks |
| --- | --- |
| `main-0001-read-date-columns.hwf` | `DATE`, `DATETIME` and `TIMESTAMP` columns read straight from the table, with no conversion function in the query |
| `main-0002-read-date-functions.hwf` | `STRFTIME()`, `DATE()` and `DATETIME()` expression columns, which SQLite returns as text |
| `main-0003-write-read-dates.hwf` | Dates Hop writes into SQLite, read back both through Hop and through SQLite's own `DATE()`/`DATETIME()` |
| `main-0004-read-data-types.hwf` | One column of every declared type a SQLite table is likely to carry, plus two expression columns |
| `main-0005-write-read-data-types.hwf` | One value of every Hop type, written into SQLite and read back |

Every fixture row carries the expected rendering of its own columns as plain
text (`x_date`, `x_integer`, …), so the pipelines compare what Hop read against
what SQLite holds without hard coding a value in a transform. A mismatch aborts
with the offending row in the log, actual values first and expected values
after, which is usually enough to see what moved.

Each fixture also carries an all-`NULL` row: a type that reads a null wrong is
as broken as one that reads a value wrong. In `scripts/create-date-samples.sql`
that row comes **first**, deliberately — the SQLite JDBC driver types an
expression column from the first row it sees, and a leading `NULL` is what makes
`STRFTIME()` and friends report `NUMERIC` instead of `TEXT`.

The two expression columns in `main-0004` are the other side of that: the driver
did see a value in their first row, so they have to keep the type it gave them.
Reading every expression as a string would be as wrong as reading a date as a
number.

Numeric conversions in the pipelines all name their decimal symbol, so the
comparisons do not depend on the platform locale.

## Connection

`metadata/rdbms/sqlite.json` is left at Hop's defaults, including
`SUPPORTS_BOOLEAN_DATA_TYPE=N` and `SUPPORTS_TIMESTAMP_DATA_TYPE=N`, so the
tests describe what someone gets from a SQLite connection out of the box rather
than from a tuned one.

## Running locally

```bash
export HOP_CONFIG_FOLDER=$PWD/integration-tests/sqlite
assemblies/client/target/hop/hop-run.sh -r local -e dev \
  -f $PWD/integration-tests/sqlite/main-0001-read-date-columns.hwf
```

Or through Docker, like every other suite:

```bash
integration-tests/scripts/run-tests-docker.sh PROJECT_NAME=sqlite
```
