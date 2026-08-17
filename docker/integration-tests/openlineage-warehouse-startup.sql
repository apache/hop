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
**/

-- Seed the "warehouse" Postgres used by the OpenLineage warehouse-lineage integration test.
-- The source table is populated; the target table is created empty for Table Output to fill.
-- Runs automatically on first cluster init (mounted into /docker-entrypoint-initdb.d).

CREATE TABLE IF NOT EXISTS public.orders_source (
  id     INTEGER,
  amount NUMERIC(10, 2)
);

INSERT INTO public.orders_source (id, amount) VALUES
  (1, 10.00),
  (2, 20.00),
  (3, 30.00);

CREATE TABLE IF NOT EXISTS public.orders_target (
  id     INTEGER,
  amount NUMERIC(10, 2)
);

-- Targets for the per-writer column-lineage tests (Insert/Update, Combination Lookup,
-- Dimension Lookup, PostgreSQL bulk loader). Each is filled by reading orders_source.
CREATE TABLE IF NOT EXISTS public.orders_upsert (
  id     INTEGER,
  amount NUMERIC(10, 2)
);

CREATE TABLE IF NOT EXISTS public.orders_bulk (
  id     INTEGER,
  amount NUMERIC(10, 2)
);

-- Combination Lookup junk dimension: technical key + the business-key columns.
CREATE TABLE IF NOT EXISTS public.orders_combi (
  combi_tk INTEGER,
  id       INTEGER,
  amount   NUMERIC(10, 2)
);

-- Dimension Lookup slowly-changing dimension: technical key, version, validity dates,
-- the natural key and the dimension attribute.
CREATE TABLE IF NOT EXISTS public.orders_dim (
  dim_tk    INTEGER,
  version   INTEGER,
  date_from TIMESTAMP,
  date_to   TIMESTAMP,
  id        INTEGER,
  amount    NUMERIC(10, 2)
);
