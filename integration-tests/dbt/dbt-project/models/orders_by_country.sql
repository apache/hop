--
-- Licensed to the Apache Software Foundation (ASF) under one or more
-- contributor license agreements.  See the NOTICE file distributed with
-- this work for additional information regarding copyright ownership.
-- The ASF licenses this file to You under the Apache License, Version 2.0
-- (the "License"); you may not use this file except in compliance with
-- the License.  You may obtain a copy of the License at
--
--       http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.
--

-- Reads the table Hop loaded (public.orders_source) and aggregates it. Keeping the source in
-- Hop's schema and the model in dbt's is what makes the handover visible in the lineage graph.
{{ config(materialized='table') }}

{#-
  min_amount arrives from the dbt action's variables list. It is read here rather than merely
  passed so the typing is actually exercised: the action renders JSON numbers unquoted, and a
  regression that quoted them would reach dbt as a string and fail this compile instead of
  silently changing nothing.
-#}
{%- set min_amount = var('min_amount', 0) -%}
{%- if min_amount is string -%}
  {{ exceptions.raise_compiler_error(
       "min_amount arrived as the string '" ~ min_amount ~ "': the dbt action must pass numeric --vars unquoted") }}
{%- endif -%}

select
    country,
    count(*)      as order_count,
    sum(amount)   as total_amount
from {{ source('hop', 'orders_source') }}
where amount >= {{ min_amount }}
group by country
