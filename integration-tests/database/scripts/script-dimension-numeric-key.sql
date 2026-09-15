/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

/* Issue #8130: a dimension whose technical key is numeric(38,0), fed by a sequence. */

DROP TABLE IF EXISTS public.dimension_numeric_key
;

DROP SEQUENCE IF EXISTS public.dimension_numeric_key_seq
;

CREATE SEQUENCE public.dimension_numeric_key_seq
;

CREATE TABLE "public".dimension_numeric_key
(
    dimension_id NUMERIC(38,0) NOT NULL
    , version INTEGER
    , date_from TIMESTAMP
    , date_to TIMESTAMP
    , "key" NUMERIC(38,0)
    , "value" TEXT
)
;

CREATE INDEX idx_dimension_numeric_key_lookup ON "public".dimension_numeric_key("key")
;

CREATE INDEX idx_dimension_numeric_key_tk ON "public".dimension_numeric_key(dimension_id)
;

DROP TABLE IF EXISTS public.dimension_numeric_tablemax
;

CREATE TABLE "public".dimension_numeric_tablemax
(
    dimension_id NUMERIC(38,0) NOT NULL
    , version INTEGER
    , date_from TIMESTAMP
    , date_to TIMESTAMP
    , "key" NUMERIC(38,0)
    , "value" TEXT
)
;

CREATE INDEX idx_dimension_numeric_tablemax_lookup ON "public".dimension_numeric_tablemax("key")
;

CREATE INDEX idx_dimension_numeric_tablemax_tk ON "public".dimension_numeric_tablemax(dimension_id)
;
