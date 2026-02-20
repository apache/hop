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
DROP TABLE IF EXISTS public.dimension_uuid
;

CREATE TABLE "public".dimension_uuid
(
    dimension_id VARCHAR(36)
    , version INTEGER
    , date_from TIMESTAMP
    , date_to TIMESTAMP
    , "key" TEXT
    , "value" TEXT
)
;

CREATE INDEX idx_dimension_uuid_lookup ON "public".dimension_uuid("key")
;

CREATE INDEX idx_dimension_uuid_tk ON "public".dimension_uuid(dimension_id)
;
