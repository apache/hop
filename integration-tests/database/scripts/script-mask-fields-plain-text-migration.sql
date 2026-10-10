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

/*
 * Issue #8791: a mapping table written before patterns had a hash secret. It has the old layout and
 * stores the source value in plain text. Masking with a secret keeps the replacement and moves the
 * row to its hashed key.
 */

DROP TABLE IF EXISTS public.mask_0047;
DROP TABLE IF EXISTS public.mask_0047_seq;
DROP TABLE IF EXISTS public.mask_0047_result;

CREATE TABLE public.mask_0047 (
  pattern_name VARCHAR(128) NOT NULL,
  source_key VARCHAR(255) NOT NULL,
  masked_value VARCHAR(2000) NOT NULL,
  PRIMARY KEY (pattern_name, source_key)
);
CREATE TABLE public.mask_0047_seq (
  pattern_name VARCHAR(128) NOT NULL,
  next_value BIGINT NOT NULL,
  PRIMARY KEY (pattern_name)
);
CREATE TABLE public.mask_0047_result (id INTEGER, first_name VARCHAR(100));

INSERT INTO public.mask_0047 VALUES ('0047-first-name', 'Matt', 'fn-legacy');
INSERT INTO public.mask_0047_seq VALUES ('0047-first-name', 100);
