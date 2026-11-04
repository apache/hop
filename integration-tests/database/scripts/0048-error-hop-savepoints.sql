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

DROP TABLE IF EXISTS public.it0048_errors;
CREATE TABLE public.it0048_errors (test VARCHAR(20), id BIGINT);

DROP TABLE IF EXISTS public.it0048_update;
CREATE TABLE public.it0048_update (id BIGINT PRIMARY KEY, name VARCHAR(5));
INSERT INTO public.it0048_update VALUES (1, 'x'), (2, 'x'), (3, 'x'), (4, 'x'), (5, 'x');

DROP TABLE IF EXISTS public.it0048_delete_child;
DROP TABLE IF EXISTS public.it0048_delete;
CREATE TABLE public.it0048_delete (id BIGINT PRIMARY KEY, name VARCHAR(5));
INSERT INTO public.it0048_delete VALUES (1, 'x'), (2, 'x'), (3, 'x'), (4, 'x'), (5, 'x');
CREATE TABLE public.it0048_delete_child (parent_id BIGINT REFERENCES public.it0048_delete (id));
INSERT INTO public.it0048_delete_child VALUES (3);

DROP TABLE IF EXISTS public.it0048_insert_update;
CREATE TABLE public.it0048_insert_update (id BIGINT PRIMARY KEY, name VARCHAR(5));
INSERT INTO public.it0048_insert_update VALUES (1, 'x');

DROP TABLE IF EXISTS public.it0048_execute_sql_row;
CREATE TABLE public.it0048_execute_sql_row (id BIGINT PRIMARY KEY);

DROP TABLE IF EXISTS public.it0048_dimension;
CREATE TABLE public.it0048_dimension (
    tk BIGINT PRIMARY KEY,
    version INTEGER,
    date_from TIMESTAMP,
    date_to TIMESTAMP,
    id BIGINT,
    name VARCHAR(5)
);
