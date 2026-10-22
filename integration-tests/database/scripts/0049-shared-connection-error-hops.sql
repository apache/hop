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

DROP TABLE IF EXISTS public.it0049_a;
CREATE TABLE public.it0049_a (id BIGINT PRIMARY KEY, name VARCHAR(5), CHECK (name = 'x' OR id % 97 <> 0));
INSERT INTO public.it0049_a SELECT g, 'x' FROM generate_series(1, 3000) g;

DROP TABLE IF EXISTS public.it0049_b;
CREATE TABLE public.it0049_b (id BIGINT PRIMARY KEY, name VARCHAR(5), CHECK (name = 'x' OR id % 89 <> 0));
INSERT INTO public.it0049_b SELECT g, 'x' FROM generate_series(1, 1500) g;

DROP TABLE IF EXISTS public.it0049_c;
CREATE TABLE public.it0049_c (id BIGINT PRIMARY KEY, name VARCHAR(5), CHECK (id % 83 <> 0));

DROP TABLE IF EXISTS public.it0049_err_a;
CREATE TABLE public.it0049_err_a (id BIGINT, name VARCHAR(20));
DROP TABLE IF EXISTS public.it0049_err_b;
CREATE TABLE public.it0049_err_b (id BIGINT, name VARCHAR(20));
DROP TABLE IF EXISTS public.it0049_err_c;
CREATE TABLE public.it0049_err_c (id BIGINT, name VARCHAR(20));
