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

DROP TABLE IF EXISTS public.testtable;

CREATE TABLE public.testtable
(
    "key" varchar NULL,
    value varchar NULL
);

INSERT INTO public.testtable
    ("key", value)
VALUES ('10', 'aa'),
       ('20', 'bb'),
       ('30', 'cc'),
       ('40', 'dd'),
       ('50', 'ee')
;

DROP TABLE IF EXISTS public.foo;

CREATE TABLE public.foo
(
    id          integer,
    name        varchar(255),
    description text
);

INSERT INTO foo (id, name, description) VALUES (1, 'Alice', 'Dont judge each day by the harvest you reap but by the seeds that you plant. -Robert Louis Stevenson');
INSERT INTO foo (id, name, description) VALUES (2, 'Charlie', 'The way to get started is to quit talking and begin doing. -Walt Disney');
INSERT INTO foo (id, name, description) VALUES (3, 'Charlie', 'Life is what happens when youre busy making other plans. -John Lennon');
INSERT INTO foo (id, name, description) VALUES (4, 'Alice', 'The way to get started is to quit talking and begin doing. -Walt Disney');
INSERT INTO foo (id, name, description) VALUES (5, 'Charlie', 'Your time is limited, dont waste it living someone elses life. -Steve Jobs');
INSERT INTO foo (id, name, description) VALUES (6, 'Bob', 'Spread love everywhere you go. -Mother Teresa');
INSERT INTO foo (id, name, description) VALUES (7, 'Alice', 'Life is what happens when youre busy making other plans. -John Lennon');
INSERT INTO foo (id, name, description) VALUES (8, 'Bob', 'If life were predictable it would cease to be life, and be without flavor. -Eleanor Roosevelt');
INSERT INTO foo (id, name, description) VALUES (9, 'Bob', 'If you look at what you have in life, youll always have more. -Oprah Winfrey');
INSERT INTO foo (id, name, description) VALUES (10, 'Alice', 'Your time is limited, dont waste it living someone elses life. -Steve Jobs');
