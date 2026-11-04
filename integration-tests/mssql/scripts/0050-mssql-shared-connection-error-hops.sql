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

DROP TABLE IF EXISTS it0050_d_child;
DROP TABLE IF EXISTS it0050_a;
DROP TABLE IF EXISTS it0050_b;
DROP TABLE IF EXISTS it0050_c;
DROP TABLE IF EXISTS it0050_d;
DROP TABLE IF EXISTS it0050_e;
DROP TABLE IF EXISTS it0050_err_a;
DROP TABLE IF EXISTS it0050_err_b;
DROP TABLE IF EXISTS it0050_err_c;
DROP TABLE IF EXISTS it0050_err_d;
DROP TABLE IF EXISTS it0050_err_e;
CREATE TABLE it0050_a (id BIGINT PRIMARY KEY, name VARCHAR(5), CHECK (name = 'x' OR id % 97 <> 0));
INSERT INTO it0050_a (id, name) SELECT g, 'x' FROM (SELECT TOP (3000) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS g FROM sys.all_objects a CROSS JOIN sys.all_objects b) n;
CREATE TABLE it0050_b (id BIGINT PRIMARY KEY, name VARCHAR(5), CHECK (name = 'x' OR id % 89 <> 0));
INSERT INTO it0050_b (id, name) SELECT g, 'x' FROM (SELECT TOP (1500) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS g FROM sys.all_objects a CROSS JOIN sys.all_objects b) n;
CREATE TABLE it0050_c (id BIGINT PRIMARY KEY, name VARCHAR(5), CHECK (id % 83 <> 0));
CREATE TABLE it0050_d (id BIGINT PRIMARY KEY, name VARCHAR(5));
INSERT INTO it0050_d (id, name) SELECT g, 'x' FROM (SELECT TOP (3000) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS g FROM sys.all_objects a CROSS JOIN sys.all_objects b) n;
CREATE TABLE it0050_d_child (parent_id BIGINT REFERENCES it0050_d (id));
INSERT INTO it0050_d_child (parent_id) SELECT g FROM (SELECT TOP (3000) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS g FROM sys.all_objects a CROSS JOIN sys.all_objects b) n WHERE g % 79 = 0;
CREATE TABLE it0050_e (id BIGINT PRIMARY KEY, name VARCHAR(5), CHECK (id % 73 <> 0));
CREATE TABLE it0050_err_a (id BIGINT, name VARCHAR(20));
CREATE TABLE it0050_err_b (id BIGINT, name VARCHAR(20));
CREATE TABLE it0050_err_c (id BIGINT, name VARCHAR(20));
CREATE TABLE it0050_err_d (id BIGINT, name VARCHAR(20));
CREATE TABLE it0050_err_e (id BIGINT, name VARCHAR(20));
