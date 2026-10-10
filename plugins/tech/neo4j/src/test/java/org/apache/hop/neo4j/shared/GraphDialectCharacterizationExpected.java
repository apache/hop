/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.neo4j.shared;

import java.util.HashMap;
import java.util.Map;

/**
 * The statements recorded from the code before the dialect SPI, for Neo4j, Memgraph and Neptune.
 * Changed on purpose since:
 *
 * <ul>
 *   <li>Labels, relationship types, properties and names are quoted with backticks.
 *   <li>NEO4J|vector.create|V3: an unnamed Neo4j vector index is refused, it couldn't be dropped.
 *   <li>NEO4J|constraint.create|K7: a unique constraint on several properties is (n.`a`, n.`b`), it
 *       was "n.a, b".
 *   <li>NEO4J|constraint.create|K8: Neo4j has no existence constraint on several properties, it was
 *       "n.a, b IS NOT NULL".
 *   <li>MEMGRAPH|vector.create|V2: Memgraph 3 has vector indexes on relationships, CREATE VECTOR
 *       EDGE INDEX. They were refused.
 * </ul>
 */
final class GraphDialectCharacterizationExpected {
  private GraphDialectCharacterizationExpected() {}

  static Map<String, String> expected() {
    Map<String, String> e = new HashMap<>();
    e.put(
        "NEO4J|index.create|A",
        "CREATE INDEX `idx` IF NOT EXISTS FOR (n:`Person`) ON (n.`name`, n.`age`)");
    e.put("NEO4J|index.drop|A", "DROP INDEX `idx` IF EXISTS");
    e.put(
        "NEO4J|index.create|B",
        "CREATE INDEX `idx` IF NOT EXISTS FOR ()-[n:`KNOWS`]-() ON (n.`since`)");
    e.put("NEO4J|index.drop|B", "DROP INDEX `idx` IF EXISTS");
    e.put("NEO4J|index.create|C", "CREATE INDEX  IF NOT EXISTS FOR (n:`Person`) ON (n.`name`)");
    e.put("NEO4J|index.drop|C", "!ERROR");
    e.put(
        "NEO4J|index.create|D",
        "CREATE INDEX `idx` IF NOT EXISTS FOR ()-[n:`KNOWS`]-() ON (n.`a`, n.`b`)");
    e.put("NEO4J|index.drop|D", "DROP INDEX `idx` IF EXISTS");
    e.put(
        "NEO4J|vector.create|V1",
        "CREATE VECTOR INDEX `doc_vectors` IF NOT EXISTS FOR (n:`Doc`) ON (n.`embedding`) OPTIONS {indexConfig: {`vector.dimensions`: 768, `vector.similarity_function`: 'cosine'}}");
    e.put("NEO4J|vector.drop|V1", "DROP INDEX `doc_vectors` IF EXISTS");
    e.put(
        "NEO4J|vector.create|V2",
        "CREATE VECTOR INDEX `doc_vectors` IF NOT EXISTS FOR ()-[n:`Doc`]-() ON (n.`embedding`) OPTIONS {indexConfig: {`vector.dimensions`: 3, `vector.similarity_function`: 'euclidean'}}");
    e.put("NEO4J|vector.drop|V2", "DROP INDEX `doc_vectors` IF EXISTS");
    e.put("NEO4J|vector.create|V3", "!ERROR");
    e.put("NEO4J|vector.drop|V3", "!ERROR");
    e.put(
        "NEO4J|vector.create|V4",
        "CREATE VECTOR INDEX `doc_vectors` IF NOT EXISTS FOR (n:`Doc`) ON (n.`embedding`) OPTIONS {indexConfig: {`vector.dimensions`: 3, `vector.similarity_function`: 'euclidean'}}");
    e.put("NEO4J|vector.drop|V4", "DROP INDEX `doc_vectors` IF EXISTS");
    e.put("NEO4J|vector.create|V5", "!ERROR");
    e.put("NEO4J|vector.drop|V5", "DROP INDEX `doc_vectors` IF EXISTS");
    e.put("NEO4J|vector.create|V6", "!ERROR");
    e.put("NEO4J|vector.drop|V6", "DROP INDEX `doc_vectors` IF EXISTS");
    e.put("NEO4J|vector.create|V7", "!ERROR");
    e.put("NEO4J|vector.drop|V7", "DROP INDEX `doc_vectors` IF EXISTS");
    e.put(
        "NEO4J|constraint.create|K1",
        "CREATE CONSTRAINT `c` IF NOT EXISTS FOR (n:`Person`) REQUIRE  n.`id` IS UNIQUE ");
    e.put("NEO4J|constraint.drop|K1", "DROP CONSTRAINT `c` IF EXISTS ");
    e.put(
        "NEO4J|constraint.create|K2",
        "CREATE CONSTRAINT `c` IF NOT EXISTS FOR (n:`Person`) REQUIRE  n.`id` IS NOT NULL ");
    e.put("NEO4J|constraint.drop|K2", "DROP CONSTRAINT `c` IF EXISTS ");
    e.put(
        "NEO4J|constraint.create|K3",
        "CREATE CONSTRAINT `c` IF NOT EXISTS FOR (n:`Person`) REQUIRE (n.`a`, n.`b`) IS NODE KEY ");
    e.put("NEO4J|constraint.drop|K3", "DROP CONSTRAINT `c` IF EXISTS ");
    e.put(
        "NEO4J|constraint.create|K4",
        "CREATE CONSTRAINT `c` IF NOT EXISTS FOR ()-[r:`KNOWS`]-() REQUIRE  r.`id` IS UNIQUE ");
    e.put("NEO4J|constraint.drop|K4", "DROP CONSTRAINT `c` IF EXISTS ");
    e.put(
        "NEO4J|constraint.create|K5",
        "CREATE CONSTRAINT `c` IF NOT EXISTS FOR ()-[r:`KNOWS`]-() REQUIRE  r.`id` IS NOT NULL ");
    e.put("NEO4J|constraint.drop|K5", "DROP CONSTRAINT `c` IF EXISTS ");
    e.put("NEO4J|constraint.create|K6", "!ERROR");
    e.put("NEO4J|constraint.drop|K6", "!ERROR");
    e.put(
        "NEO4J|constraint.create|K7",
        "CREATE CONSTRAINT `c` IF NOT EXISTS FOR (n:`Person`) REQUIRE  (n.`a`, n.`b`) IS UNIQUE ");
    e.put("NEO4J|constraint.drop|K7", "DROP CONSTRAINT `c` IF EXISTS ");
    e.put("NEO4J|constraint.create|K8", "!ERROR");
    e.put("NEO4J|constraint.drop|K8", "DROP CONSTRAINT `c` IF EXISTS ");
    e.put("NEO4J|constraint.create|K9", "!ERROR");
    e.put("NEO4J|constraint.drop|K9", "!ERROR");
    e.put(
        "NEO4J|nodekey.create|N1",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (n:`Person`) REQUIRE n.`id` IS UNIQUE;");
    e.put(
        "NEO4J|nodekey.create|N2",
        "CREATE INDEX IF NOT EXISTS FOR (n:`Person`) ON (n.`first`, n.`last`)");
    e.put("NEO4J|nodekey.create|N3", "null");
    e.put("NEO4J|nodekey.create|N4", "null");
    e.put("NEO4J|autocommit|S1", "false");
    e.put("NEO4J|autocommit|S2", "false");
    e.put("NEO4J|autocommit|S3", "false");
    e.put("NEO4J|autocommit|S4", "false");
    e.put("NEO4J|autocommit|S5", "false");
    e.put("NEO4J|autocommit|S6", "false");
    e.put("NEO4J|autocommit|S7", "false");
    e.put("NEO4J|autocommit|S8", "false");
    e.put("NEO4J|autocommit|S9", "false");
    e.put("NEO4J|autocommit|S10", "false");
    e.put("NEO4J|autocommit|S11", "false");
    e.put("NEO4J|autocommit|S12", "false");
    e.put("NEO4J|vectorValue|$p", "$p");
    e.put("NEO4J|vectorValue|pr.p", "pr.p");
    e.put("NEO4J|flag|cypher", "true");
    e.put("NEO4J|flag|nodeIndexes", "true");
    e.put("NEO4J|flag|relationshipIndexes", "true");
    e.put("NEO4J|flag|schemaChangesInTransactions", "true");
    e.put("NEO4J|flag|vectorIndexes", "true");
    e.put("NEO4J|flag|nodeConstraintTypes", "[UNIQUE, NOT_NULL, NODE_KEY]");
    e.put("NEO4J|flag|relationshipConstraintTypes", "[UNIQUE, NOT_NULL]");
    // Changed by issue #8704: the execution information location nodes only, root = nothing
    // executes it
    e.put(
        "NEO4J|path|toRoot",
        "MATCH (child:Execution {id: $executionId }) \nMATCH p = (top:Execution)-[:EXECUTES*]->(child) \nWHERE NOT ()-[:EXECUTES]->(top) \nAND   all(n IN nodes(p) WHERE n.type IS NULL) \nRETURN p \nORDER BY size(RELATIONSHIPS(p)) DESC \nLIMIT 10 \n");
    e.put(
        "NEO4J|path|toFailed",
        "MATCH (top:Execution {id: $executionId }) \nMATCH p = shortestPath((top)-[:EXECUTES*]->(child:Execution)) \nWHERE child.failed = true \nAND   child.id <> $executionId \nAND   NOT (child)-[:EXECUTES]->() \nAND   all(n IN nodes(p) WHERE n.type IS NULL) \nRETURN p \nORDER BY size(RELATIONSHIPS(p)) \nLIMIT 10 \n");
    e.put("MEMGRAPH|index.create|A", "CREATE INDEX ON :`Person`(`name`, `age`)");
    e.put("MEMGRAPH|index.drop|A", "DROP INDEX ON :`Person`(`name`, `age`)");
    e.put("MEMGRAPH|index.create|B", "CREATE EDGE INDEX ON :`KNOWS`(`since`)");
    e.put("MEMGRAPH|index.drop|B", "DROP EDGE INDEX ON :`KNOWS`(`since`)");
    e.put("MEMGRAPH|index.create|C", "CREATE INDEX ON :`Person`(`name`)");
    e.put("MEMGRAPH|index.drop|C", "DROP INDEX ON :`Person`(`name`)");
    e.put("MEMGRAPH|index.create|D", "!ERROR");
    e.put("MEMGRAPH|index.drop|D", "!ERROR");
    e.put(
        "MEMGRAPH|vector.create|V1",
        "CREATE VECTOR INDEX `doc_vectors` ON :`Doc`(`embedding`) WITH CONFIG {\"dimension\": 768, \"capacity\": 1000, \"metric\": \"cos\"}");
    e.put("MEMGRAPH|vector.drop|V1", "DROP VECTOR INDEX `doc_vectors`");
    e.put(
        "MEMGRAPH|vector.create|V2",
        "CREATE VECTOR EDGE INDEX `doc_vectors` ON :`Doc`(`embedding`) WITH CONFIG {\"dimension\": 3, \"capacity\": 1000, \"metric\": \"l2sq\"}");
    e.put("MEMGRAPH|vector.drop|V2", "DROP VECTOR INDEX `doc_vectors`");
    e.put("MEMGRAPH|vector.create|V3", "!ERROR");
    e.put("MEMGRAPH|vector.drop|V3", "!ERROR");
    e.put(
        "MEMGRAPH|vector.create|V4",
        "CREATE VECTOR INDEX `doc_vectors` ON :`Doc`(`embedding`) WITH CONFIG {\"dimension\": 3, \"capacity\": 50, \"metric\": \"l2sq\"}");
    e.put("MEMGRAPH|vector.drop|V4", "DROP VECTOR INDEX `doc_vectors`");
    e.put("MEMGRAPH|vector.create|V5", "!ERROR");
    e.put("MEMGRAPH|vector.drop|V5", "DROP VECTOR INDEX `doc_vectors`");
    e.put("MEMGRAPH|vector.create|V6", "!ERROR");
    e.put("MEMGRAPH|vector.drop|V6", "DROP VECTOR INDEX `doc_vectors`");
    e.put("MEMGRAPH|vector.create|V7", "!ERROR");
    e.put("MEMGRAPH|vector.drop|V7", "DROP VECTOR INDEX `doc_vectors`");
    e.put(
        "MEMGRAPH|constraint.create|K1",
        "CREATE CONSTRAINT ON (n:`Person`) ASSERT n.`id` IS UNIQUE");
    e.put("MEMGRAPH|constraint.drop|K1", "DROP CONSTRAINT ON (n:`Person`) ASSERT n.`id` IS UNIQUE");
    e.put(
        "MEMGRAPH|constraint.create|K2",
        "CREATE CONSTRAINT ON (n:`Person`) ASSERT EXISTS (n.`id`)");
    e.put("MEMGRAPH|constraint.drop|K2", "DROP CONSTRAINT ON (n:`Person`) ASSERT EXISTS (n.`id`)");
    e.put("MEMGRAPH|constraint.create|K3", "!ERROR");
    e.put("MEMGRAPH|constraint.drop|K3", "!ERROR");
    e.put("MEMGRAPH|constraint.create|K4", "!ERROR");
    e.put("MEMGRAPH|constraint.drop|K4", "!ERROR");
    e.put("MEMGRAPH|constraint.create|K5", "!ERROR");
    e.put("MEMGRAPH|constraint.drop|K5", "!ERROR");
    e.put("MEMGRAPH|constraint.create|K6", "!ERROR");
    e.put("MEMGRAPH|constraint.drop|K6", "!ERROR");
    e.put(
        "MEMGRAPH|constraint.create|K7",
        "CREATE CONSTRAINT ON (n:`Person`) ASSERT n.`a`, n.`b` IS UNIQUE");
    e.put(
        "MEMGRAPH|constraint.drop|K7",
        "DROP CONSTRAINT ON (n:`Person`) ASSERT n.`a`, n.`b` IS UNIQUE");
    e.put("MEMGRAPH|constraint.create|K8", "!ERROR");
    e.put("MEMGRAPH|constraint.drop|K8", "!ERROR");
    e.put(
        "MEMGRAPH|constraint.create|K9",
        "CREATE CONSTRAINT ON (n:`Person`) ASSERT n.`id` IS UNIQUE");
    e.put("MEMGRAPH|constraint.drop|K9", "DROP CONSTRAINT ON (n:`Person`) ASSERT n.`id` IS UNIQUE");
    e.put(
        "MEMGRAPH|nodekey.create|N1", "CREATE CONSTRAINT ON (n:`Person`) ASSERT n.`id` IS UNIQUE");
    e.put("MEMGRAPH|nodekey.create|N2", "CREATE INDEX ON :`Person`(`first`, `last`)");
    e.put("MEMGRAPH|nodekey.create|N3", "null");
    e.put("MEMGRAPH|nodekey.create|N4", "null");
    e.put("MEMGRAPH|autocommit|S1", "true");
    e.put("MEMGRAPH|autocommit|S2", "true");
    e.put("MEMGRAPH|autocommit|S3", "true");
    e.put("MEMGRAPH|autocommit|S4", "true");
    e.put("MEMGRAPH|autocommit|S5", "true");
    e.put("MEMGRAPH|autocommit|S6", "true");
    e.put("MEMGRAPH|autocommit|S7", "false");
    e.put("MEMGRAPH|autocommit|S8", "false");
    e.put("MEMGRAPH|autocommit|S9", "true");
    e.put("MEMGRAPH|autocommit|S10", "true");
    e.put("MEMGRAPH|autocommit|S11", "true");
    e.put("MEMGRAPH|autocommit|S12", "false");
    e.put("MEMGRAPH|vectorValue|$p", "$p");
    e.put("MEMGRAPH|vectorValue|pr.p", "pr.p");
    e.put("MEMGRAPH|flag|cypher", "true");
    e.put("MEMGRAPH|flag|nodeIndexes", "true");
    e.put("MEMGRAPH|flag|relationshipIndexes", "true");
    e.put("MEMGRAPH|flag|schemaChangesInTransactions", "false");
    e.put("MEMGRAPH|flag|vectorIndexes", "true");
    e.put("MEMGRAPH|flag|nodeConstraintTypes", "[UNIQUE, NOT_NULL]");
    e.put("MEMGRAPH|flag|relationshipConstraintTypes", "[]");
    e.put(
        "MEMGRAPH|path|toRoot",
        "MATCH p = (top:Execution)-[:EXECUTES*]->(child:Execution {id: $executionId }) \nWHERE top.parentId IS NULL \nAND   size([n IN nodes(p) WHERE n.type IS NOT NULL]) = 0 \nRETURN p \nLIMIT 10 \n");
    e.put(
        "MEMGRAPH|path|toFailed",
        "MATCH p = (top:Execution {id: $executionId })-[:EXECUTES*]->(child:Execution) \nWHERE child.failed = true \nAND   child.id <> $executionId \nOPTIONAL MATCH (child)-[grandChild:EXECUTES]->() \nWITH p, count(grandChild) AS grandChildren \nWHERE grandChildren = 0 \nAND   size([n IN nodes(p) WHERE n.type IS NOT NULL]) = 0 \nRETURN p \nORDER BY length(p) \nLIMIT 10 \n");
    e.put("NEPTUNE|index.create|A", "!ERROR");
    e.put("NEPTUNE|index.drop|A", "!ERROR");
    e.put("NEPTUNE|index.create|B", "!ERROR");
    e.put("NEPTUNE|index.drop|B", "!ERROR");
    e.put("NEPTUNE|index.create|C", "!ERROR");
    e.put("NEPTUNE|index.drop|C", "!ERROR");
    e.put("NEPTUNE|index.create|D", "!ERROR");
    e.put("NEPTUNE|index.drop|D", "!ERROR");
    e.put("NEPTUNE|vector.create|V1", "!ERROR");
    e.put("NEPTUNE|vector.drop|V1", "!ERROR");
    e.put("NEPTUNE|vector.create|V2", "!ERROR");
    e.put("NEPTUNE|vector.drop|V2", "!ERROR");
    e.put("NEPTUNE|vector.create|V3", "!ERROR");
    e.put("NEPTUNE|vector.drop|V3", "!ERROR");
    e.put("NEPTUNE|vector.create|V4", "!ERROR");
    e.put("NEPTUNE|vector.drop|V4", "!ERROR");
    e.put("NEPTUNE|vector.create|V5", "!ERROR");
    e.put("NEPTUNE|vector.drop|V5", "!ERROR");
    e.put("NEPTUNE|vector.create|V6", "!ERROR");
    e.put("NEPTUNE|vector.drop|V6", "!ERROR");
    e.put("NEPTUNE|vector.create|V7", "!ERROR");
    e.put("NEPTUNE|vector.drop|V7", "!ERROR");
    e.put("NEPTUNE|constraint.create|K1", "!ERROR");
    e.put("NEPTUNE|constraint.drop|K1", "!ERROR");
    e.put("NEPTUNE|constraint.create|K2", "!ERROR");
    e.put("NEPTUNE|constraint.drop|K2", "!ERROR");
    e.put("NEPTUNE|constraint.create|K3", "!ERROR");
    e.put("NEPTUNE|constraint.drop|K3", "!ERROR");
    e.put("NEPTUNE|constraint.create|K4", "!ERROR");
    e.put("NEPTUNE|constraint.drop|K4", "!ERROR");
    e.put("NEPTUNE|constraint.create|K5", "!ERROR");
    e.put("NEPTUNE|constraint.drop|K5", "!ERROR");
    e.put("NEPTUNE|constraint.create|K6", "!ERROR");
    e.put("NEPTUNE|constraint.drop|K6", "!ERROR");
    e.put("NEPTUNE|constraint.create|K7", "!ERROR");
    e.put("NEPTUNE|constraint.drop|K7", "!ERROR");
    e.put("NEPTUNE|constraint.create|K8", "!ERROR");
    e.put("NEPTUNE|constraint.drop|K8", "!ERROR");
    e.put("NEPTUNE|constraint.create|K9", "!ERROR");
    e.put("NEPTUNE|constraint.drop|K9", "!ERROR");
    e.put("NEPTUNE|nodekey.create|N1", "null");
    e.put("NEPTUNE|nodekey.create|N2", "null");
    e.put("NEPTUNE|nodekey.create|N3", "null");
    e.put("NEPTUNE|nodekey.create|N4", "null");
    e.put("NEPTUNE|autocommit|S1", "false");
    e.put("NEPTUNE|autocommit|S2", "false");
    e.put("NEPTUNE|autocommit|S3", "false");
    e.put("NEPTUNE|autocommit|S4", "false");
    e.put("NEPTUNE|autocommit|S5", "false");
    e.put("NEPTUNE|autocommit|S6", "false");
    e.put("NEPTUNE|autocommit|S7", "false");
    e.put("NEPTUNE|autocommit|S8", "false");
    e.put("NEPTUNE|autocommit|S9", "false");
    e.put("NEPTUNE|autocommit|S10", "false");
    e.put("NEPTUNE|autocommit|S11", "false");
    e.put("NEPTUNE|autocommit|S12", "false");
    e.put("NEPTUNE|vectorValue|$p", "$p");
    e.put("NEPTUNE|vectorValue|pr.p", "pr.p");
    e.put("NEPTUNE|flag|cypher", "true");
    e.put("NEPTUNE|flag|nodeIndexes", "false");
    e.put("NEPTUNE|flag|relationshipIndexes", "false");
    e.put("NEPTUNE|flag|schemaChangesInTransactions", "true");
    e.put("NEPTUNE|flag|vectorIndexes", "false");
    e.put("NEPTUNE|flag|nodeConstraintTypes", "[]");
    e.put("NEPTUNE|flag|relationshipConstraintTypes", "[]");
    e.put(
        "NEPTUNE|path|toRoot",
        "MATCH p = (top:Execution)-[:EXECUTES*]->(child:Execution {id: $executionId }) \nWHERE top.parentId IS NULL \nAND   size([n IN nodes(p) WHERE n.type IS NOT NULL]) = 0 \nRETURN p \nLIMIT 10 \n");
    e.put(
        "NEPTUNE|path|toFailed",
        "MATCH p = (top:Execution {id: $executionId })-[:EXECUTES*]->(child:Execution) \nWHERE child.failed = true \nAND   child.id <> $executionId \nOPTIONAL MATCH (child)-[grandChild:EXECUTES]->() \nWITH p, count(grandChild) AS grandChildren \nWHERE grandChildren = 0 \nAND   size([n IN nodes(p) WHERE n.type IS NOT NULL]) = 0 \nRETURN p \nORDER BY length(p) \nLIMIT 10 \n");
    return e;
  }
}
