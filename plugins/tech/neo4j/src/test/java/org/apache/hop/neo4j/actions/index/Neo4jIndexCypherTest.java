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

package org.apache.hop.neo4j.actions.index;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.core.exception.HopException;
import org.junit.jupiter.api.Test;

class Neo4jIndexCypherTest {

  @Test
  void dropRequiresAnIndexName() {
    IndexUpdate update = new IndexUpdate(UpdateType.DROP, ObjectType.NODE, "", "Person", "id");

    HopException exception =
        assertThrows(HopException.class, () -> Neo4jIndex.generateDropIndexCypher(update));
    assertTrue(exception.getMessage().contains("Please drop indexes with the name of the index"));
  }

  @Test
  void dropUsesIfExists() throws HopException {
    IndexUpdate update =
        new IndexUpdate(UpdateType.DROP, ObjectType.NODE, "idx_person_id", "Person", "id");

    assertEquals("DROP INDEX idx_person_id IF EXISTS", Neo4jIndex.generateDropIndexCypher(update));
  }

  @Test
  void createUsesForOnSyntax() {
    IndexUpdate update =
        new IndexUpdate(UpdateType.CREATE, ObjectType.NODE, "idx_person_id", "Person", "id,name");

    String cypher = Neo4jIndex.generateCreateIndexCypher(update);
    assertTrue(cypher.contains("CREATE INDEX idx_person_id IF NOT EXISTS FOR (n:Person)"));
    assertTrue(cypher.contains("ON (n.id, n.name)"));
  }
}
