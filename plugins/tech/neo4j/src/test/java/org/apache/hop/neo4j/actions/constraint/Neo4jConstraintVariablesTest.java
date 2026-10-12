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

package org.apache.hop.neo4j.actions.constraint;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.graph.GraphConstraintType;
import org.apache.hop.neo4j.bolt.Neo4jGraphDialect;
import org.junit.jupiter.api.Test;

/** The variables in the constraint name, object name and properties are resolved. */
class Neo4jConstraintVariablesTest {

  private static Neo4jConstraint action() {
    Neo4jConstraint action = new Neo4jConstraint("constraint");
    action.setVariable("CONSTRAINT", "person_id");
    action.setVariable("LABEL", "Person");
    action.setVariable("PROPERTY", "id");
    return action;
  }

  private static ConstraintUpdate update(UpdateType type) {
    return new ConstraintUpdate(
        type,
        ObjectType.NODE,
        GraphConstraintType.UNIQUE,
        "${CONSTRAINT}",
        "${LABEL}",
        "${PROPERTY}");
  }

  @Test
  void create() throws HopException {
    ConstraintUpdate update = update(UpdateType.CREATE);
    ConstraintUpdate resolved = action().resolved(update);
    assertEquals(
        "CREATE CONSTRAINT `person_id` IF NOT EXISTS FOR (n:`Person`) REQUIRE  n.`id` IS UNIQUE ",
        Neo4jConstraint.generateCreateConstraintCypher(resolved, Neo4jGraphDialect.INSTANCE));
    // The action itself keeps the variables
    assertEquals("${CONSTRAINT}", update.getConstraintName());
    assertEquals(GraphConstraintType.UNIQUE, resolved.getConstraintType());
  }

  @Test
  void drop() throws HopException {
    assertEquals(
        "DROP CONSTRAINT `person_id` IF EXISTS ",
        Neo4jConstraint.generateDropConstraintCypher(
            action().resolved(update(UpdateType.DROP)), Neo4jGraphDialect.INSTANCE));
  }
}
