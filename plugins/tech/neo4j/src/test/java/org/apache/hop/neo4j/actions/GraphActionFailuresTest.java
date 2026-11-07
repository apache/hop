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

package org.apache.hop.neo4j.actions;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.List;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.Result;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.graph.BaseGraphDatabase;
import org.apache.hop.core.graph.CypherGraphDialect;
import org.apache.hop.core.graph.GraphConstraintType;
import org.apache.hop.core.graph.GraphDatabaseMeta;
import org.apache.hop.core.graph.IGraphConnection;
import org.apache.hop.core.graph.IGraphDialect;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.neo4j.actions.constraint.ConstraintUpdate;
import org.apache.hop.neo4j.actions.constraint.Neo4jConstraint;
import org.apache.hop.neo4j.actions.cypherscript.CypherScript;
import org.apache.hop.neo4j.actions.index.IndexUpdate;
import org.apache.hop.neo4j.actions.index.Neo4jIndex;
import org.apache.hop.neo4j.actions.index.ObjectType;
import org.apache.hop.neo4j.actions.index.UpdateType;
import org.apache.hop.workflow.action.ActionBase;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Graph index, Graph constraint and Graph script are false with 1 error when they fail, so a
 * failure hop can handle it, instead of stopping the workflow (#9219).
 */
class GraphActionFailuresTest {

  private static final String REFUSING = "refusing";

  @BeforeAll
  static void beforeAll() throws HopException {
    HopClientEnvironment.init();
  }

  @Test
  void indexFailures() throws Exception {
    assertFalseWithOneError(index("does-not-exist", rangeIndex()));
    assertFalseWithOneError(index("", rangeIndex()));
    assertFalseWithOneError(index(REFUSING, rangeIndex()));

    IndexUpdate noType = rangeIndex();
    noType.setType(null);
    assertFalseWithOneError(index(REFUSING, noType));

    IndexUpdate noProperties = rangeIndex();
    noProperties.setObjectProperties("");
    assertFalseWithOneError(index(REFUSING, noProperties));
  }

  @Test
  void constraintFailures() throws Exception {
    assertFalseWithOneError(constraint("does-not-exist", uniqueConstraint()));
    assertFalseWithOneError(constraint("", uniqueConstraint()));
    assertFalseWithOneError(constraint(REFUSING, uniqueConstraint()));

    ConstraintUpdate noType = uniqueConstraint();
    noType.setUpdateType(null);
    assertFalseWithOneError(constraint(REFUSING, noType));

    ConstraintUpdate noProperty = uniqueConstraint();
    noProperty.setObjectProperties("");
    assertFalseWithOneError(constraint(REFUSING, noProperty));
  }

  @Test
  void scriptFailures() throws Exception {
    assertFalseWithOneError(script("does-not-exist"));
    assertFalseWithOneError(script(""));
    assertFalseWithOneError(script(REFUSING));
  }

  private static void assertFalseWithOneError(ActionBase action) throws HopException {
    Result result = action.execute(new Result(), 0);
    assertFalse(result.getResult(), action.getName() + " is true");
    assertEquals(1, result.getNrErrors(), action.getName());
  }

  private static MemoryMetadataProvider metadataProvider() throws HopException {
    MemoryMetadataProvider metadataProvider = new MemoryMetadataProvider();
    metadataProvider
        .getSerializer(GraphDatabaseMeta.class)
        .save(new GraphDatabaseMeta(REFUSING, new RefusingDatabase()));
    return metadataProvider;
  }

  private static IndexUpdate rangeIndex() {
    IndexUpdate update = new IndexUpdate();
    update.setType(UpdateType.CREATE);
    update.setObjectType(ObjectType.NODE);
    update.setIndexName("A_ID");
    update.setObjectName("A");
    update.setObjectProperties("id");
    return update;
  }

  private static ConstraintUpdate uniqueConstraint() {
    ConstraintUpdate update = new ConstraintUpdate();
    update.setUpdateType(org.apache.hop.neo4j.actions.constraint.UpdateType.CREATE);
    update.setObjectType(org.apache.hop.neo4j.actions.constraint.ObjectType.NODE);
    update.setConstraintName("a_id");
    update.setObjectName("A");
    update.setObjectProperties("id");
    update.setConstraintType(GraphConstraintType.UNIQUE);
    return update;
  }

  private static Neo4jIndex index(String connection, IndexUpdate update) throws HopException {
    Neo4jIndex action = new Neo4jIndex("index on '" + connection + "'");
    action.setMetadataProvider(metadataProvider());
    action.setConnectionName(connection);
    action.setIndexUpdates(List.of(update));
    return action;
  }

  private static Neo4jConstraint constraint(String connection, ConstraintUpdate update)
      throws HopException {
    Neo4jConstraint action = new Neo4jConstraint("constraint on '" + connection + "'");
    action.setMetadataProvider(metadataProvider());
    action.setConnectionName(connection);
    action.setConstraintUpdates(List.of(update));
    return action;
  }

  private static CypherScript script(String connection) throws HopException {
    CypherScript action = new CypherScript("script on '" + connection + "'");
    action.setMetadataProvider(metadataProvider());
    action.setConnectionName(connection);
    action.setScript("RETURN 1");
    return action;
  }

  /** A database which refuses the login, the way a driver does: with a runtime exception. */
  private static class RefusingDatabase extends BaseGraphDatabase {
    @Override
    public IGraphDialect getGraphDialect() {
      return CypherGraphDialect.DEFAULT;
    }

    @Override
    public IGraphConnection connect(ILogChannel log, IVariables variables, String connectionName) {
      throw new IllegalStateException("The client is unauthorized due to authentication failure");
    }

    @Override
    public String test(IVariables variables, String connectionName) {
      return "refusing";
    }
  }
}
