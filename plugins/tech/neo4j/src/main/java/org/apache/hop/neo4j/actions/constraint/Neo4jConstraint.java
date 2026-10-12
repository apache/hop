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
 */

package org.apache.hop.neo4j.actions.constraint;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.Result;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.graph.GraphConstraintDefinition;
import org.apache.hop.core.graph.GraphObjectType;
import org.apache.hop.core.graph.IGraphDialect;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataPropertyType;
import org.apache.hop.neo4j.actions.index.Neo4jIndex;
import org.apache.hop.neo4j.shared.NamedGraphConnection;
import org.apache.hop.neo4j.shared.NeoConnectionUtils;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.workflow.action.IAction;

@Action(
    id = "NEO4J_CONSTRAINT",
    name = "Graph constraint",
    description = "Create or delete constraints in a graph database",
    image = "graph_constraint.svg",
    categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.Scripting",
    keywords = "i18n::Neo4jConstraint.keyword",
    documentationUrl = "/workflow/actions/graph-constraint.html")
public class Neo4jConstraint extends ActionBase implements IAction {

  /** The name of the Neo4j or Bolt graph database connection. */
  @HopMetadataProperty(
      key = "connection",
      hopMetadataPropertyType = HopMetadataPropertyType.GRAPH_CONNECTION)
  private String connectionName;

  private NamedGraphConnection connection;

  @HopMetadataProperty(groupKey = "updates", key = "update")
  private List<ConstraintUpdate> constraintUpdates;

  public Neo4jConstraint() {
    this("", "");
  }

  public Neo4jConstraint(String name) {
    this(name, "");
  }

  public Neo4jConstraint(String name, String description) {
    super(name, description);
    constraintUpdates = new ArrayList<>();
  }

  @Override
  public Result execute(Result result, int nr) throws HopException {
    // Success unless something goes wrong, whatever the result of the previous action
    result.setResult(true);

    connection =
        NeoConnectionUtils.findGraphConnection(getMetadataProvider(), resolve(connectionName));

    if (connection == null) {
      result.setResult(false);
      result.increaseErrors(1L);
      throw new HopException("Please specify a Neo4j connection to use");
    }

    // Loop over the constraint updates to see which need deleting...
    //
    for (ConstraintUpdate constraintUpdate : constraintUpdates) {
      if (constraintUpdate.getUpdateType() == null) {
        throw new HopException("Please make sure to always specify a constraint update type");
      }
      switch (constraintUpdate.getUpdateType()) {
        case DROP:
          dropConstraint(constraintUpdate);
          break;
        default:
          break;
      }
    }

    // Create the constraints if needed
    //
    for (ConstraintUpdate constraintUpdate : constraintUpdates) {
      switch (constraintUpdate.getUpdateType()) {
        case CREATE:
          createConstraint(constraintUpdate);
          break;
        default:
          break;
      }
    }

    return result;
  }

  /**
   * Generate the statement to drop a constraint in the given dialect.
   *
   * @throws HopException if the database doesn't support it or information is missing
   */
  public static String generateDropConstraintCypher(
      ConstraintUpdate constraintUpdate, IGraphDialect dialect) throws HopException {
    return dialect.getDropConstraintStatement(toConstraintDefinition(constraintUpdate));
  }

  /**
   * Generate the statement to create a constraint in the given dialect.
   *
   * @throws HopException if the database doesn't support it or information is missing
   */
  public static String generateCreateConstraintCypher(
      ConstraintUpdate constraintUpdate, IGraphDialect dialect) throws HopException {
    return dialect.getCreateConstraintStatement(toConstraintDefinition(constraintUpdate));
  }

  static GraphConstraintDefinition toConstraintDefinition(ConstraintUpdate constraintUpdate) {
    return new GraphConstraintDefinition(
        constraintUpdate.getConstraintName(),
        constraintUpdate.getObjectType() == ObjectType.RELATIONSHIP
            ? GraphObjectType.RELATIONSHIP
            : GraphObjectType.NODE,
        constraintUpdate.getConstraintType(),
        constraintUpdate.getObjectName(),
        Neo4jIndex.splitProperties(constraintUpdate.getObjectProperties()));
  }

  /**
   * A copy of the update with the variables resolved in its constraint name, object name and
   * properties.
   */
  ConstraintUpdate resolved(ConstraintUpdate constraintUpdate) {
    return resolved(constraintUpdate, this);
  }

  /**
   * A copy of the update with the variables resolved in its constraint name, object name and
   * properties.
   */
  public static ConstraintUpdate resolved(ConstraintUpdate constraintUpdate, IVariables variables) {
    ConstraintUpdate copy = new ConstraintUpdate(constraintUpdate);
    copy.setConstraintName(variables.resolve(constraintUpdate.getConstraintName()));
    copy.setObjectName(variables.resolve(constraintUpdate.getObjectName()));
    copy.setObjectProperties(variables.resolve(constraintUpdate.getObjectProperties()));
    return copy;
  }

  private void dropConstraint(final ConstraintUpdate constraintUpdate) throws HopException {
    String cypher =
        generateDropConstraintCypher(resolved(constraintUpdate), connection.getDialect(this));

    // Run this cypher statement...
    //
    NeoConnectionUtils.runSchemaStatement(
        connection, getLogChannel(), this, cypher, "Dropping constraint");
  }

  private void createConstraint(ConstraintUpdate constraintUpdate) throws HopException {
    String cypher =
        generateCreateConstraintCypher(resolved(constraintUpdate), connection.getDialect(this));

    // Run this cypher statement...
    //
    NeoConnectionUtils.runSchemaStatement(
        connection, getLogChannel(), this, cypher, "Creating constraint");
  }

  @Override
  public boolean isEvaluation() {
    return true;
  }

  @Override
  public boolean isUnconditional() {
    return false;
  }

  /**
   * Gets the name of the connection
   *
   * @return value of connectionName
   */
  public String getConnectionName() {
    return connectionName;
  }

  /**
   * @param connectionName The name of the Neo4j or Bolt graph database connection to use
   */
  public void setConnectionName(String connectionName) {
    this.connectionName = connectionName;
  }

  /**
   * Gets constraintUpdates
   *
   * @return value of constraintUpdates
   */
  public List<ConstraintUpdate> getConstraintUpdates() {
    return constraintUpdates;
  }

  /**
   * @param constraintUpdates The constraintUpdates to set
   */
  public void setConstraintUpdates(List<ConstraintUpdate> constraintUpdates) {
    this.constraintUpdates = constraintUpdates;
  }
}
