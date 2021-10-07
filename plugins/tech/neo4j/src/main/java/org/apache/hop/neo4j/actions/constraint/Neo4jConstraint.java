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

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Result;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.neo4j.shared.NeoConnection;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.workflow.action.IAction;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;

import java.util.ArrayList;
import java.util.List;

@Action(
    id = "NEO4J_CONSTRAINT",
    name = "Neo4j Constraint",
    description = "Create or delete constraints in a Neo4j database",
    image = "neo4j_constraint.svg",
    categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.Scripting",
        keywords = "i18n::Neo4jConstraint.keyword",
    documentationUrl = "/workflow/actions/neo4j-constraint.html")
public class Neo4jConstraint extends ActionBase implements IAction {

  @HopMetadataProperty(key = "connection", storeWithName = true)
  private NeoConnection connection;

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

  private void dropConstraint(final ConstraintUpdate constraintUpdate) throws HopException {
    String cypher = "DROP CONSTRAINT ";

    if (StringUtils.isNotEmpty(constraintUpdate.getConstraintName())) {
      cypher += constraintUpdate.getConstraintName();
    } else {
      throw new HopException(
          "Please drop constraint on relationship properties with the name of the constraint. This was for label: "
              + constraintUpdate.getObjectName()
              + ", properties: "
              + constraintUpdate.getObjectProperties());
    }
    cypher += " IF EXISTS ";

    // Run this cypher statement...
    //
    final String _cypher = cypher;
    try (Driver driver = connection.getDriver(log, this)) {
      try (Session session = connection.getSession(log, driver, this)) {
        session.writeTransaction(
            tx -> {
              try {
                log.logDetailed("Dropping constraint with cypher: " + _cypher);
                org.neo4j.driver.Result result = tx.run(_cypher);
                result.consume();
                return true;
              } catch (Throwable e) {
                log.logError("Error dropping constraint with cypher [" + _cypher + "]", e);
                return false;
              }
            });
      }
    }
  }

  private void createConstraint(ConstraintUpdate constraintUpdate) throws HopException {
    String cypher = "CREATE CONSTRAINT ";

    if (StringUtils.isNotEmpty(constraintUpdate.getConstraintName())) {
      cypher += constraintUpdate.getConstraintName();
    } else {
      throw new HopException(
          "Please create constraints on relationship properties with a name for the constraint. This was for label: "
              + constraintUpdate.getObjectName()
              + ", properties: "
              + constraintUpdate.getObjectProperties());
    }

    cypher += " IF NOT EXISTS ON ";

    if (constraintUpdate.getObjectType() == ObjectType.NODE) {
      // Constraint on a node
      //
      cypher += "(n:" + constraintUpdate.getObjectName() + ") ";
      cypher += "ASSERT ";
      switch (constraintUpdate.getConstraintType()) {
        case UNIQUE:
          cypher += " n." + constraintUpdate.getObjectProperties() + " IS UNIQUE ";
          break;
        case NOT_NULL:
          cypher += " n." + constraintUpdate.getObjectProperties() + " IS NOT NULL ";
          break;
      }

    } else {
      // constraint on a relationship
      //
      cypher += "()-[r:" + constraintUpdate.getObjectName() + "]-() ";
      cypher += "ASSERT ";
      switch (constraintUpdate.getConstraintType()) {
        case UNIQUE:
          cypher += " r." + constraintUpdate.getObjectProperties() + " IS UNIQUE ";
          break;
        case NOT_NULL:
          cypher += " r." + constraintUpdate.getObjectProperties() + " IS NOT NULL ";
          break;
      }
    }

    // Run this cypher statement...
    //
    final String _cypher = cypher;
    try (Driver driver = connection.getDriver(log, this)) {
      try (Session session = connection.getSession(log, driver, this)) {
        session.writeTransaction(
            tx -> {
              try {
                log.logDetailed("Creating constraint with cypher: " + _cypher);
                org.neo4j.driver.Result result = tx.run(_cypher);
                result.consume();
                return true;
              } catch (Throwable e) {
                log.logError("Error creating constraint with cypher [" + _cypher + "]", e);
                return false;
              }
            });
      }
    }
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
   * Gets connection
   *
   * @return value of connection
   */
  public NeoConnection getConnection() {
    return connection;
  }

  /** @param connection The connection to set */
  public void setConnection(NeoConnection connection) {
    this.connection = connection;
  }

  /**
   * Gets constraintUpdates
   *
   * @return value of constraintUpdates
   */
  public List<ConstraintUpdate> getConstraintUpdates() {
    return constraintUpdates;
  }

  /** @param constraintUpdates The constraintUpdates to set */
  public void setConstraintUpdates(List<ConstraintUpdate> constraintUpdates) {
    this.constraintUpdates = constraintUpdates;
  }
}
