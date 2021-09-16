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

package org.apache.hop.neo4j.actions.index;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
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
    id = "NEO4J_INDEX",
    name = "Neo4j Index",
    description = "Create or delete indexes in a Neo4j database",
    image = "neo4j_index.svg",
    categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.Scripting",
    documentationUrl = "/workflow/actions/neo4j-index.html")
public class Neo4jIndex extends ActionBase implements IAction {

  @HopMetadataProperty(key = "connection", storeWithName = true)
  private NeoConnection connection;

  @HopMetadataProperty(groupKey = "updates", key = "update")
  private List<IndexUpdate> indexUpdates;

  public Neo4jIndex() {
    this("", "");
  }

  public Neo4jIndex(String name) {
    this(name, "");
  }

  public Neo4jIndex(String name, String description) {
    super(name, description);
    indexUpdates = new ArrayList<>();
  }

  @Override
  public Result execute(Result result, int nr) throws HopException {

    if (connection == null) {
      result.setResult(false);
      result.increaseErrors(1L);
      throw new HopException("Please specify a Neo4j connection to use");
    }

    // Loop over the index updates to see which need deleting...
    //
    for (IndexUpdate indexUpdate : indexUpdates) {
      if (indexUpdate.getType() == null) {
        throw new HopException("Please make sure to always specify an index update type");
      }
      switch (indexUpdate.getType()) {
        case DROP:
          dropIndex(indexUpdate);
          break;
        default:
          break;
      }
    }

    // Create the indexes if needed
    //
    for (IndexUpdate indexUpdate : indexUpdates) {
      switch (indexUpdate.getType()) {
        case CREATE:
          createIndex(indexUpdate);
          break;
        default:
          break;
      }
    }

    return result;
  }

  private void dropIndex(final IndexUpdate indexUpdate) throws HopException {
    String cypher = "DROP INDEX ";

    if (StringUtils.isNotEmpty(indexUpdate.getIndexName())) {
      cypher += indexUpdate.getIndexName();
    } else {
      cypher += " ON ";
      switch (indexUpdate.getObjectType()) {
        case NODE:
          cypher +=
              ":" + indexUpdate.getObjectName() + "(" + indexUpdate.getObjectProperties() + ")";
          break;
        case RELATIONSHIP:
          throw new HopException(
              "Please drop indexes on relationship properties with their name.  Relationship label: "
                  + indexUpdate.getObjectName()
                  + ", properties: "
                  + indexUpdate.getObjectProperties());
      }
    }
    cypher += " IF EXISTS";

    // Run this cypher statement...
    //
    final String _cypher = cypher;
    try (Driver driver = connection.getDriver(log, this)) {
      try (Session session = connection.getSession(log, driver, this)) {
        session.writeTransaction(
            tx -> {
              try {
                log.logDetailed("Dropping index with cypher: " + _cypher);
                org.neo4j.driver.Result result = tx.run(_cypher);
                result.consume();
                return true;
              } catch (Throwable e) {
                log.logError("Error dropping index with cypher [" + _cypher + "]", e);
                return false;
              }
            });
      }
    }
  }

  private void createIndex(IndexUpdate indexUpdate) throws HopException {
    String cypher = "CREATE INDEX ";

    if (StringUtils.isNotEmpty(indexUpdate.getIndexName())) {
      cypher += indexUpdate.getIndexName();
    }

    cypher += " IF NOT EXISTS";

    String[] properties = indexUpdate.getObjectProperties().split(",");

    cypher += " FOR ";
    switch (indexUpdate.getObjectType()) {
      case NODE:
        cypher += "(n:" + indexUpdate.getObjectName() + ") ";
        break;
      case RELATIONSHIP:
        cypher += "()-[n:" + indexUpdate.getObjectName() + "]-() ";
        break;
    }

    // Add the properties to index:
    //
    cypher += "ON (";
    for (int i = 0; i < properties.length; i++) {
      String property = properties[i];
      if (i > 0) {
        cypher += ", ";
      }
      cypher += "n." + Const.trim(property);
    }
    cypher += ")";

    // Run this cypher statement...
    //
    final String _cypher = cypher;
    try (Driver driver = connection.getDriver(log, this)) {
      try (Session session = connection.getSession(log, driver, this)) {
        session.writeTransaction(
            tx -> {
              try {
                log.logDetailed("Creating index with cypher: " + _cypher);
                org.neo4j.driver.Result result = tx.run(_cypher);
                result.consume();
                return true;
              } catch (Throwable e) {
                log.logError("Error creating index with cypher [" + _cypher + "]", e);
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
   * Gets indexUpdates
   *
   * @return value of indexUpdates
   */
  public List<IndexUpdate> getIndexUpdates() {
    return indexUpdates;
  }

  /** @param indexUpdates The indexUpdates to set */
  public void setIndexUpdates(List<IndexUpdate> indexUpdates) {
    this.indexUpdates = indexUpdates;
  }
}
