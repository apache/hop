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

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.Result;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.graph.GraphIndexDefinition;
import org.apache.hop.core.graph.GraphObjectType;
import org.apache.hop.core.graph.GraphVectorIndexDefinition;
import org.apache.hop.core.graph.IGraphDialect;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataPropertyType;
import org.apache.hop.neo4j.shared.NamedGraphConnection;
import org.apache.hop.neo4j.shared.NeoConnectionUtils;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.workflow.action.IAction;

@Action(
    id = "NEO4J_INDEX",
    name = "Graph index",
    description = "Create or delete indexes in a graph database",
    image = "graph_index.svg",
    categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.Scripting",
    keywords = "i18n::Neo4jIndex.keyword",
    documentationUrl = "/workflow/actions/graph-index.html")
public class Neo4jIndex extends ActionBase implements IAction {

  /** The name of the Neo4j or Bolt graph database connection. */
  @HopMetadataProperty(
      key = "connection",
      hopMetadataPropertyType = HopMetadataPropertyType.GRAPH_CONNECTION)
  private String connectionName;

  private NamedGraphConnection connection;

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
    // Success unless something goes wrong, whatever the result of the previous action
    result.setResult(true);

    connection =
        NeoConnectionUtils.findGraphConnection(getMetadataProvider(), resolve(connectionName));

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

  /**
   * Generate the statement to drop an index in the given dialect.
   *
   * @throws HopException if the database doesn't support it or information is missing
   */
  public static String generateDropIndexCypher(IndexUpdate indexUpdate, IGraphDialect dialect)
      throws HopException {
    if (indexUpdate.isVector()) {
      return dialect.getDropVectorIndexStatement(toVectorIndexDefinition(indexUpdate, false));
    }
    return dialect.getDropIndexStatement(toIndexDefinition(indexUpdate));
  }

  /**
   * Generate the statement to create an index in the given dialect.
   *
   * @throws HopException if the database doesn't support it or information is missing
   */
  public static String generateCreateIndexCypher(IndexUpdate indexUpdate, IGraphDialect dialect)
      throws HopException {
    if (indexUpdate.isVector()) {
      return dialect.getCreateVectorIndexStatement(toVectorIndexDefinition(indexUpdate, true));
    }
    return dialect.getCreateIndexStatement(toIndexDefinition(indexUpdate));
  }

  /** A copy of the update with the variables in its vector settings resolved. */
  private IndexUpdate resolved(IndexUpdate indexUpdate) {
    IndexUpdate copy = new IndexUpdate(indexUpdate);
    copy.setVectorDimensions(resolve(indexUpdate.getVectorDimensions()));
    copy.setVectorCapacity(resolve(indexUpdate.getVectorCapacity()));
    return copy;
  }

  private void dropIndex(final IndexUpdate indexUpdate) throws HopException {
    String cypher = generateDropIndexCypher(resolved(indexUpdate), connection.getDialect());

    // Run this cypher statement...
    //
    NeoConnectionUtils.runSchemaStatement(
        connection, getLogChannel(), this, cypher, "Dropping index");
  }

  private void createIndex(IndexUpdate indexUpdate) throws HopException {
    String cypher = generateCreateIndexCypher(resolved(indexUpdate), connection.getDialect());

    // Run this cypher statement...
    //
    NeoConnectionUtils.runSchemaStatement(
        connection, getLogChannel(), this, cypher, "Creating index");
  }

  static GraphIndexDefinition toIndexDefinition(IndexUpdate indexUpdate) {
    return new GraphIndexDefinition(
        indexUpdate.getIndexName(),
        toGraphObjectType(indexUpdate.getObjectType()),
        indexUpdate.getObjectName(),
        splitProperties(indexUpdate.getObjectProperties()));
  }

  /**
   * @param creating True to create the index: the vector settings are needed and validated
   */
  static GraphVectorIndexDefinition toVectorIndexDefinition(
      IndexUpdate indexUpdate, boolean creating) throws HopException {
    Integer dimensions = null;
    Integer capacity = null;
    if (creating) {
      dimensions = parsePositive(indexUpdate.getVectorDimensions(), "vector dimensions", false);
      capacity = parsePositive(indexUpdate.getVectorCapacity(), "vector capacity", true);
    }
    return new GraphVectorIndexDefinition(
        indexUpdate.getIndexName(),
        toGraphObjectType(indexUpdate.getObjectType()),
        indexUpdate.getObjectName(),
        splitProperties(indexUpdate.getObjectProperties()),
        dimensions,
        indexUpdate.getVectorSimilarity(),
        capacity);
  }

  static GraphObjectType toGraphObjectType(ObjectType objectType) {
    return objectType == ObjectType.RELATIONSHIP
        ? GraphObjectType.RELATIONSHIP
        : GraphObjectType.NODE;
  }

  /** The comma separated properties, trimmed. Empty for an empty list. */
  public static List<String> splitProperties(String properties) {
    List<String> list = new ArrayList<>();
    if (StringUtils.isEmpty(properties)) {
      return list;
    }
    for (String property : properties.split(",")) {
      list.add(Const.trim(property));
    }
    return list;
  }

  /**
   * @param optional True if the value may be empty, which gives null
   */
  private static Integer parsePositive(String value, String what, boolean optional)
      throws HopException {
    if (StringUtils.isBlank(value)) {
      if (optional) {
        return null;
      }
      throw new HopException("Please specify the " + what + " of the vector index");
    }
    try {
      int number = Integer.parseInt(value.trim());
      if (number > 0) {
        return number;
      }
    } catch (NumberFormatException e) {
      // Reported below
    }
    throw new HopException(
        "The " + what + " of a vector index must be a positive number: " + value);
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
   * Gets indexUpdates
   *
   * @return value of indexUpdates
   */
  public List<IndexUpdate> getIndexUpdates() {
    return indexUpdates;
  }

  /**
   * @param indexUpdates The indexUpdates to set
   */
  public void setIndexUpdates(List<IndexUpdate> indexUpdates) {
    this.indexUpdates = indexUpdates;
  }
}
