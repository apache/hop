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

package org.apache.hop.neo4j.actions.cypherscript;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.Result;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.graph.IGraphConnection;
import org.apache.hop.core.graph.IGraphDialect;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataPropertyType;
import org.apache.hop.neo4j.shared.NamedGraphConnection;
import org.apache.hop.neo4j.shared.NeoConnectionUtils;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.workflow.action.IAction;

@Action(
    id = "NEO4J_CYPHER_SCRIPT",
    name = "Graph script",
    description = "Execute a script of Cypher statements or Gremlin traversals on a graph database",
    image = "graph_script.svg",
    categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.Scripting",
    keywords = "i18n::CypherScript.keyword",
    documentationUrl = "/workflow/actions/graph-script.html")
public class CypherScript extends ActionBase implements IAction {
  @HopMetadataProperty(
      key = "connection",
      hopMetadataPropertyType = HopMetadataPropertyType.GRAPH_CONNECTION)
  private String connectionName;

  @HopMetadataProperty(key = "script")
  private String script;

  @HopMetadataProperty(key = "replace_variables")
  private boolean replacingVariables;

  public CypherScript() {
    this("", "");
  }

  public CypherScript(String name) {
    this(name, "");
  }

  public CypherScript(String name, String description) {
    super(name, description);
  }

  @Override
  public Result execute(Result result, int nr) throws HopException {
    // Success unless something goes wrong, whatever the result of the previous action
    result.setResult(true);
    // Replace variables & parameters
    //
    NamedGraphConnection graphConnection;
    String realConnectionName = resolve(connectionName);
    try {
      graphConnection =
          NeoConnectionUtils.getGraphConnection(getMetadataProvider(), realConnectionName);
    } catch (Exception e) {
      result.setResult(false);
      result.increaseErrors(1L);
      throw new HopException("Unable to find connection with name '" + realConnectionName + "'", e);
    }

    String realScript;
    if (replacingVariables) {
      realScript = resolve(script);
    } else {
      realScript = script;
    }
    List<String> statements = splitScript(realScript);

    int nrExecuted;

    try (IGraphConnection connection = graphConnection.connect(getLogChannel(), this)) {
      IGraphDialect dialect = connection.getGraphDialect();
      if (dialect.isSupportingSchemaChangesInTransactions()
          && connection.isSupportingTransactions()) {
        try {
          nrExecuted =
              connection.executeWrite(
                  transaction -> {
                    int executed = 0;
                    for (String cypher : statements) {
                      transaction.execute(cypher, Map.of());
                      executed++;
                      if (isDetailed()) {
                        logDetailed("Executed cypher statement: " + cypher);
                      }
                    }
                    // The transaction is committed when this work returns, and rolled back when it
                    // throws: a failed script keeps none of its statements.
                    return executed;
                  });
        } catch (Exception e) {
          logError("Error executing cypher statements, the transaction is rolled back", e);
          result.setNrErrors(1);
          result.setResult(false);
          nrExecuted = 0;
        }
      } else {
        // Index and constraint changes can't run in an explicit transaction here, or there are no
        // transactions at all: run each statement on its own.
        //
        nrExecuted = executeAutoCommit(connection, statements, result);
      }
    }

    if (result.getResult()) {
      if (isBasic()) {
        logBasic("Neo4j script executed " + nrExecuted + " statements without error");
      }
    } else {
      if (isBasic()) {
        logBasic("Neo4j script executed with error(s)");
      }
    }

    return result;
  }

  /** Split the script into statements: a semicolon at the start of a separate line. */
  private static List<String> splitScript(String script) {
    List<String> statements = new ArrayList<>();
    for (String command : script.split("\\r?\\n;")) {
      // Cleanup command: replace leading and trailing whitespaces and newlines
      //
      String cypher = command.replaceFirst("^\\s+", "").replaceFirst("\\s+$", "");
      if (StringUtils.isNotEmpty(cypher)) {
        statements.add(cypher);
      }
    }
    return statements;
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
   * Gets connectionName
   *
   * @return value of connectionName
   */
  public String getConnectionName() {
    return connectionName;
  }

  /**
   * @param connectionName The connectionName to set
   */
  public void setConnectionName(String connectionName) {
    this.connectionName = connectionName;
  }

  /**
   * Gets script
   *
   * @return value of script
   */
  public String getScript() {
    return script;
  }

  /**
   * @param script The script to set
   */
  public void setScript(String script) {
    this.script = script;
  }

  /**
   * Gets replacingVariables
   *
   * @return value of replacingVariables
   */
  public boolean isReplacingVariables() {
    return replacingVariables;
  }

  /**
   * @param replacingVariables The replacingVariables to set
   */
  public void setReplacingVariables(boolean replacingVariables) {
    this.replacingVariables = replacingVariables;
  }

  private int executeAutoCommit(
      IGraphConnection connection, List<String> statements, Result result) {
    int executed = 0;
    try {
      for (String cypher : statements) {
        connection.execute(cypher, Map.of());
        executed++;
        if (isDetailed()) {
          logDetailed("Executed cypher statement: " + cypher);
        }
      }
    } catch (Exception e) {
      logError("Error executing cypher statements...", e);
      result.setNrErrors(1);
      result.setResult(false);
    }
    return executed;
  }
}
