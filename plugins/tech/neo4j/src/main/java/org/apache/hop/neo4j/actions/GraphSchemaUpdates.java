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

import java.util.List;
import java.util.function.Function;
import org.apache.hop.core.Result;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.graph.IGraphDialect;
import org.apache.hop.neo4j.shared.NamedGraphConnection;
import org.apache.hop.neo4j.shared.NeoConnectionUtils;
import org.apache.hop.workflow.action.ActionBase;

/** Runs the index or constraint updates of the Graph index and Graph constraint actions. */
public final class GraphSchemaUpdates {

  /** What an update does. */
  public enum Operation {
    DROP,
    CREATE
  }

  /** Generates the statement of an update in the dialect of the database. */
  @FunctionalInterface
  public interface StatementGenerator<T> {
    String generate(T update, IGraphDialect dialect) throws HopException;
  }

  private GraphSchemaUpdates() {}

  /**
   * Run the drops first, then the creates. A failure fails the action with one more error instead
   * of throwing, so a failure hop can handle it: the updates before the failing one stay done, the
   * ones after it are not run.
   *
   * @param action The action, for the connection, variables and log
   * @param result The result to update
   * @param connectionName The name of the graph connection, variables not resolved yet
   * @param object What is updated, for the log: "index" or "constraint"
   * @param updates The updates
   * @param operation The operation of an update, null when it has no update type
   * @param dropStatement Generates the statement of a drop
   * @param createStatement Generates the statement of a create
   */
  public static <T> void execute(
      ActionBase action,
      Result result,
      String connectionName,
      String object,
      List<T> updates,
      Function<T, Operation> operation,
      StatementGenerator<T> dropStatement,
      StatementGenerator<T> createStatement) {
    try {
      NamedGraphConnection connection =
          NeoConnectionUtils.getGraphConnection(
              action.getMetadataProvider(), action.resolve(connectionName));

      for (T update : updates) {
        if (operation.apply(update) == null) {
          throw new HopException(
              "Please make sure to always specify the update type of every " + object);
        }
      }
      for (T update : updates) {
        if (operation.apply(update) == Operation.DROP) {
          NeoConnectionUtils.runSchemaStatement(
              connection,
              action.getLogChannel(),
              action,
              dropStatement.generate(update, connection.getDialect(action)),
              "Dropping " + object);
        }
      }
      for (T update : updates) {
        if (operation.apply(update) == Operation.CREATE) {
          NeoConnectionUtils.runSchemaStatement(
              connection,
              action.getLogChannel(),
              action,
              createStatement.generate(update, connection.getDialect(action)),
              "Creating " + object);
        }
      }
    } catch (Exception e) {
      action.logError("Error running the graph database " + object + " updates", e);
      result.setResult(false);
      result.increaseErrors(1L);
    }
  }
}
