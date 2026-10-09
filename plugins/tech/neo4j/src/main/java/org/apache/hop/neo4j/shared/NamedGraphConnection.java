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

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.graph.GraphDatabaseMeta;
import org.apache.hop.core.graph.IGraphConnection;
import org.apache.hop.core.graph.IGraphDialect;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.neo4j.bolt.BoltGraphConnection;
import org.apache.hop.neo4j.bolt.Neo4jGraphDialect;
import org.neo4j.driver.Driver;

/**
 * A connection found by name: a Neo4j connection or a graph database connection of any type. Tells
 * the dialect without connecting and opens connections on demand.
 *
 * @param name The connection name
 * @param neoConnection The Neo4j connection, or null for a graph database connection
 * @param graphDatabaseMeta The graph database connection, or null for a Neo4j connection
 */
public record NamedGraphConnection(
    String name, NeoConnection neoConnection, GraphDatabaseMeta graphDatabaseMeta) {

  /** The dialect of the database. */
  public IGraphDialect getDialect() {
    if (neoConnection != null) {
      return neoConnection.getDialect();
    }
    if (graphDatabaseMeta.getGraphDatabase() == null) {
      return Neo4jGraphDialect.INSTANCE;
    }
    return graphDatabaseMeta.getGraphDatabase().getGraphDialect();
  }

  /**
   * The dialect of the database with its settings resolved, for the statements which depend on
   * them.
   */
  public IGraphDialect getDialect(IVariables variables) {
    if (neoConnection != null || graphDatabaseMeta.getGraphDatabase() == null) {
      return getDialect();
    }
    return graphDatabaseMeta.getGraphDatabase().getGraphDialect(variables);
  }

  /** Open a connection. The caller closes it. */
  public IGraphConnection connect(ILogChannel log, IVariables variables) throws HopException {
    if (neoConnection != null) {
      Driver driver = neoConnection.getDriver(log, variables);
      try {
        return new BoltGraphConnection(
            driver,
            neoConnection.getSession(log, driver, variables),
            neoConnection.getDialect(),
            log);
      } catch (RuntimeException e) {
        driver.close();
        throw new HopException("Unable to open a session on " + name, e);
      }
    }
    return graphDatabaseMeta.connect(log, variables);
  }

  /** Test the connection, returns a description of what was tested. */
  public String test(IVariables variables) throws HopException {
    if (neoConnection != null) {
      neoConnection.test(variables);
      return neoConnection.getUrl(variables);
    }
    return graphDatabaseMeta.test(variables);
  }
}
