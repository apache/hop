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

package org.apache.hop.core.graph;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.HopMetadataObject;

/**
 * The type specific part of a {@link GraphDatabaseMeta}: the settings of one graph database type.
 * Implementations hold settings only. Everything that needs a live connection goes through {@link
 * #connect(ILogChannel, IVariables, String)}.
 */
@HopMetadataObject(objectFactory = GraphDatabaseObjectFactory.class)
public interface IGraphDatabase extends Cloneable {

  String getPluginId();

  void setPluginId(String pluginId);

  String getPluginName();

  void setPluginName(String pluginName);

  IGraphDatabase clone();

  /**
   * @return The dialect of this database type: what it supports and the syntax of its index and
   *     constraint statements, the same as {@link IGraphConnection#getGraphDialect()} of its
   *     connections. By default Cypher with the syntax of Neo4j 5.
   */
  default IGraphDialect getGraphDialect() {
    return CypherGraphDialect.DEFAULT;
  }

  /**
   * The dialect with the settings of this database resolved, for the dialects which depend on them,
   * like the graph name in the index statements of Apache AGE.
   *
   * @param variables The variables to resolve the settings with
   */
  default IGraphDialect getGraphDialect(IVariables variables) {
    return getGraphDialect();
  }

  /**
   * Open a connection to the graph database. The caller closes it.
   *
   * @param log The log channel to log to
   * @param variables The variables to resolve the settings with
   * @param connectionName The name of the graph database connection, used in messages
   * @return An open connection
   * @throws HopException In case the connection could not be opened
   */
  IGraphConnection connect(ILogChannel log, IVariables variables, String connectionName)
      throws HopException;

  /**
   * Test the connection.
   *
   * @param variables The variables to resolve the settings with
   * @param connectionName The name of the graph database connection, used in messages
   * @return A description of what was tested, for example the URL
   * @throws HopException In case the database could not be reached
   */
  String test(IVariables variables, String connectionName) throws HopException;
}
