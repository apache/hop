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

package org.apache.hop.neo4j.bolt;

import org.apache.hop.core.graph.GraphDatabasePlugin;
import org.apache.hop.core.gui.plugin.GuiPlugin;

/** Neo4j over Bolt, with the defaults of a Neo4j connection: automatic configuration. */
@GraphDatabasePlugin(
    id = "NEO4J",
    name = "i18n::Neo4jGraphDatabase.name",
    description = "i18n::Neo4jGraphDatabase.description",
    documentationUrl = "/metadata-types/graphs/graph-database-connection.html")
@GuiPlugin
public class Neo4jGraphDatabase extends BoltGraphDatabase {
  @Override
  public BoltGraphDialect getGraphDialect() {
    return Neo4jGraphDialect.INSTANCE;
  }
}
