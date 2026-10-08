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
import org.apache.hop.metadata.api.IHopMetadata;

/**
 * A metadata type which predates {@link GraphDatabaseMeta} but holds a graph database connection,
 * like the deprecated Neo4j connection. Tools which work on any graph connection by name, like
 * {@link GraphConnectionLookup}, find these through this interface without depending on the plugin
 * which defines them.
 */
public interface IGraphDatabaseMetaConvertible extends IHopMetadata {

  /**
   * @return A graph database connection with the same name and settings. Nothing is saved.
   * @throws HopException In case the graph database type of the connection isn't installed
   */
  GraphDatabaseMeta toGraphDatabaseMeta() throws HopException;
}
