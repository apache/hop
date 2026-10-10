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

import java.util.List;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.graph.CypherGraphDialect;
import org.apache.hop.core.graph.GraphIndex;
import org.apache.hop.core.graph.IGraphConnection;

/** The dialect of a graph database spoken to over Bolt: Neo4j, Memgraph or Amazon Neptune. */
public abstract class BoltGraphDialect extends CypherGraphDialect {

  protected BoltGraphDialect(String id) {
    super(id);
  }

  /**
   * List the indexes and unique constraints of the database.
   *
   * @return The indexes, or null if this database can't tell which indexes it has
   */
  public List<GraphIndex> getIndexes(IGraphConnection connection) throws HopException {
    return null;
  }
}
