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

package org.apache.hop.neo4j.transforms.cypher;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hop.core.graph.IGraphConnection;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.neo4j.core.data.GraphPropertyDataType;
import org.apache.hop.neo4j.shared.NeoConnection;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;
import org.neo4j.driver.Transaction;

@SuppressWarnings("java:S1104")
public class CypherData extends BaseTransformData implements ITransformData {

  public IRowMeta outputRowMeta;
  public NeoConnection neoConnection;

  /**
   * The connection to a graph database which isn't spoken to over Bolt, for example FalkorDB. Null
   * for Bolt connections, which use the driver and session.
   */
  public IGraphConnection graphConnection;

  public String url;
  public Driver driver;
  public Session session;
  public int[] fieldIndexes;
  public String cypher;
  public long batchSize;
  public Transaction transaction;
  public long outputCount;
  public boolean hasInput;
  public int cypherFieldIndex;

  public String unwindMapName;
  public List<Map<String, Object>> unwindList;

  public List<CypherStatement> cypherStatements;

  public Map<String, GraphPropertyDataType> returnSourceTypeMap;
  public int attempts;

  /**
   * The output rows of the attempt being executed, when the work can be retried. They are passed on
   * once the attempt succeeded, so that a retried attempt doesn't output rows twice.
   */
  public List<Object[]> attemptRows = new ArrayList<>();

  /**
   * True if the output rows of the work being executed are passed on right away instead of being
   * kept in {@link #attemptRows}. See {@code Cypher.isStreamingRows()}.
   */
  public boolean streamingRows;

  /** The number of output rows passed on right away by the work being executed. */
  public long streamedRows;

  /** The notification codes and severities logged already in this run */
  public Set<String> loggedNotifications = new HashSet<>();

  public CypherData() {
    super();
  }
}
