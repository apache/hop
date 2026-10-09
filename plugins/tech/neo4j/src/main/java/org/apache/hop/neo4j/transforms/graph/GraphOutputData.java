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

package org.apache.hop.neo4j.transforms.graph;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.graph.GraphUpsertNode;
import org.apache.hop.core.graph.GraphUpsertRelationship;
import org.apache.hop.core.graph.IGraphConnection;
import org.apache.hop.core.graph.IGraphTransaction;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.neo4j.model.GraphModel;
import org.apache.hop.neo4j.model.GraphProperty;
import org.apache.hop.neo4j.model.GraphRelationship;
import org.apache.hop.neo4j.model.validation.ModelValidator;
import org.apache.hop.neo4j.shared.NamedGraphConnection;
import org.apache.hop.neo4j.transforms.BaseNeoTransformData;
import org.apache.hop.pipeline.transform.ITransformData;

@SuppressWarnings("java:S1104")
public class GraphOutputData extends BaseNeoTransformData implements ITransformData {

  public IRowMeta outputRowMeta;
  public NamedGraphConnection graphConnection;
  public IGraphConnection connection;
  public String url;
  public int[] fieldIndexes;
  public long batchSize;
  public IGraphTransaction transaction;

  /** Write with upserts instead of Cypher, for graph databases which don't speak Cypher. */
  public boolean upserting;

  public List<GraphUpsertNode> upsertNodes;
  public List<GraphUpsertRelationship> upsertRelationships;
  public long upsertRowCount;
  public long outputCount;
  public boolean hasInput;
  public GraphModel graphModel;
  public Map<String, CypherParameters> cypherMap;
  public HashMap<String, Map<GraphProperty, Integer>> relationshipPropertyIndexMap;
  public ModelValidator modelValidator;

  // How many records were dumped into the unwind maps?
  //
  public int unwindCount;

  // A mapping between a cypher statements and it's unwind parameters map...
  public Map<String, List<Map<String, Object>>> unwindMapList;
  public Map<String, Map<String, GraphRelationship>> fieldValueRelationshipMap;
  public Map<String, Map<String, List<GraphRelationship>>> relationshipsCache;

  public static class RelationshipMappingIndexes {
    public int fieldIndex;
  }

  public List<RelationshipMappingIndexes> relMappingIndexes;

  public List<Integer> nodeMappingIndexes;

  // For a merge on a specific node name, map a field value to the specified node label
  public Map<String, Map<String, String>> nodeValueLabelMergeMap;

  // To set a label on a node after merge, for a specific node name, map a field value to the
  // specified node label
  public Map<String, Map<String, String>> nodeValueLabelSetMap;

  public GraphOutputData() {
    super();
  }
}
