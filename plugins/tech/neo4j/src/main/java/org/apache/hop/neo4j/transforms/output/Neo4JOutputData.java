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
 *
 */

package org.apache.hop.neo4j.transforms.output;

import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.neo4j.model.GraphPropertyType;
import org.apache.hop.neo4j.shared.NeoConnection;
import org.apache.hop.neo4j.transforms.BaseNeoTransformData;
import org.apache.hop.pipeline.transform.ITransformData;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;

import java.util.List;
import java.util.Map;

public class Neo4JOutputData extends BaseNeoTransformData implements ITransformData {

  public IRowMeta outputRowMeta;

  public String[] fieldNames;

  public NeoConnection neoConnection;
  public String url;
  public Driver driver;
  public Session session;

  public long batchSize;
  public long outputCount;

  public int[] fromNodePropIndexes;
  public int[] fromNodeLabelIndexes;
  public int[] toNodePropIndexes;
  public int[] toNodeLabelIndexes;
  public int[] relPropIndexes;
  public int relationshipIndex;
  public GraphPropertyType[] fromNodePropTypes;
  public GraphPropertyType[] toNodePropTypes;
  public GraphPropertyType[] relPropTypes;

  public List<Map<String, Object>> unwindList;

  public String fromLabelsClause;
  public String toLabelsClause;
  public String[] fromLabelValues;
  public String[] toLabelValues;
  public String relationshipLabelValue;

  public String previousFromLabelsClause;
  public String previousToLabelsClause;

  public boolean dynamicFromLabels;
  public boolean dynamicToLabels;
  public boolean dynamicRelLabel;

  public List<String> previousFromLabels;
  public List<String> fromLabels;
  public List<String> previousToLabels;
  public List<String> toLabels;
  public String previousRelationshipLabel;
  public String relationshipLabel;

  public OperationType fromOperationType;
  public OperationType toOperationType;
  public OperationType relOperationType;

  public String cypher;
  public boolean version4;
}
