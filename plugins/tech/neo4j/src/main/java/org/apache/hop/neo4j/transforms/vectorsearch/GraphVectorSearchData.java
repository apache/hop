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

package org.apache.hop.neo4j.transforms.vectorsearch;

import java.util.List;
import org.apache.hop.core.graph.GraphStatement;
import org.apache.hop.core.graph.IGraphConnection;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;

public class GraphVectorSearchData extends BaseTransformData implements ITransformData {
  public IGraphConnection connection;
  public GraphStatement statement;

  public IRowMeta inputRowMeta;
  public IRowMeta outputRowMeta;
  public int embeddingFieldIndex = -1;

  /** The value metas of the returned properties, in the order of the statement's columns. */
  public List<IValueMeta> propertyValueMetas;

  /** The minimum score, null for none. */
  public Double minScore;
}
