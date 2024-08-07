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
 *
 */

package org.apache.hop.neo4j.transforms.cypherbuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.neo4j.core.data.GraphPropertyDataType;
import org.apache.hop.neo4j.model.GraphPropertyType;
import org.apache.hop.neo4j.shared.NeoConnection;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;

@SuppressWarnings("java:S1104")
public class CypherBuilderData extends BaseTransformData {
  public List<Map<String, Object>> rowParametersList;
  public List<Object[]> inputRowsList;
  public List<Integer> parameterIndexes;
  public ArrayList<GraphPropertyType> parameterTypes;
  public int batchSize;
  public NeoConnection connection;
  public Driver driver;
  public Session session;
  public String unwindAlias;
  public String cypher;
  public IRowMeta outputRowMeta;
  public List<Integer> outputIndexes;
  public List<IValueMeta> outputValues;
  public List<GraphPropertyDataType> neoTypes;
  public boolean needsWriteTransaction;
  public int attempts;
}
