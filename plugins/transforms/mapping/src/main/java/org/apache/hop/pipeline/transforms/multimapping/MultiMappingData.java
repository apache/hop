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

package org.apache.hop.pipeline.transforms.multimapping;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hop.core.IRowSet;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transforms.mapping.RowDataInputMapper;

@SuppressWarnings("java:S1104")
public class MultiMappingData extends BaseTransformData implements ITransformData {

  public LocalPipelineEngine mappingPipeline;
  public PipelineMeta mappingPipelineMeta;
  public boolean wasStarted;
  public boolean infoDrained;
  public boolean producersFinished;

  public List<RowDataInputMapper> allInputMappers = new ArrayList<>();
  public List<IRowSet> infoRowSets = new ArrayList<>();
  public List<IRowSet> mainRowSets = new ArrayList<>();
  public Map<IRowSet, RowDataInputMapper> rowSetMappers = new HashMap<>();
  public Set<IRowSet> finishedRowSets = new HashSet<>();
  public int mainRowSetIndex;

  public MultiMappingData() {
    super();
    mappingPipeline = null;
    wasStarted = false;
  }
}
