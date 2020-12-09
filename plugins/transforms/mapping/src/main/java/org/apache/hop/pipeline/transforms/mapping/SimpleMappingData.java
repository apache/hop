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

package org.apache.hop.pipeline.transforms.mapping;

import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transforms.input.MappingInput;
import org.apache.hop.pipeline.transforms.output.MappingOutput;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Matt
 * @since 24-jan-2005
 */
public class SimpleMappingData extends BaseTransformData implements ITransformData {
  public Pipeline mappingPipeline;

  public MappingInput mappingInput;

  public MappingOutput mappingOutput;

  public List<Integer> renameFieldIndexes;

  public List<String> renameFieldNames;

  public boolean wasStarted;

  public PipelineMeta mappingPipelineMeta;

  public IRowMeta outputRowMeta;

  public List<MappingValueRename> inputRenameList;

  protected int linesReadTransformNr = -1;

  protected int linesInputTransformNr = -1;

  protected int linesWrittenTransformNr = -1;

  protected int linesOutputTransformNr = -1;

  protected int linesUpdatedTransformNr = -1;

  protected int linesRejectedTransformNr = -1;

  public RowDataInputMapper rowDataInputMapper;

  /**
   *
   */
  public SimpleMappingData() {
    super();
    mappingPipeline = null;
    wasStarted = false;
    inputRenameList = new ArrayList<>();
  }

  public Pipeline getMappingPipeline() {
    return mappingPipeline;
  }

  public void setMappingPipeline( Pipeline mappingPipeline ) {
    this.mappingPipeline = mappingPipeline;
  }
}
