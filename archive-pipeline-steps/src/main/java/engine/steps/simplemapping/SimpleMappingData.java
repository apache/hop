/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.apache.hop.pipeline.steps.simplemapping;

import org.apache.hop.core.row.RowMetaInterface;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.step.BaseStepData;
import org.apache.hop.pipeline.step.StepDataInterface;
import org.apache.hop.pipeline.steps.mapping.MappingValueRename;
import org.apache.hop.pipeline.steps.mappinginput.MappingInput;
import org.apache.hop.pipeline.steps.mappingoutput.MappingOutput;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Matt
 * @since 24-jan-2005
 */
public class SimpleMappingData extends BaseStepData implements StepDataInterface {
  public Pipeline mappingPipeline;

  public MappingInput mappingInput;

  public MappingOutput mappingOutput;

  public List<Integer> renameFieldIndexes;

  public List<String> renameFieldNames;

  public boolean wasStarted;

  public PipelineMeta mappingPipelineMeta;

  public RowMetaInterface outputRowMeta;

  public List<MappingValueRename> inputRenameList;

  protected int linesReadStepNr = -1;

  protected int linesInputStepNr = -1;

  protected int linesWrittenStepNr = -1;

  protected int linesOutputStepNr = -1;

  protected int linesUpdatedStepNr = -1;

  protected int linesRejectedStepNr = -1;

  public RowDataInputMapper rowDataInputMapper;

  /**
   *
   */
  public SimpleMappingData() {
    super();
    mappingPipeline = null;
    wasStarted = false;
    inputRenameList = new ArrayList<MappingValueRename>();
  }

  public Pipeline getMappingPipeline() {
    return mappingPipeline;
  }

  public void setMappingPipeline( Pipeline mappingPipeline ) {
    this.mappingPipeline = mappingPipeline;
  }
}
