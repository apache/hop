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

package org.apache.hop.pipeline;

import org.apache.hop.core.CheckResultInterface;
import org.apache.hop.core.variables.VariableSpace;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.pipeline.step.StepMeta;

import java.util.List;

public class CheckStepsExtension {
  private final List<CheckResultInterface> remarks;
  private final VariableSpace variableSpace;
  private final PipelineMeta pipelineMeta;
  private final StepMeta[] stepMetas;
  private final IMetaStore metaStore;

  public CheckStepsExtension(
    List<CheckResultInterface> remarks,
    VariableSpace space,
    PipelineMeta pipelineMeta,
    StepMeta[] stepMetas,
    IMetaStore metaStore ) {
    this.remarks = remarks;
    this.variableSpace = space;
    this.pipelineMeta = pipelineMeta;
    this.stepMetas = stepMetas;
    this.metaStore = metaStore;
  }

  public List<CheckResultInterface> getRemarks() {
    return remarks;
  }

  public VariableSpace getVariableSpace() {
    return variableSpace;
  }

  public PipelineMeta getPipelineMeta() {
    return pipelineMeta;
  }

  public StepMeta[] getStepMetas() {
    return stepMetas;
  }

  public IMetaStore getMetaStore() {
    return metaStore;
  }
}
