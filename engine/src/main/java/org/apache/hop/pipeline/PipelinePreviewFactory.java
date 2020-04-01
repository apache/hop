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

import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.StepPluginType;
import org.apache.hop.core.variables.VariableSpace;
import org.apache.hop.pipeline.step.StepMeta;
import org.apache.hop.pipeline.step.StepMetaInterface;
import org.apache.hop.pipeline.steps.dummy.DummyMeta;

public class PipelinePreviewFactory {
  public static final PipelineMeta generatePreviewPipeline( VariableSpace parent, StepMetaInterface oneMeta,
                                                            String oneStepname ) {
    PluginRegistry registry = PluginRegistry.getInstance();

    PipelineMeta previewMeta = new PipelineMeta( parent );
    // The following operation resets the internal variables!
    //
    previewMeta.setName( parent == null ? "Preview pipeline" : parent.toString() );

    // At it to the first step.
    StepMeta one = new StepMeta( registry.getPluginId( StepPluginType.class, oneMeta ), oneStepname, oneMeta );
    one.setLocation( 50, 50 );
    previewMeta.addStep( one );

    DummyMeta twoMeta = new DummyMeta();
    StepMeta two = new StepMeta( registry.getPluginId( StepPluginType.class, twoMeta ), "dummy", twoMeta );
    two.setLocation( 250, 50 );
    previewMeta.addStep( two );

    PipelineHopMeta hop = new PipelineHopMeta( one, two );
    previewMeta.addPipelineHop( hop );

    return previewMeta;
  }
}
