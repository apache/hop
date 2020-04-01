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

package org.apache.test.util;

import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.StepPluginType;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.SingleThreadedPipelineExecutor;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.step.StepMeta;
import org.apache.hop.pipeline.step.StepMetaInterface;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;

/**
 * This is a base class for creating guard tests, that check a step cannot be executed in the single-threaded mode
 *
 * @author Andrey Khayrutdinov
 */
public abstract class SingleThreadedExecutionGuarder<Meta extends StepMetaInterface> {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @BeforeClass
  public static void setUp() throws Exception {
    HopEnvironment.init();
  }

  protected abstract Meta createMeta();

  @Test( expected = HopException.class )
  public void failsWhenGivenNonSingleThreadSteps() throws Exception {
    Meta metaInterface = createMeta();

    PluginRegistry plugReg = PluginRegistry.getInstance();
    String id = plugReg.getPluginId( StepPluginType.class, metaInterface );
    assertNotNull( "pluginId", id );

    StepMeta stepMeta = new StepMeta( id, "stepMetrics", metaInterface );

    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName( "failsWhenGivenNonSingleThreadSteps" );
    pipelineMeta.addStep( stepMeta );

    Pipeline pipeline = new Pipeline( pipelineMeta );
    pipeline.prepareExecution();

    SingleThreadedPipelineExecutor executor = new SingleThreadedPipelineExecutor( pipeline );
    executor.init();
  }
}
