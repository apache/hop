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

package org.apache.hop.pipeline.steps.missing;

import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.util.AbstractStepMeta;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.step.StepDataInterface;
import org.apache.hop.pipeline.step.StepInterface;
import org.apache.hop.pipeline.step.StepMeta;
import org.apache.hop.pipeline.step.StepMetaInterface;
import org.apache.hop.pipeline.steps.StepMockUtil;
import org.apache.hop.pipeline.steps.datagrid.DataGridMeta;
import org.apache.hop.pipeline.steps.mock.StepMockHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MissingPipelineStepTest {
  private StepMockHelper<DataGridMeta, StepDataInterface> helper;

  @Before
  public void setUp() {
    helper = StepMockUtil.getStepMockHelper( DataGridMeta.class, "DataGrid_EmptyStringVsNull_Test" );
  }

  @After
  public void cleanUp() {
    helper.cleanUp();
  }

  @Test
  public void testInit() {
    StepMetaInterface stepMetaInterface = new AbstractStepMeta() {

      @Override
      public void setDefault() {
      }

      @Override
      public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr,
                                    PipelineMeta pipelineMeta,
                                    Pipeline pipeline ) {
        return null;
      }
    };

    StepMeta stepMeta = new StepMeta();

    stepMeta.setName( "TestMetaStep" );
    StepDataInterface stepDataInterface = mock( StepDataInterface.class );
    Pipeline pipeline = new Pipeline();
    LogChannel log = mock( LogChannel.class );
    doAnswer( new Answer<Void>() {
      public Void answer( InvocationOnMock invocation ) {

        return null;
      }
    } ).when( log ).logError( anyString() );
    pipeline.setLog( log );
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.addStep( stepMeta );

    MissingPipelineStep step = createAndInitStep( stepMetaInterface, stepDataInterface );

    assertFalse( step.init( stepMetaInterface, stepDataInterface ) );
  }

  private MissingPipelineStep createAndInitStep( StepMetaInterface meta, StepDataInterface data ) {
    when( helper.stepMeta.getStepMetaInterface() ).thenReturn( meta );

    MissingPipelineStep step = new MissingPipelineStep( helper.stepMeta, data, 0, helper.pipelineMeta, helper.pipeline );
    step.init( meta, data );
    return step;
  }

}
