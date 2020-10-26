/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
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

package org.apache.hop.pipeline.transforms.missing;

import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.util.AbstractTransformMeta;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transforms.TransformMockUtil;
import org.apache.hop.pipeline.transforms.datagrid.DataGridMeta;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
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

public class MissingPipelineTransformTest {
  private TransformMockHelper<DataGridMeta, ITransformData> helper;

  @Before
  public void setUp() {
    helper = TransformMockUtil.getTransformMockHelper( DataGridMeta.class, "DataGrid_EmptyStringVsNull_Test" );
  }

  @After
  public void cleanUp() {
    helper.cleanUp();
  }

  @Test
  public void testInit() {
    ITransform transformMetaInterface = new AbstractTransformMeta() {

      @Override
      public void setDefault() {
      }

      @Override
      public ITransform getTransform( TransformMeta transformMeta, ITransformData data, int copyNr,
                                    PipelineMeta pipelineMeta,
                                    Pipeline pipeline ) {
        return null;
      }
    };

    TransformMeta transformMeta = new TransformMeta();

    transformMeta.setName( "TestMetaTransform" );
    ITransformData iTransformData = mock( ITransformData.class );
    Pipeline pipeline = new LocalPipelineEngine();
    LogChannel log = mock( LogChannel.class );
    doAnswer( new Answer<Void>() {
      public Void answer( InvocationOnMock invocation ) {

        return null;
      }
    } ).when( log ).logError( anyString() );
    pipeline.setLog( log );
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.addTransform( transformMeta );

    MissingPipelineTransform transform = createAndInitTransform( transformMetaInterface, data );

    assertFalse( transform.init() );
  }

  private MissingPipelineTransform createAndInitTransform( ITransform meta, ITransformData data ) {
    when( helper.transformMeta.getITransform() ).thenReturn( meta );

    MissingPipelineTransform transform = new MissingPipelineTransform( helper.transformMeta, meta, data, 0, helper.pipelineMeta, helper.pipeline );
    transform.init();
    return transform;
  }

}
