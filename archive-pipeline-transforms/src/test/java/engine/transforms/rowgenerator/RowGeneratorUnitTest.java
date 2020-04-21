/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * http://www.project-hop.org
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

package org.apache.hop.pipeline.transforms.rowgenerator;

import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.RowAdapter;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.ITransform;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class RowGeneratorUnitTest {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  private RowGenerator rowGenerator;

  @BeforeClass
  public static void initEnvironment() throws Exception {
    HopEnvironment.init();
  }

  @Before
  public void setUp() throws HopException {
    // add variable to row generator transform
    ITransform transformMetaInterface = spy( new RowGeneratorMeta() );
    ( (RowGeneratorMeta) transformMetaInterface ).setRowLimit( "${ROW_LIMIT}" );
    String[] strings = {};
    when( ( (RowGeneratorMeta) transformMetaInterface ).getFieldName() ).thenReturn( strings );

    TransformMeta transformMeta = new TransformMeta();
    transformMeta.setITransform( transformMetaInterface );
    transformMeta.setName( "ROW_TRANSFORM_META" );
    ITransformData iTransformData = transformMeta.getITransform().getTransformData();

    // add variable to pipeline variable space
    Map<String, String> map = new HashMap<>();
    map.put( "ROW_LIMIT", "1440" );
    PipelineMeta pipelineMeta = spy( new PipelineMeta() );
    pipelineMeta.injectVariables( map );
    when( pipelineMeta.findTransform( anyString() ) ).thenReturn( transformMeta );

    Pipeline pipeline = spy( new LocalPipelineEngine( pipelineMeta, null ) );
    when( pipeline.getLogChannelId() ).thenReturn( "ROW_LIMIT" );

    //prepare row generator, substitutes variable by value from pipeline variable space
    rowGenerator = spy( new RowGenerator( transformMeta, data, 0, pipelineMeta, pipeline ) );
    rowGenerator.initializeVariablesFrom( pipeline );
    rowGenerator.init();
  }

  @Test
  public void testReadRowLimitAsTransformationVar() throws HopException {
    long rowLimit = ( (RowGeneratorData) rowGenerator.getTransformDataInterface() ).rowLimit;
    assertEquals( rowLimit, 1440 );
  }

  @Test
  public void doesNotWriteRowOnTimeWhenStopped() throws HopException, InterruptedException {
    PipelineMeta pipelineMeta = new PipelineMeta( getClass().getResource( "safe-stop.hpl" ).getPath() );
    Pipeline pipeline = new LocalPipelineEngine( pipelineMeta );
    pipeline.prepareExecution();
    pipeline.getTransforms().get( 1 ).transform.addRowListener( new RowAdapter() {
      @Override public void rowWrittenEvent( IRowMeta rowMeta, Object[] row ) throws HopTransformException {
        pipeline.safeStop();
      }
    } );
    pipeline.startThreads();
    pipeline.waitUntilFinished();
    assertEquals( 1, pipeline.getTransforms().get( 0 ).transform.getLinesWritten() );
    assertEquals( 1, pipeline.getTransforms().get( 1 ).transform.getLinesRead() );
  }
}
