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

package org.apache.hop.pipeline.transforms.dummy;

import org.apache.hop.core.RowSet;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.LoggingObjectInterface;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DummyTest {
  private TransformMockHelper<ITransform, ITransformData> transformMockHelper;

  @Before
  public void setup() throws Exception {
    transformMockHelper =
      new TransformMockHelper<ITransform, ITransformData>(
        "DUMMY TEST", ITransform.class, ITransformData.class );
    when( transformMockHelper.logChannelFactory.create( any(), any( LoggingObjectInterface.class ) ) )
      .thenReturn( transformMockHelper.logChannelInterface );
    when( transformMockHelper.pipeline.isRunning() ).thenReturn( true );
  }

  @After
  public void tearDown() {
    transformMockHelper.cleanUp();
  }

  @Test
  public void testDummyDoesntWriteOutputWithoutInputRow() throws HopException {
    Dymmy dummy =
      new Dymmy(
        transformMockHelper.transformMeta, transformMockHelper.iTransformMeta, transformMockHelper.iTransformData, 0, transformMockHelper.pipelineMeta,
        transformMockHelper.pipeline );
    dummy.processRow();
    RowSet rowSet = transformMockHelper.getMockInputRowSet();
    IRowMeta inputRowMeta = mock( IRowMeta.class );
    when( rowSet.getRowMeta() ).thenReturn( inputRowMeta );
    dummy.addRowSetToInputRowSets( rowSet );
    RowSet outputRowSet = mock( RowSet.class );
    dummy.addRowSetToOutputRowSets( outputRowSet );
    dummy.processRow();
    verify( inputRowMeta, never() ).cloneRow( any( Object[].class ) );
    verify( outputRowSet, never() ).putRow( any( IRowMeta.class ), any( Object[].class ) );
  }

  @Test
  public void testDymmyWritesOutputWithInputRow() throws HopException {
    Dymmy dummy =
      new Dymmy(
        transformMockHelper.transformMeta, transformMockHelper.iTransformMeta, transformMockHelper.iTransformData, 0, transformMockHelper.pipelineMeta,
        transformMockHelper.pipeline );
    dummy.processRow();
    Object[] row = new Object[] { "abcd" };
    RowSet rowSet = transformMockHelper.getMockInputRowSet( row );
    IRowMeta inputRowMeta = mock( IRowMeta.class );
    when( inputRowMeta.clone() ).thenReturn( inputRowMeta );
    when( rowSet.getRowMeta() ).thenReturn( inputRowMeta );
    dummy.addRowSetToInputRowSets( rowSet );
    RowSet outputRowSet = mock( RowSet.class );
    dummy.addRowSetToOutputRowSets( outputRowSet );
    when( outputRowSet.putRow( inputRowMeta, row ) ).thenReturn( true );
    dummy.processRow();
    verify( outputRowSet, times( 1 ) ).putRow( inputRowMeta, row );
  }
}
