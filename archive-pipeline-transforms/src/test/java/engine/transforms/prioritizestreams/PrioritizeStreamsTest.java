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

package org.apache.hop.pipeline.transforms.prioritizestreams;

import junit.framework.Assert;
import org.apache.hop.core.QueueRowSet;
import org.apache.hop.core.RowSet;
import org.apache.hop.core.SingleRowRowSet;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.logging.LoggingObjectInterface;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

public class PrioritizeStreamsTest {

  private static TransformMockHelper<PrioritizeStreamsMeta, ITransformData> transformMockHelper;

  @BeforeClass
  public static void setup() {
    transformMockHelper =
      new TransformMockHelper<PrioritizeStreamsMeta, ITransformData>( "Priority Streams Test",
        PrioritizeStreamsMeta.class, ITransformData.class );
    when( transformMockHelper.logChannelFactory.create( any(), any( LoggingObjectInterface.class ) ) ).thenReturn(
      transformMockHelper.logChannelInterface );
    when( transformMockHelper.pipeline.isRunning() ).thenReturn( true );
  }

  @AfterClass
  public static void tearDown() {
    transformMockHelper.cleanUp();
  }

  /**
   * [PDI-9088] Prioritize streams transform causing null pointer exception downstream
   *
   * @throws HopException
   */
  @Test
  public void testProcessRow() throws HopException {
    PrioritizeStreamsMeta meta = new PrioritizeStreamsMeta();
    meta.setTransformName( new String[] { "high", "medium", "low" } );
    PrioritizeStreamsData data = new PrioritizeStreamsData();

    PrioritizeStreamsInner transform = new PrioritizeStreamsInner( transformMockHelper );
    try {
      transform.init();
    } catch ( NullPointerException e ) {
      fail( "NullPointerException detecded, seems that IRowMeta was not set for RowSet you are attempting"
        + "to read from." );
    }

    Assert.assertTrue( "First waiting for row set is 'high'", data.currentRowSet.getClass().equals(
      SingleRowRowSet.class ) );
  }

  private class PrioritizeStreamsInner extends PrioritizeStreams {

    public PrioritizeStreamsInner( TransformMockHelper<PrioritizeStreamsMeta, ITransformData> transformMockHelper ) {
      super( transformMockHelper.transformMeta, transformMockHelper.iTransformMeta, transformMockHelper.iTransformData, 0, transformMockHelper.pipelineMeta,
        transformMockHelper.pipeline );
    }

    @Override
    public RowSet findInputRowSet( String sourceTransform ) throws HopTransformException {
      if ( sourceTransform.equals( "high" ) ) {
        return new SingleRowRowSet();
      }
      return new QueueRowSet();
    }

    @Override
    protected void checkInputLayoutValid( IRowMeta referenceRowMeta, IRowMeta compareRowMeta ) {
      // always true.
    }

    @Override
    public Object[] getRowFrom( RowSet rowSet ) throws HopTransformException {
      rowSet.setRowMeta( new RowMeta() );
      return new Object[] {};
    }

    @Override
    public void putRow( IRowMeta rmi, Object[] input ) {
      if ( rmi == null ) {
        throw new NullPointerException();
      }
    }
  }
}
