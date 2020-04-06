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

package org.apache.hop.pipeline.transforms.groupby;

import junit.framework.Assert;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.logging.LoggingObjectInterface;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

/**
 * PDI-10250 - "Group by" transform - Minimum aggregation doesn't work
 */
public class GroupByAggregationNullsTest {

  static TransformMockHelper<GroupByMeta, GroupByData> mockHelper;

  GroupBy transform;
  GroupByData data;

  int def = -113;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    mockHelper = new TransformMockHelper<GroupByMeta, GroupByData>( "Group By", GroupByMeta.class, GroupByData.class );
    when( mockHelper.logChannelFactory.create( any(), any( LoggingObjectInterface.class ) ) ).thenReturn(
      mockHelper.logChannelInterface );
    when( mockHelper.pipeline.isRunning() ).thenReturn( true );
  }

  @AfterClass
  public static void cleanUp() {
    mockHelper.cleanUp();
  }

  @Before
  public void setUp() throws Exception {
    data = new GroupByData();
    data.subjectnrs = new int[] { 0 };
    GroupByMeta meta = new GroupByMeta();
    meta.setAggregateType( new int[] { 5 } );
    IValueMeta vmi = new ValueMetaInteger();
    when( mockHelper.transformMeta.getITransform() ).thenReturn( meta );
    IRowMeta rmi = Mockito.mock( IRowMeta.class );
    data.inputRowMeta = rmi;
    data.outputRowMeta = rmi;
    when( rmi.getValueMeta( Mockito.anyInt() ) ).thenReturn( vmi );
    data.aggMeta = rmi;
    data.agg = new Object[] { def };
    transform = new GroupBy( mockHelper.transformMeta, data, 0, mockHelper.pipelineMeta, mockHelper.pipeline );
  }

  /**
   * PDI-10250 - "Group by" transform - Minimum aggregation doesn't work
   * <p>
   * HOP_AGGREGATION_MIN_NULL_IS_VALUED
   * <p>
   * Set this variable to Y to set the minimum to NULL if NULL is within an aggregate. Otherwise by default NULL is
   * ignored by the MIN aggregate and MIN is set to the minimum value that is not NULL. See also the variable
   * HOP_AGGREGATION_ALL_NULLS_ARE_ZERO.
   *
   * @throws HopValueException
   */
  @Test
  public void calcAggregateResultTestMin_1_Test() throws HopValueException {
    transform.setMinNullIsValued( true );
    transform.calcAggregate( new Object[] { null } );
    Assert.assertNull( "Value is set", data.agg[ 0 ] );
  }

  @Test
  public void calcAggregateResultTestMin_2_Test() throws HopValueException {
    transform.setMinNullIsValued( true );
    transform.calcAggregate( new Object[] { null } );
    Assert.assertNull( "Value is set", data.agg[ 0 ] );
  }

  @Test
  public void calcAggregateResultTestMin_5_Test() throws HopValueException {
    transform.calcAggregate( new Object[] { null } );
    Assert.assertEquals( "Value is NOT set", def, data.agg[ 0 ] );
  }

  @Test
  public void calcAggregateResultTestMin_3_Test() throws HopValueException {
    transform.setMinNullIsValued( false );
    transform.calcAggregate( new Object[] { null } );
    Assert.assertEquals( "Value is NOT set", def, data.agg[ 0 ] );
  }

  //PDI-15648 - Minimum aggregation doesn't work when null value in first row
  @Test
  public void getMinAggregateResultFirstValIsNullTest() throws HopValueException {
    data.agg[ 0 ] = null;
    transform.setMinNullIsValued( false );
    transform.calcAggregate( new Object[] { null } );
    transform.calcAggregate( new Object[] { 2 } );
    Assert.assertEquals( "Min aggregation doesn't properly work if the first value is null", 2, data.agg[ 0 ] );
  }

  /**
   * Set this variable to Y to return 0 when all values within an aggregate are NULL. Otherwise by default a NULL is
   * returned when all values are NULL.
   *
   * @throws HopValueException
   */
  @Test
  public void getAggregateResultTestMin_0_Test() throws HopValueException {
    // data.agg[0] is not null - this is the default behaviour
    transform.setAllNullsAreZero( true );
    Object[] row = transform.getAggregateResult();
    Assert.assertEquals( "Default value is not corrupted", def, row[ 0 ] );
  }

  @Test
  public void getAggregateResultTestMin_1_Test() throws HopValueException {
    data.agg[ 0 ] = null;
    transform.setAllNullsAreZero( true );
    Object[] row = transform.getAggregateResult();
    Assert.assertEquals( "Returns 0 if aggregation is null", new Long( 0 ), row[ 0 ] );
  }

  @Test
  public void getAggregateResultTestMin_3_Test() throws HopValueException {
    data.agg[ 0 ] = null;
    transform.setAllNullsAreZero( false );
    Object[] row = transform.getAggregateResult();
    Assert.assertNull( "Returns null if aggregation is null", row[ 0 ] );
  }

}
