/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
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
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Arrays;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

public class GroupByNullInputTest {

  public static final int NUMBER_OF_COLUMNS = 1;
  public static final String ANY_FIELD_NAME = "anyFieldName";
  static TransformMockHelper<GroupByMeta, GroupByData> mockHelper;

  private GroupBy transform;
  private GroupByData groupByData;

  private GroupByMeta groupByMeta;
  private IRowMeta rowMetaInterfaceMock;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    mockHelper = new TransformMockHelper<>( "Group By", GroupByMeta.class, GroupByData.class );
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
    groupByData = new GroupByData();
    groupByMeta = new GroupByMeta();

    when( mockHelper.transformMeta.getITransform() ).thenReturn( groupByMeta );
    rowMetaInterfaceMock = Mockito.mock( IRowMeta.class );
    groupByData.inputRowMeta = rowMetaInterfaceMock;
    groupByData.aggMeta = rowMetaInterfaceMock;

    transform = new GroupBy( mockHelper.transformMeta, groupByData, 0, mockHelper.pipelineMeta, mockHelper.pipeline );
  }

  private void setAggregationTypesAndInitData( int[] aggregationTypes ) {
    //types of aggregation functions
    int countOfAggregations = aggregationTypes.length;
    groupByData.subjectnrs = new int[ countOfAggregations ];
    System.arraycopy( aggregationTypes, 0, groupByData.subjectnrs, 0, countOfAggregations );
    groupByMeta.setAggregateType( aggregationTypes );

    //init field names for aggregation columns
    String[] fieldNames = new String[ countOfAggregations ];
    Arrays.fill( fieldNames, ANY_FIELD_NAME );
    groupByMeta.setAggregateField( fieldNames );

    groupByData.subjectnrs = new int[ countOfAggregations ];
    //init sum arrays which are set on processRow which is not always called from tests
    groupByData.previousSums = new Object[ countOfAggregations ];
    groupByData.counts = new long[ countOfAggregations ];
    groupByData.previousAvgCount = new long[ countOfAggregations ];
    groupByData.previousAvgSum = new Object[ countOfAggregations ];
    Arrays.fill( groupByData.previousSums, 0 );
    Arrays.fill( groupByData.previousAvgCount, 0 );
    Arrays.fill( groupByData.previousAvgSum, 0 );
  }

  /**
   * PMD-1037 NPE error appears when user uses "Data Profile" feature to some tables in Hive 2.
   */
  @Test
  public void testNullInputDataForStandardDeviation() throws HopValueException {
    setAggregationTypesAndInitData( new int[] { 15 } );
    IValueMeta vmi = new ValueMetaInteger();
    when( rowMetaInterfaceMock.getValueMeta( Mockito.anyInt() ) ).thenReturn( vmi );
    Object[] row1 = new Object[ NUMBER_OF_COLUMNS ];
    Arrays.fill( row1, null );
    transform.newAggregate( row1 );
    transform.calcAggregate( row1 );
    Object[] aggregateResult = transform.getAggregateResult();
    Assert.assertNull( "Returns null if aggregation is null", aggregateResult[ 0 ] );
  }

  @Test
  public void testNullInputDataForAggregationWithNumbers() throws HopValueException {
    setAggregationTypesAndInitData( new int[] { 1, 2, 5, 6, 7, 9, 10, 11, 12, 13, 14, 15 } );
    IValueMeta vmi = new ValueMetaInteger();
    when( rowMetaInterfaceMock.getValueMeta( Mockito.anyInt() ) ).thenReturn( vmi );
    Object[] row1 = new Object[ NUMBER_OF_COLUMNS ];
    Arrays.fill( row1, null );
    transform.newAggregate( row1 );
    transform.calcAggregate( row1 );
    Object[] aggregateResult = transform.getAggregateResult();
    Assert.assertNull( "Returns null if aggregation is null", aggregateResult[ 0 ] );
  }

  @Test
  public void testNullInputDataForAggregationWithNumbersMedianFunction() throws HopValueException {
    setAggregationTypesAndInitData( new int[] { 3, 4 } );
    IValueMeta vmi = new ValueMetaInteger();
    when( rowMetaInterfaceMock.getValueMeta( Mockito.anyInt() ) ).thenReturn( vmi );
    //PERCENTILE set
    groupByMeta.setValueField( new String[] { "3", "3" } );
    Object[] row1 = new Object[ NUMBER_OF_COLUMNS ];
    Arrays.fill( row1, null );
    transform.newAggregate( row1 );
    transform.calcAggregate( row1 );
    transform.getAggregateResult();
  }

  @Test
  public void testNullInputDataForAggregationWithStrings() throws HopValueException {
    setAggregationTypesAndInitData( new int[] { 8, 16, 17, 18 } );
    groupByMeta.setValueField( new String[] { "," } );
    groupByMeta.setSubjectField( new String[] { ANY_FIELD_NAME, ANY_FIELD_NAME } );
    IValueMeta vmi = new ValueMetaString();
    when( rowMetaInterfaceMock.getValueMeta( Mockito.anyInt() ) ).thenReturn( vmi );
    Object[] row1 = new Object[ NUMBER_OF_COLUMNS ];
    Arrays.fill( row1, null );
    transform.newAggregate( row1 );
    transform.calcAggregate( row1 );
    Object[] row2 = new Object[ NUMBER_OF_COLUMNS ];
    Arrays.fill( row2, null );
    transform.calcAggregate( row2 );
    Object[] row3 = new Object[ NUMBER_OF_COLUMNS ];
    Arrays.fill( row3, null );
    transform.calcAggregate( row3 );
    transform.getAggregateResult();
  }

}
