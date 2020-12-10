/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.pipeline.transforms.memgroupby;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.pipeline.transforms.memgroupby.MemoryGroupByData.HashEntry;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.*;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.HashMap;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ValueMetaFactory.class})
public class MemoryGroupByAggregationNullsTest {

  static TransformMockHelper<MemoryGroupByMeta, MemoryGroupByData> mockHelper;

  MemoryGroupBy transform;
  MemoryGroupByData data;

  static int def = 113;

  Aggregate aggregate;
  private IValueMeta vmi;
  private IRowMeta rmi;
  private MemoryGroupByMeta meta;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    mockHelper =
      new TransformMockHelper<>( "Memory Group By", MemoryGroupByMeta.class,
        MemoryGroupByData.class );
    when( mockHelper.logChannelFactory.create( any(), any( ILoggingObject.class ) ) ).thenReturn(
      mockHelper.iLogChannel );
    when( mockHelper.pipeline.isRunning() ).thenReturn( true );
  }

  @AfterClass
  public static void cleanUp() {
    mockHelper.cleanUp();
  }

  @Before
  public void setUp() throws Exception {
    data = new MemoryGroupByData();
    data.subjectnrs = new int[] { 0 };
    meta = new MemoryGroupByMeta();
    meta.setDefault();
    meta.setAggregateType( new int[] { 5 } );
    meta.setAggregateField( new String[] { "x" } );
    vmi = new ValueMetaInteger();
    when( mockHelper.transformMeta.getTransform() ).thenReturn( meta );
    rmi = Mockito.mock( IRowMeta.class );
    data.inputRowMeta = rmi;
    data.outputRowMeta = rmi;
    data.groupMeta = rmi;
    data.groupnrs = new int[] {};
    data.map = new HashMap<>();
    when( rmi.getValueMeta( Mockito.anyInt() ) ).thenReturn( vmi );
    data.aggMeta = rmi;
    transform = new MemoryGroupBy( mockHelper.transformMeta, meta, data, 0, mockHelper.pipelineMeta, mockHelper.pipeline );

    // put aggregate into map with default predefined value
    aggregate = new Aggregate();
    aggregate.agg = new Object[] { def };
    data.map.put( getHashEntry(), aggregate );
  }

  // test hash entry
  HashEntry getHashEntry() {
    return data.getHashEntry( new Object[ data.groupMeta.size() ] );
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
   * @throws HopException
   */
  @Test
  @Ignore
  public void calcAggregateResulTestMin_1_Test() throws HopException {
    transform.setMinNullIsValued( true );
    transform.addToAggregate( new Object[] { null } );

    Aggregate agg = data.map.get( getHashEntry() );
    Assert.assertNotNull( "Hash code strategy changed?", agg );

    Assert.assertNull( "Value is set", agg.agg[ 0 ] );
  }

  @Test
  @Ignore
  public void calcAggregateResulTestMin_5_Test() throws HopException {
    transform.setMinNullIsValued( false );
    transform.addToAggregate( new Object[] { null } );

    Aggregate agg = data.map.get( getHashEntry() );
    Assert.assertNotNull( "Hash code strategy changed?", agg );

    Assert.assertEquals( "Value is NOT set", def, agg.agg[ 0 ] );
  }

  /**
   * Set this variable to Y to return 0 when all values within an aggregate are NULL. Otherwise by default a NULL is
   * returned when all values are NULL.
   *
   * @throws HopValueException
   */

  @Test
  @Ignore
  public void getAggregateResulTestMin_0_Test() throws HopValueException {
    // data.agg[0] is not null - this is the default behavior
    transform.setAllNullsAreZero( true );
    Object[] row = transform.getAggregateResult( aggregate );
    Assert.assertEquals( "Default value is not corrupted", def, row[ 0 ] );
  }

  @Test
  @Ignore
  public void getAggregateResulTestMin_1_Test() throws HopValueException {
    aggregate.agg[ 0 ] = null;
    transform.setAllNullsAreZero( true );
    Object[] row = transform.getAggregateResult( aggregate );
    Assert.assertEquals( "Returns 0 if aggregation is null", new Long( 0 ), row[ 0 ] );
  }

  @Test
  @Ignore
  public void getAggregateResulTestMin_3_Test() throws HopValueException {
    aggregate.agg[ 0 ] = null;
    transform.setAllNullsAreZero( false );
    Object[] row = transform.getAggregateResult( aggregate );
    Assert.assertNull( "Returns null if aggregation is null", row[ 0 ] );
  }

  @Test
  @Ignore
  public void addToAggregateLazyConversionMinTest() throws Exception {
    vmi.setStorageType( IValueMeta.STORAGE_TYPE_BINARY_STRING );
    vmi.setStorageMetadata( new ValueMetaString() );
    aggregate.agg = new Object[] { new byte[ 0 ] };
    byte[] bytes = { 51 };
    transform.addToAggregate( new Object[] { bytes } );
    Aggregate result = data.map.get( getHashEntry() );
    Assert.assertEquals( "Returns non-null value", bytes, result.agg[ 0 ] );
  }

  // PDI-16150
  @Test
  @Ignore
  public void addToAggregateBinaryData() throws Exception {
    MemoryGroupByMeta memoryGroupByMeta = spy( meta );
    memoryGroupByMeta.setAggregateType( new int[] { MemoryGroupByMeta.TYPE_GROUP_COUNT_DISTINCT } );
    when( mockHelper.transformMeta.getTransform() ).thenReturn( memoryGroupByMeta );
    vmi.setStorageType( IValueMeta.STORAGE_TYPE_NORMAL );
    vmi.setStorageMetadata( new ValueMetaString() );
    aggregate.counts = new long[] { 0L };
    Mockito.doReturn( new String[] { "test" } ).when( memoryGroupByMeta ).getSubjectField();
    aggregate.agg = new Object[] { new byte[ 0 ] };
    transform = new MemoryGroupBy( mockHelper.transformMeta, memoryGroupByMeta, data, 0, mockHelper.pipelineMeta, mockHelper.pipeline );

    String binaryData0 = "11011";
    String binaryData1 = "01011";
    transform.addToAggregate( new Object[] { binaryData0.getBytes() } );
    transform.addToAggregate( new Object[] { binaryData1.getBytes() } );

    Object[] distinctObjs = data.map.get( getHashEntry() ).distinctObjs[ 0 ].toArray();

    Assert.assertEquals( binaryData0, distinctObjs[ 1 ] );
    Assert.assertEquals( binaryData1, distinctObjs[ 0 ] );
  }

}
