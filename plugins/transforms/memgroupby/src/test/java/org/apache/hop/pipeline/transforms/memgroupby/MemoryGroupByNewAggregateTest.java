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

import junit.framework.Assert;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.*;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

public class MemoryGroupByNewAggregateTest {

  static TransformMockHelper<MemoryGroupByMeta, MemoryGroupByData> mockHelper;
  static List<Integer> strings;
  static List<Integer> statistics;

  MemoryGroupBy transform;
  MemoryGroupByData data;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    mockHelper =
      new TransformMockHelper<>( "Memory Group By", MemoryGroupByMeta.class,
        MemoryGroupByData.class );
    when( mockHelper.logChannelFactory.create( any(), any( ILoggingObject.class ) ) ).thenReturn(
      mockHelper.iLogChannel );
    when( mockHelper.pipeline.isRunning() ).thenReturn( true );

    // In this transform we will distinct String aggregations from numeric ones
    strings = new ArrayList<>();
    strings.add( MemoryGroupByMeta.TYPE_GROUP_CONCAT_COMMA );
    strings.add( MemoryGroupByMeta.TYPE_GROUP_CONCAT_STRING );

    // Statistics will be initialized with collections...
    statistics = new ArrayList<>();
    statistics.add( MemoryGroupByMeta.TYPE_GROUP_MEDIAN );
    statistics.add( MemoryGroupByMeta.TYPE_GROUP_PERCENTILE );
  }

  @AfterClass
  public static void cleanUp() {
    mockHelper.cleanUp();
  }

  @Before
  public void setUp() throws Exception {
    data = new MemoryGroupByData();

    data.subjectnrs = new int[ 16 ];
    int[] arr = new int[ 16 ];
    String[] arrF = new String[ 16 ];

    for ( int i = 0; i < arr.length; i++ ) {
      // set aggregation types (hardcoded integer values from 1 to 18)
      arr[ i ] = i + 1;
      data.subjectnrs[ i ] = i;
    }
    Arrays.fill( arrF, "x" );

    MemoryGroupByMeta meta = new MemoryGroupByMeta();
    meta.setAggregateType( arr );
    meta.setAggregateField( arrF );

    IValueMeta vmi = new ValueMetaInteger();
    when( mockHelper.transformMeta.getTransform() ).thenReturn( meta );
    IRowMeta rmi = Mockito.mock( IRowMeta.class );
    data.inputRowMeta = rmi;
    when( rmi.getValueMeta( Mockito.anyInt() ) ).thenReturn( vmi );
    data.aggMeta = rmi;

    transform = new MemoryGroupBy( mockHelper.transformMeta, mockHelper.iTransformMeta, data, 0, mockHelper.pipelineMeta, mockHelper.pipeline );
  }

  @Test
  @Ignore
  public void testNewAggregate() throws HopException {
    Object[] r = new Object[ 16 ];
    Arrays.fill( r, null );

    Aggregate agg = new Aggregate();

    transform.newAggregate( r, agg );

    Assert.assertEquals( "All possible aggregation cases considered", 16, agg.agg.length );

    // all aggregations types is int values, filled in ascending order in perconditions
    for ( int i = 0; i < agg.agg.length; i++ ) {
      int type = i + 1;
      if ( strings.contains( type ) ) {
        Assert.assertTrue( "This is appendable type, type=" + type, agg.agg[ i ] instanceof Appendable );
      } else if ( statistics.contains( type ) ) {
        Assert.assertTrue( "This is collection, type=" + type, agg.agg[ i ] instanceof Collection );
      } else {
        Assert.assertNull( "Aggregation initialized with null, type=" + type, agg.agg[ i ] );
      }
    }
  }
}
