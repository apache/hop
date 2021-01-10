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

import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.apache.hop.pipeline.transforms.memgroupby.MemoryGroupByMeta.TYPE_GROUP_COUNT_ANY;
import static org.apache.hop.pipeline.transforms.memgroupby.MemoryGroupByMeta.TYPE_GROUP_MAX;
import static org.apache.hop.pipeline.transforms.memgroupby.MemoryGroupByMeta.TYPE_GROUP_MIN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mockStatic;

/**
 * @author Luis Martins
 */
@RunWith( PowerMockRunner.class )
@PrepareForTest( { ValueMetaFactory.class } )
public class MemoryGroupByMetaGetFieldsTest {

  private MemoryGroupByMeta memoryGroupByMeta;
  private IRowMeta rowMeta;

  private IRowMeta[] mockInfo;
  private TransformMeta mockNextTransform;
  private IVariables mockSpace;
  private IHopMetadataProvider mockIHopMetadataProvider;


  @Before
  public void setup() throws HopPluginException {
    mockSpace = mock( IVariables.class );
    doReturn( "N" ).when( mockSpace ).getVariable( any(), anyString() );

    rowMeta = spy( new RowMeta() );
    memoryGroupByMeta = spy( new MemoryGroupByMeta() );

    mockStatic( ValueMetaFactory.class );
    when( ValueMetaFactory.createValueMeta( anyInt() ) ).thenCallRealMethod();
    when( ValueMetaFactory.createValueMeta( anyString(), anyInt() ) ).thenCallRealMethod();
    when( ValueMetaFactory.createValueMeta( "maxDate", 3, -1, -1 ) ).thenReturn( new ValueMetaDate( "maxDate" ) );
    when( ValueMetaFactory.createValueMeta( "minDate", 3, -1, -1 ) ).thenReturn( new ValueMetaDate( "minDate" ) );
    when( ValueMetaFactory.createValueMeta( "countDate", 5, -1, -1 ) ).thenReturn( new ValueMetaInteger( "countDate" ) );
    when( ValueMetaFactory.getValueMetaName( 3 ) ).thenReturn( "Date" );
    when( ValueMetaFactory.getValueMetaName( 5 ) ).thenReturn( "Integer" );
  }

  @After
  public void cleanup() {
  }

  @Test
  public void getFieldsWithSubject_WithFormat() {
    ValueMetaDate valueMeta = new ValueMetaDate();
    valueMeta.setConversionMask( "yyyy-MM-dd" );
    valueMeta.setName( "date" );

    doReturn( valueMeta ).when( rowMeta ).searchValueMeta( "date" );

    memoryGroupByMeta.setSubjectField( new String[] { "date" } );
    memoryGroupByMeta.setGroupField( new String[] {} );
    memoryGroupByMeta.setAggregateField( new String[] { "maxDate" } );
    memoryGroupByMeta.setAggregateType( new int[] { TYPE_GROUP_MAX } );

    memoryGroupByMeta.getFields( rowMeta, "Memory Group by", mockInfo, mockNextTransform, mockSpace, mockIHopMetadataProvider );

    verify( rowMeta, times( 1 ) ).clear();
    verify( rowMeta, times( 1 ) ).addRowMeta( any() );
    assertEquals( "yyyy-MM-dd", rowMeta.searchValueMeta( "maxDate" ).getConversionMask() );
  }

  @Test
  public void getFieldsWithSubject_NoFormat() {
    ValueMetaDate valueMeta = new ValueMetaDate();
    valueMeta.setName( "date" );

    doReturn( valueMeta ).when( rowMeta ).searchValueMeta( "date" );

    memoryGroupByMeta.setSubjectField( new String[] { "date" } );
    memoryGroupByMeta.setGroupField( new String[] {} );
    memoryGroupByMeta.setAggregateField( new String[] { "minDate" } );
    memoryGroupByMeta.setAggregateType( new int[] { TYPE_GROUP_MIN } );

    memoryGroupByMeta.getFields( rowMeta, "Group by", mockInfo, mockNextTransform, mockSpace, mockIHopMetadataProvider );

    verify( rowMeta, times( 1 ) ).clear();
    verify( rowMeta, times( 1 ) ).addRowMeta( any() );
    assertEquals( null, rowMeta.searchValueMeta( "minDate" ).getConversionMask() );
  }

  @Test
  public void getFieldsWithoutSubject() {
    ValueMetaDate valueMeta = new ValueMetaDate();
    valueMeta.setName( "date" );

    doReturn( valueMeta ).when( rowMeta ).searchValueMeta( "date" );

    memoryGroupByMeta.setSubjectField( new String[] { null } );
    memoryGroupByMeta.setGroupField( new String[] { "date" } );
    memoryGroupByMeta.setAggregateField( new String[] { "countDate" } );
    memoryGroupByMeta.setAggregateType( new int[] { TYPE_GROUP_COUNT_ANY } );

    memoryGroupByMeta.getFields( rowMeta, "Group by", mockInfo, mockNextTransform, mockSpace, mockIHopMetadataProvider );

    verify( rowMeta, times( 1 ) ).clear();
    verify( rowMeta, times( 1 ) ).addRowMeta( any() );
    assertNotNull( rowMeta.searchValueMeta( "countDate" ) );
  }
}
