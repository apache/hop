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

package org.apache.hop.pipeline.transforms.fieldsplitter;

import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.QueueRowSet;
import org.apache.hop.core.IRowSet;
import org.apache.hop.core.SingleRowRowSet;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;

/**
 * Tests for FieldSplitter transform
 *
 * @author Pavel Sakun
 * @see FieldSplitter
 */
public class FieldSplitterTest {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();
  TransformMockHelper<FieldSplitterMeta, FieldSplitterData> transformMockHelper;

  @BeforeClass
  public static void initHop() throws Exception {
    HopEnvironment.init();
  }

  @Before
  public void setUp() {
    transformMockHelper = new TransformMockHelper<>( "Field Splitter", FieldSplitterMeta.class, FieldSplitterData.class );
    when( transformMockHelper.logChannelFactory.create( any(), any( ILoggingObject.class ) ) ).thenReturn(
      transformMockHelper.iLogChannel );
    when( transformMockHelper.pipeline.isRunning() ).thenReturn( true );
  }

  @After
  public void cleanUp() {
    transformMockHelper.cleanUp();
  }

  private IRowSet mockInputRowSet() {
    return transformMockHelper.getMockInputRowSet( new Object[][] { { "before", "b=b;c=c", "after" } } );
  }

  private FieldSplitterMeta mockProcessRowMeta() throws HopTransformException {
    FieldSplitterMeta meta = transformMockHelper.iTransformMeta;
    doReturn( "field to split" ).when( meta ).getSplitField();
    doCallRealMethod().when( meta ).getFields( any( IRowMeta.class ), anyString(),
      any( IRowMeta[].class ), any( TransformMeta.class ), any( IVariables.class ),
      any( IHopMetadataProvider.class ) );
    doReturn( new String[] { "a", "b" } ).when( meta ).getFieldName();
    doReturn( new int[] { IValueMeta.TYPE_STRING, IValueMeta.TYPE_STRING } ).when( meta )
      .getFieldType();
    doReturn( new String[] { "a=", "b=" } ).when( meta ).getFieldID();
    doReturn( new boolean[] { false, false } ).when( meta ).getFieldRemoveID();
    doReturn( new int[] { -1, -1 } ).when( meta ).getFieldLength();
    doReturn( new int[] { -1, -1 } ).when( meta ).getFieldPrecision();
    doReturn( new int[] { 0, 0 } ).when( meta ).getFieldTrimType();
    doReturn( new String[] { null, null } ).when( meta ).getFieldFormat();
    doReturn( new String[] { null, null } ).when( meta ).getFieldDecimal();
    doReturn( new String[] { null, null } ).when( meta ).getFieldGroup();
    doReturn( new String[] { null, null } ).when( meta ).getFieldCurrency();
    doReturn( new String[] { null, null } ).when( meta ).getFieldNullIf();
    doReturn( new String[] { null, null } ).when( meta ).getFieldIfNull();
    doReturn( ";" ).when( meta ).getDelimiter();
    doReturn( 2 ).when( meta ).getFieldsCount();

    return meta;
  }

  private RowMeta getInputRowMeta() {
    RowMeta inputRowMeta = new RowMeta();
    inputRowMeta.addValueMeta( new ValueMetaString( "before" ) );
    inputRowMeta.addValueMeta( new ValueMetaString( "field to split" ) );
    inputRowMeta.addValueMeta( new ValueMetaString( "after" ) );

    return inputRowMeta;
  }

  @Test
  public void testSplitFields() throws HopException {

    FieldSplitter transform = new FieldSplitter( transformMockHelper.transformMeta, mockProcessRowMeta(), transformMockHelper.iTransformData, 0, transformMockHelper.pipelineMeta, transformMockHelper.pipeline );
    transform.init();
    transform.setInputRowMeta( getInputRowMeta() );
    transform.addRowSetToInputRowSets( mockInputRowSet() );
    transform.addRowSetToOutputRowSets( new QueueRowSet() );

    boolean hasMoreRows;
    do {
      hasMoreRows = transform.processRow();
    } while ( hasMoreRows );

    IRowSet outputRowSet = transform.getOutputRowSets().get( 0 );
    Object[] actualRow = outputRowSet.getRow();
    Object[] expectedRow = new Object[] { "before", null, "b=b", "after" };

    assertEquals( "Output row is of an unexpected length", expectedRow.length, outputRowSet.getRowMeta().size() );

    for ( int i = 0; i < expectedRow.length; i++ ) {
      assertEquals( "Unexpected output value at index " + i, expectedRow[ i ], actualRow[ i ] );
    }
  }

  @Test
  public void testSplitFieldsDup() throws Exception {
    FieldSplitterMeta meta = new FieldSplitterMeta();
    meta.allocate( 2 );
    meta.setDelimiter( " " );
    meta.setEnclosure( "" );
    meta.setSplitField( "split" );
    meta.setFieldName( new String[] { "key", "val" } );
    meta.setFieldType( new int[] { IValueMeta.TYPE_STRING, IValueMeta.TYPE_STRING } );

    FieldSplitter transform = new FieldSplitter( transformMockHelper.transformMeta, meta, transformMockHelper.iTransformData, 0, transformMockHelper.pipelineMeta, transformMockHelper.pipeline );
    transform.init();

    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta( new ValueMetaString( "key" ) );
    rowMeta.addValueMeta( new ValueMetaString( "val" ) );
    rowMeta.addValueMeta( new ValueMetaString( "split" ) );

    transform.setInputRowMeta( rowMeta );
    transform.addRowSetToInputRowSets( transformMockHelper.getMockInputRowSet( new Object[] { "key", "string", "part1 part2" } ) );
    transform.addRowSetToOutputRowSets( new SingleRowRowSet() );

    assertTrue( transform.processRow() );

    IRowSet rs = transform.getOutputRowSets().get( 0 );
    Object[] row = rs.getRow();
    IRowMeta rm = rs.getRowMeta();

    assertArrayEquals(
      new Object[] { "key", "string", "part1", "part2" },
      Arrays.copyOf( row, 4 ) );

    assertArrayEquals(
      new Object[] { "key", "val", "key_1", "val_1" },
      rm.getFieldNames() );
  }
}
