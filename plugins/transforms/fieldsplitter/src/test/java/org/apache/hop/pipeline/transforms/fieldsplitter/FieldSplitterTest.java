/*! ******************************************************************************
 *
 * Pentaho Data Integration
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
import org.apache.hop.metastore.api.IMetaStore;
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
  TransformMockHelper<FieldSplitterMeta, FieldSplitterData> smh;

  @BeforeClass
  public static void initHop() throws Exception {
    HopEnvironment.init();
  }

  @Before
  public void setUp() {
    smh =
      new TransformMockHelper<FieldSplitterMeta, FieldSplitterData>( "Field Splitter", FieldSplitterMeta.class,
        FieldSplitterData.class );
    when( smh.logChannelFactory.create( any(), any( ILoggingObject.class ) ) ).thenReturn(
      smh.logChannelInterface );
    when( smh.pipeline.isRunning() ).thenReturn( true );
  }

  @After
  public void cleanUp() {
    smh.cleanUp();
  }

  private IRowSet mockInputRowSet() {
    return smh.getMockInputRowSet( new Object[][] { { "before", "b=b;c=c", "after" } } );
  }

  private FieldSplitterMeta mockProcessRowMeta() throws HopTransformException {
    FieldSplitterMeta processRowMeta = smh.processRowsTransformMetaInterface;
    doReturn( "field to split" ).when( processRowMeta ).getSplitField();
    doCallRealMethod().when( processRowMeta ).getFields( any( IRowMeta.class ), anyString(),
      any( IRowMeta[].class ), any( TransformMeta.class ), any( IVariables.class ),
      any( IMetaStore.class ) );
    doReturn( new String[] { "a", "b" } ).when( processRowMeta ).getFieldName();
    doReturn( new int[] { IValueMeta.TYPE_STRING, IValueMeta.TYPE_STRING } ).when( processRowMeta )
      .getFieldType();
    doReturn( new String[] { "a=", "b=" } ).when( processRowMeta ).getFieldID();
    doReturn( new boolean[] { false, false } ).when( processRowMeta ).getFieldRemoveID();
    doReturn( new int[] { -1, -1 } ).when( processRowMeta ).getFieldLength();
    doReturn( new int[] { -1, -1 } ).when( processRowMeta ).getFieldPrecision();
    doReturn( new int[] { 0, 0 } ).when( processRowMeta ).getFieldTrimType();
    doReturn( new String[] { null, null } ).when( processRowMeta ).getFieldFormat();
    doReturn( new String[] { null, null } ).when( processRowMeta ).getFieldDecimal();
    doReturn( new String[] { null, null } ).when( processRowMeta ).getFieldGroup();
    doReturn( new String[] { null, null } ).when( processRowMeta ).getFieldCurrency();
    doReturn( new String[] { null, null } ).when( processRowMeta ).getFieldNullIf();
    doReturn( new String[] { null, null } ).when( processRowMeta ).getFieldIfNull();
    doReturn( ";" ).when( processRowMeta ).getDelimiter();
    doReturn( 2 ).when( processRowMeta ).getFieldsCount();

    return processRowMeta;
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
    FieldSplitter transform = new FieldSplitter( smh.transformMeta, smh.iTransformData, 0, smh.pipelineMeta, smh.pipeline );
    transform.init( smh.initTransformMetaInterface, smh.iTransformData );
    transform.setInputRowMeta( getInputRowMeta() );
    transform.addRowSetToInputRowSets( mockInputRowSet() );
    transform.addRowSetToOutputRowSets( new QueueRowSet() );

    boolean hasMoreRows;
    do {
      hasMoreRows = transform.processRow( mockProcessRowMeta(), smh.processRowsTransformDataInterface );
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

    FieldSplitter transform = new FieldSplitter( smh.transformMeta, smh.iTransformData, 0, smh.pipelineMeta, smh.pipeline );
    transform.init( meta, smh.iTransformData );

    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta( new ValueMetaString( "key" ) );
    rowMeta.addValueMeta( new ValueMetaString( "val" ) );
    rowMeta.addValueMeta( new ValueMetaString( "split" ) );

    transform.setInputRowMeta( rowMeta );
    transform.addRowSetToInputRowSets( smh.getMockInputRowSet( new Object[] { "key", "string", "part1 part2" } ) );
    transform.addRowSetToOutputRowSets( new SingleRowRowSet() );

    assertTrue( transform.processRow( meta, smh.iTransformData ) );

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
