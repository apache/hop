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

package org.apache.hop.pipeline.transforms.stringoperations;

import junit.framework.Assert;
import org.apache.hop.core.QueueRowSet;
import org.apache.hop.core.IRowSet;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for StringOperations transform
 *
 * @author Pavel Sakun
 * @see StringOperations
 */
public class StringOperationsTest {
  private static TransformMockHelper<StringOperationsMeta, StringOperationsData> smh;

  @Before
  public void setup() throws Exception {
    smh =
      new TransformMockHelper<>( "StringOperations", StringOperationsMeta.class,
        StringOperationsData.class );
    when( smh.logChannelFactory.create( any(), any( ILoggingObject.class ) ) ).thenReturn(
      smh.iLogChannel );
    when( smh.pipeline.isRunning() ).thenReturn( true );
  }

  @After
  public void cleanUp() {
    smh.cleanUp();
  }

  private IRowSet mockInputRowSet() {
    ValueMetaString valueMeta = new ValueMetaString( "Value" );
    valueMeta.setStorageType( IValueMeta.STORAGE_TYPE_BINARY_STRING );
    valueMeta.setStorageMetadata( new ValueMetaString( "Value" ) );

    RowMeta inputRowMeta = new RowMeta();
    inputRowMeta.addValueMeta( valueMeta );

    IRowSet inputRowSet = smh.getMockInputRowSet( new Object[][] { { " Value ".getBytes() } } );
    doReturn( inputRowMeta ).when( inputRowSet ).getRowMeta();

    return inputRowSet;
  }

  private StringOperationsMeta mockTransformMeta() {
    StringOperationsMeta meta = mock( StringOperationsMeta.class );
    doReturn( new String[] { "Value" } ).when( meta ).getFieldInStream();
    doReturn( new String[] { "" } ).when( meta ).getFieldOutStream();
    doReturn( new int[] { StringOperationsMeta.TRIM_BOTH } ).when( meta ).getTrimType();
    doReturn( new int[] { StringOperationsMeta.LOWER_UPPER_NONE } ).when( meta ).getLowerUpper();
    doReturn( new int[] { StringOperationsMeta.PADDING_NONE } ).when( meta ).getPaddingType();
    doReturn( new String[] { "" } ).when( meta ).getPadChar();
    doReturn( new String[] { "" } ).when( meta ).getPadLen();
    doReturn( new int[] { StringOperationsMeta.INIT_CAP_NO } ).when( meta ).getInitCap();
    doReturn( new int[] { StringOperationsMeta.MASK_NONE } ).when( meta ).getMaskXML();
    doReturn( new int[] { StringOperationsMeta.DIGITS_NONE } ).when( meta ).getDigits();
    doReturn( new int[] { StringOperationsMeta.REMOVE_SPECIAL_CHARACTERS_NONE } ).when( meta ).getRemoveSpecialCharacters();

    return meta;
  }

  private StringOperationsData mockTransformData() {
    return mock( StringOperationsData.class );
  }

  private boolean verifyOutput( Object[][] expectedRows, IRowSet outputRowSet ) throws HopValueException {
    if ( expectedRows.length == outputRowSet.size() ) {
      for ( Object[] expectedRow : expectedRows ) {
        Object[] row = outputRowSet.getRow();
        if ( expectedRow.length == outputRowSet.getRowMeta().size() ) {
          for ( int j = 0; j < expectedRow.length; j++ ) {
            if ( !expectedRow[ j ].equals( outputRowSet.getRowMeta().getString( row, j ) ) ) {
              return false;
            }
          }
          return true;
        }
      }
    }
    return false;
  }

  @Test
  @Ignore
  public void testProcessBinaryInput() throws HopException {
    StringOperations transform = new StringOperations( smh.transformMeta, smh.iTransformMeta, smh.iTransformData, 0, smh.pipelineMeta, smh.pipeline );
    transform.addRowSetToInputRowSets( mockInputRowSet() );

    IRowSet outputRowSet = new QueueRowSet();
    transform.addRowSetToOutputRowSets( outputRowSet );

    StringOperationsMeta meta = mockTransformMeta();
    StringOperationsData data = mockTransformData();

    transform.init();

    boolean processResult;

    do {
      processResult = transform.init();
    } while ( processResult );

    Assert.assertTrue( outputRowSet.isDone() );

    Assert.assertTrue( "Unexpected output", verifyOutput( new Object[][] { { "Value" } }, outputRowSet ) );
  }
}
