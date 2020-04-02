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

package org.apache.hop.pipeline.transforms.update;

import junit.framework.Assert;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.LoggingObjectInterface;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.ValueMetaInterface;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.PreparedStatement;
import java.sql.Timestamp;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Regression test for PDI-11152
 *
 * @author Pavel Sakun
 */
public class PDI_11152_Test {
  TransformMockHelper<UpdateMeta, UpdateData> smh;

  @Before
  public void setUp() {
    smh = new TransformMockHelper<UpdateMeta, UpdateData>( "Update", UpdateMeta.class, UpdateData.class );
    when( smh.logChannelInterfaceFactory.create( any(), any( LoggingObjectInterface.class ) ) ).thenReturn(
      smh.logChannelInterface );
    when( smh.pipeline.isRunning() ).thenReturn( true );
  }

  @After
  public void cleanUp() {
    smh.cleanUp();
  }

  @Test
  public void testInputLazyConversion() throws HopException {
    Database db = mock( Database.class );
    RowMeta returnRowMeta = new RowMeta();
    doReturn( new Object[] { new Timestamp( System.currentTimeMillis() ) } ).when( db ).getLookup(
      any( PreparedStatement.class ) );
    returnRowMeta.addValueMeta( new ValueMetaDate( "TimeStamp" ) );
    doReturn( returnRowMeta ).when( db ).getReturnRowMeta();

    ValueMetaString storageMetadata = new ValueMetaString( "Date" );
    storageMetadata.setConversionMask( "yyyy-MM-dd" );

    ValueMetaDate valueMeta = new ValueMetaDate( "Date" );
    valueMeta.setStorageType( ValueMetaInterface.STORAGE_TYPE_BINARY_STRING );
    valueMeta.setStorageMetadata( storageMetadata );

    RowMeta inputRowMeta = new RowMeta();
    inputRowMeta.addValueMeta( valueMeta );

    UpdateMeta transformMeta = smh.processRowsTransformMetaInterface;

    UpdateData transformData = smh.processRowsTransformDataInterface;
    transformData.lookupParameterRowMeta = inputRowMeta;
    transformData.db = db;
    transformData.keynrs = transformData.valuenrs = new int[] { 0 };
    transformData.keynrs2 = new int[] { -1 };
    transformData.updateParameterRowMeta = when( mock( RowMeta.class ).size() ).thenReturn( 2 ).getMock();

    Update transform = new Update( smh.transformMeta, smh.transformDataInterface, 0, smh.pipelineMeta, smh.pipeline );
    transform.setInputRowMeta( inputRowMeta );
    transform.addRowSetToInputRowSets( smh.getMockInputRowSet( new Object[] { "2013-12-20".getBytes() } ) );
    transform.init( smh.initTransformMetaInterface, smh.initTransformDataInterface );
    transform.first = false;
    Assert.assertTrue( "Failure during row processing", transform.processRow( transformMeta, transformData ) );
  }
}
