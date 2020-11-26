/*!
 * Copyright 2018 Hitachi Vantara.  All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.pentaho.di.trans.steps.cassandrasstableoutput;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.pentaho.di.core.RowSet;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LoggingObjectInterface;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaBase;
import org.pentaho.di.trans.steps.mock.StepMockHelper;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class SSTableOutputIT {
  private static StepMockHelper<SSTableOutputMeta, SSTableOutputData> helper;
  private static AtomicInteger i;

  @BeforeClass
  public static void setUp() throws KettleException {
    //KettleEnvironment.init();
    helper =
      new StepMockHelper<SSTableOutputMeta, SSTableOutputData>( "SSTableOutputIT", SSTableOutputMeta.class,
        SSTableOutputData.class );
    when( helper.logChannelInterfaceFactory.create( any(), any( LoggingObjectInterface.class ) ) ).thenReturn(
      helper.logChannelInterface );
    when( helper.trans.isRunning() ).thenReturn( true );
  }

  @Test
  public void testCQLS2SSTableWriter() throws Exception {
    SSTableOutput ssTableOutput =
      new SSTableOutput( helper.stepMeta, helper.stepDataInterface, 0, helper.transMeta, helper.trans );
    ValueMetaInterface one = new ValueMetaBase( "key", ValueMetaBase.TYPE_INTEGER );
    ValueMetaInterface two = new ValueMetaBase( "two", ValueMetaBase.TYPE_STRING );
    List<ValueMetaInterface> valueMetaList = new ArrayList<ValueMetaInterface>(  );
    valueMetaList.add( one );
    valueMetaList.add( two );
    String[] fieldNames = new String[] { "key", "two" };
    RowMetaInterface inputRowMeta = mock( RowMetaInterface.class );
    when( inputRowMeta.clone() ).thenReturn( inputRowMeta );
    when( inputRowMeta.size() ).thenReturn( 2 );
    when( inputRowMeta.getFieldNames() ).thenReturn( fieldNames );
    when( inputRowMeta.getValueMetaList() ).thenReturn( valueMetaList );
    RowSet rowset = helper.getMockInputRowSet( new Object[] { 1, "some" } );
    when( rowset.getRowMeta() ).thenReturn( inputRowMeta );
    ssTableOutput.addRowSetToInputRowSets( rowset );
    SSTableOutputMeta meta = createStepMeta( false );
    ssTableOutput.init( meta, helper.initStepDataInterface );
    ssTableOutput.processRow( meta, helper.processRowsStepDataInterface );
    Assert.assertEquals( "Step init error.", 0, ssTableOutput.getErrors() );
    assertEquals( "org.pentaho.di.trans.steps.cassandrasstableoutput.writer.CQL2SSTableWriter",
      ssTableOutput.writer.getClass().getName() );
    ssTableOutput.dispose( meta, helper.initStepDataInterface );
    Assert.assertEquals( "Step dispose error", 0, ssTableOutput.getErrors() );
  }

  @Test
  public void testCQLS3SSTableWriter() throws Exception {
    SSTableOutput ssTableOutput =
      new SSTableOutput( helper.stepMeta, helper.stepDataInterface, 0, helper.transMeta, helper.trans );
    i = new AtomicInteger( 0 );
    ValueMetaInterface one = new ValueMetaBase( "key", ValueMetaBase.TYPE_INTEGER );
    ValueMetaInterface two = new ValueMetaBase( "two", ValueMetaBase.TYPE_STRING );
    List<ValueMetaInterface> valueMetaList = new ArrayList<ValueMetaInterface>(  );
    valueMetaList.add( one );
    valueMetaList.add( two );
    String[] fieldNames = new String[] { "key", "two" };
    RowMetaInterface inputRowMeta = mock( RowMetaInterface.class );
    when( inputRowMeta.clone() ).thenReturn( inputRowMeta );
    when( inputRowMeta.size() ).thenReturn( 2 );
    when( inputRowMeta.getFieldNames() ).thenReturn( fieldNames );
    when( inputRowMeta.getValueMetaList() ).thenReturn( valueMetaList );
    when( inputRowMeta.indexOfValue( anyString() ) ).thenAnswer( new Answer<Integer>() {
      @Override public Integer answer( InvocationOnMock invocation ) throws Throwable {
        return i.getAndIncrement();
      }
    } );
    RowSet rowset = helper.getMockInputRowSet( new Object[] { 1L, "some" } );
    when( rowset.getRowMeta() ).thenReturn( inputRowMeta );
    ssTableOutput.addRowSetToInputRowSets( rowset );
    SSTableOutputMeta meta = createStepMeta( true );
    ssTableOutput.init( meta, helper.initStepDataInterface );
    ssTableOutput.processRow( meta, helper.processRowsStepDataInterface );
    Assert.assertEquals( "Step init error.", 0, ssTableOutput.getErrors() );
    assertEquals( "org.pentaho.di.trans.steps.cassandrasstableoutput.writer.CQL3SSTableWriter",
      ssTableOutput.writer.getClass().getName() );
    ssTableOutput.dispose( meta, helper.initStepDataInterface );
    Assert.assertEquals( "Step dispose error", 0, ssTableOutput.getErrors() );
  }

  private SSTableOutputMeta createStepMeta( Boolean v3 ) throws IOException {
    File tempFile = File.createTempFile( getClass().getName(), ".tmp" );
    tempFile.deleteOnExit();

    final SSTableOutputMeta meta = new SSTableOutputMeta();
    meta.setBufferSize( "1000" );
    meta.setDirectory( tempFile.getParentFile().toURI().toString() );
    meta.setCassandraKeyspace( "key" );
    meta.setYamlPath( getClass().getResource( "cassandra.yaml" ).getFile() );
    meta.setTableName( "cfq" );
    if ( v3 ) {
      meta.setKeyField( "key,two" );
    } else {
      meta.setKeyField( "key" );

    }
    meta.setUseCQL3( v3 );

    return meta;
  }
}
