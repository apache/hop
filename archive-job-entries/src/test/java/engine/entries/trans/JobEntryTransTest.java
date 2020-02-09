/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2019 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.job.entries.trans;

import org.apache.hop.cluster.SlaveServer;
import org.apache.hop.core.Result;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.listeners.CurrentDirectoryChangedListener;
import org.apache.hop.core.parameters.NamedParams;
import org.apache.hop.core.parameters.NamedParamsDefault;
import org.apache.hop.core.parameters.UnknownParamException;
import org.apache.hop.core.variables.VariableSpace;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.job.JobMeta;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.trans.Trans;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.powermock.reflect.Whitebox.setInternalState;

public class JobEntryTransTest {
  private final String JOB_ENTRY_TRANS_NAME = "JobEntryTransName";
  private final String JOB_ENTRY_FILE_NAME = "JobEntryFileName";
  private final String JOB_ENTRY_FILE_DIRECTORY = "JobEntryFileDirectory";
  private final String JOB_ENTRY_DESCRIPTION = "JobEntryDescription";

  //prepare xml for use
  public Node getEntryNode( boolean includeTransname )
    throws ParserConfigurationException, SAXException, IOException {
    JobEntryTrans jobEntryTrans = getJobEntryTrans();
    jobEntryTrans.setDescription( JOB_ENTRY_DESCRIPTION );
    jobEntryTrans.setFileName( JOB_ENTRY_FILE_NAME );
    String string = "<job>" + jobEntryTrans.getXML() + "</job>";
    InputStream stream = new ByteArrayInputStream( string.getBytes( StandardCharsets.UTF_8 ) );
    DocumentBuilder db;
    Document doc;
    db = DocumentBuilderFactory.newInstance().newDocumentBuilder();
    doc = db.parse( stream );
    Node entryNode = doc.getFirstChild();
    return entryNode;
  }

  private JobEntryTrans getJobEntryTrans() {
    JobEntryTrans jobEntryTrans = new JobEntryTrans( JOB_ENTRY_TRANS_NAME );
    return jobEntryTrans;
  }

  @SuppressWarnings( "unchecked" )
  private void testJobEntry( boolean includeJobName )
    throws HopXMLException, ParserConfigurationException, SAXException, IOException {
    List<DatabaseMeta> databases = mock( List.class );
    List<SlaveServer> slaveServers = mock( List.class );
    IMetaStore metaStore = mock( IMetaStore.class );
    JobEntryTrans jobEntryTrans = getJobEntryTrans();
    jobEntryTrans.loadXML( getEntryNode( includeJobName ), metaStore );
  }

  @Test
  @Ignore // Not sure how this would work
  public void testCurrDirListener() throws Exception {
    JobMeta meta = mock( JobMeta.class );
    JobEntryTrans jet = getJobEntryTrans();
    jet.setParentJobMeta( meta );
    jet.setParentJobMeta( null );
    verify( meta, times( 1 ) ).addCurrentDirectoryChangedListener( any( CurrentDirectoryChangedListener.class ) );
    verify( meta, times( 1 ) ).removeCurrentDirectoryChangedListener( any( CurrentDirectoryChangedListener.class ) );
  }

  @Test
  public void testPrepareFieldNamesParameters() throws UnknownParamException {
    // array of params
    String[] parameterNames = new String[ 2 ];
    parameterNames[ 0 ] = "param1";
    parameterNames[ 1 ] = "param2";

    // array of fieldNames params
    String[] parameterFieldNames = new String[ 1 ];
    parameterFieldNames[ 0 ] = "StreamParam1";

    // array of parameterValues params
    String[] parameterValues = new String[ 2 ];
    parameterValues[ 1 ] = "ValueParam2";


    JobEntryTrans jet = new JobEntryTrans();
    VariableSpace variableSpace = new Variables();
    jet.copyVariablesFrom( variableSpace );

    //at this point StreamColumnNameParams are already inserted in namedParams
    NamedParams namedParam = Mockito.mock( NamedParamsDefault.class );
    Mockito.doReturn( "value1" ).when( namedParam ).getParameterValue( "param1" );
    Mockito.doReturn( "value2" ).when( namedParam ).getParameterValue( "param2" );

    jet.prepareFieldNamesParameters( parameterNames, parameterFieldNames, parameterValues, namedParam, jet );

    Assert.assertEquals( "value1", jet.getVariable( "param1" ) );
    Assert.assertEquals( null, jet.getVariable( "param2" ) );
  }

  @Test
  public void updateResultTest() {
    updateResultTest( 3, 5 );
  }

  @Test
  public void updateResultTestWithZeroRows() {
    updateResultTest( 3, 0 );
  }

  private void updateResultTest( int previousRowsResult, int newRowsResult ) {
    JobEntryTrans jobEntryTrans = spy( getJobEntryTrans() );
    Trans transMock = mock( Trans.class );
    setInternalState( jobEntryTrans, "trans", transMock );
    //Transformation returns result with <newRowsResult> rows
    when( transMock.getResult() ).thenReturn( generateDummyResult( newRowsResult ) );
    //Previous result has <previousRowsResult> rows
    Result resultToBeUpdated = generateDummyResult( previousRowsResult );
    //Update the result
    jobEntryTrans.updateResult( resultToBeUpdated );
    //Result should have the number of rows of the transformation result
    assertEquals( resultToBeUpdated.getRows().size(), newRowsResult );
  }

  private Result generateDummyResult( int nRows ) {
    Result result = new Result();
    List<RowMetaAndData> rows = new ArrayList<>();
    for ( int i = 0; i < nRows; ++i ) {
      rows.add( new RowMetaAndData() );
    }
    result.setRows( rows );
    return result;
  }
}
