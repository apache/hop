/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
 * http://www.project-hop.org
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
package org.apache.hop.pipeline.transforms.xml.addxml;

import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.mock;
import static java.util.Arrays.asList;

import org.apache.hop.core.IRowSet;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import javax.sql.RowSet;


public class AddXMLTest {

  private TransformMockHelper<AddXmlMeta, AddXmlData> stepMockHelper;

  @Before
  public void setup() {
    XmlField field = mock( XmlField.class );
    when( field.getElementName() ).thenReturn( "ADDXML_TEST" );
    when( field.isAttribute() ).thenReturn( true );

    stepMockHelper = new TransformMockHelper<AddXmlMeta, AddXmlData>( "ADDXML_TEST", AddXmlMeta.class, AddXmlData.class );
//    when( stepMockHelper.logChannelInterfaceFactory.create( any(), any( ILogChannel.class ) ) ).thenReturn(
//        stepMockHelper.logChannelInterface );
    when( stepMockHelper.pipeline.isRunning() ).thenReturn( true );
//    SocketRepository socketRepository = mock( SocketRepository.class );
//    when( stepMockHelper.pipeline.getSocketRepository() ).thenReturn( socketRepository );
    when( stepMockHelper.iTransformMeta.getOutputFields() ).thenReturn( new XmlField[] { field } );
    when( stepMockHelper.iTransformMeta.getRootNode() ).thenReturn( "ADDXML_TEST" );
  }

  @After
  public void tearDown() {
    stepMockHelper.cleanUp();
  }

  @Test
  @Ignore
  public void testProcessRow() throws HopException {
    AddXml addXML =
        new AddXml( stepMockHelper.transformMeta, stepMockHelper.iTransformMeta, stepMockHelper.iTransformData, 0, stepMockHelper.pipelineMeta,
            stepMockHelper.pipeline );
    addXML.init(  );
    addXML.setInputRowSets( asList( createSourceRowSet( "ADDXML_TEST" ) ) );

    assertTrue( addXML.processRow( ) );
    assertTrue( addXML.getErrors() == 0 );
    assertTrue( addXML.getLinesWritten() > 0 );
  }

  private IRowSet createSourceRowSet(String source ) {
    IRowSet sourceRowSet = stepMockHelper.getMockInputRowSet( new String[] { source } );
    IRowMeta sourceRowMeta = mock( IRowMeta.class );
    when( sourceRowMeta.getFieldNames() ).thenReturn( new String[] { source } );
    when( sourceRowSet.getRowMeta() ).thenReturn( sourceRowMeta );

    return sourceRowSet;
  }

}
