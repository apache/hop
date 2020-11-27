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

package org.apache.hop.pipeline.transforms.xml.xmljoin;

import org.apache.hop.core.IRowSet;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.transform.RowAdapter;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/**
 * Test for XmlJoin step
 * 
 * @author Pavel Sakun
 * @see XmlJoin
 */
@RunWith( MockitoJUnitRunner.class )
public class XmlJoinOmitNullValuesTest {
  TransformMockHelper<XmlJoinMeta, XmlJoinData> smh;

  @Before
  public void init() {
    smh = new TransformMockHelper<XmlJoinMeta, XmlJoinData>( "XmlJoin", XmlJoinMeta.class, XmlJoinData.class );
    when( smh.logChannelFactory.create( any(), any( ILoggingObject.class ) ) ).thenReturn(
        smh.iLogChannel );
    when( smh.pipeline.isRunning() ).thenReturn( true );
  }

  @Test
  public void testRemoveEmptyNodes() throws HopException {
    doTest(
        "<child><empty/><subChild a=\"\"><empty/></subChild><subChild><empty/></subChild><subChild><subSubChild a=\"\"/></subChild></child>",
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><root xmlns=\"http://www.myns1.com\" xmlns:xsi=\"http://www.myns2.com\" xsi:schemalocation=\"http://www.mysl1.com\"></root>",
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><root xmlns:xsi=\"http://www.myns2.com\" xsi:schemalocation=\"http://www.mysl1.com\"><child><subChild a=\"\"/><subChild><subSubChild a=\"\"/></subChild></child></root>" );
  }

  private void doTest( final String sourceXml, final String targetXml, final String expectedXml )
    throws HopException {
    XmlJoin spy = spy( new XmlJoin( smh.transformMeta, smh.iTransformMeta, smh.iTransformData, 0, smh.pipelineMeta, smh.pipeline ) );

    doReturn( createSourceRowSet( sourceXml ) ).when( spy ).findInputRowSet( "source" );
    doReturn( createTargetRowSet( targetXml ) ).when( spy ).findInputRowSet( "target" );

    XmlJoinMeta stepMeta = smh.iTransformMeta;
    when( stepMeta.getSourceXmlTransform() ).thenReturn( "source" );
    when( stepMeta.getTargetXmlTransform() ).thenReturn( "target" );
    when( stepMeta.getSourceXmlField() ).thenReturn( "sourceField" );
    when( stepMeta.getTargetXmlField() ).thenReturn( "targetField" );
    when( stepMeta.getValueXmlField() ).thenReturn( "resultField" );
    when( stepMeta.getTargetXPath() ).thenReturn( "//root" );
    when( stepMeta.isOmitNullValues() ).thenReturn( true );

    spy.init();

    spy.addRowListener( new RowAdapter() {
      @Override
      public void rowWrittenEvent(IRowMeta rowMeta, Object[] row ) throws HopTransformException {
        Assert.assertEquals( expectedXml, row[0] );
      }
    } );

    Assert.assertTrue( spy.processRow() );
    Assert.assertFalse( spy.processRow() );
  }

  private IRowSet createSourceRowSet(String sourceXml ) {
    IRowSet sourceRowSet = smh.getMockInputRowSet( new String[] { sourceXml } );
    IRowMeta sourceRowMeta = mock( IRowMeta.class );
    when( sourceRowMeta.getFieldNames() ).thenReturn( new String[] { "sourceField" } );
    when( sourceRowSet.getRowMeta() ).thenReturn( sourceRowMeta );

    return sourceRowSet;
  }

  private IRowSet createTargetRowSet( String targetXml ) {
    IRowSet targetRowSet = smh.getMockInputRowSet( new String[] { targetXml } );
    IRowMeta targetRowMeta = mock( IRowMeta.class );
    when( targetRowMeta.getFieldNames() ).thenReturn( new String[] { "targetField" } );
    when( targetRowMeta.clone() ).thenReturn( targetRowMeta );
    when( targetRowMeta.size() ).thenReturn( 1 );
    when( targetRowSet.getRowMeta() ).thenReturn( targetRowMeta );

    return targetRowSet;
  }
}
