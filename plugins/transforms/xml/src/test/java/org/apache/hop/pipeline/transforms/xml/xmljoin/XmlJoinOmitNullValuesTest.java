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

package org.apache.hop.pipeline.transforms.xml.xmljoin;

import org.apache.hop.core.IRowSet;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.transform.ITransformIOMeta;
import org.apache.hop.pipeline.transform.RowAdapter;
import org.apache.hop.pipeline.transform.errorhandling.IStream;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.List;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/**
 * Test for XmlJoin transform
 * 
 * @author Pavel Sakun
 * @see XmlJoin
 */
@RunWith( MockitoJUnitRunner.class )
public class XmlJoinOmitNullValuesTest {
  TransformMockHelper<XmlJoinMeta, XmlJoinData> tmh;

  @Before
  public void init() throws Exception {
    tmh = new TransformMockHelper<>( "XmlJoin", XmlJoinMeta.class, XmlJoinData.class );
    when( tmh.logChannelFactory.create( any(), any( ILoggingObject.class ) ) ).thenReturn(
        tmh.iLogChannel );
    when( tmh.pipeline.isRunning() ).thenReturn( true );
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
    XmlJoin spy = spy( new XmlJoin( tmh.transformMeta, tmh.iTransformMeta, tmh.iTransformData, 0, tmh.pipelineMeta, tmh.pipeline ) );

    /*
    // Find the row sets to read from
      //
      List<IStream> infoStreams = meta.getTransformIOMeta().getInfoStreams();

      // Get the two input row sets
      data.TargetRowSet = findInputRowSet( infoStreams.get( 0 ).getTransformName() );
      data.SourceRowSet = findInputRowSet( infoStreams.get( 1 ).getTransformName() );
     */
    ITransformIOMeta transformIOMeta = mock( ITransformIOMeta.class );
    when(tmh.iTransformMeta.getTransformIOMeta()).thenReturn( transformIOMeta );

    IStream inputStreamTarget = mock(IStream.class);
    IStream inputStreamSource = mock(IStream.class);
    List<IStream> inputStreams = mock( List.class );
    when(transformIOMeta.getInfoStreams()).thenReturn( inputStreams );

    IRowSet sourceRowSet = createSourceRowSet( sourceXml );
    IRowSet targetRowSet = createTargetRowSet( targetXml );
    when(inputStreams.get(0)).thenReturn( inputStreamTarget );
    when(inputStreams.get(1)).thenReturn( inputStreamSource );

    when(inputStreamTarget.getTransformName()).thenReturn("target");
    when(inputStreamSource.getTransformName()).thenReturn("source");

    doReturn( sourceRowSet ).when( spy ).findInputRowSet( "source" );
    doReturn( targetRowSet ).when( spy ).findInputRowSet( "target" );

    XmlJoinMeta transformMeta = tmh.iTransformMeta;
    when( transformMeta.getSourceXmlTransform() ).thenReturn( "source" );
    when( transformMeta.getTargetXmlTransform() ).thenReturn( "target" );
    when( transformMeta.getSourceXmlField() ).thenReturn( "sourceField" );
    when( transformMeta.getTargetXmlField() ).thenReturn( "targetField" );
    when( transformMeta.getValueXmlField() ).thenReturn( "resultField" );
    when( transformMeta.getTargetXPath() ).thenReturn( "//root" );
    when( transformMeta.isOmitNullValues() ).thenReturn( true );

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
    IRowSet sourceRowSet = tmh.getMockInputRowSet( new String[] { sourceXml } );
    IRowMeta sourceRowMeta = mock( IRowMeta.class );
    when( sourceRowMeta.getFieldNames() ).thenReturn( new String[] { "sourceField" } );
    when( sourceRowSet.getRowMeta() ).thenReturn( sourceRowMeta );

    return sourceRowSet;
  }

  private IRowSet createTargetRowSet( String targetXml ) {
    IRowSet targetRowSet = tmh.getMockInputRowSet( new String[] { targetXml } );
    IRowMeta targetRowMeta = mock( IRowMeta.class );
    when( targetRowMeta.getFieldNames() ).thenReturn( new String[] { "targetField" } );
    when( targetRowMeta.clone() ).thenReturn( targetRowMeta );
    when( targetRowMeta.size() ).thenReturn( 1 );
    when( targetRowSet.getRowMeta() ).thenReturn( targetRowMeta );

    return targetRowSet;
  }
}
