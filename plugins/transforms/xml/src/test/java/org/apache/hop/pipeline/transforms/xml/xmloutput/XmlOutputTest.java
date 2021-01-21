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
package org.apache.hop.pipeline.transforms.xml.xmloutput;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Matchers;

import org.apache.hop.pipeline.transforms.xml.xmloutput.XmlField.ContentType;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author Tatsiana_Kasiankova
 * 
 */
public class XmlOutputTest {

  private TransformMockHelper<XmlOutputMeta, XmlOutputData> transformMockHelper;
  private XmlOutput xmlOutput;
  private XmlOutputMeta xmlOutputMeta;
  private XmlOutputData xmlOutputData;
  private Pipeline trans = mock( Pipeline.class );
  private static final String[] ILLEGAL_CHARACTERS_IN_XML_ATTRIBUTES = { "<", ">", "&", "\'", "\"" };

  private static Object[] rowWithData;
  private static Object[] rowWithNullData;

  @BeforeClass
  public static void setUpBeforeClass() {

    rowWithData = initRowWithData( ILLEGAL_CHARACTERS_IN_XML_ATTRIBUTES );
    rowWithNullData = initRowWithNullData();
  }

  @Before
  public void setup() throws Exception {

    transformMockHelper =
      new TransformMockHelper<>( "XML_OUTPUT_TEST", XmlOutputMeta.class, XmlOutputData.class );
    when( transformMockHelper.logChannelFactory.create( any(), any( ILoggingObject.class ) ) ).thenReturn(
        transformMockHelper.iLogChannel );
    TransformMeta mockMeta = mock( TransformMeta.class );
    when( transformMockHelper.pipelineMeta.findTransform( Matchers.anyString() ) ).thenReturn( mockMeta );
    when( trans.getLogLevel() ).thenReturn( LogLevel.DEBUG );

    // Create and set Meta with some realistic data
    xmlOutputMeta = new XmlOutputMeta();
    xmlOutputMeta.setOutputFields( initOutputFields( rowWithData.length, ContentType.Attribute ) );
    // Set as true to prevent unnecessary for this test checks at initialization
    xmlOutputMeta.setDoNotOpenNewFileInit( false );

    xmlOutputData = new XmlOutputData();
    xmlOutputData.formatRowMeta = initRowMeta( rowWithData.length );
    xmlOutputData.fieldnrs = initFieldNmrs( rowWithData.length );
    xmlOutputData.OpenedNewFile = true;

    TransformMeta transformMeta = new TransformMeta( "TransformMetaId", "TransformMetaName", xmlOutputMeta );
    xmlOutput = spy( new XmlOutput( transformMeta, xmlOutputMeta, xmlOutputData, 0, transformMockHelper.pipelineMeta, transformMockHelper.pipeline ) );
  }

  @Test
  public void testSpecialSymbolsInAttributeValuesAreEscaped() throws HopException, XMLStreamException {
    xmlOutput.init(  );

    xmlOutputData.writer = mock( XMLStreamWriter.class );
    xmlOutput.writeRowAttributes( rowWithData );
    xmlOutput.dispose( );
    verify( xmlOutputData.writer, times( rowWithData.length ) ).writeAttribute( any(), any() );
    verify( xmlOutput, atLeastOnce() ).closeOutputStream( any() );
  }

  @Test
  public void testNullInAttributeValuesAreEscaped() throws HopException, XMLStreamException {

    testNullValuesInAttribute( 0 );
  }

  @Test
  public void testNullInAttributeValuesAreNotEscaped() throws HopException, XMLStreamException {

    xmlOutput.setVariable( Const.HOP_COMPATIBILITY_XML_OUTPUT_NULL_VALUES, "Y" );

    testNullValuesInAttribute( rowWithNullData.length  );
  }

  /**
   * [PDI-15575] Testing to verify that getIfPresent defaults the XMLField ContentType value
   */
  @Test
  public void testDefaultXmlFieldContentType() {
    XmlField[] xmlFields = initOutputFields( 4, null );
    xmlFields[0].setContentType( ContentType.getIfPresent( "Element" ) );
    xmlFields[1].setContentType( ContentType.getIfPresent( "Attribute" ) );
    xmlFields[2].setContentType( ContentType.getIfPresent( "" ) );
    xmlFields[3].setContentType( ContentType.getIfPresent( "WrongValue" ) );
    assertEquals( xmlFields[0].getContentType(), ContentType.Element );
    assertEquals( xmlFields[1].getContentType(), ContentType.Attribute );
    assertEquals( xmlFields[2].getContentType(), ContentType.Element );
    assertEquals( xmlFields[3].getContentType(), ContentType.Element );
  }

  private void testNullValuesInAttribute( int writeNullInvocationExpected ) throws HopException, XMLStreamException {

    xmlOutput.init(  );

    xmlOutputData.writer = mock( XMLStreamWriter.class );
    xmlOutput.writeRowAttributes( rowWithNullData );
    xmlOutput.dispose(  );
    verify( xmlOutputData.writer, times( writeNullInvocationExpected ) ).writeAttribute( any(), any() );
    verify( xmlOutput, atLeastOnce() ).closeOutputStream( any() );
  }

  private static Object[] initRowWithData( String[] dt ) {

    Object[] data = new Object[dt.length * 3];
    for ( int i = 0; i < dt.length; i++ ) {
      data[3 * i] = dt[i] + "TEST";
      data[3 * i + 1] = "TEST" + dt[i] + "TEST";
      data[3 * i + 2] = "TEST" + dt[i];
    }
    return data;
  }

  private static Object[] initRowWithNullData() {

    Object[] data = new Object[15];
    for ( int i = 0; i < data.length; i++ ) {

      data[i] = null;
    }

    return data;
  }

  private IRowMeta initRowMeta(int count ) {
    IRowMeta rm = new RowMeta();
    for ( int i = 0; i < count; i++ ) {
      rm.addValueMeta( new ValueMetaString( "string" ) );
    }
    return rm;
  }

  private XmlField[] initOutputFields(int i, ContentType attribute ) {

    XmlField[] fields = new XmlField[i];
    for ( int j = 0; j < fields.length; j++ ) {
      fields[j] =
          new XmlField( attribute, "Fieldname" + ( j + 1 ), "ElementName" + ( j + 1 ), 2, null, -1, -1, null, null,
              null, null );
    }

    return fields;
  }

  private int[] initFieldNmrs( int i ) {
    int[] fNmrs = new int[i];
    for ( int j = 0; j < fNmrs.length; j++ ) {
      fNmrs[j] = j;
    }
    return fNmrs;
  }
}
