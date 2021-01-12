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

import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.Const;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.util.StringUtil;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.resource.IResourceNaming;
import org.junit.BeforeClass;
import org.junit.Test;
import org.w3c.dom.Node;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class XmlOutputMetaTest {
  @BeforeClass
  public static void setUp() throws Exception {
    if ( !HopClientEnvironment.isInitialized() ) {
      HopClientEnvironment.init();
    }

  }

  @Test
  public void testLoadAndGetXml() throws Exception {
    XmlOutputMeta xmlOutputMeta = new XmlOutputMeta();
    Node transformNode = getTestNode();
    DatabaseMeta dbMeta = mock( DatabaseMeta.class );
    IHopMetadataProvider metadataProvider = mock( IHopMetadataProvider.class );
    xmlOutputMeta.loadXml( transformNode, metadataProvider );
    assertXmlOutputMeta( xmlOutputMeta );
  }

  private void assertXmlOutputMeta( XmlOutputMeta xmlOutputMeta ) {
    assertEquals( "xmlOutputFile", xmlOutputMeta.getFileName() );
    assertFalse( xmlOutputMeta.isDoNotOpenNewFileInit() );
    assertFalse( xmlOutputMeta.isServletOutput() );
    assertEquals( "hop.xml", xmlOutputMeta.getExtension() );
    assertTrue( xmlOutputMeta.isTransformNrInFilename() );
    assertTrue( xmlOutputMeta.isDateInFilename() );
    assertTrue( xmlOutputMeta.isTimeInFilename() );
    assertFalse( xmlOutputMeta.isSpecifyFormat() );
    assertTrue( StringUtil.isEmpty( xmlOutputMeta.getDateTimeFormat() ) );
    assertFalse( xmlOutputMeta.isAddToResultFiles() );
    assertFalse( xmlOutputMeta.isZipped() );
    assertEquals( "UTF-8", xmlOutputMeta.getEncoding() );
    assertTrue( StringUtil.isEmpty( xmlOutputMeta.getNameSpace() ) );
    assertEquals( "Rows", xmlOutputMeta.getMainElement() );
    assertEquals( "Row", xmlOutputMeta.getRepeatElement() );
    assertEquals( 0, xmlOutputMeta.getSplitEvery() );
    assertTrue( xmlOutputMeta.isOmitNullValues() );
    XmlField[] outputFields = xmlOutputMeta.getOutputFields();
    assertEquals( 2, outputFields.length );

    assertEquals( "fieldOne", outputFields[0].getFieldName() );
    assertEquals( XmlField.ContentType.Element, outputFields[0].getContentType() );

    assertEquals( "fieldTwo", outputFields[1].getFieldName() );
    assertEquals( XmlField.ContentType.Attribute, outputFields[1].getContentType() );
    assertEquals( "    <encoding>UTF-8</encoding>" + Const.CR
        + "    <name_space/>" + Const.CR
        + "    <xml_main_element>Rows</xml_main_element>" + Const.CR
        + "    <xml_repeat_element>Row</xml_repeat_element>" + Const.CR
        + "    <file>" + Const.CR
        + "      <name>xmlOutputFile</name>" + Const.CR
        + "      <extention>hop.xml</extention>" + Const.CR
        + "      <servlet_output>N</servlet_output>" + Const.CR
        + "      <do_not_open_newfile_init>N</do_not_open_newfile_init>" + Const.CR
        + "      <split>Y</split>" + Const.CR
        + "      <add_date>Y</add_date>" + Const.CR
        + "      <add_time>Y</add_time>" + Const.CR
        + "      <SpecifyFormat>N</SpecifyFormat>" + Const.CR
        + "      <omit_null_values>Y</omit_null_values>" + Const.CR
        + "      <date_time_format/>" + Const.CR
        + "      <add_to_result_filenames>N</add_to_result_filenames>" + Const.CR
        + "      <zipped>N</zipped>" + Const.CR
        + "      <splitevery>0</splitevery>" + Const.CR
        + "    </file>" + Const.CR
        + "    <fields>" + Const.CR
        + "      <field>" + Const.CR
        + "        <content_type>Element</content_type>" + Const.CR
        + "        <name>fieldOne</name>" + Const.CR
        + "        <element/>" + Const.CR
        + "        <type>Number</type>" + Const.CR
        + "        <format/>" + Const.CR
        + "        <currency/>" + Const.CR
        + "        <decimal/>" + Const.CR
        + "        <group/>" + Const.CR
        + "        <nullif/>" + Const.CR
        + "        <length>-1</length>" + Const.CR
        + "        <precision>-1</precision>" + Const.CR
        + "      </field>" + Const.CR
        + "      <field>" + Const.CR
        + "        <content_type>Attribute</content_type>" + Const.CR
        + "        <name>fieldTwo</name>" + Const.CR
        + "        <element/>" + Const.CR
        + "        <type>String</type>" + Const.CR
        + "        <format/>" + Const.CR
        + "        <currency/>" + Const.CR
        + "        <decimal/>" + Const.CR
        + "        <group/>" + Const.CR
        + "        <nullif/>" + Const.CR
        + "        <length>-1</length>" + Const.CR
        + "        <precision>-1</precision>" + Const.CR
        + "      </field>" + Const.CR
        + "    </fields>" + Const.CR, xmlOutputMeta.getXml() );
  }

  private Node getTestNode() throws HopXmlException {
    String xml = "<transform>" + Const.CR
        + "<name>My XML Output</name>" + Const.CR
        + "<type>XMLOutput</type>" + Const.CR
        + "<description/>" + Const.CR
        + "<distribute>Y</distribute>" + Const.CR
        + "<custom_distribution/>" + Const.CR
        + "<copies>1</copies>" + Const.CR
        + "<partitioning>" + Const.CR
        + "  <method>none</method>" + Const.CR
        + "  <schema_name/>" + Const.CR
        + "</partitioning>" + Const.CR
        + "<encoding>UTF-8</encoding>" + Const.CR
        + "<name_space/>" + Const.CR
        + "<xml_main_element>Rows</xml_main_element>" + Const.CR
        + "<xml_repeat_element>Row</xml_repeat_element>" + Const.CR
        + "<file>" + Const.CR
        + "  <name>xmlOutputFile</name>" + Const.CR
        + "  <extention>hop.xml</extention>" + Const.CR
        + "  <servlet_output>N</servlet_output>" + Const.CR
        + "  <do_not_open_newfile_init>N</do_not_open_newfile_init>" + Const.CR
        + "  <split>Y</split>" + Const.CR
        + "  <add_date>Y</add_date>" + Const.CR
        + "  <add_time>Y</add_time>" + Const.CR
        + "  <SpecifyFormat>N</SpecifyFormat>" + Const.CR
        + "  <omit_null_values>Y</omit_null_values>" + Const.CR
        + "  <date_time_format/>" + Const.CR
        + "  <add_to_result_filenames>N</add_to_result_filenames>" + Const.CR
        + "  <zipped>N</zipped>" + Const.CR
        + "  <splitevery>0</splitevery>" + Const.CR
        + "</file>" + Const.CR
        + "<fields>" + Const.CR
        + "  <field>" + Const.CR
        + "    <content_type>Element</content_type>" + Const.CR
        + "    <name>fieldOne</name>" + Const.CR
        + "    <element/>" + Const.CR
        + "    <type>Number</type>" + Const.CR
        + "    <format/>" + Const.CR
        + "    <currency/>" + Const.CR
        + "    <decimal/>" + Const.CR
        + "    <group/>" + Const.CR
        + "    <nullif/>" + Const.CR
        + "    <length>-1</length>" + Const.CR
        + "    <precision>-1</precision>" + Const.CR
        + "  </field>" + Const.CR
        + "  <field>" + Const.CR
        + "    <content_type>Attribute</content_type>" + Const.CR
        + "    <name>fieldTwo</name>" + Const.CR
        + "    <element/>" + Const.CR
        + "    <type>String</type>" + Const.CR
        + "    <format/>" + Const.CR
        + "    <currency/>" + Const.CR
        + "    <decimal/>" + Const.CR
        + "    <group/>" + Const.CR
        + "    <nullif/>" + Const.CR
        + "    <length>-1</length>" + Const.CR
        + "    <precision>-1</precision>" + Const.CR
        + "  </field>" + Const.CR
        + "</fields>" + Const.CR
        + "<cluster_schema/>" + Const.CR
        + "<remotetransforms>   <input>   </input>   <output>   </output> </remotetransforms>    <GUI>" + Const.CR
        + "<xloc>256</xloc>" + Const.CR
        + "<yloc>64</yloc>" + Const.CR
        + "<draw>Y</draw>" + Const.CR
        + "</GUI>" + Const.CR
        + "</transform>" + Const.CR;
    return XmlHandler.loadXmlString( xml, "transform" );
  }

  @Test
  public void testGetNewline() throws Exception {
    XmlOutputMeta xmlOutputMeta = new XmlOutputMeta();
    assertEquals( "\r\n", xmlOutputMeta.getNewLine( "DOS" ) );
    assertEquals( "\n", xmlOutputMeta.getNewLine( "UNIX" ) );
    assertEquals( System.getProperty( "line.separator" ), xmlOutputMeta.getNewLine( null ) );
  }

  @Test
  public void testClone() throws Exception {
    XmlOutputMeta xmlOutputMeta = new XmlOutputMeta();
    Node transformNode = getTestNode();
    DatabaseMeta dbMeta = mock( DatabaseMeta.class );
    IHopMetadataProvider metadataProvider = mock( IHopMetadataProvider.class );
    xmlOutputMeta.loadXml( transformNode, metadataProvider );
    XmlOutputMeta cloned = (XmlOutputMeta) xmlOutputMeta.clone();
    assertNotSame( cloned, xmlOutputMeta );
    assertXmlOutputMeta( cloned );
  }

  @Test
  public void testSetDefault() throws Exception {
    XmlOutputMeta xmlOutputMeta = new XmlOutputMeta();
    xmlOutputMeta.setDefault();
    assertEquals( "file", xmlOutputMeta.getFileName() );
    assertEquals( "xml", xmlOutputMeta.getExtension() );
    assertFalse( xmlOutputMeta.isTransformNrInFilename() );
    assertFalse( xmlOutputMeta.isDoNotOpenNewFileInit() );
    assertFalse( xmlOutputMeta.isDateInFilename() );
    assertFalse( xmlOutputMeta.isTimeInFilename() );
    assertFalse( xmlOutputMeta.isAddToResultFiles() );
    assertFalse( xmlOutputMeta.isZipped() );
    assertEquals( 0, xmlOutputMeta.getSplitEvery() );
    assertEquals( Const.XML_ENCODING, xmlOutputMeta.getEncoding() );
    assertEquals( "", xmlOutputMeta.getNameSpace() );
    assertNull( xmlOutputMeta.getDateTimeFormat() );
    assertFalse( xmlOutputMeta.isSpecifyFormat() );
    assertFalse( xmlOutputMeta.isOmitNullValues() );
    assertEquals( "Rows", xmlOutputMeta.getMainElement() );
    assertEquals( "Row", xmlOutputMeta.getRepeatElement() );
  }

  @Test
  public void testGetFiles() throws Exception {
    XmlOutputMeta xmlOutputMeta = new XmlOutputMeta();
    xmlOutputMeta.setDefault();
    xmlOutputMeta.setTransformNrInFilename( true );
    xmlOutputMeta.setSplitEvery( 100 );
    xmlOutputMeta.setSpecifyFormat( true );
    xmlOutputMeta.setDateTimeFormat( "99" );
    String[] files = xmlOutputMeta.getFiles( new Variables() );
    assertEquals( 10, files.length );
    assertArrayEquals( new String[] { "file99_0_00001.xml", "file99_0_00002.xml", "file99_0_00003.xml",
      "file99_1_00001.xml", "file99_1_00002.xml", "file99_1_00003.xml", "file99_2_00001.xml", "file99_2_00002.xml",
      "file99_2_00003.xml", "..." }, files );
  }

  @Test
  public void testGetFields() throws Exception {
    XmlOutputMeta xmlOutputMeta = new XmlOutputMeta();
    xmlOutputMeta.setDefault();
    XmlField xmlField = new XmlField();
    xmlField.setFieldName( "aField" );
    xmlField.setLength( 10 );
    xmlField.setPrecision( 3 );
    xmlOutputMeta.setOutputFields( new XmlField[] { xmlField } );
    IRowMeta row = mock( IRowMeta.class );
    IRowMeta rmi = mock( IRowMeta.class );
    TransformMeta nextTransform = mock( TransformMeta.class );
    IHopMetadataProvider metadataProvider = mock( IHopMetadataProvider.class );
    IValueMeta vmi = mock( IValueMeta.class );
    when( row.searchValueMeta( "aField" ) ).thenReturn( vmi );
    xmlOutputMeta.getFields( row, "", new IRowMeta[] { rmi }, nextTransform, new Variables(), metadataProvider );
    verify( vmi ).setLength( 10, 3 );
  }

  @Test
  public void testLoadXmlException() throws Exception {
    XmlOutputMeta xmlOutputMeta = new XmlOutputMeta();
    DatabaseMeta dbMeta = mock( DatabaseMeta.class );
    IHopMetadataProvider metadataProvider = mock( IHopMetadataProvider.class );
    Node transformNode = mock( Node.class );
    when( transformNode.getChildNodes() ).thenThrow( new RuntimeException( "some words" ) );
    try {
      xmlOutputMeta.loadXml( transformNode, metadataProvider );
    } catch ( HopXmlException e ) {
      assertEquals( "some words", e.getCause().getMessage() );
    }
  }

  @Test
  public void testGetRequiredFields() throws Exception {
    XmlOutputMeta xmlOutputMeta = new XmlOutputMeta();
    xmlOutputMeta.setDefault();
    XmlField xmlField = new XmlField();
    xmlField.setFieldName( "aField" );
    xmlField.setType( 1 );
    xmlField.setLength( 10 );
    xmlField.setPrecision( 3 );

    XmlField xmlField2 = new XmlField();
    xmlField2.setFieldName( "bField" );
    xmlField2.setType( 3 );
    xmlField2.setLength( 4 );
    xmlField2.setPrecision( 5 );
    xmlOutputMeta.setOutputFields( new XmlField[] { xmlField, xmlField2 } );
    IRowMeta requiredFields = xmlOutputMeta.getRequiredFields( new Variables() );
    List<IValueMeta> valueMetaList = requiredFields.getValueMetaList();
    assertEquals( 2, valueMetaList.size() );
    assertEquals( "aField", valueMetaList.get( 0 ).getName() );
    assertEquals( 1, valueMetaList.get( 0 ).getType() );
    assertEquals( 10, valueMetaList.get( 0 ).getLength() );
    assertEquals( 3, valueMetaList.get( 0 ).getPrecision() );

    assertEquals( "bField", valueMetaList.get( 1 ).getName() );
    assertEquals( 3, valueMetaList.get( 1 ).getType() );
    assertEquals( 4, valueMetaList.get( 1 ).getLength() );
    assertEquals( 5, valueMetaList.get( 1 ).getPrecision() );
  }

  @Test
  public void testExportResources() throws Exception {
    XmlOutputMeta xmlOutputMeta = new XmlOutputMeta();
    xmlOutputMeta.setDefault();
    IResourceNaming resourceNamingInterface = mock( IResourceNaming.class );
    Variables variables = new Variables();
    when( resourceNamingInterface.nameResource( any( FileObject.class ), eq( variables ), eq( true ) ) ).thenReturn(
        "exportFile" );
    xmlOutputMeta.exportResources( variables, null, resourceNamingInterface, null );
    assertEquals( "exportFile", xmlOutputMeta.getFileName() );
  }

  @Test
  public void testCheck() throws Exception {
    XmlOutputMeta xmlOutputMeta = new XmlOutputMeta();
    xmlOutputMeta.setDefault();
    PipelineMeta pipelineMeta = mock( PipelineMeta.class );
    TransformMeta transformInfo = mock( TransformMeta.class );
    IRowMeta prev = mock( IRowMeta.class );
    IHopMetadataProvider metadataProvider = mock( IHopMetadataProvider.class );
    IRowMeta info = mock( IRowMeta.class );
    ArrayList<ICheckResult> remarks = new ArrayList<>();
    xmlOutputMeta.check( remarks, pipelineMeta, transformInfo, prev, new String[] { "input" }, new String[] { "output" }, info,
        new Variables(), metadataProvider );
    assertEquals( 2, remarks.size() );
    assertEquals( "Transform is receiving info from other transforms.", remarks.get( 0 ).getText() );
    assertEquals( "File specifications are not checked.", remarks.get( 1 ).getText() );

    XmlField xmlField = new XmlField();
    xmlField.setFieldName( "aField" );
    xmlField.setType( 1 );
    xmlField.setLength( 10 );
    xmlField.setPrecision( 3 );
    xmlOutputMeta.setOutputFields( new XmlField[] { xmlField } );
    when( prev.size() ).thenReturn( 1 );
    remarks.clear();
    xmlOutputMeta.check( remarks, pipelineMeta, transformInfo, prev, new String[] { "input" }, new String[] { "output" }, info,
        new Variables(), metadataProvider );
    assertEquals( 4, remarks.size() );
    assertEquals( "Transform is connected to previous one, receiving 1 fields", remarks.get( 0 ).getText() );
    assertEquals( "All output fields are found in the input stream.", remarks.get( 1 ).getText() );
    assertEquals( "Transform is receiving info from other transforms.", remarks.get( 2 ).getText() );
    assertEquals( "File specifications are not checked.", remarks.get( 3 ).getText() );

  }
}
