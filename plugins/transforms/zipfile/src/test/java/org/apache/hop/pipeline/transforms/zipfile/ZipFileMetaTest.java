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

package org.apache.hop.pipeline.transforms.zipfile;

import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.zipfile.ZipFileMeta;
import org.junit.Test;
import org.w3c.dom.Node;

import java.util.ArrayList;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/**
 * Created by bgroves on 11/10/15.
 */
public class ZipFileMetaTest {

  private static final String SOURCE_FILENAME = "Files";
  private static final String TARGET_FILENAME = "ZipFile";
  private static final String BASE_FOLDER = "BaseFolder";
  private static final String OPERATION_TYPE = "move";
  private static final boolean ADD_RESULT_FILENAME = true;
  private static final boolean OVERWRITE_ZIP_ENTRY = true;
  private static final boolean CREATE_PARENT_FOLDER = true;
  private static final boolean KEEP_SOURCE_FOLDER = true;
  private static final String MOVE_TO_FOLDER_FIELD = "movetothisfolder";

  @Test
  public void testGettersSetters() {
    ZipFileMeta zipFileMeta = new ZipFileMeta();
    zipFileMeta.setDynamicSourceFileNameField( SOURCE_FILENAME );
    zipFileMeta.setDynamicTargetFileNameField( TARGET_FILENAME );
    zipFileMeta.setBaseFolderField( BASE_FOLDER );
    zipFileMeta.setOperationType( ZipFileMeta.getOperationTypeByDesc( OPERATION_TYPE ) );
    zipFileMeta.setaddTargetFileNametoResult( ADD_RESULT_FILENAME );
    zipFileMeta.setOverwriteZipEntry( OVERWRITE_ZIP_ENTRY );
    zipFileMeta.setCreateParentFolder( CREATE_PARENT_FOLDER );
    zipFileMeta.setKeepSouceFolder( KEEP_SOURCE_FOLDER );
    zipFileMeta.setMoveToFolderField( MOVE_TO_FOLDER_FIELD );

    assertEquals( SOURCE_FILENAME, zipFileMeta.getDynamicSourceFileNameField() );
    assertEquals( TARGET_FILENAME, zipFileMeta.getDynamicTargetFileNameField() );
    assertEquals( BASE_FOLDER, zipFileMeta.getBaseFolderField() );
    assertEquals( ZipFileMeta.getOperationTypeByDesc( OPERATION_TYPE ), zipFileMeta.getOperationType() );
    assertEquals( MOVE_TO_FOLDER_FIELD, zipFileMeta.getMoveToFolderField() );
    assertTrue( zipFileMeta.isaddTargetFileNametoResult() );
    assertTrue( zipFileMeta.isOverwriteZipEntry() );
    assertTrue( zipFileMeta.isKeepSouceFolder() );
    assertTrue( zipFileMeta.isCreateParentFolder() );
    assertEquals( MOVE_TO_FOLDER_FIELD, zipFileMeta.getMoveToFolderField() );

    assertNotNull( zipFileMeta.getTransformData() );
    assertTrue( zipFileMeta.supportsErrorHandling() );
  }

  @Test
  public void testLoadAndGetXml() throws Exception {
    ZipFileMeta zipFileMeta = new ZipFileMeta();
    Node transformNode = getTestNode();
    DatabaseMeta dbMeta = mock( DatabaseMeta.class );
    IHopMetadataProvider metadataProvider = mock( IHopMetadataProvider.class );
    TransformMeta mockParentTransformMeta = mock( TransformMeta.class );
    zipFileMeta.setParentTransformMeta( mockParentTransformMeta );
    PipelineMeta mockPipelineMeta = mock( PipelineMeta.class );
    when( mockParentTransformMeta.getParentPipelineMeta() ).thenReturn( mockPipelineMeta );
    zipFileMeta.loadXml( transformNode, metadataProvider );
    assertXmlOutputMeta( zipFileMeta );
  }

  @Test
  public void testCheck() {
    ZipFileMeta zipFileMeta = new ZipFileMeta();
    zipFileMeta.setDefault();
    PipelineMeta pipelineMeta = mock( PipelineMeta.class );
    TransformMeta transformMeta = mock( TransformMeta.class );
    IRowMeta prev = mock( IRowMeta.class );
    IHopMetadataProvider metadataProvider = mock( IHopMetadataProvider.class );
    IRowMeta info = mock( IRowMeta.class );
    ArrayList<ICheckResult> remarks = new ArrayList<>();

    zipFileMeta.check( remarks, pipelineMeta, transformMeta, prev, new String[] { "input" }, new String[] { "output" }, info,
      new Variables(), metadataProvider );
    assertEquals( 2, remarks.size() );
    assertEquals( "Source Filename field is missing!", remarks.get( 0 ).getText() );
    assertEquals( "Transform is receiving info from other transforms.", remarks.get( 1 ).getText() );

    remarks = new ArrayList<>();
    zipFileMeta = new ZipFileMeta();
    zipFileMeta.setDynamicSourceFileNameField( "sourceFileField" );
    zipFileMeta.check( remarks, pipelineMeta, transformMeta, prev, new String[ 0 ], new String[] { "output" }, info,
      new Variables(), metadataProvider );
    assertEquals( 2, remarks.size() );
    assertEquals( "Target Filename field was specified", remarks.get( 0 ).getText() );
    assertEquals( "No input received from other transforms!", remarks.get( 1 ).getText() );
  }

  @Test
  public void testGetTransform() throws Exception {
    TransformMeta transformMeta = mock( TransformMeta.class );
    when( transformMeta.getName() ).thenReturn( "Zip Transform Name" );
    ZipFileData transformData = mock( ZipFileData.class );
    PipelineMeta pipelineMeta = mock( PipelineMeta.class );
    when( pipelineMeta.findTransform( "Zip Transform Name" ) ).thenReturn( transformMeta );
    Pipeline pipeline = spy( new LocalPipelineEngine() );

    ZipFileMeta zipFileMeta = new ZipFileMeta();
    ZipFile zipFile = new ZipFile( transformMeta, zipFileMeta,transformData, 0, pipelineMeta, pipeline );
    assertEquals( transformMeta, zipFile.getTransformMeta() );
    assertEquals( transformData, zipFile.getData() );
    assertEquals( pipelineMeta, zipFile.getPipelineMeta() );
    assertEquals( pipeline, zipFile.getPipeline() );
    assertEquals( 0, zipFile.getCopy() );
  }

  @Test
  public void testOperationType() {
    assertEquals( 0, ZipFileMeta.getOperationTypeByDesc( null ) );
    assertEquals( 1, ZipFileMeta.getOperationTypeByDesc( "Move source file" ) );
    assertEquals( 1, ZipFileMeta.getOperationTypeByDesc( "move" ) );
    assertEquals( 0, ZipFileMeta.getOperationTypeByDesc( "doesn't exist" ) );

    assertEquals( "Move source file", ZipFileMeta.getOperationTypeDesc( 1 ) );
    assertEquals( "Do nothing", ZipFileMeta.getOperationTypeDesc( 100 ) );
    assertEquals( "Do nothing", ZipFileMeta.getOperationTypeDesc( -1 ) );
  }

  private Node getTestNode() throws HopXmlException {
    String xml = "<transform>" + Const.CR
      + "<name>Zip file</name>" + Const.CR
      + "<type>ZipFile</type>" + Const.CR
      + "<description/>" + Const.CR
      + "<distribute>Y</distribute>" + Const.CR
      + "<custom_distribution/>" + Const.CR
      + "<copies>1</copies>" + Const.CR
      + "<partitioning>" + Const.CR
      + "  <method>none</method>" + Const.CR
      + "  <schema_name/>" + Const.CR
      + "</partitioning>" + Const.CR
      + "<sourcefilenamefield>Files</sourcefilenamefield>" + Const.CR
      + "<targetfilenamefield>ZipFile</targetfilenamefield>" + Const.CR
      + "<baseFolderField>BaseFolder</baseFolderField>" + Const.CR
      + "<operation_type>move</operation_type>" + Const.CR
      + "<addresultfilenames>Y</addresultfilenames>" + Const.CR
      + "<overwritezipentry>Y</overwritezipentry>" + Const.CR
      + "<createparentfolder>Y</createparentfolder>" + Const.CR
      + "<keepsourcefolder>Y</keepsourcefolder>" + Const.CR
      + "<movetofolderfield/>" + Const.CR
      + "<GUI>" + Const.CR
      + "  <xloc>608</xloc>" + Const.CR
      + "  <yloc>48</yloc>" + Const.CR
      + "  <draw>Y</draw>" + Const.CR
      + "</GUI>" + Const.CR
      + "</transform>" + Const.CR;
    return XmlHandler.loadXmlString( xml, "transform" );
  }

  private void assertXmlOutputMeta( ZipFileMeta zipOutputFile ) {
    assertEquals( "BaseFolder", zipOutputFile.getBaseFolderField() );
    assertEquals( "Files", zipOutputFile.getDynamicSourceFileNameField() );
    assertEquals( "ZipFile", zipOutputFile.getDynamicTargetFileNameField() );
    assertEquals( null, zipOutputFile.getMoveToFolderField() );
    assertEquals( "    <sourcefilenamefield>Files</sourcefilenamefield>" + Const.CR
      + "    <targetfilenamefield>ZipFile</targetfilenamefield>" + Const.CR
      + "    <baseFolderField>BaseFolder</baseFolderField>" + Const.CR
      + "    <operation_type>move</operation_type>" + Const.CR
      + "    <addresultfilenames>Y</addresultfilenames>" + Const.CR
      + "    <overwritezipentry>Y</overwritezipentry>" + Const.CR
      + "    <createparentfolder>Y</createparentfolder>" + Const.CR
      + "    <keepsourcefolder>Y</keepsourcefolder>" + Const.CR
      + "    <movetofolderfield/>" + Const.CR, zipOutputFile.getXml() );

    zipOutputFile.setDefault();

    assertEquals( "    <sourcefilenamefield>Files</sourcefilenamefield>" + Const.CR
      + "    <targetfilenamefield>ZipFile</targetfilenamefield>" + Const.CR
      + "    <baseFolderField>BaseFolder</baseFolderField>" + Const.CR
      + "    <operation_type/>" + Const.CR
      + "    <addresultfilenames>N</addresultfilenames>" + Const.CR
      + "    <overwritezipentry>N</overwritezipentry>" + Const.CR
      + "    <createparentfolder>N</createparentfolder>" + Const.CR
      + "    <keepsourcefolder>N</keepsourcefolder>" + Const.CR
      + "    <movetofolderfield/>" + Const.CR, zipOutputFile.getXml() );
  }
}
