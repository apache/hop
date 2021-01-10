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

package org.apache.hop.pipeline.transforms.exceloutput;

import jxl.Workbook;
import jxl.write.WritableCellFormat;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Date;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;

/**
 * Created by Yury_Bakhmutski on 12/12/2016.
 */
public class ExcelOutputTest {
  public static final String CREATED_SHEET_NAME = "Sheet1";
  private static TransformMockHelper<ExcelOutputMeta, ExcelOutputData> helper;

  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @BeforeClass
  public static void setUp() throws HopException {
    HopEnvironment.init();
    helper =
      new TransformMockHelper<>( "ExcelOutputTest", ExcelOutputMeta.class,
        ExcelOutputData.class );
    when( helper.logChannelFactory.create( any(), any( ILoggingObject.class ) ) ).thenReturn(
      helper.iLogChannel );
    when( helper.pipeline.isRunning() ).thenReturn( true );
  }

  @AfterClass
  public static void cleanUp() {
    helper.cleanUp();
  }

  private String buildFilePath() throws IOException {
    File outFile = File.createTempFile( "aaa", ".xls" );
    String fileFullPath = outFile.getCanonicalPath();
    outFile.delete();
    return fileFullPath;
  }

  private ExcelOutputMeta createTransformMeta( String fileName, String templateFullPath, boolean isAppend )
    throws IOException {
    ExcelOutputMeta meta = new ExcelOutputMeta();
    meta.setFileName( fileName );
    if ( null == templateFullPath ) {
      meta.setTemplateEnabled( false );
    } else {
      meta.setTemplateEnabled( true );
      meta.setTemplateFileName( templateFullPath );
    }
    meta.setAppend( isAppend );
    ExcelField[] excelFields = { new ExcelField( "f1", IValueMeta.TYPE_NUMBER, null ),
      new ExcelField( "f2", IValueMeta.TYPE_STRING, null ) };
    meta.setOutputFields( excelFields );

    return meta;
  }

  @Test
  public void testExceptionClosingWorkbook() throws Exception {

    IValueMeta vmi = new ValueMetaString( "new_row" );

    ExcelOutputData data = new ExcelOutputData();
    data.fieldnrs = new int[] { 0 };
    RowMeta rowMetaToBeReturned = Mockito.spy( new RowMeta() );
    rowMetaToBeReturned.addValueMeta( 0, vmi );
    data.previousMeta = rowMetaToBeReturned;

    String excelFileFullPath = buildFilePath();
    File excelFile = new File( excelFileFullPath );
    excelFile.deleteOnExit();
    ExcelOutputMeta meta = createTransformMeta( excelFileFullPath, null, true );

    ExcelOutput excelOutput = Mockito.spy( new ExcelOutput( helper.transformMeta, meta, data, 0, helper.pipelineMeta, helper.pipeline ) );
    excelOutput.first = false;

    Object[] row = { new Date() };
    doReturn( row ).when( excelOutput ).getRow();
    doReturn( rowMetaToBeReturned ).when( excelOutput ).getInputRowMeta();

    excelOutput.init();
    excelOutput.processRow();
    excelOutput.dispose();

    excelOutput.init();
    excelOutput.processRow();
    excelOutput.dispose();

    Workbook workbook = Workbook.getWorkbook( excelFile );
    Assert.assertEquals( 1, workbook.getSheets().length );
    Assert.assertEquals( 2, workbook.getSheet( CREATED_SHEET_NAME ).getRows() );
  }

  @Test
  public void testClosingFile() throws Exception {

    IValueMeta vmi = new ValueMetaString( "new_row" );

    ExcelOutputData data = new ExcelOutputData();
    data.fieldnrs = new int[] { 0 };
    String testColumnName = "testColumnName";
    data.formats.put( testColumnName, new WritableCellFormat() );
    RowMeta rowMetaToBeReturned = Mockito.spy( new RowMeta() );
    rowMetaToBeReturned.addValueMeta( 0, vmi );

    String excelFileFullPath = buildFilePath();
    File excelFile = new File( excelFileFullPath );
    excelFile.deleteOnExit();
    ExcelOutputMeta meta = createTransformMeta( excelFileFullPath, null, true );
    meta.setSplitEvery( 1 );


    data.previousMeta = rowMetaToBeReturned;
    ExcelOutput excelOutput = Mockito.spy( new ExcelOutput( helper.transformMeta, meta, data, 0, helper.pipelineMeta, helper.pipeline ) );
    excelOutput.first = false;

    Object[] row = { new Date() };
    doReturn( row ).when( excelOutput ).getRow();
    doReturn( rowMetaToBeReturned ).when( excelOutput ).getInputRowMeta();
    doReturn( 1L ).when( excelOutput ).getLinesOutput();

    excelOutput.init();
    excelOutput.processRow();
    Assert.assertNull( data.formats.get( testColumnName ) );
  }

  @Test
  public void test_AppendNoTemplate() throws Exception {

    IValueMeta vmi = new ValueMetaString( "new_row" );

    ExcelOutputData data = new ExcelOutputData();
    data.fieldnrs = new int[] { 0 };
    RowMeta rowMetaToBeReturned = Mockito.spy( new RowMeta() );
    rowMetaToBeReturned.addValueMeta( 0, vmi );
    data.previousMeta = rowMetaToBeReturned;

    String excelFileFullPath = buildFilePath();
    File excelFile = new File( excelFileFullPath );
    excelFile.deleteOnExit();
    ExcelOutputMeta meta = createTransformMeta( excelFileFullPath, null, true );

    ExcelOutput excelOutput = Mockito.spy( new ExcelOutput( helper.transformMeta, meta, data, 0, helper.pipelineMeta, helper.pipeline ) );
    excelOutput.first = false;

    Object[] row = { new Date() };
    doReturn( row ).when( excelOutput ).getRow();
    doReturn( rowMetaToBeReturned ).when( excelOutput ).getInputRowMeta();

    excelOutput.init();
    excelOutput.processRow();
    excelOutput.dispose();
    excelOutput.init();
    excelOutput.processRow();
    excelOutput.dispose();
    excelOutput.init();
    excelOutput.processRow();
    excelOutput.dispose();
    excelOutput.init();
    excelOutput.processRow();
    excelOutput.dispose();

    Workbook workbook = Workbook.getWorkbook( excelFile );
    Assert.assertEquals( 1, workbook.getSheets().length );
    Assert.assertEquals( 4, workbook.getSheet( CREATED_SHEET_NAME ).getRows() );
  }

  @Test
  public void test_NoAppendNoTemplate() throws Exception {

    IValueMeta vmi = new ValueMetaString( "new_row" );

    ExcelOutputData data = new ExcelOutputData();
    data.fieldnrs = new int[] { 0 };
    RowMeta rowMetaToBeReturned = Mockito.spy( new RowMeta() );
    rowMetaToBeReturned.addValueMeta( 0, vmi );
    data.previousMeta = rowMetaToBeReturned;

    String excelFileFullPath = buildFilePath();
    File excelFile = new File( excelFileFullPath );
    excelFile.deleteOnExit();
    ExcelOutputMeta meta = createTransformMeta( excelFileFullPath, null, false );

    ExcelOutput excelOutput = Mockito.spy( new ExcelOutput( helper.transformMeta, meta, data, 0, helper.pipelineMeta, helper.pipeline ) );
    excelOutput.first = false;

    Object[] row = { new Date() };
    doReturn( row ).when( excelOutput ).getRow();
    doReturn( rowMetaToBeReturned ).when( excelOutput ).getInputRowMeta();

    excelOutput.init();
    excelOutput.processRow();
    excelOutput.dispose();
    excelOutput.init();
    excelOutput.processRow();
    excelOutput.dispose();
    excelOutput.init();
    excelOutput.processRow();
    excelOutput.dispose();
    excelOutput.init();
    excelOutput.processRow();
    excelOutput.dispose();

    Workbook workbook = Workbook.getWorkbook( excelFile );
    Assert.assertEquals( 1, workbook.getSheets().length );
    Assert.assertEquals( 1, workbook.getSheet( CREATED_SHEET_NAME ).getRows() );
  }

  @Test
  public void test_AppendTemplate() throws Exception {

    IValueMeta vmi = new ValueMetaString( "new_row" );

    ExcelOutputData data = new ExcelOutputData();
    data.fieldnrs = new int[] { 0 };
    RowMeta rowMetaToBeReturned = Mockito.spy( new RowMeta() );
    rowMetaToBeReturned.addValueMeta( 0, vmi );
    data.previousMeta = rowMetaToBeReturned;

    String excelFileFullPath = buildFilePath();
    File excelFile = new File( excelFileFullPath );
    excelFile.deleteOnExit();
    URL excelTemplateResource = this.getClass().getResource( "excel-template.xls" );
    String templateFullPath = excelTemplateResource.getFile();
    ExcelOutputMeta meta = createTransformMeta( excelFileFullPath, templateFullPath, true );

    ExcelOutput excelOutput = Mockito.spy( new ExcelOutput( helper.transformMeta, meta, data, 0, helper.pipelineMeta, helper.pipeline ) );
    excelOutput.first = false;

    Object[] row = { new Date() };
    doReturn( row ).when( excelOutput ).getRow();
    doReturn( rowMetaToBeReturned ).when( excelOutput ).getInputRowMeta();

    excelOutput.init();
    excelOutput.processRow();
    excelOutput.dispose();
    excelOutput.init();
    excelOutput.processRow();
    excelOutput.dispose();
    excelOutput.init();
    excelOutput.processRow();
    excelOutput.dispose();
    excelOutput.init();
    excelOutput.processRow();
    excelOutput.dispose();

    Workbook workbook = Workbook.getWorkbook( excelFile );
    Assert.assertEquals( 3, workbook.getSheets().length );
    // The existing sheets should be intact
    Assert.assertEquals( 4, workbook.getSheet( "SheetA" ).getRows() );
    Assert.assertEquals( 5, workbook.getSheet( "SheetB" ).getRows() );
    Assert.assertEquals( 4, workbook.getSheet( CREATED_SHEET_NAME ).getRows() );
  }

  @Test
  public void test_NoAppendTemplate() throws Exception {

    IValueMeta vmi = new ValueMetaString( "new_row" );

    ExcelOutputData data = new ExcelOutputData();
    data.fieldnrs = new int[] { 0 };
    RowMeta rowMetaToBeReturned = Mockito.spy( new RowMeta() );
    rowMetaToBeReturned.addValueMeta( 0, vmi );
    data.previousMeta = rowMetaToBeReturned;

    String excelFileFullPath = buildFilePath();
    File excelFile = new File( excelFileFullPath );
    excelFile.deleteOnExit();
    URL excelTemplateResource = this.getClass().getResource( "excel-template.xls" );
    String templateFullPath = excelTemplateResource.getFile();
    ExcelOutputMeta meta = createTransformMeta( excelFileFullPath, templateFullPath, false );

    ExcelOutput excelOutput = Mockito.spy( new ExcelOutput( helper.transformMeta, meta, data, 0, helper.pipelineMeta, helper.pipeline ) );
    excelOutput.first = false;

    Object[] row = { new Date() };
    doReturn( row ).when( excelOutput ).getRow();
    doReturn( rowMetaToBeReturned ).when( excelOutput ).getInputRowMeta();

    excelOutput.init();
    excelOutput.processRow();
    excelOutput.dispose();
    excelOutput.init();
    excelOutput.processRow();
    excelOutput.dispose();
    excelOutput.init();
    excelOutput.processRow();
    excelOutput.dispose();
    excelOutput.init();
    excelOutput.processRow();
    excelOutput.dispose();

    Workbook workbook = Workbook.getWorkbook( excelFile );
    Assert.assertEquals( 3, workbook.getSheets().length );
    // The existing sheets should be intact
    Assert.assertEquals( 4, workbook.getSheet( "SheetA" ).getRows() );
    Assert.assertEquals( 5, workbook.getSheet( "SheetB" ).getRows() );
    Assert.assertEquals( 1, workbook.getSheet( CREATED_SHEET_NAME ).getRows() );
  }

  @Test
  public void test_AppendTemplateWithSheet1() throws Exception {

    IValueMeta vmi = new ValueMetaString( "new_row" );

    ExcelOutputData data = new ExcelOutputData();
    data.fieldnrs = new int[] { 0 };
    RowMeta rowMetaToBeReturned = Mockito.spy( new RowMeta() );
    rowMetaToBeReturned.addValueMeta( 0, vmi );
    data.previousMeta = rowMetaToBeReturned;

    String excelFileFullPath = buildFilePath();
    File excelFile = new File( excelFileFullPath );
    excelFile.deleteOnExit();
    URL excelTemplateResource = this.getClass().getResource( "excel-template-withSheet1.xls" );
    String templateFullPath = excelTemplateResource.getFile();
    ExcelOutputMeta meta = createTransformMeta( excelFileFullPath, templateFullPath, true );

    ExcelOutput excelOutput =  Mockito.spy( new ExcelOutput( helper.transformMeta, meta, data, 0, helper.pipelineMeta, helper.pipeline ) );
    excelOutput.first = false;

    Object[] row = { new Date() };
    doReturn( row ).when( excelOutput ).getRow();
    doReturn( rowMetaToBeReturned ).when( excelOutput ).getInputRowMeta();

    excelOutput.init();
    excelOutput.processRow();
    excelOutput.dispose();
    excelOutput.init();
    excelOutput.processRow();
    excelOutput.dispose();
    excelOutput.init();
    excelOutput.processRow();
    excelOutput.dispose();
    excelOutput.init();
    excelOutput.processRow();
    excelOutput.dispose();

    Workbook workbook = Workbook.getWorkbook( excelFile );
    Assert.assertEquals( 3, workbook.getSheets().length );
    // The existing sheets should be intact
    Assert.assertEquals( 4, workbook.getSheet( "SheetA" ).getRows() );
    Assert.assertEquals( 5, workbook.getSheet( "SheetB" ).getRows() );
    // There're already 4 rows
    Assert.assertEquals( 8, workbook.getSheet( CREATED_SHEET_NAME ).getRows() );
  }

  @Test
  public void test_NoAppendTemplateWithSheet1() throws Exception {

    IValueMeta vmi = new ValueMetaString( "new_row" );

    ExcelOutputData data = new ExcelOutputData();
    data.fieldnrs = new int[] { 0 };
    RowMeta rowMetaToBeReturned = Mockito.spy( new RowMeta() );
    rowMetaToBeReturned.addValueMeta( 0, vmi );
    data.previousMeta = rowMetaToBeReturned;

    String excelFileFullPath = buildFilePath();
    File excelFile = new File( excelFileFullPath );
    excelFile.deleteOnExit();
    URL excelTemplateResource = this.getClass().getResource( "excel-template-withSheet1.xls" );
    String templateFullPath = excelTemplateResource.getFile();
    ExcelOutputMeta meta = createTransformMeta( excelFileFullPath, templateFullPath, false );

    ExcelOutput excelOutput = Mockito.spy( new ExcelOutput( helper.transformMeta, meta, data, 0, helper.pipelineMeta, helper.pipeline ) );
    excelOutput.first = false;

    Object[] row = { new Date() };
    doReturn( row ).when( excelOutput ).getRow();
    doReturn( rowMetaToBeReturned ).when( excelOutput ).getInputRowMeta();

    excelOutput.init();
    excelOutput.processRow();
    excelOutput.dispose();
    excelOutput.init();
    excelOutput.processRow();
    excelOutput.dispose();
    excelOutput.init();
    excelOutput.processRow();
    excelOutput.dispose();
    excelOutput.init();
    excelOutput.processRow();
    excelOutput.dispose();

    Workbook workbook = Workbook.getWorkbook( excelFile );
    Assert.assertEquals( 3, workbook.getSheets().length );
    // The existing sheets should be intact
    Assert.assertEquals( 4, workbook.getSheet( "SheetA" ).getRows() );
    Assert.assertEquals( 5, workbook.getSheet( "SheetB" ).getRows() );
    // There're already 4 rows
    Assert.assertEquals( 5, workbook.getSheet( CREATED_SHEET_NAME ).getRows() );
  }
}
