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

package org.apache.hop.pipeline.transforms.excelwriter;

import org.apache.hop.core.IRowSet;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaBigNumber;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.BorderStyle;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.DataFormat;
import org.apache.poi.ss.usermodel.FillPatternType;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.util.CellReference;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


/**
 * Tests for applying Format and Style from cell (from a template) when writing fields
 */
public class ExcelWriterTransform_StyleFormatTest {

  private TransformMockHelper<ExcelWriterTransformMeta, ExcelWriterTransformData> transformMockHelper;
  private ExcelWriterTransform transform;
  private ExcelWriterTransformMeta meta;
  private ExcelWriterTransformData data;
  private IRowMeta inputRowMeta;
  private IRowMeta outputRowMeta;
  private IRowSet inputRowSet;

  @Before
  /**
   * Get mock helper
   */
  public void setUp() throws Exception {
    transformMockHelper = new TransformMockHelper<>( "Excel Writer Style Format Test", ExcelWriterTransformMeta.class, ExcelWriterTransformData.class );
    when( transformMockHelper.logChannelFactory.create( any(), any( ILoggingObject.class ) ) ).thenReturn( transformMockHelper.iLogChannel );
    verify( transformMockHelper.iLogChannel, never() ).logError( anyString() );
    verify( transformMockHelper.iLogChannel, never() ).logError( anyString(), any( Object[].class ) );
    verify( transformMockHelper.iLogChannel, never() ).logError( anyString(), (Throwable) anyObject() );
    when( transformMockHelper.pipeline.isRunning() ).thenReturn( true );
  }

  @After
  /**
   * Clean-up objects
   */
  public void tearDown() {
    data.file = null;
    data.sheet = null;
    data.wb = null;
    data.clearStyleCache( 0 );

    transformMockHelper.cleanUp();
  }

  @Test
  /**
   * Test applying Format and Style from cell for XLS file format
   */
  public void testStyleFormatHssf() throws Exception {
    testStyleFormat( "xls" );
  }

  @Test
  /**
   * Test applying Format and Style from cell for XLSX file format
   */
  public void testStyleFormatXssf() throws Exception {
    testStyleFormat( "xlsx" );
  }

  /**
   * Test applying Format and Style from cell (from a template) when writing fields
   *
   * @param fileType
   * @throws Exception
   */
  private void testStyleFormat( String fileType ) throws Exception {
    setupInputOutput( fileType );
    createTransformMeta( fileType );
    createTransformData( fileType );
    setupTransformMock( fileType );
    transform.init();

    // We do not run pipeline or executing the whole transform
    // instead we just execute ExcelWriterTransformData.writeNextLine() to write to Excel workbook object
    // Values are written in A2:D2 and A3:D3 rows
    List<Object[]> rows = createRowData();
    for ( int i = 0; i < rows.size(); i++ ) {
      transform.writeNextLine( rows.get( i ) );
    }

    // Custom styles are loaded from G1 cell
    Row xlsRow = data.sheet.getRow( 0 );
    Cell baseCell = xlsRow.getCell( 6 );
    CellStyle baseCellStyle = baseCell.getCellStyle();
    DataFormat format = data.wb.createDataFormat();

    // Check style of the exported values in A3:D3
    xlsRow = data.sheet.getRow( 2 );
    for ( int i = 0; i < data.inputRowMeta.size(); i++ ) {
      Cell cell = xlsRow.getCell( i );
      CellStyle cellStyle = cell.getCellStyle();

      if ( i > 0 ) {
        assertEquals( cellStyle.getBorderRight(), baseCellStyle.getBorderRight() );
        assertEquals( cellStyle.getFillPattern(), baseCellStyle.getFillPattern() );
      } else {
        // cell A2/A3 has no custom style
        assertFalse( cellStyle.getBorderRight() == baseCellStyle.getBorderRight() );
        assertFalse( cellStyle.getFillPattern() == baseCellStyle.getFillPattern() );
      }

      if ( i != 1 ) {
        assertEquals( format.getFormat( cellStyle.getDataFormat() ), "0.00000" );
      } else {
        // cell B2/B3 use different format from the custom style
        assertEquals( format.getFormat( cellStyle.getDataFormat() ), "##0,000.0" );
      }
    }
  }

  /**
   * Setup any meta information for Excel Writer transform
   *
   * @param fileType
   * @throws HopException
   */
  private void createTransformMeta( String fileType ) throws HopException {
    meta = new ExcelWriterTransformMeta();
    meta.setDefault();

    meta.getFile().setFileName( "testExcel" );
    meta.getFile().setExtension( fileType );
    meta.getFile().setSheetname( "Sheet1" );
    meta.setHeaderEnabled( true );
    meta.setStartingCell( "A2" );

    // Try different combinations of specifying data format and style from cell
    //   1. Only format, no style
    //   2. No format, only style
    //   3. Format, and a different style without a format defined
    //   4. Format, and a different style with a different format defined but gets overridden
    List<ExcelWriterOutputField> outputFields = new ArrayList<>();
    ExcelWriterOutputField field = new ExcelWriterOutputField( "col 1", "Integer",  "0.00000" );
    field.setStyleCell( "" );
    outputFields.add(field);
    field = new ExcelWriterOutputField( "col 2", "Number", "" );
    field.setStyleCell( "G1" );
    outputFields.add(field);
    field = new ExcelWriterOutputField( "col 3",  "BigNumber" , "0.00000" );
    field.setStyleCell( "F1" );
    outputFields.add(field);
    field = new ExcelWriterOutputField( "col 4",  "Integer" , "0.00000" );
    field.setStyleCell( "G1" );
    outputFields.add(field);

    meta.setOutputFields( outputFields );
  }

  /**
   * Setup the data necessary for Excel Writer transform
   *
   * @param fileType
   * @throws HopException
   */
  private void createTransformData( String fileType ) throws HopException {
    data = new ExcelWriterTransformData();
    data.inputRowMeta = inputRowMeta.clone();
    data.outputRowMeta = inputRowMeta.clone();

    // we don't run pipeline so ExcelWriterTransform.processRow() doesn't get executed
    // we populate the ExcelWriterTransformData with bare minimum required values
    CellReference cellRef = new CellReference( meta.getStartingCell() );
    data.startingRow = cellRef.getRow();
    data.startingCol = cellRef.getCol();
    data.posX = data.startingCol;
    data.posY = data.startingRow;

    int numOfFields = data.inputRowMeta.size();
    data.fieldnrs = new int[ numOfFields ];
    data.linkfieldnrs = new int[ numOfFields ];
    data.commentfieldnrs = new int[ numOfFields ];
    for ( int i = 0; i < numOfFields; i++ ) {
      data.fieldnrs[ i ] = i;
      data.linkfieldnrs[ i ] = -1;
      data.commentfieldnrs[ i ] = -1;
    }

    // we avoid reading/writing Excel files, so ExcelWriterTransform.prepareNextOutputFile() doesn't get executed
    // create Excel workbook object
    data.wb = meta.getFile().getExtension().equalsIgnoreCase( "xlsx" ) ? new XSSFWorkbook() : new HSSFWorkbook();
    data.sheet = data.wb.createSheet();
    data.file = null;
    data.clearStyleCache( numOfFields );

    // we avoid reading template file from disk
    // so set beforehand cells with custom style and formatting
    DataFormat format = data.wb.createDataFormat();
    Row xlsRow = data.sheet.createRow( 0 );

    // Cell F1 has custom style applied, used as template
    Cell cell = xlsRow.createCell( 5 );
    CellStyle cellStyle = data.wb.createCellStyle();
    cellStyle.setBorderRight( BorderStyle.THICK );
    cellStyle.setFillPattern( FillPatternType.FINE_DOTS );
    cell.setCellStyle( cellStyle );

    // Cell G1 has same style, but also a custom data format
    cellStyle = data.wb.createCellStyle();
    cellStyle.cloneStyleFrom( cell.getCellStyle() );
    cell = xlsRow.createCell( 6 );
    cellStyle.setDataFormat( format.getFormat( "##0,000.0" ) );
    cell.setCellStyle( cellStyle );
  }

  private void setupInputOutput( String fileType) throws Exception {
    List<Object[]> rows = createRowData();
    String[] outFields = new String[] { "col 1", "col 2", "col 3", "col 4" };
    inputRowSet = transformMockHelper.getMockInputRowSet( rows );
    inputRowMeta = createRowMeta();
    inputRowSet.setRowMeta( inputRowMeta );
    outputRowMeta = mock( IRowMeta.class );
    when( outputRowMeta.size() ).thenReturn( outFields.length );
    when( inputRowSet.getRowMeta() ).thenReturn( inputRowMeta );
  }

  /**
   * Create ExcelWriterTransform object and mock some of its required data
   *
   * @param fileType
   * @throws Exception
   */
  private void setupTransformMock( String fileType ) throws Exception {
    transform = new ExcelWriterTransform( transformMockHelper.transformMeta, meta, data, 0, transformMockHelper.pipelineMeta, transformMockHelper.pipeline );
    transform.init();

    transform.addRowSetToInputRowSets( inputRowSet );
    transform.setInputRowMeta( inputRowMeta );
    transform.addRowSetToOutputRowSets( inputRowSet );
  }

  /**
   * Create data rows that are passed to Excel Writer transform
   *
   * @return
   * @throws Exception
   */
  private ArrayList<Object[]> createRowData() throws Exception {
    ArrayList<Object[]> rows = new ArrayList<>();
    Object[] row = new Object[] { new Long( 123456 ), new Double( 2.34e-4 ),
      new BigDecimal( "123456789.987654321" ), new Double( 504150 ) };
    rows.add( row );
    row = new Object[] { new Long( 1001001 ), new Double( 4.6789e10 ),
      new BigDecimal( 123123e-2 ), new Double( 12312300 ) };
    rows.add( row );
    return rows;
  }

  /**
   * Create meta information for rows that are passed to Excel Writer transform
   *
   * @return
   * @throws HopException
   */
  private IRowMeta createRowMeta() throws HopException {
    IRowMeta rm = new RowMeta();
    try {
      IValueMeta[] valuesMeta = {
        new ValueMetaInteger( "col 1" ),
        new ValueMetaNumber( "col 2" ),
        new ValueMetaBigNumber( "col 3" ),
        new ValueMetaNumber( "col 4" )
      };
      for ( int i = 0; i < valuesMeta.length; i++ ) {
        rm.addValueMeta( valuesMeta[ i ] );
      }
    } catch ( Exception ex ) {
      return null;
    }
    return rm;
  }
}
