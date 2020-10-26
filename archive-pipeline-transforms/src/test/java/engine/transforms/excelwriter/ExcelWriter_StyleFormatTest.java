/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.pipeline.transforms.excelwriter;

import org.apache.hop.core.RowSet;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.LoggingObjectInterface;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
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
public class ExcelWriter_StyleFormatTest {

  private TransformMockHelper<ExcelWriterMeta, ExcelWriterData> transformMockHelper;
  private ExcelWriter transform;
  private ExcelWriterMeta transformMeta;
  private ExcelWriterData transformData;

  @Before
  /**
   * Get mock helper
   */
  public void setUp() throws Exception {
    transformMockHelper =
      new TransformMockHelper<ExcelWriterMeta, ExcelWriterData>(
        "Excel Writer Style Format Test", ExcelWriterMeta.class, ExcelWriterData.class );
    when( transformMockHelper.logChannelFactory.create( any(), any( LoggingObjectInterface.class ) ) ).thenReturn(
      transformMockHelper.logChannelInterface );
    verify( transformMockHelper.logChannelInterface, never() ).logError( anyString() );
    verify( transformMockHelper.logChannelInterface, never() ).logError( anyString(), any( Object[].class ) );
    verify( transformMockHelper.logChannelInterface, never() ).logError( anyString(), (Throwable) anyObject() );
    when( transformMockHelper.pipeline.isRunning() ).thenReturn( true );
  }

  @After
  /**
   * Clean-up objects
   */
  public void tearDown() {
    transformData.file = null;
    transformData.sheet = null;
    transformData.wb = null;
    transformData.clearStyleCache( 0 );

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
    setupTransformMock( fileType );
    createTransformMeta( fileType );
    createTransformData( fileType );
    transform.init();

    // We do not run pipeline or executing the whole transform
    // instead we just execute ExcelWriterData.writeNextLine() to write to Excel workbook object
    // Values are written in A2:D2 and A3:D3 rows
    List<Object[]> rows = createRowData();
    for ( int i = 0; i < rows.size(); i++ ) {
      transform.writeNextLine( rows.get( i ) );
    }

    // Custom styles are loaded from G1 cell
    Row xlsRow = transformData.sheet.getRow( 0 );
    Cell baseCell = xlsRow.getCell( 6 );
    CellStyle baseCellStyle = baseCell.getCellStyle();
    DataFormat format = transformData.wb.createDataFormat();

    // Check style of the exported values in A3:D3
    xlsRow = transformData.sheet.getRow( 2 );
    for ( int i = 0; i < transformData.inputRowMeta.size(); i++ ) {
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
    transformMeta = new ExcelWriterTransformMeta();
    transformMeta.setDefault();

    transformMeta.setFileName( "testExcel" );
    transformMeta.setExtension( fileType );
    transformMeta.setSheetname( "Sheet1" );
    transformMeta.setHeaderEnabled( true );
    transformMeta.setStartingCell( "A2" );

    // Try different combinations of specifying data format and style from cell
    //   1. Only format, no style
    //   2. No format, only style
    //   3. Format, and a different style without a format defined
    //   4. Format, and a different style with a different format defined but gets overridden
    ExcelWriterField[] outputFields = new ExcelWriterField[ 4 ];
    outputFields[ 0 ] = new ExcelWriterField( "col 1", ValueMetaFactory.getIdForValueMeta( "Integer" ), "0.00000" );
    outputFields[ 0 ].setStyleCell( "" );
    outputFields[ 1 ] = new ExcelWriterField( "col 2", ValueMetaFactory.getIdForValueMeta( "Number" ), "" );
    outputFields[ 1 ].setStyleCell( "G1" );
    outputFields[ 2 ] = new ExcelWriterField( "col 3", ValueMetaFactory.getIdForValueMeta( "BigNumber" ), "0.00000" );
    outputFields[ 2 ].setStyleCell( "F1" );
    outputFields[ 3 ] = new ExcelWriterField( "col 4", ValueMetaFactory.getIdForValueMeta( "Integer" ), "0.00000" );
    outputFields[ 3 ].setStyleCell( "G1" );
    transformMeta.setOutputFields( outputFields );
  }

  /**
   * Setup the data necessary for Excel Writer transform
   *
   * @param fileType
   * @throws HopException
   */
  private void createTransformData( String fileType ) throws HopException {
    transformData = new ExcelWriterData();
    transformData.inputRowMeta = transform.getInputRowMeta().clone();
    transformData.outputRowMeta = transform.getInputRowMeta().clone();

    // we don't run pipeline so ExcelWriter.processRow() doesn't get executed
    // we populate the ExcelWriterData with bare minimum required values
    //
    CellReference cellRef = new CellReference( transformMeta.getStartingCell() );
    transformData.startingRow = cellRef.getRow();
    transformData.startingCol = cellRef.getCol();
    transformData.posX = transformData.startingCol;
    transformData.posY = transformData.startingRow;

    int numOfFields = transformData.inputRowMeta.size();
    transformData.fieldnrs = new int[ numOfFields ];
    transformData.linkfieldnrs = new int[ numOfFields ];
    transformData.commentfieldnrs = new int[ numOfFields ];
    for ( int i = 0; i < numOfFields; i++ ) {
      transformData.fieldnrs[ i ] = i;
      transformData.linkfieldnrs[ i ] = -1;
      transformData.commentfieldnrs[ i ] = -1;
    }

    // we avoid reading/writing Excel files, so ExcelWriter.prepareNextOutputFile() doesn't get executed
    // create Excel workbook object
    //
    transformData.wb = transformMeta.getExtension().equalsIgnoreCase( "xlsx" ) ? new XSSFWorkbook() : new HSSFWorkbook();
    transformData.sheet = transformData.wb.createSheet();
    transformData.file = null;
    transformData.clearStyleCache( numOfFields );

    // we avoid reading template file from disk
    // so set beforehand cells with custom style and formatting
    DataFormat format = transformData.wb.createDataFormat();
    Row xlsRow = transformData.sheet.createRow( 0 );

    // Cell F1 has custom style applied, used as template
    Cell cell = xlsRow.createCell( 5 );
    CellStyle cellStyle = transformData.wb.createCellStyle();
    cellStyle.setBorderRight( BorderStyle.THICK );
    cellStyle.setFillPattern( FillPatternType.FINE_DOTS );
    cell.setCellStyle( cellStyle );

    // Cell G1 has same style, but also a custom data format
    cellStyle = transformData.wb.createCellStyle();
    cellStyle.cloneStyleFrom( cell.getCellStyle() );
    cell = xlsRow.createCell( 6 );
    cellStyle.setDataFormat( format.getFormat( "##0,000.0" ) );
    cell.setCellStyle( cellStyle );
  }

  /**
   * Create ExcelWriter object and mock some of its required data
   *
   * @param fileType
   * @throws Exception
   */
  private void setupTransformMock( String fileType ) throws Exception {
    transform =
      new ExcelWriterTransform(
        transformMockHelper.transformMeta, transformMockHelper.iTransformMeta, transformMockHelper.iTransformData, 0, transformMockHelper.pipelineMeta, transformMockHelper.pipeline );
    transform.init();

    List<Object[]> rows = createRowData();
    String[] outFields = new String[] { "col 1", "col 2", "col 3", "col 4" };
    RowSet inputRowSet = transformMockHelper.getMockInputRowSet( rows );
    IRowMeta inputRowMeta = createRowMeta();
    inputRowSet.setRowMeta( inputRowMeta );
    IRowMeta mockOutputRowMeta = mock( IRowMeta.class );
    when( mockOutputRowMeta.size() ).thenReturn( outFields.length );
    when( inputRowSet.getRowMeta() ).thenReturn( inputRowMeta );

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
    ArrayList<Object[]> rows = new ArrayList<Object[]>();
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
