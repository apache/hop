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

import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.Const;
import org.apache.hop.core.ResultFile;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.workarounds.BufferedOutputStreamWithCloseDetection;
import org.apache.poi.common.usermodel.HyperlinkType;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.ClientAnchor;
import org.apache.poi.ss.usermodel.Comment;
import org.apache.poi.ss.usermodel.CreationHelper;
import org.apache.poi.ss.usermodel.DataFormat;
import org.apache.poi.ss.usermodel.Drawing;
import org.apache.poi.ss.usermodel.Font;
import org.apache.poi.ss.usermodel.FormulaEvaluator;
import org.apache.poi.ss.usermodel.Hyperlink;
import org.apache.poi.ss.usermodel.IndexedColors;
import org.apache.poi.ss.usermodel.RichTextString;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.util.CellReference;
import org.apache.poi.xssf.streaming.SXSSFSheet;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;

public class ExcelWriterTransform extends BaseTransform<ExcelWriterTransformMeta, ExcelWriterTransformData> implements ITransform<ExcelWriterTransformMeta, ExcelWriterTransformData> {

  private static final Class<?> PKG = ExcelWriterTransformMeta.class; // For Translator

  public static final String STREAMER_FORCE_RECALC_PROP_NAME = "HOP_EXCEL_WRITER_STREAMER_FORCE_RECALCULATE";

  public ExcelWriterTransform( TransformMeta transformMeta, ExcelWriterTransformMeta meta, ExcelWriterTransformData data, int copyNr, PipelineMeta pipelineMeta, Pipeline pipeline ) {
    super( transformMeta, meta, data, copyNr, pipelineMeta, pipeline );
  }

  @Override
  public boolean processRow() throws HopException {

    // get next row
    Object[] r = getRow();

    // first row initialization
    if ( first ) {

      first = false;
      if ( r == null ) {
        data.outputRowMeta = new RowMeta();
        data.inputRowMeta = new RowMeta();
      } else {
        data.outputRowMeta = getInputRowMeta().clone();
        data.inputRowMeta = getInputRowMeta().clone();
      }

      // if we are supposed to init the file up front, here we go
      if ( !meta.getFile().isDoNotOpenNewFileInit() ) {
        data.firstFileOpened = true;

        try {
          prepareNextOutputFile();
        } catch ( HopException e ) {
          logError( BaseMessages.getString( PKG, "ExcelWriterTransform.Exception.CouldNotPrepareFile",
            resolve( meta.getFile().getFileName() ) ) );
          setErrors( 1L );
          stopAll();
          return false;
        }
      }

      if ( r != null ) {
        // if we are supposed to init the file delayed, here we go
        if ( meta.getFile().isDoNotOpenNewFileInit() ) {
          data.firstFileOpened = true;
          prepareNextOutputFile();
        }

        // Let's remember where the fields are in the input row
        int outputFieldsCount = meta.getOutputFields().size();
        data.commentauthorfieldnrs = new int[ outputFieldsCount ];
        data.commentfieldnrs = new int[ outputFieldsCount ];
        data.linkfieldnrs = new int[ outputFieldsCount ];
        data.fieldnrs = new int[ outputFieldsCount ];

        int i = 0;
        for ( ExcelWriterOutputField outputField : meta.getOutputFields() ) {
          // Output Fields
          String outputFieldName = outputField.getName();
          data.fieldnrs[ i ] = data.inputRowMeta.indexOfValue( outputFieldName );
          if ( data.fieldnrs[ i ] < 0 ) {
            logError( "Field [" + outputFieldName + "] couldn't be found in the input stream!" );
            setErrors( 1 );
            stopAll();
            return false;
          }

          // Comment Fields
          String commentField = outputField.getCommentField();
          data.commentfieldnrs[ i ] = data.inputRowMeta.indexOfValue( commentField );
          if ( data.commentfieldnrs[ i ] < 0 && !Utils.isEmpty( commentField ) ) {
            logError( "Comment Field ["
              + commentField + "] couldn't be found in the input stream!" );
            setErrors( 1 );
            stopAll();
            return false;
          }

          // Comment Author Fields
          String commentAuthorField = outputField.getCommentAuthorField();
          data.commentauthorfieldnrs[ i ] =
            data.inputRowMeta.indexOfValue( commentAuthorField );
          if ( data.commentauthorfieldnrs[ i ] < 0
            && !Utils.isEmpty( commentAuthorField ) ) {
            logError( "Comment Author Field ["
              + commentAuthorField + "] couldn't be found in the input stream!" );
            setErrors( 1 );
            stopAll();
            return false;
          }

          // Link Fields
          String hyperlinkField = outputField.getHyperlinkField();
          data.linkfieldnrs[ i ] = data.inputRowMeta.indexOfValue( hyperlinkField );
          if ( data.linkfieldnrs[ i ] < 0 && !Utils.isEmpty( hyperlinkField ) ) {
            logError( "Link Field ["
              + hyperlinkField + "] couldn't be found in the input stream!" );
            setErrors( 1 );
            stopAll();
            return false;
          }

          // Increase counter
          ++i;
        }
      }
    }

    if ( r != null ) {
      // File Splitting Feature, is it time to create a new file?
      if ( !meta.isAppendLines() && meta.getFile().getSplitEvery() > 0 && data.datalines > 0 && data.datalines % meta.getFile().getSplitEvery() == 0 ) {
        closeOutputFile();
        prepareNextOutputFile();
      }

      writeNextLine( r );
      incrementLinesOutput();

      data.datalines++;

      // pass on the row unchanged
      putRow( data.outputRowMeta, r );

      // Some basic logging
      if ( checkFeedback( getLinesOutput() ) ) {
        if ( log.isBasic() ) {
          logBasic( "Linenr " + getLinesOutput() );
        }
      }
      return true;
    } else {
      // after the last row, the (last) file is closed
      if ( data.wb != null ) {
        closeOutputFile();
      }
      setOutputDone();
      clearWorkbookMem();
      return false;
    }
  }

  // clears all memory that POI may hold
  private void clearWorkbookMem() {
    data.file = null;
    data.sheet = null;
    data.wb = null;
    data.clearStyleCache( 0 );

  }

  private void closeOutputFile() throws HopException {
    try ( BufferedOutputStreamWithCloseDetection out = new BufferedOutputStreamWithCloseDetection( HopVfs.getOutputStream( data.file, false ) ) ) {
      // may have to write a footer here
      if ( meta.isFooterEnabled() ) {
        writeHeader();
      }
      // handle auto size for columns
      if ( meta.getFile().isAutosizecolums() ) {

        // track all columns for autosizing if using streaming worksheet
        if ( data.sheet instanceof SXSSFSheet ) {
          ( (SXSSFSheet) data.sheet ).trackAllColumnsForAutoSizing();
        }

        if ( meta.getOutputFields() == null || meta.getOutputFields().size() == 0 ) {
          for ( int i = 0; i < data.inputRowMeta.size(); i++ ) {
            data.sheet.autoSizeColumn( i + data.startingCol );
          }
        } else {
          for ( int i = 0; i < meta.getOutputFields().size(); i++ ) {
            data.sheet.autoSizeColumn( i + data.startingCol );
          }
        }
      }
      // force recalculation of formulas if requested
      if ( meta.isForceFormulaRecalculation() ) {
        recalculateAllWorkbookFormulas();
      }

      data.wb.write( out );
    } catch ( IOException e ) {
      throw new HopException( e );
    }
  }

  // recalculates all formula fields for the entire workbook
  // package-local visibility for testing purposes
  void recalculateAllWorkbookFormulas() {
    if ( data.wb instanceof XSSFWorkbook ) {
      // XLSX needs full reevaluation
      FormulaEvaluator evaluator = data.wb.getCreationHelper().createFormulaEvaluator();
      for ( int sheetNum = 0; sheetNum < data.wb.getNumberOfSheets(); sheetNum++ ) {
        Sheet sheet = data.wb.getSheetAt( sheetNum );
        for ( Row r : sheet ) {
          for ( Cell c : r ) {
            if ( c.getCellType() == Cell.CELL_TYPE_FORMULA ) {
              evaluator.evaluateFormulaCell( c );
            }
          }
        }
      }
    } else if ( data.wb instanceof HSSFWorkbook ) {
      // XLS supports a "dirty" flag to have excel recalculate everything when a sheet is opened
      for ( int sheetNum = 0; sheetNum < data.wb.getNumberOfSheets(); sheetNum++ ) {
        HSSFSheet sheet = ( (HSSFWorkbook) data.wb ).getSheetAt( sheetNum );
        sheet.setForceFormulaRecalculation( true );
      }
    } else {
      String forceRecalc = getVariable( STREAMER_FORCE_RECALC_PROP_NAME, "N" );
      if ( "Y".equals( forceRecalc ) ) {
        data.wb.setForceFormulaRecalculation( true );
      }
    }
  }

  public void writeNextLine( Object[] r ) throws HopException {
    try {
      openLine();
      Row xlsRow = data.sheet.getRow( data.posY );
      if ( xlsRow == null ) {
        xlsRow = data.sheet.createRow( data.posY );
      }
      Object v = null;
      if ( meta.getOutputFields() == null || meta.getOutputFields().size() == 0 ) {
        //  Write all values in stream to text file.
        int nr = data.inputRowMeta.size();
        data.clearStyleCache( nr );
        data.linkfieldnrs = new int[ nr ];
        data.commentfieldnrs = new int[ nr ];
        for ( int i = 0; i < nr; i++ ) {
          v = r[ i ];
          writeField( v, data.inputRowMeta.getValueMeta( i ), null, xlsRow, data.posX++, r, i, false );
        }
        // go to the next line
        data.posX = data.startingCol;
        data.posY++;
      } else {
        /*
         * Only write the fields specified!
         */
        for ( int i = 0; i < meta.getOutputFields().size(); i++ ) {
          v = r[ data.fieldnrs[ i ] ];
          ExcelWriterOutputField field = meta.getOutputFields().get(i);
          writeField(
            v, data.inputRowMeta.getValueMeta( data.fieldnrs[ i ] ), field, xlsRow,
            data.posX++, r, i, false );
        }
        // go to the next line
        data.posX = data.startingCol;
        data.posY++;
      }
    } catch ( Exception e ) {
      logError( "Error writing line :" + e.toString() );
      throw new HopException( e );
    }
  }

  private Comment createCellComment( String author, String comment ) {
    // comments only supported for XLSX
    if ( data.sheet instanceof XSSFSheet ) {
      CreationHelper factory = data.wb.getCreationHelper();
      Drawing drawing = data.sheet.createDrawingPatriarch();

      ClientAnchor anchor = factory.createClientAnchor();
      Comment cmt = drawing.createCellComment( anchor );
      RichTextString str = factory.createRichTextString( comment );
      cmt.setString( str );
      cmt.setAuthor( author );
      return cmt;
    }
    return null;
  }

  /**
   * @param reference
   * @return the cell the reference points to
   */
  private Cell getCellFromReference( String reference ) {

    CellReference cellRef = new CellReference( reference );
    String sheetName = cellRef.getSheetName();

    Sheet sheet = data.sheet;
    if ( !Utils.isEmpty( sheetName ) ) {
      sheet = data.wb.getSheet( sheetName );
    }
    if ( sheet == null ) {
      return null;
    }
    // reference is assumed to be absolute
    Row xlsRow = sheet.getRow( cellRef.getRow() );
    if ( xlsRow == null ) {
      return null;
    }
    Cell styleCell = xlsRow.getCell( cellRef.getCol() );
    return styleCell;
  }

  //VisibleForTesting
  void writeField(Object v, IValueMeta vMeta, ExcelWriterOutputField excelField, Row xlsRow,
                  int posX, Object[] row, int fieldNr, boolean isTitle ) throws HopException {
    try {
      boolean cellExisted = true;
      // get the cell
      Cell cell = xlsRow.getCell( posX );
      if ( cell == null ) {
        cellExisted = false;
        cell = xlsRow.createCell( posX );
      }

      // if cell existed and existing cell's styles should not be changed, don't
      if ( !( cellExisted && meta.isLeaveExistingStylesUnchanged() ) ) {

        // if the style of this field is cached, reuse it
        if ( !isTitle && data.getCachedStyle( fieldNr ) != null ) {
          cell.setCellStyle( data.getCachedStyle( fieldNr ) );
        } else {
          // apply style if requested
          if ( excelField != null ) {

            // determine correct cell for title or data rows
            String styleRef = null;
            if ( !isTitle && !Utils.isEmpty( excelField.getStyleCell() ) ) {
              styleRef = excelField.getStyleCell();
            } else if ( isTitle && !Utils.isEmpty( excelField.getTitleStyleCell() ) ) {
              styleRef = excelField.getTitleStyleCell();
            }

            if ( styleRef != null ) {
              Cell styleCell = getCellFromReference( styleRef );
              if ( styleCell != null && cell != styleCell ) {
                cell.setCellStyle( styleCell.getCellStyle() );
              }
            }
          }

          // set cell format as specified, specific format overrides cell specification
          if ( !isTitle
            && excelField != null && !Utils.isEmpty( excelField.getFormat() )
            && !excelField.getFormat().startsWith( "Image" ) ) {
            setDataFormat( excelField.getFormat(), cell );
          }
          // cache it for later runs
          if ( !isTitle ) {
            data.cacheStyle( fieldNr, cell.getCellStyle() );
          }
        }
      }

      // create link on cell if requested
      if ( !isTitle && excelField != null && data.linkfieldnrs[ fieldNr ] >= 0 ) {
        String link = data.inputRowMeta.getValueMeta( data.linkfieldnrs[ fieldNr ] ).getString( row[ data.linkfieldnrs[ fieldNr ] ] );
        if ( !Utils.isEmpty( link ) ) {
          CreationHelper ch = data.wb.getCreationHelper();
          // set the link on the cell depending on link type
          Hyperlink hyperLink = null;
          if ( link.startsWith( "http:" ) || link.startsWith( "https:" ) || link.startsWith( "ftp:" ) ) {
            hyperLink = ch.createHyperlink( HyperlinkType.URL );
            hyperLink.setLabel( "URL Link" );
          } else if ( link.startsWith( "mailto:" ) ) {
            hyperLink = ch.createHyperlink( HyperlinkType.EMAIL );
            hyperLink.setLabel( "Email Link" );
          } else if ( link.startsWith( "'" ) ) {
            hyperLink = ch.createHyperlink( HyperlinkType.DOCUMENT );
            hyperLink.setLabel( "Link within this document" );
          } else {
            hyperLink = ch.createHyperlink( HyperlinkType.FILE );
            hyperLink.setLabel( "Link to a file" );
          }

          hyperLink.setAddress( link );
          cell.setHyperlink( hyperLink );

          // if cell existed and existing cell's styles should not be changed, don't
          if ( !( cellExisted && meta.isLeaveExistingStylesUnchanged() ) ) {

            if ( data.getCachedLinkStyle( fieldNr ) != null ) {
              cell.setCellStyle( data.getCachedLinkStyle( fieldNr ) );
            } else {
              Font origFont = data.wb.getFontAt( cell.getCellStyle().getFontIndex() );
              Font hlinkFont = data.wb.createFont();
              // reproduce original font characteristics

              hlinkFont.setBold( origFont.getBold() );
              hlinkFont.setCharSet( origFont.getCharSet() );
              hlinkFont.setFontHeight( origFont.getFontHeight() );
              hlinkFont.setFontName( origFont.getFontName() );
              hlinkFont.setItalic( origFont.getItalic() );
              hlinkFont.setStrikeout( origFont.getStrikeout() );
              hlinkFont.setTypeOffset( origFont.getTypeOffset() );
              // make it blue and underlined
              hlinkFont.setUnderline( Font.U_SINGLE );
              hlinkFont.setColor( IndexedColors.BLUE.getIndex() );
              CellStyle style = cell.getCellStyle();
              style.setFont( hlinkFont );
              cell.setCellStyle( style );
              data.cacheLinkStyle( fieldNr, cell.getCellStyle() );
            }
          }
        }
      }

      // create comment on cell if requested
      if ( !isTitle && excelField != null && data.commentfieldnrs[ fieldNr ] >= 0 && data.wb instanceof XSSFWorkbook ) {
        String comment = data.inputRowMeta.getValueMeta( data.commentfieldnrs[ fieldNr ] ).getString( row[ data.commentfieldnrs[ fieldNr ] ] );
        if ( !Utils.isEmpty( comment ) ) {
          String author = data.commentauthorfieldnrs[ fieldNr ] >= 0
            ? data.inputRowMeta.getValueMeta( data.commentauthorfieldnrs[ fieldNr ] ).getString( row[ data.commentauthorfieldnrs[ fieldNr ] ] ) : "Apache Hop";
          cell.setCellComment( createCellComment( author, comment ) );
        }
      }
      // cell is getting a formula value or static content
      if ( !isTitle && excelField != null && excelField.isFormula() ) {
        // formula case
        cell.setCellFormula( vMeta.getString( v ) );
      } else {
        // static content case
        switch ( vMeta.getType() ) {
          case IValueMeta.TYPE_DATE:
            if ( v != null && vMeta.getDate( v ) != null ) {
              cell.setCellValue( vMeta.getDate( v ) );
            }
            break;
          case IValueMeta.TYPE_BOOLEAN:
            if ( v != null ) {
              cell.setCellValue( vMeta.getBoolean( v ) );
            }
            break;
          case IValueMeta.TYPE_BIGNUMBER:
          case IValueMeta.TYPE_NUMBER:
          case IValueMeta.TYPE_INTEGER:
            if ( v != null ) {
              cell.setCellValue( vMeta.getNumber( v ) );
            }
            break;
          default:
            // fallthrough: output the data value as a string
            if ( v != null ) {
              cell.setCellValue( vMeta.getString( v ) );
            }
            break;
        }
      }
    } catch ( Exception e ) {
      logError( "Error writing field (" + data.posX + "," + data.posY + ") : " + e.toString() );
      logError( Const.getStackTracker( e ) );
      throw new HopException( e );
    }
  }

  /**
   * Set specified cell format
   *
   * @param excelFieldFormat the specified format
   * @param cell             the cell to set up format
   */
  private void setDataFormat( String excelFieldFormat, Cell cell ) {
    if ( log.isDebug() ) {
      logDebug( BaseMessages.getString( PKG, "ExcelWriterTransform.Log.SetDataFormat", excelFieldFormat, CellReference.convertNumToColString( cell.getColumnIndex() ), cell.getRowIndex() ) );
    }

    DataFormat format = data.wb.createDataFormat();
    short formatIndex = format.getFormat( excelFieldFormat );
    CellStyle style = data.wb.createCellStyle();
    style.cloneStyleFrom( cell.getCellStyle() );
    style.setDataFormat( formatIndex );
    cell.setCellStyle( style );
  }

  /**
   * Returns the output filename that belongs to this transform observing the file split feature
   *
   * @return current output filename to write to
   */
  public String buildFilename( int splitNr ) {
    return meta.buildFilename( this, getCopy(), splitNr );
  }

  /**
   * Copies a VFS File
   *
   * @param in  the source file object
   * @param out the destination file object
   * @throws HopException
   */
  public static void copyFile( FileObject in, FileObject out ) throws HopException {
    try ( BufferedInputStream fis = new BufferedInputStream( HopVfs.getInputStream( in ) );
          BufferedOutputStream fos = new BufferedOutputStream( HopVfs.getOutputStream( out, false ) ) ) {
      byte[] buf = new byte[ 1024 * 1024 ]; // copy in chunks of 1 MB
      int i = 0;
      while ( ( i = fis.read( buf ) ) != -1 ) {
        fos.write( buf, 0, i );
      }
    } catch ( Exception e ) {
      throw new HopException( e );
    }
  }

  public void prepareNextOutputFile() throws HopException {
    try {
      // sheet name shouldn't exceed 31 character
      if ( data.realSheetname != null && data.realSheetname.length() > 31 ) {
        throw new HopException( BaseMessages.getString( PKG, "ExcelWriterTransform.Exception.MaxSheetName", data.realSheetname ) );
      }
      // clear style cache
      int numOfFields = meta.getOutputFields() != null && meta.getOutputFields().size() > 0 ? meta.getOutputFields().size() : 0;
      if ( numOfFields == 0 ) {
        numOfFields = data.inputRowMeta != null ? data.inputRowMeta.size() : 0;
      }
      data.clearStyleCache( numOfFields );

      // build new filename
      String buildFilename = buildFilename( data.splitnr );

      data.file = HopVfs.getFileObject( buildFilename );

      if ( log.isDebug() ) {
        logDebug( BaseMessages.getString( PKG, "ExcelWriterTransform.Log.OpeningFile", buildFilename ) );
      }

      // determine whether existing file must be deleted
      if ( data.file.exists() && data.createNewFile ) {
        if ( !data.file.delete() ) {
          if ( log.isBasic() ) {
            logBasic( BaseMessages.getString( PKG, "ExcelWriterTransform.Log.CouldNotDeleteStaleFile", buildFilename ) );
          }
          setErrors( 1 );
          throw new HopException( "Could not delete stale file " + buildFilename );
        }
      }

      // adding filename to result
      if ( meta.isAddToResultFilenames() ) {
        // Add this to the result file names...
        ResultFile resultFile = new ResultFile( ResultFile.FILE_TYPE_GENERAL, data.file, getPipelineMeta().getName(), getTransformName() );
        resultFile.setComment( "This file was created with an Excel writer transform by Hop : The Hop Orchestration Platform" );
        addResultFile( resultFile );
      }
      boolean appendingToSheet = true;
      // if now no file exists we must create it as indicated by user
      if ( !data.file.exists() ) {
        // if template file is enabled
        if ( meta.getTemplate().isTemplateEnabled() ) {
          // handle template case (must have same format)
          // ensure extensions match
          String templateExt = HopVfs.getFileObject( data.realTemplateFileName ).getName().getExtension();
          if ( !meta.getFile().getExtension().equalsIgnoreCase( templateExt ) ) {
            throw new HopException( "Template Format Mismatch: Template has extension: "
              + templateExt + ", but output file has extension: " + meta.getFile().getExtension()
              + ". Template and output file must share the same format!" );
          }

          if ( HopVfs.getFileObject( data.realTemplateFileName ).exists() ) {
            // if the template exists just copy the template in place
            copyFile( HopVfs.getFileObject( data.realTemplateFileName ), data.file );
          } else {
            // template is missing, log it and get out
            if ( log.isBasic() ) {
              logBasic( BaseMessages.getString( PKG, "ExcelWriterTransform.Log.TemplateMissing", data.realTemplateFileName ) );
            }
            setErrors( 1 );
            throw new HopException( "Template file missing: " + data.realTemplateFileName );
          }
        } else {
          // handle fresh file case, just create a fresh workbook
          Workbook wb = meta.getFile().getExtension().equalsIgnoreCase( "xlsx" ) ? new XSSFWorkbook() : new HSSFWorkbook();
          BufferedOutputStreamWithCloseDetection out = new BufferedOutputStreamWithCloseDetection( HopVfs.getOutputStream( data.file, false ) );
          wb.createSheet( data.realSheetname );
          wb.write( out );
          out.close();
          wb.close();
        }
        appendingToSheet = false;
      }

      // file is guaranteed to be in place now
      if ( meta.getFile().getExtension().equalsIgnoreCase( "xlsx" ) ) {
        XSSFWorkbook xssfWorkbook = new XSSFWorkbook( HopVfs.getInputStream( data.file ) );
        if ( meta.getFile().isStreamingData() && !meta.getTemplate().isTemplateEnabled() ) {
          data.wb = new SXSSFWorkbook( xssfWorkbook, 100 );
        } else {
          //Initialize it later after writing header/template because SXSSFWorkbook can't read/rewrite existing data,
          // only append.
          data.wb = xssfWorkbook;
        }
      } else {
        data.wb = new HSSFWorkbook( HopVfs.getInputStream( data.file ) );
      }

      int existingActiveSheetIndex = data.wb.getActiveSheetIndex();
      int replacingSheetAt = -1;

      if ( data.wb.getSheet( data.realSheetname ) != null ) {
        // sheet exists, replace or reuse as indicated by user
        if ( data.createNewSheet ) {
          replacingSheetAt = data.wb.getSheetIndex( data.wb.getSheet( data.realSheetname ) );
          data.wb.removeSheetAt( replacingSheetAt );
        }
      }

      // if sheet is now missing, we need to create a new one
      if ( data.wb.getSheet( data.realSheetname ) == null ) {
        if ( meta.getTemplate().isTemplateSheetEnabled() ) {
          Sheet ts = data.wb.getSheet( data.realTemplateSheetName );
          // if template sheet is missing, break
          if ( ts == null ) {
            throw new HopException( BaseMessages.getString( PKG, "ExcelWriterTransform.Exception.TemplateNotFound", data.realTemplateSheetName ) );
          }
          data.sheet = data.wb.cloneSheet( data.wb.getSheetIndex( ts ) );
          data.wb.setSheetName( data.wb.getSheetIndex( data.sheet ), data.realSheetname );
          // unhide sheet in case it was hidden
          data.wb.setSheetHidden( data.wb.getSheetIndex( data.sheet ), false );
          if ( meta.getTemplate().isTemplateSheetHidden() ) {
            data.wb.setSheetHidden( data.wb.getSheetIndex( ts ), true );
          }
        } else {
          // no template to use, simply create a new sheet
          data.sheet = data.wb.createSheet( data.realSheetname );
        }
        if ( replacingSheetAt > -1 ) {
          data.wb.setSheetOrder( data.sheet.getSheetName(), replacingSheetAt );
        }
        // preserves active sheet selection in workbook
        data.wb.setActiveSheet( existingActiveSheetIndex );
        data.wb.setSelectedTab( existingActiveSheetIndex );
        appendingToSheet = false;
      } else {
        // sheet is there and should be reused
        data.sheet = data.wb.getSheet( data.realSheetname );
      }
      // if use chose to make the current sheet active, do so
      if ( meta.isMakeSheetActive() ) {
        int sheetIndex = data.wb.getSheetIndex( data.sheet );
        data.wb.setActiveSheet( sheetIndex );
        data.wb.setSelectedTab( sheetIndex );
      }
      // handle write protection
      if ( meta.getFile().isProtectsheet() ) {
        protectSheet( data.sheet, data.realPassword );
      }

      // starting cell support
      if ( !Utils.isEmpty( data.realStartingCell ) ) {
        CellReference cellRef = new CellReference( data.realStartingCell );
        data.startingRow = cellRef.getRow();
        data.startingCol = cellRef.getCol();
      } else {
        data.startingRow = 0;
        data.startingCol = 0;
      }

      data.posX = data.startingCol;
      data.posY = data.startingRow;

      // Find last row and append accordingly
      if ( !data.createNewSheet && meta.isAppendLines() && appendingToSheet ) {
        if ( data.sheet.getPhysicalNumberOfRows() > 0 ) {
          data.posY = data.sheet.getLastRowNum() + 1;
        } else {
          data.posY = 0;
        }
      }

      // offset by configured value
      // Find last row and append accordingly
      if ( !data.createNewSheet && meta.getAppendOffset() != 0 && appendingToSheet ) {
        data.posY += meta.getAppendOffset();
      }

      // may have to write a few empty lines
      if ( !data.createNewSheet && meta.getAppendEmpty() > 0 && appendingToSheet ) {
        for ( int i = 0; i < meta.getAppendEmpty(); i++ ) {
          openLine();
          if ( !data.shiftExistingCells || meta.isAppendLines() ) {
            data.posY++;
          }
        }
      }

      // may have to write a header here
      if ( meta.isHeaderEnabled() && !( !data.createNewSheet && meta.isAppendOmitHeader() && appendingToSheet ) ) {
        writeHeader();
      }
      if ( log.isDebug() ) {
        logDebug( BaseMessages.getString( PKG, "ExcelWriterTransform.Log.FileOpened", buildFilename ) );
      }
      // this is the number of the new output file
      data.splitnr++;
    } catch ( Exception e ) {
      logError( "Error opening new file", e );
      setErrors( 1 );
      throw new HopException( e );
    }
  }

  private void openLine() {
    if ( data.shiftExistingCells ) {
      data.sheet.shiftRows( data.posY, Math.max( data.posY, data.sheet.getLastRowNum() ), 1 );
    }
  }

  private void writeHeader() throws HopException {
    try {
      openLine();
      Row xlsRow = data.sheet.getRow( data.posY );
      if ( xlsRow == null ) {
        xlsRow = data.sheet.createRow( data.posY );
      }
      int posX = data.posX;
      // If we have fields specified: list them in this order!
      if ( meta.getOutputFields() != null && meta.getOutputFields().size() > 0 ) {
        for ( int i = 0; i < meta.getOutputFields().size(); i++ ) {
          ExcelWriterOutputField field = meta.getOutputFields().get(i);
          String fieldName = !Utils.isEmpty(field.getTitle() ) ? field.getTitle() : field.getName();
          IValueMeta vMeta = new ValueMetaString( fieldName );
          writeField( fieldName, vMeta, field, xlsRow, posX++, null, -1, true );
        }
        // Just put all field names in
      } else if ( data.inputRowMeta != null ) {
        for ( int i = 0; i < data.inputRowMeta.size(); i++ ) {
          String fieldName = data.inputRowMeta.getFieldNames()[ i ];
          IValueMeta vMeta = new ValueMetaString( fieldName );
          writeField( fieldName, vMeta, null, xlsRow, posX++, null, -1, true );
        }
      }
      data.posY++;
      incrementLinesOutput();
    } catch ( Exception e ) {
      throw new HopException( e );
    }
  }

  @Override
  public boolean init(){
    if ( super.init() ) {
      data.splitnr = 0;
      data.datalines = 0;
      data.realSheetname = resolve( meta.getFile().getSheetname() );
      data.realTemplateSheetName = resolve( meta.getTemplate().getTemplateSheetName() );
      data.realTemplateFileName = resolve( meta.getTemplate().getTemplateFileName() );
      data.realStartingCell = resolve( meta.getStartingCell() );
      data.realPassword = Utils.resolvePassword( variables, meta.getFile().getPassword() );
      data.realProtectedBy = resolve( meta.getFile().getProtectedBy() );

      data.shiftExistingCells = ExcelWriterTransformMeta.ROW_WRITE_PUSH_DOWN.equals( meta.getRowWritingMethod() );
      data.createNewSheet = ExcelWriterTransformMeta.IF_SHEET_EXISTS_CREATE_NEW.equals( meta.getFile().getIfSheetExists() );
      data.createNewFile = ExcelWriterTransformMeta.IF_FILE_EXISTS_CREATE_NEW.equals( meta.getFile().getIfFileExists() );
      return true;
    }
    return false;
  }

  /**
   * pipeline run end
   *
   */
  @Override
  public void dispose(){
    clearWorkbookMem();
    super.dispose();
  }

  /**
   * Write protect Sheet by setting password works only for xls output at the moment
   */
  protected void protectSheet( Sheet sheet, String password ) {
    if ( sheet instanceof HSSFSheet ) {
      // Write protect Sheet by setting password
      // works only for xls output at the moment
      sheet.protectSheet( password );
    }
  }
}
