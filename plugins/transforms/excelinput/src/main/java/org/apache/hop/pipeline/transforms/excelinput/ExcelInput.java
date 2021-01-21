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

package org.apache.hop.pipeline.transforms.excelinput;

import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.Const;
import org.apache.hop.core.ResultFile;
import org.apache.hop.core.IRowSet;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopFileException;
import org.apache.hop.core.fileinput.FileInputList;
import org.apache.hop.core.playlist.FilePlayListAll;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.spreadsheet.IKCell;
import org.apache.hop.core.spreadsheet.KCellType;
import org.apache.hop.core.spreadsheet.IKSheet;
import org.apache.hop.core.util.EnvUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.errorhandling.CompositeFileErrorHandler;
import org.apache.hop.pipeline.transform.errorhandling.IFileErrorHandler;
import org.apache.hop.pipeline.transform.errorhandling.FileErrorHandlerContentLineNumber;
import org.apache.hop.pipeline.transform.errorhandling.FileErrorHandlerMissingFiles;
import org.apache.poi.openxml4j.util.ZipSecureFile;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

/**
 * This class reads data from one or more Microsoft Excel files.
 *
 * @author Matt
 * @author timh
 * @since 19-NOV-2003
 */
public class ExcelInput extends BaseTransform<ExcelInputMeta, ExcelInputData> implements ITransform<ExcelInputMeta, ExcelInputData> {

  private static final Class<?> PKG = ExcelInputMeta.class; // For Translator

  public ExcelInput( TransformMeta transformMeta, ExcelInputMeta meta, ExcelInputData data, int copyNr, PipelineMeta pipelineMeta,
                     Pipeline pipeline ) {
    super( transformMeta, meta, data, copyNr, pipelineMeta, pipeline );
    setZipBombConfiguration();
  }

  /**
   * Build an empty row based on the meta-data...
   *
   * @return
   */

  private Object[] fillRow( int startcolumn, ExcelInputRow excelInputRow ) throws HopException {
    Object[] r = new Object[ data.outputRowMeta.size() ];

    // Keep track whether or not we handled an error for this line yet.
    boolean errorHandled = false;

    // Set values in the row...
    IKCell cell = null;

    for ( int i = startcolumn; i < excelInputRow.cells.length && i - startcolumn < meta.getField().length; i++ ) {
      cell = excelInputRow.cells[ i ];

      int rowcolumn = i - startcolumn;

      if ( cell == null ) {
        r[ rowcolumn ] = null;
        continue;
      }

      IValueMeta targetMeta = data.outputRowMeta.getValueMeta( rowcolumn );
      IValueMeta sourceMeta = null;

      try {
        checkType( cell, targetMeta );
      } catch ( HopException ex ) {
        if ( !meta.isErrorIgnored() ) {
          ex = new HopCellValueException( ex, this.data.sheetnr, this.data.rownr, i, "" );
          throw ex;
        }
        if ( log.isBasic() ) {
          logBasic( BaseMessages.getString( PKG, "ExcelInput.Log.WarningProcessingExcelFile", "" + targetMeta, ""
            + data.filename, ex.getMessage() ) );
        }

        if ( !errorHandled ) {
          data.errorHandler.handleLineError( excelInputRow.rownr, excelInputRow.sheetName );
          errorHandled = true;
        }

        if ( meta.isErrorLineSkipped() ) {
          return null;
        }
      }

      KCellType cellType = cell.getType();
      if ( KCellType.BOOLEAN == cellType || KCellType.BOOLEAN_FORMULA == cellType ) {
        r[ rowcolumn ] = cell.getValue();
        sourceMeta = data.valueMetaBoolean;
      } else {
        if ( KCellType.DATE.equals( cellType ) || KCellType.DATE_FORMULA.equals( cellType ) ) {
          Date date = (Date) cell.getValue();
          long time = date.getTime();
          int offset = TimeZone.getDefault().getOffset( time );
          r[ rowcolumn ] = new Date( time - offset );
          sourceMeta = data.valueMetaDate;
        } else {
          if ( KCellType.LABEL == cellType || KCellType.STRING_FORMULA == cellType ) {
            String string = (String) cell.getValue();
            switch ( meta.getField()[ rowcolumn ].getTrimType() ) {
              case ExcelInputMeta.TYPE_TRIM_LEFT:
                string = Const.ltrim( string );
                break;
              case ExcelInputMeta.TYPE_TRIM_RIGHT:
                string = Const.rtrim( string );
                break;
              case ExcelInputMeta.TYPE_TRIM_BOTH:
                string = Const.trim( string );
                break;
              default:
                break;
            }
            r[ rowcolumn ] = string;
            sourceMeta = data.valueMetaString;
          } else {
            if ( KCellType.NUMBER == cellType || KCellType.NUMBER_FORMULA == cellType ) {
              r[ rowcolumn ] = cell.getValue();
              sourceMeta = data.valueMetaNumber;
            } else {
              if ( log.isDetailed() ) {
                KCellType ct = cell.getType();
                logDetailed( BaseMessages.getString( PKG, "ExcelInput.Log.UnknownType", ( ( ct != null ) ? ct
                  .toString() : "null" ), cell.getContents() ) );
              }
              r[ rowcolumn ] = null;
            }
          }
        }
      }

      ExcelInputField field = meta.getField()[ rowcolumn ];

      // Change to the appropriate type if needed...
      //
      try {
        // Null stays null folks.
        //
        if ( sourceMeta != null && sourceMeta.getType() != targetMeta.getType() && r[ rowcolumn ] != null ) {
          IValueMeta sourceMetaCopy = sourceMeta.clone();
          sourceMetaCopy.setConversionMask( field.getFormat() );
          sourceMetaCopy.setGroupingSymbol( field.getGroupSymbol() );
          sourceMetaCopy.setDecimalSymbol( field.getDecimalSymbol() );
          sourceMetaCopy.setCurrencySymbol( field.getCurrencySymbol() );

          switch ( targetMeta.getType() ) {
            // Use case: we find a numeric value: convert it using the supplied format to the desired data type...
            //
            case IValueMeta.TYPE_NUMBER:
            case IValueMeta.TYPE_INTEGER:
              switch ( field.getType() ) {
                case IValueMeta.TYPE_DATE:
                  // number to string conversion (20070522.00 --> "20070522")
                  //
                  IValueMeta valueMetaNumber = new ValueMetaNumber( "num" );
                  valueMetaNumber.setConversionMask( "#" );
                  Object string = sourceMetaCopy.convertData( valueMetaNumber, r[ rowcolumn ] );

                  // String to date with mask...
                  //
                  r[ rowcolumn ] = targetMeta.convertData( sourceMetaCopy, string );
                  break;
                default:
                  r[ rowcolumn ] = targetMeta.convertData( sourceMetaCopy, r[ rowcolumn ] );
                  break;
              }
              break;
            // Use case: we find a date: convert it using the supplied format to String...
            //
            default:
              r[ rowcolumn ] = targetMeta.convertData( sourceMetaCopy, r[ rowcolumn ] );
          }
        }
      } catch ( HopException ex ) {
        if ( !meta.isErrorIgnored() ) {
          ex = new HopCellValueException( ex, this.data.sheetnr, cell.getRow(), i, field.getName() );
          throw ex;
        }
        if ( log.isBasic() ) {
          logBasic( BaseMessages.getString( PKG, "ExcelInput.Log.WarningProcessingExcelFile", "" + targetMeta, ""
            + data.filename, ex.toString() ) );
        }
        if ( !errorHandled ) {
          // check if we didn't log an error already for this one.
          data.errorHandler.handleLineError( excelInputRow.rownr, excelInputRow.sheetName );
          errorHandled = true;
        }

        if ( meta.isErrorLineSkipped() ) {
          return null;
        } else {
          r[ rowcolumn ] = null;
        }
      }
    }

    int rowIndex = meta.getField().length;

    // Do we need to include the filename?
    if ( !Utils.isEmpty( meta.getFileField() ) ) {
      r[ rowIndex ] = data.filename;
      rowIndex++;
    }

    // Do we need to include the sheetname?
    if ( !Utils.isEmpty( meta.getSheetField() ) ) {
      r[ rowIndex ] = excelInputRow.sheetName;
      rowIndex++;
    }

    // Do we need to include the sheet rownumber?
    if ( !Utils.isEmpty( meta.getSheetRowNumberField() ) ) {
      r[ rowIndex ] = new Long( data.rownr );
      rowIndex++;
    }

    // Do we need to include the rownumber?
    if ( !Utils.isEmpty( meta.getRowNumberField() ) ) {
      r[ rowIndex ] = new Long( getLinesWritten() + 1 );
      rowIndex++;
    }
    // Possibly add short filename...
    if ( !Utils.isEmpty( meta.getShortFileNameField() ) ) {
      r[ rowIndex ] = data.shortFilename;
      rowIndex++;
    }
    // Add Extension
    if ( !Utils.isEmpty( meta.getExtensionField() ) ) {
      r[ rowIndex ] = data.extension;
      rowIndex++;
    }
    // add path
    if ( !Utils.isEmpty( meta.getPathField() ) ) {
      r[ rowIndex ] = data.path;
      rowIndex++;
    }
    // Add Size
    if ( !Utils.isEmpty( meta.getSizeField() ) ) {
      r[ rowIndex ] = new Long( data.size );
      rowIndex++;
    }
    // add Hidden
    if ( !Utils.isEmpty( meta.isHiddenField() ) ) {
      r[ rowIndex ] = new Boolean( data.hidden );
      rowIndex++;
    }
    // Add modification date
    if ( !Utils.isEmpty( meta.getLastModificationDateField() ) ) {
      r[ rowIndex ] = data.lastModificationDateTime;
      rowIndex++;
    }
    // Add Uri
    if ( !Utils.isEmpty( meta.getUriField() ) ) {
      r[ rowIndex ] = data.uriName;
      rowIndex++;
    }
    // Add RootUri
    if ( !Utils.isEmpty( meta.getRootUriField() ) ) {
      r[ rowIndex ] = data.rootUriName;
      rowIndex++;
    }

    return r;
  }

  private void checkType( IKCell cell, IValueMeta v ) throws HopException {
    if ( !meta.isStrictTypes() ) {
      return;
    }
    switch ( cell.getType() ) {
      case BOOLEAN:
        if ( !( v.getType() == IValueMeta.TYPE_STRING || v.getType() == IValueMeta.TYPE_NONE || v
          .getType() == IValueMeta.TYPE_BOOLEAN ) ) {
          throw new HopException( BaseMessages.getString( PKG, "ExcelInput.Exception.InvalidTypeBoolean", v
            .getTypeDesc() ) );
        }
        break;

      case DATE:
        if ( !( v.getType() == IValueMeta.TYPE_STRING || v.getType() == IValueMeta.TYPE_NONE || v
          .getType() == IValueMeta.TYPE_DATE ) ) {
          throw new HopException( BaseMessages.getString( PKG, "ExcelInput.Exception.InvalidTypeDate", cell
            .getContents(), v.getTypeDesc() ) );
        }
        break;

      case LABEL:
        if ( v.getType() == IValueMeta.TYPE_BOOLEAN
          || v.getType() == IValueMeta.TYPE_DATE || v.getType() == IValueMeta.TYPE_INTEGER
          || v.getType() == IValueMeta.TYPE_NUMBER ) {
          throw new HopException( BaseMessages.getString( PKG, "ExcelInput.Exception.InvalidTypeLabel", cell
            .getContents(), v.getTypeDesc() ) );
        }
        break;

      case EMPTY:
        // OK
        break;

      case NUMBER:
        if ( !( v.getType() == IValueMeta.TYPE_STRING
          || v.getType() == IValueMeta.TYPE_NONE || v.getType() == IValueMeta.TYPE_INTEGER
          || v.getType() == IValueMeta.TYPE_BIGNUMBER || v.getType() == IValueMeta.TYPE_NUMBER ) ) {
          throw new HopException( BaseMessages.getString( PKG, "ExcelInput.Exception.InvalidTypeNumber", cell
            .getContents(), v.getTypeDesc() ) );
        }
        break;

      default:
        throw new HopException( BaseMessages.getString( PKG, "ExcelInput.Exception.UnsupportedType", cell
          .getType().getDescription(), cell.getContents() ) );
    }
  }

  public boolean processRow() throws HopException {

    if ( first ) {
      first = false;

      data.outputRowMeta = new RowMeta(); // start from scratch!
      meta.getFields( data.outputRowMeta, getTransformName(), null, null, this, metadataProvider );

      if ( meta.isAcceptingFilenames() ) {
        // Read the files from the specified input stream...
        data.files.getFiles().clear();

        int idx = -1;
        IRowSet rowSet = findInputRowSet( meta.getAcceptingTransformName() );
        Object[] fileRow = getRowFrom( rowSet );
        while ( fileRow != null ) {
          if ( idx < 0 ) {
            idx = rowSet.getRowMeta().indexOfValue( meta.getAcceptingField() );
            if ( idx < 0 ) {
              logError( BaseMessages.getString( PKG, "ExcelInput.Error.FilenameFieldNotFound", ""
                + meta.getAcceptingField() ) );

              setErrors( 1 );
              stopAll();
              return false;
            }
          }
          String fileValue = rowSet.getRowMeta().getString( fileRow, idx );
          try {
            data.files.addFile( HopVfs.getFileObject( fileValue ) );
          } catch ( HopFileException e ) {
            throw new HopException( BaseMessages.getString(
              PKG, "ExcelInput.Exception.CanNotCreateFileObject", fileValue ), e );
          }

          // Grab another row
          fileRow = getRowFrom( rowSet );
        }
      }

      handleMissingFiles();
    }

    // See if we're not done processing...
    // We are done processing if the filenr >= number of files.
    if ( data.filenr >= data.files.nrOfFiles() ) {
      if ( log.isDetailed() ) {
        logDetailed( BaseMessages.getString( PKG, "ExcelInput.Log.NoMoreFiles", "" + data.filenr ) );
      }

      setOutputDone(); // signal end to receiver(s)
      return false; // end of data or error.
    }

    if ( meta.getRowLimit() > 0 && getLinesInput() >= meta.getRowLimit() ) {
      // The close of the openFile is in dispose()
      if ( log.isDetailed() ) {
        logDetailed( BaseMessages.getString( PKG, "ExcelInput.Log.RowLimitReached", "" + meta.getRowLimit() ) );
      }

      setOutputDone(); // signal end to receiver(s)
      return false; // end of data or error.
    }

    Object[] r = getRowFromWorkbooks();
    if ( r != null ) {
      incrementLinesInput();

      // OK, see if we need to repeat values.
      if ( data.previousRow != null ) {
        for ( int i = 0; i < meta.getField().length; i++ ) {
          IValueMeta valueMeta = data.outputRowMeta.getValueMeta( i );
          Object valueData = r[ i ];

          if ( valueMeta.isNull( valueData ) && meta.getField()[ i ].isRepeated() ) {
            // Take the value from the previous row.
            r[ i ] = data.previousRow[ i ];
          }
        }
      }

      // Remember this row for the next time around!
      data.previousRow = data.outputRowMeta.cloneRow( r );

      // Send out the good news: we found a row of data!
      putRow( data.outputRowMeta, r );

      return true;
    } else {
      // This row is ignored / eaten
      // We continue though.
      return true;
    }
  }

  private void handleMissingFiles() throws HopException {
    List<FileObject> nonExistantFiles = data.files.getNonExistantFiles();

    if ( !nonExistantFiles.isEmpty() ) {
      String message = FileInputList.getRequiredFilesDescription( nonExistantFiles );
      if ( log.isBasic() ) {
        logBasic( BaseMessages.getString( PKG, "ExcelInput.Log.RequiredFilesTitle" ), BaseMessages.getString(
          PKG, "ExcelInput.Warning.MissingFiles", message ) );
      }

      if ( meta.isErrorIgnored() ) {
        for ( FileObject fileObject : nonExistantFiles ) {
          data.errorHandler.handleNonExistantFile( fileObject );
        }
      } else {
        throw new HopException( BaseMessages.getString(
          PKG, "ExcelInput.Exception.MissingRequiredFiles", message ) );
      }
    }

    List<FileObject> nonAccessibleFiles = data.files.getNonAccessibleFiles();
    if ( !nonAccessibleFiles.isEmpty() ) {
      String message = FileInputList.getRequiredFilesDescription( nonAccessibleFiles );
      if ( log.isBasic() ) {
        logBasic( BaseMessages.getString( PKG, "ExcelInput.Log.RequiredFilesTitle" ), BaseMessages.getString(
          PKG, "ExcelInput.Log.RequiredFilesMsgNotAccessible", message ) );
      }

      if ( meta.isErrorIgnored() ) {
        for ( FileObject fileObject : nonAccessibleFiles ) {
          data.errorHandler.handleNonAccessibleFile( fileObject );
        }
      } else {
        throw new HopException( BaseMessages.getString(
          PKG, "ExcelInput.Exception.RequiredFilesNotAccessible", message ) );
      }
    }
  }

  public Object[] getRowFromWorkbooks() {
    // This procedure outputs a single Excel data row on the destination
    // rowsets...

    Object[] retval = null;

    try {
      // First, see if a file has been opened?
      if ( data.workbook == null ) {
        // Open a new openFile..
        data.file = data.files.getFile( data.filenr );
        data.filename = HopVfs.getFilename( data.file );
        // Add additional fields?
        if ( meta.getShortFileNameField() != null && meta.getShortFileNameField().length() > 0 ) {
          data.shortFilename = data.file.getName().getBaseName();
        }
        if ( meta.getPathField() != null && meta.getPathField().length() > 0 ) {
          data.path = HopVfs.getFilename( data.file.getParent() );
        }
        if ( meta.isHiddenField() != null && meta.isHiddenField().length() > 0 ) {
          data.hidden = data.file.isHidden();
        }
        if ( meta.getExtensionField() != null && meta.getExtensionField().length() > 0 ) {
          data.extension = data.file.getName().getExtension();
        }
        if ( meta.getLastModificationDateField() != null && meta.getLastModificationDateField().length() > 0 ) {
          data.lastModificationDateTime = new Date( data.file.getContent().getLastModifiedTime() );
        }
        if ( meta.getUriField() != null && meta.getUriField().length() > 0 ) {
          data.uriName = data.file.getName().getURI();
        }
        if ( meta.getRootUriField() != null && meta.getRootUriField().length() > 0 ) {
          data.rootUriName = data.file.getName().getRootURI();
        }
        if ( meta.getSizeField() != null && meta.getSizeField().length() > 0 ) {
          data.size = data.file.getContent().getSize();
        }

        if ( meta.isAddResultFile() ) {
          ResultFile resultFile =
            new ResultFile( ResultFile.FILE_TYPE_GENERAL, data.file, getPipelineMeta().getName(), toString() );
          resultFile.setComment( BaseMessages.getString( PKG, "ExcelInput.Log.FileReadByTransform" ) );
          addResultFile( resultFile );
        }

        if ( log.isDetailed() ) {
          logDetailed( BaseMessages.getString( PKG, "ExcelInput.Log.OpeningFile", ""
            + data.filenr + " : " + data.filename ) );
        }

        data.workbook = WorkbookFactory.getWorkbook( meta.getSpreadSheetType(), data.filename, meta.getEncoding() );

        data.errorHandler.handleFile( data.file );
        // Start at the first sheet again...
        data.sheetnr = 0;

        // See if we have sheet names to retrieve, otherwise we'll have to get all sheets...
        //
        if ( meta.readAllSheets() ) {
          data.sheetNames = data.workbook.getSheetNames();
          data.startColumn = new int[ data.sheetNames.length ];
          data.startRow = new int[ data.sheetNames.length ];
          for ( int i = 0; i < data.sheetNames.length; i++ ) {
            data.startColumn[ i ] = data.defaultStartColumn;
            data.startRow[ i ] = data.defaultStartRow;
          }
        }
      }

      boolean nextsheet = false;

      // What sheet were we handling?
      if ( log.isDebug() ) {
        logDetailed( BaseMessages
          .getString( PKG, "ExcelInput.Log.GetSheet", "" + data.filenr + "." + data.sheetnr ) );
      }

      String sheetName = data.sheetNames[ data.sheetnr ];
      IKSheet sheet = data.workbook.getSheet( sheetName );
      if ( sheet != null ) {
        // at what row do we continue reading?
        if ( data.rownr < 0 ) {
          data.rownr = data.startRow[ data.sheetnr ];

          // Add an extra row if we have a header row to skip...
          if ( meta.startsWithHeader() ) {
            data.rownr++;
          }
        }
        // Start at the specified column
        data.colnr = data.startColumn[ data.sheetnr ];

        // Build a new row and fill in the data from the sheet...
        try {
          IKCell[] line = sheet.getRow( data.rownr );
          // Already increase cursor 1 row
          int lineNr = ++data.rownr;
          // Excel starts counting at 0
          if ( !data.filePlayList.isProcessingNeeded( data.file, lineNr, sheetName ) ) {
            retval = null; // placeholder, was already null
          } else {
            if ( log.isRowLevel() ) {
              logRowlevel( BaseMessages.getString( PKG, "ExcelInput.Log.GetLine", "" + lineNr, data.filenr
                + "." + data.sheetnr ) );
            }

            if ( log.isRowLevel() ) {
              logRowlevel( BaseMessages.getString( PKG, "ExcelInput.Log.ReadLineWith", "" + line.length ) );
            }

            ExcelInputRow excelInputRow = new ExcelInputRow( sheet.getName(), lineNr, line );
            Object[] r = fillRow( data.colnr, excelInputRow );
            if ( log.isRowLevel() ) {
              logRowlevel( BaseMessages.getString(
                PKG, "ExcelInput.Log.ConvertedLinToRow", "" + lineNr, data.outputRowMeta.getString( r ) ) );
            }

            boolean isEmpty = isLineEmpty( line );
            if ( !isEmpty || !meta.ignoreEmptyRows() ) {
              // Put the row
              retval = r;
            } else {
              if ( data.rownr > sheet.getRows() ) {
                nextsheet = true;
              }
            }

            if ( isEmpty && meta.stopOnEmpty() ) {
              nextsheet = true;
            }
          }
        } catch ( ArrayIndexOutOfBoundsException e ) {
          if ( log.isRowLevel() ) {
            logRowlevel( BaseMessages.getString( PKG, "ExcelInput.Log.OutOfIndex" ) );
          }

          // We tried to read below the last line in the sheet.
          // Go to the next sheet...
          nextsheet = true;
        }
      } else {
        nextsheet = true;
      }

      if ( nextsheet ) {
        // Go to the next sheet
        data.sheetnr++;

        // Reset the start-row:
        data.rownr = -1;

        // no previous row yet, don't take it from the previous sheet!
        // (that would be plain wrong!)
        data.previousRow = null;

        // Perhaps it was the last sheet?
        if ( data.sheetnr >= data.sheetNames.length ) {
          jumpToNextFile();
        }
      }
    } catch ( Exception e ) {
      logError( BaseMessages.getString( PKG, "ExcelInput.Error.ProcessRowFromExcel", data.filename + "", e
        .toString() ), e );

      setErrors( 1 );
      stopAll();
      return null;
    }

    return retval;
  }

  private boolean isLineEmpty( IKCell[] line ) {
    if ( line.length == 0 ) {
      return true;
    }

    boolean isEmpty = true;
    for ( int i = 0; i < line.length && isEmpty; i++ ) {
      if ( line[ i ] != null && !Utils.isEmpty( line[ i ].getContents() ) ) {
        isEmpty = false;
      }
    }
    return isEmpty;
  }

  private void jumpToNextFile() throws HopException {
    data.sheetnr = 0;

    // Reset the start-row:
    data.rownr = -1;

    // no previous row yet, don't take it from the previous sheet! (that
    // whould be plain wrong!)
    data.previousRow = null;

    // Close the openFile!
    data.workbook.close();
    data.workbook = null; // marker to open again.
    data.errorHandler.close();

    // advance to the next file!
    data.filenr++;
  }

  private void initErrorHandling() {
    List<IFileErrorHandler> errorHandlers = new ArrayList<>( 2 );

    if ( meta.getLineNumberFilesDestinationDirectory() != null ) {
      errorHandlers.add( new FileErrorHandlerContentLineNumber(
        getPipeline().getExecutionStartDate(), resolve( meta.getLineNumberFilesDestinationDirectory() ),
        meta.getLineNumberFilesExtension(), "Latin1", this ) );
    }
    if ( meta.getErrorFilesDestinationDirectory() != null ) {
      errorHandlers.add( new FileErrorHandlerMissingFiles(
        getPipeline().getExecutionStartDate(), resolve( meta.getErrorFilesDestinationDirectory() ), meta
        .getErrorFilesExtension(), "Latin1", this ) );
    }
    data.errorHandler = new CompositeFileErrorHandler( errorHandlers );
  }

  private void initReplayFactory() {
    data.filePlayList = FilePlayListAll.INSTANCE;
  }

  /**
   * This method is responsible for setting the configuration values that control how the ZipSecureFile class behaves
   * when trying to detect zipbombs (check PDI-17586 for more details).
   */
  protected void setZipBombConfiguration() {

    // The minimum allowed ratio between de- and inflated bytes to detect a zipbomb.
    String minInflateRatioVariable =
      EnvUtil
        .getSystemProperty( Const.HOP_ZIP_MIN_INFLATE_RATIO, Const.HOP_ZIP_MIN_INFLATE_RATIO_DEFAULT_STRING );
    double minInflateRatio;
    try {
      minInflateRatio = Double.parseDouble( minInflateRatioVariable );
    } catch ( NullPointerException | NumberFormatException e ) {
      minInflateRatio = Const.HOP_ZIP_MIN_INFLATE_RATIO_DEFAULT;
    }
    ZipSecureFile.setMinInflateRatio( minInflateRatio );

    // The maximum file size of a single zip entry.
    String maxEntrySizeVariable =
      EnvUtil.getSystemProperty( Const.HOP_ZIP_MAX_ENTRY_SIZE, Const.HOP_ZIP_MAX_ENTRY_SIZE_DEFAULT_STRING );
    long maxEntrySize;
    try {
      maxEntrySize = Long.parseLong( maxEntrySizeVariable );
    } catch ( NullPointerException | NumberFormatException e ) {
      maxEntrySize = Const.HOP_ZIP_MAX_ENTRY_SIZE_DEFAULT;
    }
    ZipSecureFile.setMaxEntrySize( maxEntrySize );

    // The maximum number of characters of text that are extracted before an exception is thrown during extracting
    // text from documents.
    String maxTextSizeVariable =
      EnvUtil.getSystemProperty( Const.HOP_ZIP_MAX_TEXT_SIZE, Const.HOP_ZIP_MAX_TEXT_SIZE_DEFAULT_STRING );
    long maxTextSize;
    try {
      maxTextSize = Long.parseLong( maxTextSizeVariable );
    } catch ( NullPointerException | NumberFormatException e ) {
      maxTextSize = Const.HOP_ZIP_MAX_TEXT_SIZE_DEFAULT;
    }
    ZipSecureFile.setMaxTextSize( maxTextSize );
  }

  public boolean init(){

    if ( super.init() ) {
      initErrorHandling();
      initReplayFactory();
      data.files = meta.getFileList( this );
      if ( data.files.nrOfFiles() == 0 && data.files.nrOfMissingFiles() > 0 && !meta.isAcceptingFilenames() ) {

        logError( BaseMessages.getString( PKG, "ExcelInput.Error.NoFileSpecified" ) );
        return false;
      }

      if ( meta.getEmptyFields().size() > 0 ) {
        // Determine the maximum filename length...
        data.maxfilelength = -1;

        for ( FileObject file : data.files.getFiles() ) {
          String name = HopVfs.getFilename( file );
          if ( name.length() > data.maxfilelength ) {
            data.maxfilelength = name.length();
          }
        }

        // Determine the maximum sheet name length...
        data.maxsheetlength = -1;
        if ( !meta.readAllSheets() ) {
          data.sheetNames = new String[ meta.getSheetName().length ];
          data.startColumn = new int[ meta.getSheetName().length ];
          data.startRow = new int[ meta.getSheetName().length ];
          for ( int i = 0; i < meta.getSheetName().length; i++ ) {
            data.sheetNames[ i ] = meta.getSheetName()[ i ];
            data.startColumn[ i ] = meta.getStartColumn()[ i ];
            data.startRow[ i ] = meta.getStartRow()[ i ];

            if ( meta.getSheetName()[ i ].length() > data.maxsheetlength ) {
              data.maxsheetlength = meta.getSheetName()[ i ].length();
            }
          }
        } else {
          // Allocated at open file time: we want ALL sheets.
          if ( meta.getStartRow().length == 1 ) {
            data.defaultStartRow = meta.getStartRow()[ 0 ];
          } else {
            data.defaultStartRow = 0;
          }
          if ( meta.getStartColumn().length == 1 ) {
            data.defaultStartColumn = meta.getStartColumn()[ 0 ];
          } else {
            data.defaultStartColumn = 0;
          }
        }

        return true;
      } else {
        logError( BaseMessages.getString( PKG, "ExcelInput.Error.NotInputFieldsDefined" ) );
      }
    }
    return false;
  }

  public void dispose(){

    if ( data.workbook != null ) {
      data.workbook.close();
    }
    if ( data.file != null ) {
      try {
        data.file.close();
      } catch ( Exception e ) {
        // Ignore close errors
      }
    }
    try {
      data.errorHandler.close();
    } catch ( HopException e ) {
      if ( log.isDebug() ) {
        logDebug( BaseMessages.getString( PKG, "ExcelInput.Error.CouldNotCloseErrorHandler", e.toString() ) );

        logDebug( Const.getStackTracker( e ) );
      }
    }
    super.dispose();
  }
}
