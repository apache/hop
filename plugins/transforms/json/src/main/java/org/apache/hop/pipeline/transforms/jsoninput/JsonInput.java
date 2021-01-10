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

package org.apache.hop.pipeline.transforms.jsoninput;

import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.hop.core.QueueRowSet;
import org.apache.hop.core.ResultFile;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.file.BaseFileInputTransform;
import org.apache.hop.pipeline.transforms.file.IBaseFileInputReader;
import org.apache.hop.pipeline.transforms.jsoninput.exception.JsonInputException;
import org.apache.hop.pipeline.transforms.jsoninput.reader.FastJsonReader;
import org.apache.hop.pipeline.transforms.jsoninput.reader.InputsReader;
import org.apache.hop.pipeline.transforms.jsoninput.reader.RowOutputConverter;
import org.apache.poi.util.IOUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.BitSet;

/**
 * Read Json files, parse them and convert them to rows and writes these to one or more output streams.
 *
 * @author Samatar
 * @author edube
 * @author jadametz
 * @since 20-06-2010
 */
public class JsonInput extends BaseFileInputTransform<JsonInputMeta, JsonInputData> implements ITransform<JsonInputMeta, JsonInputData> {
  private static final Class<?> PKG = JsonInputMeta.class; // For Translator

  private RowOutputConverter rowOutputConverter;

  private static final byte[] EMPTY_JSON = "{}".getBytes(); // for replacing null inputs

  public JsonInput( TransformMeta transformMeta, JsonInputMeta meta, JsonInputData data, int copyNr, PipelineMeta pipelineMeta, Pipeline pipeline ) {
    super( transformMeta, meta, data, copyNr, pipelineMeta, pipeline );
  }

  @Override
  public boolean init() {
    data.rownr = 1L;
    data.nrInputFields = meta.getInputFields().length;
    data.repeatedFields = new BitSet( data.nrInputFields );
    // Take care of variable substitution
    for ( int i = 0; i < data.nrInputFields; i++ ) {
      JsonInputField field = meta.getInputFields()[ i ];
      if ( field.isRepeated() ) {
        data.repeatedFields.set( i );
      }
    }
    try {
      // Init a new JSON reader
      createReader();
    } catch ( HopException e ) {
      logError( e.getMessage() );
      return false;
    }
    return true;
  }

  @Override
  public boolean processRow() throws HopException {
    if ( first ) {
      first = false;
      prepareToRowProcessing();
    }

    Object[] outRow;
    try {
      // Grab a row
      outRow = getOneOutputRow();
      if ( outRow == null ) {
        setOutputDone(); // signal end to receiver(s)
        return false; // end of data or error.
      }

      if ( log.isRowLevel() ) {
        logRowlevel( BaseMessages.getString( PKG, "JsonInput.Log.ReadRow", data.outputRowMeta.getString( outRow ) ) );
      }
      incrementLinesInput();
      data.rownr++;

      putRow( data.outputRowMeta, outRow ); // copy row to output rowset(s);

      if ( meta.getRowLimit() > 0 && data.rownr > meta.getRowLimit() ) {
        // limit has been reached: stop now.
        setOutputDone();
        return false;
      }

    } catch ( JsonInputException e ) {
      if ( !getTransformMeta().isDoingErrorHandling() ) {
        stopErrorExecution( e );
        return false;
      }
    } catch ( Exception e ) {
      logError( BaseMessages.getString( PKG, "JsonInput.ErrorInTransformRunning", e.getMessage() ) );
      if ( getTransformMeta().isDoingErrorHandling() ) {
        sendErrorRow( e.toString() );
      } else {
        incrementErrors();
        stopErrorExecution( e );
        return false;
      }
    }
    return true;
  }

  private void stopErrorExecution( Exception e ) {
    stopAll();
    setOutputDone();
  }

  @Override
  protected void prepareToRowProcessing() throws HopException, HopTransformException, HopValueException {
    if ( !meta.isInFields() ) {
      data.outputRowMeta = new RowMeta();
      if ( !meta.isDoNotFailIfNoFile() && (data.files==null || data.files.nrOfFiles() == 0) ) {
        String errMsg = BaseMessages.getString( PKG, "JsonInput.Log.NoFiles" );
        logError( errMsg );
        inputError( errMsg );
      }
    } else {
      data.readrow = getRow();
      data.inputRowMeta = getInputRowMeta();
      if ( data.inputRowMeta == null ) {
        data.hasFirstRow = false;
        return;
      }
      data.hasFirstRow = true;
      data.outputRowMeta = data.inputRowMeta.clone();

      // Check if source field is provided
      if ( Utils.isEmpty( meta.getFieldValue() ) ) {
        logError( BaseMessages.getString( PKG, "JsonInput.Log.NoField" ) );
        throw new HopException( BaseMessages.getString( PKG, "JsonInput.Log.NoField" ) );
      }

      // cache the position of the field
      if ( data.indexSourceField < 0 ) {
        data.indexSourceField = getInputRowMeta().indexOfValue( meta.getFieldValue() );
        if ( data.indexSourceField < 0 ) {
          logError( BaseMessages.getString( PKG, "JsonInput.Log.ErrorFindingField", meta.getFieldValue() ) );
          throw new HopException( BaseMessages.getString( PKG, "JsonInput.Exception.CouldnotFindField",
            meta.getFieldValue() ) );
        }
      }

      // if RemoveSourceField option is set, we remove the source field from the output meta
      if ( meta.isRemoveSourceField() ) {
        data.outputRowMeta.removeValueMeta( data.indexSourceField );
        // Get total previous fields minus one since we remove source field
        data.totalpreviousfields = data.inputRowMeta.size() - 1;
      } else {
        // Get total previous fields
        data.totalpreviousfields = data.inputRowMeta.size();
      }
    }
    meta.getFields( data.outputRowMeta, getTransformName(), null, null, this, metadataProvider );

    // Create convert meta-data objects that will contain Date & Number formatters
    data.convertRowMeta = data.outputRowMeta.cloneToType( IValueMeta.TYPE_STRING );
    data.inputs = new InputsReader( this, meta, data, new InputErrorHandler() ).iterator();
    // data.recordnr = 0;
    data.readerRowSet = new QueueRowSet();
    data.readerRowSet.setDone();
    this.rowOutputConverter = new RowOutputConverter( getLogChannel() );
  }

  private void addFileToResultFilesname( FileObject file ) {
    if ( meta.addResultFile() ) {
      // Add this to the result file names...
      ResultFile resultFile =
        new ResultFile( ResultFile.FILE_TYPE_GENERAL, file, getPipelineMeta().getName(), getTransformName() );
      resultFile.setComment( BaseMessages.getString( PKG, "JsonInput.Log.FileAddedResult" ) );
      addResultFile( resultFile );
    }
  }

  public boolean onNewFile( FileObject file ) throws FileSystemException {
    if ( file == null ) {
      String errMsg = BaseMessages.getString( PKG, "JsonInput.Log.IsNotAFile", "null" );
      logError( errMsg );
      inputError( errMsg );
      return false;
    } else if ( !file.exists() ) {
      String errMsg = BaseMessages.getString( PKG, "JsonInput.Log.IsNotAFile", file.getName().getFriendlyURI() );
      logError( errMsg );
      inputError( errMsg );
      return false;
    }
    if ( hasAdditionalFileFields() ) {
      fillFileAdditionalFields( data, file );
    }
    if ( file.getContent().getSize() == 0 ) {
      // log only basic as a warning (was before logError)
      if ( meta.isIgnoreEmptyFile() ) {
        logBasic( BaseMessages.getString( PKG, "JsonInput.Error.FileSizeZero", "" + file.getName() ) );
      } else {
        logError( BaseMessages.getString( PKG, "JsonInput.Error.FileSizeZero", "" + file.getName() ) );
        incrementErrors();
        return false;
      }
    }
    return true;
  }

  @Override
  protected void fillFileAdditionalFields( JsonInputData data, FileObject file ) throws FileSystemException {
    super.fillFileAdditionalFields( data, file );
    data.filename = HopVfs.getFilename( file );
    data.filenr++;
    if ( log.isDetailed() ) {
      logDetailed( BaseMessages.getString( PKG, "JsonInput.Log.OpeningFile", file.toString() ) );
    }
    addFileToResultFilesname( file );
  }

  private void parseNextInputToRowSet( InputStream input ) throws HopException {
    try {
      data.readerRowSet = data.reader.parse( input );
      input.close();
    } catch ( HopException ke ) {
      logInputError( ke );
      throw new JsonInputException( ke );
    } catch ( Exception e ) {
      logInputError( e );
      throw new JsonInputException( e );
    }
  }

  private void logInputError( HopException e ) {
    logError( e.getLocalizedMessage(), e );
    inputError( e.getLocalizedMessage() );
  }

  private void logInputError( Exception e ) {
    String errMsg = ( !meta.isInFields() || meta.getIsAFile() )
      ? BaseMessages.getString( PKG, "JsonReader.Error.ParsingFile", data.filename )
      : BaseMessages.getString( PKG, "JsonReader.Error.ParsingString", data.readrow[ data.indexSourceField ] );
    logError( errMsg, e );
    inputError( errMsg );
  }

  private void incrementErrors() {
    setErrors( getErrors() + 1 );
  }

  private void inputError( String errorMsg ) {
    if ( getTransformMeta().isDoingErrorHandling() ) {
      sendErrorRow( errorMsg );
    } else {
      incrementErrors();
    }
  }

  private class InputErrorHandler implements InputsReader.ErrorHandler {

    @Override
    public void error( Exception e ) {
      logError( BaseMessages.getString( PKG, "JsonInput.Log.UnexpectedError", e.toString() ) );
      setErrors( getErrors() + 1 );
    }

    @Override
    public void fileOpenError( FileObject file, FileSystemException e ) {
      String msg = BaseMessages.getString(
        PKG, "JsonInput.Log.UnableToOpenFile", "" + data.filenr, file.toString(), e.toString() );
      logError( msg );
      inputError( msg );
    }

    @Override
    public void fileCloseError( FileObject file, FileSystemException e ) {
      error( e );
    }
  }

  /**
   * get final row for output
   */
  private Object[] getOneOutputRow() throws HopException {
    if ( meta.isInFields() && !data.hasFirstRow ) {
      return null;
    }
    Object[] rawReaderRow = null;
    while ( ( rawReaderRow = data.readerRowSet.getRow() ) == null ) {
      if ( data.inputs.hasNext() && data.readerRowSet.isDone() ) {
        try ( InputStream nextIn = data.inputs.next() ) {

          if ( nextIn != null ) {
            parseNextInputToRowSet( nextIn );
          } else {
            parseNextInputToRowSet( new ByteArrayInputStream( EMPTY_JSON ) );
          }

        } catch ( IOException e ) {
          logError( BaseMessages.getString( PKG, "JsonInput.Log.UnexpectedError", e.toString() ), e );
          incrementErrors();
        }
      } else {
        if ( isDetailed() ) {
          logDetailed( BaseMessages.getString( PKG, "JsonInput.Log.FinishedProcessing" ) );
        }
        return null;
      }
    }
    Object[] outputRow = rowOutputConverter.getRow( buildBaseOutputRow(), rawReaderRow, data );
    addExtraFields( outputRow, data );
    return outputRow;
  }

  private void sendErrorRow( String errorMsg ) {
    try {
      // same error as before
      String defaultErrCode = "JsonInput001";
      if ( data.readrow != null ) {
        putError( getInputRowMeta(), data.readrow, 1, errorMsg, meta.getFieldValue(), defaultErrCode );
      } else {
        // when no input only error fields are recognized
        putError( new RowMeta(), new Object[ 0 ], 1, errorMsg, null, defaultErrCode );
      }
    } catch ( HopTransformException e ) {
      logError( e.getLocalizedMessage(), e );
    }
  }

  private boolean hasAdditionalFileFields() {
    return data.file != null;
  }

  /**
   * allocates out row
   */
  private Object[] buildBaseOutputRow() {
    Object[] outputRowData;
    if ( data.readrow != null ) {
      if ( meta.isRemoveSourceField() && data.indexSourceField > -1 ) {
        // skip the source field in the output array
        int sz = data.readrow.length;
        outputRowData = RowDataUtil.allocateRowData( data.outputRowMeta.size() );
        int ii = 0;
        for ( int i = 0; i < sz; i++ ) {
          if ( i != data.indexSourceField ) {
            outputRowData[ ii++ ] = data.readrow[ i ];
          }
        }
      } else {
        outputRowData = RowDataUtil.createResizedCopy( data.readrow, data.outputRowMeta.size() );
      }
    } else {
      outputRowData = RowDataUtil.allocateRowData( data.outputRowMeta.size() );
    }
    return outputRowData;
  }

  // should be refactored
  private void addExtraFields( Object[] outputRowData, JsonInputData data ) {
    int rowIndex = data.totalpreviousfields + data.nrInputFields;

    // See if we need to add the filename to the row...
    if ( meta.includeFilename() && !Utils.isEmpty( meta.getFilenameField() ) ) {
      outputRowData[ rowIndex++ ] = data.filename;
    }
    // See if we need to add the row number to the row...
    if ( meta.includeRowNumber() && !Utils.isEmpty( meta.getRowNumberField() ) ) {
      outputRowData[ rowIndex++ ] = new Long( data.rownr );
    }
    // Possibly add short filename...
    if ( meta.getShortFileNameField() != null && meta.getShortFileNameField().length() > 0 ) {
      outputRowData[ rowIndex++ ] = data.shortFilename;
    }
    // Add Extension
    if ( meta.getExtensionField() != null && meta.getExtensionField().length() > 0 ) {
      outputRowData[ rowIndex++ ] = data.extension;
    }
    // add path
    if ( meta.getPathField() != null && meta.getPathField().length() > 0 ) {
      outputRowData[ rowIndex++ ] = data.path;
    }
    // Add Size
    if ( meta.getSizeField() != null && meta.getSizeField().length() > 0 ) {
      outputRowData[ rowIndex++ ] = new Long( data.size );
    }
    // add Hidden
    if ( meta.isHiddenField() != null && meta.isHiddenField().length() > 0 ) {
      outputRowData[ rowIndex++ ] = new Boolean( data.path );
    }
    // Add modification date
    if ( meta.getLastModificationDateField() != null && meta.getLastModificationDateField().length() > 0 ) {
      outputRowData[ rowIndex++ ] = data.lastModificationDateTime;
    }
    // Add Uri
    if ( meta.getUriField() != null && meta.getUriField().length() > 0 ) {
      outputRowData[ rowIndex++ ] = data.uriName;
    }
    // Add RootUri
    if ( meta.getRootUriField() != null && meta.getRootUriField().length() > 0 ) {
      outputRowData[ rowIndex++ ] = data.rootUriName;
    }
  }

  private void createReader() throws HopException {
    // provide reader input fields with real path [PDI-15942]
    // [PDI-18283] Need to have this run before we create the FastJsonReader, so we can use resolve Json Paths
    JsonInputField[] inputFields = new JsonInputField[data.nrInputFields];
    for ( int i = 0; i < data.nrInputFields; i++ ) {
      JsonInputField field = meta.getInputFields()[ i ].clone();
      field.setPath( resolve( field.getPath() ) );
      inputFields[i] = field;
    }
    // Instead of putting in the meta.inputFields, we put in our json path resolved input fields
    data.reader = new FastJsonReader( inputFields, meta.isDefaultPathLeafToNull(), log );
    data.reader.setIgnoreMissingPath( meta.isIgnoreMissingPath() );
  }

  @Override
  public void dispose( ) {
    if ( data.file != null ) {
      IOUtils.closeQuietly( data.file );
    }
    data.inputs = null;
    data.reader = null;
    data.readerRowSet = null;
    data.repeatedFields = null;
    super.dispose( );
  }

  /**
   * Only to comply with super, does nothing good.
   *
   * @throws NotImplementedException everytime
   */
  @Override
  protected IBaseFileInputReader createReader( JsonInputMeta meta, JsonInputData data, FileObject file )
    throws Exception {
    throw new NotImplementedException();
  }

}
