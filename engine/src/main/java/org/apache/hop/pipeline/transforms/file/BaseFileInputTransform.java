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

package org.apache.hop.pipeline.transforms.file;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.hop.core.Result;
import org.apache.hop.core.ResultFile;
import org.apache.hop.core.IRowSet;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopFileException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.errorhandling.CompositeFileErrorHandler;
import org.apache.hop.pipeline.transform.errorhandling.IFileErrorHandler;
import org.apache.hop.pipeline.transform.errorhandling.FileErrorHandlerContentLineNumber;
import org.apache.hop.pipeline.transform.errorhandling.FileErrorHandlerMissingFiles;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class contains base functionality for file-based input transforms.
 *
 * @author Alexander Buloichik
 */
public abstract class BaseFileInputTransform<Meta extends BaseFileInputMeta, Data extends BaseFileInputTransformData> extends
  BaseTransform<Meta, Data> implements IBaseFileInputTransformControl {

  private static final Class<?> PKG = BaseFileInputTransform.class; // For Translator

  /**
   * Create reader for specific file.
   */
  protected abstract IBaseFileInputReader createReader( Meta meta, Data data, FileObject file ) throws Exception;

  public BaseFileInputTransform( TransformMeta transformMeta, Meta meta, Data data, int copyNr, PipelineMeta pipelineMeta,
                                 Pipeline pipeline ) {
    super( transformMeta, meta, data, copyNr, pipelineMeta, pipeline );
  }

  /**
   * Initialize transform before execute.
   */
  @Override
  public boolean init(){
    initErrorHandling();

    meta.additionalOutputFields.normalize();
    data.files = meta.getFileInputList( this );
    data.currentFileIndex = 0;

    // If there are missing files,
    // fail if we don't ignore errors
    //
    Result previousResult = getPipeline().getPreviousResult();
    Map<String, ResultFile> resultFiles = ( previousResult != null ) ? previousResult.getResultFiles() : null;

    if ( ( previousResult == null || resultFiles == null || resultFiles.size() == 0 ) && data.files
      .nrOfMissingFiles() > 0 && !meta.inputFiles.acceptingFilenames && !meta.errorHandling.errorIgnored ) {
      logError( BaseMessages.getString( PKG, "BaseFileInputTransform.Log.Error.NoFilesSpecified" ) );
      return false;
    }

    return super.init();
  }

  /**
   * Open next VFS file for processing.
   * <p>
   * This method will support different parallelization methods later.
   */
  protected boolean openNextFile() {
    try {
      if ( data.currentFileIndex >= data.files.nrOfFiles() ) {
        // all files already processed
        return false;
      }

      // Is this the last file?
      data.file = data.files.getFile( data.currentFileIndex );
      data.filename = HopVfs.getFilename( data.file );

      fillFileAdditionalFields( data, data.file );
      if ( meta.inputFiles.passingThruFields ) {
        StringBuilder sb = new StringBuilder();
        sb.append( data.currentFileIndex ).append( "_" ).append( data.file );
        data.currentPassThruFieldsRow = data.passThruFields.get( sb.toString() );
      }

      // Add this files to the result of this pipeline.
      //
      if ( meta.inputFiles.isaddresult ) {
        ResultFile resultFile =
          new ResultFile( ResultFile.FILE_TYPE_GENERAL, data.file, getPipelineMeta().getName(), toString() );
        resultFile.setComment( "File was read by an Text File input transform" );
        addResultFile( resultFile );
      }
      if ( log.isBasic() ) {
        logBasic( "Opening file: " + data.file.getName().getFriendlyURI() );
      }

      data.dataErrorLineHandler.handleFile( data.file );

      data.reader = createReader( meta, data, data.file );
    } catch ( Exception e ) {
      if ( !handleOpenFileException( e ) ) {
        return false;
      }
      data.reader = null;
    }
    // Move file pointer ahead!
    data.currentFileIndex++;

    return true;
  }

  protected boolean handleOpenFileException( Exception e ) {
    String errorMsg =
      "Couldn't open file #" + data.currentFileIndex + " : " + data.file.getName().getFriendlyURI();
    if ( !failAfterBadFile( errorMsg ) ) { // !meta.isSkipBadFiles()) stopAll();
      return true;
    }
    stopAll();
    setErrors( getErrors() + 1 );
    logError( errorMsg, e );
    return false;
  }

  /**
   * Process next row. This methods opens next file automatically.
   */
  @Override
  public boolean processRow() throws HopException {
    if ( first ) {
      first = false;
      prepareToRowProcessing();

      if ( !openNextFile() ) {
        setOutputDone(); // signal end to receiver(s)
        closeLastFile();
        return false;
      }
    }

    while ( true ) {
      if ( data.reader != null && data.reader.readRow() ) {
        // row processed
        return true;
      }
      // end of current file
      closeLastFile();

      if ( !openNextFile() ) {
        // there are no more files
        break;
      }
    }

    // after all files processed
    setOutputDone(); // signal end to receiver(s)
    closeLastFile();
    return false;
  }

  /**
   * Prepare to process. Executed only first time row processing. It can't be possible to prepare to process in the
   * init() phrase, because files can be in fields from previous transform.
   */
  protected void prepareToRowProcessing() throws HopException {
    data.outputRowMeta = new RowMeta();
    IRowMeta[] infoTransform = null;

    if ( meta.inputFiles.acceptingFilenames ) {
      // input files from previous transform
      infoTransform = filesFromPreviousTransform();
    }

    // get the metadata populated. Simple and easy.
    meta.getFields( data.outputRowMeta, getTransformName(), infoTransform, null, this, metadataProvider );
    // Create convert meta-data objects that will contain Date & Number formatters
    //
    data.convertRowMeta = data.outputRowMeta.cloneToType( IValueMeta.TYPE_STRING );

    BaseFileInputTransformUtils.handleMissingFiles( data.files, log, meta.errorHandling.errorIgnored,
      data.dataErrorLineHandler );

    // Count the number of repeat fields...
    for ( int i = 0; i < meta.inputFields.length; i++ ) {
      if ( meta.inputFields[ i ].isRepeated() ) {
        data.nr_repeats++;
      }
    }
  }

  @Override
  public boolean checkFeedback( long lines ) {
    return super.checkFeedback( lines );
  }

  /**
   * Initialize error handling.
   * <p>
   * TODO: should we set charset for error files from content meta ? What about case for automatic charset ?
   */
  private void initErrorHandling() {
    List<IFileErrorHandler> dataErrorLineHandlers = new ArrayList<>( 2 );
    if ( meta.errorHandling.lineNumberFilesDestinationDirectory != null ) {
      dataErrorLineHandlers.add( new FileErrorHandlerContentLineNumber( getPipeline().getExecutionStartDate(),
        resolve( meta.errorHandling.lineNumberFilesDestinationDirectory ),
        meta.errorHandling.lineNumberFilesExtension, meta.getEncoding(), this ) );
    }
    if ( meta.errorHandling.errorFilesDestinationDirectory != null ) {
      dataErrorLineHandlers.add( new FileErrorHandlerMissingFiles( getPipeline().getExecutionStartDate(), resolve(
        meta.errorHandling.errorFilesDestinationDirectory ), meta.errorHandling.errorFilesExtension, meta.getEncoding(), this ) );
    }
    data.dataErrorLineHandler = new CompositeFileErrorHandler( dataErrorLineHandlers );
  }

  /**
   * Read files from previous transform.
   */
  private IRowMeta[] filesFromPreviousTransform() throws HopException {
    IRowMeta[] infoTransform = null;

    data.files.getFiles().clear();

    int idx = -1;
    IRowSet rowSet = findInputRowSet( meta.inputFiles.acceptingTransformName );

    Object[] fileRow = getRowFrom( rowSet );
    while ( fileRow != null ) {
      IRowMeta prevInfoFields = rowSet.getRowMeta();
      if ( idx < 0 ) {
        if ( meta.inputFiles.passingThruFields ) {
          data.passThruFields = new HashMap<>();
          infoTransform = new IRowMeta[] { prevInfoFields };
          data.nrPassThruFields = prevInfoFields.size();
        }
        idx = prevInfoFields.indexOfValue( meta.inputFiles.acceptingField );
        if ( idx < 0 ) {
          logError( BaseMessages.getString( PKG, "BaseFileInputTransform.Log.Error.UnableToFindFilenameField",
            meta.inputFiles.acceptingField ) );
          setErrors( getErrors() + 1 );
          stopAll();
          return null;
        }
      }
      String fileValue = prevInfoFields.getString( fileRow, idx );
      try {
        FileObject fileObject = HopVfs.getFileObject( fileValue );
        data.files.addFile( fileObject );
        if ( meta.inputFiles.passingThruFields ) {
          StringBuilder sb = new StringBuilder();
          sb.append( data.files.nrOfFiles() > 0 ? data.files.nrOfFiles() - 1 : 0 ).append( "_" ).append( fileObject.toString() );
          data.passThruFields.put( sb.toString(), fileRow );
        }
      } catch ( HopFileException e ) {
        logError( BaseMessages.getString( PKG, "BaseFileInputTransform.Log.Error.UnableToCreateFileObject", fileValue ), e );
      }

      // Grab another row
      fileRow = getRowFrom( rowSet );
    }

    if ( data.files.nrOfFiles() == 0 ) {
      if ( log.isDetailed() ) {
        logDetailed( BaseMessages.getString( PKG, "BaseFileInputTransform.Log.Error.NoFilesSpecified" ) );
      }
      return null;
    }
    return infoTransform;
  }

  /**
   * Close last opened file/
   */
  protected void closeLastFile() {
    if ( data.reader != null ) {
      try {
        data.reader.close();
      } catch ( Exception ex ) {
        failAfterBadFile( "Error close reader" );
      }
      data.reader = null;
    }
    if ( data.file != null ) {
      try {
        data.file.close();
      } catch ( Exception ex ) {
        failAfterBadFile( "Error close file" );
      }
      data.file = null;
    }
  }

  /**
   * Dispose transform.
   */
  @Override
  public void dispose(){
    closeLastFile();

    super.dispose();
  }

  /**
   * @param errorMsg Message to send to rejected row if enabled
   * @return If should stop processing after having problems with a file
   */
  public boolean failAfterBadFile( String errorMsg ) {

    if ( getTransformMeta().isDoingErrorHandling() && data.filename != null && !data.rejectedFiles.containsKey(
      data.filename ) ) {
      data.rejectedFiles.put( data.filename, true );
      rejectCurrentFile( errorMsg );
    }

    return !meta.errorHandling.errorIgnored || !meta.errorHandling.skipBadFiles;
  }

  /**
   * Send file name and/or error message to error output
   *
   * @param errorMsg Message to send to rejected row if enabled
   */
  private void rejectCurrentFile( String errorMsg ) {
    if ( StringUtils.isNotBlank( meta.errorHandling.fileErrorField ) || StringUtils.isNotBlank(
      meta.errorHandling.fileErrorMessageField ) ) {
      IRowMeta rowMeta = getInputRowMeta();
      if ( rowMeta == null ) {
        rowMeta = new RowMeta();
      }

      int errorFileIndex =
        ( StringUtils.isBlank( meta.errorHandling.fileErrorField ) ) ? -1 : BaseFileInputTransformUtils.addValueMeta(
          getTransformName(), rowMeta, this.resolve( meta.errorHandling.fileErrorField ) );

      int errorMessageIndex =
        StringUtils.isBlank( meta.errorHandling.fileErrorMessageField ) ? -1 : BaseFileInputTransformUtils.addValueMeta(
          getTransformName(), rowMeta, this.resolve( meta.errorHandling.fileErrorMessageField ) );

      try {
        Object[] rowData = getRow();
        if ( rowData == null ) {
          rowData = RowDataUtil.allocateRowData( rowMeta.size() );
        }

        if ( errorFileIndex >= 0 ) {
          rowData[ errorFileIndex ] = data.filename;
        }
        if ( errorMessageIndex >= 0 ) {
          rowData[ errorMessageIndex ] = errorMsg;
        }

        putError( rowMeta, rowData, getErrors(), data.filename, null, "ERROR_CODE" );
      } catch ( Exception e ) {
        logError( "Error sending error row", e );
      }
    }
  }

  /**
   * Prepare file-dependent data for fill additional fields.
   */
  protected void fillFileAdditionalFields( Data data, FileObject file ) throws FileSystemException {
    data.shortFilename = file.getName().getBaseName();
    data.path = HopVfs.getFilename( file.getParent() );
    data.hidden = file.isHidden();
    data.extension = file.getName().getExtension();
    data.uriName = file.getName().getURI();
    data.rootUriName = file.getName().getRootURI();
    if ( file.getType().hasContent() ) {
      data.lastModificationDateTime = new Date( file.getContent().getLastModifiedTime() );
      data.size = file.getContent().getSize();
    } else {
      data.lastModificationDateTime = null;
      data.size = null;
    }
  }
}
