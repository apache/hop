/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.pipeline.transforms.xbaseinput;

import org.apache.hop.core.ResultFile;
import org.apache.hop.core.RowSet;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.vfs.HopVFS;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.ITransform;

/**
 * Reads data from an XBase (dBase, foxpro, ...) file.
 *
 * @author Matt
 * @since 8-sep-2004
 */
public class XBaseInput extends BaseTransform implements ITransform {
  private static final Class<?> PKG = XBaseInputMeta.class; // Needed by Translator

  private XBaseInputMeta meta;
  private XBaseInputData data;

  public XBaseInput( TransformMeta transformMeta, ITransformData data, int copyNr, PipelineMeta pipelineMeta,
                     Pipeline pipeline ) {
    super( transformMeta, meta, data, copyNr, pipelineMeta, pipeline );
  }

  public boolean processRow() throws HopException {
    meta = (XBaseInputMeta) smi;
    data = (XBaseInputData) sdi;

    // See if we need to get a list of files from input...
    if ( first ) { // we just got started

      first = false;

      // The output row meta data, what does it look like?
      //
      data.outputRowMeta = new RowMeta();

      if ( meta.isAcceptingFilenames() ) {
        // Read the files from the specified input stream...
        data.files.getFiles().clear();

        int idx = -1;

        RowSet rowSet = findInputRowSet( meta.getAcceptingTransformName() );
        Object[] fileRowData = getRowFrom( rowSet );
        while ( fileRowData != null ) {
          IRowMeta fileRowMeta = rowSet.getRowMeta();
          if ( idx < 0 ) {
            idx = fileRowMeta.indexOfValue( meta.getAcceptingField() );
            if ( idx < 0 ) {
              logError( BaseMessages.getString( PKG, "XBaseInput.Log.Error.UnableToFindFilenameField", meta
                .getAcceptingField() ) );
              setErrors( 1 );
              stopAll();
              return false;
            }
          }
          try {
            String filename = fileRowMeta.getString( fileRowData, idx );
            data.files.addFile( HopVFS.getFileObject( filename, getPipelineMeta() ) );
          } catch ( Exception e ) {
            throw new HopException( e );
          }

          // Grab another row
          //
          fileRowData = getRowFrom( rowSet );
        }

        if ( data.files.nrOfFiles() == 0 ) {
          logBasic( BaseMessages.getString( PKG, "XBaseInput.Log.Error.NoFilesSpecified" ) );
          setOutputDone();
          return false;
        }
      }

      data.outputRowMeta = meta.getOutputFields( data.files, getTransformName() );

      // Open the first file & read the required rows in the buffer, stop
      // if it fails, exception will stop processLoop
      //
      openNextFile();
    }

    // Allocate the output row in advance, because we possibly want to add a few extra fields...
    //
    Object[] row = data.xbi.getRow( RowDataUtil.allocateRowData( data.outputRowMeta.size() ) );
    while ( row == null && data.fileNr < data.files.nrOfFiles() ) { // No more rows left in this file
      openNextFile();
      row = data.xbi.getRow( RowDataUtil.allocateRowData( data.outputRowMeta.size() ) );
    }

    if ( row == null ) {
      setOutputDone(); // signal end to receiver(s)
      return false; // end of data or error.
    }

    // OK, so we have read a line: increment the input counter
    incrementLinesInput();
    int outputIndex = data.fields.size();

    // Possibly add a filename...
    if ( meta.includeFilename() ) {
      row[ outputIndex++ ] = data.file_dbf.getName().getURI();
    }

    // Possibly add a row number...
    if ( meta.isRowNrAdded() ) {
      row[ outputIndex++ ] = new Long( getLinesInput() );
    }

    putRow( data.outputRowMeta, row ); // fill the rowset(s). (wait for empty)

    if ( checkFeedback( getLinesInput() ) ) {
      logBasic( BaseMessages.getString( PKG, "XBaseInput.Log.LineNr" ) + getLinesInput() );
    }

    if ( meta.getRowLimit() > 0 && getLinesInput() >= meta.getRowLimit() ) { // limit has been reached: stop now.
      setOutputDone();
      return false;
    }

    return true;
  }

  public boolean init() {
    meta = (XBaseInputMeta) smi;
    data = (XBaseInputData) sdi;

    if ( super.init() ) {
      data.files = meta.getTextFileList( this );
      data.fileNr = 0;

      if ( data.files.nrOfFiles() == 0 && !meta.isAcceptingFilenames() ) {
        logError( BaseMessages.getString( PKG, "XBaseInput.Log.Error.NoFilesSpecified" ) );
        return false;
      }
      if ( meta.isAcceptingFilenames() ) {
        try {
          if ( Utils.isEmpty( meta.getAcceptingTransformName() )
            || findInputRowSet( meta.getAcceptingTransformName() ) == null ) {
            logError( BaseMessages.getString( PKG, "XBaseInput.Log.Error.InvalidAcceptingTransformName" ) );
            return false;
          }

          if ( Utils.isEmpty( meta.getAcceptingField() ) ) {
            logError( BaseMessages.getString( PKG, "XBaseInput.Log.Error.InvalidAcceptingFieldName" ) );
            return false;
          }
        } catch ( Exception e ) {
          logError( e.getMessage() );
          return false;
        }
      }
      return true;
    }
    return false;
  }

  private void openNextFile() throws HopException {
    // Close the last file before opening the next...
    if ( data.xbi != null ) {
      logBasic( BaseMessages.getString( PKG, "XBaseInput.Log.FinishedReadingRecords" ) );
      data.xbi.close();
    }

    // Replace possible environment variables...
    data.file_dbf = data.files.getFile( data.fileNr );
    data.fileNr++;

    try {
      data.xbi = new XBase( log, HopVFS.getInputStream( data.file_dbf ) );
      data.xbi.setDbfFile( data.file_dbf.getName().getURI() );
      data.xbi.open();
      if ( !Utils.isEmpty( meta.getCharactersetName() ) ) {
        data.xbi.getReader().setCharactersetName( meta.getCharactersetName() );
      }

      logBasic( BaseMessages.getString( PKG, "XBaseInput.Log.OpenedXBaseFile" ) + " : [" + data.xbi + "]" );
      data.fields = data.xbi.getFields();

      // Add this to the result file names...
      ResultFile resultFile =
        new ResultFile( ResultFile.FILE_TYPE_GENERAL, data.file_dbf, getPipelineMeta().getName(), getTransformName() );
      resultFile.setComment( BaseMessages.getString( PKG, "XBaseInput.ResultFile.Comment" ) );
      addResultFile( resultFile );
    } catch ( Exception e ) {
      logError( BaseMessages.getString( PKG, "XBaseInput.Log.Error.CouldNotOpenXBaseFile1" )
        + data.file_dbf + BaseMessages.getString( PKG, "XBaseInput.Log.Error.CouldNotOpenXBaseFile2" )
        + e.getMessage() );
      throw new HopException( e );
    }
  }

  public void.dispose() {
    closeLastFile();

    super.dispose();
  }

  private void closeLastFile() {
    logBasic( BaseMessages.getString( PKG, "XBaseInput.Log.FinishedReadingRecords" ) );
    if ( data.xbi != null ) {
      data.xbi.close();
    }
  }

}
