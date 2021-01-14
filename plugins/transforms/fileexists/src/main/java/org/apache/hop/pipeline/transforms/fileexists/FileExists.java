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

package org.apache.hop.pipeline.transforms.fileexists;

import org.apache.commons.vfs2.FileType;
import org.apache.hop.core.ResultFile;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;

/**
 * Check if a table exists in a Database *
 *
 * @author Samatar
 * @since 03-Juin-2008
 */

public class FileExists extends BaseTransform<FileExistsMeta, FileExistsData> implements ITransform<FileExistsMeta, FileExistsData> {

  private static final Class<?> PKG = FileExistsMeta.class; // For Translator

  public FileExists( TransformMeta transformMeta, FileExistsMeta meta, FileExistsData data, int copyNr, PipelineMeta pipelineMeta,
                     Pipeline pipeline ) {
    super( transformMeta, meta, data, copyNr, pipelineMeta, pipeline );
  }

  public boolean processRow() throws HopException {

    boolean sendToErrorRow = false;
    String errorMessage = null;

    Object[] r = getRow(); // Get row from input rowset & set row busy!
    if ( r == null ) { // no more input to be expected...

      setOutputDone();
      return false;
    }

    boolean fileexists = false;
    String filetype = null;

    try {
      if ( first ) {
        first = false;
        // get the RowMeta
        data.previousRowMeta = getInputRowMeta().clone();
        data.NrPrevFields = data.previousRowMeta.size();
        data.outputRowMeta = data.previousRowMeta;
        meta.getFields( data.outputRowMeta, getTransformName(), null, null, this, metadataProvider );

        // Check is tablename field is provided
        if ( Utils.isEmpty( meta.getDynamicFilenameField() ) ) {
          logError( BaseMessages.getString( PKG, "FileExists.Error.FilenameFieldMissing" ) );
          throw new HopException( BaseMessages.getString( PKG, "FileExists.Error.FilenameFieldMissing" ) );
        }

        // cache the position of the field
        if ( data.indexOfFileename < 0 ) {
          data.indexOfFileename = data.previousRowMeta.indexOfValue( meta.getDynamicFilenameField() );
          if ( data.indexOfFileename < 0 ) {
            // The field is unreachable !
            logError( BaseMessages.getString( PKG, "FileExists.Exception.CouldnotFindField" )
              + "[" + meta.getDynamicFilenameField() + "]" );
            throw new HopException( BaseMessages.getString( PKG, "FileExists.Exception.CouldnotFindField", meta
              .getDynamicFilenameField() ) );
          }
        }
      } // End If first

      Object[] outputRow = RowDataUtil.allocateRowData( data.outputRowMeta.size() );
      for ( int i = 0; i < data.NrPrevFields; i++ ) {
        outputRow[ i ] = r[ i ];
      }
      // get filename
      String filename = data.previousRowMeta.getString( r, data.indexOfFileename );
      if ( !Utils.isEmpty( filename ) ) {
        data.file = HopVfs.getFileObject( filename );

        // Check if file
        fileexists = data.file.exists();

        // include file type?
        if ( meta.includeFileType() && fileexists && !Utils.isEmpty( meta.getFileTypeFieldName() ) ) {
          filetype = data.file.getType().toString();
        }

        // add filename to result filenames?
        if ( meta.addResultFilenames() && fileexists && data.file.getType() == FileType.FILE ) {
          // Add this to the result file names...
          ResultFile resultFile =
            new ResultFile( ResultFile.FILE_TYPE_GENERAL, data.file, getPipelineMeta().getName(), getTransformName() );
          resultFile.setComment( BaseMessages.getString( PKG, "FileExists.Log.FileAddedResult" ) );
          addResultFile( resultFile );

          if ( log.isDetailed() ) {
            logDetailed( BaseMessages.getString( PKG, "FileExists.Log.FilenameAddResult", data.file.toString() ) );
          }
        }
      }

      // Add result field to input stream
      outputRow[ data.NrPrevFields ] = fileexists;
      int rowIndex = data.NrPrevFields;
      rowIndex++;

      if ( meta.includeFileType() && !Utils.isEmpty( meta.getFileTypeFieldName() ) ) {
        outputRow[ rowIndex ] = filetype;
      }

      // add new values to the row.
      putRow( data.outputRowMeta, outputRow ); // copy row to output rowset(s);

      if ( log.isRowLevel() ) {
        logRowlevel( BaseMessages.getString( PKG, "FileExists.LineNumber", getLinesRead()
          + " : " + getInputRowMeta().getString( r ) ) );
      }
    } catch ( Exception e ) {
      if ( getTransformMeta().isDoingErrorHandling() ) {
        sendToErrorRow = true;
        errorMessage = e.toString();
      } else {
        logError( BaseMessages.getString( PKG, "FileExists.ErrorInTransformRunning" ) + e.getMessage() );
        setErrors( 1 );
        stopAll();
        setOutputDone(); // signal end to receiver(s)
        return false;
      }
      if ( sendToErrorRow ) {
        // Simply add this row to the error row
        putError( getInputRowMeta(), r, 1, errorMessage, meta.getResultFieldName(), "FileExistsO01" );
      }
    }

    return true;
  }

  public boolean init(){
    if ( super.init() ) {
      if ( Utils.isEmpty( meta.getResultFieldName() ) ) {
        logError( BaseMessages.getString( PKG, "FileExists.Error.ResultFieldMissing" ) );
        return false;
      }
      return true;
    }
    return false;
  }

  public void dispose(){
    if ( data.file != null ) {
      try {
        data.file.close();
        data.file = null;
      } catch ( Exception e ) {
        // Ignore close errors
      }

    }
    super.dispose();
  }

}
