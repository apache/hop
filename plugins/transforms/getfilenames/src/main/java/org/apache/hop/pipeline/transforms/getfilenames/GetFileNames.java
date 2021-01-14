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

package org.apache.hop.pipeline.transforms.getfilenames;

import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileType;
import org.apache.hop.core.Const;
import org.apache.hop.core.ResultFile;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.fileinput.FileInputList;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;

import java.io.IOException;
import java.util.Date;
import java.util.List;

/**
 * Read all sorts of text files, convert them to rows and writes these to one or more output streams.
 *
 * @author Matt
 * @since 4-apr-2003
 */
public class GetFileNames extends BaseTransform<GetFileNamesMeta, GetFileNamesData> implements ITransform<GetFileNamesMeta, GetFileNamesData> {

  private static final Class<?> PKG = GetFileNamesMeta.class; // For Translator

  public GetFileNames( TransformMeta transformMeta, GetFileNamesMeta meta, GetFileNamesData data, int copyNr, PipelineMeta pipelineMeta,
                       Pipeline pipeline ) {
    super( transformMeta, meta, data, copyNr, pipelineMeta, pipeline );
  }

  /**
   * Build an empty row based on the meta-data...
   *
   * @return
   */

  private Object[] buildEmptyRow() {
    Object[] rowData = RowDataUtil.allocateRowData( data.outputRowMeta.size() );

    return rowData;
  }

  public boolean processRow() throws HopException {
    if ( !meta.isFileField() ) {
      if ( data.filenr >= data.filessize ) {
        setOutputDone();
        return false;
      }
    } else {
      if ( data.filenr >= data.filessize ) {
        // Grab one row from previous transform ...
        data.readrow = getRow();
      }

      if ( data.readrow == null ) {
        setOutputDone();
        return false;
      }

      if ( first ) {
        first = false;

        data.inputRowMeta = getInputRowMeta();
        data.outputRowMeta = data.inputRowMeta.clone();
        meta.getFields( data.outputRowMeta, getTransformName(), null, null, this, metadataProvider );

        // Get total previous fields
        data.totalpreviousfields = data.inputRowMeta.size();

        // Check is filename field is provided
        if ( Utils.isEmpty( meta.getDynamicFilenameField() ) ) {
          logError( BaseMessages.getString( PKG, "GetFileNames.Log.NoField" ) );
          throw new HopException( BaseMessages.getString( PKG, "GetFileNames.Log.NoField" ) );
        }

        // cache the position of the field
        if ( data.indexOfFilenameField < 0 ) {
          data.indexOfFilenameField = data.inputRowMeta.indexOfValue( meta.getDynamicFilenameField() );
          if ( data.indexOfFilenameField < 0 ) {
            // The field is unreachable !
            logError( BaseMessages.getString( PKG, "GetFileNames.Log.ErrorFindingField", meta
              .getDynamicFilenameField() ) );
            throw new HopException( BaseMessages.getString(
              PKG, "GetFileNames.Exception.CouldnotFindField", meta.getDynamicFilenameField() ) );
          }
        }

        // If wildcard field is specified, Check if field exists
        if ( !Utils.isEmpty( meta.getDynamicWildcardField() ) ) {
          if ( data.indexOfWildcardField < 0 ) {
            data.indexOfWildcardField = data.inputRowMeta.indexOfValue( meta.getDynamicWildcardField() );
            if ( data.indexOfWildcardField < 0 ) {
              // The field is unreachable !
              logError( BaseMessages.getString( PKG, "GetFileNames.Log.ErrorFindingField" )
                + "[" + meta.getDynamicWildcardField() + "]" );
              throw new HopException( BaseMessages.getString(
                PKG, "GetFileNames.Exception.CouldnotFindField", meta.getDynamicWildcardField() ) );
            }
          }
        }
        // If ExcludeWildcard field is specified, Check if field exists
        if ( !Utils.isEmpty( meta.getDynamicExcludeWildcardField() ) ) {
          if ( data.indexOfExcludeWildcardField < 0 ) {
            data.indexOfExcludeWildcardField =
              data.inputRowMeta.indexOfValue( meta.getDynamicExcludeWildcardField() );
            if ( data.indexOfExcludeWildcardField < 0 ) {
              // The field is unreachable !
              logError( BaseMessages.getString( PKG, "GetFileNames.Log.ErrorFindingField" )
                + "[" + meta.getDynamicExcludeWildcardField() + "]" );
              throw new HopException( BaseMessages.getString(
                PKG, "GetFileNames.Exception.CouldnotFindField", meta.getDynamicExcludeWildcardField() ) );
            }
          }
        }
      }
    } // end if first

    try {
      Object[] outputRow = buildEmptyRow();
      int outputIndex = 0;
      Object[] extraData = new Object[ data.nrTransformFields ];
      if ( meta.isFileField() ) {
        if ( data.filenr >= data.filessize ) {
          // Get value of dynamic filename field ...
          String filename = getInputRowMeta().getString( data.readrow, data.indexOfFilenameField );
          String wildcard = "";
          if ( data.indexOfWildcardField >= 0 ) {
            wildcard = getInputRowMeta().getString( data.readrow, data.indexOfWildcardField );
          }
          String excludewildcard = "";
          if ( data.indexOfExcludeWildcardField >= 0 ) {
            excludewildcard = getInputRowMeta().getString( data.readrow, data.indexOfExcludeWildcardField );
          }

          String[] filesname = { filename };
          String[] filesmask = { wildcard };
          String[] excludefilesmask = { excludewildcard };
          String[] filesrequired = { "N" };
          boolean[] includesubfolders = { meta.isDynamicIncludeSubFolders() };
          // Get files list
          data.files =
            meta.getDynamicFileList(
              this, filesname, filesmask, excludefilesmask, filesrequired, includesubfolders );
          data.filessize = data.files.nrOfFiles();
          data.filenr = 0;
        }

        // Clone current input row
        outputRow = data.readrow.clone();
      }
      if ( data.filessize > 0 ) {
        data.file = data.files.getFile( data.filenr );

        if ( meta.isAddResultFile() ) {
          // Add this to the result file names...
          ResultFile resultFile =
            new ResultFile( ResultFile.FILE_TYPE_GENERAL, data.file, getPipelineMeta().getName(), getTransformName() );
          resultFile.setComment( BaseMessages.getString( PKG, "GetFileNames.Log.FileReadByTransform" ) );
          addResultFile( resultFile );
        }

        // filename
        extraData[ outputIndex++ ] = HopVfs.getFilename( data.file );

        // short_filename
        extraData[ outputIndex++ ] = data.file.getName().getBaseName();

        try {
          // Path
          extraData[ outputIndex++ ] = HopVfs.getFilename( data.file.getParent() );

          // type
          extraData[ outputIndex++ ] = data.file.getType().toString();

          // exists
          extraData[ outputIndex++ ] = Boolean.valueOf( data.file.exists() );

          // ishidden
          extraData[ outputIndex++ ] = Boolean.valueOf( data.file.isHidden() );

          // isreadable
          extraData[ outputIndex++ ] = Boolean.valueOf( data.file.isReadable() );

          // iswriteable
          extraData[ outputIndex++ ] = Boolean.valueOf( data.file.isWriteable() );

          // lastmodifiedtime
          extraData[ outputIndex++ ] = new Date( data.file.getContent().getLastModifiedTime() );

          // size
          Long size = null;
          if ( data.file.getType().equals( FileType.FILE ) ) {
            size = new Long( data.file.getContent().getSize() );
          }

          extraData[ outputIndex++ ] = size;

        } catch ( IOException e ) {
          throw new HopException( e );
        }

        // extension
        extraData[ outputIndex++ ] = data.file.getName().getExtension();

        // uri
        extraData[ outputIndex++ ] = data.file.getName().getURI();

        // rooturi
        extraData[ outputIndex++ ] = data.file.getName().getRootURI();

        // See if we need to add the row number to the row...
        if ( meta.includeRowNumber() && !Utils.isEmpty( meta.getRowNumberField() ) ) {
          extraData[ outputIndex++ ] = new Long( data.rownr );
        }

        data.rownr++;
        // Add row data
        outputRow = RowDataUtil.addRowData( outputRow, data.totalpreviousfields, extraData );
        // Send row
        putRow( data.outputRowMeta, outputRow );

        if ( meta.getRowLimit() > 0 && data.rownr >= meta.getRowLimit() ) { // limit has been reached: stop now.
          setOutputDone();
          return false;
        }

      }
    } catch ( Exception e ) {
      throw new HopTransformException( e );
    }

    data.filenr++;

    if ( checkFeedback( getLinesInput() ) ) {
      if ( log.isBasic() ) {
        logBasic( BaseMessages.getString( PKG, "GetFileNames.Log.NrLine", "" + getLinesInput() ) );
      }
    }

    return true;
  }

  private void handleMissingFiles() throws HopException {
    if ( meta.isdoNotFailIfNoFile() && data.files.nrOfFiles() == 0 ) {
      logBasic( BaseMessages.getString( PKG, "GetFileNames.Log.NoFile" ) );
      return;
    }
    List<FileObject> nonExistantFiles = data.files.getNonExistantFiles();

    if ( nonExistantFiles.size() != 0 ) {
      String message = FileInputList.getRequiredFilesDescription( nonExistantFiles );
      logBasic( "ERROR: Missing " + message );
      throw new HopException( "Following required files are missing: " + message );
    }

    List<FileObject> nonAccessibleFiles = data.files.getNonAccessibleFiles();
    if ( nonAccessibleFiles.size() != 0 ) {
      String message = FileInputList.getRequiredFilesDescription( nonAccessibleFiles );
      logBasic( "WARNING: Not accessible " + message );
      throw new HopException( "Following required files are not accessible: " + message );
    }
  }

  public boolean init(){

    if ( super.init() ) {

      try {
        // Create the output row meta-data
        data.outputRowMeta = new RowMeta();
        meta.getFields( data.outputRowMeta, getTransformName(), null, null, this, metadataProvider ); // get the
        // metadata
        // populated
        data.nrTransformFields = data.outputRowMeta.size();

        if ( !meta.isFileField() ) {
          data.files = meta.getFileList( this );
          data.filessize = data.files.nrOfFiles();
          handleMissingFiles();
        } else {
          data.filessize = 0;
        }

      } catch ( Exception e ) {
        logError( "Error initializing transform: " + e.toString() );
        logError( Const.getStackTracker( e ) );
        return false;
      }

      data.rownr = 1L;
      data.filenr = 0;
      data.totalpreviousfields = 0;

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
