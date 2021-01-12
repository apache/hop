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

package org.apache.hop.pipeline.transforms.getsubfolders;

import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.Const;
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
 * Read all subfolder inside a specified folder and convert them to rows and writes these to one or more output streams.
 *
 * @author Samatar
 * @since 18-July-2008
 */
public class GetSubFolders extends BaseTransform<GetSubFoldersMeta, GetSubFoldersData> implements ITransform<GetSubFoldersMeta, GetSubFoldersData> {

  private static final Class<?> PKG = GetSubFoldersMeta.class; // For Translator

  public GetSubFolders( TransformMeta transformMeta, GetSubFoldersMeta meta, GetSubFoldersData data, int copyNr, PipelineMeta pipelineMeta,
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

    if ( meta.isFoldernameDynamic() && ( data.filenr >= data.filessize ) ) {
      // Grab one row from previous transform ...
      data.readrow = getRow();
    }

    if ( first ) {
      first = false;

      if ( meta.isFoldernameDynamic() ) {
        data.inputRowMeta = getInputRowMeta();
        data.outputRowMeta = data.inputRowMeta.clone();
        meta.getFields( data.outputRowMeta, getTransformName(), null, null, this, metadataProvider );

        // Get total previous fields
        data.totalpreviousfields = data.inputRowMeta.size();

        // Check is filename field is provided
        if ( Utils.isEmpty( meta.getDynamicFoldernameField() ) ) {
          logError( BaseMessages.getString( PKG, "GetSubFolders.Log.NoField" ) );
          throw new HopException( BaseMessages.getString( PKG, "GetSubFolders.Log.NoField" ) );
        }

        // cache the position of the field
        if ( data.indexOfFoldernameField < 0 ) {
          String realDynamicFoldername = resolve( meta.getDynamicFoldernameField() );
          data.indexOfFoldernameField = data.inputRowMeta.indexOfValue( realDynamicFoldername );
          if ( data.indexOfFoldernameField < 0 ) {
            // The field is unreachable !
            logError( BaseMessages.getString( PKG, "GetSubFolders.Log.ErrorFindingField" )
              + "[" + realDynamicFoldername + "]" );
            throw new HopException( BaseMessages.getString(
              PKG, "GetSubFolders.Exception.CouldnotFindField", realDynamicFoldername ) );
          }
        }
      } else {
        // Create the output row meta-data
        data.outputRowMeta = new RowMeta();
        meta.getFields( data.outputRowMeta, getTransformName(), null, null, this, metadataProvider ); // get the
        // metadata
        // populated
        // data.nrTransformFields= data.outputRowMeta.size();

        data.files = meta.getFolderList( this );
        data.filessize = data.files.nrOfFiles();
        handleMissingFiles();

      }
      data.nrTransformFields = data.outputRowMeta.size();

    } // end if first
    if ( meta.isFoldernameDynamic() ) {
      if ( data.readrow == null ) {
        setOutputDone();
        return false;
      }
    } else {
      if ( data.filenr >= data.filessize ) {
        setOutputDone();
        return false;
      }
    }

    try {
      Object[] outputRow = buildEmptyRow();
      int outputIndex = 0;
      Object[] extraData = new Object[ data.nrTransformFields ];
      if ( meta.isFoldernameDynamic() ) {
        if ( data.filenr >= data.filessize ) {
          // Get value of dynamic filename field ...
          String filename = getInputRowMeta().getString( data.readrow, data.indexOfFoldernameField );

          String[] filesname = { filename };
          String[] filesrequired = { GetSubFoldersMeta.NO };
          // Get files list
          data.files = meta.getDynamicFolderList( this, filesname, filesrequired );
          data.filessize = data.files.nrOfFiles();
          data.filenr = 0;
        }

        // Clone current input row
        outputRow = data.readrow.clone();
      }
      if ( data.filessize > 0 ) {
        data.file = data.files.getFile( data.filenr );

        // filename
        extraData[ outputIndex++ ] = HopVfs.getFilename( data.file );

        // short_filename
        extraData[ outputIndex++ ] = data.file.getName().getBaseName();

        try {
          // Path
          extraData[ outputIndex++ ] = HopVfs.getFilename( data.file.getParent() );

          // ishidden
          extraData[ outputIndex++ ] = Boolean.valueOf( data.file.isHidden() );

          // isreadable
          extraData[ outputIndex++ ] = Boolean.valueOf( data.file.isReadable() );

          // iswriteable
          extraData[ outputIndex++ ] = Boolean.valueOf( data.file.isWriteable() );

          // lastmodifiedtime
          extraData[ outputIndex++ ] = new Date( data.file.getContent().getLastModifiedTime() );

        } catch ( IOException e ) {
          throw new HopException( e );
        }

        // uri
        extraData[ outputIndex++ ] = data.file.getName().getURI();

        // rooturi
        extraData[ outputIndex++ ] = data.file.getName().getRootURI();

        // childrens files
        extraData[ outputIndex++ ] = new Long( data.file.getChildren().length );

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
        logBasic( BaseMessages.getString( PKG, "GetSubFolders.Log.NrLine", "" + getLinesInput() ) );
      }
    }

    return true;
  }

  private void handleMissingFiles() throws HopException {
    List<FileObject> nonExistantFiles = data.files.getNonExistantFiles();

    if ( nonExistantFiles.size() != 0 ) {
      String message = FileInputList.getRequiredFilesDescription( nonExistantFiles );
      logError( BaseMessages.getString( PKG, "GetSubFolders.Error.MissingFiles", message ) );
      throw new HopException( BaseMessages.getString( PKG, "GetSubFolders.Exception.MissingFiles", message ) );
    }

    List<FileObject> nonAccessibleFiles = data.files.getNonAccessibleFiles();
    if ( nonAccessibleFiles.size() != 0 ) {
      String message = FileInputList.getRequiredFilesDescription( nonAccessibleFiles );
      logError( BaseMessages.getString( PKG, "GetSubFolders.Error.NoAccessibleFiles", message ) );
      throw new HopException( BaseMessages
        .getString( PKG, "GetSubFolders.Exception.NoAccessibleFiles", message ) );
    }
  }

  public boolean init(){

    if ( super.init() ) {
      try {
        data.filessize = 0;
        data.rownr = 1L;
        data.filenr = 0;
        data.totalpreviousfields = 0;
      } catch ( Exception e ) {
        logError( "Error initializing transform: " + e.toString() );
        logError( Const.getStackTracker( e ) );
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
