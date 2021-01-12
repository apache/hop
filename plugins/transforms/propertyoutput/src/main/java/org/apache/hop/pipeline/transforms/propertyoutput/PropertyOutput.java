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

package org.apache.hop.pipeline.transforms.propertyoutput;

import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.ResultFile;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;

import java.io.OutputStream;
import java.util.Properties;

/**
 * Output rows to Properties file and create a file.
 *
 * @author Samatar
 * @since 13-Apr-2008
 */

public class PropertyOutput
  extends BaseTransform<PropertyOutputMeta,PropertyOutputData>
  implements ITransform<PropertyOutputMeta,PropertyOutputData> {

  private static final Class<?> PKG = PropertyOutputMeta.class; // For Translator

  public PropertyOutput( TransformMeta transformMeta, PropertyOutputMeta meta, PropertyOutputData data, int copyNr, PipelineMeta pipelineMeta,
                         Pipeline pipeline ) {
    super( transformMeta, meta, data, copyNr, pipelineMeta, pipeline );
  }

  public boolean processRow() throws HopException {

    Object[] r = getRow(); // this also waits for a previous transform to be finished.

    if ( r == null ) { // no more input to be expected...
      setOutputDone();
      return false;
    }

    if ( first ) {
      first = false;
      data.inputRowMeta = getInputRowMeta();
      data.outputRowMeta = data.inputRowMeta.clone();
      meta.getFields( data.outputRowMeta, getTransformName(), null, null, this, metadataProvider );

      // Let's take the index of Key field ...
      data.indexOfKeyField = data.inputRowMeta.indexOfValue( meta.getKeyField() );
      if ( data.indexOfKeyField < 0 ) {
        // The field is unreachable !
        logError( BaseMessages.getString( PKG, "PropertyOutput.Log.ErrorFindingField", meta.getKeyField() ) );
        throw new HopException( BaseMessages.getString( PKG, "PropertyOutput.Log.ErrorFindingField", meta.getKeyField() ) );
      }

      // Let's take the index of Key field ...
      data.indexOfValueField = data.inputRowMeta.indexOfValue( meta.getValueField() );
      if ( data.indexOfValueField < 0 ) {
        // The field is unreachable !
        logError( BaseMessages.getString( PKG, "PropertyOutput.Log.ErrorFindingField", meta.getValueField() ) );
        throw new HopException( BaseMessages.getString( PKG, "PropertyOutput.Log.ErrorFindingField", meta.getValueField() ) );
      }

      if ( meta.isFileNameInField() ) {
        String realFieldName = resolve( meta.getFileNameField() );
        if ( Utils.isEmpty( realFieldName ) ) {
          logError( BaseMessages.getString( PKG, "PropertyOutput.Log.FilenameInFieldEmpty" ) );
          throw new HopException( BaseMessages.getString( PKG, "PropertyOutput.Log.FilenameInFieldEmpty" ) );
        }
        data.indexOfFieldfilename = data.inputRowMeta.indexOfValue( realFieldName );
        if ( data.indexOfFieldfilename < 0 ) {
          // The field is unreachable !
          logError( BaseMessages.getString( PKG, "PropertyOutput.Log.ErrorFindingField", meta.getValueField() ) );
          throw new HopException( BaseMessages.getString( PKG, "PropertyOutput.Log.ErrorFindingField", meta
            .getValueField() ) );
        }
      } else {
        // Let's check for filename...
        data.filename = buildFilename();
        // Check if filename is empty..
        if ( Utils.isEmpty( data.filename ) ) {
          logError( BaseMessages.getString( PKG, "PropertyOutput.Log.FilenameEmpty" ) );
          throw new HopException( BaseMessages.getString( PKG, "PropertyOutput.Log.FilenameEmpty" ) );
        }
        openNewFile();
      }
    } // end first

    // Get value field
    String propkey = data.inputRowMeta.getString( r, data.indexOfKeyField );
    String propvalue = data.inputRowMeta.getString( r, data.indexOfValueField );

    try {
      if ( meta.isFileNameInField() ) {
        data.filename = data.inputRowMeta.getString( r, data.indexOfFieldfilename );
        if ( Utils.isEmpty( data.filename ) ) {
          throw new HopException( BaseMessages.getString( PKG, "PropertyOutputMeta.Log.FileNameEmty" ) );
        }
        if ( !checkSameFile() ) {
          // close previous file
          closeFile();
          // Open new file
          openNewFile();
        }
      }

      if ( !data.KeySet.contains( propkey ) ) {
        if ( log.isDetailed() ) {
          logDetailed( BaseMessages.getString( PKG, "PropertyOutput.Log.Key", propkey ) );
          logDetailed( BaseMessages.getString( PKG, "PropertyOutput.Log.Value", propvalue ) );
        }
        // Update property
        data.pro.setProperty( propkey, propvalue );
        putRow( data.outputRowMeta, r ); // in case we want it to go further...
        incrementLinesOutput();

        if ( checkFeedback( getLinesRead() ) ) {
          if ( log.isBasic() ) {
            logBasic( "linenr " + getLinesRead() );
          }
        }
        data.KeySet.add( propkey );
      }
    } catch ( HopTransformException e ) {
      boolean sendToErrorRow = false;
      String errorMessage = null;
      if ( getTransformMeta().isDoingErrorHandling() ) {
        sendToErrorRow = true;
        errorMessage = e.toString();
      } else {
        logError( BaseMessages.getString( PKG, "PropertyOutputMeta.Log.ErrorInTransform" ) + e.getMessage() );
        setErrors( 1 );
        stopAll();
        setOutputDone(); // signal end to receiver(s)
        return false;
      }
      if ( sendToErrorRow ) {
        putError( data.outputRowMeta, r, 1L, errorMessage, null, "PROPSOUTPUTO001" );
      }
    }
    return true;
  }

  public boolean checkSameFile() throws HopException {
    return data.previousFileName.equals( data.filename );
  }

  private void openNewFile() throws HopException {
    try ( FileObject newFile = HopVfs.getFileObject( data.filename ) ) {
      data.pro = new Properties();
      data.KeySet.clear();

      data.file = newFile;
      if ( meta.isAppend() && data.file.exists() ) {
        data.pro.load( HopVfs.getInputStream( data.file ) );
      }
      // Create parent folder if needed...
      createParentFolder();
      //save processing file
      data.previousFileName = data.filename;
    } catch ( Exception e ) {
      throw new HopException( "Error opening file [" + data.filename + "]!", e );
    }
  }

  private void createParentFolder() throws HopException {
    if ( meta.isCreateParentFolder() ) {
      FileObject parentfolder = null;
      try {
        // Do we need to create parent folder ?

        // Check for parent folder
        // Get parent folder
        parentfolder = data.file.getParent();
        if ( !parentfolder.exists() ) {
          if ( log.isDetailed() ) {
            logDetailed( BaseMessages.getString( PKG, "PropertyOutput.Log.ParentFolderExists", parentfolder.getName().toString() ) );
          }
          parentfolder.createFolder();
          if ( log.isDetailed() ) {
            logDetailed( BaseMessages.getString( PKG, "PropertyOutput.Log.CanNotCreateParentFolder", parentfolder.getName().toString() ) );
          }
        }
      } catch ( Exception e ) {
        logError( BaseMessages.getString( PKG, "PropertyOutput.Log.CanNotCreateParentFolder", parentfolder.getName().toString() ) );
        throw new HopException( BaseMessages.getString( PKG, "PropertyOutput.Log.CanNotCreateParentFolder", parentfolder.getName().toString() ) );
      } finally {
        if ( parentfolder != null ) {
          try {
            parentfolder.close();
          } catch ( Exception ex ) { /* Ignore */
          }
        }
      }
    }
  }

  private boolean closeFile() {
    if ( data.file == null ) {
      return true;
    }
    boolean retval = false;
    try ( OutputStream propsFile = HopVfs.getOutputStream( data.file, false ) ) {
      data.pro.store( propsFile, resolve( meta.getComment() ) );

      if ( meta.isAddToResult() ) {
        // Add this to the result file names...
        ResultFile resultFile = new ResultFile( ResultFile.FILE_TYPE_GENERAL, data.file, getPipelineMeta().getName(), getTransformName() );
        resultFile.setComment( BaseMessages.getString( PKG, "PropertyOutput.Log.FileAddedResult" ) );
        addResultFile( resultFile );
      }
      data.KeySet.clear();
      retval = true;
    } catch ( Exception e ) {
      logError( "Exception trying to close file [" + data.file.getName() + "]! :" + e.toString() );
      setErrors( 1 );
    } finally {
      if ( data.file != null ) {
        try {
          data.file.close();
          data.file = null;
        } catch ( Exception e ) { /* Ignore */
          logDetailed( "Exception trying to close file [" + data.file.getName() + "]! :", e );
        }
      }
      if ( data.pro != null ) {
        data.pro = null;
      }
    }
    return retval;
  }

  public String buildFilename() {
    return meta.buildFilename( this, getCopy() );
  }

  public boolean init() {

    if ( super.init() ) {
      return true;
    }
    return false;
  }

  public void dispose() {

    closeFile();

    setOutputDone();
    super.dispose();
  }

}
