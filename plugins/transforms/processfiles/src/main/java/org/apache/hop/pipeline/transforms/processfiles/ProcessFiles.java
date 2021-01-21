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

package org.apache.hop.pipeline.transforms.processfiles;

import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSelectInfo;
import org.apache.commons.vfs2.FileSelector;
import org.apache.commons.vfs2.FileType;
import org.apache.hop.core.ResultFile;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;


public class ProcessFiles extends BaseTransform<ProcessFilesMeta, ProcessFilesData> implements ITransform<ProcessFilesMeta, ProcessFilesData> {
  private static final Class<?> PKG = ProcessFilesMeta.class; // For Translator

  public ProcessFiles( TransformMeta transformMeta,ProcessFilesMeta meta, ProcessFilesData data, int copyNr, PipelineMeta pipelineMeta,
                       Pipeline pipeline ) {
    super( transformMeta, meta, data, copyNr, pipelineMeta, pipeline );
  }

  public boolean processRow() throws HopException {

    Object[] r = getRow(); // Get row from input rowset & set row busy!
    if ( r == null ) { // no more input to be expected...

      setOutputDone();
      return false;
    }
    if ( first ) {
      first = false;
      // Check is source filename field is provided
      if ( Utils.isEmpty( meta.getDynamicSourceFileNameField() ) ) {
        throw new HopException( BaseMessages.getString( PKG, "ProcessFiles.Error.SourceFilenameFieldMissing" ) );
      }
      // Check is target filename field is provided
      if ( meta.getOperationType() != ProcessFilesMeta.OPERATION_TYPE_DELETE
        && Utils.isEmpty( meta.getDynamicTargetFileNameField() ) ) {
        throw new HopException( BaseMessages.getString( PKG, "ProcessFiles.Error.TargetFilenameFieldMissing" ) );
      }

      // cache the position of the source filename field
      if ( data.indexOfSourceFilename < 0 ) {
        data.indexOfSourceFilename = getInputRowMeta().indexOfValue( meta.getDynamicSourceFileNameField() );
        if ( data.indexOfSourceFilename < 0 ) {
          // The field is unreachable !
          throw new HopException( BaseMessages.getString( PKG, "ProcessFiles.Exception.CouldnotFindField", meta
            .getDynamicSourceFileNameField() ) );
        }
      }
      // cache the position of the source filename field
      if ( meta.getOperationType() != ProcessFilesMeta.OPERATION_TYPE_DELETE && data.indexOfTargetFilename < 0 ) {
        data.indexOfTargetFilename = getInputRowMeta().indexOfValue( meta.getDynamicTargetFileNameField() );
        if ( data.indexOfTargetFilename < 0 ) {
          // The field is unreachable !
          throw new HopException( BaseMessages.getString( PKG, "ProcessFiles.Exception.CouldnotFindField", meta
            .getDynamicTargetFileNameField() ) );
        }
      }

      if ( meta.simulate ) {
        if ( log.isBasic() ) {
          logBasic( BaseMessages.getString( PKG, "ProcessFiles.Log.SimulationModeON" ) );
        }
      }
    } // End If first
    try {
      // get source filename
      String sourceFilename = getInputRowMeta().getString( r, data.indexOfSourceFilename );

      if ( Utils.isEmpty( sourceFilename ) ) {
        logError( BaseMessages.getString( PKG, "ProcessFiles.Error.SourceFileEmpty" ) );
        throw new HopException( BaseMessages.getString( PKG, "ProcessFiles.Error.SourceFileEmpty" ) );
      }
      data.sourceFile = HopVfs.getFileObject( sourceFilename );

      if ( !data.sourceFile.exists() ) {
        logError( BaseMessages.getString( PKG, "ProcessFiles.Error.SourceFileNotExist", sourceFilename ) );
        throw new HopException( BaseMessages.getString(
          PKG, "ProcessFiles.Error.SourceFileNotExist", sourceFilename ) );
      }
      if ( data.sourceFile.getType() != FileType.FILE ) {
        logError( BaseMessages.getString( PKG, "ProcessFiles.Error.SourceFileNotFile", sourceFilename ) );
        throw new HopException( BaseMessages.getString(
          PKG, "ProcessFiles.Error.SourceFileNotFile", sourceFilename ) );
      }
      String targetFilename = null;
      if ( meta.getOperationType() != ProcessFilesMeta.OPERATION_TYPE_DELETE ) {
        // get value for target filename
        targetFilename = getInputRowMeta().getString( r, data.indexOfTargetFilename );

        if ( Utils.isEmpty( targetFilename ) ) {
          logError( BaseMessages.getString( PKG, "ProcessFiles.Error.TargetFileEmpty" ) );
          throw new HopException( BaseMessages.getString( PKG, "ProcessFiles.Error.TargetFileEmpty" ) );
        }
        data.targetFile = HopVfs.getFileObject( targetFilename );
        if ( data.targetFile.exists() ) {
          if ( log.isDetailed() ) {
            logDetailed( BaseMessages.getString( PKG, "ProcessFiles.Log.TargetFileExists", targetFilename ) );
          }
          // check if target is really a file otherwise it could overwrite a complete folder by copy or move operations
          if ( data.targetFile.getType() != FileType.FILE ) {
            logError( BaseMessages.getString( PKG, "ProcessFiles.Error.TargetFileNotFile", targetFilename ) );
            throw new HopException( BaseMessages.getString(
              PKG, "ProcessFiles.Error.TargetFileNotFile", targetFilename ) );
          }

        } else {
          // let's check parent folder
          FileObject parentFolder = data.targetFile.getParent();
          if ( !parentFolder.exists() ) {
            if ( !meta.isCreateParentFolder() ) {
              throw new HopException( BaseMessages.getString(
                PKG, "ProcessFiles.Error.TargetParentFolderNotExists", parentFolder.toString() ) );
            } else {
              parentFolder.createFolder();
            }
          }
          if ( parentFolder != null ) {
            parentFolder.close();
          }
        }
      }

      switch ( meta.getOperationType() ) {
        case ProcessFilesMeta.OPERATION_TYPE_COPY:
          if ( ( ( meta.isOverwriteTargetFile() && data.targetFile.exists() ) || !data.targetFile.exists() )
            && !meta.simulate ) {
            data.targetFile.copyFrom( data.sourceFile, new TextOneToOneFileSelector() );
            if ( log.isDetailed() ) {
              logDetailed( BaseMessages.getString(
                PKG, "ProcessFiles.Log.SourceFileCopied", sourceFilename, targetFilename ) );
            }
          } else {
            if ( log.isDetailed() ) {
              logDetailed( BaseMessages.getString(
                PKG, "ProcessFiles.Log.TargetNotOverwritten", sourceFilename, targetFilename ) );
            }
          }
          break;
        case ProcessFilesMeta.OPERATION_TYPE_MOVE:
          if ( ( ( meta.isOverwriteTargetFile() && data.targetFile.exists() ) || !data.targetFile.exists() )
            && !meta.simulate ) {
            data.sourceFile.moveTo( HopVfs.getFileObject( targetFilename ));
            if ( log.isDetailed() ) {
              logDetailed( BaseMessages.getString(
                PKG, "ProcessFiles.Log.SourceFileMoved", sourceFilename, targetFilename ) );
            }
          } else {
            if ( log.isDetailed() ) {
              logDetailed( BaseMessages.getString(
                PKG, "ProcessFiles.Log.TargetNotOverwritten", sourceFilename, targetFilename ) );
            }
          }
          break;
        case ProcessFilesMeta.OPERATION_TYPE_DELETE:
          if ( !meta.simulate ) {
            if ( !data.sourceFile.delete() ) {
              throw new HopException( BaseMessages.getString(
                PKG, "ProcessFiles.Error.CanNotDeleteFile", data.sourceFile.toString() ) );
            }
          }
          if ( log.isDetailed() ) {
            logDetailed( BaseMessages.getString( PKG, "ProcessFiles.Log.SourceFileDeleted", sourceFilename ) );
          }
          break;
        default:

          break;
      }

      // add filename to result filenames?
      if ( meta.isaddTargetFileNametoResult()
        && meta.getOperationType() != ProcessFilesMeta.OPERATION_TYPE_DELETE
        && data.sourceFile.getType() == FileType.FILE ) {
        // Add this to the result file names...
        ResultFile resultFile =
          new ResultFile( ResultFile.FILE_TYPE_GENERAL, data.targetFile, getPipelineMeta().getName(), getTransformName() );
        resultFile.setComment( BaseMessages.getString( PKG, "ProcessFiles.Log.FileAddedResult" ) );
        addResultFile( resultFile );

        if ( log.isDetailed() ) {
          logDetailed( BaseMessages.getString( PKG, "ProcessFiles.Log.FilenameAddResult", data.sourceFile
            .toString() ) );
        }
      }

      putRow( getInputRowMeta(), r ); // copy row to possible alternate rowset(s).

      if ( checkFeedback( getLinesRead() ) ) {
        if ( log.isBasic() ) {
          logBasic( BaseMessages.getString( PKG, "ProcessFiles.LineNumber" ) + getLinesRead() );
        }
      }
    } catch ( Exception e ) {
      boolean sendToErrorRow = false;
      String errorMessage = null;

      if ( getTransformMeta().isDoingErrorHandling() ) {
        sendToErrorRow = true;
        errorMessage = e.toString();
      } else {
        logError( BaseMessages.getString( PKG, "ProcessFiles.ErrorInTransformRunning" ) + e.getMessage() );
        setErrors( 1 );
        stopAll();
        setOutputDone(); // signal end to receiver(s)
        return false;
      }
      if ( sendToErrorRow ) {
        // Simply add this row to the error row
        putError( getInputRowMeta(), r, 1, errorMessage, null, "ProcessFiles001" );
      }
    }

    return true;
  }

  private class TextOneToOneFileSelector implements FileSelector {
    public boolean includeFile( FileSelectInfo info ) throws Exception {
      return true;
    }

    public boolean traverseDescendents( FileSelectInfo info ) {
      return false;
    }
  }

  public boolean init() {

    if ( super.init() ) {
      return true;
    }
    return false;
  }

}
