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

package org.apache.hop.pipeline.transforms.zipfile;

import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileType;
import org.apache.hop.core.Const;
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

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.zip.Deflater;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

/**
 * Zip file *
 *
 * @author Samatar
 * @since 03-Juin-2008
 */

public class ZipFile extends BaseTransform<ZipFileMeta,ZipFileData> implements ITransform<ZipFileMeta, ZipFileData> {
  private static final Class<?> PKG = ZipFileMeta.class; // For Translator

  public ZipFile(TransformMeta transformMeta, ZipFileMeta meta, ZipFileData data, int copyNr, PipelineMeta pipelineMeta,
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

      data.outputRowMeta = getInputRowMeta().clone();
      meta.getFields( data.outputRowMeta, getTransformName(), null, null, this, getPipeline().getMetadataProvider() );

      // Check is source filename field is provided
      if ( Utils.isEmpty( meta.getDynamicSourceFileNameField() ) ) {
        throw new HopException( BaseMessages.getString( PKG, "ZipFile.Error.SourceFilenameFieldMissing" ) );
      }
      // Check is target filename field is provided
      if ( Utils.isEmpty( meta.getDynamicTargetFileNameField() ) ) {
        throw new HopException( BaseMessages.getString( PKG, "ZipFile.Error.TargetFilenameFieldMissing" ) );
      }

      // cache the position of the source filename field
      if ( data.indexOfSourceFilename < 0 ) {
        data.indexOfSourceFilename = getInputRowMeta().indexOfValue( meta.getDynamicSourceFileNameField() );
        if ( data.indexOfSourceFilename < 0 ) {
          // The field is unreachable !
          throw new HopException( BaseMessages.getString( PKG, "ZipFile.Exception.CouldnotFindField", meta
            .getDynamicSourceFileNameField() ) );
        }
      }

      data.indexOfZipFilename = getInputRowMeta().indexOfValue( meta.getDynamicTargetFileNameField() );
      if ( data.indexOfZipFilename < 0 ) {
        // The field is unreachable !
        throw new HopException( BaseMessages.getString( PKG, "ZipFile.Exception.CouldnotFindField", meta
          .getDynamicTargetFileNameField() ) );
      }

      if ( meta.isKeepSouceFolder() ) {
        if ( !Utils.isEmpty( meta.getBaseFolderField() ) ) {
          // cache the position of the source filename field
          data.indexOfBaseFolder = getInputRowMeta().indexOfValue( meta.getBaseFolderField() );
          if ( data.indexOfBaseFolder < 0 ) {
            // The field is unreachable !
            throw new HopException( BaseMessages.getString( PKG, "ZipFile.Exception.CouldnotFindField", meta
              .getBaseFolderField() ) );
          }
        }
      }

      // Move to folder
      if ( meta.getOperationType() == ZipFileMeta.OPERATION_TYPE_MOVE ) {
        if ( Utils.isEmpty( meta.getMoveToFolderField() ) ) {
          throw new HopException( BaseMessages.getString( PKG, "ZipFile.Exception.EmptyMovetoFolder" ) );
        }
        data.indexOfMoveToFolder = getInputRowMeta().indexOfValue( meta.getMoveToFolderField() );
        if ( data.indexOfMoveToFolder < 0 ) {
          // The field is unreachable !
          throw new HopException( BaseMessages.getString( PKG, "ZipFile.Exception.CouldnotFindField", meta
            .getMoveToFolderField() ) );
        }
      }

    } // End If first

    boolean sendToErrorRow = false;
    String errorMessage = null;

    try {
      // get source filename
      String sourceFilename = getInputRowMeta().getString( r, data.indexOfSourceFilename );

      if ( Utils.isEmpty( sourceFilename ) ) {
        log.logError( toString(), BaseMessages.getString( PKG, "ZipFile.Error.SourceFileEmpty" ) );
        throw new HopException( BaseMessages.getString( PKG, "ZipFile.Error.SourceFileEmpty" ) );
      }
      data.sourceFile = HopVfs.getFileObject( sourceFilename );

      // Check sourcefile
      boolean skip = false;
      if ( !data.sourceFile.exists() ) {
        log
          .logError( toString(), BaseMessages
            .getString( PKG, "ZipFile.Error.SourceFileNotExist", sourceFilename ) );
        throw new HopException( BaseMessages
          .getString( PKG, "ZipFile.Error.SourceFileNotExist", sourceFilename ) );
      } else {
        if ( data.sourceFile.getType() != FileType.FILE ) {
          log.logError( toString(), BaseMessages
            .getString( PKG, "ZipFile.Error.SourceFileNotFile", sourceFilename ) );
          throw new HopException( BaseMessages.getString(
            PKG, "ZipFile.Error.SourceFileNotFile", sourceFilename ) );
        }
      }

      // get basefolder
      if ( data.indexOfBaseFolder > -1 ) {
        data.baseFolder = getInputRowMeta().getString( r, data.indexOfBaseFolder );
      }

      // get destination folder
      String moveToFolder = null;
      if ( data.indexOfMoveToFolder > -1 ) {
        moveToFolder = getInputRowMeta().getString( r, data.indexOfMoveToFolder );
        if ( Utils.isEmpty( moveToFolder ) ) {
          throw new HopException( BaseMessages.getString( PKG, "ZipFile.Error.EmptyMoveToFolder" ) );
        }
      }

      if ( !skip ) {
        // get value for target filename
        String targetFilename = getInputRowMeta().getString( r, data.indexOfZipFilename );

        if ( Utils.isEmpty( targetFilename ) ) {
          log.logError( toString(), BaseMessages.getString( PKG, "ZipFile.Error.TargetFileEmpty" ) );
          throw new HopException( BaseMessages.getString( PKG, "ZipFile.Error.TargetFileEmpty" ) );
        }
        data.zipFile = HopVfs.getFileObject( targetFilename );
        if ( data.zipFile.exists() ) {
          if ( log.isDetailed() ) {
            log.logDetailed( toString(), BaseMessages.getString(
              PKG, "ZipFile.Log.TargetFileExists", targetFilename ) );
          }
        } else {
          // let's check parent folder
          FileObject parentFolder = data.zipFile.getParent();
          if ( !parentFolder.exists() ) {
            if ( !meta.isCreateParentFolder() ) {
              // Parent folder not exist
              // So we will fail
              throw new HopException( BaseMessages.getString(
                PKG, "ZipFile.Error.TargetParentFolderNotExists", parentFolder.toString() ) );
            } else {
              // Create parent folder
              parentFolder.createFolder();
            }
          }
          if ( parentFolder != null ) {
            parentFolder.close();
          }
        }

        // Let's zip
        zipFile();

        // file was zipped, let's see if we need to move or delete it
        processFile( moveToFolder );

        // add filename to result filenames?
        addFilenameToResult();
      }

      getLinesInput();
      putRow( data.outputRowMeta, r ); // copy row to output rowset(s);

      if ( checkFeedback( getLinesRead() ) ) {
        if ( log.isDetailed() ) {
          logDetailed( BaseMessages.getString( PKG, "ZipFile.LineNumber", "" + getLinesRead() ) );
        }
      }
    } catch ( Exception e ) {
      if ( getTransformMeta().isDoingErrorHandling() ) {
        sendToErrorRow = true;
        errorMessage = e.toString();
      } else {
        logError( BaseMessages.getString( PKG, "ZipFile.ErrorInTransformRunning" ) + e.getMessage() );
        setErrors( 1 );
        stopAll();
        setOutputDone(); // signal end to receiver(s)
        return false;
      }
      if ( sendToErrorRow ) {
        // Simply add this row to the error row
        putError( getInputRowMeta(), r, 1, errorMessage, null, "ZipFile001" );
      }
    } finally {
      try {
        if ( data.sourceFile != null ) {
          data.sourceFile.close();
        }
        if ( data.zipFile != null ) {
          data.zipFile.close();
        }
      } catch ( Exception e ) { /* Ignore */
      }
    }

    return true;
  }

  private void processFile( String folder ) throws Exception {

    switch ( meta.getOperationType() ) {
      case ZipFileMeta.OPERATION_TYPE_MOVE:
        FileObject file = null;
        FileObject moveToFolder = null;
        try {
          // Move to folder
          moveToFolder = HopVfs.getFileObject( folder );

          if ( moveToFolder.exists() ) {
            if ( moveToFolder.getType() != FileType.FOLDER ) {
              throw new HopException( BaseMessages.getString( PKG, "ZipFile.Error.NotAFolder", folder ) );
            }
          } else {
            moveToFolder.createFolder();
          }

          // get target filename
          String targetfilename =
            HopVfs.getFilename( moveToFolder )
              + Const.FILE_SEPARATOR + data.sourceFile.getName().getBaseName();
          file = HopVfs.getFileObject( targetfilename );

          // Move file
          data.sourceFile.moveTo( file );

        } finally {
          if ( file != null ) {
            try {
              file.close();
            } catch ( Exception e ) { /* Ignore */
            }
          }
          if ( moveToFolder != null ) {
            try {
              moveToFolder.close();
            } catch ( Exception e ) { /* Ignore */
            }
          }
        }
        break;
      case ZipFileMeta.OPERATION_TYPE_DELETE:
        data.sourceFile.delete();
        break;
      default:
        break;
    }
  }

  private void addFilenameToResult() throws FileSystemException {
    if ( meta.isaddTargetFileNametoResult() ) {
      // Add this to the result file names...
      ResultFile resultFile =
        new ResultFile( ResultFile.FILE_TYPE_GENERAL, data.zipFile, getPipelineMeta().getName(), getTransformName() );
      resultFile.setComment( BaseMessages.getString( PKG, "ZipFile.Log.FileAddedResult" ) );
      addResultFile( resultFile );

      if ( log.isDetailed() ) {
        log.logDetailed( toString(), BaseMessages.getString( PKG, "ZipFile.Log.FilenameAddResult", data.sourceFile
          .toString() ) );
      }
    }
  }

  private File getFile( final String filename ) {
    try {
      URI uri = new URI( filename );
      return new File( uri );
    } catch ( URISyntaxException ex ) {
      // Ignore errors
    }
    return new File( filename );
  }

  private void zipFile() throws HopException {

    String localrealZipfilename = HopVfs.getFilename( data.zipFile );
    boolean updateZip = false;

    byte[] buffer = null;
    OutputStream dest = null;
    BufferedOutputStream buff = null;
    ZipOutputStream out = null;
    InputStream in = null;
    ZipInputStream zin = null;
    ZipEntry entry = null;
    File tempFile = null;
    HashSet<String> fileSet = new HashSet<>();

    try {

      updateZip = ( data.zipFile.exists() && meta.isOverwriteZipEntry() );

      if ( updateZip ) {
        // the Zipfile exists
        // and we weed to update entries
        // Let's create a temp file
        File fileZip = getFile( localrealZipfilename );
        tempFile = File.createTempFile( fileZip.getName(), null );
        // delete it, otherwise we cannot rename existing zip to it.
        tempFile.delete();

        updateZip = fileZip.renameTo( tempFile );
      }

      // Prepare Zip File
      buffer = new byte[ 18024 ];
      dest = HopVfs.getOutputStream( localrealZipfilename, false );
      buff = new BufferedOutputStream( dest );
      out = new ZipOutputStream( buff );

      if ( updateZip ) {
        // User want to append files to existing Zip file
        // The idea is to rename the existing zip file to a temporary file
        // and then adds all entries in the existing zip along with the new files,
        // excluding the zip entries that have the same name as one of the new files.

        zin = new ZipInputStream( new FileInputStream( tempFile ) );
        entry = zin.getNextEntry();

        while ( entry != null ) {
          String name = entry.getName();

          if ( !fileSet.contains( name ) ) {

            // Add ZIP entry to output stream.
            out.putNextEntry( new ZipEntry( name ) );
            // Transfer bytes from the ZIP file to the output file
            int len;
            while ( ( len = zin.read( buffer ) ) > 0 ) {
              out.write( buffer, 0, len );
            }

            fileSet.add( name );
          }
          entry = zin.getNextEntry();
        }
        // Close the streams
        zin.close();
      }

      // Set the method
      out.setMethod( ZipOutputStream.DEFLATED );
      out.setLevel( Deflater.BEST_COMPRESSION );

      // Associate a file input stream for the current file
      in = HopVfs.getInputStream( data.sourceFile );

      // Add ZIP entry to output stream.
      //
      String relativeName = data.sourceFile.getName().getBaseName();

      if ( meta.isKeepSouceFolder() ) {
        // Get full filename
        relativeName = HopVfs.getFilename( data.sourceFile );

        if ( data.baseFolder != null ) {
          // Remove base folder
          data.baseFolder += Const.FILE_SEPARATOR;
          relativeName = relativeName.replace( data.baseFolder, "" );
        }
      }
      if ( !fileSet.contains( relativeName ) ) {
        out.putNextEntry( new ZipEntry( relativeName ) );

        int len;
        while ( ( len = in.read( buffer ) ) > 0 ) {
          out.write( buffer, 0, len );
        }
      }
    } catch ( Exception e ) {
      throw new HopException( BaseMessages.getString( PKG, "ZipFile.ErrorCreatingZip" ), e );
    } finally {
      try {
        if ( in != null ) {
          // Close the current file input stream
          in.close();
        }
        if ( out != null ) {
          // Close the ZipOutPutStream
          out.flush();
          out.closeEntry();
          out.close();
        }
        if ( buff != null ) {
          buff.close();
        }
        if ( dest != null ) {
          dest.close();
        }
        // Delete Temp File
        if ( tempFile != null ) {
          tempFile.delete();
        }
        fileSet = null;

      } catch ( Exception e ) { /* Ignore */
      }
    }

  }

  public boolean init() {

    if ( super.init() ) {
      return true;
    }
    return false;
  }

}
