/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * http://www.project-hop.org
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

package org.apache.hop.pipeline.transforms.sftpput;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.Const;
import org.apache.hop.core.ResultFile;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopJobException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.vfs.HopVFS;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.workflow.actions.sftp.SFTPClient;
import org.apache.hop.workflow.actions.sftpput.JobEntrySFTPPUT;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.ITransform;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Send file to SFTP host.
 *
 * @author Samatar Hassan
 * @since 30-April-2012
 */
public class SftpPut extends BaseTransform implements ITransform {
  private static final Class<?> PKG = SFTPPutMeta.class; // Needed by Translator

  private SFTPPutMeta meta;
  private SFTPPutData data;

  public SftpPut(TransformMeta transformMeta, ITransformData data, int copyNr, PipelineMeta pipelineMeta,
                 Pipeline pipeline ) {
    super( transformMeta, meta, data, copyNr, pipelineMeta, pipeline );
  }

  public boolean processRow() throws HopException {
    meta = (SFTPPutMeta) smi;
    data = (SFTPPutData) sdi;

    boolean sendToErrorRow = false;
    String errorMessage = null;

    Object[] r = getRow(); // get row, set busy!
    if ( r == null ) { // no more input to be expected...

      setOutputDone();
      return false;
    }

    if ( first ) {
      // Go there only for the first row received
      first = false;

      try {
        // String substitution..
        String realServerName = environmentSubstitute( meta.getServerName() );
        String realServerPort = environmentSubstitute( meta.getServerPort() );
        String realUsername = environmentSubstitute( meta.getUserName() );
        String realPassword =
          Encr.decryptPasswordOptionallyEncrypted( environmentSubstitute( meta.getPassword() ) );
        String realKeyFilename = null;
        String realPassPhrase = null;

        if ( meta.isUseKeyFile() ) {
          // We must have here a private keyfilename
          realKeyFilename = environmentSubstitute( meta.getKeyFilename() );
          if ( Utils.isEmpty( realKeyFilename ) ) {
            // Error..Missing keyfile
            logError( BaseMessages.getString( PKG, "SFTPPut.Error.KeyFileMissing" ) );
            return false;
          }
          if ( !HopVFS.fileExists( realKeyFilename ) ) {
            // Error.. can not reach keyfile
            logError( BaseMessages.getString( PKG, "SFTPPut.Error.KeyFileNotFound", realKeyFilename ) );
            return false;
          }
          realPassPhrase = environmentSubstitute( meta.getKeyPassPhrase() );
        }

        // Let's try to establish SFTP connection....
        // Create sftp client to host ...
        data.sftpclient =
          createSftpClient( realServerName, realServerPort, realUsername, realKeyFilename, realPassPhrase );

        // connection successfully established
        if ( isDetailed() ) {
          logDetailed( BaseMessages.getString(
            PKG, "SFTPPUT.Log.OpenedConnection", realServerName, realServerPort, realUsername ) );
        }

        // Set compression
        data.sftpclient.setCompression( meta.getCompression() );

        // Set proxy?
        String realProxyHost = environmentSubstitute( meta.getProxyHost() );
        if ( !Utils.isEmpty( realProxyHost ) ) {
          // Set proxy
          data.sftpclient.setProxy(
            realProxyHost, environmentSubstitute( meta.getProxyPort() ), environmentSubstitute( meta
              .getProxyUsername() ), environmentSubstitute( meta.getProxyPassword() ), meta.getProxyType() );
        }

        // login to ftp host ...
        data.sftpclient.login( realPassword );

      } catch ( Exception e ) {
        throw new HopException( BaseMessages.getString( PKG, "SFTPPUT.Error.Connection" ), e );
      }

      // Let's perform some checks

      checkSourceFileField( meta.getSourceFileFieldName(), data );

      checkRemoteFoldernameField( meta.getRemoteDirectoryFieldName(), data );

      checkRemoteFilenameField( meta.getRemoteFilenameFieldName(), data );

      // Move to folder
      if ( meta.getAfterFTPS() == JobEntrySFTPPUT.AFTER_FTPSPUT_MOVE ) {
        checkDestinationFolderField( meta.getDestinationFolderFieldName(), data );
      }
    }

    // Read data top upload
    String sourceData = getInputRowMeta().getString( r, data.indexOfSourceFileFieldName );

    InputStream inputStream = null;
    FileObject destinationFolder = null;
    String destinationFilename;
    FileObject file = null;

    try {
      if ( Utils.isEmpty( sourceData ) ) {
        // Source data is empty
        throw new HopTransformException( BaseMessages.getString( PKG, "SFTPPut.Error.SourceDataEmpty" ) );
      }

      if ( meta.isInputStream() ) {
        // Source data is a stream
        inputStream = new ByteArrayInputStream( getInputRowMeta().getBinary( r, data.indexOfSourceFileFieldName ) );

        if ( data.indexOfRemoteFilename == -1 ) {
          // for the case when put data is transferred via input field
          // it is mandatory to specify remote file name
          throw new HopTransformException( BaseMessages.getString( PKG, "SFTPPut.Error.RemoteFilenameFieldMissing" ) );
        }
        destinationFilename = getInputRowMeta().getString( r, data.indexOfRemoteFilename );
      } else {
        // source data is a file
        // let's check file
        file = HopVFS.getFileObject( sourceData );

        if ( !file.exists() ) {
          // We can not find file
          throw new HopTransformException( BaseMessages.getString( PKG, "SFTPPut.Error.CanNotFindField", sourceData ) );
        }
        // get stream from file
        inputStream = HopVFS.getInputStream( file );

        // Destination filename
        destinationFilename = file.getName().getBaseName();
      }

      if ( file != null ) {
        if ( meta.getAfterFTPS() == JobEntrySFTPPUT.AFTER_FTPSPUT_MOVE ) {
          String realDestationFolder = getInputRowMeta().getString( r, data.indexOfMoveToFolderFieldName );

          if ( Utils.isEmpty( realDestationFolder ) ) {
            // Move to destination folder is empty
            throw new HopTransformException( BaseMessages.getString(
              PKG, "SFTPPut.Error.MoveToDestinationFolderIsEmpty" ) );
          }

          destinationFolder = HopVFS.getFileObject( realDestationFolder );

          if ( !destinationFolder.exists() ) {
            // We can not find folder
            throw new HopTransformException( BaseMessages.getString(
              PKG, "SFTPPut.Error.CanNotFindFolder", realDestationFolder ) );
          }
        }
      }

      // move to spool dir ...
      setSFTPDirectory( getInputRowMeta().getString( r, data.indexOfRemoteDirectory ) );

      // Upload a stream
      data.sftpclient.put( inputStream, destinationFilename );

      if ( file != null ) {
        // We successfully uploaded the file
        // what's next ...
        finishTheJob( file, sourceData, destinationFolder );
      }

      putRow( getInputRowMeta(), r ); // copy row to possible alternate rowset(s).

      if ( checkFeedback( getLinesRead() ) ) {
        if ( isDetailed() ) {
          logDetailed( BaseMessages.getString( PKG, "SFTPPut.Log.LineNumber" ) + getLinesRead() );
        }
      }
    } catch ( Exception e ) {

      if ( getTransformMeta().isDoingErrorHandling() ) {
        sendToErrorRow = true;
        errorMessage = e.toString();
      } else {

        logError( BaseMessages.getString( PKG, "SFTPPut.Log.ErrorInTransform" ), e );
        setErrors( 1 );
        stopAll();
        setOutputDone(); // signal end to receiver(s)
        return false;
      }

      if ( sendToErrorRow ) {
        // Simply add this row to the error row
        putError( getInputRowMeta(), r, 1, errorMessage, null, "SFTPPUT001" );
      }
    } finally {
      try {
        if ( destinationFolder != null ) {
          destinationFolder.close();
        }
        if ( file != null ) {
          file.close();
        }
        if ( inputStream != null ) {
          inputStream.close();
        }
      } catch ( Exception e ) {
        // ignore this
      }
    }
    return true;
  }

  @VisibleForTesting
  void checkSourceFileField( String sourceFilenameFieldName, SFTPPutData data ) throws HopTransformException {
    // Sourcefilename field
    sourceFilenameFieldName = environmentSubstitute( sourceFilenameFieldName );
    if ( Utils.isEmpty( sourceFilenameFieldName ) ) {
      // source filename field is missing
      throw new HopTransformException( BaseMessages.getString( PKG, "SFTPPut.Error.SourceFileNameFieldMissing" ) );
    }

    data.indexOfSourceFileFieldName = getInputRowMeta().indexOfValue( sourceFilenameFieldName );
    if ( data.indexOfSourceFileFieldName == -1 ) {
      // source filename field is missing
      throw new HopTransformException( BaseMessages.getString(
        PKG, "SFTPPut.Error.CanNotFindField", sourceFilenameFieldName ) );
    }
  }

  @VisibleForTesting
  void checkRemoteFoldernameField( String remoteFoldernameFieldName, SFTPPutData data ) throws HopTransformException {
    // Remote folder fieldname
    remoteFoldernameFieldName = environmentSubstitute( remoteFoldernameFieldName );
    if ( Utils.isEmpty( remoteFoldernameFieldName ) ) {
      // remote folder field is missing
      throw new HopTransformException( BaseMessages.getString( PKG, "SFTPPut.Error.RemoteFolderNameFieldMissing" ) );
    }

    data.indexOfRemoteDirectory = getInputRowMeta().indexOfValue( remoteFoldernameFieldName );
    if ( data.indexOfRemoteDirectory == -1 ) {
      // remote foldername field is missing
      throw new HopTransformException( BaseMessages.getString(
        PKG, "SFTPPut.Error.CanNotFindField", remoteFoldernameFieldName ) );
    }
  }

  @VisibleForTesting
  void checkRemoteFilenameField( String remoteFilenameFieldName, SFTPPutData data )
    throws HopTransformException {
    remoteFilenameFieldName = environmentSubstitute( remoteFilenameFieldName );
    if ( !Utils.isEmpty( remoteFilenameFieldName ) ) {
      data.indexOfRemoteFilename = getInputRowMeta().indexOfValue( remoteFilenameFieldName );
      if ( data.indexOfRemoteFilename == -1 ) {
        // remote file name field is set, but was not found
        throw new HopTransformException( BaseMessages.getString(
          PKG, "SFTPPut.Error.CanNotFindField", remoteFilenameFieldName ) );
      }
    }
  }

  @VisibleForTesting
  void checkDestinationFolderField( String realDestinationFoldernameFieldName, SFTPPutData data ) throws HopTransformException {
    realDestinationFoldernameFieldName = environmentSubstitute( realDestinationFoldernameFieldName );
    if ( Utils.isEmpty( realDestinationFoldernameFieldName ) ) {
      throw new HopTransformException( BaseMessages.getString(
        PKG, "SFTPPut.Log.DestinatFolderNameFieldNameMissing" ) );
    }

    data.indexOfMoveToFolderFieldName = getInputRowMeta().indexOfValue( realDestinationFoldernameFieldName );
    if ( data.indexOfMoveToFolderFieldName == -1 ) {
      // move to folder field is missing
      throw new HopTransformException( BaseMessages.getString(
        PKG, "SFTPPut.Error.CanNotFindField", realDestinationFoldernameFieldName ) );
    }
  }

  @VisibleForTesting
  SFTPClient createSftpClient( String realServerName, String realServerPort, String realUsername,
                               String realKeyFilename, String realPassPhrase )
    throws HopJobException, UnknownHostException {
    return new SFTPClient(
      InetAddress.getByName( realServerName ), Const.toInt( realServerPort, 22 ), realUsername,
      realKeyFilename, realPassPhrase );
  }

  protected void finishTheJob( FileObject file, String sourceData, FileObject destinationFolder ) throws HopException {
    try {
      switch ( meta.getAfterFTPS() ) {
        case JobEntrySFTPPUT.AFTER_FTPSPUT_DELETE:
          // Delete source file
          if ( !file.exists() ) {
            file.delete();
            if ( isDetailed() ) {
              logDetailed( BaseMessages.getString( PKG, "SFTPPut.Log.DeletedFile", sourceData ) );
            }
          }
          break;
        case JobEntrySFTPPUT.AFTER_FTPSPUT_MOVE:
          // Move source file
          FileObject destination = null;
          try {
            destination =
              HopVFS.getFileObject( destinationFolder.getName().getBaseName()
                + Const.FILE_SEPARATOR + file.getName().getBaseName(), this );
            file.moveTo( destination );
            if ( isDetailed() ) {
              logDetailed( BaseMessages.getString( PKG, "SFTPPut.Log.FileMoved", file, destination ) );
            }
          } finally {
            if ( destination != null ) {
              destination.close();
            }
          }
          break;
        default:
          if ( meta.isAddFilenameResut() ) {
            // Add this to the result file names...
            ResultFile resultFile =
              new ResultFile( ResultFile.FILE_TYPE_GENERAL, file, getPipelineMeta().getName(), getTransformName() );
            resultFile.setComment( BaseMessages.getString( PKG, "SFTPPut.Log.FilenameAddedToResultFilenames" ) );
            addResultFile( resultFile );
            if ( isDetailed() ) {
              logDetailed( BaseMessages.getString( PKG, "SFTPPut.Log.FilenameAddedToResultFilenames", sourceData ) );
            }
          }
          break;
      }
    } catch ( Exception e ) {
      throw new HopException( e );
    }
  }

  protected void setSFTPDirectory( String spoolDirectory ) throws HopException {

    boolean existfolder = data.sftpclient.folderExists( spoolDirectory );
    if ( !existfolder ) {
      if ( !meta.isCreateRemoteFolder() ) {
        throw new HopException( BaseMessages.getString(
          PKG, "SFTPPut.Error.CanNotFindRemoteFolder", spoolDirectory ) );
      }
      if ( isDetailed() ) {
        logDetailed( BaseMessages.getString( PKG, "SFTPPut.Error.CanNotFindRemoteFolder", spoolDirectory ) );
      }

      // Let's create folder
      data.sftpclient.createFolder( spoolDirectory );
      if ( isDetailed() ) {
        logDetailed( BaseMessages.getString( PKG, "SFTPPut.Log.RemoteFolderCreated", spoolDirectory ) );
      }
    }
    data.sftpclient.chdir( spoolDirectory );

    if ( isDetailed() ) {
      logDetailed( BaseMessages.getString( PKG, "SFTPPut.Log.ChangedDirectory", spoolDirectory ) );
    }
  }

  public void.dispose() {
    meta = (SFTPPutMeta) smi;
    data = (SFTPPutData) sdi;

    try {
      // Close SFTP connection
      if ( data.sftpclient != null ) {
        data.sftpclient.disconnect();
      }
    } catch ( Exception e ) {
      // Ignore errors
    }
    super.dispose();
  }
}
