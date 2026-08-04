/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hop.pipeline.transforms.sftpput;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.NameScope;
import org.apache.commons.vfs2.Selectors;
import org.apache.hop.core.Const;
import org.apache.hop.core.ResultFile;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.sftpput.SftpPutMeta.AfterSftpPut;
import org.apache.hop.vfs.sftp.SftpConnections;
import org.apache.hop.vfs.sftp.metadata.SftpConnection;

/** Uploads files or field content to the server of a named SFTP connection. */
public class SftpPut extends BaseTransform<SftpPutMeta, SftpPutData> {

  private static final Class<?> PKG = SftpPutMeta.class;

  public SftpPut(
      TransformMeta transformMeta,
      SftpPutMeta meta,
      SftpPutData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public boolean processRow() throws HopException {

    Object[] row = getRow();
    if (row == null) {
      logUploadSummary();
      setOutputDone();
      return false;
    }

    if (first) {
      first = false;
      data.inputRowMeta = getInputRowMeta();
      data.connection = SftpConnections.load(getMetadataProvider(), resolve(meta.getConnection()));

      // The name of the connection is the VFS scheme we upload through. Asking for the file system
      // manager with our variables registers the connections of this project if nothing did that
      // yet.
      //
      if (!HopVfs.getFileSystemManager(this).hasProvider(data.connection.getName())) {
        throw new HopException(
            BaseMessages.getString(
                PKG, "SftpPut.Error.ConnectionNotRegistered", data.connection.getName()));
      }

      data.indexOfSourceFileFieldName =
          indexOfField(meta.getSourceFileFieldName(), "SftpPut.Error.SourceFileNameFieldMissing");
      data.indexOfRemoteDirectory =
          indexOfField(
              meta.getRemoteDirectoryFieldName(), "SftpPut.Error.RemoteFolderNameFieldMissing");

      if (!Utils.isEmpty(meta.getRemoteFilenameFieldName())) {
        data.indexOfRemoteFilename =
            indexOfField(meta.getRemoteFilenameFieldName(), "SftpPut.Error.CanNotFindField");
      } else if (meta.isInputIsStream()) {
        // Without a file to take the name from, the name has to come from a field.
        throw new HopTransformException(
            BaseMessages.getString(PKG, "SftpPut.Error.RemoteFilenameFieldMissing"));
      }

      if (meta.getAfterSftpPut() == AfterSftpPut.MOVE) {
        data.indexOfMoveToFolderFieldName =
            indexOfField(
                meta.getDestinationFolderFieldName(),
                "SftpPut.Error.DestinationFolderFieldMissing");
      }

      logConnection();
    }

    try {
      upload(row);
      putRow(data.inputRowMeta, row);
    } catch (Exception e) {
      if (getTransformMeta().isDoingErrorHandling()) {
        if (isDebug()) {
          logDebug(
              BaseMessages.getString(
                  PKG, "SftpPut.Log.RowSentToErrorStream", getLinesRead(), e.getMessage()));
        }
        putError(data.inputRowMeta, row, 1, e.toString(), null, "SFTPPUT001");
      } else {
        logError(BaseMessages.getString(PKG, "SftpPut.Log.ErrorInTransform"), e);
        setErrors(1);
        stopAll();
        setOutputDone();
        return false;
      }
    }

    if (checkFeedback(getLinesRead()) && isBasic()) {
      logBasic(BaseMessages.getString(PKG, "SftpPut.Log.LineNumber") + getLinesRead());
    }
    return true;
  }

  /**
   * Which connection are we on, on which server, as who? With variables in the metadata that's the
   * first thing you want to see in the log.
   */
  private void logConnection() {
    if (!isDetailed()) {
      return;
    }
    SftpConnection connection = data.connection;
    logDetailed(
        BaseMessages.getString(
            PKG,
            "SftpPut.Log.ConnectedTo",
            connection.getName(),
            Const.NVL(resolve(connection.getServerName()), ""),
            Const.NVL(
                resolve(connection.getServerPort()), Integer.toString(SftpConnection.DEFAULT_PORT)),
            Const.NVL(resolve(connection.getUsername()), "")));
    logDetailed(
        BaseMessages.getString(
            PKG,
            "SftpPut.Log.UsingFields",
            Const.NVL(resolve(meta.getSourceFileFieldName()), ""),
            Const.NVL(resolve(meta.getRemoteDirectoryFieldName()), "-"),
            Const.NVL(resolve(meta.getRemoteFilenameFieldName()), "-"),
            meta.getAfterSftpPut().getDescription()));
  }

  private void logUploadSummary() {
    if (data.connection == null || !isDetailed()) {
      return;
    }
    logDetailed(
        BaseMessages.getString(
            PKG,
            "SftpPut.Log.Summary",
            Long.toString(data.uploadedFiles),
            Long.toString(data.uploadedBytes),
            data.connection.getName()));
  }

  private void upload(Object[] row) throws HopException {
    String sourceData = data.inputRowMeta.getString(row, data.indexOfSourceFileFieldName);
    if (Utils.isEmpty(sourceData)) {
      throw new HopTransformException(BaseMessages.getString(PKG, "SftpPut.Error.SourceDataEmpty"));
    }

    String remoteFolder = data.inputRowMeta.getString(row, data.indexOfRemoteDirectory);
    String remoteFilename =
        data.indexOfRemoteFilename < 0
            ? null
            : data.inputRowMeta.getString(row, data.indexOfRemoteFilename);

    if (meta.isInputIsStream()) {
      byte[] content = data.inputRowMeta.getBinary(row, data.indexOfSourceFileFieldName);
      if (isDebug()) {
        logDebug(
            BaseMessages.getString(
                PKG,
                "SftpPut.Log.UploadingField",
                data.inputRowMeta.getValueMeta(data.indexOfSourceFileFieldName).getName(),
                Integer.toString(content == null ? 0 : content.length),
                SftpConnections.buildUri(data.connection, remoteFolder, remoteFilename)));
      }
      try (InputStream inputStream = new ByteArrayInputStream(content)) {
        copyToRemote(inputStream, remoteFolder, remoteFilename);
      } catch (HopException e) {
        throw e;
      } catch (Exception e) {
        throw new HopException(e);
      }
      return;
    }

    try (FileObject sourceFile = HopVfs.getFileObject(sourceData, this)) {
      if (!sourceFile.exists()) {
        throw new HopTransformException(
            BaseMessages.getString(
                PKG, "SftpPut.Error.CanNotFindFile", sourceFile.getPublicURIString()));
      }
      String targetName =
          Utils.isEmpty(remoteFilename) ? sourceFile.getName().getBaseName() : remoteFilename;
      if (isDebug()) {
        logDebug(
            BaseMessages.getString(
                PKG,
                "SftpPut.Log.UploadingFile",
                sourceFile.getPublicURIString(),
                SftpConnections.buildUri(data.connection, remoteFolder, targetName)));
      }
      try (InputStream inputStream = HopVfs.getInputStream(sourceFile)) {
        copyToRemote(inputStream, remoteFolder, targetName);
      }

      finishTheJob(sourceFile, sourceData, row);
    } catch (HopException e) {
      throw e;
    } catch (Exception e) {
      throw new HopException(e);
    }
  }

  private void copyToRemote(InputStream inputStream, String remoteFolder, String remoteFilename)
      throws HopException {
    if (Utils.isEmpty(remoteFilename)) {
      throw new HopTransformException(
          BaseMessages.getString(PKG, "SftpPut.Error.RemoteFilenameEmpty"));
    }

    String targetUri = SftpConnections.buildUri(data.connection, remoteFolder, remoteFilename);
    try {
      createRemoteFolder(remoteFolder);

      long startTime = System.currentTimeMillis();
      long bytes;
      try (FileObject targetFile = HopVfs.getFileObject(targetUri, this);
          OutputStream outputStream = HopVfs.getOutputStream(targetFile, false)) {
        bytes = inputStream.transferTo(outputStream);
      }
      data.uploadedFiles++;
      data.uploadedBytes += bytes;

      if (isDetailed()) {
        logDetailed(
            BaseMessages.getString(
                PKG,
                "SftpPut.Log.FileUploaded",
                targetUri,
                Long.toString(bytes),
                Long.toString(System.currentTimeMillis() - startTime)));
      }
    } catch (HopException e) {
      throw e;
    } catch (Exception e) {
      throw new HopException(
          BaseMessages.getString(PKG, "SftpPut.Error.UnableToUpload", targetUri), e);
    }
  }

  private void createRemoteFolder(String remoteFolder) throws HopException {
    if (Utils.isEmpty(remoteFolder)) {
      // No folder given: the file lands in the folder the connection starts in.
      return;
    }
    String folderUri = SftpConnections.buildUri(data.connection, remoteFolder, null);
    try (FileObject folder = HopVfs.getFileObject(folderUri, this)) {
      if (folder.exists()) {
        if (isDebug()) {
          logDebug(BaseMessages.getString(PKG, "SftpPut.Log.RemoteFolderExists", folderUri));
        }
        return;
      }
      if (!meta.isCreateRemoteFolder()) {
        throw new HopException(
            BaseMessages.getString(PKG, "SftpPut.Error.CanNotFindRemoteFolder", remoteFolder));
      }
      folder.createFolder();
      if (isDetailed()) {
        logDetailed(BaseMessages.getString(PKG, "SftpPut.Log.RemoteFolderCreated", remoteFolder));
      }
    } catch (HopException e) {
      throw e;
    } catch (Exception e) {
      throw new HopException(
          BaseMessages.getString(PKG, "SftpPut.Error.CanNotFindRemoteFolder", remoteFolder), e);
    }
  }

  /** Delete or move the source file, now that it's on the server. */
  private void finishTheJob(FileObject sourceFile, String sourceData, Object[] row)
      throws HopException {
    try {
      switch (meta.getAfterSftpPut()) {
        case DELETE:
          sourceFile.delete();
          if (isDebug()) {
            logDebug(
                BaseMessages.getString(
                    PKG, "SftpPut.Log.DeletedFile", sourceFile.getPublicURIString()));
          }
          // Deliberately no result file: there's no file left to point at.
          break;
        case MOVE:
          moveSourceFile(sourceFile, row);
          break;
        default:
          addToResult(sourceFile);
          break;
      }
    } catch (HopException e) {
      throw e;
    } catch (Exception e) {
      throw new HopException(e);
    }
  }

  private void addToResult(FileObject file) {
    if (!meta.isAddFilenameToResult()) {
      return;
    }
    ResultFile resultFile =
        new ResultFile(
            ResultFile.FILE_TYPE_GENERAL, file, getPipelineMeta().getName(), getTransformName());
    resultFile.setComment(
        BaseMessages.getString(PKG, "SftpPut.Log.FilenameAddedToResultFilenames"));
    addResultFile(resultFile);
  }

  private void moveSourceFile(FileObject sourceFile, Object[] row) throws Exception {
    String destinationFolderName =
        data.inputRowMeta.getString(row, data.indexOfMoveToFolderFieldName);
    if (Utils.isEmpty(destinationFolderName)) {
      throw new HopTransformException(
          BaseMessages.getString(PKG, "SftpPut.Error.MoveToDestinationFolderIsEmpty"));
    }
    try (FileObject destinationFolder = HopVfs.getFileObject(destinationFolderName, this)) {
      if (!destinationFolder.exists()) {
        if (!meta.isCreateDestinationFolder()) {
          throw new HopTransformException(
              BaseMessages.getString(PKG, "SftpPut.Error.CanNotFindFolder", destinationFolderName));
        }
        destinationFolder.createFolder();
      }
      try (FileObject destination =
          destinationFolder.resolveFile(sourceFile.getName().getBaseName(), NameScope.CHILD)) {
        move(sourceFile, destination);
        addToResult(destination);
        if (isDebug()) {
          logDebug(
              BaseMessages.getString(
                  PKG,
                  "SftpPut.Log.FileMoved",
                  sourceFile.getPublicURIString(),
                  destination.getPublicURIString()));
        }
      }
    }
  }

  /**
   * Move a file, also when the two folders sit on different file systems of the operating system.
   *
   * <p>Two local folders are one and the same file system as far as VFS is concerned, whichever
   * disks they're on, so {@link FileObject#moveTo(FileObject)} always picks a rename. A rename
   * across a mount point is exactly what the OS refuses to do, so fall back to a copy followed by a
   * delete. See <a href="https://github.com/apache/hop/issues/5936">issue #5936</a>.
   */
  private void move(FileObject sourceFile, FileObject destination) throws HopException {
    try {
      sourceFile.moveTo(destination);
    } catch (FileSystemException renameFailed) {
      try {
        destination.copyFrom(sourceFile, Selectors.SELECT_SELF);
        sourceFile.delete();
      } catch (Exception copyFailed) {
        renameFailed.addSuppressed(copyFailed);
        throw new HopException(
            BaseMessages.getString(
                PKG,
                "SftpPut.Error.UnableToMove",
                sourceFile.getPublicURIString(),
                destination.getPublicURIString()),
            renameFailed);
      }
    }
  }

  private int indexOfField(String fieldName, String missingMessageKey)
      throws HopTransformException {
    String resolved = resolve(fieldName);
    if (Utils.isEmpty(resolved)) {
      throw new HopTransformException(BaseMessages.getString(PKG, missingMessageKey));
    }
    int index = data.inputRowMeta.indexOfValue(resolved);
    if (index < 0) {
      throw new HopTransformException(
          BaseMessages.getString(PKG, "SftpPut.Error.CanNotFindField", resolved));
    }
    return index;
  }
}
