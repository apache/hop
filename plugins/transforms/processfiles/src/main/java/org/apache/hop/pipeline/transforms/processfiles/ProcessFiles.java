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

package org.apache.hop.pipeline.transforms.processfiles;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSelectInfo;
import org.apache.commons.vfs2.FileSelector;
import org.apache.commons.vfs2.FileType;
import org.apache.hop.core.Const;
import org.apache.hop.core.ResultFile;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.io.CountingInputStream;
import org.apache.hop.core.io.CountingOutputStream;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.lineage.LineageFileIoEmitter;
import org.apache.hop.lineage.model.FileIoOperation;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;

public class ProcessFiles extends BaseTransform<ProcessFilesMeta, ProcessFilesData> {
  private static final Class<?> PKG = ProcessFilesMeta.class;

  private static final int COPY_BUFFER_SIZE = 8192;

  public ProcessFiles(
      TransformMeta transformMeta,
      ProcessFilesMeta meta,
      ProcessFilesData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public boolean processRow() throws HopException {

    Object[] r = getRow(); // Get row from input rowset & set row busy!
    if (r == null) { // no more input to be expected...

      setOutputDone();
      return false;
    }
    if (first) {
      first = false;
      // Check is source filename field is provided
      if (Utils.isEmpty(meta.getSourceFilenameField())) {
        throw new HopException(
            BaseMessages.getString(PKG, "ProcessFiles.Error.SourceFilenameFieldMissing"));
      }
      // Check is target filename field is provided
      if (meta.getOperationType() != ProcessFilesMeta.OPERATION_TYPE_DELETE
          && Utils.isEmpty(meta.getTargetFilenameField())) {
        throw new HopException(
            BaseMessages.getString(PKG, "ProcessFiles.Error.TargetFilenameFieldMissing"));
      }

      // cache the position of the source filename field
      if (data.indexOfSourceFilename < 0) {
        data.indexOfSourceFilename = getInputRowMeta().indexOfValue(meta.getSourceFilenameField());
        if (data.indexOfSourceFilename < 0) {
          // The field is unreachable !
          throw new HopException(
              BaseMessages.getString(
                  PKG, "ProcessFiles.Exception.CouldnotFindField", meta.getSourceFilenameField()));
        }
      }
      // cache the position of the source filename field
      if (meta.getOperationType() != ProcessFilesMeta.OPERATION_TYPE_DELETE
          && data.indexOfTargetFilename < 0) {
        data.indexOfTargetFilename = getInputRowMeta().indexOfValue(meta.getTargetFilenameField());
        if (data.indexOfTargetFilename < 0) {
          // The field is unreachable !
          throw new HopException(
              BaseMessages.getString(
                  PKG, "ProcessFiles.Exception.CouldnotFindField", meta.getTargetFilenameField()));
        }
      }

      if (meta.simulate && isBasic()) {
        logBasic(BaseMessages.getString(PKG, "ProcessFiles.Log.SimulationModeON"));
      }
    } // End If first
    try {
      // get source filename
      String sourceFilename = getInputRowMeta().getString(r, data.indexOfSourceFilename);

      if (Utils.isEmpty(sourceFilename)) {
        logError(BaseMessages.getString(PKG, "ProcessFiles.Error.SourceFileEmpty"));
        throw new HopException(BaseMessages.getString(PKG, "ProcessFiles.Error.SourceFileEmpty"));
      }
      data.sourceFile = HopVfs.getFileObject(sourceFilename, variables);

      if (!data.sourceFile.exists()) {
        logError(
            BaseMessages.getString(PKG, "ProcessFiles.Error.SourceFileNotExist", sourceFilename));
        throw new HopException(
            BaseMessages.getString(PKG, "ProcessFiles.Error.SourceFileNotExist", sourceFilename));
      }
      if (data.sourceFile.getType() != FileType.FILE) {
        logError(
            BaseMessages.getString(PKG, "ProcessFiles.Error.SourceFileNotFile", sourceFilename));
        throw new HopException(
            BaseMessages.getString(PKG, "ProcessFiles.Error.SourceFileNotFile", sourceFilename));
      }
      String targetFilename = null;
      if (meta.getOperationType() != ProcessFilesMeta.OPERATION_TYPE_DELETE) {
        // get value for target filename
        targetFilename = getInputRowMeta().getString(r, data.indexOfTargetFilename);

        if (Utils.isEmpty(targetFilename)) {
          logError(BaseMessages.getString(PKG, "ProcessFiles.Error.TargetFileEmpty"));
          throw new HopException(BaseMessages.getString(PKG, "ProcessFiles.Error.TargetFileEmpty"));
        }
        data.targetFile = HopVfs.getFileObject(targetFilename, variables);
        if (data.targetFile.exists()) {
          if (isDetailed()) {
            logDetailed(
                BaseMessages.getString(
                    PKG, "ProcessFiles.Log.TargetFileExists", friendlyPath(targetFilename)));
          }
          // check if target is really a file otherwise it could overwrite a complete folder by copy
          // or move operations
          if (data.targetFile.getType() != FileType.FILE) {
            logError(
                BaseMessages.getString(
                    PKG, "ProcessFiles.Error.TargetFileNotFile", targetFilename));
            throw new HopException(
                BaseMessages.getString(
                    PKG, "ProcessFiles.Error.TargetFileNotFile", targetFilename));
          }

        } else {
          // let's check parent folder
          FileObject parentFolder = data.targetFile.getParent();
          if (!parentFolder.exists()) {
            if (!meta.isCreateParentFolder()) {
              throw new HopException(
                  BaseMessages.getString(
                      PKG,
                      "ProcessFiles.Error.TargetParentFolderNotExists",
                      parentFolder.toString()));
            } else {
              parentFolder.createFolder();
              if (isDetailed()) {
                logDetailed(
                    BaseMessages.getString(
                        PKG,
                        "ProcessFiles.Log.Detail.ParentFolderCreated",
                        friendlyPath(parentFolder.toString())));
              }
            }
          }
          if (parentFolder != null) {
            parentFolder.close();
          }
        }
      }

      if (isDetailed()) {
        String targetForLog =
            meta.getOperationType() == ProcessFilesMeta.OPERATION_TYPE_DELETE
                ? "-"
                : friendlyPath(targetFilename);
        logDetailed(
            BaseMessages.getString(
                PKG,
                "ProcessFiles.Log.Detail.RowHeader",
                operationTypeLabel(),
                friendlyPath(sourceFilename),
                targetForLog));
      }

      switch (meta.getOperationType()) {
        case ProcessFilesMeta.OPERATION_TYPE_COPY:
          processCopyOperation(sourceFilename, targetFilename);
          break;
        case ProcessFilesMeta.OPERATION_TYPE_MOVE:
          processMoveOperation(sourceFilename, targetFilename);
          break;
        case ProcessFilesMeta.OPERATION_TYPE_DELETE:
          processDeleteOperation(sourceFilename);
          break;
        default:
          break;
      }

      // add filename to result filenames?
      if (meta.isAddResultFilenames()
          && meta.getOperationType() != ProcessFilesMeta.OPERATION_TYPE_DELETE
          && data.targetFile.getType() == FileType.FILE) {
        // Add this to the result file names...
        ResultFile resultFile =
            new ResultFile(
                ResultFile.FILE_TYPE_GENERAL,
                data.targetFile,
                getPipelineMeta().getName(),
                getTransformName());
        resultFile.setComment(BaseMessages.getString(PKG, "ProcessFiles.Log.FileAddedResult"));
        addResultFile(resultFile);

        if (isDetailed()) {
          logDetailed(
              BaseMessages.getString(
                  PKG,
                  "ProcessFiles.Log.FilenameAddResult",
                  friendlyPath(data.targetFile.toString())));
        }
      }

      putRow(getInputRowMeta(), r); // copy row to possible alternate rowset(s).

      if (checkFeedback(getLinesRead()) && isBasic()) {
        logBasic(
            BaseMessages.getString(PKG, "ProcessFiles.LineNumber", Long.toString(getLinesRead())));
      }
    } catch (Exception e) {
      boolean sendToErrorRow = false;
      String errorMessage = null;

      if (getTransformMeta().isDoingErrorHandling()) {
        sendToErrorRow = true;
        errorMessage = e.toString();
      } else {
        logError(
            BaseMessages.getString(PKG, "ProcessFiles.ErrorInTransformRunning", e.getMessage()), e);
        setErrors(1);
        stopAll();
        setOutputDone(); // signal end to receiver(s)
        return false;
      }
      if (sendToErrorRow) {
        // Simply add this row to the error row
        putError(getInputRowMeta(), r, 1, errorMessage, null, "ProcessFiles001");
      }
    }

    return true;
  }

  private String operationTypeLabel() {
    switch (meta.getOperationType()) {
      case ProcessFilesMeta.OPERATION_TYPE_COPY:
        return BaseMessages.getString(PKG, "ProcessFilesMeta.operationType.Copy");
      case ProcessFilesMeta.OPERATION_TYPE_MOVE:
        return BaseMessages.getString(PKG, "ProcessFilesMeta.operationType.Move");
      case ProcessFilesMeta.OPERATION_TYPE_DELETE:
        return BaseMessages.getString(PKG, "ProcessFilesMeta.operationType.Delete");
      default:
        return "?";
    }
  }

  /** Friendly URI for log lines (variables resolved). */
  private String friendlyPath(String path) {
    if (Utils.isEmpty(path)) {
      return "";
    }
    try {
      return HopVfs.getFriendlyURI(path, variables);
    } catch (Exception e) {
      return path;
    }
  }

  private void processCopyOperation(String sourceFilename, String targetFilename) throws Exception {
    boolean targetFileExists =
        data.targetFile.exists() && data.targetFile.getType() == FileType.FILE;
    boolean shouldPerform =
        !data.targetFile.exists() || (meta.isOverwriteTargetFile() && targetFileExists);

    if (meta.simulate) {
      if (shouldPerform && isDetailed()) {
        logDetailed(
            BaseMessages.getString(
                PKG,
                "ProcessFiles.Log.Detail.WouldCopy",
                friendlyPath(sourceFilename),
                friendlyPath(targetFilename)));
      }
      if (!shouldPerform && isDetailed()) {
        logDetailed(
            BaseMessages.getString(
                PKG,
                "ProcessFiles.Log.Detail.SimulateSkipCopyExists",
                friendlyPath(targetFilename),
                friendlyPath(sourceFilename)));
      }
      return;
    }

    if (shouldPerform) {
      if (meta.isOverwriteTargetFile() && data.targetFile.exists() && isBasic()) {
        logBasic(
            BaseMessages.getString(
                PKG, "ProcessFiles.Log.Basic.OverwriteWarningCopy", friendlyPath(targetFilename)));
      }
      if (isDetailed()) {
        logDetailed(
            BaseMessages.getString(
                PKG,
                "ProcessFiles.Log.Detail.CopyStarted",
                friendlyPath(sourceFilename),
                friendlyPath(targetFilename)));
      }
      // Delete target first; some providers do not truncate correctly on overwrite.
      data.targetFile.delete();
      if (isDataVolumeMetricEnabled()) {
        copyFileTrackingVolume(data.sourceFile, data.targetFile);
      } else {
        data.targetFile.copyFrom(data.sourceFile, new TextOneToOneFileSelector());
      }
      if (isBasic()) {
        logBasic(
            BaseMessages.getString(
                PKG,
                "ProcessFiles.Log.Basic.FileCopied",
                friendlyPath(sourceFilename),
                friendlyPath(targetFilename)));
      }
      if (isDetailed()) {
        logDetailed(
            BaseMessages.getString(
                PKG,
                "ProcessFiles.Log.Detail.FileCopied",
                friendlyPath(sourceFilename),
                friendlyPath(targetFilename)));
      }
      emitCopyLineage();
    } else if (isDetailed()) {
      logDetailed(
          BaseMessages.getString(
              PKG,
              "ProcessFiles.Log.Detail.SkippedCopyExistsNoOverwrite",
              friendlyPath(targetFilename),
              friendlyPath(sourceFilename)));
    }
  }

  private void processMoveOperation(String sourceFilename, String targetFilename) throws Exception {
    boolean targetFileExists =
        data.targetFile.exists() && data.targetFile.getType() == FileType.FILE;
    boolean shouldPerform =
        !data.targetFile.exists() || (meta.isOverwriteTargetFile() && targetFileExists);

    if (meta.simulate) {
      if (shouldPerform && isDetailed()) {
        logDetailed(
            BaseMessages.getString(
                PKG,
                "ProcessFiles.Log.Detail.WouldMove",
                friendlyPath(sourceFilename),
                friendlyPath(targetFilename)));
      }
      if (!shouldPerform && isDetailed()) {
        logDetailed(
            BaseMessages.getString(
                PKG,
                "ProcessFiles.Log.Detail.SimulateSkipMoveExists",
                friendlyPath(targetFilename),
                friendlyPath(sourceFilename)));
      }
      return;
    }

    if (shouldPerform) {
      if (meta.isOverwriteTargetFile() && data.targetFile.exists() && isBasic()) {
        logBasic(
            BaseMessages.getString(
                PKG, "ProcessFiles.Log.Basic.OverwriteWarningMove", friendlyPath(targetFilename)));
      }
      if (isDetailed()) {
        logDetailed(
            BaseMessages.getString(
                PKG,
                "ProcessFiles.Log.Detail.MoveStarted",
                friendlyPath(sourceFilename),
                friendlyPath(targetFilename)));
      }
      String sourceUri = vfsUriString(data.sourceFile);
      String targetUri = vfsUriString(data.targetFile);
      Long lineageBytes = nullableContentSize(data.sourceFile);
      long movedBytes = 0;
      if (isDataVolumeMetricEnabled()) {
        movedBytes = safeContentSize(data.sourceFile);
      }
      data.sourceFile.moveTo(HopVfs.getFileObject(targetFilename, variables));
      if (movedBytes > 0) {
        dataVolumeIn = (dataVolumeIn != null ? dataVolumeIn : 0L) + movedBytes;
        dataVolumeOut = (dataVolumeOut != null ? dataVolumeOut : 0L) + movedBytes;
      }
      if (isBasic()) {
        logBasic(
            BaseMessages.getString(
                PKG,
                "ProcessFiles.Log.Basic.FileMoved",
                friendlyPath(sourceFilename),
                friendlyPath(targetFilename)));
      }
      if (isDetailed()) {
        logDetailed(
            BaseMessages.getString(
                PKG,
                "ProcessFiles.Log.Detail.FileMoved",
                friendlyPath(sourceFilename),
                friendlyPath(targetFilename)));
      }
      emitMoveLineageAfterSuccess(sourceUri, targetUri, lineageBytes);
    } else if (isDetailed()) {
      logDetailed(
          BaseMessages.getString(
              PKG,
              "ProcessFiles.Log.Detail.SkippedMoveExistsNoOverwrite",
              friendlyPath(targetFilename),
              friendlyPath(sourceFilename)));
    }
  }

  private void processDeleteOperation(String sourceFilename) throws Exception {
    if (meta.simulate) {
      if (isDetailed()) {
        logDetailed(
            BaseMessages.getString(
                PKG, "ProcessFiles.Log.Detail.WouldDelete", friendlyPath(sourceFilename)));
      }
      return;
    }
    String sourceUri = vfsUriString(data.sourceFile);
    Long deleteBytes = nullableContentSize(data.sourceFile);
    if (!data.sourceFile.delete()) {
      throw new HopException(
          BaseMessages.getString(
              PKG, "ProcessFiles.Error.CanNotDeleteFile", data.sourceFile.toString()));
    }
    emitDeleteLineage(sourceUri, deleteBytes);
    if (isBasic()) {
      logBasic(
          BaseMessages.getString(
              PKG, "ProcessFiles.Log.Basic.FileDeleted", friendlyPath(sourceFilename)));
    }
    if (isDetailed()) {
      logDetailed(
          BaseMessages.getString(
              PKG, "ProcessFiles.Log.Detail.FileDeleted", friendlyPath(sourceFilename)));
    }
  }

  private void emitCopyLineage() {
    if (meta.simulate) {
      return;
    }
    Long bytes = nullableContentSize(data.targetFile);
    if (bytes == null) {
      bytes = nullableContentSize(data.sourceFile);
    }
    LineageFileIoEmitter.emitTransformFileIo(
        this, FileIoOperation.COPY, data.sourceFile, data.targetFile, bytes, true, null);
  }

  private void emitMoveLineageAfterSuccess(String sourceUri, String targetUri, Long bytes) {
    if (meta.simulate || sourceUri == null || targetUri == null) {
      return;
    }
    LineageFileIoEmitter.emitTransformFileIo(
        this, FileIoOperation.MOVE, sourceUri, targetUri, bytes, true, null);
  }

  private void emitDeleteLineage(String sourceUri, Long bytes) {
    if (meta.simulate || sourceUri == null) {
      return;
    }
    LineageFileIoEmitter.emitTransformFileIo(
        this, FileIoOperation.DELETE, sourceUri, null, bytes, true, null);
  }

  private static Long nullableContentSize(FileObject file) {
    long s = safeContentSize(file);
    return s > 0 ? s : null;
  }

  private static String vfsUriString(FileObject file) {
    if (file == null) {
      return null;
    }
    try {
      return file.getName().getURI();
    } catch (Exception e) {
      return null;
    }
  }

  private boolean isDataVolumeMetricEnabled() {
    return getPipeline() != null
        && Const.toBoolean(getPipeline().getVariable(Const.HOP_METRIC_DATA_VOLUME, "N"));
  }

  private void copyFileTrackingVolume(FileObject source, FileObject destination)
      throws IOException {
    FileObject parent = destination.getParent();
    if (parent != null && !parent.exists()) {
      parent.createFolder();
    }
    long read;
    long written;
    try (InputStream rawIn = HopVfs.getInputStream(source);
        CountingInputStream in = new CountingInputStream(rawIn);
        OutputStream rawOut = HopVfs.getOutputStream(destination, false);
        CountingOutputStream out = new CountingOutputStream(rawOut)) {
      byte[] buffer = new byte[COPY_BUFFER_SIZE];
      int len;
      while ((len = in.read(buffer)) != -1) {
        if (len > 0) {
          out.write(buffer, 0, len);
        }
      }
      out.flush();
      read = in.getCount();
      written = out.getCount();
    }
    dataVolumeIn = (dataVolumeIn != null ? dataVolumeIn : 0L) + read;
    dataVolumeOut = (dataVolumeOut != null ? dataVolumeOut : 0L) + written;
  }

  private static long safeContentSize(FileObject file) {
    try {
      if (file != null && file.getType().hasContent()) {
        return Math.max(0L, file.getContent().getSize());
      }
    } catch (Exception ignored) {
      // metrics only
    }
    return 0L;
  }

  private class TextOneToOneFileSelector implements FileSelector {
    @Override
    public boolean includeFile(FileSelectInfo info) throws Exception {
      return true;
    }

    @Override
    public boolean traverseDescendents(FileSelectInfo info) {
      return false;
    }
  }
}
