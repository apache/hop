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

package org.apache.hop.pipeline.transforms.binaryfileoutput;

import java.io.OutputStream;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.ResultFile;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.lineage.LineageFileIoEmitter;
import org.apache.hop.lineage.model.FileIoOperation;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;

/** Write raw binary field content to a file. Filename is taken from an input field per row. */
public class BinaryFileOutput extends BaseTransform<BinaryFileOutputMeta, BinaryFileOutputData> {
  private static final Class<?> PKG = BinaryFileOutputMeta.class;

  public BinaryFileOutput(
      TransformMeta transformMeta,
      BinaryFileOutputMeta meta,
      BinaryFileOutputData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public boolean processRow() throws HopException {
    Object[] r = getRow();
    if (r == null) {
      setOutputDone();
      return false;
    }

    if (first) {
      first = false;
      processFirst();
    }

    try {
      writeBinaryFile(r);
      putRow(data.outputRowMeta, r);
      incrementLinesOutput();

      if (checkFeedback(getLinesRead()) && isBasic()) {
        logBasic(
            BaseMessages.getString(
                PKG, "BinaryFileOutput.LineNumber", Long.toString(getLinesRead())));
      }
    } catch (Exception e) {
      if (getTransformMeta().isDoingErrorHandling()) {
        putError(data.outputRowMeta, r, 1L, e.toString(), null, "BINARYFILEOUTPUT001");
      } else {
        logError(
            BaseMessages.getString(PKG, "BinaryFileOutput.ErrorInTransformRunning", e.getMessage()),
            e);
        setErrors(1);
        stopAll();
        setOutputDone();
        return false;
      }
    }

    return true;
  }

  private void processFirst() throws HopException {
    data.outputRowMeta = getInputRowMeta().clone();

    if (Utils.isEmpty(meta.getBinaryField())) {
      throw new HopException(
          BaseMessages.getString(PKG, "BinaryFileOutput.Error.BinaryFieldMissing"));
    }
    if (Utils.isEmpty(meta.getFilenameField())) {
      throw new HopException(
          BaseMessages.getString(PKG, "BinaryFileOutput.Error.FilenameFieldMissing"));
    }

    data.indexOfBinaryField = getInputRowMeta().indexOfValue(meta.getBinaryField());
    if (data.indexOfBinaryField < 0) {
      throw new HopException(
          BaseMessages.getString(
              PKG, "BinaryFileOutput.Exception.CouldnotFindField", meta.getBinaryField()));
    }

    data.indexOfFilenameField = getInputRowMeta().indexOfValue(meta.getFilenameField());
    if (data.indexOfFilenameField < 0) {
      throw new HopException(
          BaseMessages.getString(
              PKG, "BinaryFileOutput.Exception.CouldnotFindField", meta.getFilenameField()));
    }
  }

  private void writeBinaryFile(Object[] r) throws HopException {
    String filename = getInputRowMeta().getString(r, data.indexOfFilenameField);
    if (Utils.isEmpty(filename)) {
      throw new HopException(BaseMessages.getString(PKG, "BinaryFileOutput.Error.FilenameEmpty"));
    }

    byte[] content = getInputRowMeta().getBinary(r, data.indexOfBinaryField);
    if (content == null) {
      throw new HopException(BaseMessages.getString(PKG, "BinaryFileOutput.Error.BinaryNull"));
    }

    try (FileObject file = HopVfs.getFileObject(filename, variables)) {
      ensureParentFolder(file);
      if (file.exists() && !meta.isOverwriteFile()) {
        throw new HopException(
            BaseMessages.getString(PKG, "BinaryFileOutput.Error.FileExists", filename));
      }

      try (OutputStream out = HopVfs.getOutputStream(file, false)) {
        out.write(content);
      }

      long written = content.length;
      dataVolumeOut = (dataVolumeOut != null ? dataVolumeOut : 0L) + written;

      if (!data.isBeamContext() && written > 0) {
        try {
          LineageFileIoEmitter.emitTransformFileIo(
              this, FileIoOperation.WRITE, null, file, written, true, null);
        } catch (Exception ignored) {
          // optional lineage
        }
      }

      if (meta.isAddResultFilenames()) {
        ResultFile resultFile =
            new ResultFile(
                ResultFile.FILE_TYPE_GENERAL,
                file,
                getPipelineMeta().getName(),
                getTransformName());
        resultFile.setComment(BaseMessages.getString(PKG, "BinaryFileOutput.Log.FileAddedResult"));
        addResultFile(resultFile);
        if (isDetailed()) {
          logDetailed(
              BaseMessages.getString(
                  PKG, "BinaryFileOutput.Log.FilenameAddResult", file.toString()));
        }
      }

      if (isDetailed()) {
        logDetailed(
            BaseMessages.getString(
                PKG, "BinaryFileOutput.Log.FileWritten", Long.toString(written), filename));
      }
    } catch (HopException e) {
      throw e;
    } catch (Exception e) {
      throw new HopException(
          BaseMessages.getString(
              PKG, "BinaryFileOutput.Error.WriteFailed", filename, e.getMessage()),
          e);
    }
  }

  private void ensureParentFolder(FileObject file) throws Exception {
    FileObject parent = file.getParent();
    if (parent == null) {
      return;
    }
    try {
      if (!parent.exists()) {
        if (!meta.isCreateParentFolder()) {
          throw new HopException(
              BaseMessages.getString(
                  PKG, "BinaryFileOutput.Error.ParentFolderNotExists", parent.toString()));
        }
        parent.createFolder();
        if (isDetailed()) {
          logDetailed(
              BaseMessages.getString(
                  PKG, "BinaryFileOutput.Log.ParentFolderCreated", parent.toString()));
        }
      }
    } finally {
      parent.close();
    }
  }
}
