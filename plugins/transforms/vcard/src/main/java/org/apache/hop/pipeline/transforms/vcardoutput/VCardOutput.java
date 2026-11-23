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
package org.apache.hop.pipeline.transforms.vcardoutput;

import ezvcard.VCard;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.ResultFile;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.vcard.VCardMapper;

public class VCardOutput extends BaseTransform<VCardOutputMeta, VCardOutputData> {

  private static final Class<?> PKG = VCardOutput.class;

  private static final int BUFFER_SIZE = 64 * 1024;

  public VCardOutput(
      TransformMeta transformMeta,
      VCardOutputMeta meta,
      VCardOutputData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public boolean init() {
    if (!super.init()) {
      return false;
    }
    try {
      data.charset = outputCharset();
      if (meta.writesToFile() && !meta.isFileNameInField()) {
        // One name for the whole run: the date and time in it are those of the start
        data.staticFileName = resolve(meta.getFileSettings().buildFilename(this, new Date()));
        if (!meta.getFileSettings().isDoNotOpenNewFileInit()
            && !Utils.isEmpty(data.staticFileName)) {
          // The file has to exist even when no row arrives
          openFile(data.staticFileName);
        }
      }
      return true;
    } catch (Exception e) {
      logError(BaseMessages.getString(PKG, "VCardOutput.Exception.WriteFailed", e.getMessage()), e);
      return false;
    }
  }

  @Override
  public void dispose() {
    try {
      closeFile();
    } catch (Exception e) {
      logError(BaseMessages.getString(PKG, "VCardOutput.Exception.WriteFailed", e.getMessage()), e);
    }
    super.dispose();
  }

  @Override
  public boolean processRow() throws HopException {
    Object[] row = getRow();
    if (row == null) {
      // Complete the file before the next transforms are told that this one is done
      try {
        closeFile();
      } catch (IOException e) {
        throw new HopTransformException(
            BaseMessages.getString(PKG, "VCardOutput.Exception.WriteFailed", e.getMessage()), e);
      }
      setOutputDone();
      return false;
    }

    if (first) {
      first = false;
      data.outputRowMeta = getInputRowMeta().clone();
      meta.getFields(data.outputRowMeta, getTransformName(), null, null, this, metadataProvider);
      if (meta.isFileNameInField()) {
        data.fileNameFieldIndex = getInputRowMeta().indexOfValue(meta.getFileNameField());
        if (data.fileNameFieldIndex < 0) {
          throw new HopException(
              BaseMessages.getString(
                  PKG, "VCardOutput.Exception.FileNameFieldNotFound", meta.getFileNameField()));
        }
      }
      if (meta.outputsValue()) {
        data.outputFieldIndex = data.outputRowMeta.indexOfValue(meta.getOutputField());
      }
    }

    VCard card = new VCard();
    VCardMapper.applyMappings(
        card,
        row,
        getInputRowMeta(),
        meta.getFieldMappings(),
        meta.isAddProdId(),
        meta.isAddRevision());
    String vcardText;
    try {
      vcardText = VCardMapper.write(card, meta.getVcardVersion().getVCardVersion());
    } catch (IOException e) {
      throw new HopException("Unable to write vCard", e);
    }

    if (meta.writesToFile()) {
      writeToFile(row, vcardText);
    }

    if (meta.outputsValue()) {
      Object[] outputRow;
      if (meta.isPassInputFields()) {
        outputRow = RowDataUtil.resizeArray(row, data.outputRowMeta.size());
      } else {
        outputRow = RowDataUtil.allocateRowData(data.outputRowMeta.size());
      }
      if (data.outputFieldIndex >= 0) {
        outputRow[data.outputFieldIndex] = vcardText;
      }
      putRow(data.outputRowMeta, outputRow);
    } else if (meta.isPassInputFields() || meta.writesToFile()) {
      putRow(getInputRowMeta(), row);
    }

    incrementLinesOutput();

    return true;
  }

  private void writeToFile(Object[] row, String vcardText) throws HopTransformException {
    try {
      openFile(resolveFileName(row));
      data.outputStream.write(vcardText.getBytes(data.charset));
    } catch (Exception e) {
      throw new HopTransformException(
          BaseMessages.getString(PKG, "VCardOutput.Exception.WriteFailed", e.getMessage()), e);
    }
  }

  /**
   * Makes the given file the open one. The file stays open from row to row; it is only closed when
   * another file name comes along or when the transform ends.
   *
   * <p>The first time a run writes to a file it is created, or emptied unless "Append" is on. A
   * file the run wrote to before is added to, so every card of the run ends up in it.
   */
  private void openFile(String filename) throws Exception {
    if (filename.equals(data.currentFileName)) {
      return;
    }
    closeFile();

    boolean firstTime = data.writtenFiles.add(filename);
    if (firstTime) {
      createParentFolder(filename);
    }
    boolean append = meta.getFileSettings().isAppend() || !firstTime;
    data.outputStream =
        new BufferedOutputStream(HopVfs.getOutputStream(filename, append), BUFFER_SIZE);
    data.currentFileName = filename;

    if (firstTime && meta.isAddingToResult()) {
      ResultFile resultFile =
          new ResultFile(
              ResultFile.FILE_TYPE_GENERAL,
              HopVfs.getFileObject(filename, this),
              getPipelineMeta().getName(),
              getTransformName());
      addResultFile(resultFile);
    }
  }

  private void closeFile() throws IOException {
    if (data.outputStream != null) {
      try {
        data.outputStream.close();
      } finally {
        data.outputStream = null;
        data.currentFileName = null;
      }
    }
  }

  private String resolveFileName(Object[] row) throws HopException {
    String filename;
    if (meta.isFileNameInField()) {
      filename = resolve(getInputRowMeta().getString(row, data.fileNameFieldIndex));
    } else {
      filename = data.staticFileName;
    }
    if (Utils.isEmpty(filename)) {
      throw new HopException(BaseMessages.getString(PKG, "VCardOutput.Exception.FileNameMissing"));
    }
    return filename;
  }

  private Charset outputCharset() {
    String encoding = resolve(meta.getEncoding());
    if (Utils.isEmpty(encoding)) {
      return StandardCharsets.UTF_8;
    }
    return Charset.forName(encoding);
  }

  private void createParentFolder(String filename) throws Exception {
    if (!meta.getFileSettings().isCreateParentFolder()) {
      return;
    }
    FileObject fileObject = HopVfs.getFileObject(filename, this);
    FileObject parent = fileObject.getParent();
    if (parent != null && !parent.exists()) {
      parent.createFolder();
    }
    if (parent != null) {
      parent.close();
    }
    fileObject.close();
  }
}
