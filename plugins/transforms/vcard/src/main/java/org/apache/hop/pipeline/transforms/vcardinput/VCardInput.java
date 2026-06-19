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
package org.apache.hop.pipeline.transforms.vcardinput;

import ezvcard.VCard;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import org.apache.commons.io.IOUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.hop.core.ResultFile;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.vcard.VCardFieldMapping;
import org.apache.hop.pipeline.transforms.vcard.VCardMapper;

public class VCardInput extends BaseTransform<VCardInputMeta, VCardInputData> {

  private static final Class<?> PKG = VCardInput.class;

  public VCardInput(
      TransformMeta transformMeta,
      VCardInputMeta meta,
      VCardInputData data,
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
    if (!meta.getFileInput().isAcceptingFilenames()) {
      data.files = meta.getFileInputList(this);
      if (data.files.nrOfFiles() == 0) {
        if (meta.isDoNotFailIfNoFile()) {
          logBasic(BaseMessages.getString(PKG, "VCardInput.Log.NoFiles"));
        } else {
          logError(BaseMessages.getString(PKG, "VCardInput.Exception.NoFiles"));
          return false;
        }
      }
    }
    return true;
  }

  @Override
  public boolean processRow() throws HopException {
    if (first) {
      first = false;
      prepareOutputRowMeta();
    }
    return processFromFiles();
  }

  private void prepareOutputRowMeta() throws HopException {
    IRowMeta inputMeta = getInputRowMeta();
    data.outputRowMeta = new RowMeta();
    if (meta.getFileInput().isAcceptingFilenames()) {
      if (inputMeta == null) {
        throw new HopException(BaseMessages.getString(PKG, "VCardInput.Exception.NoInputStream"));
      }
      data.acceptFieldIndex = inputMeta.indexOfValue(meta.getFileInput().getAcceptingField());
      if (data.acceptFieldIndex < 0) {
        throw new HopException(
            BaseMessages.getString(
                PKG,
                "VCardInput.Exception.AcceptFieldNotFound",
                meta.getFileInput().getAcceptingField()));
      }
      if (meta.getFileInput().isPassingThruFields()) {
        data.outputRowMeta = inputMeta.clone();
      }
    }
    meta.getFields(data.outputRowMeta, getTransformName(), null, null, this, metadataProvider);
    data.filenr = 0;
    clearPendingCards();
  }

  private boolean processFromFiles() throws HopException {
    if (hasPendingCards()) {
      outputNextPendingCard();
      return true;
    }

    if (meta.getFileInput().isAcceptingFilenames()) {
      Object[] row = getRow();
      if (row == null) {
        setOutputDone();
        return false;
      }
      data.currentInputRow = meta.getFileInput().isPassingThruFields() ? row : null;
      String path = resolve(getInputRowMeta().getString(row, data.acceptFieldIndex));
      loadCardsFromPath(path);
      if (!hasPendingCards()) {
        return true;
      }
      outputNextPendingCard();
      return true;
    }

    return processStaticFileList();
  }

  private boolean processStaticFileList() throws HopException {
    while (true) {
      if (hasPendingCards()) {
        outputNextPendingCard();
        return true;
      }
      if (data.filenr >= data.files.nrOfFiles()) {
        setOutputDone();
        return false;
      }
      if (!openAndParseNextFile()) {
        data.filenr++;
        continue;
      }
      if (!hasPendingCards()) {
        data.filenr++;
      }
    }
  }

  private boolean openAndParseNextFile() throws HopTransformException {
    closeCurrentFile();
    data.file = data.files.getFile(data.filenr);
    data.currentFilename = HopVfs.getFilename(data.file);
    try {
      if (!data.file.exists()) {
        logError(BaseMessages.getString(PKG, "VCardInput.Log.FileNotFound", data.currentFilename));
        setErrors(getErrors() + 1);
        return false;
      }
      long size = data.file.getContent().getSize();
      if (size == 0) {
        if (meta.isIgnoringEmptyFile()) {
          logBasic(BaseMessages.getString(PKG, "VCardInput.Log.EmptyFile", data.currentFilename));
          return false;
        }
        logError(BaseMessages.getString(PKG, "VCardInput.Log.EmptyFile", data.currentFilename));
        setErrors(getErrors() + 1);
        return false;
      }
      if (isDetailed()) {
        logDetailed(
            BaseMessages.getString(PKG, "VCardInput.Log.OpeningFile", data.currentFilename));
      }
      String vcardText = readFileContent(data.file);
      parseCards(vcardText);
      if (meta.getFileInput().isAddingResult()) {
        addResultFile(data.file);
      }
      return true;
    } catch (Exception e) {
      throw new HopTransformException(
          BaseMessages.getString(
              PKG, "VCardInput.Exception.ReadFileFailed", data.currentFilename, e.getMessage()),
          e);
    }
  }

  private void loadCardsFromPath(String path) throws HopTransformException {
    clearPendingCards();
    if (Utils.isEmpty(path)) {
      return;
    }
    FileObject file = null;
    try {
      file = HopVfs.getFileObject(path, variables);
      data.currentFilename = HopVfs.getFilename(file);
      if (!file.exists()) {
        logError(BaseMessages.getString(PKG, "VCardInput.Log.FileNotFound", data.currentFilename));
        setErrors(getErrors() + 1);
        return;
      }
      long size = file.getContent().getSize();
      if (size == 0) {
        if (meta.isIgnoringEmptyFile()) {
          logBasic(BaseMessages.getString(PKG, "VCardInput.Log.EmptyFile", data.currentFilename));
        } else {
          logError(BaseMessages.getString(PKG, "VCardInput.Log.EmptyFile", data.currentFilename));
          setErrors(getErrors() + 1);
        }
        return;
      }
      String vcardText = readFileContent(file);
      parseCards(vcardText);
      if (meta.getFileInput().isAddingResult()) {
        addResultFile(file);
      }
    } catch (Exception e) {
      throw new HopTransformException(
          BaseMessages.getString(PKG, "VCardInput.Exception.ReadFileFailed", path, e.getMessage()),
          e);
    } finally {
      closeQuietly(file);
    }
  }

  private void parseCards(String vcardText) throws HopTransformException {
    clearPendingCards();
    if (Utils.isEmpty(vcardText)) {
      return;
    }
    try {
      data.pendingCards = VCardMapper.parseAll(vcardText);
      data.pendingCardIndex = 0;
    } catch (Exception e) {
      throw new HopTransformException(
          BaseMessages.getString(PKG, "VCardInput.Exception.ParseFailed", e.getMessage()), e);
    }
  }

  private void outputNextPendingCard() throws HopTransformException {
    putMappedRow(data.currentInputRow, data.pendingCards.get(data.pendingCardIndex++));
    if (data.pendingCardIndex >= data.pendingCards.size()) {
      clearPendingCards();
      if (!meta.getFileInput().isAcceptingFilenames()) {
        data.filenr++;
      }
    }
  }

  private boolean hasPendingCards() {
    return data.pendingCards != null && data.pendingCardIndex < data.pendingCards.size();
  }

  private void clearPendingCards() {
    data.pendingCards = null;
    data.pendingCardIndex = 0;
  }

  private String readFileContent(FileObject file) throws Exception {
    Charset charset = charset();
    try (InputStream inputStream = HopVfs.getInputStream(file)) {
      return IOUtils.toString(inputStream, charset);
    }
  }

  private Charset charset() {
    String encoding = resolve(meta.getEncoding());
    if (Utils.isEmpty(encoding)) {
      return StandardCharsets.UTF_8;
    }
    return Charset.forName(encoding);
  }

  private void putMappedRow(Object[] inputRow, VCard card) throws HopTransformException {
    Object[] outputRow;
    if (meta.getFileInput().isPassingThruFields() && inputRow != null) {
      outputRow = RowDataUtil.resizeArray(inputRow, data.outputRowMeta.size());
    } else {
      outputRow = RowDataUtil.allocateRowData(data.outputRowMeta.size());
    }

    for (VCardFieldMapping mapping : meta.getFieldMappings()) {
      if (mapping == null || Utils.isEmpty(mapping.getHopField())) {
        continue;
      }
      int index = data.outputRowMeta.indexOfValue(mapping.getHopField());
      if (index >= 0) {
        outputRow[index] = VCardMapper.extractValue(card, mapping);
      }
    }

    if (meta.isIncludeFilename() && !Utils.isEmpty(meta.getFilenameField())) {
      int index = data.outputRowMeta.indexOfValue(meta.getFilenameField());
      if (index >= 0) {
        outputRow[index] = data.currentFilename;
      }
    }

    putRow(data.outputRowMeta, outputRow);
  }

  private void addResultFile(FileObject file) throws FileSystemException {
    ResultFile resultFile =
        new ResultFile(
            ResultFile.FILE_TYPE_GENERAL, file, getPipelineMeta().getName(), getTransformName());
    resultFile.setComment(BaseMessages.getString(PKG, "VCardInput.Log.ResultFileWasRead"));
    addResultFile(resultFile);
  }

  private void closeCurrentFile() {
    closeQuietly(data.file);
    data.file = null;
  }

  private void closeQuietly(FileObject file) {
    if (file != null) {
      try {
        file.close();
      } catch (FileSystemException e) {
        logError(
            BaseMessages.getString(PKG, "VCardInput.Log.UnableToCloseFile", file.getName()), e);
      }
    }
  }

  @Override
  public void dispose() {
    closeCurrentFile();
    super.dispose();
  }
}
