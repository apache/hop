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
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
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
  public boolean processRow() throws HopException {
    Object[] row = getRow();
    if (row == null) {
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
      int outputIndex = data.outputRowMeta.indexOfValue(meta.getOutputField());
      if (outputIndex >= 0) {
        outputRow[outputIndex] = vcardText;
      }
      putRow(data.outputRowMeta, outputRow);
    }

    return true;
  }

  private void writeToFile(Object[] row, String vcardText) throws HopTransformException {
    try {
      String filename = resolveFileName(row);
      createParentFolder(filename);
      try (OutputStream out = HopVfs.getOutputStream(filename, meta.getFileSettings().isAppend())) {
        out.write(vcardText.getBytes(outputCharset()));
      }
      if (meta.isAddingToResult()) {
        ResultFile resultFile =
            new ResultFile(
                ResultFile.FILE_TYPE_GENERAL,
                HopVfs.getFileObject(filename, this),
                getPipelineMeta().getName(),
                getTransformName());
        addResultFile(resultFile);
      }
    } catch (Exception e) {
      throw new HopTransformException(
          BaseMessages.getString(PKG, "VCardOutput.Exception.WriteFailed", e.getMessage()), e);
    }
  }

  private String resolveFileName(Object[] row) throws HopException {
    String filename;
    if (meta.isFileNameInField()) {
      filename = getInputRowMeta().getString(row, data.fileNameFieldIndex);
    } else {
      filename = meta.getFileSettings().buildFilename(this, false);
    }
    if (Utils.isEmpty(filename)) {
      throw new HopException(BaseMessages.getString(PKG, "VCardOutput.Exception.FileNameMissing"));
    }
    return resolve(filename);
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
