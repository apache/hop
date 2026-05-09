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

package org.apache.hop.pipeline.transforms.jsonnormalize;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.BitSet;
import java.util.Map;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.hop.core.ResultFile;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.io.CountingInputStream;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.util.StringUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.file.BaseFileInputTransform;
import org.apache.hop.pipeline.transforms.file.IBaseFileInputReader;
import org.apache.hop.pipeline.transforms.jsoninput.JsonInputField;
import org.apache.hop.pipeline.transforms.jsoninput.JsonInputMeta;
import org.apache.hop.pipeline.transforms.jsoninput.reader.RowOutputConverter;

/**
 * Reads JSON (files, URLs, or fields) and emits one row per array element after pandas-style
 * flattening of each record.
 */
public class JsonNormalizeInput
    extends BaseFileInputTransform<JsonNormalizeInputMeta, JsonNormalizeInputData> {
  private static final Class<?> PKG = JsonNormalizeInputMeta.class;

  private static final byte[] EMPTY_JSON = "{}".getBytes();
  private static final JsonNode EMPTY_OBJECT_NODE = JsonNodeFactory.instance.objectNode();

  private RowOutputConverter rowOutputConverter;

  public JsonNormalizeInput(
      TransformMeta transformMeta,
      JsonNormalizeInputMeta meta,
      JsonNormalizeInputData data,
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
    data.rownr = 1L;
    data.nrInputFields = meta.getInputFields().size();
    data.repeatedFields = new BitSet(data.nrInputFields);
    for (int i = 0; i < data.nrInputFields; i++) {
      JsonInputField field = meta.getInputFields().get(i);
      if (field.isRepeated()) {
        data.repeatedFields.set(i);
      }
    }
    data.resolvedFields = new JsonInputField[data.nrInputFields];
    for (int i = 0; i < data.nrInputFields; i++) {
      JsonInputField field = new JsonInputField(meta.getInputFields().get(i));
      field.setPath(resolve(field.getPath()));
      data.resolvedFields[i] = field;
    }
    return true;
  }

  @Override
  public boolean processRow() throws HopException {
    if (first) {
      first = false;
      prepareToRowProcessing(false);
    } else if (data.indexSourceField == -1 && data.inputRowMeta != null) {
      data.readrow = getRow();
      if (data.readrow != null) {
        throw new HopValueException(
            BaseMessages.getString(JsonInputMeta.class, "JsonInput.Log.ReceivingMultiRows"));
      }
    }

    Object[] outRow;
    try {
      outRow = getOneOutputRow();
      if (outRow == null) {
        setOutputDone();
        return false;
      }

      if (isRowLevel()) {
        logRowlevel(
            BaseMessages.getString(
                PKG, "JsonInput.Log.ReadRow", data.outputRowMeta.getString(outRow)));
      }
      incrementLinesInput();
      data.rownr++;

      putRow(data.outputRowMeta, outRow);

      if (meta.getRowLimit() > 0 && data.rownr > meta.getRowLimit()) {
        setOutputDone();
        return false;
      }

    } catch (Exception e) {
      logError(
          BaseMessages.getString(
              PKG, "JsonNormalizeInput.ErrorInTransformRunning", e.getMessage()));
      if (getTransformMeta().isDoingErrorHandling()) {
        sendErrorRow(e.toString());
      } else {
        setErrors(getErrors() + 1);
        stopAll();
        setOutputDone();
        return false;
      }
    }
    return true;
  }

  @Override
  protected void prepareToRowProcessing(boolean errorIgnored)
      throws HopException, HopTransformException, HopValueException {
    data.readrow = getRow();
    data.inputRowMeta = getInputRowMeta();
    if (!meta.isInFields() && data.inputRowMeta == null) {
      data.outputRowMeta = new RowMeta();
      if (!meta.isDoNotFailIfNoFile() && (data.files == null || data.files.nrOfFiles() == 0)) {
        String errMsg = BaseMessages.getString(JsonInputMeta.class, "JsonInput.Log.NoFiles");
        logError(errMsg);
        inputError(errMsg);
      }
    } else {
      if (data.inputRowMeta == null) {
        data.hasFirstRow = false;
        return;
      }
      data.hasFirstRow = true;
      data.outputRowMeta = data.inputRowMeta.clone();

      if (meta.isInFields() && !StringUtil.isEmpty(meta.getFieldValue())) {
        data.indexSourceField = getInputRowMeta().indexOfValue(meta.getFieldValue());
        if (data.indexSourceField < 0) {
          logError(
              BaseMessages.getString(
                  JsonInputMeta.class, "JsonInput.Log.ErrorFindingField", meta.getFieldValue()));
          throw new HopException(
              BaseMessages.getString(
                  PKG, "JsonInput.Exception.CouldnotFindField", meta.getFieldValue()));
        }
      }

      if (meta.isRemoveSourceField() && data.indexSourceField >= 0) {
        data.outputRowMeta.removeValueMeta(data.indexSourceField);
        data.totalpreviousfields = data.inputRowMeta.size() - 1;
      } else {
        data.totalpreviousfields = data.inputRowMeta.size();
      }
    }
    meta.getFields(data.outputRowMeta, getTransformName(), null, null, this, metadataProvider);

    data.convertRowMeta = data.outputRowMeta.cloneToType(IValueMeta.TYPE_STRING);

    NormalizeInputsReader readerFactory =
        new NormalizeInputsReader(this, meta, data, new InputErrorHandler());
    if (data.inputRowMeta != null
        && data.inputRowMeta.getValueMeta(data.indexSourceField) != null
        && data.inputRowMeta.getValueMeta(data.indexSourceField).isJson()) {
      data.jsonInputs = readerFactory.jsonFieldIterator();
      data.inputs = null;
      data.processingJson = true;
    } else {
      data.inputs = readerFactory.iterator();
      data.jsonInputs = null;
      data.processingJson = false;
    }

    data.currentRecords = null;
    data.recordPointer = 0;
    rowOutputConverter = new RowOutputConverter(getLogChannel());
  }

  private void addFileToResultFilesname(FileObject file) {
    if (meta.addResultFile()) {
      ResultFile resultFile =
          new ResultFile(
              ResultFile.FILE_TYPE_GENERAL, file, getPipelineMeta().getName(), getTransformName());
      resultFile.setComment(
          BaseMessages.getString(JsonInputMeta.class, "JsonInput.Log.FileAddedResult"));
      addResultFile(resultFile);
    }
  }

  public boolean onNewFile(FileObject file) throws FileSystemException {
    if (file == null) {
      String errMsg =
          BaseMessages.getString(JsonInputMeta.class, "JsonInput.Log.IsNotAFile", "null");
      logError(errMsg);
      inputError(errMsg);
      return false;
    } else if (!file.exists()) {
      String errMsg =
          BaseMessages.getString(
              JsonInputMeta.class, "JsonInput.Log.IsNotAFile", file.getName().getFriendlyURI());
      logError(errMsg);
      inputError(errMsg);
      return false;
    }
    if (hasAdditionalFileFields()) {
      fillFileAdditionalFields(data, file);
    }
    if (file.getContent().getSize() == 0) {
      if (meta.isIgnoringEmptyFile()) {
        logBasic(
            BaseMessages.getString(
                JsonInputMeta.class, "JsonInput.Error.FileSizeZero", "" + file.getName()));
      } else {
        logError(
            BaseMessages.getString(
                JsonInputMeta.class, "JsonInput.Error.FileSizeZero", "" + file.getName()));
        setErrors(getErrors() + 1);
        return false;
      }
    }
    return true;
  }

  @Override
  protected void fillFileAdditionalFields(JsonNormalizeInputData data, FileObject file)
      throws FileSystemException {
    super.fillFileAdditionalFields(data, file);
    data.filename = HopVfs.getFilename(file);
    data.filenr++;
    if (isDetailed()) {
      logDetailed(
          BaseMessages.getString(
              JsonInputMeta.class, "JsonInput.Log.OpeningFile", file.toString()));
    }
    addFileToResultFilesname(file);
  }

  private void incrementErrors() {
    setErrors(getErrors() + 1);
  }

  private void inputError(String errorMsg) {
    if (getTransformMeta().isDoingErrorHandling()) {
      sendErrorRow(errorMsg);
    } else {
      incrementErrors();
    }
  }

  @Override
  public boolean failAfterBadFile(String errorMsg, boolean errorIgnored, boolean skipBadFiles) {
    return true;
  }

  private class InputErrorHandler implements NormalizeInputsReader.ErrorHandler {
    @Override
    public void error(Exception e) {
      logError(
          BaseMessages.getString(
              JsonInputMeta.class, "JsonInput.Log.UnexpectedError", e.toString()));
      setErrors(getErrors() + 1);
    }

    @Override
    public void fileOpenError(FileObject file, FileSystemException e) {
      String msg =
          BaseMessages.getString(
              PKG,
              "JsonInput.Log.UnableToOpenFile",
              "" + data.filenr,
              file.toString(),
              e.toString());
      logError(msg);
      inputError(msg);
    }

    @Override
    public void fileCloseError(FileObject file, FileSystemException e) {
      error(e);
    }
  }

  private Object[] getOneOutputRow() throws HopException {
    if (meta.isInFields() && !data.hasFirstRow) {
      return null;
    }
    while (true) {
      if (data.currentRecords != null && data.recordPointer < data.currentRecords.size()) {
        JsonNode record = data.currentRecords.get(data.recordPointer++);
        Object[] rawPart = buildFlattenedRow(record);
        Object[] outputRow = rowOutputConverter.getRow(buildBaseOutputRow(), rawPart, data);
        addExtraFields(outputRow, data);
        return outputRow;
      }
      if (!loadNextDocument()) {
        if (isDetailed()) {
          logDetailed(
              BaseMessages.getString(JsonInputMeta.class, "JsonInput.Log.FinishedProcessing"));
        }
        return null;
      }
    }
  }

  private boolean loadNextDocument() throws HopException {
    String recordPath = resolve(meta.getRecordPath());
    while (true) {
      if (data.processingJson) {
        if (data.jsonInputs == null || !data.jsonInputs.hasNext()) {
          return false;
        }
        JsonNode root = data.jsonInputs.next();
        if (root == null) {
          root = EMPTY_OBJECT_NODE;
        }
        data.currentRecords = JsonNormalizeRecords.extractRecordsFromNode(root, recordPath);
      } else {
        if (data.inputs == null || !data.inputs.hasNext()) {
          return false;
        }
        InputStream in = data.inputs.next();
        if (in != null) {
          CountingInputStream countingIn = new CountingInputStream(in);
          try {
            data.currentRecords =
                JsonNormalizeRecords.extractRecordsFromStream(countingIn, recordPath);
          } finally {
            dataVolumeIn = (dataVolumeIn != null ? dataVolumeIn : 0L) + countingIn.getCount();
            BaseTransform.closeQuietly(countingIn);
          }
        } else {
          data.currentRecords =
              JsonNormalizeRecords.extractRecordsFromStream(
                  new ByteArrayInputStream(EMPTY_JSON), recordPath);
        }
      }
      data.recordPointer = 0;
      if (data.currentRecords != null && !data.currentRecords.isEmpty()) {
        return true;
      }
    }
  }

  private Object[] buildFlattenedRow(JsonNode record) throws HopException {
    Map<String, JsonNode> flat =
        JsonNodeFlattener.flatten(
            record,
            meta.getFieldSeparator(),
            meta.getMaxFlattenDepth(),
            meta.getArrayHandling(),
            meta.getBeyondDepthBehavior());
    Object[] out = new Object[data.nrInputFields];
    for (int i = 0; i < data.nrInputFields; i++) {
      String path = data.resolvedFields[i].getPath();
      if (!flat.containsKey(path)) {
        if (!meta.isIgnoreMissingField()) {
          throw new HopException(
              BaseMessages.getString(PKG, "JsonNormalizeInput.Error.MissingPath", path));
        }
        out[i] = null;
        continue;
      }
      JsonNode v = flat.get(path);
      out[i] = (v == null || v.isNull()) ? null : v;
    }
    return out;
  }

  private void sendErrorRow(String errorMsg) {
    try {
      String defaultErrCode = "JsonNormalizeInput001";
      if (data.readrow != null) {
        putError(
            getInputRowMeta(), data.readrow, 1, errorMsg, meta.getFieldValue(), defaultErrCode);
      } else {
        putError(new RowMeta(), new Object[0], 1, errorMsg, null, defaultErrCode);
      }
    } catch (HopTransformException e) {
      logError(e.getLocalizedMessage(), e);
    }
  }

  private boolean hasAdditionalFileFields() {
    return data.file != null;
  }

  private Object[] buildBaseOutputRow() {
    Object[] outputRowData;
    if (data.readrow != null) {
      if (meta.isRemoveSourceField() && data.indexSourceField > -1) {
        int sz = data.readrow.length;
        outputRowData = RowDataUtil.allocateRowData(data.outputRowMeta.size());
        int ii = 0;
        for (int i = 0; i < sz; i++) {
          if (i != data.indexSourceField) {
            outputRowData[ii++] = data.readrow[i];
          }
        }
      } else {
        outputRowData = RowDataUtil.createResizedCopy(data.readrow, data.outputRowMeta.size());
      }
    } else {
      outputRowData = RowDataUtil.allocateRowData(data.outputRowMeta.size());
    }
    return outputRowData;
  }

  private void addExtraFields(Object[] outputRowData, JsonNormalizeInputData d) {
    int rowIndex = d.totalpreviousfields + d.nrInputFields;

    if (meta.includeFilename() && !Utils.isEmpty(meta.getFilenameField())) {
      outputRowData[rowIndex++] = d.filename;
    }
    if (meta.includeRowNumber() && !Utils.isEmpty(meta.getRowNumberField())) {
      outputRowData[rowIndex++] = d.rownr;
    }
    if (!Utils.isEmpty(meta.getShortFileNameField())) {
      outputRowData[rowIndex++] = d.shortFilename;
    }
    if (!Utils.isEmpty(meta.getExtensionField())) {
      outputRowData[rowIndex++] = d.extension;
    }
    if (!Utils.isEmpty(meta.getPathField())) {
      outputRowData[rowIndex++] = d.path;
    }
    if (!Utils.isEmpty(meta.getSizeField())) {
      outputRowData[rowIndex++] = d.size;
    }
    if (!Utils.isEmpty(meta.isHiddenField())) {
      outputRowData[rowIndex++] = Boolean.valueOf(d.path);
    }
    if (meta.getLastModificationDateField() != null
        && !meta.getLastModificationDateField().isEmpty()) {
      outputRowData[rowIndex++] = d.lastModificationDateTime;
    }
    if (!Utils.isEmpty(meta.getUriField())) {
      outputRowData[rowIndex++] = d.uriName;
    }
    if (!Utils.isEmpty(meta.getRootUriField())) {
      outputRowData[rowIndex++] = d.rootUriName;
    }
  }

  @Override
  protected IBaseFileInputReader createReader(
      JsonNormalizeInputMeta meta, JsonNormalizeInputData data, FileObject file) throws Exception {
    throw new NotImplementedException();
  }

  @Override
  public void dispose() {
    if (data.file != null) {
      closeQuietly(data.file);
    }
    data.inputs = null;
    data.jsonInputs = null;
    data.currentRecords = null;
    super.dispose();
  }
}
