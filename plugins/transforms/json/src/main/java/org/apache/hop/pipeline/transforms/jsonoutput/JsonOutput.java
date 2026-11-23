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

package org.apache.hop.pipeline.transforms.jsonoutput;

import java.io.BufferedOutputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.Const;
import org.apache.hop.core.ResultFile;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.io.CountingOutputStream;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.lineage.LineageFileIoEmitter;
import org.apache.hop.lineage.model.FileIoContentSchema;
import org.apache.hop.lineage.model.FileIoOperation;
import org.apache.hop.lineage.model.FileIoPathSyntax;
import org.apache.hop.lineage.model.FileIoTabularColumn;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;

/** Converts input rows to one or more Xml files. */
public class JsonOutput extends BaseTransform<JsonOutputMeta, JsonOutputData> {
  private static final Class<?> PKG = JsonOutput.class;

  public JsonOutput(
      TransformMeta transformMeta,
      JsonOutputMeta meta,
      JsonOutputData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);

    JsonOutputMeta jsonOutputMeta = (JsonOutputMeta) (transformMeta.getTransform());
  }

  public void execute(Object[] row) throws HopException {

    // The values of the row, in the order of the output fields
    Object[] values = new Object[data.nrFields];
    for (int i = 0; i < data.nrFields; i++) {
      int index = data.fieldIndexes[i];
      IValueMeta v = data.inputRowMeta.getValueMeta(index);

      switch (v.getType()) {
        case IValueMeta.TYPE_BOOLEAN:
          values[i] = data.inputRowMeta.getBoolean(row, index);
          break;
        case IValueMeta.TYPE_INTEGER:
          values[i] = data.inputRowMeta.getInteger(row, index);
          break;
        case IValueMeta.TYPE_NUMBER:
          values[i] = data.inputRowMeta.getNumber(row, index);
          break;
        case IValueMeta.TYPE_BIGNUMBER:
          values[i] = data.inputRowMeta.getBigNumber(row, index);
          break;
        default:
          values[i] = data.inputRowMeta.getString(row, index);
          break;
      }
    }

    // Add the row to the text of the block. A block that only goes to a file is written as the
    // rows come in, so it never has to fit in memory as a whole.
    boolean first = data.blockRows == 0;
    if (first) {
      startBlock();
      if (data.streamsToFile() && !openNewFile()) {
        throw new HopTransformException(
            BaseMessages.getString(PKG, "JsonOutput.Error.OpenNewFile", buildFilename()));
      }
    }
    data.rowsText.appendRow(data.block, first, values);
    data.blockRows++;
    if (data.streamsToFile()) {
      writeBlockText();
    }

    data.nrRow++;

    if (data.nrRowsInBloc > 0 && data.nrRow % data.nrRowsInBloc == 0) {
      // We can now output an object
      outputRow(row);
    }
  }

  private void startBlock() {
    if (data.block == null) {
      data.block = new StringBuilder();
    }
    data.block.setLength(0);
    JsonRowsText.appendStart(data.block, data.realBlocName);
  }

  /** Writes the text collected so far to the open file. */
  private void writeBlockText() throws HopTransformException {
    try {
      data.writer.write(data.block.toString());
      data.block.setLength(0);
    } catch (Exception e) {
      throw new HopTransformException(BaseMessages.getString(PKG, "JsonOutput.Error.Writing"), e);
    }
  }

  @Override
  public boolean processRow() throws HopException {
    Object[] r = getRow(); // This also waits for a row to be finished.
    if (r == null) {
      writeJsonToFile();

      setOutputDone();
      return false;
    }

    if (first) {
      first = false;
      data.inputRowMeta = getInputRowMeta();
      data.inputRowMetaSize = data.inputRowMeta.size();
      if (data.outputValue) {
        data.outputRowMeta = data.inputRowMeta.clone();
        meta.getFields(data.outputRowMeta, getTransformName(), null, null, this, metadataProvider);
      }

      // Cache the field name indexes
      //
      data.nrFields = meta.getOutputFields().size();
      data.fieldIndexes = new int[data.nrFields];
      List<String> elementNames = new ArrayList<>();
      for (int i = 0; i < data.nrFields; i++) {
        data.fieldIndexes[i] =
            data.inputRowMeta.indexOfValue(meta.getOutputFields().get(i).getFieldName());
        if (data.fieldIndexes[i] < 0) {
          throw new HopException(BaseMessages.getString(PKG, "JsonOutput.Exception.FieldNotFound"));
        }
        JsonOutputField field = meta.getOutputFields().get(i);
        field.setElementName(resolve(field.getElementName()));
        elementNames.add(field.getElementName());
      }
      data.rowsText = new JsonRowsText(elementNames);
    }

    data.rowsAreSafe = false;
    data.lastRow = r;
    execute(r);

    if (data.writeToFile && !data.outputValue) {
      putRow(data.inputRowMeta, r); // in case we want it go further...
      incrementLinesOutput();
    }
    return true;
  }

  private void writeJsonToFile() throws HopTransformException {
    // no more input to be expected...
    if (!data.rowsAreSafe) {
      // Let's output the remaining unsafe data on the last row we've seen
      outputRow(data.lastRow);
    }
  }

  private void outputRow(Object[] rowData) throws HopTransformException {
    // We can now output an object
    boolean hasRows = data.blockRows > 0;
    if (!hasRows) {
      startBlock();
    }
    JsonRowsText.appendEnd(data.block);

    if (data.outputValue && data.outputRowMeta != null) {
      Object[] outputRowData =
          RowDataUtil.addValueData(rowData, data.inputRowMetaSize, data.block.toString());
      incrementLinesOutput();
      putRow(data.outputRowMeta, outputRowData);
    }

    if (data.writeToFile && hasRows) {
      // Open a file
      if (!openNewFile()) {
        throw new HopTransformException(
            BaseMessages.getString(PKG, "JsonOutput.Error.OpenNewFile", buildFilename()));
      }
      // Write data to file: the whole block, or what is left of it when the rows went out already
      writeBlockText();
      // Close file
      closeFile();
    }
    // Data are safe
    data.rowsAreSafe = true;
    data.lastRow = null;
    data.block.setLength(0);
    data.blockRows = 0;
  }

  @Override
  public boolean init() {

    if (super.init()) {

      data.writeToFile =
          (!Objects.equals(meta.getOperationType(), JsonOutputMeta.OPERATION_TYPE_OUTPUT_VALUE));
      data.outputValue =
          (!Objects.equals(meta.getOperationType(), JsonOutputMeta.OPERATION_TYPE_WRITE_TO_FILE));

      if (data.outputValue && Utils.isEmpty(resolve(meta.getOutputValue()))) {
        // We need to have output field name
        logError(BaseMessages.getString(PKG, "JsonOutput.Error.MissingOutputFieldName"));
        stopAll();
        setErrors(1);
        return false;
      }
      if (data.writeToFile) {
        // We need to have output field name
        if (Utils.isEmpty(meta.getFileName())) {
          logError(BaseMessages.getString(PKG, "JsonOutput.Error.MissingTargetFilename"));
          stopAll();
          setErrors(1);
          return false;
        }
        if (!meta.isDoNotOpenNewFileInit() && !openNewFile()) {
          logError(BaseMessages.getString(PKG, "JsonOutput.Error.OpenNewFile", buildFilename()));
          stopAll();
          setErrors(1);
          return false;
        }
      }
      data.realBlocName = Const.NVL(resolve(meta.getJsonBloc()), "");
      data.nrRowsInBloc = Const.toInt(resolve(meta.getNrRowsInBloc()), 0);
      return true;
    }

    return false;
  }

  @Override
  public void dispose() {
    // Is there some left-over data to be written to disk?
    //
    if (data.blockRows > 0) {
      try {
        outputRow(data.lastRow);
      } catch (Exception e) {
        logError("Error writing final rows to disk", e);
      }
    }
    closeFile();
    super.dispose();
  }

  @Override
  public void startBundle() throws HopException {
    if (!first) {
      openNewFile();
    }
  }

  @Override
  public void batchComplete() throws HopException {
    if (!data.isBeamContext()) {
      writeJsonToFile();
    }
  }

  @Override
  public void finishBundle() throws HopException {
    writeJsonToFile();
  }

  private void createParentFolder(String filename) throws HopTransformException {
    if (!meta.isCreateParentFolder()) {
      return;
    }
    // Check for parent folder
    FileObject parentfolder = null;
    try {
      // Get parent folder
      parentfolder = HopVfs.getFileObject(filename, variables).getParent();
      if (parentfolder == null) {
        throw new HopTransformException(
            BaseMessages.getString(PKG, "JsonOutput.Error.ErrorCreatingParentFolder", filename));
      }
      if (!parentfolder.exists()) {
        String parentUri = HopVfs.getFriendlyURI(parentfolder);
        if (isDebug()) {
          logDebug(BaseMessages.getString(PKG, "JsonOutput.Error.ParentFolderNotExist", parentUri));
        }
        try {
          parentfolder.createFolder();
        } catch (Exception createEx) {
          // Another concurrent writer (e.g. Beam parallel outputs) may have created it
          parentfolder = HopVfs.getFileObject(filename, variables).getParent();
          if (parentfolder == null || !parentfolder.exists()) {
            throw createEx;
          }
        }
        if (isDebug()) {
          logDebug(BaseMessages.getString(PKG, "JsonOutput.Log.ParentFolderCreated"));
        }
      }
    } catch (Exception e) {
      String parentDesc =
          parentfolder != null ? HopVfs.getFriendlyURI(parentfolder) : String.valueOf(filename);
      throw new HopTransformException(
          BaseMessages.getString(PKG, "JsonOutput.Error.ErrorCreatingParentFolder", parentDesc), e);
    } finally {
      if (parentfolder != null) {
        try {
          parentfolder.close();
        } catch (Exception ex) {
          /* Ignore */
        }
      }
    }
  }

  public boolean openNewFile() {
    if (data.writer != null) {
      return true;
    }
    boolean retval = false;

    try {

      String filename = buildFilename();
      createParentFolder(filename);
      if (meta.isAddToResult()) {
        // Add this to the result file names...
        ResultFile resultFile =
            new ResultFile(
                ResultFile.FILE_TYPE_GENERAL,
                HopVfs.getFileObject(filename),
                getPipelineMeta().getName(),
                getTransformName());
        resultFile.setComment(BaseMessages.getString(PKG, "JsonOutput.ResultFilenames.Comment"));
        addResultFile(resultFile);
      }

      OutputStream outputStream;
      OutputStream fos = HopVfs.getOutputStream(filename, meta.isFileAppended());
      data.countingStream = new CountingOutputStream(fos);
      outputStream = data.countingStream;

      if (!Utils.isEmpty(meta.getEncoding())) {
        data.writer =
            new OutputStreamWriter(
                new BufferedOutputStream(outputStream, 5000), resolve(meta.getEncoding()));
      } else {
        data.writer = new OutputStreamWriter(new BufferedOutputStream(outputStream, 5000));
      }

      if (isDetailed()) {
        logDetailed(BaseMessages.getString(PKG, "JsonOutput.FileOpened", filename));
      }

      data.openedFilename = filename;
      data.splitnr++;

      retval = true;

    } catch (Exception e) {
      logError(BaseMessages.getString(PKG, "JsonOutput.Error.OpeningFile", e.toString()), e);
    }

    return retval;
  }

  public String buildFilename() {
    return meta.buildFilename(
        variables,
        getCopy() + "",
        null,
        data.splitnr + "",
        data.isBeamContext(),
        getLogChannelId(),
        data.getBeamBundleNr(),
        false);
  }

  private FileIoContentSchema jsonWrittenFileContentSchema() {
    if (meta.getOutputFields() == null || meta.getOutputFields().isEmpty()) {
      return null;
    }
    List<FileIoTabularColumn> cols = new ArrayList<>();
    for (JsonOutputField f : meta.getOutputFields()) {
      String typeDesc = "String";
      if (data.outputRowMeta != null) {
        IValueMeta v = data.outputRowMeta.searchValueMeta(f.getFieldName());
        if (v != null) {
          typeDesc = v.getTypeDesc();
        }
      }
      cols.add(
          new FileIoTabularColumn(
              f.getFieldName(),
              typeDesc,
              -1,
              -1,
              Utils.isEmpty(f.getElementName()) ? null : f.getElementName(),
              FileIoPathSyntax.JSON_PATH,
              false));
    }
    return FileIoContentSchema.tabularWithMergedTree("json", cols);
  }

  protected boolean closeFile() {
    if (data.writer == null) {
      return true;
    }
    boolean retval = false;

    try {
      data.writer.flush();
      if (data.countingStream != null) {
        long written = data.countingStream.getCount();
        dataVolumeOut = (dataVolumeOut != null ? dataVolumeOut : 0L) + written;
        if (!data.isBeamContext() && written > 0 && data.openedFilename != null) {
          try {
            FileObject outFile = HopVfs.getFileObject(data.openedFilename, this);
            LineageFileIoEmitter.emitTransformFileIo(
                this,
                FileIoOperation.WRITE,
                null,
                outFile,
                written,
                true,
                null,
                jsonWrittenFileContentSchema());
          } catch (Exception ignored) {
            // optional lineage
          }
        }
      }
      data.openedFilename = null;
      data.writer.close();
      data.writer = null;
      data.countingStream = null;
      retval = true;
    } catch (Exception e) {
      logError(BaseMessages.getString(PKG, "JsonOutput.Error.ClosingFile", e.toString()));
      setErrors(1);
      retval = false;
    }

    return retval;
  }
}
