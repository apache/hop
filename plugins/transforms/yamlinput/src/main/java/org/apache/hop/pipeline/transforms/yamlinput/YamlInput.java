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

package org.apache.hop.pipeline.transforms.yamlinput;

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.Const;
import org.apache.hop.core.ResultFile;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.fileinput.FileInputList;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
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

/**
 * Read YAML files, parse them and convert them to rows and writes these to one or more output
 * streams.
 */
public class YamlInput extends BaseTransform<YamlInputMeta, YamlInputData> {
  private static final Class<?> PKG = YamlInputMeta.class;

  public YamlInput(
      TransformMeta transformMeta,
      YamlInputMeta meta,
      YamlInputData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  private void handleMissingFiles() throws HopException {
    List<FileObject> nonExistentFiles = data.files.getNonExistentFiles();
    if (!nonExistentFiles.isEmpty()) {
      String message = FileInputList.getRequiredFilesDescription(nonExistentFiles);
      logError(
          BaseMessages.getString(PKG, "YamlInput.Log.RequiredFilesTitle"),
          BaseMessages.getString(PKG, "YamlInput.Log.RequiredFiles", message));

      throw new HopException(
          BaseMessages.getString(PKG, "YamlInput.Log.RequiredFilesMissing", message));
    }

    List<FileObject> nonAccessibleFiles = data.files.getNonAccessibleFiles();
    if (!nonAccessibleFiles.isEmpty()) {
      String message = FileInputList.getRequiredFilesDescription(nonAccessibleFiles);
      logError(
          BaseMessages.getString(PKG, "YamlInput.Log.RequiredFilesTitle"),
          BaseMessages.getString(PKG, "YamlInput.Log.RequiredNotAccessibleFiles", message));

      throw new HopException(
          BaseMessages.getString(PKG, "YamlInput.Log.RequiredNotAccessibleFilesMissing", message));
    }
  }

  private boolean readNextString() {
    try {
      // Grab another row ...
      data.readRow = getRow();

      if (data.readRow == null) {
        // finished processing!
        if (isDetailed()) {
          logDetailed(BaseMessages.getString(PKG, "YamlInput.Log.FinishedProcessing"));
        }
        return true;
      }

      if (first) {
        first = false;

        data.outputRowMeta = getInputRowMeta().clone();
        // Get total previous fields
        data.totalPreviousFields = data.outputRowMeta.size();
        data.totalOutFields = data.totalPreviousFields + data.nrInputFields;
        meta.getFields(data.outputRowMeta, getTransformName(), null, null, this, metadataProvider);

        // Check if YAML field is provided
        if (Utils.isEmpty(meta.getYamlField())) {
          logError(BaseMessages.getString(PKG, "YamlInput.Log.NoField"));
          throw new HopException(BaseMessages.getString(PKG, "YamlInput.Log.NoField"));
        }

        // cache the position of the field
        data.indexOfYamlField = getInputRowMeta().indexOfValue(meta.getYamlField());
        if (data.indexOfYamlField < 0) {
          // The field is unreachable !
          logError(
              BaseMessages.getString(PKG, "YamlInput.Log.ErrorFindingField", meta.getYamlField()));
          throw new HopException(
              BaseMessages.getString(
                  PKG, "YamlInput.Exception.CouldnotFindField", meta.getYamlField()));
        }
      }

      // get field value
      String fieldValue = getInputRowMeta().getString(data.readRow, data.indexOfYamlField);
      getLinesInput();

      if (isDetailed()) {
        logDetailed(
            BaseMessages.getString(
                PKG, "YamlInput.Log.YAMLStream", meta.getYamlField(), fieldValue));
      }

      if (meta.isSourceFile()) {

        // source is a file.

        FileObject yamlFile = HopVfs.getFileObject(fieldValue, variables);
        data.yaml = new YamlReader();
        data.yaml.loadFile(yamlFile);
        dataVolumeIn =
            (dataVolumeIn != null ? dataVolumeIn : 0L) + data.yaml.getBytesReadFromFile();
        emitYamlFileReadLineage(yamlFile, data.yaml.getBytesReadFromFile());

        addFileToResultFilesName(data.yaml.getFile());
      } else {
        data.yaml = new YamlReader();
        data.yaml.loadString(fieldValue);
      }
    } catch (Exception e) {
      logError(BaseMessages.getString(PKG, "YamlInput.Log.UnexpectedError", e.toString()));
      stopAll();
      logError(Const.getStackTracker(e));
      setErrors(1);
      return true;
    }
    return false;
  }

  private void emitYamlFileReadLineage(FileObject file, long bytesRead) {
    if (file == null) {
      return;
    }
    LineageFileIoEmitter.emitTransformFileIo(
        this,
        FileIoOperation.READ,
        file,
        null,
        bytesRead > 0 ? bytesRead : null,
        true,
        null,
        yamlFileReadContentSchema());
  }

  private FileIoContentSchema yamlFileReadContentSchema() {
    if (meta.getInputFields() == null || meta.getInputFields().isEmpty()) {
      return null;
    }
    List<FileIoTabularColumn> cols = new ArrayList<>();
    for (YamlInputField f : meta.getInputFields()) {
      cols.add(
          new FileIoTabularColumn(
              f.getName(),
              f.getTypeDesc(),
              f.getLength(),
              f.getPrecision(),
              f.getPath(),
              FileIoPathSyntax.YAML_PATH,
              false));
    }
    return FileIoContentSchema.tabularWithMergedTree("yaml", cols);
  }

  private void addFileToResultFilesName(FileObject file) {
    if (meta.isAddingResultFile()) {
      // Add this to the result file names...
      ResultFile resultFile =
          new ResultFile(
              ResultFile.FILE_TYPE_GENERAL, file, getPipelineMeta().getName(), getTransformName());
      resultFile.setComment(BaseMessages.getString(PKG, "YamlInput.Log.FileAddedResult"));
      addResultFile(resultFile);
    }
  }

  private boolean openNextFile() {
    try {
      if (data.fileIndex >= data.files.nrOfFiles()) {
        // finished processing!
        if (isDetailed()) {
          logDetailed(BaseMessages.getString(PKG, "YamlInput.Log.FinishedProcessing"));
        }
        return false;
      }
      // Get file to process from list
      data.file = data.files.getFile(data.fileIndex);

      // Move file pointer ahead!
      data.fileIndex++;

      if (meta.isIgnoringEmptyFile() && data.file.getContent().getSize() == 0) {
        if (isBasic()) {
          logBasic(
              BaseMessages.getString(PKG, "YamlInput.Error.FileSizeZero", data.file.getName()));
        }
        // Let's open the next file
        openNextFile();

      } else {
        if (isDetailed()) {
          logDetailed(
              BaseMessages.getString(PKG, "YamlInput.Log.OpeningFile", data.file.toString()));
        }

        // We have a file
        // define a YAML reader and load file
        data.yaml = new YamlReader();
        data.yaml.loadFile(data.file);
        dataVolumeIn =
            (dataVolumeIn != null ? dataVolumeIn : 0L) + data.yaml.getBytesReadFromFile();
        emitYamlFileReadLineage(data.file, data.yaml.getBytesReadFromFile());

        addFileToResultFilesName(data.file);

        if (isDetailed()) {
          logDetailed(
              BaseMessages.getString(PKG, "YamlInput.Log.FileOpened", data.file.toString()));
        }
      }
    } catch (Exception e) {
      logError(
          BaseMessages.getString(
              PKG,
              "YamlInput.Log.UnableToOpenFile",
              "" + data.fileIndex,
              data.file.toString(),
              e.toString()));
      stopAll();
      setErrors(1);
      logError(Const.getStackTracker(e));
      return false;
    }
    return true;
  }

  @Override
  public boolean processRow() throws HopException {
    if (first && !meta.isInFields()) {
      first = false;

      data.files = meta.getFiles(this);

      if (!meta.isDoNotFailIfNoFile() && data.files.nrOfFiles() == 0) {
        throw new HopException(BaseMessages.getString(PKG, "YamlInput.Log.NoFiles"));
      }

      handleMissingFiles();

      // Create the output row meta-data
      data.outputRowMeta = new RowMeta();
      data.totalPreviousFields = 0;
      data.totalOutFields = data.totalPreviousFields + data.nrInputFields;
      meta.getFields(data.outputRowMeta, getTransformName(), null, null, this, metadataProvider);
      data.totalOutStreamFields = data.outputRowMeta.size();
    }
    // Grab a row
    Object[] r = getOneRow();
    if (Utils.isEmpty(r)) {
      // signal end to receiver(s)
      setOutputDone();
      // end of data or error.
      return false;
    }

    if (isRowLevel()) {
      logRowlevel(
          BaseMessages.getString(PKG, "YamlInput.Log.ReadRow", data.outputRowMeta.getString(r)));
    }
    incrementLinesOutput();

    data.rowIndex++;
    Object[] rowCopy = data.outputRowMeta.cloneRow(r);
    // copy row to output rowset(s)
    putRow(data.outputRowMeta, rowCopy);

    if (meta.getRowLimit() > 0 && data.rowIndex > meta.getRowLimit()) {
      // limit has been reached: stop now.
      setOutputDone();
      return false;
    }
    return true;
  }

  private Object[] getOneRow() throws HopException {
    if (!meta.isInFields()) {
      return getOneRowFromFileMode();
    }

    return getOneRowFromFieldMode();
  }

  private Object[] getOneRowFromFileMode() throws HopException {
    while (true) {
      if (data.file == null && !openNextFile()) {
        return new Object[0];
      }

      if (data.file == null) {
        continue;
      }

      Object[] row = getRowData();
      if (row != null && row.length > 0) {
        return row;
      }

      if (!openNextFile()) {
        return new Object[0];
      }
    }
  }

  private Object[] getOneRowFromFieldMode() throws HopException {
    while (true) {
      if (data.readRow == null) {
        if (readNextString()) {
          return new Object[0];
        }
        continue;
      }

      Object[] row = getRowData();
      if (row != null && row.length > 0) {
        return row;
      }

      if (readNextString()) {
        return new Object[0];
      }
    }
  }

  private Object[] getRowData() throws HopException {
    // Build an empty row based on the meta-data
    Object[] outputRowData = null;

    try {
      // Create new row...
      outputRowData = data.yaml.getRow(data.rowMeta);
      if (outputRowData == null || outputRowData.length == 0) {
        return new Object[0];
      }

      if (data.readRow != null) {
        outputRowData =
            RowDataUtil.addRowData(data.readRow, data.totalPreviousFields, outputRowData);
      } else {
        outputRowData = RowDataUtil.resizeArray(outputRowData, data.totalOutStreamFields);
      }

      int rowIndex = data.totalOutFields;

      // See if we need to add the filename to the row...
      if (meta.isIncludeFilename() && !Utils.isEmpty(meta.getFilenameField())) {
        outputRowData[rowIndex++] = HopVfs.getFilename(data.file);
      }
      // See if we need to add the row number to the row...
      if (meta.isIncludeRowNumber() && !Utils.isEmpty(meta.getRowNumberField())) {
        outputRowData[rowIndex] = data.rowIndex;
      }
    } catch (Exception e) {
      boolean sendToErrorRow = false;
      String errorMessage = null;

      if (getTransformMeta().isDoingErrorHandling()) {
        sendToErrorRow = true;
        errorMessage = e.toString();
      } else {
        logError(BaseMessages.getString(PKG, "YamlInput.ErrorInTransformRunning", e.toString()));
        setErrors(1);
        stopAll();
        logError(Const.getStackTracker(e));
        setOutputDone(); // signal end to receiver(s)
      }
      if (sendToErrorRow) {
        // Simply add this row to the error row
        putError(getInputRowMeta(), outputRowData, 1, errorMessage, null, "YamlInput001");
      }
    }

    return outputRowData;
  }

  @Override
  public boolean init() {
    if (super.init()) {
      data.rowIndex = 1L;
      data.nrInputFields = meta.getInputFields().size();

      data.rowMeta = new RowMeta();
      for (int i = 0; i < data.nrInputFields; i++) {
        YamlInputField field = meta.getInputFields().get(i);
        String path = resolve(field.getPath());

        try {
          IValueMeta valueMeta = ValueMetaFactory.createValueMeta(path, field.getType());
          valueMeta.setTrimType(field.getTrimType());
          data.rowMeta.addValueMeta(valueMeta);
        } catch (Exception e) {
          logError("Unable to create value meta", e);
          return false;
        }
      }

      return true;
    }
    return false;
  }
}
