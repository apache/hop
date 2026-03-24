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

package org.apache.hop.pipeline.transforms.fileinput.text;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.Result;
import org.apache.hop.core.ResultFile;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.file.TextFileInputField;
import org.apache.hop.core.playlist.FilePlayListAll;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.errorhandling.CompositeFileErrorHandler;
import org.apache.hop.pipeline.transform.errorhandling.FileErrorHandlerContentLineNumber;
import org.apache.hop.pipeline.transform.errorhandling.FileErrorHandlerMissingFiles;
import org.apache.hop.pipeline.transform.errorhandling.IFileErrorHandler;
import org.apache.hop.pipeline.transforms.file.BaseFileInputTransform;
import org.apache.hop.pipeline.transforms.file.BaseFileInputTransformUtils;
import org.apache.hop.pipeline.transforms.file.IBaseFileInputReader;
import org.apache.hop.staticschema.metadata.SchemaDefinition;
import org.apache.hop.staticschema.metadata.SchemaFieldDefinition;
import org.apache.hop.staticschema.util.SchemaDefinitionUtil;

/**
 * Read all sorts of text files, convert them to rows and writes these to one or more output
 * streams.
 */
public class TextFileInput extends BaseFileInputTransform<TextFileInputMeta, TextFileInputData> {

  private static final Class<?> PKG = TextFileInputMeta.class;

  public TextFileInput(
      TransformMeta transformMeta,
      TextFileInputMeta meta,
      TextFileInputData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  protected IBaseFileInputReader createReader(
      TextFileInputMeta meta, TextFileInputData data, FileObject file) throws Exception {
    return new TextFileInputReader(this, meta, data, file, getLogChannel());
  }

  @Override
  public boolean init() {
    if (!super.init()) {
      return false;
    }
    data.files = meta.getFileInputList(this);
    data.currentFileIndex = 0;

    // If there are missing files,
    // fail if we don't ignore errors
    //
    Result previousResult = getPipeline().getPreviousResult();
    Map<String, ResultFile> resultFiles =
        (previousResult != null) ? previousResult.getResultFiles() : null;

    if ((previousResult == null || Utils.isEmpty(resultFiles))
        && data.files.nrOfMissingFiles() > 0
        && !meta.getFileInput().isAcceptingFilenames()
        && !meta.getErrorHandling().isErrorIgnored()) {
      logError(BaseMessages.getString(PKG, "BaseFileInputTransform.Log.Error.NoFilesSpecified"));
      return false;
    }

    initErrorHandling();

    data.filePlayList = FilePlayListAll.INSTANCE;

    data.filterProcessor = new TextFileFilterProcessor(meta.getFilters(), this);

    // calculate the file format type in advance so we can use a switch
    data.fileFormatType = meta.getFileFormatTypeNr();

    // calculate the file type in advance CSV or Fixed?
    data.fileType = meta.getFileTypeNr();

    // Handle the possibility of a variable substitution
    data.separator = resolve(meta.getContent().getSeparator());
    data.enclosure = resolve(meta.getContent().getEnclosure());
    data.escapeCharacter = resolve(meta.getContent().getEscapeCharacter());
    // CSV without separator defined
    if (meta.getFileType().equalsIgnoreCase("CSV")
        && (Utils.isEmpty(meta.getContent().getSeparator()))) {
      logError(BaseMessages.getString(PKG, "TextFileInput.Exception.NoSeparator"));
      return false;
    }

    // If use shema and ignore fields set the field definitions in the transform
    if (meta.isIgnoreFields()) {
      try {
        SchemaDefinition loadedSchemaDefinition =
            (new SchemaDefinitionUtil())
                .loadSchemaDefinition(metadataProvider, meta.getSchemaDefinition());
        if (loadedSchemaDefinition != null) {
          IRowMeta r = loadedSchemaDefinition.getRowMeta();
          if (r != null) {
            meta.getInputFields().clear();

            for (int i = 0; i < r.size(); i++) {
              final SchemaFieldDefinition schemaFieldDefinition =
                  loadedSchemaDefinition.getFieldDefinitions().get(i);
              TextFileInputField f = new TextFileInputField();
              f.setName(schemaFieldDefinition.getName());
              f.setType(r.getValueMeta(i).getType());
              f.setFormat(r.getValueMeta(i).getFormatMask());
              f.setLength(r.getValueMeta(i).getLength());
              f.setPrecision(r.getValueMeta(i).getPrecision());
              f.setCurrencySymbol(r.getValueMeta(i).getCurrencySymbol());
              f.setDecimalSymbol(r.getValueMeta(i).getDecimalSymbol());
              f.setGroupSymbol(r.getValueMeta(i).getGroupingSymbol());
              f.setIfNullValue(schemaFieldDefinition.getIfNullValue());
              f.setTrimType(r.getValueMeta(i).getTrimType());
              f.setRepeated(false);
              meta.getInputFields().add(f);
            }
          }
        }
      } catch (HopTransformException | HopPluginException e) {
        // ignore any errors here.
      }
    }

    return true;
  }

  /** Initialize error handling. */
  private void initErrorHandling() {
    List<IFileErrorHandler> dataErrorLineHandlers = new ArrayList<>(2);
    if (meta.getErrorHandling().getLineNumberFilesDestinationDirectory() != null) {
      dataErrorLineHandlers.add(
          new FileErrorHandlerContentLineNumber(
              getPipeline().getExecutionStartDate(),
              resolve(meta.getErrorHandling().getLineNumberFilesDestinationDirectory()),
              meta.getErrorHandling().getLineNumberFilesExtension(),
              meta.getEncoding(),
              this));
    }
    if (meta.getErrorHandling().getErrorFilesDestinationDirectory() != null) {
      dataErrorLineHandlers.add(
          new FileErrorHandlerMissingFiles(
              getPipeline().getExecutionStartDate(),
              resolve(meta.getErrorHandling().getErrorFilesDestinationDirectory()),
              meta.getErrorHandling().getErrorFilesExtension(),
              meta.getEncoding(),
              this));
    }
    data.dataErrorLineHandler = new CompositeFileErrorHandler(dataErrorLineHandlers);
  }

  /** Process next row. These methods open the next file automatically. */
  @Override
  public boolean processRow() throws HopException {
    if (first) {
      first = false;
      prepareToRowProcessing(meta.getErrorHandling().isErrorIgnored());

      // Count the number of repeat fields...
      for (int i = 0; i < meta.getInputFields().size(); i++) {
        TextFileInputField f = meta.getInputFields().get(i);
        if (f.isRepeated()) {
          data.nr_repeats++;
        }
      }

      if (!openNextFile(
          meta.getErrorHandling().isErrorIgnored(), meta.getErrorHandling().isSkipBadFiles())) {
        setOutputDone(); // signal end to receiver(s)
        closeLastFile(
            meta.getErrorHandling().isErrorIgnored(), meta.getErrorHandling().isSkipBadFiles());
        return false;
      }
    }

    while (true) {
      if (data.reader != null && data.reader.readRow()) {
        // row processed
        return true;
      }
      // end of current file
      closeLastFile(
          meta.getErrorHandling().isErrorIgnored(), meta.getErrorHandling().isSkipBadFiles());

      if (!openNextFile(
          meta.getErrorHandling().isErrorIgnored(), meta.getErrorHandling().isSkipBadFiles())) {
        // there are no more files
        break;
      }
    }

    // after all files processed
    setOutputDone(); // signal end to receiver(s)
    closeLastFile(
        meta.getErrorHandling().isErrorIgnored(), meta.getErrorHandling().isSkipBadFiles());
    return false;
  }

  /** Dispose transform. */
  @Override
  public void dispose() {
    closeLastFile(
        meta.getErrorHandling().isErrorIgnored(), meta.getErrorHandling().isSkipBadFiles());

    super.dispose();
  }

  /**
   * @param errorMsg Message to send to rejected row if enabled
   * @return If should stop processing after having problems with a file
   */
  @Override
  public boolean failAfterBadFile(String errorMsg, boolean errorIgnored, boolean skipBadFiles) {

    if (getTransformMeta().isDoingErrorHandling()
        && data.filename != null
        && !data.rejectedFiles.containsKey(data.filename)) {
      data.rejectedFiles.put(data.filename, true);
      rejectCurrentFile(errorMsg);
    }

    return !errorIgnored || !skipBadFiles;
  }

  /**
   * Send file name and/or error message to error output
   *
   * @param errorMsg Message to send to rejected row if enabled
   */
  private void rejectCurrentFile(String errorMsg) {
    if (StringUtils.isNotBlank(meta.getErrorHandling().getFileErrorField())
        || StringUtils.isNotBlank(meta.getErrorHandling().getFileErrorMessageField())) {
      IRowMeta rowMeta = getInputRowMeta();
      if (rowMeta == null) {
        rowMeta = new RowMeta();
      }

      int errorFileIndex =
          (StringUtils.isBlank(meta.getErrorHandling().getFileErrorField()))
              ? -1
              : BaseFileInputTransformUtils.addValueMeta(
                  getTransformName(),
                  rowMeta,
                  this.resolve(meta.getErrorHandling().getFileErrorField()));

      int errorMessageIndex =
          StringUtils.isBlank(meta.getErrorHandling().getFileErrorMessageField())
              ? -1
              : BaseFileInputTransformUtils.addValueMeta(
                  getTransformName(),
                  rowMeta,
                  this.resolve(meta.getErrorHandling().getFileErrorMessageField()));

      try {
        Object[] rowData = getRow();
        if (rowData == null) {
          rowData = RowDataUtil.allocateRowData(rowMeta.size());
        }

        if (errorFileIndex >= 0) {
          rowData[errorFileIndex] = data.filename;
        }
        if (errorMessageIndex >= 0) {
          rowData[errorMessageIndex] = errorMsg;
        }

        putError(rowMeta, rowData, getErrors(), data.filename, null, "ERROR_CODE");
      } catch (Exception e) {
        logError("Error sending error row", e);
      }
    }
  }
}
