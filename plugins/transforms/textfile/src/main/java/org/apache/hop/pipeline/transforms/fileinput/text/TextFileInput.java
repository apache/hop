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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.Result;
import org.apache.hop.core.ResultFile;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.file.TextFileInputField;
import org.apache.hop.core.playlist.FilePlayListAll;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.vfs.HopVfs;
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
import org.apache.hop.pipeline.transforms.fileinput.text.TextFileChunkPlan.Source;
import org.apache.hop.pipeline.transforms.fileinput.text.TextFileRecordSplitter.RecordFormat;
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

    int copies = getTransformMeta().getCopies(this);
    String parallelProblem = meta.getParallelReadProblem(copies, this);
    if (parallelProblem != null) {
      logError(parallelProblem);
      return false;
    }
    data.parallel = meta.getContent().isRunningInParallel() && copies > 1;
    try {
      data.readAhead = meta.getReadAhead(this);
    } catch (HopException e) {
      logError(e.getMessage().trim());
      return false;
    }

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
      logError(
          BaseMessages.getString(
              BaseFileInputTransform.class, "BaseFileInputTransform.Log.Error.NoFilesSpecified"));
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

  /**
   * Opens the next piece to read. Without parallel reading that is the next file. With it, it is
   * the next chunk of the plan the copies share: a part of a large file, or a whole small one.
   */
  @Override
  protected boolean openNextFile(boolean errorIgnored, boolean skipBadFiles) {
    if (!data.parallel) {
      return super.openNextFile(errorIgnored, skipBadFiles);
    }
    TextFileChunk chunk;
    try {
      chunk = chunkPlan().next();
    } catch (HopException e) {
      logError(e.getMessage(), e);
      setErrors(getErrors() + 1);
      stopAll();
      return false;
    }
    if (chunk == null) {
      return false;
    }
    data.chunk = chunk;
    data.file = chunk.file();
    data.currentPassThruFieldsRow = chunk.passThruRow();
    try {
      data.filename = HopVfs.getFilename(data.file);
      fillFileAdditionalFields(data, data.file);
      if (chunk.startsFile()) {
        // Once per file, not once per chunk
        if (meta.getFileInput().isAddingResult()) {
          ResultFile resultFile =
              new ResultFile(
                  ResultFile.FILE_TYPE_GENERAL, data.file, getPipelineMeta().getName(), toString());
          resultFile.setComment("File was read by an Text File input transform");
          addResultFile(resultFile);
        }
        emitLineageFileRead(data.file);
      }
      if (isDetailed()) {
        logDetailed(
            "Reading "
                + data.file.getName().getFriendlyURI()
                + (chunk.wholeFile() ? "" : " from byte " + chunk.start() + " to " + chunk.end()));
      }
      data.dataErrorLineHandler.handleFile(data.file);
      data.reader = createReader(meta, data, data.file);
    } catch (Exception e) {
      if (!handleOpenFileException(e, errorIgnored, skipBadFiles)) {
        return false;
      }
      data.reader = null;
    }
    data.currentFileIndex++;
    return true;
  }

  /** What the copies of one transform share while running in parallel. */
  private static final class Shared {
    /** The files each copy received from the previous transform, by copy number. */
    private final List<List<Source>> sourcesByCopy = new ArrayList<>();

    /** The copies that still have to hand in their filenames. */
    private int copiesToHandIn;

    /**
     * The output row layout of a copy that received filename rows, with their passed-through
     * fields. A copy that received none knows nothing of those fields.
     */
    private IRowMeta passThruOutputRowMeta;

    private int nrPassThruFields;

    private TextFileChunkPlan plan;

    private Shared(int copies) {
      copiesToHandIn = copies;
      for (int copy = 0; copy < copies; copy++) {
        sourcesByCopy.add(List.of());
      }
    }
  }

  /** The state shared by the copies of this transform, created by the first copy to ask. */
  private Shared shared() {
    Map<String, Object> extensionData = getPipeline().getExtensionDataMap();
    String key = TextFileChunkPlan.class.getName() + "/" + getTransformName();
    synchronized (extensionData) {
      return (Shared)
          extensionData.computeIfAbsent(key, k -> new Shared(getTransformMeta().getCopies(this)));
    }
  }

  /**
   * Hands the filenames this copy received from the previous transform to the other copies, and
   * waits until every copy has done so: the plan splits all the files, whichever copy received
   * them. Each copy already reads all its filename rows before reading a file, so the wait costs no
   * streaming.
   *
   * @return false when the transform was stopped while waiting
   */
  private boolean shareFilenames() throws HopException {
    List<Source> sources = new ArrayList<>();
    if (receivesOwnShareOfFilenames()) {
      List<FileObject> files = data.files.getFiles();
      for (int i = 0; i < files.size(); i++) {
        Object[] passThruRow =
            data.passThruFields == null ? null : data.passThruFields.get(i + "_" + files.get(i));
        sources.add(new Source(files.get(i), passThruRow));
      }
    }
    boolean passingThru = meta.getFileInput().isPassingThruFields();

    Shared shared = shared();
    synchronized (shared) {
      shared.sourcesByCopy.set(getCopy(), sources);
      if (passingThru && data.passThruFields != null && shared.passThruOutputRowMeta == null) {
        shared.passThruOutputRowMeta = data.outputRowMeta;
        shared.nrPassThruFields = data.nrPassThruFields;
      }
      shared.copiesToHandIn--;
      shared.notifyAll();
      try {
        while (shared.copiesToHandIn > 0) {
          if (isStopped()) {
            return false;
          }
          shared.wait(100);
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return false;
      }
      // A copy that received no filename rows still reads chunks of the other copies' files, with
      // their passed-through fields: it takes the row layout of a copy that received some.
      if (passingThru && data.passThruFields == null && shared.passThruOutputRowMeta != null) {
        data.outputRowMeta = shared.passThruOutputRowMeta.clone();
        data.convertRowMeta = data.outputRowMeta.cloneToType(IValueMeta.TYPE_STRING);
        data.nrPassThruFields = shared.nrPassThruFields;
      }
    }
    return true;
  }

  /**
   * Whether the filename rows this copy received are its own share. A previous transform that
   * copies its rows instead of distributing them sends every filename to every copy: then only the
   * first copy hands them in, or every file would be read once per copy.
   */
  private boolean receivesOwnShareOfFilenames() {
    TransformMeta source =
        getPipelineMeta().findTransform(meta.getFileInput().getAcceptingTransformName());
    boolean copied = source != null && !source.isDistributes() && !source.isPartitioned();
    return !copied || getCopy() == 0;
  }

  /**
   * The chunks of this transform, shared by its copies: the first copy to ask splits the files, the
   * others take from the same queue. With filenames from a previous transform, those of all copies
   * are split together.
   */
  private TextFileChunkPlan chunkPlan() throws HopException {
    Shared shared = shared();
    synchronized (shared) {
      if (shared.plan == null) {
        List<Source> sources = new ArrayList<>();
        if (meta.getFileInput().isAcceptingFilenames()) {
          shared.sourcesByCopy.forEach(sources::addAll);
        } else {
          data.files.getFiles().forEach(file -> sources.add(new Source(file, null)));
        }
        try {
          shared.plan =
              TextFileChunkPlan.buildFromSources(
                  sources,
                  TextFileChunkPlan.parseChunkSize(resolve(meta.getContent().getChunkSize())),
                  getTransformMeta().getCopies(this),
                  meta.getContent().isHeader(),
                  meta.getContent().getNrHeaderLines(),
                  resolve(meta.getEncoding()),
                  meta.isBreakInEnclosureAllowed()
                      ? new RecordFormat(
                          data.enclosure,
                          data.escapeCharacter,
                          data.fileFormatType,
                          meta.getContent().isNoEmptyLines())
                      : null,
                  data.readAhead,
                  getLogChannel());
        } catch (IOException e) {
          throw new HopException("Unable to split the files to read them in parallel", e);
        }
      }
      return shared.plan;
    }
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

      if (data.parallel && meta.getFileInput().isAcceptingFilenames() && !shareFilenames()) {
        setOutputDone();
        return false;
      }

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
