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

package org.apache.hop.pipeline.transforms.file;

import java.util.Date;
import java.util.HashMap;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.hop.core.IRowSet;
import org.apache.hop.core.ResultFile;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopFileException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.lineage.LineageFileIoEmitter;
import org.apache.hop.lineage.model.FileIoOperation;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;

/** This class contains base functionality for file-based input transforms. */
public abstract class BaseFileInputTransform<
        Meta extends BaseFileInputMeta, Data extends BaseFileInputTransformData>
    extends BaseTransform<Meta, Data> implements IBaseFileInputTransformControl {

  private static final Class<?> PKG = BaseFileInputTransform.class;

  /** Create reader for specific file. */
  protected abstract IBaseFileInputReader createReader(Meta meta, Data data, FileObject file)
      throws Exception;

  public BaseFileInputTransform(
      TransformMeta transformMeta,
      Meta meta,
      Data data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  /**
   * Open next VFS file for processing.
   *
   * <p>This method will support different parallelization methods later.
   */
  protected boolean openNextFile(boolean errorIgnored, boolean skipBadFiles) {
    try {
      if (data.currentFileIndex >= data.files.nrOfFiles()) {
        // all files already processed
        return false;
      }

      // Is this the last file?
      data.file = data.files.getFile(data.currentFileIndex);
      data.filename = HopVfs.getFilename(data.file);

      fillFileAdditionalFields(data, data.file);
      if (meta.getFileInput().isPassingThruFields()) {
        StringBuilder sb = new StringBuilder();
        sb.append(data.currentFileIndex).append("_").append(data.file);
        data.currentPassThruFieldsRow = data.passThruFields.get(sb.toString());
      }

      // Add this files to the result of this pipeline.
      //
      if (meta.getFileInput().isAddingResult()) {
        ResultFile resultFile =
            new ResultFile(
                ResultFile.FILE_TYPE_GENERAL, data.file, getPipelineMeta().getName(), toString());
        resultFile.setComment("File was read by an Text File input transform");
        addResultFile(resultFile);
      }
      if (isBasic()) {
        logBasic("Opening file: " + data.file.getName().getFriendlyURI());
      }

      data.dataErrorLineHandler.handleFile(data.file);

      data.reader = createReader(meta, data, data.file);
      emitLineageFileRead(data.file);
    } catch (Exception e) {
      if (!handleOpenFileException(e, errorIgnored, skipBadFiles)) {
        return false;
      }
      data.reader = null;
    }
    // Move file pointer ahead!
    data.currentFileIndex++;

    return true;
  }

  protected boolean handleOpenFileException(
      Exception e, boolean errorIgnored, boolean skipBadFiles) {
    String errorMsg =
        "Couldn't open file #"
            + data.currentFileIndex
            + " : "
            + data.file.getName().getFriendlyURI();
    if (!failAfterBadFile(errorMsg, errorIgnored, skipBadFiles)) {
      return true;
    }
    stopAll();
    setErrors(getErrors() + 1);
    logError(errorMsg, e);
    return false;
  }

  /**
   * Prepare to process. Executed only first time row processing. It can't be possible to prepare to
   * process in the init() phrase, because files can be in fields from previous transform.
   */
  protected void prepareToRowProcessing(boolean errorIgnored) throws HopException {
    data.outputRowMeta = new RowMeta();
    IRowMeta[] infoTransform = null;

    if (meta.getFileInput().isAcceptingFilenames()) {
      // input files from previous transform
      infoTransform = filesFromPreviousTransform();
    }

    // get the metadata populated. Simple and easy.
    meta.getFields(
        data.outputRowMeta, getTransformName(), infoTransform, null, this, metadataProvider);
    // Create convert meta-data objects that will contain Date & Number formatters
    //
    data.convertRowMeta = data.outputRowMeta.cloneToType(IValueMeta.TYPE_STRING);

    BaseFileInputTransformUtils.handleMissingFiles(
        data.files, getLogChannel(), errorIgnored, data.dataErrorLineHandler);
  }

  @Override
  public boolean checkFeedback(long lines) {
    return super.checkFeedback(lines);
  }

  @Override
  public void addDataVolumeIn(long bytes) {
    if (bytes > 0) {
      dataVolumeIn = (dataVolumeIn != null ? dataVolumeIn : 0L) + bytes;
    }
  }

  /** Best-effort {@link FileIoOperation#READ} lineage for the file being opened. */
  private void emitLineageFileRead(FileObject file) {
    if (file == null) {
      return;
    }
    Long size = null;
    try {
      if (file.getType().hasContent()) {
        size = file.getContent().getSize();
      }
    } catch (Exception ignored) {
      // optional metadata for lineage only
    }
    LineageFileIoEmitter.emitTransformFileIo(
        this, FileIoOperation.READ, file, null, size, true, null);
  }

  /** Read files from previous transform. */
  private IRowMeta[] filesFromPreviousTransform() throws HopException {
    IRowMeta[] infoTransform = null;

    data.files.getFiles().clear();

    int idx = -1;
    IRowSet rowSet = findInputRowSet(meta.getFileInput().getAcceptingTransformName());

    Object[] fileRow = getRowFrom(rowSet);
    while (fileRow != null) {
      IRowMeta prevInfoFields = rowSet.getRowMeta();
      if (idx < 0) {
        if (meta.getFileInput().isPassingThruFields()) {
          data.passThruFields = new HashMap<>();
          infoTransform = new IRowMeta[] {prevInfoFields};
          data.nrPassThruFields = prevInfoFields.size();
        }
        idx = prevInfoFields.indexOfValue(meta.getFileInput().getAcceptingField());
        if (idx < 0) {
          logError(
              BaseMessages.getString(
                  PKG,
                  "BaseFileInputTransform.Log.Error.UnableToFindFilenameField",
                  meta.getFileInput().getAcceptingField()));
          setErrors(getErrors() + 1);
          stopAll();
          return null;
        }
      }
      String fileValue = prevInfoFields.getString(fileRow, idx);
      try {
        FileObject fileObject = HopVfs.getFileObject(fileValue, variables);
        data.files.addFile(fileObject);
        if (meta.getFileInput().isPassingThruFields()) {
          StringBuilder sb = new StringBuilder();
          sb.append(data.files.nrOfFiles() > 0 ? data.files.nrOfFiles() - 1 : 0)
              .append("_")
              .append(fileObject.toString());
          data.passThruFields.put(sb.toString(), fileRow);
        }
      } catch (HopFileException e) {
        logError(
            BaseMessages.getString(
                PKG, "BaseFileInputTransform.Log.Error.UnableToCreateFileObject", fileValue),
            e);
      }

      // Grab another row
      fileRow = getRowFrom(rowSet);
    }

    if (data.files.nrOfFiles() == 0) {
      if (isDetailed()) {
        logDetailed(
            BaseMessages.getString(PKG, "BaseFileInputTransform.Log.Error.NoFilesSpecified"));
      }
      return null;
    }
    return infoTransform;
  }

  /** Close last opened file/ */
  protected void closeLastFile(boolean errorIgnored, boolean skipBadFiles) {
    if (data.reader != null) {
      try {
        data.reader.close();
      } catch (Exception ex) {
        failAfterBadFile("Error close reader", errorIgnored, skipBadFiles);
      }
      data.reader = null;
    }
    if (data.file != null) {
      try {
        data.file.close();
      } catch (Exception ex) {
        failAfterBadFile("Error close file", errorIgnored, skipBadFiles);
      }
      data.file = null;
    }
  }

  /** Prepare file-dependent data for fill additional fields. */
  protected void fillFileAdditionalFields(Data data, FileObject file) throws FileSystemException {
    data.shortFilename = file.getName().getBaseName();
    data.path = HopVfs.getFilename(file.getParent());
    data.hidden = file.isHidden();
    data.extension = file.getName().getExtension();
    data.uriName = file.getName().getURI();
    data.rootUriName = file.getName().getRootURI();
    if (file.getType().hasContent()) {
      data.lastModificationDateTime = new Date(file.getContent().getLastModifiedTime());
      data.size = file.getContent().getSize();
    } else {
      data.lastModificationDateTime = null;
      data.size = null;
    }
  }
}
