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

package org.apache.hop.pipeline.transforms.loadfileinput;

import com.google.common.annotations.VisibleForTesting;
import java.io.BufferedInputStream;
import java.io.InputStream;
import java.util.Date;
import java.util.List;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.Const;
import org.apache.hop.core.ResultFile;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.fileinput.FileInputList;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.lineage.LineageFileIoEmitter;
import org.apache.hop.lineage.model.FileIoOperation;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;

/**
 * Read files, parse them and convert them to rows and writes these to one or more output streams.
 */
public class LoadFileInput extends BaseTransform<LoadFileInputMeta, LoadFileInputData> {

  private static final Class<?> PKG = LoadFileInputMeta.class;

  public LoadFileInput(
      TransformMeta transformMeta,
      LoadFileInputMeta meta,
      LoadFileInputData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  private void addFileToResultFilesName(FileObject file) {
    if (meta.isAddingResultFile()) {
      // Add this to the result file names...
      ResultFile resultFile =
          new ResultFile(
              ResultFile.FILE_TYPE_GENERAL, file, getPipelineMeta().getName(), getTransformName());
      resultFile.setComment("File was read by a LoadFileInput transform");
      addResultFile(resultFile);
    }
  }

  boolean openNextFile() {
    try {
      if (meta.isFileInField()) {
        data.readrow = getRow(); // Grab another row ...

        if (data.readrow == null) { // finished processing!

          if (isDetailed()) {
            logDetailed(BaseMessages.getString(PKG, "LoadFileInput.Log.FinishedProcessing"));
          }
          return false;
        }

        if (first) {
          first = false;

          data.inputRowMeta = getInputRowMeta();
          data.outputRowMeta = data.inputRowMeta.clone();
          meta.getFields(
              data.outputRowMeta, getTransformName(), null, null, this, metadataProvider);

          // Create convert meta-data objects that will contain Date & Number formatters
          // All non binary content is handled as a String. It would be converted to the target type
          // after the processing.
          data.convertRowMeta = data.outputRowMeta.cloneToType(IValueMeta.TYPE_STRING);

          if (meta.isFileInField()) {
            // Check is filename field is provided
            if (Utils.isEmpty(meta.getDynamicFilenameField())) {
              logError(BaseMessages.getString(PKG, "LoadFileInput.Log.NoField"));
              throw new HopException(BaseMessages.getString(PKG, "LoadFileInput.Log.NoField"));
            }

            // cache the position of the field
            if (data.indexOfFilenameField < 0) {
              data.indexOfFilenameField =
                  data.inputRowMeta.indexOfValue(meta.getDynamicFilenameField());
              if (data.indexOfFilenameField < 0) {
                // The field is unreachable !
                logError(
                    BaseMessages.getString(PKG, "LoadFileInput.Log.ErrorFindingField")
                        + "["
                        + meta.getDynamicFilenameField()
                        + "]");
                throw new HopException(
                    BaseMessages.getString(
                        PKG,
                        "LoadFileInput.Exception.CouldnotFindField",
                        meta.getDynamicFilenameField()));
              }
            }
            // Get the number of previous fields
            data.totalpreviousfields = data.inputRowMeta.size();
          }
        } // end if first

        // get field value
        String fieldValue = data.inputRowMeta.getString(data.readrow, data.indexOfFilenameField);

        if (isDetailed()) {
          logDetailed(
              BaseMessages.getString(
                  PKG, "LoadFileInput.Log.Stream", meta.getDynamicFilenameField(), fieldValue));
        }

        // Source is a file.
        data.file = HopVfs.getFileObject(fieldValue, variables);
      } else {
        if (data.filenr >= data.files.nrOfFiles()) {
          // finished processing!

          if (isDetailed()) {
            logDetailed(BaseMessages.getString(PKG, "LoadFileInput.Log.FinishedProcessing"));
          }
          return false;
        }

        // Is this the last file?
        data.last_file = (data.filenr == data.files.nrOfFiles() - 1);
        data.file = data.files.getFile(data.filenr);
      }

      // Check if file exists
      if (meta.isIgnoreMissingPath() && !data.file.exists()) {
        logBasic(
            BaseMessages.getString(
                PKG, "LoadFileInput.Error.FileNotExists", "" + data.file.getName()));
        return openNextFile();
      }

      // Check if file is empty
      data.fileSize = data.file.getContent().getSize();
      // Move file pointer ahead!
      data.filenr++;

      if (meta.isIgnoreEmptyFile() && data.fileSize == 0) {
        logError(
            BaseMessages.getString(
                PKG, "LoadFileInput.Error.FileSizeZero", "" + data.file.getName()));
        return openNextFile();

      } else {
        if (isDetailed()) {
          logDetailed(
              BaseMessages.getString(PKG, "LoadFileInput.Log.OpeningFile", data.file.toString()));
        }
        data.filename = HopVfs.getFilename(data.file);
        // Add additional fields?
        if (!Utils.isEmpty(meta.getAdditionalFields().getShortFilenameField())) {
          data.shortFilename = data.file.getName().getBaseName();
        }
        if (!Utils.isEmpty(meta.getAdditionalFields().getPathField())) {
          data.path = HopVfs.getFilename(data.file.getParent());
        }
        if (!Utils.isEmpty(meta.getAdditionalFields().getHiddenField())) {
          data.hidden = data.file.isHidden();
        }
        if (!Utils.isEmpty(meta.getAdditionalFields().getExtensionField())) {
          data.extension = data.file.getName().getExtension();
        }
        if (meta.getAdditionalFields().getLastModificationField() != null
            && !meta.getAdditionalFields().getLastModificationField().isEmpty()) {
          data.lastModificationDateTime = new Date(data.file.getContent().getLastModifiedTime());
        }
        if (!Utils.isEmpty(meta.getAdditionalFields().getUriField())) {
          data.uriName = data.file.getName().getURI();
        }
        if (!Utils.isEmpty(meta.getAdditionalFields().getRootUriField())) {
          data.rootUriName = data.file.getName().getRootURI();
        }
        // get File content
        getFileContent();

        long lineageBytes =
            data.filecontent != null
                ? data.filecontent.length
                : (data.fileSize > 0 ? data.fileSize : 0L);
        LineageFileIoEmitter.emitTransformFileIo(
            this,
            FileIoOperation.READ,
            data.file,
            null,
            lineageBytes > 0 ? lineageBytes : null,
            true,
            null);

        addFileToResultFilesName(data.file);

        if (isDetailed()) {
          logDetailed(
              BaseMessages.getString(PKG, "LoadFileInput.Log.FileOpened", data.file.toString()));
        }
      }

    } catch (Exception e) {
      logError(
          BaseMessages.getString(
              PKG,
              "LoadFileInput.Log.UnableToOpenFile",
              "" + data.filenr,
              data.file.toString(),
              e.toString()));
      stopAll();
      setErrors(1);
      return false;
    }
    return true;
  }

  @Override
  public boolean processRow() throws HopException {
    try {
      // Grab a row
      Object[] outputRowData = getOneRow();
      if (outputRowData == null) {
        setOutputDone(); // signal end to receiver(s)
        return false; // end of data or error.
      }

      if (isRowLevel()) {
        logRowlevel(
            BaseMessages.getString(
                PKG, "LoadFileInput.Log.ReadRow", data.outputRowMeta.getString(outputRowData)));
      }

      putRow(data.outputRowMeta, outputRowData);

      if (meta.getRowLimit() > 0
          && data.rownr > meta.getRowLimit()) { // limit has been reached: stop now.
        setOutputDone();
        return false;
      }
    } catch (HopException e) {
      logError(
          BaseMessages.getString(PKG, "LoadFileInput.ErrorInTransformRunning", e.getMessage()));
      logError(Const.getStackTracker(e));
      setErrors(1);
      stopAll();
      setOutputDone(); // signal end to receiver(s)
      return false;
    }
    return true;
  }

  void getFileContent() throws HopException {
    try {
      data.filecontent = getFileBinaryContent(data.file.toString(), variables);
      if (data.filecontent != null) {
        dataVolumeIn = (dataVolumeIn != null ? dataVolumeIn : 0L) + data.filecontent.length;
      }
    } catch (OutOfMemoryError o) {
      logError(
          "There is no enaugh memory to load the content of the file ["
              + data.file.getName()
              + "]");
      throw new HopException(o);
    } catch (Exception e) {
      throw new HopException(e);
    }
  }

  /**
   * Read a file.
   *
   * @param vfsFilename the filename or URL to read from
   * @return The content of the file as a byte[]
   * @throws HopException In case we couldn't load the binary content.
   */
  public static byte[] getFileBinaryContent(String vfsFilename, IVariables variables)
      throws HopException {
    try (InputStream inputStream = HopVfs.getInputStream(vfsFilename, variables)) {
      return IOUtils.toByteArray(new BufferedInputStream(inputStream));
    } catch (Exception e) {
      throw new HopException(
          BaseMessages.getString(
              PKG, "LoadFileInput.Error.GettingFileContent", vfsFilename, e.toString()));
    }
  }

  private void handleMissingFiles() throws HopException {
    List<FileObject> nonExistentFiles = data.files.getNonExistentFiles();

    if (!nonExistentFiles.isEmpty()) {
      String message = FileInputList.getRequiredFilesDescription(nonExistentFiles);
      logError(
          BaseMessages.getString(PKG, "LoadFileInput.Log.RequiredFilesTitle"),
          BaseMessages.getString(PKG, "LoadFileInput.Log.RequiredFiles", message));

      throw new HopException(
          BaseMessages.getString(PKG, "LoadFileInput.Log.RequiredFilesMissing", message));
    }

    List<FileObject> nonAccessibleFiles = data.files.getNonAccessibleFiles();
    if (!nonAccessibleFiles.isEmpty()) {
      String message = FileInputList.getRequiredFilesDescription(nonAccessibleFiles);
      logError(
          BaseMessages.getString(PKG, "LoadFileInput.Log.RequiredFilesTitle"),
          BaseMessages.getString(PKG, "LoadFileInput.Log.RequiredNotAccessibleFiles", message));
      throw new HopException(
          BaseMessages.getString(
              PKG, "LoadFileInput.Log.RequiredNotAccessibleFilesMissing", message));
    }
  }

  /**
   * @return an empty row based on the meta-data
   */
  private Object[] buildEmptyRow() {
    return RowDataUtil.allocateRowData(data.outputRowMeta.size());
  }

  @VisibleForTesting
  Object[] getOneRow() throws HopException {
    if (!openNextFile()) {
      return null;
    }

    // Build an empty row based on the meta-data
    Object[] outputRowData = buildEmptyRow();

    try {
      // Create new row or clone
      if (meta.isFileInField()) {
        outputRowData = copyOrCloneArrayFromLoadFile(outputRowData, data.readrow);
      }

      // Read fields...
      for (int i = 0; i < data.nrInputFields; i++) {
        // Get field
        LoadFileInputField loadFileInputField = meta.getInputFields().get(i);

        String o = null;
        int indexField = data.totalpreviousfields + i;
        IValueMeta targetValueMeta = data.outputRowMeta.getValueMeta(indexField);
        IValueMeta sourceValueMeta = data.convertRowMeta.getValueMeta(indexField);

        switch (loadFileInputField.getElementType()) {
          case LoadFileInputField.ElementType.FILE_CONTENT:
            if (targetValueMeta.getType() != IValueMeta.TYPE_BINARY) {
              // handle as a String
              if (StringUtils.isNotEmpty(meta.getEncoding())) {
                o = new String(data.filecontent, meta.getEncoding());
              } else {
                o = new String(data.filecontent);
              }
              // convert string (processing type) to the target type
              outputRowData[indexField] =
                  targetValueMeta.convertData(
                      sourceValueMeta, trimField(loadFileInputField.getTrimType(), o));
            } else {
              // save as byte[] without any conversion
              outputRowData[indexField] = data.filecontent;
            }
            break;
          case LoadFileInputField.ElementType.FILE_SIZE:
            o = String.valueOf(data.fileSize);
            outputRowData[indexField] = targetValueMeta.convertData(sourceValueMeta, o);
            break;
          default:
            break;
        }

        // Do we need to repeat this field if it is null?
        if (loadFileInputField.isRepeated() && data.previousRow != null && o == null) {
          outputRowData[indexField] = data.previousRow[indexField];
        }
      } // End of loop over fields...
      int rowIndex = data.totalpreviousfields + data.nrInputFields;

      // See if we need to add the filename to the row...
      if (meta.isIncludeFilename()
          && meta.getFilenameField() != null
          && !meta.getFilenameField().isEmpty()) {
        outputRowData[rowIndex++] = data.filename;
      }

      // See if we need to add the row number to the row...
      if (meta.isIncludeRowNumber()
          && meta.getRowNumberField() != null
          && !meta.getRowNumberField().isEmpty()) {
        outputRowData[rowIndex++] = data.rownr;
      }
      // Possibly add short filename...
      if (!Utils.isEmpty(meta.getAdditionalFields().getShortFilenameField())) {
        outputRowData[rowIndex++] = data.shortFilename;
      }
      // Add Extension
      if (!Utils.isEmpty(meta.getAdditionalFields().getExtensionField())) {
        outputRowData[rowIndex++] = data.extension;
      }
      // add path
      if (!Utils.isEmpty(meta.getAdditionalFields().getPathField())) {
        outputRowData[rowIndex++] = data.path;
      }

      // add Hidden
      if (!Utils.isEmpty(meta.getAdditionalFields().getHiddenField())) {
        outputRowData[rowIndex++] = data.hidden;
      }
      // Add modification date
      if (meta.getAdditionalFields().getLastModificationField() != null
          && !meta.getAdditionalFields().getLastModificationField().isEmpty()) {
        outputRowData[rowIndex++] = data.lastModificationDateTime;
      }
      // Add Uri
      if (!Utils.isEmpty(meta.getAdditionalFields().getUriField())) {
        outputRowData[rowIndex++] = data.uriName;
      }
      // Add RootUri
      if (!Utils.isEmpty(meta.getAdditionalFields().getRootUriField())) {
        outputRowData[rowIndex] = data.rootUriName;
      }
      IRowMeta iRowMeta = getInputRowMeta();

      data.previousRow =
          iRowMeta == null ? outputRowData : iRowMeta.cloneRow(outputRowData); // copy it to make
      // surely the next transform doesn't change it in between...

      incrementLinesInput();
      data.rownr++;

    } catch (Exception e) {
      throw new HopException("Error during processing a row", e);
    }

    return outputRowData;
  }

  private String trimField(int trimType, String o) {
    // DO Trimming!
    switch (trimType) {
      case LoadFileInputField.TYPE_TRIM_LEFT:
        o = Const.ltrim(o);
        break;
      case LoadFileInputField.TYPE_TRIM_RIGHT:
        o = Const.rtrim(o);
        break;
      case LoadFileInputField.TYPE_TRIM_BOTH:
        o = Const.trim(o);
        break;
      default:
        break;
    }

    return o;
  }

  @Override
  public boolean init() {

    if (super.init()) {
      if (!meta.isFileInField()) {
        try {
          data.files = meta.getFiles(this);
          handleMissingFiles();
          // Create the output row meta-data
          data.outputRowMeta = new RowMeta();
          meta.getFields(
              data.outputRowMeta,
              getTransformName(),
              null,
              null,
              this,
              metadataProvider); // get the
          // metadata
          // populated

          // Create convert meta-data objects that will contain Date & Number formatters
          // All non-binary content is handled as a String. It would be converted to the target type
          // after the processing.
          data.convertRowMeta = data.outputRowMeta.cloneToType(IValueMeta.TYPE_STRING);
        } catch (Exception e) {
          logError("Error at transform initialization: " + e.toString());
          logError(Const.getStackTracker(e));
          return false;
        }
      }
      data.rownr = 1L;
      data.nrInputFields = meta.getInputFields().size();

      return true;
    }
    return false;
  }

  @Override
  public void dispose() {

    if (data.file != null) {
      try {
        data.file.close();
      } catch (Exception e) {
        // Ignore errors
      }
    }
    super.dispose();
  }

  protected Object[] copyOrCloneArrayFromLoadFile(Object[] outputRowData, Object[] readrow) {
    // if readrow array is shorter than outputRowData reserved variables, then we can not clone it
    // because we have to
    // preserve the outputRowData reserved variables. Clone, creates a new array with a new length,
    // equals to the
    // readRow length and with that set we lost our outputRowData reserved variables - needed for
    // future additions.
    // The equals case works in both clauses, but arraycopy is up to 5 times faster for smaller
    // arrays.
    if (readrow.length <= outputRowData.length) {
      System.arraycopy(readrow, 0, outputRowData, 0, readrow.length);
    } else {
      // if readrow array is longer than outputRowData reserved variables, then we can only clone
      // it.
      // Copy does not work here and will return an error since we are trying to copy a bigger array
      // into a shorter one.
      outputRowData = readrow.clone();
    }
    return outputRowData;
  }
}
