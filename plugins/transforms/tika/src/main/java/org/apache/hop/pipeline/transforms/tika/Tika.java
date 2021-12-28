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

package org.apache.hop.pipeline.transforms.tika;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.Const;
import org.apache.hop.core.ResultFile;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopFileException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.fileinput.FileInputList;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.tika.metadata.Metadata;
import org.json.simple.JSONObject;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Date;
import java.util.List;

public class Tika extends BaseTransform<TikaMeta, TikaData>
    implements ITransform<TikaMeta, TikaData> {
  private static final Class<?> PKG =
      TikaMeta.class; // for i18n purposes, needed by Translator2!! $NON-NLS-1$

  public Tika(
      TransformMeta transformMeta,
      TikaMeta meta,
      TikaData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  private void addFileToResultFilesname(FileObject file) {
    if (meta.isAddingResultFile()) {
      // Add this to the result file names...
      ResultFile resultFile =
          new ResultFile(
              ResultFile.FILE_TYPE_GENERAL, file, getPipelineMeta().getName(), getTransformName());
      resultFile.setComment("File was read by a Tika transform");
      addResultFile(resultFile);
    }
  }

  private boolean openNextFile() {
    try {
      if (meta.isFileInField()) {
        data.readRow = getRow(); // Grab another row ...

        if (data.readRow == null) // finished processing!
        {
          if (isDetailed()) {
            logDetailed(BaseMessages.getString(PKG, "Tika.Log.FinishedProcessing"));
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
          //
          data.convertRowMeta = data.outputRowMeta.cloneToType(IValueMeta.TYPE_STRING);

          if (meta.isFileInField()) {
            // Check is filename field is provided
            if (StringUtils.isEmpty(meta.getDynamicFilenameField())) {
              logError(BaseMessages.getString(PKG, "Tika.Log.NoField"));
              throw new HopException(BaseMessages.getString(PKG, "Tika.Log.NoField"));
            }

            // cache the position of the field
            if (data.indexOfFilenameField < 0) {
              data.indexOfFilenameField =
                  data.inputRowMeta.indexOfValue(meta.getDynamicFilenameField());
              if (data.indexOfFilenameField < 0) {
                // The field is unreachable !
                logError(
                    BaseMessages.getString(PKG, "Tika.Log.ErrorFindingField")
                        + "["
                        + meta.getDynamicFilenameField()
                        + "]");
                throw new HopException(
                    BaseMessages.getString(
                        PKG, "Tika.Exception.CouldnotFindField", meta.getDynamicFilenameField()));
              }
            }
            // Get the number of previous fields
            data.totalPreviousFields = data.inputRowMeta.size();
          }
        } // end if first

        // get field value
        String fieldvalue = data.inputRowMeta.getString(data.readRow, data.indexOfFilenameField);

        if (isDetailed()) {
          logDetailed(
              BaseMessages.getString(
                  PKG, "Tika.Log.Stream", meta.getDynamicFilenameField(), fieldvalue));
        }

        try {
          // Source is a file.
          data.file = HopVfs.getFileObject(fieldvalue);
        } catch (HopFileException e) {
          throw new HopException(e);
        } finally {
          try {
            if (data.file != null) {
              data.file.close();
            }
          } catch (Exception e) {
            logError("Error closing file", e);
          }
        }
      } else {
        if (data.fileNr >= data.files.nrOfFiles()) // finished processing!
        {
          if (isDetailed()) {
            logDetailed(BaseMessages.getString(PKG, "Tika.Log.FinishedProcessing"));
          }
          return false;
        }

        // Is this the last file?
        data.file = data.files.getFile(data.fileNr);
      }

      // Check if file is empty
      if (data.file.getContent() != null) {
        data.fileSize = data.file.getContent().getSize();
      } else {
        data.fileSize = 0;
      }
      // Move file pointer ahead!
      data.fileNr++;

      if (meta.isIgnoreEmptyFile() && data.fileSize == 0) {
        logError(BaseMessages.getString(PKG, "Tika.Error.FileSizeZero", "" + data.file.getName()));
        openNextFile();

      } else {
        if (isDetailed()) {
          logDetailed(BaseMessages.getString(PKG, "Tika.Log.OpeningFile", data.file.toString()));
        }
        data.filename = HopVfs.getFilename(data.file);
        // Add additional fields?
        if (StringUtils.isNotEmpty(meta.getShortFileFieldName())) {
          data.shortFilename = data.file.getName().getBaseName();
        }
        if (StringUtils.isNotEmpty(meta.getPathFieldName())) {
          data.path = HopVfs.getFilename(data.file.getParent());
        }
        if (StringUtils.isNotEmpty(meta.getHiddenFieldName())) {
          data.hidden = data.file.isHidden();
        }
        if (StringUtils.isNotEmpty(meta.getExtensionFieldName())) {
          data.extension = data.file.getName().getExtension();
        }
        if (StringUtils.isNotEmpty(meta.getLastModificationTimeFieldName())) {
          data.lastModificationDateTime = new Date(data.file.getContent().getLastModifiedTime());
        }
        if (StringUtils.isNotEmpty(meta.getUriFieldName())) {
          data.uriName = data.file.getName().getURI();
        }
        if (StringUtils.isNotEmpty(meta.getRootUriNameFieldName())) {
          data.rootUriName = data.file.getName().getRootURI();
        }
        // get File content
        getFileContent();

        addFileToResultFilesname(data.file);

        if (isDetailed()) {
          logDetailed(BaseMessages.getString(PKG, "Tika.Log.FileOpened", data.file.toString()));
        }
      }
    } catch (Exception e) {
      logError(
          BaseMessages.getString(
              PKG,
              "Tika.Log.UnableToOpenFile",
              "" + data.fileNr,
              data.file.toString(),
              e.toString()),
          e);
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
                PKG, "Tika.Log.ReadRow", data.outputRowMeta.getString(outputRowData)));
      }

      putRow(data.outputRowMeta, outputRowData);

      if (meta.getRowLimit() > 0
          && data.rowNr > meta.getRowLimit()) // limit has been reached: stop now.
      {
        setOutputDone();
        return false;
      }
    } catch (HopException e) {
      String errorMessage = "Error encountered : " + e.getMessage();

      if (getTransformMeta().isDoingErrorHandling()) {
        putError(
            getInputRowMeta(), new Object[0], 1, errorMessage, meta.getFilenameField(), "Tika001");
      } else {
        logError(BaseMessages.getString(PKG, "Tika.ErrorInTransformRunning", e.getMessage()));
        throw new HopTransformException(
            BaseMessages.getString(PKG, "Tika.ErrorInTransformRunning"), e);
      }
    }
    return true;
  }

  private void getFileContent() throws HopException {
    try {
      data.fileContent = getTextFileContent(data.file.toString(), meta.getEncoding());
    } catch (OutOfMemoryError o) {
      logError(BaseMessages.getString(PKG, "Tika.Error.NotEnoughMemory", data.file.getName()));
      throw new HopException(o);
    } catch (Exception e) {
      throw new HopException(e);
    }
  }

  /**
   * Read a text file.
   *
   * @param vfsFilename the filename or URL to read from
   * @param encoding the character set of the string (UTF-8, ISO8859-1, etc)
   * @return The content of the file as a String
   * @throws HopException
   */
  public String getTextFileContent(String vfsFilename, String encoding) throws HopException {
    InputStream inputStream = null;
    String retval = null;
    try {
      // HACK: Check for local files, use a FileInputStream in that case
      //  The version of VFS we use will close the stream when all bytes are read, and if
      //  the file is less than 64KB (which is what Tika will read), then bad things happen.
      if (vfsFilename.startsWith("file:")) {
        inputStream = new FileInputStream(vfsFilename.substring(5));
      } else {
        inputStream = HopVfs.getInputStream(vfsFilename);
      }
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      data.tikaOutput.parse(inputStream, meta.getOutputFormat(), baos);
      retval = baos.toString();
    } catch (Exception e) {
      throw new HopException(
          BaseMessages.getString(PKG, "Tika.Error.GettingFileContent", vfsFilename, e.toString()),
          e);
    }
    if (inputStream != null) {
      try {
        inputStream.close();
      } catch (Exception e) {
        log.logError("Error closing reader", e);
      }
    }
    return retval;
  }

  private void handleMissingFiles() throws HopException {
    List<FileObject> nonExistantFiles = data.files.getNonExistantFiles();

    if (!nonExistantFiles.isEmpty()) {
      String message = FileInputList.getRequiredFilesDescription(nonExistantFiles);
      logError(
          BaseMessages.getString(PKG, "Tika.Log.RequiredFilesTitle"),
          BaseMessages.getString(PKG, "Tika.Log.RequiredFiles", message));

      throw new HopException(BaseMessages.getString(PKG, "Tika.Log.RequiredFilesMissing", message));
    }

    List<FileObject> nonAccessibleFiles = data.files.getNonAccessibleFiles();
    if (!nonAccessibleFiles.isEmpty()) {
      String message = FileInputList.getRequiredFilesDescription(nonAccessibleFiles);
      logError(
          BaseMessages.getString(PKG, "Tika.Log.RequiredFilesTitle"),
          BaseMessages.getString(PKG, "Tika.Log.RequiredNotAccessibleFiles", message));
      throw new HopException(
          BaseMessages.getString(PKG, "Tika.Log.RequiredNotAccessibleFilesMissing", message));
    }
  }

  /**
   * Build an empty row based on the meta-data...
   *
   * @return
   */
  private Object[] buildEmptyRow() {
    return RowDataUtil.allocateRowData(data.outputRowMeta.size());
  }

  private Object[] getOneRow() throws HopException {
    if (!openNextFile()) {
      return null;
    }

    // Build an empty row based on the meta-data
    Object[] outputRowData = buildEmptyRow();

    try {
      // Create new row or clone
      if (meta.isFileInField()) {
        System.arraycopy(data.readRow, 0, outputRowData, 0, data.readRow.length);
      }

      int indexField = data.totalPreviousFields;

      if (StringUtils.isNotEmpty(meta.getContentFieldName())) {
        outputRowData[indexField++] = data.fileContent;
      }
      if (StringUtils.isNotEmpty(meta.getFileSizeFieldName())) {
        outputRowData[indexField++] = data.fileSize;
      }
      if (StringUtils.isNotEmpty(meta.getMetadataFieldName())) {
        outputRowData[indexField++] = getMetadataJson(data.tikaOutput.getLastMetadata());
      }
      // See if we need to add the filename to the row...
      if (StringUtils.isNotEmpty(meta.getFilenameField())) {
        outputRowData[indexField++] = data.filename;
      }
      // See if we need to add the row number to the row...
      if (StringUtils.isNotEmpty(meta.getRowNumberField())) {
        outputRowData[indexField++] = data.rowNr;
      }
      // Possibly add short filename...
      if (StringUtils.isNotEmpty(meta.getShortFileFieldName())) {
        outputRowData[indexField++] = data.shortFilename;
      }
      // Add Extension
      if (StringUtils.isNotEmpty(meta.getExtensionFieldName())) {
        outputRowData[indexField++] = data.extension;
      }
      // add path
      if (StringUtils.isNotEmpty(meta.getPathFieldName())) {
        outputRowData[indexField++] = data.path;
      }

      // add Hidden
      if (StringUtils.isNotEmpty(meta.getHiddenFieldName())) {
        outputRowData[indexField++] = data.hidden;
      }
      // Add modification date
      if (StringUtils.isNotEmpty(meta.getLastModificationTimeFieldName())) {
        outputRowData[indexField++] = data.lastModificationDateTime;
      }
      // Add Uri
      if (StringUtils.isNotEmpty(meta.getUriFieldName())) {
        outputRowData[indexField++] = data.uriName;
      }
      // Add RootUri
      if (StringUtils.isNotEmpty(meta.getRootUriNameFieldName())) {
        outputRowData[indexField++] = data.rootUriName;
      }

      incrementLinesInput();
      data.rowNr++;

    } catch (Exception e) {
      throw new HopException("Impossible de charger le fichier", e);
    }

    return outputRowData;
  }

  private String getMetadataJson(Metadata metadata) {
    JSONObject obj = new JSONObject();
    for (String name : metadata.names()) {
      obj.put(name, metadata.get(name));
    }
    return obj.toJSONString();
  }

  @Override
  public boolean init() {

    if (super.init()) {
      if (!meta.isFileInField()) {
        try {
          data.files = meta.getFiles(this);
          handleMissingFiles();

          // Create the output row meta-data
          //
          data.outputRowMeta = new RowMeta();

          // Get the output row metadata populated
          //
          meta.getFields(
              data.outputRowMeta, getTransformName(), null, null, this, metadataProvider);

          // Create convert meta-data objects that will contain Date & Number formatters
          //
          data.convertRowMeta = data.outputRowMeta.cloneToType(IValueMeta.TYPE_STRING);
        } catch (Exception e) {
          logError("Error at step initialization: " + e.toString());
          logError(Const.getStackTracker(e));
          return false;
        }

        try {
          ClassLoader classLoader = meta.getClass().getClassLoader();

          data.tikaOutput = new TikaOutput(classLoader, log, this);

        } catch (Exception e) {
          logError("Tika Error", e);
        }
      }
      data.rowNr = 1L;

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
        logError("Error closing file", e);
      }
    }
    super.dispose();
  }
}
