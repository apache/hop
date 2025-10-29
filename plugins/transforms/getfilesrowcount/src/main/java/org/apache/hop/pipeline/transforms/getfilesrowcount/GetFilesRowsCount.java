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

package org.apache.hop.pipeline.transforms.getfilesrowcount;

import static org.apache.hop.pipeline.transforms.getfilesrowcount.GetFilesRowsCountMeta.SeparatorFormat.CRLF;

import java.io.InputStream;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.vfs2.FileType;
import org.apache.hop.core.ResultFile;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.getfilesrowcount.GetFilesRowsCountMeta.SeparatorFormat;

/** Read all files, count rows number */
public class GetFilesRowsCount extends BaseTransform<GetFilesRowsCountMeta, GetFilesRowsCountData> {

  private static final Class<?> PKG = GetFilesRowsCountMeta.class;

  public GetFilesRowsCount(
      TransformMeta transformMeta,
      GetFilesRowsCountMeta meta,
      GetFilesRowsCountData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  private boolean getOneRow() throws HopException {
    if (!openNextFile()) {
      return true;
    }

    // Build an empty row based on the meta-data
    //
    try {
      // Create a new output row or clone the input row
      //
      Object[] outputRow;
      if (meta.isFileFromField()) {
        outputRow = getInputRowMeta().cloneRow(data.inputRow);
        outputRow = RowDataUtil.resizeArray(outputRow, data.outputRowMeta.size());
      } else {
        outputRow = RowDataUtil.allocateRowData(data.outputRowMeta.size());
      }

      if (meta.isSmartCount() && data.foundData) {
        // We have data right the last separator,
        // we need to update the outputRow count
        data.rowNumber++;
      }

      outputRow[data.totalPreviousFields] = data.rowNumber;

      if (meta.isIncludeFilesCount()) {
        outputRow[data.totalPreviousFields + 1] = data.fileNumber;
      }

      if (meta.isFileFromField() || data.lastFile) {
        putRow(data.outputRowMeta, outputRow);
        if (isDetailed()) {
          logDetailed(
              BaseMessages.getString(PKG, "GetFilesRowsCount.Log.TotalRowsFiles"),
              data.rowNumber,
              data.fileNumber);
        }
      }

      incrementLinesInput();

    } catch (Exception e) {
      throw new HopException("Unable to read row from file", e);
    }

    return false;
  }

  @Override
  public boolean processRow() throws HopException {
    boolean done = getOneRow();
    if (done) {
      setOutputDone(); // signal end to receiver(s)
      return false; // end of data or error.
    }
    return true;
  }

  private void getRowNumber() throws HopException {
    try {
      if (data.file.getType() == FileType.FILE) {
        try (InputStream inputStream = HopVfs.getInputStream(data.file)) {
          // BufferedInputStream default buffer size
          byte[] buf = new byte[8192];
          int n;
          boolean prevCR = false;
          while ((n = inputStream.read(buf)) != -1) {
            for (int i = 0; i < n; i++) {
              data.foundData = true;
              if (meta.getRowSeparatorFormat() == CRLF) {
                // We need to check for CRLF
                if (buf[i] == '\r' || buf[i] == '\n') {
                  if (buf[i] == '\r') {
                    // we have a carriage return
                    // keep track of it..maybe we will have a line feed right after :-)
                    prevCR = true;
                  } else if (buf[i] == '\n' && prevCR) {
                    // we have a line feed
                    // let's see if we had previously a carriage return
                    // we have a carriage return followed by a line feed
                    data.rowNumber++;
                    // Maybe we won't have data after
                    data.foundData = false;
                    prevCR = false;
                  }
                } else {
                  // we have another char (other than \n , \r)
                  prevCR = false;
                }

              } else {
                if (buf[i] == data.separator) {
                  data.rowNumber++;
                  // Maybe we won't have data after
                  data.foundData = false;
                }
              }
            }
          }
        } catch (Exception ex) {
          throw new HopException(ex);
        }
      }
      if (isDetailed()) {
        logDetailed(
            BaseMessages.getString(
                PKG,
                "GetFilesRowsCount.Log.RowsInFile",
                data.file.toString(),
                "" + data.rowNumber));
      }
    } catch (Exception e) {
      throw new HopException(e);
    }
  }

  private boolean openNextFile() {
    if (data.lastFile) {
      return false; // Done!
    }

    try {
      if (!meta.isFileFromField()) {
        if (getNextFilenameFromField()) {
          return false;
        }
      } else {
        data.inputRow = getRow();
        if (data.inputRow == null) {
          if (isDetailed()) {
            logDetailed(BaseMessages.getString(PKG, "GetFilesRowsCount.Log.FinishedProcessing"));
          }
          return false;
        }

        if (first) {
          first = false;

          data.inputRowMeta = getInputRowMeta();
          data.outputRowMeta = data.inputRowMeta.clone();
          meta.getFields(
              data.outputRowMeta, getTransformName(), null, null, this, metadataProvider);

          // Get total previous fields
          data.totalPreviousFields = data.inputRowMeta.size();

          // Check is filename field is provided
          if (Utils.isEmpty(meta.getOutputFilenameField())) {
            logError(BaseMessages.getString(PKG, "GetFilesRowsCount.Log.NoField"));
            throw new HopException(BaseMessages.getString(PKG, "GetFilesRowsCount.Log.NoField"));
          }

          // cache the position of the field
          if (data.indexOfFilenameField < 0) {
            data.indexOfFilenameField =
                getInputRowMeta().indexOfValue(meta.getOutputFilenameField());
            if (data.indexOfFilenameField < 0) {
              // The field is unreachable !
              logError(
                  BaseMessages.getString(
                      PKG,
                      "GetFilesRowsCount.Log.ErrorFindingField",
                      meta.getOutputFilenameField()));
              throw new HopException(
                  BaseMessages.getString(
                      PKG,
                      "GetFilesRowsCount.Exception.CouldnotFindField",
                      meta.getOutputFilenameField()));
            }
          }
        } // End if first

        String filename = getInputRowMeta().getString(data.inputRow, data.indexOfFilenameField);
        if (isDetailed()) {
          logDetailed(
              BaseMessages.getString(
                  PKG,
                  "GetFilesRowsCount.Log.FilenameInStream",
                  meta.getOutputFilenameField(),
                  filename));
        }

        data.file = HopVfs.getFileObject(filename, variables);

        // Init Row number
        if (meta.isFileFromField()) {
          data.rowNumber = 0;
        }
      }

      // Move file pointer ahead!
      data.fileNumber++;

      if (meta.isAddResultFilename()) {
        // Add this to the result file names...
        ResultFile resultFile =
            new ResultFile(
                ResultFile.FILE_TYPE_GENERAL,
                data.file,
                getPipelineMeta().getName(),
                getTransformName());
        resultFile.setComment(BaseMessages.getString(PKG, "GetFilesRowsCount.Log.FileAddedResult"));
        addResultFile(resultFile);
      }

      if (isDetailed()) {
        logDetailed(
            BaseMessages.getString(PKG, "GetFilesRowsCount.Log.OpeningFile", data.file.toString()));
      }
      getRowNumber();
      if (isDetailed()) {
        logDetailed(
            BaseMessages.getString(PKG, "GetFilesRowsCount.Log.FileOpened", data.file.toString()));
      }

    } catch (Exception e) {
      logError(
          BaseMessages.getString(
              PKG,
              "GetFilesRowsCount.Log.UnableToOpenFile",
              "" + data.fileNumber,
              data.file.toString(),
              e.toString()));
      stopAll();
      setErrors(1);
      return false;
    }
    return true;
  }

  private boolean getNextFilenameFromField() {
    if (data.fileNumber >= data.files.nrOfFiles()) {
      // finished processing!

      if (isDetailed()) {
        logDetailed(BaseMessages.getString(PKG, "GetFilesRowsCount.Log.FinishedProcessing"));
      }
      return true;
    }

    // Is this the last file?
    data.lastFile = (data.fileNumber == data.files.nrOfFiles() - 1);
    data.file = data.files.getFile((int) data.fileNumber);
    return false;
  }

  @Override
  public boolean init() {
    if (!super.init()) {
      return false;
    }
    SeparatorFormat format = meta.getRowSeparatorFormat();
    switch (format) {
      case CUSTOM:
        if (StringUtils.isEmpty(meta.getRowSeparator())) {
          logError(
              BaseMessages.getString(PKG, "GetFilesRowsCount.Error.NoSeparator.Title"),
              BaseMessages.getString(PKG, "GetFilesRowsCount.Error.NoSeparator.Msg"));
          setErrors(1L);
          return false;
        }
        data.separator = resolve(meta.getRowSeparator()).charAt(0);
        break;
      case LF:
        data.separator = '\n';
        break;
      case CR:
        data.separator = '\r';
        break;
      case TAB:
        data.separator = '\t';
        break;
      default:
        break;
    }

    if (isDetailed()) {
      logDetailed(
          BaseMessages.getString(PKG, "GetFilesRowsCount.Log.Separator.Title"),
          BaseMessages.getString(PKG, "GetFilesRowsCount.Log.Separatoris.Infos")
              + " "
              + data.separator);
    }

    if (!meta.isFileFromField()) {
      data.files = meta.getFiles(this);
      if (data.files == null || data.files.nrOfFiles() == 0) {
        logError(BaseMessages.getString(PKG, "GetFilesRowsCount.Log.NoFiles"));
        return false;
      }
      try {
        // Create the output row meta-data
        data.outputRowMeta = new RowMeta();
        meta.getFields(
            data.outputRowMeta, getTransformName(), null, null, this, metadataProvider); // get the
        // metadata
        // populated

      } catch (Exception e) {
        logError("Error initializing transform: ", e);
        return false;
      }
    }
    data.rowNumber = 0;
    data.fileNumber = 0;
    data.totalPreviousFields = 0;

    return true;
  }

  @Override
  public void dispose() {
    if (data.file != null) {
      try {
        data.file.close();
        data.file = null;
      } catch (Exception e) {
        logError("Error closing file", e);
      }
    }
    if (data.lineStringBuilder != null) {
      data.lineStringBuilder = null;
    }

    super.dispose();
  }
}
