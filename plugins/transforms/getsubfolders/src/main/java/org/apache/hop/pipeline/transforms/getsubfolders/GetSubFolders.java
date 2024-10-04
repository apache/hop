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

package org.apache.hop.pipeline.transforms.getsubfolders;

import java.util.Date;
import java.util.List;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.fileinput.FileInputList;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;

/**
 * Read all subfolder inside a specified folder and convert them to rows and writes these to one or
 * more output streams.
 */
public class GetSubFolders extends BaseTransform<GetSubFoldersMeta, GetSubFoldersData> {
  private static final Class<?> PKG = GetSubFoldersMeta.class;

  public GetSubFolders(
      TransformMeta transformMeta,
      GetSubFoldersMeta meta,
      GetSubFoldersData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public boolean processRow() throws HopException {
    if (meta.isFolderNameDynamic() && (data.fileIndex >= data.filesCount)) {
      // Grab one row from previous transform ...
      data.inputRow = getRow();
    }

    if (first) {
      first = false;
      initialize();
    }

    if (meta.isFolderNameDynamic()) {
      if (data.inputRow == null) {
        setOutputDone();
        return false;
      }
    } else {
      if (data.fileIndex >= data.filesCount) {
        setOutputDone();
        return false;
      }
    }

    if (outputNextFile()) {
      return false;
    }

    data.fileIndex++;

    if (checkFeedback(getLinesInput()) && isBasic()) {
      logBasic(BaseMessages.getString(PKG, "GetSubFolders.Log.NrLine", "" + getLinesInput()));
    }

    return true;
  }

  private boolean outputNextFile() throws HopTransformException {
    try {
      Object[] outputRow = RowDataUtil.allocateRowData(data.outputRowMeta.size());
      int outputIndex = 0;
      Object[] extraData = new Object[data.nrTransformFields];
      if (meta.isFolderNameDynamic()) {
        if (data.fileIndex >= data.filesCount) {
          // Get value of dynamic filename field ...
          String filename = getInputRowMeta().getString(data.inputRow, data.indexOfFolderNameField);

          String[] filesNames = {filename};
          String[] filesRequired = {"N"};

          // Get files list
          data.files = meta.getDynamicFolderList(this, filesNames, filesRequired);
          data.filesCount = data.files.nrOfFiles();
          data.fileIndex = 0;
        }

        // Clone current input row
        outputRow = data.inputRow.clone();
      }
      if (data.filesCount > 0) {
        data.file = data.files.getFile(data.fileIndex);

        // filename
        extraData[outputIndex++] = HopVfs.getFilename(data.file);

        // short filename
        extraData[outputIndex++] = data.file.getName().getBaseName();

        // Path
        extraData[outputIndex++] = HopVfs.getFilename(data.file.getParent());

        // is hidden?
        extraData[outputIndex++] = data.file.isHidden();

        // is readable?
        extraData[outputIndex++] = data.file.isReadable();

        // is writeable?
        extraData[outputIndex++] = data.file.isWriteable();

        // last modified time
        extraData[outputIndex++] = new Date(data.file.getContent().getLastModifiedTime());

        // uri
        extraData[outputIndex++] = data.file.getName().getURI();

        // root URI
        extraData[outputIndex++] = data.file.getName().getRootURI();

        // nr of child files
        extraData[outputIndex++] = (long) data.file.getChildren().length;

        // See if we need to add the row number to the row...
        if (meta.isIncludeRowNumber() && !Utils.isEmpty(meta.getRowNumberField())) {
          extraData[outputIndex] = data.rowNumber;
        }

        data.rowNumber++;
        // Add row data
        outputRow = RowDataUtil.addRowData(outputRow, data.totalPreviousFields, extraData);
        // Send row
        putRow(data.outputRowMeta, outputRow);

        if (meta.getRowLimit() > 0
            && data.rowNumber >= meta.getRowLimit()) { // limit has been reached: stop now.
          setOutputDone();
          return true;
        }
      }
    } catch (Exception e) {
      throw new HopTransformException(e);
    }
    return false;
  }

  private void initialize() throws HopException {
    if (meta.isFolderNameDynamic()) {
      data.inputRowMeta = getInputRowMeta();
      data.outputRowMeta = data.inputRowMeta.clone();
      meta.getFields(data.outputRowMeta, getTransformName(), null, null, this, metadataProvider);

      // Get total previous fields
      data.totalPreviousFields = data.inputRowMeta.size();

      // Check is filename field is provided
      if (Utils.isEmpty(meta.getDynamicFolderNameField())) {
        logError(BaseMessages.getString(PKG, "GetSubFolders.Log.NoField"));
        throw new HopException(BaseMessages.getString(PKG, "GetSubFolders.Log.NoField"));
      }

      // cache the position of the field
      if (data.indexOfFolderNameField < 0) {
        String realDynamicFolderName = resolve(meta.getDynamicFolderNameField());
        data.indexOfFolderNameField = data.inputRowMeta.indexOfValue(realDynamicFolderName);
        if (data.indexOfFolderNameField < 0) {
          // The field is unreachable !
          logError(
              BaseMessages.getString(PKG, "GetSubFolders.Log.ErrorFindingField")
                  + "["
                  + realDynamicFolderName
                  + "]");
          throw new HopException(
              BaseMessages.getString(
                  PKG, "GetSubFolders.Exception.CouldnotFindField", realDynamicFolderName));
        }
      }
    } else {
      // Create the output row meta-data
      data.outputRowMeta = new RowMeta();
      meta.getFields(
          data.outputRowMeta, getTransformName(), null, null, this, metadataProvider); // get the
      // metadata
      // populated

      data.files = meta.getFolderList(this);
      data.filesCount = data.files.nrOfFiles();
      handleMissingFiles();
    }
    data.nrTransformFields = data.outputRowMeta.size();
  }

  private void handleMissingFiles() throws HopException {
    List<FileObject> nonExistantFiles = data.files.getNonExistentFiles();

    if (!nonExistantFiles.isEmpty()) {
      String message = FileInputList.getRequiredFilesDescription(nonExistantFiles);
      logError(BaseMessages.getString(PKG, "GetSubFolders.Error.MissingFiles", message));
      throw new HopException(
          BaseMessages.getString(PKG, "GetSubFolders.Exception.MissingFiles", message));
    }

    List<FileObject> nonAccessibleFiles = data.files.getNonAccessibleFiles();
    if (!nonAccessibleFiles.isEmpty()) {
      String message = FileInputList.getRequiredFilesDescription(nonAccessibleFiles);
      logError(BaseMessages.getString(PKG, "GetSubFolders.Error.NoAccessibleFiles", message));
      throw new HopException(
          BaseMessages.getString(PKG, "GetSubFolders.Exception.NoAccessibleFiles", message));
    }
  }

  @Override
  public boolean init() {
    if (super.init()) {
      try {
        data.filesCount = 0;
        data.rowNumber = 1L;
        data.fileIndex = 0;
        data.totalPreviousFields = 0;
      } catch (Exception e) {
        logError("Error initializing transform: ", e);
        return false;
      }

      return true;
    }
    return false;
  }

  @Override
  public void dispose() {
    if (data.file != null) {
      try {
        data.file.close();
        data.file = null;
      } catch (Exception e) {
        // Ignore close errors
      }
    }
    super.dispose();
  }
}
