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

package org.apache.hop.pipeline.transforms.propertyoutput;

import java.io.OutputStream;
import java.util.Properties;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.ResultFile;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.io.CountingOutputStream;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.lineage.LineageFileIoEmitter;
import org.apache.hop.lineage.model.FileIoOperation;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;

/** Output rows to Properties file and create a file. */
public class PropertyOutput extends BaseTransform<PropertyOutputMeta, PropertyOutputData> {
  private static final Class<?> PKG = PropertyOutputMeta.class;

  public PropertyOutput(
      TransformMeta transformMeta,
      PropertyOutputMeta meta,
      PropertyOutputData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public boolean processRow() throws HopException {
    Object[] r = getRow(); // this also waits for a previous transform to be finished.

    if (r == null) { // no more input to be expected...
      // Close the currently open output file here so the FILE_IO lineage event is emitted
      // before PipelineCompleted flushes the lineage hub. dispose() is only a safety net
      // and runs after the flush in the local pipeline engine's normal completion path.
      closeFile();
      setOutputDone();
      return false;
    }

    if (first) {
      first = false;
      processRowFirstCall();
    }

    // Get value field
    String propKey = data.inputRowMeta.getString(r, data.indexOfKeyField);
    String propValue = data.inputRowMeta.getString(r, data.indexOfValueField);

    try {
      if (meta.isFileNameInField()) {
        data.filename = data.inputRowMeta.getString(r, data.indexOfFieldFileName);
        if (Utils.isEmpty(data.filename)) {
          throw new HopException(
              BaseMessages.getString(PKG, "PropertyOutputMeta.Log.FileNameEmty"));
        }
        if (!checkSameFile()) {
          // close previous file
          closeFile();
          // Open new file
          openNewFile();
        }
      }

      if (!data.keySet.contains(propKey)) {
        if (isDetailed()) {
          logDetailed(BaseMessages.getString(PKG, "PropertyOutput.Log.Key", propKey));
          logDetailed(BaseMessages.getString(PKG, "PropertyOutput.Log.Value", propValue));
        }
        // Update property
        data.properties.setProperty(propKey, propValue);
        putRow(data.outputRowMeta, r); // in case we want it to go further...
        incrementLinesOutput();

        data.keySet.add(propKey);
      }
    } catch (HopTransformException e) {
      if (!getTransformMeta().isDoingErrorHandling()) {
        logError(
            BaseMessages.getString(PKG, "PropertyOutputMeta.Log.ErrorInTransform")
                + e.getMessage());
        setErrors(1);
        stopAll();
        setOutputDone(); // signal end to receiver(s)
        return false;
      }
      putError(data.outputRowMeta, r, 1L, e.toString(), null, "PROPSOUTPUTO001");
    }
    return true;
  }

  private void processRowFirstCall() throws HopException {
    data.inputRowMeta = getInputRowMeta();
    data.outputRowMeta = data.inputRowMeta.clone();
    meta.getFields(data.outputRowMeta, getTransformName(), null, null, this, metadataProvider);

    // Let's take the index of Key field ...
    data.indexOfKeyField = data.inputRowMeta.indexOfValue(meta.getKeyField());
    if (data.indexOfKeyField < 0) {
      // The field is unreachable !
      logError(
          BaseMessages.getString(PKG, "PropertyOutput.Log.ErrorFindingField", meta.getKeyField()));
      throw new HopException(
          BaseMessages.getString(PKG, "PropertyOutput.Log.ErrorFindingField", meta.getKeyField()));
    }

    // Let's take the index of Key field ...
    data.indexOfValueField = data.inputRowMeta.indexOfValue(meta.getValueField());
    if (data.indexOfValueField < 0) {
      // The field is unreachable !
      logError(
          BaseMessages.getString(
              PKG, "PropertyOutput.Log.ErrorFindingField", meta.getValueField()));
      throw new HopException(
          BaseMessages.getString(
              PKG, "PropertyOutput.Log.ErrorFindingField", meta.getValueField()));
    }

    if (meta.isFileNameInField()) {
      String realFieldName = resolve(meta.getFileNameField());
      if (Utils.isEmpty(realFieldName)) {
        logError(BaseMessages.getString(PKG, "PropertyOutput.Log.FilenameInFieldEmpty"));
        throw new HopException(
            BaseMessages.getString(PKG, "PropertyOutput.Log.FilenameInFieldEmpty"));
      }
      data.indexOfFieldFileName = data.inputRowMeta.indexOfValue(realFieldName);
      if (data.indexOfFieldFileName < 0) {
        // The field is unreachable !
        logError(
            BaseMessages.getString(
                PKG, "PropertyOutput.Log.ErrorFindingField", meta.getValueField()));
        throw new HopException(
            BaseMessages.getString(
                PKG, "PropertyOutput.Log.ErrorFindingField", meta.getValueField()));
      }
    } else {
      // Let's check for filename...
      data.filename = buildFilename();
      // Check if filename is empty..
      if (Utils.isEmpty(data.filename)) {
        logError(BaseMessages.getString(PKG, "PropertyOutput.Log.FilenameEmpty"));
        throw new HopException(BaseMessages.getString(PKG, "PropertyOutput.Log.FilenameEmpty"));
      }
      openNewFile();
    }
  }

  public boolean checkSameFile() {
    return data.previousFileName.equals(data.filename);
  }

  private void openNewFile() throws HopException {
    try (FileObject newFile = HopVfs.getFileObject(data.filename, variables)) {
      data.properties = new Properties();
      data.keySet.clear();

      data.file = newFile;
      if (meta.getFileDetails().isAppending() && data.file.exists()) {
        data.properties.load(HopVfs.getInputStream(data.file));
      }
      // Create parent folder if needed...
      createParentFolder();
      // save processing file
      data.previousFileName = data.filename;
    } catch (Exception e) {
      throw new HopException("Error opening file [" + data.filename + "]!", e);
    }
  }

  private void createParentFolder() throws HopException {
    if (!meta.getFileDetails().isCreateParentFolder()) {
      return;
    }
    try (FileObject parentFolder = data.file.getParent()) {
      // Do we need to create parent folder ?
      // Check for parent folder
      // Get parent folder
      //
      if (!parentFolder.exists()) {
        if (isDetailed()) {
          logDetailed(
              BaseMessages.getString(
                  PKG, "PropertyOutput.Log.ParentFolderExists", parentFolder.getName().toString()));
        }
        parentFolder.createFolder();
        if (isDetailed()) {
          logDetailed(
              BaseMessages.getString(
                  PKG,
                  "PropertyOutput.Log.ParentFolderCreated",
                  parentFolder.getName().toString()));
        }
      }
    } catch (Exception e) {
      logError(
          BaseMessages.getString(
              PKG,
              "PropertyOutput.Log.ParentFolderCouldNotBeCreated",
              data.file.getName().toString()));
      throw new HopException(
          BaseMessages.getString(
              PKG,
              "PropertyOutput.Log.ParentFolderCouldNotBeCreated",
              data.file.getName().toString()));
    }
  }

  private void closeFile() {
    if (data.file == null) {
      return;
    }
    try (OutputStream raw = HopVfs.getOutputStream(data.file, false);
        CountingOutputStream propsFile = new CountingOutputStream(raw)) {
      data.properties.store(propsFile, resolve(meta.getComment()));

      long written = propsFile.getCount();
      dataVolumeOut = (dataVolumeOut != null ? dataVolumeOut : 0L) + written;
      if (!data.isBeamContext() && written > 0 && data.file != null) {
        try {
          LineageFileIoEmitter.emitTransformFileIo(
              this, FileIoOperation.WRITE, null, data.file, written, true, null);
        } catch (Exception ignored) {
          // optional lineage
        }
      }

      if (meta.getFileDetails().isAddToResult()) {
        // Add this to the result file names...
        ResultFile resultFile =
            new ResultFile(
                ResultFile.FILE_TYPE_GENERAL,
                data.file,
                getPipelineMeta().getName(),
                getTransformName());
        resultFile.setComment(BaseMessages.getString(PKG, "PropertyOutput.Log.FileAddedResult"));
        addResultFile(resultFile);
      }
      data.keySet.clear();
    } catch (Exception e) {
      logError("Exception trying to close file [" + data.file.getName() + "]! :" + e.toString());
      setErrors(1);
    } finally {
      if (data.file != null) {
        try {
          data.file.close();
          data.file = null;
        } catch (Exception e) {
          /* Ignore */
          logDetailed("Exception trying to close file [" + data.file.getName() + "]! :", e);
        }
      }
      if (data.properties != null) {
        data.properties = null;
      }
    }
  }

  public String buildFilename() {
    return meta.buildFilename(this, getCopy());
  }

  @Override
  public void dispose() {
    closeFile();
    setOutputDone();
    super.dispose();
  }
}
