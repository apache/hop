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

package org.apache.hop.pipeline.transforms.propertyinput;

import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import org.apache.commons.lang.StringUtils;
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
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.ini4j.Wini;

/**
 * Read all Properties files (&amp; INI files) , convert them to rows and writes these to one or
 * more output streams.
 */
public class PropertyInput extends BaseTransform<PropertyInputMeta, PropertyInputData> {
  private static final Class<?> PKG = PropertyInputMeta.class;

  public PropertyInput(
      TransformMeta transformMeta,
      PropertyInputMeta meta,
      PropertyInputData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public boolean processRow() throws HopException {
    if (first && !meta.isFileField()) {
      data.files = meta.getFiles(this);
      if (data.files == null || data.files.nrOfFiles() == 0) {
        setOutputDone(); // signal end to receiver(s)
        return false;
      }

      handleMissingFiles();

      // Create the output row meta-data
      data.outputRowMeta = new RowMeta();
      meta.getFields(
          data.outputRowMeta,
          getTransformName(),
          null,
          null,
          this,
          metadataProvider); // get the metadata
      // populated

      // Create convert meta-data objects that will contain Date & Number formatters
      //
      data.convertRowMeta = data.outputRowMeta.cloneToType(IValueMeta.TYPE_STRING);
    }
    try {
      // Grab one row
      Object[] outputRowData = getOneRow();
      if (outputRowData == null) {
        setOutputDone(); // signal end to receiver(s)
        return false; // end of data or error.
      }

      putRow(data.outputRowMeta, outputRowData); // copy row to output rowset(s)

      if (meta.getRowLimit() > 0
          && data.rowNumber > meta.getRowLimit()) { // limit has been reached: stop now.
        setOutputDone();
        return false;
      }
    } catch (HopException e) {
      if (getTransformMeta().isDoingErrorHandling()) {
        putError(new RowMeta(), new Object[0], 1, e.toString(), null, "PropertyInput001");
      } else {
        logError(
            BaseMessages.getString(PKG, "PropertyInput.ErrorInTransformRunning", e.getMessage()));
        setErrors(1);
        stopAll();
        setOutputDone(); // signal end to receiver(s)
        return false;
      }
    }
    return true;
  }

  private void handleMissingFiles() throws HopException {
    List<FileObject> nonExistantFiles = data.files.getNonExistentFiles();
    if (!nonExistantFiles.isEmpty()) {
      String message = FileInputList.getRequiredFilesDescription(nonExistantFiles);
      logError(
          BaseMessages.getString(PKG, "PropertyInput.Log.RequiredFilesTitle"),
          BaseMessages.getString(PKG, "PropertyInput.Log.RequiredFiles", message));

      throw new HopException(
          BaseMessages.getString(PKG, "PropertyInput.Log.RequiredFilesMissing", message));
    }

    List<FileObject> nonAccessibleFiles = data.files.getNonAccessibleFiles();
    if (!nonAccessibleFiles.isEmpty()) {
      String message = FileInputList.getRequiredFilesDescription(nonAccessibleFiles);
      logError(
          BaseMessages.getString(PKG, "PropertyInput.Log.RequiredFilesTitle"),
          BaseMessages.getString(PKG, "PropertyInput.Log.RequiredNotAccessibleFiles", message));

      throw new HopException(
          BaseMessages.getString(
              PKG, "PropertyInput.Log.RequiredNotAccessibleFilesMissing", message));
    }
  }

  private Object[] getOneRow() throws HopException {
    try {
      if (meta.isFileField()) {
        while ((data.inputRow == null)
            || ((data.propFiles && !data.iterator.hasNext())
                || (!data.propFiles && !data.iniNameIterator.hasNext()))) {

          // In case we read all sections
          // maybe we have to change section for ini files...
          if (!data.propFiles
              && data.realSection == null
              && data.inputRow != null
              && data.sectionNameIterator.hasNext()) {
            data.iniSection = data.wini.get(data.sectionNameIterator.next().toString());
            data.iniNameIterator = data.iniSection.keySet().iterator();
          } else {
            if (!openNextFile()) {
              return null;
            }
          }
        }
      } else {
        while ((data.file == null)
            || ((data.propFiles && !data.iterator.hasNext())
                || (!data.propFiles && !data.iniNameIterator.hasNext()))) {
          // In case we read all sections
          // maybe we have to change section for ini files...
          if (!data.propFiles
              && data.realSection == null
              && data.file != null
              && data.sectionNameIterator.hasNext()) {
            data.iniSection = data.wini.get(data.sectionNameIterator.next());
            data.iniNameIterator = data.iniSection.keySet().iterator();
          } else {
            if (!openNextFile()) {
              return null;
            }
          }
        }
      }
    } catch (Exception e) {
      logError("Unable to read row from file : " + e.getMessage());
      return null;
    }
    // Build an empty row based on the meta-data
    Object[] outputRow;

    // Create new row or clone
    if (meta.isFileField()) {
      outputRow =
          RowDataUtil.createResizedCopy(
              getInputRowMeta().cloneRow(data.inputRow), data.outputRowMeta.size());
    } else {
      outputRow = RowDataUtil.allocateRowData(data.outputRowMeta.size());
    }

    try {
      String key = null;
      if (data.propFiles) {
        key = data.iterator.next().toString();
      } else {
        key = data.iniNameIterator.next();
      }

      // Execute for each Input field...
      for (int i = 0; i < meta.getInputFields().size(); i++) {
        PropertyInputMeta.PIField field = meta.getInputFields().get(i);
        // Get field value
        String value = null;

        if (field.getColumn() == PropertyInputMeta.KeyValue.KEY) {
          value = key;
        } else {
          if (meta.isResolvingValueVariable()) {
            if (data.propFiles) {
              value = resolve(data.properties.getProperty(key));
            } else {
              value = resolve(data.iniSection.fetch(key)); // for INI files
            }
          } else {
            if (data.propFiles) {
              value = data.properties.getProperty(key);
            } else {
              value = data.iniSection.fetch(key); // for INI files
            }
          }
        }

        // DO Trimming!
        if (field.getTrimType() != null) {
          switch (field.getTrimType()) {
            case LEFT:
              value = Const.ltrim(value);
              break;
            case RIGHT:
              value = Const.rtrim(value);
              break;
            case BOTH:
              value = Const.trim(value);
              break;
            default:
              break;
          }
        }

        if (meta.isFileField()) {
          // Add result field to input stream
          outputRow = RowDataUtil.addValueData(outputRow, data.totalPreviousFields + i, value);
        }

        // DO CONVERSIONS...
        //
        IValueMeta targetValueMeta = data.outputRowMeta.getValueMeta(data.totalPreviousFields + i);
        IValueMeta sourceValueMeta = data.convertRowMeta.getValueMeta(data.totalPreviousFields + i);
        outputRow[data.totalPreviousFields + i] =
            targetValueMeta.convertData(sourceValueMeta, value);

        // Do we need to repeat this field if it is null?
        if (field.isRepeating() && (data.previousRow != null && Utils.isEmpty(value))) {
          outputRow[data.totalPreviousFields + i] = data.previousRow[data.totalPreviousFields + i];
        }
      } // End of loop over fields...

      int rowIndex = meta.getInputFields().size();

      // See if we need to add the filename to the row...
      if (meta.isIncludingFilename() && !Utils.isEmpty(meta.getFilenameField())) {
        outputRow[data.totalPreviousFields + rowIndex++] = data.filename;
      }

      // See if we need to add the row number to the row...
      if (meta.isIncludeRowNumber() && !Utils.isEmpty(meta.getRowNumberField())) {
        outputRow[data.totalPreviousFields + rowIndex++] = data.rowNumber;
      }

      // See if we need to add the section for INI files ...
      if (meta.isIncludeIniSection() && !Utils.isEmpty(meta.getIniSectionField())) {
        outputRow[data.totalPreviousFields + rowIndex++] = resolve(data.iniSection.getName());
      }
      // Possibly add short filename...
      if (StringUtils.isNotEmpty(meta.getShortFileFieldName())) {
        outputRow[data.totalPreviousFields + rowIndex++] = data.shortFilename;
      }
      // Add Extension
      if (StringUtils.isNotEmpty(meta.getExtensionFieldName())) {
        outputRow[data.totalPreviousFields + rowIndex++] = data.extension;
      }
      // add path
      if (StringUtils.isNotEmpty(meta.getPathFieldName())) {
        outputRow[data.totalPreviousFields + rowIndex++] = data.path;
      }
      // Add Size
      if (StringUtils.isNotEmpty(meta.getSizeFieldName())) {
        outputRow[data.totalPreviousFields + rowIndex++] = data.size;
      }
      // add Hidden
      if (StringUtils.isNotEmpty(meta.getHiddenFieldName())) {
        outputRow[data.totalPreviousFields + rowIndex++] = data.hidden;
      }
      // Add modification date
      if (StringUtils.isNotEmpty(meta.getLastModificationTimeFieldName())) {
        outputRow[data.totalPreviousFields + rowIndex++] = data.lastModificationDateTime;
      }
      // Add Uri
      if (StringUtils.isNotEmpty(meta.getUriNameFieldName())) {
        outputRow[data.totalPreviousFields + rowIndex++] = data.uriName;
      }
      // Add RootUri
      if (StringUtils.isNotEmpty(meta.getRootUriNameFieldName())) {
        outputRow[data.totalPreviousFields + rowIndex] = data.rootUriName;
      }
      IRowMeta inputRowMeta = getInputRowMeta();

      data.previousRow =
          inputRowMeta == null ? outputRow : inputRowMeta.cloneRow(outputRow); // copy it to make
      // surely the next transform doesn't change it in between...

      incrementLinesInput();
      data.rowNumber++;
    } catch (Exception e) {
      throw new HopException(
          BaseMessages.getString(
              PKG, "PropertyInput.Error.CanNotReadFromFile", data.file.toString()),
          e);
    }

    return outputRow;
  }

  private boolean openNextFile() {
    InputStream inputStream = null;
    try {
      if (!meta.isFileField()) {
        if (data.fileNumber >= data.files.nrOfFiles()) { // finished processing!

          if (isDetailed()) {
            logDetailed(BaseMessages.getString(PKG, "PropertyInput.Log.FinishedProcessing"));
          }
          return false;
        }

        // Is this the last file?
        data.lastFile = (data.fileNumber == data.files.nrOfFiles() - 1);
        data.file = data.files.getFile(data.fileNumber);

        // Move file pointer ahead!
        data.fileNumber++;
      } else {
        data.inputRow = getRow(); // Get row from input rowset & set row busy!
        if (data.inputRow == null) {
          if (isDetailed()) {
            logDetailed(BaseMessages.getString(PKG, "PropertyInput.Log.FinishedProcessing"));
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

          // Create convert meta-data objects that will contain Date & Number formatters
          data.convertRowMeta = data.outputRowMeta.cloneToType(IValueMeta.TYPE_STRING);

          // Check is filename field is provided
          if (Utils.isEmpty(meta.getDynamicFilenameField())) {
            logError(BaseMessages.getString(PKG, "PropertyInput.Log.NoField"));
            throw new HopException(BaseMessages.getString(PKG, "PropertyInput.Log.NoField"));
          }

          // cache the position of the field
          if (data.indexOfFilenameField < 0) {
            data.indexOfFilenameField =
                getInputRowMeta().indexOfValue(meta.getDynamicFilenameField());
            if (data.indexOfFilenameField < 0) {
              // The field is unreachable !
              logError(
                  BaseMessages.getString(PKG, "PropertyInput.Log.ErrorFindingField")
                      + "["
                      + meta.getDynamicFilenameField()
                      + "]");
              throw new HopException(
                  BaseMessages.getString(
                      PKG,
                      "PropertyInput.Exception.CouldNotFindField",
                      meta.getDynamicFilenameField()));
            }
          }
        } // End if first

        String filename = getInputRowMeta().getString(data.inputRow, data.indexOfFilenameField);
        if (isDetailed()) {
          logDetailed(
              BaseMessages.getString(
                  PKG,
                  "PropertyInput.Log.FilenameInStream",
                  meta.getDynamicFilenameField(),
                  filename));
        }

        data.file = HopVfs.getFileObject(filename, variables);
        // Check if file exists!
      }

      // Check if file is empty
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
      if (StringUtils.isNotEmpty(meta.getUriNameFieldName())) {
        data.uriName = data.file.getName().getURI();
      }
      if (StringUtils.isNotEmpty(meta.getRootUriNameFieldName())) {
        data.rootUriName = data.file.getName().getRootURI();
      }
      if (StringUtils.isNotEmpty(meta.getSizeFieldName())) {
        data.size = data.file.getContent().getSize();
      }

      if (meta.isResettingRowNumber()) {
        data.rowNumber = 0;
      }

      if (isDetailed()) {
        logDetailed(
            BaseMessages.getString(PKG, "PropertyInput.Log.OpeningFile", data.file.toString()));
      }

      if (meta.isAddResult()) {
        // Add this to the result file names...
        ResultFile resultFile =
            new ResultFile(
                ResultFile.FILE_TYPE_GENERAL,
                data.file,
                getPipelineMeta().getName(),
                getTransformName());
        resultFile.setComment(BaseMessages.getString(PKG, "PropertyInput.Log.FileAddedResult"));
        addResultFile(resultFile);
      }

      inputStream = data.file.getContent().getInputStream();
      if (data.propFiles) {
        // load properties file
        data.properties = new Properties();
        data.properties.load(inputStream);
        data.iterator = data.properties.keySet().iterator();
      } else {

        // create wini object
        data.wini = new Wini();
        if (!Utils.isEmpty(data.realEncoding)) {
          data.wini.getConfig().setFileEncoding(Charset.forName(data.realEncoding));
        }

        // load INI file
        data.wini.load(inputStream);

        if (data.realSection != null) {
          // just one section
          data.iniSection = data.wini.get(data.realSection);
          if (data.iniSection == null) {
            throw new HopException(
                BaseMessages.getString(
                    PKG,
                    "PropertyInput.Error.CanNotFindSection",
                    data.realSection,
                    "" + data.file.getName()));
          }
        } else {
          // We need to fetch all sections
          data.sectionNameIterator = data.wini.keySet().iterator();
          data.iniSection = data.wini.get(data.sectionNameIterator.next());
        }
        data.iniNameIterator = data.iniSection.keySet().iterator();
      }

      if (isDetailed()) {
        logDetailed(
            BaseMessages.getString(PKG, "PropertyInput.Log.FileOpened", data.file.toString()));
        logDetailed(
            BaseMessages.getString(
                PKG,
                "PropertyInput.log.TotalKey",
                "" + (data.propFiles ? data.properties.size() : data.iniSection.size()),
                HopVfs.getFilename(data.file)));
      }
    } catch (Exception e) {
      logError(
          BaseMessages.getString(
              PKG,
              "PropertyInput.Log.UnableToOpenFile",
              "" + data.fileNumber,
              data.file.toString(),
              e.toString()));
      stopAll();
      setErrors(1);
      return false;
    } finally {
      BaseTransform.closeQuietly(inputStream);
    }
    return true;
  }

  @Override
  public boolean init() {

    if (super.init()) {

      String realEncoding = resolve(meta.getEncoding());
      if (!Utils.isEmpty(realEncoding)) {
        data.realEncoding = realEncoding;
      }
      String realSection = resolve(meta.getSection());
      if (!Utils.isEmpty(realSection)) {
        data.realSection = realSection;
      }
      data.propFiles = meta.getFileType() == PropertyInputMeta.FileType.PROPERTY;
      data.rowNumber = 1L;
      data.totalPreviousFields = 0;

      return true;
    }
    return false;
  }

  @Override
  public void dispose() {
    if (data.inputRow != null) {
      data.inputRow = null;
    }
    if (data.iniSection != null) {
      data.iniSection.clear();
    }
    data.iniSection = null;
    if (data.sectionNameIterator != null) {
      data.sectionNameIterator = null;
    }
    if (data.file != null) {
      try {
        data.file.close();
        data.file = null;
      } catch (Exception e) {
        // Ignore errors
      }
    }

    super.dispose();
  }
}
