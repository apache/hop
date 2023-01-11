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

package org.apache.hop.pipeline.transforms.jsoninput;

import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.fileinput.FileInputList;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaBoolean;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.file.BaseFileInputAdditionalField;
import org.apache.hop.pipeline.transforms.file.BaseFileInputFiles;
import org.apache.hop.pipeline.transforms.file.BaseFileInputMeta;
import org.apache.hop.resource.IResourceNaming;
import org.apache.hop.resource.ResourceDefinition;
import org.w3c.dom.Node;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/** Store run-time data on the JsonInput transform. */
@Transform(
    id = "JsonInput",
    image = "JSI.svg",
    documentationUrl = "/pipeline/transforms/jsoninput.html",
    name = "i18n::JsonInput.name",
    description = "i18n::JsonInput.description",
    keywords = "i18n::JsonInputMeta.keyword",
    categoryDescription = "i18n::JsonInput.category")
public class JsonInputMeta
    extends BaseFileInputMeta<
        JsonInput,
        JsonInputData,
        JsonInputMeta.AdditionalFileOutputFields,
        JsonInputMeta.InputFiles,
        JsonInputField> {
  private static final Class<?> PKG = JsonInputMeta.class; // For Translator

  public static final String[] RequiredFilesDesc =
      new String[] {
        BaseMessages.getString(PKG, "System.Combo.No"),
        BaseMessages.getString(PKG, "System.Combo.Yes")
      };

  // TextFileInputMeta.Content.includeFilename
  /** Flag indicating that we should include the filename in the output */
  @HopMetadataProperty(
      key = "include",
      injectionKey = "FILE_NAME_OUTPUT",
      injectionKeyDescription = "JsonInput.Injection.FILE_NAME_OUTPUT")
  private boolean includeFilename; // InputFiles.isaddresult?..

  // TextFileInputMeta.Content.filenameField
  /** The name of the field in the output containing the filename */
  @HopMetadataProperty(
      key = "include_field",
      injectionKey = "FILE_NAME_FIELDNAME",
      injectionKeyDescription = "JsonInput.Injection.FILE_NAME_FIELDNAME")
  private String filenameField;

  /** Flag indicating that a row number field should be included in the output */
  @HopMetadataProperty(
      key = "rownum",
      injectionKey = "ROW_NUMBER_OUTPUT",
      injectionKeyDescription = "JsonInput.Injection.ROW_NUMBER_OUTPUT")
  private boolean includeRowNumber;

  // TextFileInputMeta.Content.rowNumberField
  /** The name of the field in the output containing the row number */
  @HopMetadataProperty(
      key = "rownum_field",
      injectionKey = "ROW_NUMBER_FIELDNAME",
      injectionKeyDescription = "JsonInput.Injection.ROW_NUMBER_FIELDNAME")
  private String rowNumberField;

  // TextFileInputMeta.Content.rowLimit
  /** The maximum number or lines to read */
  @HopMetadataProperty(
      key = "limit",
      injectionKey = "ROW_LIMIT",
      injectionKeyDescription = "JsonInput.Injection.ROW_LIMIT")
  private long rowLimit;

  public static class InputFiles extends BaseFileInputFiles {
    public void allocate(int nrFiles) {
      fileName = new ArrayList<>();
      fileMask = new ArrayList<>();
      excludeFileMask = new ArrayList<>();
      fileRequired = new ArrayList<>();
      includeSubFolders = new ArrayList<>();
      for (int i = 0; i < nrFiles; i++) {
        fileName.add("");
        fileMask.add("");
        excludeFileMask.add("");
        fileRequired.add(NO);
        includeSubFolders.add(NO);
      }
    }

    @Override
    public InputFiles clone() {
      InputFiles clone = (InputFiles) super.clone();
      clone.allocate(this.fileName.size());
      return clone;
    }
  }

  public static class AdditionalFileOutputFields extends BaseFileInputAdditionalField {

    public void getFields(
        IRowMeta r,
        String name,
        IRowMeta[] info,
        IVariables variables,
        IHopMetadataProvider metadataProvider)
        throws HopTransformException {
      // TextFileInput is the same, this can be refactored further
      if (shortFilenameField != null) {
        IValueMeta v = new ValueMetaString(variables.resolve(shortFilenameField));
        v.setLength(100, -1);
        v.setOrigin(name);
        r.addValueMeta(v);
      }
      if (extensionField != null) {
        IValueMeta v = new ValueMetaString(variables.resolve(extensionField));
        v.setLength(100, -1);
        v.setOrigin(name);
        r.addValueMeta(v);
      }
      if (pathField != null) {
        IValueMeta v = new ValueMetaString(variables.resolve(pathField));
        v.setLength(100, -1);
        v.setOrigin(name);
        r.addValueMeta(v);
      }
      if (sizeField != null) {
        IValueMeta v = new ValueMetaInteger(variables.resolve(sizeField));
        v.setOrigin(name);
        v.setLength(9);
        r.addValueMeta(v);
      }
      if (hiddenField != null) {
        IValueMeta v = new ValueMetaBoolean(variables.resolve(hiddenField));
        v.setOrigin(name);
        r.addValueMeta(v);
      }

      if (lastModificationField != null) {
        IValueMeta v = new ValueMetaDate(variables.resolve(lastModificationField));
        v.setOrigin(name);
        r.addValueMeta(v);
      }
      if (uriField != null) {
        IValueMeta v = new ValueMetaString(variables.resolve(uriField));
        v.setLength(100, -1);
        v.setOrigin(name);
        r.addValueMeta(v);
      }

      if (rootUriField != null) {
        IValueMeta v = new ValueMetaString(variables.resolve(rootUriField));
        v.setLength(100, -1);
        v.setOrigin(name);
        r.addValueMeta(v);
      }
    }
  }

  /** Is In fields */
  @HopMetadataProperty(
      key = "valueField",
      injectionKey = "SOURCE_FIELD_NAME",
      injectionKeyDescription = "JsonInput.Injection.SOURCE_FIELD_NAME")
  private String valueField;

  /** Is In fields */
  @HopMetadataProperty(
      key = "IsInFields",
      injectionKey = "SOURCE_IN_FIELD",
      injectionKeyDescription = "JsonInput.Injection.SOURCE_IN_FIELD")
  private boolean inFields;

  /** Is a File */
  @HopMetadataProperty(
      key = "IsAFile",
      injectionKey = "SOURCE_FIELD_IS_FILENAME",
      injectionKeyDescription = "JsonInput.Injection.SOURCE_FIELD_IS_FILENAME")
  private boolean isAFile;

  /** Flag: add result filename */
  @HopMetadataProperty(
      key = "addresultfile",
      injectionKey = "ADD_RESULT_FILE",
      injectionKeyDescription = "JsonInput.Injection.ADD_RESULT_FILE")
  private boolean addResultFile;

  /** Flag : do we ignore empty files */
  @HopMetadataProperty(
      key = "IsIgnoreEmptyFile",
      injectionKey = "IGNORE_EMPTY_FILE",
      injectionKeyDescription = "JsonInput.Injection.IGNORE_EMPTY_FILE")
  private boolean isIgnoreEmptyFile;

  /** Flag : do not fail if no file */
  @HopMetadataProperty(
      key = "doNotFailIfNoFile",
      injectionKey = "DO_NOT_FAIL_IF_NO_FILE",
      injectionKeyDescription = "JsonInput.Injection.DO_NOT_FAIL_IF_NO_FILE")
  private boolean doNotFailIfNoFile;

  @HopMetadataProperty(
      key = "ignoreMissingPath",
      injectionKey = "IGNORE_MISSING_PATH",
      injectionKeyDescription = "JsonInput.Injection.IGNORE_MISSING_PATH")
  private boolean ignoreMissingPath;

  /** Flag : read url as source */
  @HopMetadataProperty(
      key = "readurl",
      injectionKey = "READ_SOURCE_AS_URL",
      injectionKeyDescription = "JsonInput.Injection.READ_SOURCE_AS_URL")
  private boolean readurl;

  @HopMetadataProperty(
      key = "removeSourceField",
      injectionKey = "REMOVE_SOURCE_FIELDS",
      injectionKeyDescription = "JsonInput.Injection.REMOVE_SOURCE_FIELDS")
  private boolean removeSourceField;

  @HopMetadataProperty(
      key = "defaultPathLeafToNull",
      injectionKey = "ROW_LIMIT",
      injectionKeyDescription = "JsonInput.Injection.ROW_LIMIT")
  private boolean defaultPathLeafToNull;

  public JsonInputMeta() {
    additionalOutputFields = new AdditionalFileOutputFields();
    inputFiles = new InputFiles();
    inputFields = new JsonInputField[0];
  }

  /**
   * Returns the defaultPathLeafToNull.
   *
   * @return defaultPathLeafToNull
   */
  public boolean isDefaultPathLeafToNull() {
    return defaultPathLeafToNull;
  }

  /**
   * Set the defaultPathLeafToNull
   *
   * @param defaultPathLeafToNull the defaultPathLeafToNull to set.
   */
  public void setDefaultPathLeafToNull(boolean defaultPathLeafToNull) {
    this.defaultPathLeafToNull = defaultPathLeafToNull;
  }

  /**
   * @return Returns the shortFileFieldName.
   */
  public String getShortFileNameField() {
    return additionalOutputFields.shortFilenameField;
  }

  /**
   * @param field The shortFileFieldName to set.
   */
  public void setShortFileNameField(String field) {
    additionalOutputFields.shortFilenameField = field;
  }

  /**
   * @return Returns the pathFieldName.
   */
  public String getPathField() {
    return additionalOutputFields.pathField;
  }

  /**
   * @param field The pathFieldName to set.
   */
  public void setPathField(String field) {
    additionalOutputFields.pathField = field;
  }

  /**
   * @return Returns the hiddenFieldName.
   */
  public String isHiddenField() { // name..
    return additionalOutputFields.hiddenField;
  }

  /**
   * @param field The hiddenFieldName to set.
   */
  public void setIsHiddenField(String field) { // name..
    additionalOutputFields.hiddenField = field;
  }

  /**
   * @return Returns the lastModificationTimeFieldName.
   */
  public String getLastModificationDateField() {
    return additionalOutputFields.lastModificationField;
  }

  /**
   * @param field The lastModificationTimeFieldName to set.
   */
  public void setLastModificationDateField(String field) {
    additionalOutputFields.lastModificationField = field;
  }

  /**
   * @return Returns the uriNameFieldName.
   */
  public String getUriField() {
    return additionalOutputFields.uriField;
  }

  /**
   * @param field The uriNameFieldName to set.
   */
  public void setUriField(String field) {
    additionalOutputFields.uriField = field;
  }

  /**
   * @return Returns the uriNameFieldName.
   */
  public String getRootUriField() {
    return additionalOutputFields.rootUriField;
  }

  /**
   * @param field The rootUriNameFieldName to set.
   */
  public void setRootUriField(String field) {
    additionalOutputFields.rootUriField = field;
  }

  /**
   * @return Returns the extensionFieldName.
   */
  public String getExtensionField() {
    return additionalOutputFields.extensionField;
  }

  /**
   * @param field The extensionFieldName to set.
   */
  public void setExtensionField(String field) {
    additionalOutputFields.extensionField = field;
  }

  /**
   * @return Returns the sizeFieldName.
   */
  public String getSizeField() {
    return additionalOutputFields.sizeField;
  }

  /**
   * @param field The sizeFieldName to set.
   */
  public void setSizeField(String field) {
    additionalOutputFields.sizeField = field;
  }

  /**
   * @return the add result filesname flag
   */
  public boolean addResultFile() {
    return addResultFile;
  }

  public boolean isReadUrl() {
    return readurl;
  }

  public void setReadUrl(boolean readurl) {
    this.readurl = readurl;
  }

  public boolean isRemoveSourceField() {
    return removeSourceField;
  }

  public void setRemoveSourceField(boolean removeSourceField) {
    this.removeSourceField = removeSourceField;
  }

  public void setAddResultFile(boolean addResultFile) {
    this.addResultFile = addResultFile;
  }

  @Override
  public JsonInputField[] getInputFields() {
    return super.getInputFields();
  }

  /**
   * @param inputFields The input fields to set.
   */
  public void setInputFields(JsonInputField[] inputFields) {
    this.inputFields = inputFields;
  }

  public List<String> getExcludeFileMask() {
    return inputFiles.excludeFileMask;
  }

  public void setExcludeFileMask(List<String> excludeFileMask) {
    inputFiles.excludeFileMask = excludeFileMask;
  }

  /** Get field value. */
  public String getFieldValue() {
    return valueField;
  }

  public void setFieldValue(String value) {
    this.valueField = value;
    inputFiles.acceptingField = value;
  }

  public boolean isInFields() {
    return inFields;
  }

  public void setInFields(boolean inFields) {
    this.inFields = inFields;
    inputFiles.acceptingFilenames = inFields;
  }

  public List<String> getFileMask() {
    return inputFiles.fileMask;
  }

  public void setFileMask(List<String> fileMask) {
    inputFiles.fileMask = fileMask;
  }

  public List<String> getFileRequired() {
    return inputFiles.fileRequired;
  }

  public void setFileRequired(List<String> fileRequiredin) {
    for (String fileRequired : fileRequiredin) {
      this.inputFiles.fileRequired.add(getRequiredFilesCode(fileRequired));
    }
  }

  public void setIncludeSubFolders(String[] includeSubFoldersin) {
    for (int i = 0; i < includeSubFoldersin.length; i++) {
      this.inputFiles.includeSubFolders.add(getRequiredFilesCode(includeSubFoldersin[i]));
    }
  }

  public List<String> getFileName() {
    return inputFiles.fileName;
  }

  public void setFileName(List<String> fileName) {
    this.inputFiles.fileName = fileName;
  }

  public String getFilenameField() {
    return filenameField;
  }

  public void setFilenameField(String filenameField) {
    this.filenameField = filenameField;
  }

  public boolean includeFilename() {
    return includeFilename;
  }

  public void setIncludeFilename(boolean includeFilename) {
    this.includeFilename = includeFilename;
  }

  /**
   * @return Returns the includeRowNumber.
   */
  public boolean includeRowNumber() {
    return includeRowNumber;
  }

  /**
   * @param includeRowNumber The includeRowNumber to set.
   */
  public void setIncludeRowNumber(boolean includeRowNumber) {
    this.includeRowNumber = includeRowNumber;
  }

  /**
   * @return Returns the rowLimit.
   */
  public long getRowLimit() {
    return rowLimit;
  }

  /**
   * @param rowLimit The rowLimit to set.
   */
  public void setRowLimit(long rowLimit) {
    this.rowLimit = rowLimit;
  }

  /**
   * @return the IsIgnoreEmptyFile flag
   */
  public boolean isIgnoreEmptyFile() {
    return isIgnoreEmptyFile;
  }

  /**
   * @param isIgnoreEmptyFile the IsIgnoreEmptyFile to set
   */
  public void setIgnoreEmptyFile(boolean isIgnoreEmptyFile) {
    this.isIgnoreEmptyFile = isIgnoreEmptyFile;
  }

  public boolean isDoNotFailIfNoFile() {
    return doNotFailIfNoFile;
  }

  public void setDoNotFailIfNoFile(boolean doNotFailIfNoFile) {
    this.doNotFailIfNoFile = doNotFailIfNoFile;
  }

  public boolean isIgnoreMissingPath() {
    return ignoreMissingPath;
  }

  public void setIgnoreMissingPath(boolean ignoreMissingPath) {
    this.ignoreMissingPath = ignoreMissingPath;
  }

  public String getRowNumberField() {
    return rowNumberField;
  }

  public void setRowNumberField(String rowNumberField) {
    this.rowNumberField = rowNumberField;
  }

  public boolean getIsAFile() {
    return isAFile;
  }

  public void setIsAFile(boolean isAFile) {
    this.isAFile = isAFile;
  }

  public List<String> getIncludeSubFolders() {
    return inputFiles.includeSubFolders;
  }

  @Override
  public JsonInputMeta clone() {
    JsonInputMeta clone = (JsonInputMeta) super.clone();
    clone.setFileName(getFileName());
    clone.setFileMask(getFileMask());
    clone.setExcludeFileMask(getExcludeFileMask());
    for (int i = 0; i < inputFields.length; i++) {
      clone.inputFields[i] = inputFields[i].clone();
    }
    return clone;
  }

  public String getRequiredFilesDesc(String tt) {
    if (Utils.isEmpty(tt)) {
      return RequiredFilesDesc[0];
    }
    if (tt.equalsIgnoreCase(RequiredFilesCode[1])) {
      return RequiredFilesDesc[1];
    } else {
      return RequiredFilesDesc[0];
    }
  }

  // For backward compatibility: if "defaultPathLeafToNull" tag is absent in the transform node at
  // all, then we set
  // defaultPathLeafToNull as default true.
  private static boolean getDefaultPathLeafToNull(Node transformnode) {
    boolean result = true;
    List<Node> nodes = XmlHandler.getNodes(transformnode, "defaultPathLeafToNull");
    if (nodes != null && nodes.size() > 0) {
      result = "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformnode, "defaultPathLeafToNull"));
    }
    return result;
  }

  /**
   * @deprecated
   * @param nrFiles
   * @param nrFields
   */
  @Deprecated(since="2.0")
  public void allocate(int nrFiles, int nrFields) {
    initArrayFields(nrFiles, nrFields);
  }

  private void initArrayFields(int nrfiles, int nrFields) {
    setInputFields(new JsonInputField[nrFields]);
    inputFiles.allocate(nrfiles);
    inputFields = new JsonInputField[nrFields];
  }

  @Override
  public void setDefault() {
    additionalOutputFields = new AdditionalFileOutputFields();

    isIgnoreEmptyFile = false;
    ignoreMissingPath = true;
    defaultPathLeafToNull = true;
    doNotFailIfNoFile = true;
    includeFilename = false;
    filenameField = "";
    includeRowNumber = false;
    rowNumberField = "";
    isAFile = false;
    addResultFile = false;

    readurl = false;

    removeSourceField = false;

    int nrFiles = 0;
    int nrFields = 0;

    initArrayFields(nrFiles, nrFields);

    for (int i = 0; i < nrFields; i++) {
      getInputFields()[i] = new JsonInputField("field" + (i + 1));
    }

    rowLimit = 0;

    inFields = false;
    valueField = "";
  }

  @Override
  public void getFields(
      IRowMeta rowMeta,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {

    if (inFields && removeSourceField && !Utils.isEmpty(valueField)) {
      int index = rowMeta.indexOfValue(valueField);
      if (index != -1) {
        rowMeta.removeValueMeta(index);
      }
    }

    for (JsonInputField field : getInputFields()) {
      try {
        rowMeta.addValueMeta(field.toValueMeta(name, variables));
      } catch (Exception e) {
        throw new HopTransformException(e);
      }
    }

    if (includeFilename) {
      IValueMeta v = new ValueMetaString(variables.resolve(filenameField));
      v.setLength(250);
      v.setPrecision(-1);
      v.setOrigin(name);
      rowMeta.addValueMeta(v);
    }

    if (includeRowNumber) {
      IValueMeta v = new ValueMetaInteger(variables.resolve(rowNumberField));
      v.setLength(IValueMeta.DEFAULT_INTEGER_LENGTH, 0);
      v.setOrigin(name);
      rowMeta.addValueMeta(v);
    }
    // Add additional fields
    additionalOutputFields.normalize();
    additionalOutputFields.getFields(rowMeta, name, info, variables, metadataProvider);
  }

  public FileInputList getFiles(IVariables variables) {
    return FileInputList.createFileList(
        variables,
        getFileName().toArray(new String[0]),
        getFileMask().toArray(new String[0]),
        getExcludeFileMask().toArray(new String[0]),
        getFileRequired().toArray(new String[0]),
        inputFiles.includeSubFolderBoolean());
  }

  @Override
  public void check(
      List<ICheckResult> remarks,
      PipelineMeta pipelineMeta,
      TransformMeta transformMeta,
      IRowMeta prev,
      String[] input,
      String[] output,
      IRowMeta info,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {
    CheckResult cr;

    if (!isInFields()) {
      // See if we get input...
      if (input.length <= 0) {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(PKG, "JsonInputMeta.CheckResult.NoInputExpected"),
                transformMeta);
        remarks.add(cr);
      } else {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(PKG, "JsonInputMeta.CheckResult.NoInput"),
                transformMeta);
        remarks.add(cr);
      }
    }

    if (getInputFields().length <= 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "JsonInputMeta.CheckResult.NoInputField"),
              transformMeta);
      remarks.add(cr);
    }

    if (isInFields()) {
      if (Utils.isEmpty(getFieldValue())) {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(PKG, "JsonInputMeta.CheckResult.NoField"),
                transformMeta);
        remarks.add(cr);
      } else {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(PKG, "JsonInputMeta.CheckResult.FieldOk"),
                transformMeta);
        remarks.add(cr);
      }
    } else {
      FileInputList fileInputList = getFiles(variables);
      if (fileInputList == null || fileInputList.getFiles().size() == 0) {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(PKG, "JsonInputMeta.CheckResult.NoFiles"),
                transformMeta);
        remarks.add(cr);
      } else {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(
                    PKG, "JsonInputMeta.CheckResult.FilesOk", "" + fileInputList.getFiles().size()),
                transformMeta);
        remarks.add(cr);
      }
    }
  }

  @Override
  public boolean supportsErrorHandling() {
    return true;
  }

  /**
   * Since the exported transformation that runs this will reside in a ZIP file, we can't reference
   * files relatively. So what this does is turn the name of files into absolute paths OR it simply
   * includes the resource in the ZIP file. For now, we'll simply turn it into an absolute path and
   * pray that the file is on a shared drive or something like that.
   *
   * @param variables the variable variables to use
   * @param definitions
   * @param resourceNamingInterface
   * @param metadataProvider the metadataProvider in which non-Hop metadata could reside.
   * @return the filename of the exported resource
   */
  @Override
  public String exportResources(
      IVariables variables,
      Map<String, ResourceDefinition> definitions,
      IResourceNaming resourceNamingInterface,
      IHopMetadataProvider metadataProvider)
      throws HopException {
    try {
      // The object that we're modifying here is a copy of the original!
      // So let's change the filename from relative to absolute by grabbing the file object...
      // In case the name of the file comes from previous transforms, forget about this!
      //
      List<String> newFilenames = new ArrayList<>();

      if (!isInFields()) {
        FileInputList fileList = getFiles(variables);
        if (fileList.getFiles().size() > 0) {
          for (FileObject fileObject : fileList.getFiles()) {
            // From : ${Internal.Transformation.Filename.Directory}/../foo/bar.xml
            // To : /home/matt/test/files/foo/bar.xml
            //
            // If the file doesn't exist, forget about this effort too!
            //
            if (fileObject.exists()) {
              // Convert to an absolute path and add it to the list.
              //
              newFilenames.add(fileObject.getName().getPath());
            }
          }

          // Still here: set a new list of absolute filenames!
          //
          setFileName(newFilenames);
          setFileMask(new ArrayList<>(Collections.nCopies(newFilenames.size(), null))); // all null since converted to absolute path.
          setFileRequired(new ArrayList<>(Collections.nCopies(newFilenames.size(), YES))); // all null, turn to "Y" :
        }
      }
      return null;
    } catch (Exception e) {
      throw new HopException(e);
    }
  }

  @Override
  public String getEncoding() {
    return "UTF-8";
  }
}
