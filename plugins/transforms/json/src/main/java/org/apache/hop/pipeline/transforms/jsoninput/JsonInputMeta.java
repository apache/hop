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
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.fileinput.FileInputList;
import org.apache.hop.core.injection.Injection;
import org.apache.hop.core.injection.InjectionSupported;
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
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.file.BaseFileInputAdditionalField;
import org.apache.hop.pipeline.transforms.file.BaseFileInputFiles;
import org.apache.hop.pipeline.transforms.file.BaseFileInputMeta;
import org.apache.hop.resource.IResourceNaming;
import org.apache.hop.resource.ResourceDefinition;
import org.w3c.dom.Node;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/** Store run-time data on the JsonInput transform. */
@Transform(
    id = "JsonInput",
    image = "JSI.svg",
    documentationUrl = "https://hop.apache.org/manual/latest/plugins/transforms/jsoninput.html",
    name = "i18n::JsonInput.name",
    description = "i18n::JsonInput.description",
    categoryDescription = "i18n::JsonInput.category")
@InjectionSupported(
    localizationPrefix = "JsonInput.Injection.",
    groups = {"FILENAME_LINES", "FIELDS"},
    hide = {
      "ACCEPT_FILE_NAMES",
      "ACCEPT_FILE_TRANSFORM",
      "PASS_THROUGH_FIELDS",
      "ACCEPT_FILE_FIELD",
      "ADD_FILES_TO_RESULT",
      "IGNORE_ERRORS",
      "FILE_ERROR_FIELD",
      "FILE_ERROR_MESSAGE_FIELD",
      "SKIP_BAD_FILES",
      "WARNING_FILES_TARGET_DIR",
      "WARNING_FILES_EXTENTION",
      "ERROR_FILES_TARGET_DIR",
      "ERROR_FILES_EXTENTION",
      "LINE_NR_FILES_TARGET_DIR",
      "LINE_NR_FILES_EXTENTION",
      "FIELD_NULL_STRING",
      "FIELD_POSITION",
      "FIELD_IGNORE",
      "FIELD_IF_NULL"
    })
public class JsonInputMeta
    extends BaseFileInputMeta<
        JsonInputMeta.AdditionalFileOutputFields,
        JsonInputMeta.InputFiles,
        JsonInputField,
        JsonInput,
        JsonInputData>
    implements ITransformMeta<JsonInput, JsonInputData> {
  private static final Class<?> PKG = JsonInputMeta.class; // For Translator

  public static final String[] RequiredFilesDesc =
      new String[] {
        BaseMessages.getString(PKG, "System.Combo.No"),
        BaseMessages.getString(PKG, "System.Combo.Yes")
      };

  // TextFileInputMeta.Content.includeFilename
  /** Flag indicating that we should include the filename in the output */
  @Injection(name = "FILE_NAME_OUTPUT")
  private boolean includeFilename; // InputFiles.isaddresult?..

  // TextFileInputMeta.Content.filenameField
  /** The name of the field in the output containing the filename */
  @Injection(name = "FILE_NAME_FIELDNAME")
  private String filenameField;

  /** Flag indicating that a row number field should be included in the output */
  @Injection(name = "ROW_NUMBER_OUTPUT")
  private boolean includeRowNumber;

  // TextFileInputMeta.Content.rowNumberField
  /** The name of the field in the output containing the row number */
  @Injection(name = "ROW_NUMBER_FIELDNAME")
  private String rowNumberField;

  // TextFileInputMeta.Content.rowLimit
  /** The maximum number or lines to read */
  @Injection(name = "ROW_LIMIT")
  private long rowLimit;

  protected void setInputFiles(InputFiles inputFiles) {
    this.inputFiles = inputFiles;
  }

  protected InputFiles getInputFiles() {
    return this.inputFiles;
  }

  public static class InputFiles extends BaseFileInputFiles {
    public void allocate(int nrFiles) {
      fileName = new String[nrFiles];
      fileMask = new String[nrFiles];
      excludeFileMask = new String[nrFiles];
      fileRequired = new String[nrFiles];
      includeSubFolders = new String[nrFiles];
      Arrays.fill(fileName, "");
      Arrays.fill(fileMask, "");
      Arrays.fill(excludeFileMask, "");
      Arrays.fill(fileRequired, NO);
      Arrays.fill(includeSubFolders, NO);
    }

    @Override
    public InputFiles clone() {
      InputFiles clone = (InputFiles) super.clone();
      clone.allocate(this.fileName.length);
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
  @Injection(name = "SOURCE_FIELD_NAME")
  private String valueField;

  /** Is In fields */
  @Injection(name = "SOURCE_IN_FIELD")
  private boolean inFields;

  /** Is a File */
  @Injection(name = "SOURCE_FIELD_IS_FILENAME")
  private boolean isAFile;

  /** Flag: add result filename */
  @Injection(name = "ADD_RESULT_FILE")
  private boolean addResultFile;

  /** Flag : do we ignore empty files */
  @Injection(name = "IGNORE_EMPTY_FILE")
  private boolean isIgnoreEmptyFile;

  /** Flag : do not fail if no file */
  @Injection(name = "DO_NOT_FAIL_IF_NO_FILE")
  private boolean doNotFailIfNoFile;

  @Injection(name = "IGNORE_MISSING_PATH")
  private boolean ignoreMissingPath;

  /** Flag : read url as source */
  @Injection(name = "READ_SOURCE_AS_URL")
  private boolean readurl;

  @Injection(name = "REMOVE_SOURCE_FIELDS")
  private boolean removeSourceField;

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

  /** @return Returns the shortFileFieldName. */
  public String getShortFileNameField() {
    return additionalOutputFields.shortFilenameField;
  }

  /** @param field The shortFileFieldName to set. */
  public void setShortFileNameField(String field) {
    additionalOutputFields.shortFilenameField = field;
  }

  /** @return Returns the pathFieldName. */
  public String getPathField() {
    return additionalOutputFields.pathField;
  }

  /** @param field The pathFieldName to set. */
  public void setPathField(String field) {
    additionalOutputFields.pathField = field;
  }

  /** @return Returns the hiddenFieldName. */
  public String isHiddenField() { // name..
    return additionalOutputFields.hiddenField;
  }

  /** @param field The hiddenFieldName to set. */
  public void setIsHiddenField(String field) { // name..
    additionalOutputFields.hiddenField = field;
  }

  /** @return Returns the lastModificationTimeFieldName. */
  public String getLastModificationDateField() {
    return additionalOutputFields.lastModificationField;
  }

  /** @param field The lastModificationTimeFieldName to set. */
  public void setLastModificationDateField(String field) {
    additionalOutputFields.lastModificationField = field;
  }

  /** @return Returns the uriNameFieldName. */
  public String getUriField() {
    return additionalOutputFields.uriField;
  }

  /** @param field The uriNameFieldName to set. */
  public void setUriField(String field) {
    additionalOutputFields.uriField = field;
  }

  /** @return Returns the uriNameFieldName. */
  public String getRootUriField() {
    return additionalOutputFields.rootUriField;
  }

  /** @param field The rootUriNameFieldName to set. */
  public void setRootUriField(String field) {
    additionalOutputFields.rootUriField = field;
  }

  /** @return Returns the extensionFieldName. */
  public String getExtensionField() {
    return additionalOutputFields.extensionField;
  }

  /** @param field The extensionFieldName to set. */
  public void setExtensionField(String field) {
    additionalOutputFields.extensionField = field;
  }

  /** @return Returns the sizeFieldName. */
  public String getSizeField() {
    return additionalOutputFields.sizeField;
  }

  /** @param field The sizeFieldName to set. */
  public void setSizeField(String field) {
    additionalOutputFields.sizeField = field;
  }

  /** @return the add result filesname flag */
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

  /** @param inputFields The input fields to set. */
  public void setInputFields(JsonInputField[] inputFields) {
    this.inputFields = inputFields;
  }

  /** @deprecated use {@link#getExcludeFileMask()} */
  @Deprecated
  public String[] getExludeFileMask() {
    return getExcludeFileMask();
  }

  public String[] getExcludeFileMask() {
    return inputFiles.excludeFileMask;
  }

  public void setExcludeFileMask(String[] excludeFileMask) {
    inputFiles.excludeFileMask = excludeFileMask;
  }

  /** Get field value. */
  public String getFieldValue() {
    return valueField;
  }

  public void setFieldValue(String value) {
    this.valueField = value;
    inputFiles.acceptingField = value; // TODO
  }

  public boolean isInFields() {
    return inFields;
  }

  public void setInFields(boolean inFields) {
    this.inFields = inFields;
    inputFiles.acceptingFilenames = inFields;
  }

  public String[] getFileMask() {
    return inputFiles.fileMask;
  }

  public void setFileMask(String[] fileMask) {
    inputFiles.fileMask = fileMask;
  }

  public String[] getFileRequired() {
    return inputFiles.fileRequired;
  }

  public void setFileRequired(String[] fileRequiredin) {
    for (int i = 0; i < fileRequiredin.length; i++) {
      this.inputFiles.fileRequired[i] = getRequiredFilesCode(fileRequiredin[i]);
    }
  }

  public void setIncludeSubFolders(String[] includeSubFoldersin) {
    for (int i = 0; i < includeSubFoldersin.length; i++) {
      this.inputFiles.includeSubFolders[i] = getRequiredFilesCode(includeSubFoldersin[i]);
    }
  }

  public String[] getFileName() {
    return inputFiles.fileName;
  }

  public void setFileName(String[] fileName) {
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

  /** @return Returns the includeRowNumber. */
  public boolean includeRowNumber() {
    return includeRowNumber;
  }

  /** @param includeRowNumber The includeRowNumber to set. */
  public void setIncludeRowNumber(boolean includeRowNumber) {
    this.includeRowNumber = includeRowNumber;
  }

  /** @return Returns the rowLimit. */
  public long getRowLimit() {
    return rowLimit;
  }

  /** @param rowLimit The rowLimit to set. */
  public void setRowLimit(long rowLimit) {
    this.rowLimit = rowLimit;
  }

  /** @return the IsIgnoreEmptyFile flag */
  public boolean isIgnoreEmptyFile() {
    return isIgnoreEmptyFile;
  }

  /** @param isIgnoreEmptyFile the IsIgnoreEmptyFile to set */
  public void setIgnoreEmptyFile(boolean isIgnoreEmptyFile) {
    this.isIgnoreEmptyFile = isIgnoreEmptyFile;
  }

  @Deprecated
  public boolean isdoNotFailIfNoFile() {
    return isDoNotFailIfNoFile();
  }

  @Deprecated
  public void setdoNotFailIfNoFile(boolean doNotFailIfNoFile) {
    setDoNotFailIfNoFile(doNotFailIfNoFile);
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

  public String[] getIncludeSubFolders() {
    return inputFiles.includeSubFolders;
  }

  @Override
  public void loadXml(Node transformnode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    readData(transformnode, metadataProvider);
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

  @Override
  public String getXml() {
    StringBuilder retval = new StringBuilder(400);

    retval.append("    ").append(XmlHandler.addTagValue("include", includeFilename));
    retval.append("    ").append(XmlHandler.addTagValue("include_field", filenameField));
    retval.append("    ").append(XmlHandler.addTagValue("rownum", includeRowNumber));
    retval.append("    ").append(XmlHandler.addTagValue("addresultfile", addResultFile));

    retval.append("    ").append(XmlHandler.addTagValue("readurl", readurl));

    retval.append("    ").append(XmlHandler.addTagValue("removeSourceField", removeSourceField));

    retval.append("    " + XmlHandler.addTagValue("IsIgnoreEmptyFile", isIgnoreEmptyFile));
    retval.append("    " + XmlHandler.addTagValue("doNotFailIfNoFile", doNotFailIfNoFile));
    retval.append("    " + XmlHandler.addTagValue("ignoreMissingPath", ignoreMissingPath));
    retval.append("    " + XmlHandler.addTagValue("defaultPathLeafToNull", defaultPathLeafToNull));
    retval.append("    ").append(XmlHandler.addTagValue("rownum_field", rowNumberField));

    retval.append("    <file>").append(Const.CR);
    for (int i = 0; i < getFileName().length; i++) {
      retval.append("      ").append(XmlHandler.addTagValue("name", getFileName()[i]));
      retval.append("      ").append(XmlHandler.addTagValue("filemask", getFileMask()[i]));
      retval
          .append("      ")
          .append(XmlHandler.addTagValue("exclude_filemask", getExcludeFileMask()[i]));
      retval.append("      ").append(XmlHandler.addTagValue("file_required", getFileRequired()[i]));
      retval
          .append("      ")
          .append(XmlHandler.addTagValue("include_subfolders", getIncludeSubFolders()[i]));
    }
    retval.append("    </file>").append(Const.CR);

    retval.append("    <fields>").append(Const.CR);
    for (int i = 0; i < getInputFields().length; i++) {
      JsonInputField field = getInputFields()[i];
      retval.append(field.getXml());
    }
    retval.append("    </fields>").append(Const.CR);

    retval.append("    ").append(XmlHandler.addTagValue("limit", rowLimit));

    retval.append("    ").append(XmlHandler.addTagValue("IsInFields", inFields));
    retval.append("    ").append(XmlHandler.addTagValue("IsAFile", isAFile));
    retval.append("    ").append(XmlHandler.addTagValue("valueField", valueField));

    retval
        .append("    ")
        .append(XmlHandler.addTagValue("shortFileFieldName", getShortFileNameField()));
    retval.append("    ").append(XmlHandler.addTagValue("pathFieldName", getPathField()));
    retval.append("    ").append(XmlHandler.addTagValue("hiddenFieldName", isHiddenField()));
    retval
        .append("    ")
        .append(
            XmlHandler.addTagValue(
                "lastModificationTimeFieldName", getLastModificationDateField()));
    retval.append("    ").append(XmlHandler.addTagValue("uriNameFieldName", getUriField()));
    retval.append("    ").append(XmlHandler.addTagValue("rootUriNameFieldName", getUriField()));
    retval.append("    ").append(XmlHandler.addTagValue("extensionFieldName", getExtensionField()));
    retval.append("    ").append(XmlHandler.addTagValue("sizeFieldName", getSizeField()));
    return retval.toString();
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

  private void readData(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    try {
      includeFilename = "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "include"));
      filenameField = XmlHandler.getTagValue(transformNode, "include_field");
      addResultFile = "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "addresultfile"));
      readurl = "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "readurl"));
      removeSourceField =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "removeSourceField"));
      isIgnoreEmptyFile =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "IsIgnoreEmptyFile"));
      ignoreMissingPath =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "ignoreMissingPath"));
      defaultPathLeafToNull = getDefaultPathLeafToNull(transformNode);
      doNotFailIfNoFile =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "doNotFailIfNoFile"));
      includeRowNumber = "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "rownum"));
      rowNumberField = XmlHandler.getTagValue(transformNode, "rownum_field");

      Node filenode = XmlHandler.getSubNode(transformNode, "file");
      Node fields = XmlHandler.getSubNode(transformNode, "fields");
      int nrFiles = XmlHandler.countNodes(filenode, "name");
      int nrFields = XmlHandler.countNodes(fields, "field");

      initArrayFields(nrFiles, nrFields);

      for (int i = 0; i < nrFiles; i++) {
        Node filenamenode = XmlHandler.getSubNodeByNr(filenode, "name", i);
        Node filemasknode = XmlHandler.getSubNodeByNr(filenode, "filemask", i);
        Node excludefilemasknode = XmlHandler.getSubNodeByNr(filenode, "exclude_filemask", i);
        Node fileRequirednode = XmlHandler.getSubNodeByNr(filenode, "file_required", i);
        Node includeSubFoldersnode = XmlHandler.getSubNodeByNr(filenode, "include_subfolders", i);
        getFileName()[i] = XmlHandler.getNodeValue(filenamenode);
        getFileMask()[i] = XmlHandler.getNodeValue(filemasknode);
        getExcludeFileMask()[i] = XmlHandler.getNodeValue(excludefilemasknode);
        getFileRequired()[i] = XmlHandler.getNodeValue(fileRequirednode);
        getIncludeSubFolders()[i] = XmlHandler.getNodeValue(includeSubFoldersnode);
      }

      for (int i = 0; i < nrFields; i++) {
        Node fnode = XmlHandler.getSubNodeByNr(fields, "field", i);
        JsonInputField field = new JsonInputField(fnode);
        getInputFields()[i] = field;
      }

      // Is there a limit on the number of rows we process?
      rowLimit = Const.toLong(XmlHandler.getTagValue(transformNode, "limit"), 0L);

      setInFields("Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "IsInFields")));
      isAFile = "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "IsAFile"));
      setFieldValue(XmlHandler.getTagValue(transformNode, "valueField"));
      setShortFileNameField(XmlHandler.getTagValue(transformNode, "shortFileFieldName"));
      setPathField(XmlHandler.getTagValue(transformNode, "pathFieldName"));
      setIsHiddenField(XmlHandler.getTagValue(transformNode, "hiddenFieldName"));
      setLastModificationDateField(
          XmlHandler.getTagValue(transformNode, "lastModificationTimeFieldName"));
      setUriField(XmlHandler.getTagValue(transformNode, "uriNameFieldName"));
      setRootUriField(XmlHandler.getTagValue(transformNode, "rootUriNameFieldName"));
      setExtensionField(XmlHandler.getTagValue(transformNode, "extensionFieldName"));
      setSizeField(XmlHandler.getTagValue(transformNode, "sizeFieldName"));
    } catch (Exception e) {
      throw new HopXmlException(
          BaseMessages.getString(PKG, "JsonInputMeta.Exception.ErrorLoadingXml", e.toString()));
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

  @Deprecated // ?needs to be public?
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
        getFileName(),
        getFileMask(),
        getExcludeFileMask(),
        getFileRequired(),
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
                CheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(PKG, "JsonInputMeta.CheckResult.NoInputExpected"),
                transformMeta);
        remarks.add(cr);
      } else {
        cr =
            new CheckResult(
                CheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(PKG, "JsonInputMeta.CheckResult.NoInput"),
                transformMeta);
        remarks.add(cr);
      }
    }

    if (getInputFields().length <= 0) {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "JsonInputMeta.CheckResult.NoInputField"),
              transformMeta);
      remarks.add(cr);
    }

    if (isInFields()) {
      if (Utils.isEmpty(getFieldValue())) {
        cr =
            new CheckResult(
                CheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(PKG, "JsonInputMeta.CheckResult.NoField"),
                transformMeta);
        remarks.add(cr);
      } else {
        cr =
            new CheckResult(
                CheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(PKG, "JsonInputMeta.CheckResult.FieldOk"),
                transformMeta);
        remarks.add(cr);
      }
    } else {
      FileInputList fileInputList = getFiles(variables);
      // String files[] = getFiles();
      if (fileInputList == null || fileInputList.getFiles().size() == 0) {
        cr =
            new CheckResult(
                CheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(PKG, "JsonInputMeta.CheckResult.NoFiles"),
                transformMeta);
        remarks.add(cr);
      } else {
        cr =
            new CheckResult(
                CheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(
                    PKG, "JsonInputMeta.CheckResult.FilesOk", "" + fileInputList.getFiles().size()),
                transformMeta);
        remarks.add(cr);
      }
    }
  }

  @Override
  public JsonInput createTransform(
      TransformMeta transformMeta,
      JsonInputData jsonInputData,
      int cnr,
      PipelineMeta tr,
      Pipeline pipeline) {
    return new JsonInput(transformMeta, this, jsonInputData, cnr, tr, pipeline);
  }

  @Override
  public JsonInputData getTransformData() {
    return new JsonInputData();
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
   * @param metadataProvider the metadataProvider in which non-kettle metadata could reside.
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
          setFileName(newFilenames.toArray(new String[newFilenames.size()]));
          setFileMask(
              new String[newFilenames.size()]); // all null since converted to absolute path.
          setFileRequired(new String[newFilenames.size()]); // all null, turn to "Y" :
          Arrays.fill(getFileRequired(), YES);
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
