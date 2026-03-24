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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.fileinput.FileInputList;
import org.apache.hop.core.fileinput.InputFile;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.file.BaseFileInput;
import org.apache.hop.pipeline.transforms.file.BaseFileInputAdditionalFields;
import org.apache.hop.pipeline.transforms.file.BaseFileInputMeta;
import org.apache.hop.resource.IResourceNaming;
import org.apache.hop.resource.ResourceDefinition;
import org.w3c.dom.Node;

/** Store run-time data on the JsonInput transform. */
@Transform(
    id = "JsonInput",
    image = "json-input.svg",
    documentationUrl = "/pipeline/transforms/jsoninput.html",
    name = "i18n::JsonInput.name",
    description = "i18n::JsonInput.description",
    keywords = "i18n::JsonInputMeta.keyword",
    categoryDescription = "i18n::JsonInput.category")
@Getter
@Setter
public class JsonInputMeta extends BaseFileInputMeta<JsonInput, JsonInputData, BaseFileInput> {
  private static final Class<?> PKG = JsonInputMeta.class;

  protected static final String[] RequiredFilesDesc =
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
  private boolean includeFilename;

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
  /** The maximum number of lines to read */
  @HopMetadataProperty(
      key = "limit",
      injectionKey = "ROW_LIMIT",
      injectionKeyDescription = "JsonInput.Injection.ROW_LIMIT")
  private long rowLimit;

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
  private boolean sourceAFile;

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
  private boolean ignoringEmptyFile;

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
  private boolean ignoringMissingPath;

  /** Flag : read url as source */
  @HopMetadataProperty(
      key = "readurl",
      injectionKey = "READ_SOURCE_AS_URL",
      injectionKeyDescription = "JsonInput.Injection.READ_SOURCE_AS_URL")
  private boolean readUrl;

  @HopMetadataProperty(
      key = "removeSourceField",
      injectionKey = "REMOVE_SOURCE_FIELDS",
      injectionKeyDescription = "JsonInput.Injection.REMOVE_SOURCE_FIELDS")
  private boolean removeSourceField;

  @HopMetadataProperty(
      key = "defaultPathLeafToNull",
      defaultBoolean = true,
      injectionKey = "DEFAULT_PATH_LEAF_TO_NULL",
      injectionKeyDescription = "JsonInput.Injection.DEFAULT_PATH_LEAF_TO_NULL")
  private boolean defaultPathLeafToNull;

  @HopMetadataProperty(inline = true)
  protected BaseFileInputAdditionalFields additionalOutputFields;

  @HopMetadataProperty(
      key = "file",
      inline = true,
      childKeysToIgnore = {
        "accept_filenames",
        "accept_transform_name",
        "passing_through_fields",
        "accept_field",
        "add_to_result_filenames",
      })
  protected BaseFileInput fileInput;

  @HopMetadataProperty(
      key = "field",
      groupKey = "fields",
      injectionGroupKey = "FIELDS",
      injectionGroupDescription = "JsonInput.Injection.FIELDS")
  private List<JsonInputField> inputFields;

  public JsonInputMeta() {
    super();
    inputFields = new ArrayList<>();
    additionalOutputFields = new BaseFileInputAdditionalFields();
    fileInput = new BaseFileInput();
    ignoringEmptyFile = false;
    ignoringMissingPath = true;
    defaultPathLeafToNull = true;
    doNotFailIfNoFile = true;
    includeFilename = false;
    filenameField = "";
    includeRowNumber = false;
    rowNumberField = "";
    sourceAFile = false;
    addResultFile = false;
    readUrl = false;
    removeSourceField = false;
    rowLimit = 0;
    inFields = false;
    valueField = "";
  }

  public JsonInputMeta(JsonInputMeta m) {
    this();
    this.addResultFile = m.addResultFile;
    this.defaultPathLeafToNull = m.defaultPathLeafToNull;
    this.doNotFailIfNoFile = m.doNotFailIfNoFile;
    this.filenameField = m.filenameField;
    this.ignoringMissingPath = m.ignoringMissingPath;
    this.includeFilename = m.includeFilename;
    this.includeRowNumber = m.includeRowNumber;
    this.inFields = m.inFields;
    this.sourceAFile = m.sourceAFile;
    this.ignoringEmptyFile = m.ignoringEmptyFile;
    this.readUrl = m.readUrl;
    this.removeSourceField = m.removeSourceField;
    this.rowLimit = m.rowLimit;
    this.rowNumberField = m.rowNumberField;
    this.valueField = m.valueField;
    this.additionalOutputFields = new BaseFileInputAdditionalFields(m.additionalOutputFields);
    this.fileInput = new BaseFileInput(m.fileInput);
    m.inputFields.forEach(f -> inputFields.add(new JsonInputField(f)));
  }

  @Override
  public JsonInputMeta clone() {
    return new JsonInputMeta(this);
  }

  /**
   * @return Returns the shortFileFieldName.
   */
  public String getShortFileNameField() {
    return additionalOutputFields.getShortFilenameField();
  }

  /**
   * @param field The shortFileFieldName to set.
   */
  public void setShortFileNameField(String field) {
    additionalOutputFields.setShortFilenameField(field);
  }

  /**
   * @return Returns the pathFieldName.
   */
  public String getPathField() {
    return additionalOutputFields.getPathField();
  }

  /**
   * @param field The pathFieldName to set.
   */
  public void setPathField(String field) {
    additionalOutputFields.setPathField(field);
  }

  /**
   * @return Returns the hiddenFieldName.
   */
  public String isHiddenField() { // name..
    return additionalOutputFields.getHiddenField();
  }

  /**
   * @param field The hiddenFieldName to set.
   */
  public void setIsHiddenField(String field) { // name..
    additionalOutputFields.setHiddenField(field);
  }

  /**
   * @return Returns the lastModificationTimeFieldName.
   */
  public String getLastModificationDateField() {
    return additionalOutputFields.getLastModificationField();
  }

  /**
   * @param field The lastModificationTimeFieldName to set.
   */
  public void setLastModificationDateField(String field) {
    additionalOutputFields.setLastModificationField(field);
  }

  /**
   * @return Returns the uriNameFieldName.
   */
  public String getUriField() {
    return additionalOutputFields.getUriField();
  }

  /**
   * @param field The uriNameFieldName to set.
   */
  public void setUriField(String field) {
    additionalOutputFields.setUriField(field);
  }

  /**
   * @return Returns the uriNameFieldName.
   */
  public String getRootUriField() {
    return additionalOutputFields.getRootUriField();
  }

  /**
   * @param field The rootUriNameFieldName to set.
   */
  public void setRootUriField(String field) {
    additionalOutputFields.setRootUriField(field);
  }

  /**
   * @return Returns the extensionFieldName.
   */
  public String getExtensionField() {
    return additionalOutputFields.getExtensionField();
  }

  /**
   * @param field The extensionFieldName to set.
   */
  public void setExtensionField(String field) {
    additionalOutputFields.setExtensionField(field);
  }

  /**
   * @return Returns the sizeFieldName.
   */
  public String getSizeField() {
    return additionalOutputFields.getSizeField();
  }

  /**
   * @param field The sizeFieldName to set.
   */
  public void setSizeField(String field) {
    additionalOutputFields.setSizeField(field);
  }

  /**
   * @return the add result filesname flag
   */
  public boolean addResultFile() {
    return addResultFile;
  }

  /** Get field value. */
  public String getFieldValue() {
    return valueField;
  }

  public void setFieldValue(String value) {
    this.valueField = value;
    fileInput.setAcceptingField(value);
  }

  public void setInFields(boolean inFields) {
    this.inFields = inFields;
    fileInput.setAcceptingFilenames(inFields);
  }

  public boolean includeFilename() {
    return includeFilename;
  }

  /**
   * @return Returns the includeRowNumber.
   */
  public boolean includeRowNumber() {
    return includeRowNumber;
  }

  public boolean getIsAFile() {
    return sourceAFile;
  }

  public void setIsAFile(boolean isAFile) {
    this.sourceAFile = isAFile;
  }

  /** Convert inline file block contents from old XML */
  @Override
  public void convertLegacyXml(Node node) {
    convertLegacyXml(getFileInput().getInputFiles(), node);
  }

  public String getRequiredFilesDesc(String tt) {
    if (Utils.isEmpty(tt)) {
      return RequiredFilesDesc[0];
    }
    if (tt.equalsIgnoreCase(REQUIRED_FILES_CODE[1])) {
      return RequiredFilesDesc[1];
    } else {
      return RequiredFilesDesc[0];
    }
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
    additionalOutputFields.getFields(rowMeta, name, variables);
  }

  public FileInputList getFiles(IVariables variables) {
    return FileInputList.createFileList(variables, getFileInput().getInputFiles());
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

    if (getInputFields().isEmpty()) {
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
      if (fileInputList == null || fileInputList.getFiles().isEmpty()) {
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
   * @param definitions The definitions to
   * @param resourceNamingInterface The resource naming interface
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
        if (!fileList.getFiles().isEmpty()) {
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
          getFileInput().getInputFiles().clear();
          for (String newFilename : newFilenames) {
            InputFile inputFile = new InputFile();
            inputFile.setFileName(newFilename);
            inputFile.setFileRequired(true);
            getFileInput().getInputFiles().add(inputFile);
          }
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
