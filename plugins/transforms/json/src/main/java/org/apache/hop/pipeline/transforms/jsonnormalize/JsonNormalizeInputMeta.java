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

package org.apache.hop.pipeline.transforms.jsonnormalize;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
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
import org.apache.hop.pipeline.transforms.jsoninput.JsonInputField;
import org.apache.hop.resource.IResourceNaming;
import org.apache.hop.resource.ResourceDefinition;
import org.w3c.dom.Node;

@Transform(
    id = "JsonNormalizeInput",
    image = "json-normalize-input.svg",
    documentationUrl = "/pipeline/transforms/jsonnormalizeinput.html",
    name = "i18n::JsonNormalizeInput.name",
    description = "i18n::JsonNormalizeInput.description",
    keywords = "i18n::JsonNormalizeInputMeta.keyword",
    categoryDescription = "i18n::JsonInput.category")
@Getter
@Setter
public class JsonNormalizeInputMeta
    extends BaseFileInputMeta<JsonNormalizeInput, JsonNormalizeInputData, BaseFileInput> {
  private static final Class<?> PKG = JsonNormalizeInputMeta.class;

  protected static final String[] RequiredFilesDesc =
      new String[] {
        BaseMessages.getString(PKG, "System.Combo.No"),
        BaseMessages.getString(PKG, "System.Combo.Yes")
      };

  @HopMetadataProperty(
      key = "include",
      injectionKey = "FILE_NAME_OUTPUT",
      injectionKeyDescription = "JsonNormalizeInput.Injection.FILE_NAME_OUTPUT")
  private boolean includeFilename;

  @HopMetadataProperty(
      key = "include_field",
      injectionKey = "FILE_NAME_FIELDNAME",
      injectionKeyDescription = "JsonNormalizeInput.Injection.FILE_NAME_FIELDNAME")
  private String filenameField;

  @HopMetadataProperty(
      key = "rownum",
      injectionKey = "ROW_NUMBER_OUTPUT",
      injectionKeyDescription = "JsonNormalizeInput.Injection.ROW_NUMBER_OUTPUT")
  private boolean includeRowNumber;

  @HopMetadataProperty(
      key = "rownum_field",
      injectionKey = "ROW_NUMBER_FIELDNAME",
      injectionKeyDescription = "JsonNormalizeInput.Injection.ROW_NUMBER_FIELDNAME")
  private String rowNumberField;

  @HopMetadataProperty(
      key = "limit",
      injectionKey = "ROW_LIMIT",
      injectionKeyDescription = "JsonNormalizeInput.Injection.ROW_LIMIT")
  private long rowLimit;

  @HopMetadataProperty(
      key = "valueField",
      injectionKey = "SOURCE_FIELD_NAME",
      injectionKeyDescription = "JsonNormalizeInput.Injection.SOURCE_FIELD_NAME")
  private String valueField;

  @HopMetadataProperty(
      key = "IsInFields",
      injectionKey = "SOURCE_IN_FIELD",
      injectionKeyDescription = "JsonNormalizeInput.Injection.SOURCE_IN_FIELD")
  private boolean inFields;

  @HopMetadataProperty(
      key = "IsAFile",
      injectionKey = "SOURCE_FIELD_IS_FILENAME",
      injectionKeyDescription = "JsonNormalizeInput.Injection.SOURCE_FIELD_IS_FILENAME")
  private boolean sourceAFile;

  @HopMetadataProperty(
      key = "addresultfile",
      injectionKey = "ADD_RESULT_FILE",
      injectionKeyDescription = "JsonNormalizeInput.Injection.ADD_RESULT_FILE")
  private boolean addResultFile;

  @HopMetadataProperty(
      key = "IsIgnoreEmptyFile",
      injectionKey = "IGNORE_EMPTY_FILE",
      injectionKeyDescription = "JsonNormalizeInput.Injection.IGNORE_EMPTY_FILE")
  private boolean ignoringEmptyFile;

  @HopMetadataProperty(
      key = "doNotFailIfNoFile",
      injectionKey = "DO_NOT_FAIL_IF_NO_FILE",
      injectionKeyDescription = "JsonNormalizeInput.Injection.DO_NOT_FAIL_IF_NO_FILE")
  private boolean doNotFailIfNoFile;

  @HopMetadataProperty(
      key = "readurl",
      injectionKey = "READ_SOURCE_AS_URL",
      injectionKeyDescription = "JsonNormalizeInput.Injection.READ_SOURCE_AS_URL")
  private boolean readUrl;

  @HopMetadataProperty(
      key = "removeSourceField",
      injectionKey = "REMOVE_SOURCE_FIELDS",
      injectionKeyDescription = "JsonNormalizeInput.Injection.REMOVE_SOURCE_FIELDS")
  private boolean removeSourceField;

  @HopMetadataProperty(
      key = "recordPath",
      injectionKey = "RECORD_PATH",
      injectionKeyDescription = "JsonNormalizeInput.Injection.RECORD_PATH")
  private String recordPath;

  /** Join segments when flattening (pandas json_normalize {@code sep}). */
  @HopMetadataProperty(
      key = "fieldSeparator",
      injectionKey = "FIELD_SEPARATOR",
      injectionKeyDescription = "JsonNormalizeInput.Injection.FIELD_SEPARATOR")
  private String fieldSeparator;

  /**
   * Maximum nesting depth for flattening objects. {@code -1} means unlimited (pandas {@code
   * max_level}=None).
   */
  @HopMetadataProperty(
      key = "maxFlattenDepth",
      injectionKey = "MAX_FLATTEN_DEPTH",
      injectionKeyDescription = "JsonNormalizeInput.Injection.MAX_FLATTEN_DEPTH")
  private int maxFlattenDepth;

  /** STRINGIFY | SINGLE_ELEMENT | ERROR — how to handle JSON arrays in a record. */
  @HopMetadataProperty(
      key = "arrayHandling",
      injectionKey = "ARRAY_HANDLING",
      injectionKeyDescription = "JsonNormalizeInput.Injection.ARRAY_HANDLING")
  private String arrayHandling;

  /** STRINGIFY | OMIT | ERROR — nested objects when max depth is reached. */
  @HopMetadataProperty(
      key = "beyondDepthBehavior",
      injectionKey = "BEYOND_DEPTH_BEHAVIOR",
      injectionKeyDescription = "JsonNormalizeInput.Injection.BEYOND_DEPTH_BEHAVIOR")
  private String beyondDepthBehavior;

  @HopMetadataProperty(
      key = "ignoreMissingField",
      defaultBoolean = true,
      injectionKey = "IGNORE_MISSING_FIELD",
      injectionKeyDescription = "JsonNormalizeInput.Injection.IGNORE_MISSING_FIELD")
  private boolean ignoreMissingField;

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
      injectionGroupDescription = "JsonNormalizeInput.Injection.FIELDS")
  private List<JsonInputField> inputFields;

  public JsonNormalizeInputMeta() {
    super();
    inputFields = new ArrayList<>();
    additionalOutputFields = new BaseFileInputAdditionalFields();
    fileInput = new BaseFileInput();
    ignoringEmptyFile = false;
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
    recordPath = "$";
    fieldSeparator = ".";
    maxFlattenDepth = -1;
    arrayHandling = JsonNodeFlattener.CODE_ARRAY_STRINGIFY;
    beyondDepthBehavior = JsonNodeFlattener.CODE_BEYOND_STRINGIFY;
    ignoreMissingField = true;
  }

  public JsonNormalizeInputMeta(JsonNormalizeInputMeta m) {
    this();
    this.addResultFile = m.addResultFile;
    this.doNotFailIfNoFile = m.doNotFailIfNoFile;
    this.filenameField = m.filenameField;
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
    this.recordPath = m.recordPath;
    this.fieldSeparator = m.fieldSeparator;
    this.maxFlattenDepth = m.maxFlattenDepth;
    this.arrayHandling = m.arrayHandling;
    this.beyondDepthBehavior = m.beyondDepthBehavior;
    this.ignoreMissingField = m.ignoreMissingField;
    this.additionalOutputFields = new BaseFileInputAdditionalFields(m.additionalOutputFields);
    this.fileInput = new BaseFileInput(m.fileInput);
    m.inputFields.forEach(f -> inputFields.add(new JsonInputField(f)));
  }

  @Override
  public JsonNormalizeInputMeta clone() {
    return new JsonNormalizeInputMeta(this);
  }

  public String getShortFileNameField() {
    return additionalOutputFields.getShortFilenameField();
  }

  public void setShortFileNameField(String field) {
    additionalOutputFields.setShortFilenameField(field);
  }

  public String getPathField() {
    return additionalOutputFields.getPathField();
  }

  public void setPathField(String field) {
    additionalOutputFields.setPathField(field);
  }

  public String isHiddenField() {
    return additionalOutputFields.getHiddenField();
  }

  public void setIsHiddenField(String field) {
    additionalOutputFields.setHiddenField(field);
  }

  public String getLastModificationDateField() {
    return additionalOutputFields.getLastModificationField();
  }

  public void setLastModificationDateField(String field) {
    additionalOutputFields.setLastModificationField(field);
  }

  public String getUriField() {
    return additionalOutputFields.getUriField();
  }

  public void setUriField(String field) {
    additionalOutputFields.setUriField(field);
  }

  public String getRootUriField() {
    return additionalOutputFields.getRootUriField();
  }

  public void setRootUriField(String field) {
    additionalOutputFields.setRootUriField(field);
  }

  public String getExtensionField() {
    return additionalOutputFields.getExtensionField();
  }

  public void setExtensionField(String field) {
    additionalOutputFields.setExtensionField(field);
  }

  public String getSizeField() {
    return additionalOutputFields.getSizeField();
  }

  public void setSizeField(String field) {
    additionalOutputFields.setSizeField(field);
  }

  public boolean addResultFile() {
    return addResultFile;
  }

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

  public boolean includeRowNumber() {
    return includeRowNumber;
  }

  public boolean getIsAFile() {
    return sourceAFile;
  }

  public void setIsAFile(boolean isAFile) {
    this.sourceAFile = isAFile;
  }

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
              BaseMessages.getString(PKG, "JsonNormalizeInputMeta.CheckResult.NoInputField"),
              transformMeta);
      remarks.add(cr);
    }

    if (Utils.isEmpty(getRecordPath())) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "JsonNormalizeInputMeta.CheckResult.NoRecordPath"),
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

  @Override
  public String exportResources(
      IVariables variables,
      Map<String, ResourceDefinition> definitions,
      IResourceNaming resourceNamingInterface,
      IHopMetadataProvider metadataProvider)
      throws HopException {
    try {
      List<String> newFilenames = new ArrayList<>();

      if (!isInFields()) {
        FileInputList fileList = getFiles(variables);
        if (!fileList.getFiles().isEmpty()) {
          for (FileObject fileObject : fileList.getFiles()) {
            if (fileObject.exists()) {
              newFilenames.add(fileObject.getName().getPath());
            }
          }

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
    return Const.UTF_8;
  }
}
