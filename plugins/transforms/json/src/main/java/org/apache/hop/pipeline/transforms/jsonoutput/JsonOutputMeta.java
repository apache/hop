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

package org.apache.hop.pipeline.transforms.jsonoutput;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

/** This class knows how to handle the MetaData for the Json output transform */
@Transform(
    id = "JsonOutput",
    image = "JSO.svg",
    name = "i18n::JsonOutput.name",
    description = "i18n::JsonOutput.description",
    categoryDescription = "i18n::JsonOutput.category",
    keywords = "i18n::JsonOutputMeta.keyword",
    documentationUrl = "/pipeline/transforms/jsonoutput.html")
public class JsonOutputMeta extends BaseFileOutputMeta<JsonOutput, JsonOutputData> {
  private static final Class<?> PKG = JsonOutputMeta.class;

  /** Operations type */
  @HopMetadataProperty(
      key = "operation_type",
      injectionKeyDescription = "JsonOutput.Injection.OPERATION")
  private String operationType;

  public static final String OPERATION_TYPE_OUTPUT_VALUE = "outputvalue";

  public static final String OPERATION_TYPE_WRITE_TO_FILE = "writetofile";

  public static final String OPERATION_TYPE_BOTH = "both";

  /** The operations description */
  public static final Map<String, String> operationTypeDesc =
      Map.of(
          OPERATION_TYPE_OUTPUT_VALUE,
          BaseMessages.getString(PKG, "JsonOutputMeta.operationType.OutputValue"),
          OPERATION_TYPE_WRITE_TO_FILE,
          BaseMessages.getString(PKG, "JsonOutputMeta.operationType.WriteToFile"),
          "both",
          BaseMessages.getString(PKG, "JsonOutputMeta.operationType.Both"));

  public static final Map<String, String> operationDescType =
      Map.of(
          BaseMessages.getString(PKG, "JsonOutputMeta.operationType.OutputValue"),
              OPERATION_TYPE_OUTPUT_VALUE,
          BaseMessages.getString(PKG, "JsonOutputMeta.operationType.WriteToFile"),
              OPERATION_TYPE_WRITE_TO_FILE,
          BaseMessages.getString(PKG, "JsonOutputMeta.operationType.Both"), "both");

  /** The encoding to use for reading: null or empty string means system default encoding */
  @HopMetadataProperty(injectionKeyDescription = "JsonOutput.Injection.ENCODING")
  private String encoding;

  /** The name value containing the resulting Json fragment */
  @HopMetadataProperty(injectionKeyDescription = "JsonOutput.Injection.OUTPUT_VALUE")
  private String outputValue;

  /** The name of the json bloc */
  @HopMetadataProperty(injectionKeyDescription = "JsonOutput.Injection.JSON_BLOC_NAME")
  private String jsonBloc;

  @HopMetadataProperty(injectionKeyDescription = "JsonOutput.Injection.NR_ROWS_IN_BLOC")
  private String nrRowsInBloc;

  /* THE FIELD SPECIFICATIONS ... */

  /** The output fields */
  @HopMetadataProperty(
      groupKey = "fields",
      key = "field",
      injectionKey = "FIELD",
      injectionGroupKey = "FIELDS",
      injectionKeyDescription = "JsonOutput.Injection.FIELD",
      injectionGroupDescription = "JsonOutput.Injection.FIELDS")
  private List<JsonOutputField> outputFields;

  @HopMetadataProperty(injectionKeyDescription = "JsonOutput.Injection.ADD_TO_RESULT")
  private boolean addToResult;

  /** Flag to indicate the we want to append to the end of an existing file (if it exists) */
  @HopMetadataProperty(injectionKeyDescription = "JsonOutput.Injection.APPEND")
  private boolean fileAppended;

  /** Flag: create parent folder if needed */
  @HopMetadataProperty(injectionKeyDescription = "JsonOutput.Injection.CREATE_PARENT_FOLDER")
  private boolean createParentFolder;

  @HopMetadataProperty(injectionKeyDescription = "JsonOutput.Injection.DONT_CREATE_AT_START")
  private boolean doNotOpenNewFileInit;

  public JsonOutputMeta() {
    super(); // allocate BaseTransformMeta

    outputFields = new ArrayList<>();
  }

  public boolean isDoNotOpenNewFileInit() {
    return doNotOpenNewFileInit;
  }

  public void setDoNotOpenNewFileInit(boolean doNotOpenNewFileInit) {
    this.doNotOpenNewFileInit = doNotOpenNewFileInit;
  }

  /**
   * @return Returns the create parent folder flag.
   */
  public boolean isCreateParentFolder() {
    return createParentFolder;
  }

  /**
   * @param createparentfolder The create parent folder flag to set.
   */
  public void setCreateParentFolder(boolean createparentfolder) {
    this.createParentFolder = createparentfolder;
  }

  /**
   * @return Returns the fileAppended.
   */
  public boolean isFileAppended() {
    return fileAppended;
  }

  /**
   * @param fileAppended The fileAppended to set.
   */
  public void setFileAppended(boolean fileAppended) {
    this.fileAppended = fileAppended;
  }

  /**
   * @param dateInFilename The dateInFilename to set.
   */
  public void setDateInFilename(boolean dateInFilename) {
    this.dateInFilename = dateInFilename;
  }

  /**
   * @param timeInFilename The timeInFilename to set.
   */
  public void setTimeInFilename(boolean timeInFilename) {
    this.timeInFilename = timeInFilename;
  }

  /**
   * @return Returns the Add to result filesname flag.
   */
  public boolean isAddToResult() {
    return addToResult;
  }

  public String getOperationType() {
    return operationType;
  }

  public void setOperationType(String operationType) {
    this.operationType = operationType;
  }

  /**
   * @return Returns the outputFields.
   */
  public List<JsonOutputField> getOutputFields() {
    return outputFields;
  }

  /**
   * @param outputFields The outputFields to set.
   */
  public void setOutputFields(List<JsonOutputField> outputFields) {
    this.outputFields = outputFields;
  }

  @Override
  public Object clone() {
    return super.clone();
  }

  /**
   * @param addToResult The Add file to result to set.
   */
  public void setAddToResult(boolean addToResult) {
    this.addToResult = addToResult;
  }

  @Override
  public void setDefault() {
    encoding = Const.XML_ENCODING;
    outputValue = "outputValue";
    jsonBloc = "data";
    nrRowsInBloc = "0";
    operationType = OPERATION_TYPE_WRITE_TO_FILE;
    doNotOpenNewFileInit = true;
    int nrFields = 0;

    for (int i = 0; i < nrFields; i++) {
      JsonOutputField outputField = new JsonOutputField();
      outputField.setFieldName("field" + i);
      outputField.setElementName("field" + i);
      outputFields.add(outputField);
    }
  }

  @Override
  public void getFields(
      IRowMeta row,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {

    if (!Objects.equals(getOperationType(), OPERATION_TYPE_WRITE_TO_FILE)) {
      IValueMeta v = new ValueMetaString(variables.resolve(this.getOutputValue()));
      v.setOrigin(name);
      row.addValueMeta(v);
    }
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
    if (!Objects.equals(getOperationType(), JsonOutputMeta.OPERATION_TYPE_WRITE_TO_FILE)
        && Utils.isEmpty(variables.resolve(getOutputValue()))) {
      // We need to have output field name
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "JsonOutput.Error.MissingOutputFieldName"),
              transformMeta);
      remarks.add(cr);
    }
    if (Utils.isEmpty(variables.resolve(getFileName()))) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "JsonOutput.Error.MissingTargetFilename"),
              transformMeta);
      remarks.add(cr);
    }
    // Check output fields
    if (prev != null && !prev.isEmpty()) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "JsonOutputMeta.CheckResult.FieldsReceived", "" + prev.size()),
              transformMeta);
      remarks.add(cr);

      String errorMessage = "";
      boolean errorFound = false;

      // Starting from selected fields in ...
      for (int i = 0; i < outputFields.size(); i++) {
        int idx = prev.indexOfValue(outputFields.get(i).getFieldName());
        if (idx < 0) {
          errorMessage += "\t\t" + outputFields.get(i).getFieldName() + Const.CR;
          errorFound = true;
        }
      }
      if (errorFound) {
        errorMessage =
            BaseMessages.getString(PKG, "JsonOutputMeta.CheckResult.FieldsNotFound", errorMessage);
        cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
        remarks.add(cr);
      } else {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(PKG, "JsonOutputMeta.CheckResult.AllFieldsFound"),
                transformMeta);
        remarks.add(cr);
      }
    }

    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "JsonOutputMeta.CheckResult.ExpectedInputOk"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "JsonOutputMeta.CheckResult.ExpectedInputError"),
              transformMeta);
      remarks.add(cr);
    }

    cr =
        new CheckResult(
            ICheckResult.TYPE_RESULT_COMMENT,
            BaseMessages.getString(PKG, "JsonOutputMeta.CheckResult.FilesNotChecked"),
            transformMeta);
    remarks.add(cr);
  }

  public String getEncoding() {
    return encoding;
  }

  public void setEncoding(String encoding) {
    this.encoding = encoding;
  }

  /**
   * @return Returns the jsonBloc.
   */
  public String getJsonBloc() {
    return jsonBloc;
  }

  /**
   * @param jsonBloc The root node to set.
   */
  public void setJsonBloc(String jsonBloc) {
    this.jsonBloc = jsonBloc;
  }

  /**
   * @return Returns the jsonBloc.
   */
  public String getNrRowsInBloc() {
    return nrRowsInBloc;
  }

  /**
   * @param nrRowsInBloc The nrRowsInBloc.
   */
  public void setNrRowsInBloc(String nrRowsInBloc) {
    this.nrRowsInBloc = nrRowsInBloc;
  }

  @Override
  public int getSplitEvery() {
    try {
      return Integer.parseInt(getNrRowsInBloc());
    } catch (final Exception e) {
      return 1;
    }
  }

  @Override
  public void setSplitEvery(int splitEvery) {
    setNrRowsInBloc(splitEvery + "");
  }

  public String getOutputValue() {
    return outputValue;
  }

  public void setOutputValue(String outputValue) {
    this.outputValue = outputValue;
  }
}
