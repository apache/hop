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

package org.apache.hop.pipeline.transforms.jsonoutputenhanced;

import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.injection.Injection;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IEnumHasCodeAndDescription;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.w3c.dom.Node;

@Transform(
    id = "EnhancedJsonOutput",
    image = "json-output.svg",
    name = "i18n::EnhancedJsonOutput.name",
    description = "i18n::EnhancedJsonOutput.description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Output",
    keywords = "i18n::JsonEOutputMeta.keyword",
    documentationUrl = "/pipeline/transforms/enhancedjsonoutput.html")
@Getter
@Setter
public class JsonEOutputMeta extends BaseTransformMeta<JsonEOutput, JsonEOutputData> {
  private static final Class<?> PKG = JsonEOutputMeta.class;
  public static final String CONST_SPACES_LONG = "        ";
  public static final String CONST_SPACES = "      ";
  public static final String CONST_OUTPUT_VALUE = "outputValue";
  public static final String CONST_KEY_FIELD = "key_field";
  public static final String CONST_FIELD = "field";

  @Getter
  public enum OperationType implements IEnumHasCodeAndDescription {
    OUTPUT_VALUE(
        "outputvalue", BaseMessages.getString(PKG, "JsonEOutputMeta.operationType.OutputValue")),
    WRITE_TO_FILE(
        "writetofile", BaseMessages.getString(PKG, "JsonEOutputMeta.operationType.WriteToFile")),
    BOTH("both", BaseMessages.getString(PKG, "JsonEOutputMeta.operationType.Both")),
    ;

    private final String code;
    private final String description;

    OperationType(String code, String description) {
      this.code = code;
      this.description = description;
    }

    public static String[] getDescriptions() {
      return IEnumHasCodeAndDescription.getDescriptions(OperationType.class);
    }

    public static OperationType lookupDescription(String description) {
      return IEnumHasCodeAndDescription.lookupDescription(
          OperationType.class, description, WRITE_TO_FILE);
    }
  }

  /** Operations type */
  @Injection(name = "", group = "GENERAL")
  @HopMetadataProperty(
      key = "operation_type",
      storeWithCode = true,
      injectionKey = "OPERATION",
      injectionKeyDescription = "JsonEOutput.Injection.OPERATION")
  private OperationType operationType;

  /** The encoding to use for reading: null or empty string means system default encoding */
  @HopMetadataProperty(
      key = "encoding",
      injectionKey = "ENCODING",
      injectionKeyDescription = "JsonEOutput.Injection.ENCODING")
  private String encoding;

  /** The name value containing the resulting JSON fragment */
  @HopMetadataProperty(
      key = "outputValue",
      injectionKey = "OUTPUT_VALUE",
      injectionKeyDescription = "JsonEOutput.Injection.OUTPUT_VALUE")
  private String outputValue;

  /** The name of the JSON bloc */
  @HopMetadataProperty(
      key = "jsonBloc",
      injectionKey = "JSON_BLOC_NAME",
      injectionKeyDescription = "JsonEOutput.Injection.JSON_BLOC_NAME")
  private String jsonBloc;

  /** Choose if you want the output prettyfied */
  @HopMetadataProperty(
      key = "json_prittified",
      injectionKey = "PRITTIFY",
      injectionKeyDescription = "JsonEOutput.Injection.PRITTIFY")
  private boolean jsonPrettified;

  @HopMetadataProperty(
      key = "addToResult",
      injectionKey = "ADD_TO_RESULT",
      injectionKeyDescription = "JsonEOutput.Injection.ADD_TO_RESULT")
  private boolean addingToResult;

  /** Flag to indicate to force unmarshall to JSON Arrays even with a single occurrence in a list */
  @HopMetadataProperty(
      key = "use_arrays_with_single_instance",
      injectionKey = "FORCE_JSON_ARRAYS",
      injectionKeyDescription = "JsonEOutput.Injection.FORCE_JSON_ARRAYS")
  private boolean useArrayWithSingleInstance;

  /** Flag to indicate to force unmarshall to JSON Arrays even with a single occurrence in a list */
  @HopMetadataProperty(
      key = "use_single_item_per_group",
      injectionKey = "FORCE_SINGLE_ITEM",
      injectionKeyDescription = "JsonEOutput.Injection.FORCE_SINGLE_ITEM")
  private boolean useSingleItemPerGroup;

  @HopMetadataProperty(
      key = "json_size_field",
      injectionKey = "JSON_SIZE_FIELD",
      injectionKeyDescription = "JsonEOutput.Injection.JSON_SIZE_FIELD")
  private String jsonSizeFieldName;

  @HopMetadataProperty(key = "file")
  private FileSettings fileSettings;

  /** The output fields */
  @HopMetadataProperty(
      key = "field",
      groupKey = "fields",
      injectionKey = "FIELD",
      injectionKeyDescription = "JsonEOutput.Injection.FIELD",
      injectionGroupKey = "FIELDS",
      injectionGroupDescription = "JsonEOutput.Injection.FIELDS")
  private List<JsonEOutputField> outputFields;

  /** The key fields */
  @HopMetadataProperty(
      key = "key_field",
      groupKey = "key_fields",
      injectionGroupKey = "KEY_FIELDS",
      injectionGroupDescription = "JsonEOutput.Injection.KEY_FIELDS")
  private List<JsonEOutputKeyField> keyFields;

  public JsonEOutputMeta() {
    super();
    this.fileSettings = new FileSettings();
    this.outputFields = new ArrayList<>();
    this.keyFields = new ArrayList<>();
    operationType = OperationType.WRITE_TO_FILE;
    encoding = Const.XML_ENCODING;
    outputValue = CONST_OUTPUT_VALUE;
    jsonBloc = "result";
  }

  public JsonEOutputMeta(JsonEOutputMeta m) {
    this();
    this.addingToResult = m.addingToResult;
    this.encoding = m.encoding;
    this.jsonBloc = m.jsonBloc;
    this.jsonPrettified = m.jsonPrettified;
    this.jsonSizeFieldName = m.jsonSizeFieldName;
    this.operationType = m.operationType;
    this.outputValue = m.outputValue;
    this.useArrayWithSingleInstance = m.useArrayWithSingleInstance;
    this.useSingleItemPerGroup = m.useSingleItemPerGroup;
    this.fileSettings = new FileSettings(m.fileSettings);
    m.keyFields.forEach(f -> this.keyFields.add(new JsonEOutputKeyField(f)));
    m.outputFields.forEach(f -> this.outputFields.add(new JsonEOutputField(f)));
  }

  @Override
  public Object clone() {
    return new JsonEOutputMeta(this);
  }

  @Override
  public void getFields(
      IRowMeta row,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {

    if (getOperationType() != OperationType.WRITE_TO_FILE) {
      IRowMeta rowMeta = row.clone();
      row.clear();

      for (int i = 0; i < this.getKeyFields().size(); i++) {
        JsonEOutputKeyField keyField = this.getKeyFields().get(i);
        IValueMeta vmi = rowMeta.getValueMeta(rowMeta.indexOfValue(keyField.getFieldName()));
        row.addValueMeta(i, vmi);
      }

      ValueMetaString vm = new ValueMetaString(this.getOutputValue());
      row.addValueMeta(this.getKeyFields().size(), vm);

      int fieldLength = this.getKeyFields().size() + 1;
      if (!Utils.isEmpty(this.jsonSizeFieldName)) {
        row.addValueMeta(fieldLength, new ValueMetaInteger(this.jsonSizeFieldName));
      }
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
    if (getOperationType() != OperationType.WRITE_TO_FILE
        && Utils.isEmpty(variables.resolve(getOutputValue()))) {
      // We need to have output field name
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "JsonEOutput.Error.MissingOutputFieldName"),
              transformMeta);
      remarks.add(cr);
    }
    if (Utils.isEmpty(variables.resolve(getFileSettings().getFileName()))) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "JsonEOutput.Error.MissingTargetFilename"),
              transformMeta);
      remarks.add(cr);
    }
    // Check output fields
    if (prev != null && !prev.isEmpty()) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "JsonEOutputMeta.CheckResult.FieldsReceived", "" + prev.size()),
              transformMeta);
      remarks.add(cr);

      String errorMessage = "";
      boolean errorFound = false;

      // Starting from selected fields in ...
      for (JsonEOutputField outputField : outputFields) {
        int idx = prev.indexOfValue(outputField.getFieldName());
        if (idx < 0) {
          errorMessage += "\t\t" + outputField.getFieldName() + Const.CR;
          errorFound = true;
        }
      }
      if (errorFound) {
        errorMessage =
            BaseMessages.getString(PKG, "JsonEOutputMeta.CheckResult.FieldsNotFound", errorMessage);
        cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
        remarks.add(cr);
      } else {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(PKG, "JsonEOutputMeta.CheckResult.AllFieldsFound"),
                transformMeta);
        remarks.add(cr);
      }
    }

    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "JsonEOutputMeta.CheckResult.ExpectedInputOk"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "JsonEOutputMeta.CheckResult.ExpectedInputError"),
              transformMeta);
      remarks.add(cr);
    }

    cr =
        new CheckResult(
            ICheckResult.TYPE_RESULT_COMMENT,
            BaseMessages.getString(PKG, "JsonEOutputMeta.CheckResult.FilesNotChecked"),
            transformMeta);
    remarks.add(cr);
  }

  @Override
  public void convertLegacyXml(Node node) throws HopException {
    // See if there's an older "file/extention" node...
    //
    Node extentionNode = XmlHandler.getSubNode(node, "file", "extention");
    if (extentionNode != null) {
      fileSettings.setExtension(XmlHandler.getNodeValue(extentionNode));
    }
    // Flattened
    Node jsonSizeFieldNode = XmlHandler.getSubNode(node, "additional_fields", "json_size_field");
    if (jsonSizeFieldNode != null) {
      jsonSizeFieldName = XmlHandler.getNodeValue(jsonSizeFieldNode);
    }
  }
}
