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

import java.util.List;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.injection.Injection;
import org.apache.hop.core.injection.InjectionDeep;
import org.apache.hop.core.injection.InjectionSupported;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.w3c.dom.Node;

@Transform(
    id = "EnhancedJsonOutput",
    image = "JSO.svg",
    name = "i18n::EnhancedJsonOutput.name",
    description = "i18n::EnhancedJsonOutput.description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Output",
    keywords = "i18n::JsonOutputMeta.keyword",
    documentationUrl = "/pipeline/transforms/enhancedjsonoutput.html")
@InjectionSupported(
    localizationPrefix = "JsonOutput.Injection.",
    groups = {"GENERAL", "FIELDS"})
public class JsonOutputMeta extends BaseFileOutputMeta<JsonOutput, JsonOutputData> {
  private static final Class<?> PKG = JsonOutputMeta.class;
  public static final String CONST_SPACES_LONG = "        ";
  public static final String CONST_SPACES = "      ";
  public static final String CONST_OUTPUT_VALUE = "outputValue";
  public static final String CONST_KEY_FIELD = "key_field";
  public static final String CONST_FIELD = "field";

  /** Operations type */
  @Injection(name = "OPERATION", group = "GENERAL")
  private int operationType;

  /** The operations description */
  public static final String[] operationTypeDesc = {
    BaseMessages.getString(PKG, "JsonOutputMeta.operationType.OutputValue"),
    BaseMessages.getString(PKG, "JsonOutputMeta.operationType.WriteToFile"),
    BaseMessages.getString(PKG, "JsonOutputMeta.operationType.Both")
  };

  /** The operations type codes */
  public static final String[] operationTypeCode = {"outputvalue", "writetofile", "both"};

  public static final int OPERATION_TYPE_OUTPUT_VALUE = 0;
  public static final int OPERATION_TYPE_WRITE_TO_FILE = 1;
  public static final int OPERATION_TYPE_BOTH = 2;

  /** The encoding to use for reading: null or empty string means system default encoding */
  @Injection(name = "ENCODING", group = "GENERAL")
  private String encoding;

  /** The name value containing the resulting Json fragment */
  @Injection(name = "OUTPUT_VALUE", group = "GENERAL")
  private String outputValue;

  /** The name of the json bloc */
  @Injection(name = "JSON_BLOC_NAME", group = "GENERAL")
  private String jsonBloc;

  /** Choose if you want the output prittyfied */
  @Injection(name = "PRITTIFY", group = "GENERAL")
  private boolean jsonPrittified;

  /* THE FIELD SPECIFICATIONS ... */

  /** The output fields */
  @InjectionDeep private JsonOutputField[] outputFields;

  /** The key fields */
  @InjectionDeep private JsonOutputKeyField[] keyFields;

  @Injection(name = "ADD_TO_RESULT", group = "GENERAL")
  private boolean addToResult;

  /** Flag to indicate the we want to append to the end of an existing file (if it exists) */
  @Injection(name = "APPEND", group = "GENERAL")
  private boolean fileAppended;

  /** Flag to indicate to force unmarshall to JSON Arrays even with a single occurrence in a list */
  @Injection(name = "FORCE_JSON_ARRAYS", group = "GENERAL")
  private boolean useArrayWithSingleInstance;

  /** Flag to indicate to force unmarshall to JSON Arrays even with a single occurrence in a list */
  @Injection(name = "FORCE_SINGLE_ITEM", group = "GENERAL")
  private boolean useSingleItemPerGroup;

  /** Flag: create parent folder if needed */
  @Injection(name = "CREATE_PARENT_FOLDER", group = "GENERAL")
  private boolean createparentfolder;

  private boolean doNotOpenNewFileInit;

  private String jsonSizeFieldname;

  public String getJsonSizeFieldname() {
    return jsonSizeFieldname;
  }

  public void setJsonSizeFieldname(String jsonSizeFieldname) {
    this.jsonSizeFieldname = jsonSizeFieldname;
  }

  public JsonOutputMeta() {
    super();
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
    return createparentfolder;
  }

  /**
   * @param createparentfolder The create parent folder flag to set.
   */
  public void setCreateParentFolder(boolean createparentfolder) {
    this.createparentfolder = createparentfolder;
  }

  /**
   * @return Returns the extension.
   */
  @Override
  public String getExtension() {
    return extension;
  }

  /**
   * @param extension The extension to set.
   */
  @Override
  public void setExtension(String extension) {
    this.extension = extension;
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
   * @return Returns the fileName.
   */
  @Override
  public String getFileName() {
    return fileName;
  }

  /**
   * @return Returns the timeInFilename.
   */
  @Override
  public boolean isTimeInFilename() {
    return timeInFilename;
  }

  /**
   * @return Returns the dateInFilename.
   */
  @Override
  public boolean isDateInFilename() {
    return dateInFilename;
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
   * @param fileName The fileName to set.
   */
  @Override
  public void setFileName(String fileName) {
    this.fileName = fileName;
  }

  /**
   * @return Returns the Add to result filename flag.
   */
  public boolean addToResult() {
    return addToResult;
  }

  public int getOperationType() {
    return operationType;
  }

  public static int getOperationTypeByDesc(String tt) {
    if (tt == null) {
      return 0;
    }

    for (int i = 0; i < operationTypeDesc.length; i++) {
      if (operationTypeDesc[i].equalsIgnoreCase(tt)) {
        return i;
      }
    }
    // If this fails, try to match using the code.
    return getOperationTypeByCode(tt);
  }

  private static int getOperationTypeByCode(String tt) {
    if (tt == null) {
      return 0;
    }

    for (int i = 0; i < operationTypeCode.length; i++) {
      if (operationTypeCode[i].equalsIgnoreCase(tt)) {
        return i;
      }
    }
    return 0;
  }

  public static String getOperationTypeDesc(int i) {
    if (i < 0 || i >= operationTypeDesc.length) {
      return operationTypeDesc[0];
    }
    return operationTypeDesc[i];
  }

  public void setOperationType(int operationType) {
    this.operationType = operationType;
  }

  /**
   * @return Returns the outputFields.
   */
  public JsonOutputField[] getOutputFields() {
    return outputFields;
  }

  /**
   * @param outputFields The outputFields to set.
   */
  public void setOutputFields(JsonOutputField[] outputFields) {
    this.outputFields = outputFields;
  }

  public JsonOutputKeyField[] getKeyFields() {
    return keyFields;
  }

  public void setKeyFields(JsonOutputKeyField[] keyFields) {
    this.keyFields = keyFields;
  }

  @Override
  public void loadXml(Node transformnode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    readData(transformnode);
  }

  public void allocate(int nrfields) {
    outputFields = new JsonOutputField[nrfields];
  }

  public void allocateKey(int nrfields) {
    keyFields = new JsonOutputKeyField[nrfields];
  }

  @Override
  public Object clone() {

    JsonOutputMeta retval = (JsonOutputMeta) super.clone();
    int nrOutputFields = outputFields.length;

    retval.allocate(nrOutputFields);

    for (int i = 0; i < nrOutputFields; i++) {
      retval.outputFields[i] = (JsonOutputField) outputFields[i].clone();
    }

    int nrKeyFields = keyFields.length;

    retval.allocateKey(nrKeyFields);

    for (int i = 0; i < nrKeyFields; i++) {
      retval.keyFields[i] = (JsonOutputKeyField) keyFields[i].clone();
    }

    return retval;
  }

  /**
   * @param addToResult The Add file to result to set.
   */
  public void setAddToResult(boolean addToResult) {
    this.addToResult = addToResult;
  }

  private void readData(Node transformnode) throws HopXmlException {
    try {
      outputValue = XmlHandler.getTagValue(transformnode, CONST_OUTPUT_VALUE);
      jsonBloc = XmlHandler.getTagValue(transformnode, "jsonBloc");
      operationType =
          getOperationTypeByCode(
              Const.NVL(XmlHandler.getTagValue(transformnode, "operation_type"), ""));
      useArrayWithSingleInstance =
          "Y"
              .equalsIgnoreCase(
                  XmlHandler.getTagValue(transformnode, "use_arrays_with_single_instance"));
      useSingleItemPerGroup =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformnode, "use_single_item_per_group"));
      jsonPrittified =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformnode, "json_prittified"));

      encoding = XmlHandler.getTagValue(transformnode, "encoding");
      addToResult = "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformnode, "addToResult"));
      fileName = XmlHandler.getTagValue(transformnode, "file", "name");
      splitOutputAfter =
          Integer.parseInt(XmlHandler.getTagValue(transformnode, "file", "split_output_after"));
      createparentfolder =
          "Y"
              .equalsIgnoreCase(
                  XmlHandler.getTagValue(transformnode, "file", "create_parent_folder"));
      extension =
          Const.NVL(
              XmlHandler.getTagValue(transformnode, "file", "extention"),
              XmlHandler.getTagValue(transformnode, "file", "extension"));
      fileAppended = "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformnode, "file", "append"));
      transformNrInFilename =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformnode, "file", "split"));
      partNrInFilename =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformnode, "file", "haspartno"));
      dateInFilename =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformnode, "file", "add_date"));
      timeInFilename =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformnode, "file", "add_time"));
      doNotOpenNewFileInit =
          "Y"
              .equalsIgnoreCase(
                  XmlHandler.getTagValue(transformnode, "file", "doNotOpenNewFileInit"));

      Node keyFieldNodes = XmlHandler.getSubNode(transformnode, "key_fields");
      int nrKeyFields = XmlHandler.countNodes(keyFieldNodes, CONST_KEY_FIELD);

      allocateKey(nrKeyFields);

      for (int i = 0; i < nrKeyFields; i++) {
        Node fnode = XmlHandler.getSubNodeByNr(keyFieldNodes, CONST_KEY_FIELD, i);

        keyFields[i] = new JsonOutputKeyField();
        keyFields[i].setFieldName(XmlHandler.getTagValue(fnode, "key_field_name"));
        keyFields[i].setElementName(XmlHandler.getTagValue(fnode, "key_field_element"));
      }

      Node fields = XmlHandler.getSubNode(transformnode, "fields");
      int nrfields = XmlHandler.countNodes(fields, CONST_FIELD);

      allocate(nrfields);

      for (int i = 0; i < nrfields; i++) {
        Node fnode = XmlHandler.getSubNodeByNr(fields, CONST_FIELD, i);

        outputFields[i] = new JsonOutputField();
        outputFields[i].setFieldName(XmlHandler.getTagValue(fnode, "name"));
        outputFields[i].setElementName(XmlHandler.getTagValue(fnode, "element"));
        outputFields[i].setJSONFragment(
            !"N".equalsIgnoreCase(XmlHandler.getTagValue(fnode, "json_fragment")));
        outputFields[i].setWithoutEnclosing(
            "Y".equalsIgnoreCase(XmlHandler.getTagValue(fnode, "is_without_enclosing")));
        outputFields[i].setRemoveIfBlank(
            !"N".equalsIgnoreCase(XmlHandler.getTagValue(fnode, "remove_if_blank")));
      }

      jsonSizeFieldname =
          XmlHandler.getTagValue(transformnode, "additional_fields", "json_size_field");

    } catch (Exception e) {
      throw new HopXmlException("Unable to load Transform info from XML", e);
    }
  }

  @Override
  public void setDefault() {

    encoding = Const.XML_ENCODING;
    outputValue = CONST_OUTPUT_VALUE;
    jsonBloc = "result";
    splitOutputAfter = 0;
    operationType = OPERATION_TYPE_WRITE_TO_FILE;

    int nrfields = 0;

    allocate(nrfields);

    for (int i = 0; i < nrfields; i++) {
      outputFields[i] = new JsonOutputField();
      outputFields[i].setFieldName(CONST_FIELD + i);
      outputFields[i].setElementName(CONST_FIELD + i);
      outputFields[i].setJSONFragment(false);
      outputFields[i].setRemoveIfBlank(false);
    }

    int nrKeyFields = 0;

    allocateKey(nrKeyFields);

    for (int i = 0; i < nrKeyFields; i++) {
      keyFields[i] = new JsonOutputKeyField();
      keyFields[i].setFieldName(CONST_KEY_FIELD + i);
    }
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

    if (getOperationType() != OPERATION_TYPE_WRITE_TO_FILE) {
      IRowMeta rowMeta = row.clone();
      row.clear();

      JsonOutputKeyField[] keyFields = this.getKeyFields();
      for (int i = 0; i < this.getKeyFields().length; i++) {
        IValueMeta vmi = rowMeta.getValueMeta(rowMeta.indexOfValue(keyFields[i].getFieldName()));
        row.addValueMeta(i, vmi);
      }

      ValueMetaString vm = new ValueMetaString(this.getOutputValue());
      row.addValueMeta(this.getKeyFields().length, vm);

      int fieldLength = this.getKeyFields().length + 1;
      if (this.jsonSizeFieldname != null && this.jsonSizeFieldname.length() > 0) {
        row.addValueMeta(fieldLength, new ValueMetaInteger(this.jsonSizeFieldname));
        fieldLength++;
      }
    }
  }

  @Override
  public String getXml() {
    StringBuffer retval = new StringBuffer(500);

    retval.append("    ").append(XmlHandler.addTagValue(CONST_OUTPUT_VALUE, outputValue));
    retval.append("    ").append(XmlHandler.addTagValue("jsonBloc", jsonBloc));
    retval
        .append("    ")
        .append(XmlHandler.addTagValue("operation_type", getOperationTypeCode(operationType)));
    retval
        .append("    ")
        .append(
            XmlHandler.addTagValue("use_arrays_with_single_instance", useArrayWithSingleInstance));
    retval
        .append("    ")
        .append(XmlHandler.addTagValue("use_single_item_per_group", useSingleItemPerGroup));
    retval.append("    ").append(XmlHandler.addTagValue("json_prittified", jsonPrittified));
    retval.append("    ").append(XmlHandler.addTagValue("encoding", encoding));
    retval.append("    ").append(XmlHandler.addTagValue("addtoresult", addToResult));
    retval.append("    <file>" + Const.CR);
    retval.append(CONST_SPACES).append(XmlHandler.addTagValue("name", fileName));
    retval
        .append(CONST_SPACES)
        .append(XmlHandler.addTagValue("split_output_after", Integer.toString(splitOutputAfter)));
    retval.append(CONST_SPACES).append(XmlHandler.addTagValue("extention", extension));
    retval.append(CONST_SPACES).append(XmlHandler.addTagValue("append", fileAppended));
    retval.append(CONST_SPACES).append(XmlHandler.addTagValue("split", transformNrInFilename));
    retval.append(CONST_SPACES).append(XmlHandler.addTagValue("haspartno", partNrInFilename));
    retval.append(CONST_SPACES).append(XmlHandler.addTagValue("add_date", dateInFilename));
    retval.append(CONST_SPACES).append(XmlHandler.addTagValue("add_time", timeInFilename));
    retval
        .append(CONST_SPACES)
        .append(XmlHandler.addTagValue("create_parent_folder", createparentfolder));
    retval
        .append(CONST_SPACES)
        .append(XmlHandler.addTagValue("doNotOpenNewFileInit", doNotOpenNewFileInit));
    retval.append("      </file>" + Const.CR);
    retval.append("     <additional_fields>" + Const.CR);
    retval
        .append(CONST_SPACES)
        .append(XmlHandler.addTagValue("json_size_field", jsonSizeFieldname));
    retval.append("      </additional_fields>" + Const.CR);

    retval.append("    <key_fields>").append(Const.CR);
    for (int i = 0; i < keyFields.length; i++) {
      JsonOutputKeyField keyField = keyFields[i];

      if (keyField.getFieldName() != null && keyField.getFieldName().length() != 0) {
        retval.append("      <key_field>").append(Const.CR);
        retval
            .append(CONST_SPACES_LONG)
            .append(XmlHandler.addTagValue("key_field_name", keyField.getFieldName()));
        retval
            .append(CONST_SPACES_LONG)
            .append(XmlHandler.addTagValue("key_field_element", keyField.getElementName()));
        retval.append("    </key_field>" + Const.CR);
      }
    }
    retval.append("    </key_fields>").append(Const.CR);

    retval.append("    <fields>").append(Const.CR);
    for (int i = 0; i < outputFields.length; i++) {
      JsonOutputField field = outputFields[i];

      if (field.getFieldName() != null && field.getFieldName().length() != 0) {
        retval.append("      <field>").append(Const.CR);
        retval
            .append(CONST_SPACES_LONG)
            .append(XmlHandler.addTagValue("name", field.getFieldName()));
        retval
            .append(CONST_SPACES_LONG)
            .append(XmlHandler.addTagValue("element", field.getElementName()));
        retval
            .append(CONST_SPACES_LONG)
            .append(XmlHandler.addTagValue("json_fragment", field.isJSONFragment()));
        retval
            .append(CONST_SPACES_LONG)
            .append(XmlHandler.addTagValue("is_without_enclosing", field.isWithoutEnclosing()));
        retval
            .append(CONST_SPACES_LONG)
            .append(XmlHandler.addTagValue("remove_if_blank", field.isRemoveIfBlank()));
        retval.append("    </field>" + Const.CR);
      }
    }
    retval.append("    </fields>").append(Const.CR);
    return retval.toString();
  }

  private static String getOperationTypeCode(int i) {
    if (i < 0 || i >= operationTypeCode.length) {
      return operationTypeCode[0];
    }
    return operationTypeCode[i];
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
    if (getOperationType() != JsonOutputMeta.OPERATION_TYPE_WRITE_TO_FILE
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
      for (int i = 0; i < outputFields.length; i++) {
        int idx = prev.indexOfValue(outputFields[i].getFieldName());
        if (idx < 0) {
          errorMessage += "\t\t" + outputFields[i].getFieldName() + Const.CR;
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

  public String getOutputValue() {
    return outputValue;
  }

  public void setOutputValue(String outputValue) {
    this.outputValue = outputValue;
  }

  public boolean isUseArrayWithSingleInstance() {
    return useArrayWithSingleInstance;
  }

  public void setUseArrayWithSingleInstance(boolean useArrayWithSingleInstance) {
    this.useArrayWithSingleInstance = useArrayWithSingleInstance;
  }

  public boolean isJsonPrittified() {
    return jsonPrittified;
  }

  public void setJsonPrittified(boolean jsonPrittified) {
    this.jsonPrittified = jsonPrittified;
  }

  public boolean isUseSingleItemPerGroup() {
    return useSingleItemPerGroup;
  }

  public void setUseSingleItemPerGroup(boolean useSingleItemPerGroup) {
    this.useSingleItemPerGroup = useSingleItemPerGroup;
  }
}
