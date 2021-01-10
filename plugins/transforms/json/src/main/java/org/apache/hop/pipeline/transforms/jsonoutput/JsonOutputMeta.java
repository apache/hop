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

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
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
import org.w3c.dom.Node;

import java.util.List;

/**
 * This class knows how to handle the MetaData for the Json output transform
 *
 * @since 14-june-2010
 */
@Transform(
    id = "JsonOutput",
    image = "JSO.svg",
    name = "i18n::JsonOutput.name",
    description = "i18n::JsonOutput.description",
    categoryDescription = "i18n::JsonOutput.category",
    keywords = {"json", "javascript", "object", "notation"},
    documentationUrl = "https://hop.apache.org/manual/latest/plugins/transforms/jsonoutput.html")
public class JsonOutputMeta extends BaseFileOutputMeta
    implements ITransformMeta<JsonOutput, JsonOutputData> {
  private static final Class<?> PKG = JsonOutputMeta.class; // For Translator

  /** Operations type */
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
  private String encoding;

  /** The name value containing the resulting Json fragment */
  private String outputValue;

  /** The name of the json bloc */
  private String jsonBloc;

  private String nrRowsInBloc;

  /* THE FIELD SPECIFICATIONS ... */

  /** The output fields */
  private JsonOutputField[] outputFields;

  private boolean AddToResult;

  /** Flag to indicate the we want to append to the end of an existing file (if it exists) */
  private boolean fileAppended;

  /** Flag to indicate whether or not to create JSON structures compatible with pre PDI-4.3.0 */
  private boolean compatibilityMode;

  /** Flag: create parent folder if needed */
  private boolean createparentfolder;

  private boolean DoNotOpenNewFileInit;

  public JsonOutputMeta() {
    super(); // allocate BaseTransformMeta
  }

  public boolean isDoNotOpenNewFileInit() {
    return DoNotOpenNewFileInit;
  }

  public void setDoNotOpenNewFileInit(boolean DoNotOpenNewFileInit) {
    this.DoNotOpenNewFileInit = DoNotOpenNewFileInit;
  }

  /** @return Returns the create parent folder flag. */
  public boolean isCreateParentFolder() {
    return createparentfolder;
  }

  /** @param createparentfolder The create parent folder flag to set. */
  public void setCreateParentFolder(boolean createparentfolder) {
    this.createparentfolder = createparentfolder;
  }

  /** @return Returns the fileAppended. */
  public boolean isFileAppended() {
    return fileAppended;
  }

  /** @param fileAppended The fileAppended to set. */
  public void setFileAppended(boolean fileAppended) {
    this.fileAppended = fileAppended;
  }

  /** @param dateInFilename The dateInFilename to set. */
  public void setDateInFilename(boolean dateInFilename) {
    this.dateInFilename = dateInFilename;
  }

  /** @param timeInFilename The timeInFilename to set. */
  public void setTimeInFilename(boolean timeInFilename) {
    this.timeInFilename = timeInFilename;
  }

  /** @return Returns the Add to result filesname flag. */
  public boolean AddToResult() {
    return AddToResult;
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

  public void setOperationType(int operationType) {
    this.operationType = operationType;
  }

  public static String getOperationTypeDesc(int i) {
    if (i < 0 || i >= operationTypeDesc.length) {
      return operationTypeDesc[0];
    }
    return operationTypeDesc[i];
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

  /** @return Returns the outputFields. */
  public JsonOutputField[] getOutputFields() {
    return outputFields;
  }

  /** @param outputFields The outputFields to set. */
  public void setOutputFields(JsonOutputField[] outputFields) {
    this.outputFields = outputFields;
  }

  public void loadXml(Node transformnode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    readData(transformnode);
  }

  public void allocate(int nrFields) {
    outputFields = new JsonOutputField[nrFields];
  }

  public Object clone() {
    JsonOutputMeta retval = (JsonOutputMeta) super.clone();
    int nrFields = outputFields.length;

    retval.allocate(nrFields);

    for (int i = 0; i < nrFields; i++) {
      retval.outputFields[i] = (JsonOutputField) outputFields[i].clone();
    }

    return retval;
  }

  /** @param AddToResult The Add file to result to set. */
  public void setAddToResult(boolean AddToResult) {
    this.AddToResult = AddToResult;
  }

  private void readData(Node transformnode) throws HopXmlException {
    try {
      outputValue = XmlHandler.getTagValue(transformnode, "outputValue");
      jsonBloc = XmlHandler.getTagValue(transformnode, "jsonBloc");
      nrRowsInBloc = XmlHandler.getTagValue(transformnode, "nrRowsInBloc");
      operationType =
          getOperationTypeByCode(
              Const.NVL(XmlHandler.getTagValue(transformnode, "operation_type"), ""));
      compatibilityMode =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformnode, "compatibility_mode"));

      encoding = XmlHandler.getTagValue(transformnode, "encoding");
      AddToResult = "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformnode, "AddToResult"));
      fileName = XmlHandler.getTagValue(transformnode, "file", "name");
      createparentfolder =
          "Y"
              .equalsIgnoreCase(
                  XmlHandler.getTagValue(transformnode, "file", "create_parent_folder"));
      extension = XmlHandler.getTagValue(transformnode, "file", "extention");
      fileAppended = "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformnode, "file", "append"));
      partNrInFilename =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformnode, "file", "haspartno"));
      dateInFilename =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformnode, "file", "add_date"));
      timeInFilename =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformnode, "file", "add_time"));
      DoNotOpenNewFileInit =
          "Y"
              .equalsIgnoreCase(
                  XmlHandler.getTagValue(transformnode, "file", "DoNotOpenNewFileInit"));

      Node fields = XmlHandler.getSubNode(transformnode, "fields");
      int nrFields = XmlHandler.countNodes(fields, "field");

      allocate(nrFields);

      for (int i = 0; i < nrFields; i++) {
        Node fnode = XmlHandler.getSubNodeByNr(fields, "field", i);

        outputFields[i] = new JsonOutputField();
        outputFields[i].setFieldName(XmlHandler.getTagValue(fnode, "name"));
        outputFields[i].setElementName(XmlHandler.getTagValue(fnode, "element"));
      }
    } catch (Exception e) {
      throw new HopXmlException("Unable to load transform info from Xml", e);
    }
  }

  public void setDefault() {
    encoding = Const.XML_ENCODING;
    outputValue = "outputValue";
    jsonBloc = "data";
    nrRowsInBloc = "1";
    operationType = OPERATION_TYPE_WRITE_TO_FILE;
    extension = "js";
    int nrFields = 0;

    allocate(nrFields);

    for (int i = 0; i < nrFields; i++) {
      outputFields[i] = new JsonOutputField();
      outputFields[i].setFieldName("field" + i);
      outputFields[i].setElementName("field" + i);
    }
  }

  public void getFields(
      IRowMeta row,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {

    if (getOperationType() != OPERATION_TYPE_WRITE_TO_FILE) {
      IValueMeta v = new ValueMetaString(variables.resolve(this.getOutputValue()));
      v.setOrigin(name);
      row.addValueMeta(v);
    }
  }

  private static String getOperationTypeCode(int i) {
    if (i < 0 || i >= operationTypeCode.length) {
      return operationTypeCode[0];
    }
    return operationTypeCode[i];
  }

  public String getXml() {
    StringBuffer retval = new StringBuffer(500);

    retval.append("    ").append(XmlHandler.addTagValue("outputValue", outputValue));
    retval.append("    ").append(XmlHandler.addTagValue("jsonBloc", jsonBloc));
    retval.append("    ").append(XmlHandler.addTagValue("nrRowsInBloc", nrRowsInBloc));
    retval
        .append("    ")
        .append(XmlHandler.addTagValue("operation_type", getOperationTypeCode(operationType)));
    retval.append("    ").append(XmlHandler.addTagValue("compatibility_mode", compatibilityMode));
    retval.append("    ").append(XmlHandler.addTagValue("encoding", encoding));
    retval.append("    ").append(XmlHandler.addTagValue("addtoresult", AddToResult));
    retval.append("    <file>" + Const.CR);
    retval.append("      ").append(XmlHandler.addTagValue("name", fileName));
    retval.append("      ").append(XmlHandler.addTagValue("extention", extension));
    retval.append("      ").append(XmlHandler.addTagValue("append", fileAppended));
    retval.append("      ").append(XmlHandler.addTagValue("haspartno", partNrInFilename));
    retval.append("      ").append(XmlHandler.addTagValue("add_date", dateInFilename));
    retval.append("      ").append(XmlHandler.addTagValue("add_time", timeInFilename));
    retval
        .append("      ")
        .append(XmlHandler.addTagValue("create_parent_folder", createparentfolder));
    retval
        .append("      ")
        .append(XmlHandler.addTagValue("DoNotOpenNewFileInit", DoNotOpenNewFileInit));
    retval.append("      </file>" + Const.CR);

    retval.append("    <fields>").append(Const.CR);
    for (int i = 0; i < outputFields.length; i++) {
      JsonOutputField field = outputFields[i];

      if (field.getFieldName() != null && field.getFieldName().length() != 0) {
        retval.append("      <field>").append(Const.CR);
        retval.append("        ").append(XmlHandler.addTagValue("name", field.getFieldName()));
        retval.append("        ").append(XmlHandler.addTagValue("element", field.getElementName()));
        retval.append("    </field>" + Const.CR);
      }
    }
    retval.append("    </fields>").append(Const.CR);
    return retval.toString();
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
    if (getOperationType() != JsonOutputMeta.OPERATION_TYPE_WRITE_TO_FILE) {
      // We need to have output field name
      if (Utils.isEmpty(variables.resolve(getOutputValue()))) {
        cr =
            new CheckResult(
                CheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(PKG, "JsonOutput.Error.MissingOutputFieldName"),
                transformMeta);
        remarks.add(cr);
      }
    }
    if (Utils.isEmpty(variables.resolve(getFileName()))) {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "JsonOutput.Error.MissingTargetFilename"),
              transformMeta);
      remarks.add(cr);
    }
    // Check output fields
    if (prev != null && prev.size() > 0) {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_OK,
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
        cr = new CheckResult(CheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
        remarks.add(cr);
      } else {
        cr =
            new CheckResult(
                CheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(PKG, "JsonOutputMeta.CheckResult.AllFieldsFound"),
                transformMeta);
        remarks.add(cr);
      }
    }

    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "JsonOutputMeta.CheckResult.ExpectedInputOk"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "JsonOutputMeta.CheckResult.ExpectedInputError"),
              transformMeta);
      remarks.add(cr);
    }

    cr =
        new CheckResult(
            CheckResult.TYPE_RESULT_COMMENT,
            BaseMessages.getString(PKG, "JsonOutputMeta.CheckResult.FilesNotChecked"),
            transformMeta);
    remarks.add(cr);
  }

  @Override
  public JsonOutput createTransform(
      TransformMeta transformMeta,
      JsonOutputData data,
      int cnr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    return new JsonOutput(transformMeta, this, data, cnr, pipelineMeta, pipeline);
  }

  public JsonOutputData getTransformData() {
    return new JsonOutputData();
  }

  public String getEncoding() {
    return encoding;
  }

  public void setEncoding(String encoding) {
    this.encoding = encoding;
  }

  /** @return Returns the jsonBloc. */
  public String getJsonBloc() {
    return jsonBloc;
  }

  /** @param jsonBloc The root node to set. */
  public void setJsonBloc(String jsonBloc) {
    this.jsonBloc = jsonBloc;
  }

  /** @return Returns the jsonBloc. */
  public String getNrRowsInBloc() {
    return nrRowsInBloc;
  }

  /** @param nrRowsInBloc The nrRowsInBloc. */
  public void setNrRowsInBloc(String nrRowsInBloc) {
    this.nrRowsInBloc = nrRowsInBloc;
  }

  public int getSplitEvery() {
    try {
      return Integer.parseInt(getNrRowsInBloc());
    } catch (final Exception e) {
      return 1;
    }
  }

  public void setSplitEvery(int splitEvery) {
    setNrRowsInBloc(splitEvery + "");
  }

  public String getOutputValue() {
    return outputValue;
  }

  public void setOutputValue(String outputValue) {
    this.outputValue = outputValue;
  }

  public boolean isCompatibilityMode() {
    return compatibilityMode;
  }

  public void setCompatibilityMode(boolean compatibilityMode) {
    this.compatibilityMode = compatibilityMode;
  }

  public boolean writesToFile() {
    return getOperationType() == JsonOutputMeta.OPERATION_TYPE_WRITE_TO_FILE
        || getOperationType() == JsonOutputMeta.OPERATION_TYPE_BOTH;
  }
}
