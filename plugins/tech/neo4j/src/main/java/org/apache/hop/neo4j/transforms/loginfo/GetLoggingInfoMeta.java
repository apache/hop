/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.neo4j.transforms.loginfo;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.injection.Injection;
import org.apache.hop.core.injection.InjectionSupported;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaNone;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.w3c.dom.Node;

import java.util.Arrays;
import java.util.List;

/*
 * Created on 05-aug-2003
 *
 */
@Transform(
    id = "GetLoggingInfo",
    name = "Get Neo4j Logging Info",
    description = "Queries the Neo4j logging graph and gets information back",
    categoryDescription = "Neo4j",
    image = "systeminfo.svg",
    documentationUrl =
        "https://hop.apache.org/manual/latest/plugins/transforms/neo4j-get-logging-info.html")
@InjectionSupported(localizationPrefix = "GetLoggingInfoMeta.Injection.")
public class GetLoggingInfoMeta extends BaseTransformMeta
    implements ITransformMeta<GetLoggingInfo, GetLoggingInfoData> {
  private static Class<?> PKG =
      GetLoggingInfoMeta.class; // for i18n purposes, needed by Translator2!!

  @Injection(name = "FIELD_NAME")
  private String[] fieldName;

  @Injection(name = "FIELD_TYPE", converter = GetLoggingInfoMetaInjectionTypeConverter.class)
  private GetLoggingInfoTypes[] fieldType;

  @Injection(name = "FIELD_ARGUMENT")
  private String[] fieldArgument;

  public GetLoggingInfoMeta() {
    super(); // allocate BaseTransformMeta
  }

  /** @return Returns the fieldName. */
  public String[] getFieldName() {
    return fieldName;
  }

  /** @param fieldName The fieldName to set. */
  public void setFieldName(String[] fieldName) {
    this.fieldName = fieldName;
  }

  /** @return Returns the fieldType. */
  public GetLoggingInfoTypes[] getFieldType() {
    return fieldType;
  }

  /** @param fieldType The fieldType to set. */
  public void setFieldType(GetLoggingInfoTypes[] fieldType) {
    this.fieldType = fieldType;
  }

  /**
   * Gets fieldArgument
   *
   * @return value of fieldArgument
   */
  public String[] getFieldArgument() {
    return fieldArgument;
  }

  /** @param fieldArgument The fieldArgument to set */
  public void setFieldArgument(String[] fieldArgument) {
    this.fieldArgument = fieldArgument;
  }

  public void allocate(int count) {
    fieldName = new String[count];
    fieldType = new GetLoggingInfoTypes[count];
    fieldArgument = new String[count];
  }

  @Override
  public Object clone() {
    GetLoggingInfoMeta retval = (GetLoggingInfoMeta) super.clone();

    int count = fieldName.length;

    retval.allocate(count);

    System.arraycopy(fieldName, 0, retval.fieldName, 0, count);
    System.arraycopy(fieldType, 0, retval.fieldType, 0, count);
    System.arraycopy(fieldArgument, 0, retval.fieldArgument, 0, count);

    return retval;
  }

  @Override
  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    try {
      Node fields = XmlHandler.getSubNode(transformNode, "fields");
      int count = XmlHandler.countNodes(fields, "field");
      String type;

      allocate(count);

      for (int i = 0; i < count; i++) {
        Node fnode = XmlHandler.getSubNodeByNr(fields, "field", i);

        fieldName[i] = XmlHandler.getTagValue(fnode, "name");
        type = XmlHandler.getTagValue(fnode, "type");
        fieldType[i] = GetLoggingInfoTypes.getTypeFromString(type);
        fieldArgument[i] = XmlHandler.getTagValue(fnode, "argument");
      }
    } catch (Exception e) {
      throw new HopXmlException("Unable to read transform information from Xml", e);
    }
  }

  @Override
  public void setDefault() {
    allocate(4);

    fieldName[0] = "startOfPipelineDelta";
    fieldType[0] = GetLoggingInfoTypes.TYPE_SYSTEM_INFO_PIPELINE_DATE_FROM;
    fieldName[1] = "endOfPipelineDelta";
    fieldType[1] = GetLoggingInfoTypes.TYPE_SYSTEM_INFO_PIPELINE_DATE_TO;
    fieldName[2] = "startOfWorkflowDelta";
    fieldType[2] = GetLoggingInfoTypes.TYPE_SYSTEM_INFO_WORKFLOW_DATE_FROM;
    fieldName[3] = "endOfWorkflowDelta";
    fieldType[3] = GetLoggingInfoTypes.TYPE_SYSTEM_INFO_WORKFLOW_DATE_TO;
  }

  @Override
  public void getFields(
      IRowMeta row,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables space,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {
    for (int i = 0; i < fieldName.length; i++) {
      IValueMeta v;

      switch (fieldType[i]) {
        case TYPE_SYSTEM_INFO_PIPELINE_DATE_FROM:
        case TYPE_SYSTEM_INFO_PIPELINE_DATE_TO:
        case TYPE_SYSTEM_INFO_WORKFLOW_DATE_FROM:
        case TYPE_SYSTEM_INFO_WORKFLOW_DATE_TO:
        case TYPE_SYSTEM_INFO_PIPELINE_PREVIOUS_EXECUTION_DATE:
        case TYPE_SYSTEM_INFO_PIPELINE_PREVIOUS_SUCCESS_DATE:
        case TYPE_SYSTEM_INFO_WORKFLOW_PREVIOUS_EXECUTION_DATE:
        case TYPE_SYSTEM_INFO_WORKFLOW_PREVIOUS_SUCCESS_DATE:
          v = new ValueMetaDate(fieldName[i]);
          break;
        default:
          v = new ValueMetaNone(fieldName[i]);
          break;
      }
      v.setOrigin(name);
      row.addValueMeta(v);
    }
  }

  @Override
  public String getXml() {
    StringBuilder retval = new StringBuilder();

    retval.append("    <fields>" + Const.CR);

    for (int i = 0; i < fieldName.length; i++) {
      retval.append("      <field>" + Const.CR);
      retval.append("        " + XmlHandler.addTagValue("name", fieldName[i]));
      retval.append(
          "        "
              + XmlHandler.addTagValue("type", fieldType[i] != null ? fieldType[i].getCode() : ""));
      retval.append("        " + XmlHandler.addTagValue("argument", fieldArgument[i]));
      retval.append("        </field>" + Const.CR);
    }
    retval.append("      </fields>" + Const.CR);

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
      IVariables space,
      IHopMetadataProvider metadataProvider) {
    // See if we have input streams leading to this transform!
    int nrRemarks = remarks.size();
    for (int i = 0; i < fieldName.length; i++) {
      if (fieldType[i].ordinal() <= GetLoggingInfoTypes.TYPE_SYSTEM_INFO_NONE.ordinal()) {
        CheckResult cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(
                    PKG, "SystemDataMeta.CheckResult.FieldHasNoType", fieldName[i]),
                transformMeta);
        remarks.add(cr);
      }
    }
    if (remarks.size() == nrRemarks) {
      CheckResult cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "SystemDataMeta.CheckResult.AllTypesSpecified"),
              transformMeta);
      remarks.add(cr);
    }
  }

  @Override
  public GetLoggingInfo createTransform(
      TransformMeta transformMeta,
      GetLoggingInfoData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    return new GetLoggingInfo(transformMeta, this, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public GetLoggingInfoData getTransformData() {
    return new GetLoggingInfoData();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof GetLoggingInfoMeta)) {
      return false;
    }
    GetLoggingInfoMeta that = (GetLoggingInfoMeta) o;

    if (!Arrays.equals(fieldName, that.fieldName)) {
      return false;
    }
    if (!Arrays.equals(fieldType, that.fieldType)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = Arrays.hashCode(fieldName);
    result = 31 * result + Arrays.hashCode(fieldType);
    return result;
  }
}
