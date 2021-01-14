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

package org.apache.hop.pipeline.transforms.systemdata;

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
import org.apache.hop.core.row.value.ValueMetaBoolean;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaNone;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.w3c.dom.Node;

import java.util.Arrays;
import java.util.List;

@InjectionSupported(localizationPrefix = "SystemDataMeta.Injection.")
@Transform(
    id = "SystemInfo",
    image = "systeminfo.svg",
    name = "i18n::BaseTransform.TypeLongDesc.SystemInfo",
    description = "i18n::BaseTransform.TypeTooltipDesc.SystemInfo",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Input",
    documentationUrl = "https://hop.apache.org/manual/latest/plugins/transforms/systemdata.html")
public class SystemDataMeta extends BaseTransformMeta
    implements ITransformMeta<SystemData, SystemDataData> {
  private static final Class<?> PKG = SystemDataMeta.class; // For Translator

  @Injection(name = "FIELD_NAME")
  private String[] fieldName;

  @Injection(name = "FIELD_TYPE", converter = SystemDataMetaInjectionTypeConverter.class)
  private SystemDataTypes[] fieldType;

  public SystemDataMeta() {
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
  public SystemDataTypes[] getFieldType() {
    return fieldType;
  }

  /** @param fieldType The fieldType to set. */
  public void setFieldType(SystemDataTypes[] fieldType) {
    this.fieldType = fieldType;
  }

  @Override
  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    readData(transformNode);
  }

  public void allocate(int count) {
    fieldName = new String[count];
    fieldType = new SystemDataTypes[count];
  }

  @Override
  public Object clone() {
    SystemDataMeta retval = (SystemDataMeta) super.clone();

    int count = fieldName.length;

    retval.allocate(count);

    System.arraycopy(fieldName, 0, retval.fieldName, 0, count);
    System.arraycopy(fieldType, 0, retval.fieldType, 0, count);

    return retval;
  }

  @Override
  public ITransform createTransform(
      TransformMeta transformMeta,
      SystemDataData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    return new SystemData(transformMeta, this, data, copyNr, pipelineMeta, pipeline);
  }

  private void readData(Node transformNode) throws HopXmlException {
    try {
      Node fields = XmlHandler.getSubNode(transformNode, "fields");
      int count = XmlHandler.countNodes(fields, "field");
      String type;

      allocate(count);

      for (int i = 0; i < count; i++) {
        Node fnode = XmlHandler.getSubNodeByNr(fields, "field", i);

        fieldName[i] = XmlHandler.getTagValue(fnode, "name");
        type = XmlHandler.getTagValue(fnode, "type");
        fieldType[i] = SystemDataTypes.getTypeFromString(type);
      }
    } catch (Exception e) {
      throw new HopXmlException("Unable to read transform information from XML", e);
    }
  }

  @Override
  public void setDefault() {
    int count = 0;

    allocate(count);

    for (int i = 0; i < count; i++) {
      fieldName[i] = "field" + i;
      fieldType[i] = SystemDataTypes.TYPE_SYSTEM_INFO_SYSTEM_DATE;
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
    for (int i = 0; i < fieldName.length; i++) {
      IValueMeta v;

      switch (fieldType[i]) {
        case TYPE_SYSTEM_INFO_SYSTEM_START: // All date values...
        case TYPE_SYSTEM_INFO_SYSTEM_DATE:
        case TYPE_SYSTEM_INFO_PIPELINE_DATE_FROM:
        case TYPE_SYSTEM_INFO_PIPELINE_DATE_TO:
        case TYPE_SYSTEM_INFO_JOB_DATE_FROM:
        case TYPE_SYSTEM_INFO_JOB_DATE_TO:
        case TYPE_SYSTEM_INFO_PREV_DAY_START:
        case TYPE_SYSTEM_INFO_PREV_DAY_END:
        case TYPE_SYSTEM_INFO_THIS_DAY_START:
        case TYPE_SYSTEM_INFO_THIS_DAY_END:
        case TYPE_SYSTEM_INFO_NEXT_DAY_START:
        case TYPE_SYSTEM_INFO_NEXT_DAY_END:
        case TYPE_SYSTEM_INFO_PREV_MONTH_START:
        case TYPE_SYSTEM_INFO_PREV_MONTH_END:
        case TYPE_SYSTEM_INFO_THIS_MONTH_START:
        case TYPE_SYSTEM_INFO_THIS_MONTH_END:
        case TYPE_SYSTEM_INFO_NEXT_MONTH_START:
        case TYPE_SYSTEM_INFO_NEXT_MONTH_END:
        case TYPE_SYSTEM_INFO_MODIFIED_DATE:
        case TYPE_SYSTEM_INFO_PREV_WEEK_START:
        case TYPE_SYSTEM_INFO_PREV_WEEK_END:
        case TYPE_SYSTEM_INFO_PREV_WEEK_OPEN_END:
        case TYPE_SYSTEM_INFO_PREV_WEEK_START_US:
        case TYPE_SYSTEM_INFO_PREV_WEEK_END_US:
        case TYPE_SYSTEM_INFO_THIS_WEEK_START:
        case TYPE_SYSTEM_INFO_THIS_WEEK_END:
        case TYPE_SYSTEM_INFO_THIS_WEEK_OPEN_END:
        case TYPE_SYSTEM_INFO_THIS_WEEK_START_US:
        case TYPE_SYSTEM_INFO_THIS_WEEK_END_US:
        case TYPE_SYSTEM_INFO_NEXT_WEEK_START:
        case TYPE_SYSTEM_INFO_NEXT_WEEK_END:
        case TYPE_SYSTEM_INFO_NEXT_WEEK_OPEN_END:
        case TYPE_SYSTEM_INFO_NEXT_WEEK_START_US:
        case TYPE_SYSTEM_INFO_NEXT_WEEK_END_US:
        case TYPE_SYSTEM_INFO_PREV_QUARTER_START:
        case TYPE_SYSTEM_INFO_PREV_QUARTER_END:
        case TYPE_SYSTEM_INFO_THIS_QUARTER_START:
        case TYPE_SYSTEM_INFO_THIS_QUARTER_END:
        case TYPE_SYSTEM_INFO_NEXT_QUARTER_START:
        case TYPE_SYSTEM_INFO_NEXT_QUARTER_END:
        case TYPE_SYSTEM_INFO_PREV_YEAR_START:
        case TYPE_SYSTEM_INFO_PREV_YEAR_END:
        case TYPE_SYSTEM_INFO_THIS_YEAR_START:
        case TYPE_SYSTEM_INFO_THIS_YEAR_END:
        case TYPE_SYSTEM_INFO_NEXT_YEAR_START:
        case TYPE_SYSTEM_INFO_NEXT_YEAR_END:
          v = new ValueMetaDate(fieldName[i]);
          break;
        case TYPE_SYSTEM_INFO_PIPELINE_NAME:
        case TYPE_SYSTEM_INFO_FILENAME:
        case TYPE_SYSTEM_INFO_MODIFIED_USER:
        case TYPE_SYSTEM_INFO_HOSTNAME:
        case TYPE_SYSTEM_INFO_HOSTNAME_REAL:
        case TYPE_SYSTEM_INFO_IP_ADDRESS:
        case TYPE_SYSTEM_INFO_PREVIOUS_RESULT_LOG_TEXT:
          v = new ValueMetaString(fieldName[i]);
          break;
        case TYPE_SYSTEM_INFO_COPYNR:
        case TYPE_SYSTEM_INFO_CURRENT_PID:
        case TYPE_SYSTEM_INFO_JVM_TOTAL_MEMORY:
        case TYPE_SYSTEM_INFO_JVM_FREE_MEMORY:
        case TYPE_SYSTEM_INFO_JVM_MAX_MEMORY:
        case TYPE_SYSTEM_INFO_JVM_AVAILABLE_MEMORY:
        case TYPE_SYSTEM_INFO_AVAILABLE_PROCESSORS:
        case TYPE_SYSTEM_INFO_JVM_CPU_TIME:
        case TYPE_SYSTEM_INFO_TOTAL_PHYSICAL_MEMORY_SIZE:
        case TYPE_SYSTEM_INFO_TOTAL_SWAP_SPACE_SIZE:
        case TYPE_SYSTEM_INFO_COMMITTED_VIRTUAL_MEMORY_SIZE:
        case TYPE_SYSTEM_INFO_FREE_PHYSICAL_MEMORY_SIZE:
        case TYPE_SYSTEM_INFO_FREE_SWAP_SPACE_SIZE:
        case TYPE_SYSTEM_INFO_PREVIOUS_RESULT_EXIT_STATUS:
        case TYPE_SYSTEM_INFO_PREVIOUS_RESULT_ENTRY_NR:
        case TYPE_SYSTEM_INFO_PREVIOUS_RESULT_NR_ERRORS:
        case TYPE_SYSTEM_INFO_PREVIOUS_RESULT_NR_FILES:
        case TYPE_SYSTEM_INFO_PREVIOUS_RESULT_NR_FILES_RETRIEVED:
        case TYPE_SYSTEM_INFO_PREVIOUS_RESULT_NR_LINES_DELETED:
        case TYPE_SYSTEM_INFO_PREVIOUS_RESULT_NR_LINES_INPUT:
        case TYPE_SYSTEM_INFO_PREVIOUS_RESULT_NR_LINES_OUTPUT:
        case TYPE_SYSTEM_INFO_PREVIOUS_RESULT_NR_LINES_READ:
        case TYPE_SYSTEM_INFO_PREVIOUS_RESULT_NR_LINES_REJECTED:
        case TYPE_SYSTEM_INFO_PREVIOUS_RESULT_NR_LINES_UPDATED:
        case TYPE_SYSTEM_INFO_PREVIOUS_RESULT_NR_LINES_WRITTEN:
          v = new ValueMetaInteger(fieldName[i]);
          v.setLength(IValueMeta.DEFAULT_INTEGER_LENGTH, 0);
          break;
        case TYPE_SYSTEM_INFO_PREVIOUS_RESULT_RESULT:
        case TYPE_SYSTEM_INFO_PREVIOUS_RESULT_IS_STOPPED:
          v = new ValueMetaBoolean(fieldName[i]);
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
      IVariables variables,
      IHopMetadataProvider metadataProvider) {
    // See if we have input streams leading to this transform!
    int nrRemarks = remarks.size();
    for (int i = 0; i < fieldName.length; i++) {
      if (fieldType[i].ordinal() <= SystemDataTypes.TYPE_SYSTEM_INFO_NONE.ordinal()) {
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
  public SystemDataData getTransformData() {
    return new SystemDataData();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof SystemDataMeta)) {
      return false;
    }
    SystemDataMeta that = (SystemDataMeta) o;

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
