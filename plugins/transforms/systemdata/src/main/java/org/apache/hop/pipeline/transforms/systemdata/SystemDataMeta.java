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

import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaBoolean;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaNone;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

@Transform(
    id = "SystemInfo",
    image = "systeminfo.svg",
    name = "i18n::BaseTransform.TypeLongDesc.SystemInfo",
    description = "i18n::BaseTransform.TypeTooltipDesc.SystemInfo",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Input",
    keywords = "i18n::SystemDataMeta.keyword",
    documentationUrl = "/pipeline/transforms/getsystemdata.html")
@Getter
@Setter
public class SystemDataMeta extends BaseTransformMeta<SystemData, SystemDataData> {
  private static final Class<?> PKG = SystemDataMeta.class;

  @HopMetadataProperty(
      groupKey = "fields",
      injectionGroupKey = "FIELDS",
      injectionGroupDescription = "SystemDataMeta.Injection.FIELDS",
      key = "field",
      injectionKey = "FIELD",
      injectionKeyDescription = "SystemDataMeta.Injection.FIELD")
  private List<SystemInfoField> fields;

  public SystemDataMeta() {
    super();
    fields = new ArrayList<>();
  }

  @Override
  public Object clone() {
    return new SystemDataMeta(this);
  }

  public SystemDataMeta(SystemDataMeta m) {
    this();
    m.fields.forEach(f -> fields.add(new SystemInfoField(f)));
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
    for (SystemInfoField field : fields) {
      IValueMeta v;

      switch (field.getFieldType()) {
        case SYSTEM_START,
            SYSTEM_DATE,
            PIPELINE_DATE_FROM,
            PIPELINE_DATE_TO,
            WORKFLOW_DATE_FROM,
            WORKFLOW_DATE_TO,
            PREV_DAY_START,
            PREV_DAY_END,
            THIS_DAY_START,
            THIS_DAY_END,
            NEXT_DAY_START,
            NEXT_DAY_END,
            PREV_MONTH_START,
            PREV_MONTH_END,
            THIS_MONTH_START,
            THIS_MONTH_END,
            NEXT_MONTH_START,
            NEXT_MONTH_END,
            MODIFIED_DATE,
            PREV_WEEK_START,
            PREV_WEEK_END,
            PREV_WEEK_OPEN_END,
            PREV_WEEK_START_US,
            PREV_WEEK_END_US,
            THIS_WEEK_START,
            THIS_WEEK_END,
            THIS_WEEK_OPEN_END,
            THIS_WEEK_START_US,
            THIS_WEEK_END_US,
            NEXT_WEEK_START,
            NEXT_WEEK_END,
            NEXT_WEEK_OPEN_END,
            NEXT_WEEK_START_US,
            NEXT_WEEK_END_US,
            PREV_QUARTER_START,
            PREV_QUARTER_END,
            THIS_QUARTER_START,
            THIS_QUARTER_END,
            NEXT_QUARTER_START,
            NEXT_QUARTER_END,
            PREV_YEAR_START,
            PREV_YEAR_END,
            THIS_YEAR_START,
            THIS_YEAR_END,
            NEXT_YEAR_START,
            NEXT_YEAR_END:
          v = new ValueMetaDate(field.getFieldName());
          break;
        case PIPELINE_NAME,
            FILENAME,
            MODIFIED_USER,
            HOSTNAME,
            HOSTNAME_REAL,
            IP_ADDRESS,
            PREVIOUS_RESULT_LOG_TEXT:
          v = new ValueMetaString(field.getFieldName());
          break;
        case COPYNR,
            CURRENT_PID,
            JVM_TOTAL_MEMORY,
            JVM_FREE_MEMORY,
            JVM_MAX_MEMORY,
            JVM_AVAILABLE_MEMORY,
            AVAILABLE_PROCESSORS,
            JVM_CPU_TIME,
            TOTAL_PHYSICAL_MEMORY_SIZE,
            TOTAL_SWAP_SPACE_SIZE,
            COMMITTED_VIRTUAL_MEMORY_SIZE,
            FREE_PHYSICAL_MEMORY_SIZE,
            FREE_SWAP_SPACE_SIZE,
            PREVIOUS_RESULT_EXIT_STATUS,
            PREVIOUS_RESULT_ENTRY_NR,
            PREVIOUS_RESULT_NR_ERRORS,
            PREVIOUS_RESULT_NR_ROWS,
            PREVIOUS_RESULT_NR_FILES,
            PREVIOUS_RESULT_NR_FILES_RETRIEVED,
            PREVIOUS_RESULT_NR_LINES_DELETED,
            PREVIOUS_RESULT_NR_LINES_INPUT,
            PREVIOUS_RESULT_NR_LINES_OUTPUT,
            PREVIOUS_RESULT_NR_LINES_READ,
            PREVIOUS_RESULT_NR_LINES_REJECTED,
            PREVIOUS_RESULT_NR_LINES_UPDATED,
            PREVIOUS_RESULT_NR_LINES_WRITTEN:
          v = new ValueMetaInteger(field.getFieldName());
          v.setLength(IValueMeta.DEFAULT_INTEGER_LENGTH, 0);
          break;
        case PREVIOUS_RESULT_RESULT, PREVIOUS_RESULT_IS_STOPPED:
          v = new ValueMetaBoolean(field.getFieldName());
          break;
        default:
          v = new ValueMetaNone(field.getFieldName());
          break;
      }
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
    // See if we have input streams leading to this transform!
    int nrRemarks = remarks.size();
    for (SystemInfoField field : fields) {
      if (field.getFieldType().getIndex() <= SystemDataType.NONE.getIndex()) {
        CheckResult cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(
                    PKG, "SystemDataMeta.CheckResult.FieldHasNoType", field.getFieldName()),
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

  @Getter
  @Setter
  public static class SystemInfoField {
    @HopMetadataProperty(
        key = "name",
        injectionKey = "FIELD_NAME",
        injectionKeyDescription = "SystemDataMeta.Injection.FIELD_NAME")
    private String fieldName;

    @HopMetadataProperty(
        key = "type",
        injectionKey = "FIELD_TYPE",
        injectionKeyDescription = "SystemDataMeta.Injection.FIELD_TYPE",
        storeWithCode = true)
    private SystemDataType fieldType;

    public SystemInfoField() {}

    public SystemInfoField(SystemInfoField f) {
      this.fieldName = f.fieldName;
      this.fieldType = f.fieldType;
    }
  }
}
