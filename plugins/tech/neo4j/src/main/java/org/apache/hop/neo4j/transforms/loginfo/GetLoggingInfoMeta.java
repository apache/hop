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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaNone;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

@Transform(
    id = "GetLoggingInfo",
    name = "i18n::GetLoggingInfoDialog.DialogTitle",
    description = "i18n::GetLoggingInfoDialog.Description",
    categoryDescription = "Neo4j",
    image = "systeminfo.svg",
    keywords = "i18n::GetLoggingInfoMeta.keyword",
    documentationUrl = "/pipeline/transforms/neo4j-getloginfo.html")
@Getter
@Setter
public class GetLoggingInfoMeta extends BaseTransformMeta<GetLoggingInfo, GetLoggingInfoData> {
  private static final Class<?> PKG =
      GetLoggingInfoMeta.class; // for i18n purposes, needed by Translator2!!
  public static final String CONST_SPACES = "        ";

  @HopMetadataProperty(
      key = "field",
      groupKey = "fields",
      injectionKey = "FIELD",
      injectionGroupKey = "FIELDS")
  private List<GetLoggingInfoField> fields;

  public GetLoggingInfoMeta() {
    super(); // allocate BaseTransformMeta
    fields = new ArrayList<>();
  }

  @Override
  public Object clone() {
    GetLoggingInfoMeta retval = (GetLoggingInfoMeta) super.clone();

    return retval;
  }

  @Override
  public void setDefault() {

    fields = new ArrayList<>();

    GetLoggingInfoField f1 = new GetLoggingInfoField();
    f1.setFieldName("startOfPipelineDelta");
    f1.setFieldType(GetLoggingInfoTypes.TYPE_SYSTEM_INFO_PIPELINE_DATE_FROM.getCode());
    fields.add(f1);

    GetLoggingInfoField f2 = new GetLoggingInfoField();
    f2.setFieldName("endOfPipelineDelta");
    f2.setFieldType(GetLoggingInfoTypes.TYPE_SYSTEM_INFO_PIPELINE_DATE_TO.getCode());
    fields.add(f2);

    GetLoggingInfoField f3 = new GetLoggingInfoField();
    f3.setFieldName("startOfWorkflowDelta");
    f3.setFieldType(GetLoggingInfoTypes.TYPE_SYSTEM_INFO_WORKFLOW_DATE_TO.getCode());
    fields.add(f3);

    GetLoggingInfoField f4 = new GetLoggingInfoField();
    f4.setFieldName("endOfWorkflowDelta");
    f4.setFieldType(GetLoggingInfoTypes.TYPE_SYSTEM_INFO_WORKFLOW_DATE_TO.getCode());
    fields.add(f4);
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
    for (int i = 0; i < fields.size(); i++) {
      IValueMeta v;

      switch (GetLoggingInfoTypes.getTypeFromString(fields.get(i).getFieldType())) {
        case TYPE_SYSTEM_INFO_PIPELINE_DATE_FROM,
            TYPE_SYSTEM_INFO_PIPELINE_DATE_TO,
            TYPE_SYSTEM_INFO_WORKFLOW_DATE_FROM,
            TYPE_SYSTEM_INFO_WORKFLOW_DATE_TO,
            TYPE_SYSTEM_INFO_PIPELINE_PREVIOUS_EXECUTION_DATE,
            TYPE_SYSTEM_INFO_PIPELINE_PREVIOUS_SUCCESS_DATE,
            TYPE_SYSTEM_INFO_WORKFLOW_PREVIOUS_EXECUTION_DATE,
            TYPE_SYSTEM_INFO_WORKFLOW_PREVIOUS_SUCCESS_DATE:
          v = new ValueMetaDate(fields.get(i).getFieldName());
          break;
        default:
          v = new ValueMetaNone(fields.get(i).getFieldName());
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
      IVariables space,
      IHopMetadataProvider metadataProvider) {
    // See if we have input streams leading to this transform!
    int nrRemarks = remarks.size();
    for (int i = 0; i < fields.size(); i++) {
      if (GetLoggingInfoTypes.getTypeFromString(fields.get(i).getFieldType()).ordinal()
          <= GetLoggingInfoTypes.TYPE_SYSTEM_INFO_NONE.ordinal()) {
        CheckResult cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(
                    PKG, "SystemDataMeta.CheckResult.FieldHasNoType", fields.get(i).getFieldName()),
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
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof GetLoggingInfoMeta)) {
      return false;
    }
    GetLoggingInfoMeta that = (GetLoggingInfoMeta) o;

    return fields.equals(that.fields);
  }

  @Override
  public int hashCode() {
    int result = Arrays.asList(fields).hashCode();
    result = 31 * result + Arrays.asList().hashCode();
    return result;
  }
}
