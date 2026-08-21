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

package org.apache.hop.pipeline.transforms.rowsfromresult;

import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

@Transform(
    id = "RowsFromResult",
    image = "rowsfromresult.svg",
    name = "i18n::RowsFromResult.Name",
    description = "i18n::RowsFromResult.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Workflow",
    keywords = "i18n::RowsFromResultMeta.keyword",
    documentationUrl = "/pipeline/transforms/getrowsfromresult.html")
@Getter
@Setter
public class RowsFromResultMeta extends BaseTransformMeta<RowsFromResult, RowsFromResultData> {
  private static final Class<?> PKG = RowsFromResult.class;

  @Getter
  @Setter
  public static class ResultRowField {
    @HopMetadataProperty(
        key = "name",
        injectionKey = "FIELD_NAME",
        injectionKeyDescription = "RowsFromResultMeta.Injection.FIELD_NAME")
    private String name;

    @HopMetadataProperty(
        key = "type",
        intCodeConverter = ValueMetaBase.ValueTypeCodeConverter.class,
        injectionKey = "FIELD_TYPE",
        injectionKeyDescription = "RowsFromResultMeta.Injection.FIELD_TYPE")
    private int hopType;

    @HopMetadataProperty(
        key = "length",
        injectionKey = "FIELD_LENGTH",
        injectionKeyDescription = "RowsFromResultMeta.Injection.FIELD_LENGTH")
    private int length;

    @HopMetadataProperty(
        key = "precision",
        injectionKey = "FIELD_PRECISION",
        injectionKeyDescription = "RowsFromResultMeta.Injection.FIELD_PRECISION")
    private int precision;

    public ResultRowField() {}

    public ResultRowField(ResultRowField f) {
      this();
      this.name = f.name;
      this.hopType = f.hopType;
      this.length = f.length;
      this.precision = f.precision;
    }
  }

  @HopMetadataProperty(
      key = "field",
      groupKey = "fields",
      injectionKey = "FIELD",
      injectionGroupKey = "FIELDS",
      injectionKeyDescription = "RowsFromResultMeta.Injection.FIELD",
      injectionGroupDescription = "RowsFromResultMeta.Injection.FIELDS")
  private List<ResultRowField> resultFields;

  public RowsFromResultMeta() {
    super();
    this.resultFields = new ArrayList<>();
  }

  @Override
  public void getFields(
      IRowMeta r,
      String origin,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {
    for (ResultRowField field : resultFields) {
      IValueMeta v;
      try {
        v =
            ValueMetaFactory.createValueMeta(
                field.getName(), field.getHopType(), field.getLength(), field.getPrecision());
        v.setOrigin(origin);
        r.addValueMeta(v);
      } catch (HopPluginException e) {
        throw new HopTransformException(e);
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
    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      CheckResult cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(
                  PKG,
                  "RowsFromResultMeta.CheckResult.TransformExpectingNoReadingInfoFromOtherTransforms"),
              transformMeta);
      remarks.add(cr);
    } else {
      CheckResult cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "RowsFromResultMeta.CheckResult.NoInputReceivedError"),
              transformMeta);
      remarks.add(cr);
    }
  }
}
