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

package org.apache.hop.pipeline.transforms.nullif;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

@Transform(
    id = "NullIf",
    image = "nullif.svg",
    name = "i18n::NullIf.Name",
    description = "i18n::NullIf.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Transform",
    keywords = "i18n::NullIfMeta.keyword",
    documentationUrl = "/pipeline/transforms/nullif.html")
public class NullIfMeta extends BaseTransformMeta<NullIf, NullIfData> {

  private static final Class<?> PKG = NullIfMeta.class;

  @HopMetadataProperty(
      groupKey = "fields",
      key = "field",
      injectionGroupKey = "FIELDS",
      injectionGroupDescription = "NullIf.Injection.FIELDS")
  private List<NullIfField> fields;

  public NullIfMeta() {
    super();
    this.fields = new ArrayList<>();
  }

  public NullIfMeta(NullIfMeta meta) {
    this();
    for (NullIfField field : meta.fields) {
      fields.add(new NullIfField(field.getName(), field.getValue()));
    }
  }

  public List<NullIfField> getFields() {
    return fields;
  }

  public void setFields(List<NullIfField> fields) {
    this.fields = fields;
  }

  @Override
  public Object clone() {
    return new NullIfMeta(this);
  }

  @Override
  public void getFields(
      IRowMeta r,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {
    if (r == null) {
      r = new RowMeta(); // give back values
      // Meta-data doesn't change here, only the value possibly turns to NULL
    }

    return;
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
    if (prev == null || prev.isEmpty()) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_WARNING,
              BaseMessages.getString(PKG, "NullIfMeta.CheckResult.NoReceivingFieldsError"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "NullIfMeta.CheckResult.TransformReceivingFieldsOK", prev.size() + ""),
              transformMeta);
      remarks.add(cr);
    }

    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "NullIfMeta.CheckResult.TransformRecevingInfoFromOtherTransforms"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "NullIfMeta.CheckResult.NoInputReceivedError"),
              transformMeta);
      remarks.add(cr);
    }
  }
}
