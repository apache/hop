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

package org.apache.hop.pipeline.transforms.setvariable;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

/** Sets environment variables based on content in certain fields of a single input row. */
@Transform(
    id = "SetVariable",
    image = "setvariable.svg",
    name = "i18n::SetVariable.Name",
    description = "i18n::SetVariable.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Workflow",
    keywords = "i18n::SetVariableMeta.keyword",
    documentationUrl = "/pipeline/transforms/setvariable.html")
public class SetVariableMeta extends BaseTransformMeta<SetVariable, SetVariableData> {
  private static final Class<?> PKG = SetVariableMeta.class;

  @HopMetadataProperty(groupKey = "fields", key = "field")
  List<VariableItem> variables;

  @HopMetadataProperty(key = "use_formatting")
  private boolean usingFormatting;

  public SetVariableMeta() {
    super(); // allocate BaseTransformMeta
    variables = new ArrayList<>();
  }

  public List<VariableItem> getVariables() {
    return variables;
  }

  public void setVariables(List<VariableItem> variables) {
    this.variables = variables;
  }

  @Override
  public void setDefault() {

    usingFormatting = true;
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
              BaseMessages.getString(
                  PKG, "SetVariableMeta.CheckResult.NotReceivingFieldsFromPreviousTransforms"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG,
                  "SetVariableMeta.CheckResult.ReceivingFieldsFromPreviousTransforms",
                  "" + prev.size()),
              transformMeta);
      remarks.add(cr);
    }

    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "SetVariableMeta.CheckResult.ReceivingInfoFromOtherTransforms"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(
                  PKG, "SetVariableMeta.CheckResult.NotReceivingInfoFromOtherTransforms"),
              transformMeta);
      remarks.add(cr);
    }
  }

  /**
   * @return the usingFormatting
   */
  public boolean isUsingFormatting() {
    return usingFormatting;
  }

  /**
   * @param usingFormatting the usingFormatting to set
   */
  public void setUsingFormatting(boolean usingFormatting) {
    this.usingFormatting = usingFormatting;
  }
}
