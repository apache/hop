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

package org.apache.hop.pipeline.transforms.uniquerowsbyhashset;

import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
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

@Transform(
    id = "UniqueRowsByHashSet",
    image = "uniquerowsbyhashset.svg",
    name = "i18n::UniqueRowsByHashSet.Name",
    description = "i18n::UniqueRowsByHashSet.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Transform",
    keywords = "i18n::UniqueRowsByHashSetMeta.keyword",
    documentationUrl = "/pipeline/transforms/uniquerowsbyhashset.html")
@Getter
@Setter
public class UniqueRowsByHashSetMeta
    extends BaseTransformMeta<UniqueRowsByHashSet, UniqueRowsByHashSetData> {
  private static final Class<?> PKG = UniqueRowsByHashSetMeta.class;

  @Getter
  @Setter
  public static class CompareField {
    @HopMetadataProperty(
        key = "name",
        injectionKey = "COMPARE_FIELD_NAME",
        injectionKeyDescription = "UniqueRowsByHashSetMeta.Injection.COMPARE_FIELD_NAME")
    private String name;

    public CompareField() {}

    public CompareField(CompareField f) {
      this();
      this.name = f.name;
    }

    public CompareField(String name) {
      this.name = name;
    }
  }

  /**
   * Whether to compare strictly by hash value or to store the row values for strict equality
   * checking
   */
  @HopMetadataProperty(
      key = "store_values",
      injectionKey = "STORE_VALUES",
      injectionKeyDescription = "UniqueRowsByHashSetMeta.Injection.STORE_VALUES")
  private boolean storeValues;

  /** The fields to compare for duplicates, null means all */
  @HopMetadataProperty(
      key = "field",
      groupKey = "fields",
      injectionKey = "COMPARE_FIELD",
      injectionGroupKey = "COMPARE_FIELDS",
      injectionKeyDescription = "UniqueRowsByHashSetMeta.Injection.COMPARE_FIELD",
      injectionGroupDescription = "UniqueRowsByHashSetMeta.Injection.COMPARE_FIELDS")
  private List<CompareField> compareFields;

  @HopMetadataProperty(
      key = "reject_duplicate_row",
      injectionKey = "REJECT_DUPLICATE_ROW",
      injectionKeyDescription = "UniqueRowsByHashSetMeta.Injection.REJECT_DUPLICATE_ROW")
  private boolean rejectDuplicateRow;

  @HopMetadataProperty(
      key = "error_description",
      injectionKey = "ERROR_DESCRIPTION",
      injectionKeyDescription = "UniqueRowsByHashSetMeta.Injection.ERROR_DESCRIPTION")
  private String errorDescription;

  public UniqueRowsByHashSetMeta() {
    super();
    compareFields = new ArrayList<>();
    rejectDuplicateRow = false;
    errorDescription = null;
    storeValues = true;
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

    if (input.length > 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG,
                  "UniqueRowsByHashSetMeta.CheckResult.TransformReceivingInfoFromOtherTransforms"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(
                  PKG, "UniqueRowsByHashSetMeta.CheckResult.NoInputReceivedFromOtherTransforms"),
              transformMeta);
      remarks.add(cr);
    }
  }

  @Override
  public boolean supportsErrorHandling() {
    return isRejectDuplicateRow();
  }
}
