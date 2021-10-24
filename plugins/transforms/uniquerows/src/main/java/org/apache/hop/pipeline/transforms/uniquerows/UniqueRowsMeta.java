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

package org.apache.hop.pipeline.transforms.uniquerows;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

import java.util.ArrayList;
import java.util.List;

/*
 * Unique rows
 */
@Transform(
    id = "Unique",
    image = "uniquerows.svg",
    name = "i18n::UniqueRows.Name",
    description = "i18n::UniqueRows.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Transform",
    keywords = "i18n::UniqueRowsMeta.keyword",
    documentationUrl = "/pipeline/transforms/uniquerows.html")
public class UniqueRowsMeta extends BaseTransformMeta
    implements ITransformMeta<UniqueRows, UniqueRowsData> {
  private static final Class<?> PKG = UniqueRowsMeta.class; // For Translator

  /** Indicate that we want to count the number of doubles */
  @HopMetadataProperty(
      key = "count_rows",
      injectionKeyDescription = "UniqueRowsMeta.Injection.CountRows")
  private boolean countRows;

  /** The fieldname that will contain the number of doubles */
  @HopMetadataProperty(
      key = "count_field",
      injectionKeyDescription = "UniqueRowsMeta.Injection.CountField")
  private String countField;

  /** The fields to compare for double, null means all */
  @HopMetadataProperty(
      groupKey = "fields",
      key = "field",
      injectionGroupDescription = "UniqueRowsMeta.Injection.Fields",
      injectionKeyDescription = "UniqueRowsMeta.Injection.Field")
  private List<UniqueField> compareFields;

  @HopMetadataProperty(
      key = "reject_duplicate_row",
      injectionKeyDescription = "UniqueRowsMeta.Injection.RejectDuplicateRow")
  private boolean rejectDuplicateRow;

  @HopMetadataProperty(
      key = "error_description",
      injectionKeyDescription = "UniqueRowsMeta.Injection.ErrorDescription")
  private String errorDescription;

  public UniqueRowsMeta() {
    super();
    compareFields = new ArrayList<>();
  }

  public UniqueRowsMeta(UniqueRowsMeta meta) {
    super();
    this.countRows = meta.countRows;
    this.countField = meta.countField;
    this.errorDescription = meta.errorDescription;
    this.rejectDuplicateRow = meta.rejectDuplicateRow;
    this.compareFields = new ArrayList<>();
    for (UniqueField field : meta.getCompareFields()) {
      compareFields.add(new UniqueField(field.getName(), field.isCaseInsensitive()));
    }
  }

  /** @return Returns the countRows. */
  public boolean isCountRows() {
    return countRows;
  }

  /** @param countRows The countRows to set. */
  public void setCountRows(boolean countRows) {
    this.countRows = countRows;
  }

  /** @return Returns the countField. */
  public String getCountField() {
    return countField;
  }

  /** @param countField The countField to set. */
  public void setCountField(String countField) {
    this.countField = countField;
  }

  /** @param compareField The compareField to set. */
  public void setCompareFields(List<UniqueField> compareField) {
    this.compareFields = compareField;
  }

  /** @return Returns the compareField. */
  public List<UniqueField> getCompareFields() {
    return compareFields;
  }

  /** @param rejectDuplicateRow The rejectDuplicateRow to set. */
  public void setRejectDuplicateRow(boolean rejectDuplicateRow) {
    this.rejectDuplicateRow = rejectDuplicateRow;
  }

  /** @return Returns the rejectDuplicateRow. */
  public boolean isRejectDuplicateRow() {
    return rejectDuplicateRow;
  }

  /** @return Returns the errorDescription. */
  public String getErrorDescription() {
    return errorDescription;
  }

  /** @param errorDescription The errorDescription to set. */
  public void setErrorDescription(String errorDescription) {
    this.errorDescription = errorDescription;
  }

  @Override
  public Object clone() {
    return new UniqueRowsMeta(this);
  }

  @Override
  public UniqueRows createTransform(
      TransformMeta transformMeta,
      UniqueRowsData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    return new UniqueRows(transformMeta, this, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public void setDefault() {
    countRows = false;
    countField = "";
    rejectDuplicateRow = false;
    errorDescription = null;
    compareFields = new ArrayList<>();
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
    // change the case insensitive flag too
    for (UniqueField field : compareFields) {
      int idx = row.indexOfValue(field.getName());
      if (idx >= 0) {
        row.getValueMeta(idx).setCaseInsensitive(field.isCaseInsensitive());
      }
    }
    if (countRows) {
      IValueMeta v = new ValueMetaInteger(countField);
      v.setLength(IValueMeta.DEFAULT_INTEGER_LENGTH, 0);
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
    CheckResult cr;

    if (input.length > 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "UniqueRowsMeta.CheckResult.TransformReceivingInfoFromOtherTransforms"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(
                  PKG, "UniqueRowsMeta.CheckResult.NoInputReceivedFromOtherTransforms"),
              transformMeta);
      remarks.add(cr);
    }
  }

  @Override
  public UniqueRowsData getTransformData() {
    return new UniqueRowsData();
  }

  @Override
  public boolean supportsErrorHandling() {
    return isRejectDuplicateRow();
  }
}
