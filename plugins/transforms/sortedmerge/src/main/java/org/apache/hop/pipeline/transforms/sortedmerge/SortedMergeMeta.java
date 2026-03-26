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

package org.apache.hop.pipeline.transforms.sortedmerge;

import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.PipelineMeta.PipelineType;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

@Transform(
    id = "SortedMerge",
    image = "sortedmerge.svg",
    name = "i18n::SortedMerge.Name",
    description = "i18n::SortedMerge.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Transform",
    keywords = "i18n::SortedMergeMeta.keyword",
    documentationUrl = "/pipeline/transforms/sortedmerge.html")
@Getter
@Setter
public class SortedMergeMeta extends BaseTransformMeta<SortedMerge, SortedMergeData> {
  private static final Class<?> PKG = SortedMergeMeta.class;
  public static final String CONST_FIELD = "field";

  @HopMetadataProperty(
      key = "field",
      groupKey = "fields",
      injectionGroupKey = "FIELDS",
      injectionGroupDescription = "SortedMerge.Injection.FIELDS")
  private List<MergeField> mergeFields;

  public SortedMergeMeta() {
    mergeFields = new ArrayList<>();
  }

  public SortedMergeMeta(SortedMergeMeta m) {
    this();
    m.mergeFields.forEach(f -> this.mergeFields.add(new MergeField(f)));
  }

  @Override
  public Object clone() {
    return new SortedMergeMeta(this);
  }

  @Override
  public void getFields(
      IRowMeta inputRowMeta,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {
    // Set the sorted properties: ascending/descending
    for (MergeField field : mergeFields) {
      int idx = inputRowMeta.indexOfValue(field.getFieldName());
      if (idx >= 0) {
        IValueMeta valueMeta = inputRowMeta.getValueMeta(idx);
        valueMeta.setSortedDescending(field.isAscending());
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
    CheckResult cr;

    if (prev != null && !prev.isEmpty()) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "SortedMergeMeta.CheckResult.FieldsReceived", "" + prev.size()),
              transformMeta);
      remarks.add(cr);

      String errorMessage = "";
      boolean errorFound = false;

      // Starting from selected fields in ...
      for (MergeField field : mergeFields) {
        int idx = prev.indexOfValue(field.getFieldName());
        if (idx < 0) {
          errorMessage += "\t\t" + field.getFieldName() + Const.CR;
          errorFound = true;
        }
      }
      if (errorFound) {
        errorMessage =
            BaseMessages.getString(
                PKG, "SortedMergeMeta.CheckResult.SortKeysNotFound", errorMessage);

        cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
        remarks.add(cr);
      } else {
        if (!mergeFields.isEmpty()) {
          cr =
              new CheckResult(
                  ICheckResult.TYPE_RESULT_OK,
                  BaseMessages.getString(PKG, "SortedMergeMeta.CheckResult.AllSortKeysFound"),
                  transformMeta);
          remarks.add(cr);
        } else {
          cr =
              new CheckResult(
                  ICheckResult.TYPE_RESULT_ERROR,
                  BaseMessages.getString(PKG, "SortedMergeMeta.CheckResult.NoSortKeysEntered"),
                  transformMeta);
          remarks.add(cr);
        }
      }
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "SortedMergeMeta.CheckResult.NoFields"),
              transformMeta);
      remarks.add(cr);
    }

    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "SortedMergeMeta.CheckResult.ExpectedInputOk"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "SortedMergeMeta.CheckResult.ExpectedInputError"),
              transformMeta);
      remarks.add(cr);
    }
  }

  @Override
  public PipelineType[] getSupportedPipelineTypes() {
    return new PipelineType[] {
      PipelineType.Normal,
    };
  }

  @Getter
  @Setter
  public static class MergeField {
    @HopMetadataProperty(
        key = "name",
        injectionKey = "FIELD_NAME",
        injectionKeyDescription = "SortedMerge.Injection.FIELD_NAME")
    private String fieldName;

    @HopMetadataProperty(
        key = "ascending",
        injectionKey = "ASCENDING",
        injectionKeyDescription = "SortedMerge.Injection.ASCENDING")
    private boolean ascending;

    public MergeField() {}

    public MergeField(MergeField f) {
      this();
      this.ascending = f.ascending;
      this.fieldName = f.fieldName;
    }

    public MergeField(String fieldName, boolean ascending) {
      this();
      this.ascending = ascending;
      this.fieldName = fieldName;
    }
  }
}
