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

package org.apache.hop.pipeline.transforms.analyticquery;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.PipelineMeta.PipelineType;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

import java.util.ArrayList;
import java.util.List;

/**
 * @author ngoodman
 * @since 27-jan-2009
 */
@Transform(
    id = "AnalyticQuery",
    image = "analyticquery.svg",
    name = "i18n::AnalyticQuery.Name",
    description = "i18n::AnalyticQuery.Description",
    categoryDescription =
        "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Statistics",
    documentationUrl =
        "https://hop.apache.org/manual/latest/pipeline/transforms/analyticquery.html")
public class AnalyticQueryMeta extends BaseTransformMeta<AnalyticQuery, AnalyticQueryData> {

  private static final Class<?> PKG = AnalyticQuery.class; // For Translator

  /** Fields to partition by ie, CUSTOMER, PRODUCT */
  @HopMetadataProperty(groupKey = "group", key = "field", injectionGroupKey = "group")
  private List<GroupField> groupFields;

  @HopMetadataProperty(groupKey = "fields", key = "field")
  private List<QueryField> queryFields;

  public AnalyticQueryMeta() {
    groupFields = new ArrayList<>();
    queryFields = new ArrayList<>();
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
    // re-assemble a new row of metadata
    //
    IRowMeta fields = new RowMeta();

    // Add existing values
    fields.addRowMeta(r);

    // add analytic values
    for (QueryField queryField : queryFields) {

      int indexOfSubject = r.indexOfValue(queryField.getSubjectField());

      // if we found the subjectField in the IRowMeta, and we should....
      if (indexOfSubject > -1) {
        IValueMeta vmi = r.getValueMeta(indexOfSubject).clone();
        vmi.setOrigin(origin);
        vmi.setName(queryField.getAggregateField());
        fields.addValueMeta(vmi);
      } else {
        // we have a condition where the subjectField can't be found from the iRowMeta
        StringBuilder sbFieldNames = new StringBuilder();
        String[] fieldNames = r.getFieldNames();
        for (int j = 0; j < fieldNames.length; j++) {
          sbFieldNames
              .append("[")
              .append(fieldNames[j])
              .append("]")
              .append(j < fieldNames.length - 1 ? ", " : "");
        }
        throw new HopTransformException(
            BaseMessages.getString(
                PKG,
                "AnalyticQueryMeta.Exception.SubjectFieldNotFound",
                getParentTransformMeta().getName(),
                queryField.getSubjectField(),
                sbFieldNames.toString()));
      }
    }

    r.clear();
    // Add back to Row Meta
    r.addRowMeta(fields);
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
              BaseMessages.getString(PKG, "AnalyticQueryMeta.CheckResult.ReceivingInfoOK"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "AnalyticQueryMeta.CheckResult.NoInputError"),
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

  /**
   * Gets groupFields
   *
   * @return value of groupFields
   */
  public List<GroupField> getGroupFields() {
    return groupFields;
  }

  /** @param groupFields The groupFields to set */
  public void setGroupFields(List<GroupField> groupFields) {
    this.groupFields = groupFields;
  }

  /**
   * Gets queryFields
   *
   * @return value of queryFields
   */
  public List<QueryField> getQueryFields() {
    return queryFields;
  }

  /** @param queryFields The queryFields to set */
  public void setQueryFields(List<QueryField> queryFields) {
    this.queryFields = queryFields;
  }
}
