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
 *
 */
package org.apache.hop.pipeline.transforms.googleanalytics;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

@Transform(
    id = "GoogleAnalytics",
    image = "google-analytics.svg",
    name = "i18n::BaseTransform.TypeLongDesc.GoogleAnalytics",
    description = "i18n::BaseTransform.TypeTooltipDesc.GoogleAnalytics",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Input",
    documentationUrl = "pipeline/transforms/google-analytics.html")
public class GoogleAnalyticsMeta extends BaseTransformMeta<GoogleAnalytics, GoogleAnalyticsData> {

  public static final String[] TYPE_SAMPLING_LEVEL_CODE =
      new String[] {"DEFAULT", "FASTER", "HIGHER_PRECISION"};
  public static final String FIELD_TYPE_DIMENSION = "Dimension";
  public static final String FIELD_TYPE_METRIC = "Metric";
  public static final String FIELD_TYPE_DATA_SOURCE_PROPERTY = "Data Source Property";
  public static final String FIELD_TYPE_DATA_SOURCE_FIELD = "Data Source Field";

  @HopMetadataProperty(key = "oauth_service_account", injectionKeyDescription = "")
  private String oAuthServiceAccount;

  @HopMetadataProperty(key = "oauth_key_file", injectionKeyDescription = "")
  private String oAuthKeyFile;

  @HopMetadataProperty(key = "app_name", injectionKeyDescription = "")
  private String gaAppName;

  @HopMetadataProperty(key = "ga_property_id", injectionKeyDescription = "")
  private String gaProperty;

  @HopMetadataProperty(key = "start_date", injectionKeyDescription = "")
  private String startDate;

  @HopMetadataProperty(key = "end_date", injectionKeyDescription = "")
  private String endDate;

  @HopMetadataProperty(key = "dimensions", injectionKeyDescription = "")
  private String dimensions;

  @HopMetadataProperty(key = "metrics", injectionKeyDescription = "")
  private String metrics;

  @HopMetadataProperty(key = "order_by", injectionKeyDescription = "")
  private String sort;

  @HopMetadataProperty(key = "row_limit", injectionKeyDescription = "")
  private int rowLimit;

  @HopMetadataProperty(
      groupKey = "fields",
      key = "field",
      injectionGroupKey = "FIELDS",
      injectionKeyDescription = "")
  private List<GoogleAnalyticsField> googleAnalyticsFields;

  public GoogleAnalyticsMeta() {
    this.googleAnalyticsFields = new ArrayList<>();
  }

  public String getOAuthServiceAccount() {
    return oAuthServiceAccount;
  }

  public void setOAuthServiceAccount(String oAuthServiceAccount) {
    this.oAuthServiceAccount = oAuthServiceAccount;
  }

  public String getOAuthKeyFile() {
    return oAuthKeyFile;
  }

  public void setOAuthKeyFile(String oAuthKeyFile) {
    this.oAuthKeyFile = oAuthKeyFile;
  }

  public String getGaAppName() {
    return gaAppName;
  }

  public void setGaAppName(String gaAppName) {
    this.gaAppName = gaAppName;
  }

  public String getGaProperty() {
    return gaProperty;
  }

  public void setGaProperty(String gaProperty) {
    this.gaProperty = gaProperty;
  }

  public String getStartDate() {
    return startDate;
  }

  public void setStartDate(String startDate) {
    this.startDate = startDate;
  }

  public String getEndDate() {
    return endDate;
  }

  public void setEndDate(String endDate) {
    this.endDate = endDate;
  }

  public String getDimensions() {
    return dimensions;
  }

  public void setDimensions(String dimensions) {
    this.dimensions = dimensions;
  }

  public String getMetrics() {
    return metrics;
  }

  public void setMetrics(String metrics) {
    this.metrics = metrics;
  }

  public String getSort() {
    return sort;
  }

  public void setSort(String sort) {
    this.sort = sort;
  }

  public int getRowLimit() {
    return rowLimit;
  }

  public void setRowLimit(int rowLimit) {
    this.rowLimit = rowLimit;
  }

  public List<GoogleAnalyticsField> getGoogleAnalyticsFields() {
    return googleAnalyticsFields;
  }

  public void setGoogleAnalyticsFields(List<GoogleAnalyticsField> googleAnalyticsFields) {
    this.googleAnalyticsFields = googleAnalyticsFields;
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
    for (GoogleAnalyticsField googleAnalyticsField : googleAnalyticsFields) {
      try {
        if (!Utils.isEmpty(googleAnalyticsField.getOutputFieldName())) {
          int type = ValueMetaFactory.getIdForValueMeta(googleAnalyticsField.getType());
          if (type == IValueMeta.TYPE_NONE) {
            type = IValueMeta.TYPE_STRING;
          }
          IValueMeta v =
              ValueMetaFactory.createValueMeta(googleAnalyticsField.getOutputFieldName(), type);
          inputRowMeta.addValueMeta(v);
        }
      } catch (Exception e) {
        throw new HopTransformException(
            "Unable to create value of type " + googleAnalyticsField.getType(), e);
      }
    }
  }
}
