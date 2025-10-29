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

import com.google.analytics.data.v1beta.BetaAnalyticsDataClient;
import com.google.analytics.data.v1beta.BetaAnalyticsDataSettings;
import com.google.analytics.data.v1beta.DateRange;
import com.google.analytics.data.v1beta.Dimension;
import com.google.analytics.data.v1beta.DimensionHeader;
import com.google.analytics.data.v1beta.Metric;
import com.google.analytics.data.v1beta.MetricHeader;
import com.google.analytics.data.v1beta.MetricType;
import com.google.analytics.data.v1beta.Row;
import com.google.analytics.data.v1beta.RunReportRequest;
import com.google.analytics.data.v1beta.RunReportResponse;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.Credentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.TransformStatus;

public class GoogleAnalytics extends BaseTransform<GoogleAnalyticsMeta, GoogleAnalyticsData> {

  private BetaAnalyticsDataClient analyticsData;

  private List<Dimension> dimensionList;
  private List<Metric> metricList;
  private InputStream inputStream;
  private int requestOffset = 0;
  private int requestRowSize = 100000;

  private int rowLimit;

  public GoogleAnalytics(
      TransformMeta transformMeta,
      GoogleAnalyticsMeta meta,
      GoogleAnalyticsData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public boolean init() {

    if (super.init()) {
      try {
        if (meta.getRowLimit() == 0) {
          rowLimit = Integer.MAX_VALUE;
        } else {
          rowLimit = meta.getRowLimit();
        }
        inputStream = new FileInputStream(meta.getOAuthKeyFile());
        Credentials credentials = ServiceAccountCredentials.fromStream(inputStream);

        BetaAnalyticsDataSettings settings =
            BetaAnalyticsDataSettings.newHttpJsonBuilder()
                .setCredentialsProvider(FixedCredentialsProvider.create(credentials))
                .build();
        analyticsData = BetaAnalyticsDataClient.create(settings);

        dimensionList = new ArrayList<>();
        String dimensionString = meta.getDimensions();
        for (String dimension : dimensionString.split(",")) {
          dimensionList.add(Dimension.newBuilder().setName(dimension).build());
        }
        metricList = new ArrayList<>();
        String metricsString = meta.getMetrics();
        for (String metric : metricsString.split(",")) {
          metricList.add(Metric.newBuilder().setName(metric).build());
        }

        if (rowLimit < requestRowSize) {
          requestRowSize = rowLimit;
        }
      } catch (IOException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
      return true;
    }
    return false;
  }

  private RunReportRequest getRequest() {

    return RunReportRequest.newBuilder()
        .setProperty("properties/" + meta.getGaProperty())
        .addAllDimensions(dimensionList)
        .addAllMetrics(metricList)
        .addDateRanges(
            DateRange.newBuilder().setStartDate(meta.getStartDate()).setEndDate(meta.getEndDate()))
        .setLimit(requestRowSize)
        .setOffset(requestOffset)
        .build();
  }

  @Override
  public boolean processRow() throws HopException {

    if (first) {
      first = false;

      data.outputMeta = new RowMeta();
      meta.getFields(data.outputMeta, getTransformName(), null, null, this, metadataProvider);

      readResponse();
    }
    logBasic("all rows processed. ready to shut down.");
    setOutputDone();
    return false;
  }

  private void readResponse() {
    List<DimensionHeader> dimensionHeaders;
    RunReportResponse response = analyticsData.runReport(getRequest());
    dimensionHeaders = response.getDimensionHeadersList();
    List<MetricHeader> metricHeaders = response.getMetricHeadersList();

    if (!response.getRowsList().isEmpty()) {
      try {
        for (Row gaRow : response.getRowsList()) {
          Object[] newRow = RowDataUtil.allocateRowData(meta.getGoogleAnalyticsFields().size());

          for (int i = 0; i < meta.getGoogleAnalyticsFields().size(); i++) {
            GoogleAnalyticsField field = meta.getGoogleAnalyticsFields().get(i);
            String fieldName = field.getFeedField();
            String type = field.getFeedFieldType();
            if (type.equals(GoogleAnalyticsMeta.FIELD_TYPE_DIMENSION)) {
              for (int j = 0; j < dimensionHeaders.size(); j++) {
                if (dimensionHeaders.get(j).getName().equals(fieldName)) {
                  newRow[i] = gaRow.getDimensionValues(j).getValue();
                }
              }
            } else if (type.equals(GoogleAnalyticsMeta.FIELD_TYPE_METRIC)) {
              for (int j = 0; j < metricHeaders.size(); j++) {
                if (metricHeaders.get(j).getName().equals(fieldName)) {
                  MetricType metricType = metricHeaders.get(j).getType();
                  switch (metricType) {
                    case TYPE_INTEGER:
                      IValueMeta valueMetaInt = new ValueMetaInteger("int");
                      IValueMeta gaValueMetaInt =
                          data.outputMeta.getValueMeta(dimensionList.size() + j);
                      Long longValue = Long.valueOf(gaRow.getMetricValues(j).getValue());
                      newRow[i] = gaValueMetaInt.convertData(valueMetaInt, longValue);
                      break;
                    case TYPE_FLOAT,
                        TYPE_SECONDS,
                        TYPE_MILLISECONDS,
                        TYPE_MINUTES,
                        TYPE_HOURS,
                        TYPE_STANDARD,
                        TYPE_CURRENCY,
                        TYPE_FEET,
                        TYPE_MILES,
                        TYPE_METERS,
                        TYPE_KILOMETERS:
                      IValueMeta valueMetaNumber = new ValueMetaNumber("num");
                      IValueMeta gaValueMetaNumber =
                          data.outputMeta.getValueMeta(dimensionList.size() + j);
                      Double doubleValue = Double.valueOf(gaRow.getMetricValues(j).getValue());
                      newRow[i] = gaValueMetaNumber.convertData(valueMetaNumber, doubleValue);
                      break;
                    default:
                      newRow[i] = gaRow.getMetricValues(j).getValue();
                      break;
                  }
                }
              }
            }
          }
          putRow(data.outputMeta, newRow);
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
      // check if we're still below the row limit, adjust REQUEST_ROW_SIZE if we're getting close.
      int rowsProcessed = (int) getLinesWritten();
      if (getLinesWritten() + requestRowSize > rowLimit) {
        requestRowSize = rowLimit - (int) getLinesWritten();
      }
      requestOffset = (int) getLinesWritten();
      if (rowsProcessed < rowLimit) {
        readResponse();
      }
    }
  }

  @Override
  public Collection<TransformStatus> subStatuses() {
    return super.subStatuses();
  }

  @Override
  public void dispose() {
    try {
      inputStream.close();
    } catch (IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
    analyticsData.close();
    super.dispose();
  }
}
