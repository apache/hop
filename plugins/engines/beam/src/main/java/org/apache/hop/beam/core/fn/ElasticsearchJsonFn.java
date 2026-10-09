/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hop.beam.core.fn;

import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.hop.beam.core.BeamHop;
import org.apache.hop.beam.core.HopRow;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.pipeline.Pipeline;

/** Extracts the configured String field using Hop's value conversion. */
public class ElasticsearchJsonFn extends DoFn<HopRow, String> {
  private final String transformName;
  private final String jsonField;
  private final String rowMetaXml;
  private transient IRowMeta rowMeta;
  private transient int fieldIndex;

  public ElasticsearchJsonFn(String transformName, String jsonField, String rowMetaXml) {
    this.transformName = transformName;
    this.jsonField = jsonField;
    this.rowMetaXml = rowMetaXml;
  }

  @Setup
  public void setup() throws Exception {
    BeamHop.init();
    // Unlike JsonRowMeta, XML retains lazy storage metadata and its character encoding.
    rowMeta =
        new RowMeta(
            XmlHandler.getSubNode(XmlHandler.loadXmlString(rowMetaXml), RowMeta.XML_META_TAG));
    fieldIndex = rowMeta.indexOfValue(jsonField);
    if (fieldIndex < 0 || !rowMeta.getValueMeta(fieldIndex).isString()) {
      throw new HopException("JSON field must exist and have type String: " + jsonField);
    }
    Metrics.counter(Pipeline.METRIC_NAME_INIT, transformName).inc();
  }

  public String extractJson(HopRow row) throws HopException {
    if (row == null || row.getRow() == null || fieldIndex >= row.getRow().length) {
      throw new HopException("Missing JSON value in field: " + jsonField);
    }
    String value;
    try {
      value = rowMeta.getString(row.getRow(), fieldIndex);
    } catch (Exception e) {
      throw new HopException("Unable to convert JSON field: " + jsonField);
    }
    if (value == null || value.isBlank()) {
      throw new HopException("JSON value is required in field: " + jsonField);
    }
    return org.apache.hop.beam.transforms.elasticsearch.BeamElasticsearchConfig.compactJsonObject(
        value, "JSON field " + jsonField);
  }

  @ProcessElement
  public void processElement(@Element HopRow row, OutputReceiver<String> output) throws Exception {
    Metrics.counter(Pipeline.METRIC_NAME_READ, transformName).inc();
    try {
      output.output(extractJson(row));
    } catch (Exception e) {
      Metrics.counter(Pipeline.METRIC_NAME_ERROR, transformName).inc();
      throw e;
    }
    Metrics.counter(Pipeline.METRIC_NAME_OUTPUT, transformName).inc();
  }
}
