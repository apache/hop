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

package org.apache.hop.beam.core.fn;

import lombok.RequiredArgsConstructor;
import org.apache.beam.sdk.io.splunk.SplunkEvent;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.beam.core.BeamHop;
import org.apache.hop.beam.core.HopRow;
import org.apache.hop.core.exception.HopRuntimeException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.pipeline.Pipeline;

/** Converts one Hop field to a Splunk HEC event. Null events fail and are not published. */
@RequiredArgsConstructor
public class HopToSplunkFn extends DoFn<HopRow, SplunkEvent> {
  private final String transformName;
  private final String eventField;
  private final String rowMetaXml;
  private final String host;
  private final String source;
  private final String sourceType;
  private final String index;
  private transient IRowMeta rowMeta;
  private transient int fieldIndex;
  private transient Counter readCounter;
  private transient Counter outputCounter;
  private transient Counter errorCounter;

  @Setup
  public void setup() {
    readCounter = Metrics.counter(Pipeline.METRIC_NAME_READ, transformName);
    outputCounter = Metrics.counter(Pipeline.METRIC_NAME_OUTPUT, transformName);
    errorCounter = Metrics.counter(Pipeline.METRIC_NAME_ERROR, transformName);
    try {
      BeamHop.init();
      rowMeta =
          new RowMeta(
              XmlHandler.getSubNode(XmlHandler.loadXmlString(rowMetaXml), RowMeta.XML_META_TAG));
      fieldIndex = rowMeta.indexOfValue(eventField);
      if (fieldIndex < 0)
        throw new HopRuntimeException("Splunk event field not found: " + eventField);
      Metrics.counter(Pipeline.METRIC_NAME_INIT, transformName).inc();
    } catch (Exception e) {
      errorCounter.inc();
      throw new HopRuntimeException("Unable to initialize Splunk event field: " + eventField, e);
    }
  }

  @ProcessElement
  public void process(@Element HopRow row, OutputReceiver<SplunkEvent> output) {
    readCounter.inc();
    try {
      String event = rowMeta.getString(row.getRow(), fieldIndex);
      if (event == null) throw new HopRuntimeException("Splunk event cannot be null");
      SplunkEvent.Builder builder = SplunkEvent.newBuilder().withEvent(event);
      if (StringUtils.isNotEmpty(host)) builder.withHost(host);
      if (StringUtils.isNotEmpty(source)) builder.withSource(source);
      if (StringUtils.isNotEmpty(sourceType)) builder.withSourceType(sourceType);
      if (StringUtils.isNotEmpty(index)) builder.withIndex(index);
      output.output(builder.create());
      // Handed to SplunkIO. HTTP success is not a Hop callback and is not counted as written.
      outputCounter.inc();
    } catch (Exception e) {
      errorCounter.inc();
      throw new HopRuntimeException("Unable to convert Splunk event field: " + eventField, e);
    }
  }
}
