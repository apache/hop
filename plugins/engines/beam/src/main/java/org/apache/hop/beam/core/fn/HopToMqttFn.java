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

import java.nio.charset.StandardCharsets;
import lombok.RequiredArgsConstructor;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.hop.beam.core.BeamHop;
import org.apache.hop.beam.core.HopRow;
import org.apache.hop.core.exception.HopRuntimeException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.pipeline.Pipeline;

/** Converts the selected Hop value to MQTT bytes. Null payloads fail, never silently disappear. */
@RequiredArgsConstructor
public class HopToMqttFn extends DoFn<HopRow, byte[]> {
  private final String transformName;
  private final String payloadField;
  private final String payloadType;
  private final String rowMetaXml;
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
      // JsonRowMeta drops storage type, encoding and indexed values. XML keeps them.
      rowMeta =
          new RowMeta(
              XmlHandler.getSubNode(XmlHandler.loadXmlString(rowMetaXml), RowMeta.XML_META_TAG));
      fieldIndex = rowMeta.indexOfValue(payloadField);
      if (fieldIndex < 0)
        throw new HopRuntimeException("MQTT payload field not found: " + payloadField);
      if ("Binary".equalsIgnoreCase(payloadType)
          && rowMeta.getValueMeta(fieldIndex).getType() != IValueMeta.TYPE_BINARY)
        throw new HopRuntimeException(
            "MQTT binary payload requires a Binary field: " + payloadField);
      Metrics.counter(Pipeline.METRIC_NAME_INIT, transformName).inc();
    } catch (Exception e) {
      errorCounter.inc();
      throw new HopRuntimeException("Unable to initialize MQTT payload field: " + payloadField, e);
    }
  }

  @ProcessElement
  public void process(@Element HopRow row, OutputReceiver<byte[]> output) {
    readCounter.inc();
    try {
      Object value = row.getRow()[fieldIndex];
      if (value == null) throw new HopRuntimeException("MQTT payload cannot be null");
      byte[] bytes =
          "Binary".equalsIgnoreCase(payloadType)
              ? rowMeta.getValueMeta(fieldIndex).getBinary(value)
              : rowMeta.getString(row.getRow(), fieldIndex).getBytes(StandardCharsets.UTF_8);
      output.output(bytes);
      // Payload bytes handed to MqttIO. The QoS 1 PUBACK happens later inside Beam and is not
      // counted.
      outputCounter.inc();
    } catch (Exception e) {
      errorCounter.inc();
      throw new HopRuntimeException("Unable to convert MQTT payload field: " + payloadField, e);
    }
  }
}
