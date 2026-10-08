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
import org.apache.hop.beam.core.HopRow;
import org.apache.hop.core.exception.HopRuntimeException;
import org.apache.hop.pipeline.Pipeline;

/** Converts one MQTT payload to one Hop row, without exposing payloads in logs. */
@RequiredArgsConstructor
public class MqttToHopFn extends DoFn<byte[], HopRow> {
  private final String transformName;
  private final String payloadType;
  private transient Counter inputCounter;
  private transient Counter writtenCounter;
  private transient Counter errorCounter;

  @Setup
  public void setup() {
    inputCounter = Metrics.counter(Pipeline.METRIC_NAME_INPUT, transformName);
    writtenCounter = Metrics.counter(Pipeline.METRIC_NAME_WRITTEN, transformName);
    errorCounter = Metrics.counter(Pipeline.METRIC_NAME_ERROR, transformName);
    Metrics.counter(Pipeline.METRIC_NAME_INIT, transformName).inc();
  }

  @ProcessElement
  public void process(@Element byte[] payload, OutputReceiver<HopRow> output) {
    inputCounter.inc();
    try {
      Object value =
          "Binary".equalsIgnoreCase(payloadType)
              ? payload
              : new String(payload, StandardCharsets.UTF_8);
      output.output(new HopRow(new Object[] {value}));
      writtenCounter.inc();
    } catch (Exception e) {
      errorCounter.inc();
      throw new HopRuntimeException("Unable to convert MQTT payload", e);
    }
  }
}
