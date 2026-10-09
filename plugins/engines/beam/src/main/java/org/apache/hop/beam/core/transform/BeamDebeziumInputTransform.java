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

package org.apache.hop.beam.core.transform;

import org.apache.beam.io.debezium.DebeziumIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hop.beam.core.BeamHop;
import org.apache.hop.beam.core.HopRow;
import org.apache.hop.beam.core.coder.HopRowCoder;
import org.apache.hop.core.exception.HopRuntimeException;
import org.apache.hop.pipeline.Pipeline;

/** Debezium's real JSON source followed by a single-string Hop row conversion. */
public class BeamDebeziumInputTransform extends PTransform<PBegin, PCollection<HopRow>> {
  private final String transformName;
  private final DebeziumIO.Read<String> read;

  public BeamDebeziumInputTransform(String transformName, DebeziumIO.Read<String> read) {
    super(transformName);
    this.transformName = transformName;
    this.read = read;
  }

  @Override
  public PCollection<HopRow> expand(PBegin input) {
    return input
        .apply("Debezium CDC", read)
        .apply("JSON to Hop row", ParDo.of(new JsonToHopRowFn(transformName)))
        .setCoder(new HopRowCoder());
  }

  public static class JsonToHopRowFn extends DoFn<String, HopRow> {
    private final String transformName;
    private transient Counter inputCounter;
    private transient Counter writtenCounter;

    public JsonToHopRowFn(String transformName) {
      this.transformName = transformName;
    }

    @Setup
    public void setup() {
      try {
        BeamHop.init();
        inputCounter = Metrics.counter(Pipeline.METRIC_NAME_INPUT, transformName);
        writtenCounter = Metrics.counter(Pipeline.METRIC_NAME_WRITTEN, transformName);
        Metrics.counter(Pipeline.METRIC_NAME_INIT, transformName).inc();
      } catch (Exception e) {
        Metrics.counter(Pipeline.METRIC_NAME_ERROR, transformName).inc();
        throw new HopRuntimeException("Unable to initialize Debezium JSON row conversion", e);
      }
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
      inputCounter.inc();
      context.output(new HopRow(new Object[] {context.element()}));
      writtenCounter.inc();
    }
  }
}
