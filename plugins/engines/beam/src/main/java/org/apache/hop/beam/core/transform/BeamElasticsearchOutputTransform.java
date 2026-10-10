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
package org.apache.hop.beam.core.transform;

import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PDone;
import org.apache.hop.beam.core.HopRow;
import org.apache.hop.beam.core.fn.ElasticsearchJsonFn;

/** JSON document sink backed by Beam's bulk writer. */
public class BeamElasticsearchOutputTransform extends PTransform<PCollection<HopRow>, PDone> {
  private final String transformName;
  private final ElasticsearchIO.Write write;
  private final String jsonField;
  private final String rowMetaXml;

  public BeamElasticsearchOutputTransform(
      String transformName, ElasticsearchIO.Write write, String jsonField, String rowMetaXml) {
    this.transformName = transformName;
    this.write = write;
    this.jsonField = jsonField;
    this.rowMetaXml = rowMetaXml;
  }

  private static class CountWritesFn extends DoFn<ElasticsearchIO.Document, Void> {
    private final String transformName;

    CountWritesFn(String transformName) {
      this.transformName = transformName;
    }

    @ProcessElement
    public void processElement() {
      Metrics.counter(org.apache.hop.pipeline.Pipeline.METRIC_NAME_WRITTEN, transformName).inc();
    }
  }

  @Override
  public PDone expand(PCollection<HopRow> input) {
    PCollectionTuple results =
        input
            .apply(
                "Extract JSON",
                ParDo.of(new ElasticsearchJsonFn(transformName, jsonField, rowMetaXml)))
            .setCoder(StringUtf8Coder.of())
            .apply("Write documents", write);
    results
        .get(ElasticsearchIO.Write.SUCCESSFUL_WRITES)
        .apply("Count written documents", ParDo.of(new CountWritesFn(transformName)));
    return PDone.in(input.getPipeline());
  }
}
