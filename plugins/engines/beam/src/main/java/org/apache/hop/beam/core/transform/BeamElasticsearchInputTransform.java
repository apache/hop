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

import org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hop.beam.core.HopRow;
import org.apache.hop.beam.core.coder.HopRowCoder;
import org.apache.hop.beam.core.fn.StringToHopRowFn;

/** Bounded JSON document source backed by Beam's Elasticsearch scroll reader. */
public class BeamElasticsearchInputTransform extends PTransform<PBegin, PCollection<HopRow>> {
  private final String transformName;
  private final ElasticsearchIO.Read read;
  private final String rowMetaJson;

  public BeamElasticsearchInputTransform(
      String transformName, ElasticsearchIO.Read read, String rowMetaJson) {
    this.transformName = transformName;
    this.read = read;
    this.rowMetaJson = rowMetaJson;
  }

  @Override
  public PCollection<HopRow> expand(PBegin input) {
    return input
        .apply("Read documents", read)
        .apply("JSON to Hop rows", ParDo.of(new StringToHopRowFn(transformName, rowMetaJson)))
        .setCoder(new HopRowCoder());
  }
}
