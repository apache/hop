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
package org.apache.hop.beam.transforms.elasticsearch;

import static org.junit.jupiter.api.Assertions.*;

import java.util.HashMap;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hop.beam.core.HopRow;
import org.apache.hop.beam.core.coder.HopRowCoder;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class BeamElasticsearchOutputMetaTest {
  @ParameterizedTest
  @ValueSource(strings = {"", "${MISSING}", "missing", "number"})
  void rejectsMissingOrNonStringPayloadBeforeSubmittingGraph(String field) {
    BeamElasticsearchOutputMeta meta = configured();
    meta.setJsonField(field);
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("payload"));
    rowMeta.addValueMeta(new ValueMetaInteger("number"));
    assertThrows(HopException.class, () -> build(meta, rowMeta, false));
  }

  @Test
  void sinkRequiresIncomingRows() {
    assertThrows(HopException.class, () -> build(configured(), new RowMeta(), true));
  }

  static BeamElasticsearchOutputMeta configured() {
    BeamElasticsearchOutputMeta meta = new BeamElasticsearchOutputMeta();
    meta.setHosts("http://localhost:9200");
    meta.setIndex("documents");
    meta.setJsonField("payload");
    return meta;
  }

  static void build(BeamElasticsearchOutputMeta meta, RowMeta rowMeta, boolean noInput)
      throws Exception {
    Pipeline pipeline = BeamElasticsearchIOTest.pipeline();
    PCollection<HopRow> rows = noInput ? null : pipeline.apply(Create.empty(new HopRowCoder()));
    meta.handleTransform(
        null,
        new Variables(),
        "direct",
        null,
        null,
        null,
        new PipelineMeta(),
        new TransformMeta("BeamElasticsearchOutput", "write", meta),
        new HashMap<>(),
        pipeline,
        rowMeta,
        List.of(),
        rows,
        null);
  }
}
