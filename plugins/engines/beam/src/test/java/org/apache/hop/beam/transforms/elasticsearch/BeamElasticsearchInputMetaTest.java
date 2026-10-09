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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.beam.pipeline.IBeamPipelineTransformHandler;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.jupiter.api.Test;

class BeamElasticsearchInputMetaTest {
  @org.junit.jupiter.params.ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(strings = {"", " ", "${MISSING}"})
  void sourceRejectsEmptyOrUnresolvedOutputField(String field) {
    BeamElasticsearchInputMeta meta = new BeamElasticsearchInputMeta();
    meta.setJsonField(field);
    assertThrows(
        org.apache.hop.core.exception.HopTransformException.class,
        () -> meta.getFields(new RowMeta(), "elastic", null, null, new Variables(), null));
  }

  @org.junit.jupiter.params.ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(
      strings = {"not-json", "[]", "{} trailing", "{\"query\":{\"term\":{\"x\":\"${MISSING}\"}}}"})
  void rejectsInvalidOrUnresolvedQueryBeforeSubmittingGraph(String query) {
    BeamElasticsearchInputMeta meta = new BeamElasticsearchInputMeta();
    meta.setHosts("http://localhost:9200");
    meta.setIndex("documents");
    meta.setQuery(query);
    assertThrows(
        org.apache.hop.core.exception.HopException.class,
        () ->
            meta.handleTransform(
                null,
                new Variables(),
                "direct",
                null,
                null,
                null,
                new org.apache.hop.pipeline.PipelineMeta(),
                new org.apache.hop.pipeline.transform.TransformMeta(
                    "BeamElasticsearchInput", "read", meta),
                new java.util.HashMap<>(),
                BeamElasticsearchIOTest.pipeline(),
                new RowMeta(),
                java.util.List.of(),
                null,
                null));
  }

  @Test
  void sourceRejectsIncomingRowsRatherThanIgnoringThem() {
    BeamElasticsearchInputMeta meta = new BeamElasticsearchInputMeta();
    meta.setHosts("http://localhost:9200");
    meta.setIndex("docs");
    org.apache.beam.sdk.Pipeline pipeline = BeamElasticsearchIOTest.pipeline();
    org.apache.beam.sdk.values.PCollection<org.apache.hop.beam.core.HopRow> input =
        pipeline.apply(
            org.apache.beam.sdk.transforms.Create.empty(
                new org.apache.hop.beam.core.coder.HopRowCoder()));
    assertThrows(
        org.apache.hop.core.exception.HopException.class,
        () ->
            meta.handleTransform(
                null,
                new Variables(),
                "direct",
                null,
                null,
                null,
                new org.apache.hop.pipeline.PipelineMeta(),
                new TransformMeta("BeamElasticsearchInput", "read", meta),
                new java.util.HashMap<>(),
                pipeline,
                new RowMeta(),
                java.util.List.of(),
                input,
                null));
  }

  @Test
  void sourceProducesOneJsonStringFieldAndHasNoIncomingRows() throws Exception {
    BaseTransformMeta<?, ?> meta =
        (BaseTransformMeta<?, ?>)
            Class.forName("org.apache.hop.beam.transforms.elasticsearch.BeamElasticsearchInputMeta")
                .getConstructor()
                .newInstance();
    assertInstanceOf(IBeamPipelineTransformHandler.class, meta);
    assertFalse(meta.consumesMainInput());
    assertTrue(meta.canStartWithoutInput());
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("unrelated"));
    meta.getFields(rowMeta, "elastic", null, null, new Variables(), null);
    assertEquals(1, rowMeta.size());
    assertEquals("json", rowMeta.getValueMeta(0).getName());
    assertTrue(rowMeta.getValueMeta(0).isString());
    assertEquals("elastic", rowMeta.getValueMeta(0).getOrigin());
  }
}
