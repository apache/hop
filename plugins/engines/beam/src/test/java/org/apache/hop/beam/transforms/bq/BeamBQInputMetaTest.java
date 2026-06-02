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
 */

package org.apache.hop.beam.transforms.bq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.beam.pipeline.IBeamPipelineTransformHandler;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.pipeline.transform.TransformSerializationTestUtil;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Coverage for {@link BeamBQInputMeta} — XML round-trip via {@link TransformSerializationTestUtil},
 * the {@link IBeamPipelineTransformHandler} flags this transform exposes to the Beam converter, and
 * {@code getFields} adding declared columns to the output row meta.
 */
class BeamBQInputMetaTest {

  @BeforeAll
  static void initHopEnvironment() throws Exception {
    HopEnvironment.init();
    PluginRegistry.init();
  }

  @Test
  void xmlRoundTripPreservesEveryField() throws Exception {
    BeamBQInputMeta meta =
        TransformSerializationTestUtil.testSerialization(
            "/beam-bq-input-transform.xml", BeamBQInputMeta.class);

    assertEquals("test-project", meta.getProjectId());
    assertEquals("test_dataset", meta.getDatasetId());
    assertEquals("test_table", meta.getTableId());
    assertEquals("SELECT id, name FROM `test-project.test_dataset.test_table`", meta.getQuery());

    assertEquals(2, meta.getFields().size());
    BQField first = meta.getFields().get(0);
    assertEquals("id", first.getName());
    assertEquals("Integer", first.getHopType());

    BQField second = meta.getFields().get(1);
    assertEquals("name", second.getName());
    assertEquals("String", second.getHopType());
    assertEquals("renamed_name", second.getNewName());
  }

  @Test
  void isInputForBeamConverter() {
    assertTrue(new BeamBQInputMeta().isInput());
  }

  @Test
  void isNotOutputForBeamConverter() {
    assertFalse(new BeamBQInputMeta().isOutput());
  }

  @Test
  void implementsBeamPipelineTransformHandlerSoBeamCanRecognizeIt() {
    // BeamPipelineEngine.supports relies on this interface to surface SUPPORTED at design time.
    assertTrue(new BeamBQInputMeta() instanceof IBeamPipelineTransformHandler);
  }

  @Test
  void getFieldsAddsConfiguredColumnsToRowMeta() throws Exception {
    BeamBQInputMeta meta =
        TransformSerializationTestUtil.testSerialization(
            "/beam-bq-input-transform.xml", BeamBQInputMeta.class);
    IRowMeta rowMeta = new RowMeta();

    meta.getFields(rowMeta, "step", null, null, new Variables(), null);

    assertEquals(2, rowMeta.size());
    assertEquals("id", rowMeta.getValueMeta(0).getName());
    assertEquals(IValueMeta.TYPE_INTEGER, rowMeta.getValueMeta(0).getType());
    // Second field has a new_name override — the renamed value is what reaches downstream.
    assertEquals("renamed_name", rowMeta.getValueMeta(1).getName());
    assertEquals(IValueMeta.TYPE_STRING, rowMeta.getValueMeta(1).getType());
  }
}
