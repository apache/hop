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
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.pipeline.transform.TransformSerializationTestUtil;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Coverage for {@link BeamBQOutputMeta} — XML round-trip, the {@link IBeamPipelineTransformHandler}
 * flags exposed to the Beam converter, the default values set by {@link
 * BeamBQOutputMeta#setDefault()}, and the pass-through {@code getFields} contract (the local-engine
 * implementation forwards rows downstream so input row meta must survive).
 */
class BeamBQOutputMetaTest {

  @BeforeAll
  static void initHopEnvironment() throws Exception {
    HopEnvironment.init();
    PluginRegistry.init();
  }

  @Test
  void xmlRoundTripPreservesEveryField() throws Exception {
    BeamBQOutputMeta meta =
        TransformSerializationTestUtil.testSerialization(
            "/beam-bq-output-transform.xml", BeamBQOutputMeta.class);

    assertEquals("test-project", meta.getProjectId());
    assertEquals("test_dataset", meta.getDatasetId());
    assertEquals("test_table", meta.getTableId());
    assertTrue(meta.isCreatingIfNeeded());
    assertTrue(meta.isTruncatingTable());
    assertFalse(meta.isFailingIfNotEmpty());
  }

  @Test
  void isOutputForBeamConverter() {
    assertTrue(new BeamBQOutputMeta().isOutput());
  }

  @Test
  void isNotInputForBeamConverter() {
    assertFalse(new BeamBQOutputMeta().isInput());
  }

  @Test
  void implementsBeamPipelineTransformHandlerSoBeamCanRecognizeIt() {
    assertTrue(new BeamBQOutputMeta() instanceof IBeamPipelineTransformHandler);
  }

  @Test
  void setDefaultEnablesCreateIfNeeded() {
    BeamBQOutputMeta meta = new BeamBQOutputMeta();
    meta.setDefault();
    assertTrue(meta.isCreatingIfNeeded(), "new transforms default to auto-creating the table");
    assertFalse(meta.isTruncatingTable());
    assertFalse(meta.isFailingIfNotEmpty());
  }

  @Test
  void getFieldsIsPassThroughForLocalEngine() throws Exception {
    // On the local Hop engine the Output forwards input rows downstream (mirroring TableOutput),
    // so getFields must NOT clear the row meta — that was the bug fix on top of the original
    // Beam-only implementation where getFields cleared the row.
    BeamBQOutputMeta meta = new BeamBQOutputMeta();
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger("id"));
    rowMeta.addValueMeta(new ValueMetaString("name"));

    meta.getFields(rowMeta, "step", null, null, new Variables(), null);

    assertEquals(2, rowMeta.size(), "row meta must survive getFields untouched");
    assertEquals("id", rowMeta.getValueMeta(0).getName());
    assertEquals(IValueMeta.TYPE_INTEGER, rowMeta.getValueMeta(0).getType());
    assertEquals("name", rowMeta.getValueMeta(1).getName());
    assertEquals(IValueMeta.TYPE_STRING, rowMeta.getValueMeta(1).getType());
  }
}
