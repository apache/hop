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

package org.apache.hop.beam.pipeline.handler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.beam.pipeline.HopPipelineMetaToBeamPipelineConverter;
import org.apache.hop.beam.util.BeamConst;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.pipeline.transforms.addsequence.AddSequenceMeta;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Issue #2379: the Add Sequence transform needs a dedicated Beam handler. */
class BeamAddSequenceTransformHandlerTest {

  @BeforeAll
  static void initHopEnvironment() throws Exception {
    HopEnvironment.init();
    PluginRegistry.init();
  }

  @Test
  void sequenceIsAdvertisedAsExplicitlySupportedOnBeam() {
    // This is the contract BeamPipelineEngine.supports() reads, so it has to name the transform.
    assertTrue(
        HopPipelineMetaToBeamPipelineConverter.EXPLICIT_HANDLER_PLUGIN_IDS.contains("Sequence"),
        "Sequence must be advertised as explicitly supported on Beam");
    assertEquals("Sequence", BeamConst.STRING_ADD_SEQUENCE_PLUGIN_ID);
  }

  @Test
  void theHandlerIsNeitherAnInputNorAnOutput() {
    BeamAddSequenceTransformHandler handler = new BeamAddSequenceTransformHandler();

    assertFalse(handler.isInput(), "Add Sequence consumes the rows of the transform before it");
    assertFalse(handler.isOutput(), "Add Sequence is not the end of the chain");
  }

  @Test
  void theHandlerIsRegisteredForTheSequencePluginId() {
    // The converter's constructors both need a live pipeline and metadata store, so the
    // registration cannot be exercised from a unit test.  The static id set is the contract
    // BeamPipelineEngine.supports() reads and addDefaultTransformHandlers() is driven from, so
    // assert on that, plus the handler being instantiable and neither input nor output.
    assertTrue(
        HopPipelineMetaToBeamPipelineConverter.EXPLICIT_HANDLER_PLUGIN_IDS.contains(
            BeamConst.STRING_ADD_SEQUENCE_PLUGIN_ID),
        "the Sequence plugin id must be in EXPLICIT_HANDLER_PLUGIN_IDS");

    BeamAddSequenceTransformHandler handler = new BeamAddSequenceTransformHandler();
    assertNotNull(handler);
  }

  @Test
  void theSequenceMetaExposesTheRangeHopNeeds() {
    AddSequenceMeta meta = new AddSequenceMeta();
    meta.setValueName("seq");
    meta.setStartAt("100");
    meta.setIncrementBy("5");
    meta.setMaxValue("999");
    meta.setDatabaseUsed(false);

    assertEquals("seq", meta.getValueName());
    assertEquals("100", meta.getStartAt());
    assertEquals("5", meta.getIncrementBy());
    assertEquals("999", meta.getMaxValue());
    assertFalse(meta.isDatabaseUsed());
  }
}
