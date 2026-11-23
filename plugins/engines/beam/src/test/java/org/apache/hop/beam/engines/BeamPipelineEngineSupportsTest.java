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

package org.apache.hop.beam.engines;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;

import org.apache.hop.beam.engines.direct.BeamDirectPipelineEngine;
import org.apache.hop.beam.pipeline.HopPipelineMetaToBeamPipelineConverter;
import org.apache.hop.beam.pipeline.IBeamPipelineTransformHandler;
import org.apache.hop.beam.util.BeamConst;
import org.apache.hop.core.plugins.EngineCompatibility;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Verifies that {@code BeamPipelineEngine.supports} returns the same verdict the converter would
 * give at run time. Uses the concrete Direct engine — the override lives on the abstract parent so
 * any concrete subclass exercises the same path.
 */
class BeamPipelineEngineSupportsTest {

  private final BeamDirectPipelineEngine engine = new BeamDirectPipelineEngine();

  @Test
  void uniqueRowsIsHardBanned() {
    EngineCompatibility verdict = engine.supports(pluginWithIdAndMainType("Unique", Object.class));
    assertTrue(verdict.isUnsupported(), "Unique rows should be UNSUPPORTED");
    assertNotNull(verdict.getReason(), "reason should be set");
    assertEquals(
        HopPipelineMetaToBeamPipelineConverter.HARD_BANNED_PLUGIN_IDS.get("Unique"),
        verdict.getReason(),
        "reason should match the canonical ban message");
  }

  /**
   * The bans go by plugin id: in an installed Hop the meta class of a transform comes from its own
   * plugin class loader, so a ban by class never matched and these transforms ran with wrong
   * results.
   */
  @ParameterizedTest
  @ValueSource(
      strings = {
        "Unique",
        "SortRows",
        "JoinRows",
        "AnalyticQuery",
        "FieldsChangeSequence",
        "RepeatFields",
        "Flattener",
        "SortedMerge",
        "MergeRows",
        "MultiwayMergeJoin"
      })
  void transformsThatNeedOrderedRowsAreHardBanned(String pluginId) {
    EngineCompatibility verdict = engine.supports(pluginWithIdAndMainType(pluginId, Object.class));
    assertTrue(verdict.isUnsupported(), pluginId + " should be UNSUPPORTED");
    assertTrue(verdict.getReason().contains("not supported on Beam"), verdict.getReason());
  }

  /**
   * Transforms that need all their rows at once run on Beam, with the whole stream. So does Group
   * by without group fields: with group fields the converter refuses it, which depends on the
   * transform's settings, not on its plugin.
   */
  @ParameterizedTest
  @ValueSource(
      strings = {
        "GroupBy",
        "UniqueRowsByHashSet",
        "BlockingTransform",
        "DetectEmptyStream",
        "DetectLastRow",
        "ReservoirSampling",
        "SampleRows"
      })
  void wholeStreamTransformsAreNotBanned(String pluginId) {
    EngineCompatibility verdict = engine.supports(pluginWithIdAndMainType(pluginId, Object.class));
    assertFalse(verdict.isUnsupported(), pluginId + " should run on Beam");
  }

  @Test
  void maskFieldsPluginIdIsHardBanned() {
    // Ban by plugin id. Referencing MaskFieldsMeta here would put that class on the Beam engine
    // class loader, which is what broke every Beam run after the transform was added.
    EngineCompatibility verdict =
        engine.supports(
            pluginWithIdAndMainType(BeamConst.STRING_MASK_FIELDS_PLUGIN_ID, Object.class));
    assertTrue(verdict.isUnsupported(), "MaskFields should be UNSUPPORTED");
    assertEquals(
        HopPipelineMetaToBeamPipelineConverter.HARD_BANNED_PLUGIN_IDS.get(
            BeamConst.STRING_MASK_FIELDS_PLUGIN_ID),
        verdict.getReason());
  }

  @Test
  void mainTypeImplementingBeamHandlerIsSupported() {
    EngineCompatibility verdict = engine.supports(pluginWithMainType(StubBeamMeta.class));
    assertTrue(verdict.isSupported(), "transform meta implementing IBeamPipelineTransformHandler");
  }

  @Test
  void explicitHandlerPluginIdIsSupported() {
    // MergeJoin and RowGenerator are put into the converter's handler map by
    // addDefaultTransformHandlers — same set is exposed as EXPLICIT_HANDLER_PLUGIN_IDS.
    IPlugin plugin = pluginWithIdAndMainType(BeamConst.STRING_MERGE_JOIN_PLUGIN_ID, Object.class);
    assertTrue(engine.supports(plugin).isSupported());
  }

  @Test
  void unknownTransformFallsToUnknown() {
    // ordinary transform with no Beam wiring → converter would use generic ParDo, engine has no
    // opinion at design time
    EngineCompatibility verdict =
        engine.supports(pluginWithIdAndMainType("OrdinaryThing", Object.class));
    assertEquals(EngineCompatibility.Verdict.UNKNOWN, verdict.getVerdict());
  }

  @Test
  void nullPluginYieldsUnknown() {
    assertEquals(EngineCompatibility.Verdict.UNKNOWN, engine.supports(null).getVerdict());
  }

  // ─── helpers ─────────────────────────────────────────────────────────────────

  private static IPlugin pluginWithMainType(Class<?> mainType) {
    return pluginWithIdAndMainType("dummy-id", mainType);
  }

  private static IPlugin pluginWithIdAndMainType(String id, Class<?> mainType) {
    IPlugin plugin = mock(IPlugin.class);
    lenient().when(plugin.getIds()).thenReturn(new String[] {id});
    lenient().when(plugin.getMainType()).thenAnswer(inv -> mainType);
    return plugin;
  }

  /** Test double whose class is what BeamPipelineEngine.supports introspects (assignability). */
  @SuppressWarnings("unused")
  private abstract static class StubBeamMeta extends BaseTransformMeta
      implements IBeamPipelineTransformHandler {}
}
