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

package org.apache.hop.core.plugins;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.function.Function;
import org.junit.jupiter.api.Test;

class EngineCompatibilityResolverTest {

  // ─── matchesPattern ──────────────────────────────────────────────────────────

  @Test
  void matchesPattern_literalExactMatch() {
    assertTrue(EngineCompatibilityResolver.matchesPattern("Local", "Local"));
  }

  @Test
  void matchesPattern_literalCaseSensitive() {
    assertFalse(EngineCompatibilityResolver.matchesPattern("local", "Local"));
  }

  @Test
  void matchesPattern_literalNoMatch() {
    assertFalse(EngineCompatibilityResolver.matchesPattern("Local", "Remote"));
  }

  @Test
  void matchesPattern_trailingWildcardMatchesPrefix() {
    assertTrue(EngineCompatibilityResolver.matchesPattern("Beam*", "BeamDirectPipelineEngine"));
    assertTrue(EngineCompatibilityResolver.matchesPattern("Beam*", "BeamSparkPipelineEngine"));
  }

  @Test
  void matchesPattern_trailingWildcardDoesNotMatchUnrelated() {
    assertFalse(EngineCompatibilityResolver.matchesPattern("Beam*", "Local"));
  }

  @Test
  void matchesPattern_bareWildcardMatchesEverything() {
    assertTrue(EngineCompatibilityResolver.matchesPattern("*", "Local"));
    assertTrue(EngineCompatibilityResolver.matchesPattern("*", "BeamDirectPipelineEngine"));
  }

  @Test
  void matchesPattern_emptyPrefixWildcardMatchesEverything() {
    // pattern "" matches nothing, pattern "*" matches everything (covered above)
    assertFalse(EngineCompatibilityResolver.matchesPattern("", "Local"));
  }

  @Test
  void matchesPattern_nullSafe() {
    assertFalse(EngineCompatibilityResolver.matchesPattern(null, "Local"));
    assertFalse(EngineCompatibilityResolver.matchesPattern("Local", null));
    assertFalse(EngineCompatibilityResolver.matchesPattern(null, null));
  }

  // ─── matchesAny ──────────────────────────────────────────────────────────────

  @Test
  void matchesAny_secondPatternMatches() {
    assertTrue(
        EngineCompatibilityResolver.matchesAny(
            new String[] {"Local", "Beam*"}, "BeamDirectPipelineEngine"));
  }

  @Test
  void matchesAny_noPatternMatches() {
    assertFalse(EngineCompatibilityResolver.matchesAny(new String[] {"Local", "Remote"}, "Beam"));
  }

  @Test
  void matchesAny_emptyArrayReturnsFalse() {
    assertFalse(EngineCompatibilityResolver.matchesAny(new String[0], "Local"));
  }

  @Test
  void matchesAny_nullArrayReturnsFalse() {
    assertFalse(EngineCompatibilityResolver.matchesAny(null, "Local"));
  }

  // ─── unionEngineIds ──────────────────────────────────────────────────────────

  @Test
  void unionEngineIds_preservesOrderAndDeduplicates() {
    String[] result =
        EngineCompatibilityResolver.unionEngineIds(
            new String[] {"Local", "Beam*"}, new String[] {"Beam*", "Remote"});
    assertArrayEquals(new String[] {"Local", "Beam*", "Remote"}, result);
  }

  @Test
  void unionEngineIds_nullSafe() {
    assertArrayEquals(new String[0], EngineCompatibilityResolver.unionEngineIds(null, null));
    assertArrayEquals(
        new String[] {"a"}, EngineCompatibilityResolver.unionEngineIds(new String[] {"a"}, null));
    assertArrayEquals(
        new String[] {"a"}, EngineCompatibilityResolver.unionEngineIds(null, new String[] {"a"}));
  }

  // ─── resolveAnnotation ───────────────────────────────────────────────────────

  @Test
  void resolveAnnotation_noOpinionWhenBothEmpty() {
    IPlugin plugin = pluginWith(new String[0], new String[0]);
    assertEquals(
        EngineCompatibility.Verdict.UNKNOWN,
        EngineCompatibilityResolver.resolveAnnotation(plugin, "Local").getVerdict());
  }

  @Test
  void resolveAnnotation_excludedMatchUnsupported() {
    IPlugin plugin = pluginWith(new String[0], new String[] {"BeamSparkPipelineEngine"});
    EngineCompatibility verdict =
        EngineCompatibilityResolver.resolveAnnotation(plugin, "BeamSparkPipelineEngine");
    assertTrue(verdict.isUnsupported());
  }

  @Test
  void resolveAnnotation_excludedNoMatchUnknown() {
    IPlugin plugin = pluginWith(new String[0], new String[] {"BeamSparkPipelineEngine"});
    // not excluded from Direct → no opinion (UNKNOWN), engine may still decide
    assertEquals(
        EngineCompatibility.Verdict.UNKNOWN,
        EngineCompatibilityResolver.resolveAnnotation(plugin, "BeamDirectPipelineEngine")
            .getVerdict());
  }

  @Test
  void resolveAnnotation_supportedAllowListMatchSupported() {
    IPlugin plugin = pluginWith(new String[] {"Beam*"}, new String[0]);
    assertTrue(
        EngineCompatibilityResolver.resolveAnnotation(plugin, "BeamDirectPipelineEngine")
            .isSupported());
  }

  @Test
  void resolveAnnotation_supportedAllowListNoMatchUnsupported() {
    IPlugin plugin = pluginWith(new String[] {"Beam*"}, new String[0]);
    assertTrue(EngineCompatibilityResolver.resolveAnnotation(plugin, "Local").isUnsupported());
  }

  // ─── resolve (engine + annotation combination) ───────────────────────────────

  @Test
  void resolve_engineUnknownAnnotationSupportedYieldsSupported() {
    IPlugin plugin = pluginWith(new String[] {"Local"}, new String[0]);
    Function<IPlugin, EngineCompatibility> engine = p -> EngineCompatibility.unknown();
    assertTrue(EngineCompatibilityResolver.resolve(plugin, "Local", engine).isSupported());
  }

  @Test
  void resolve_engineSupportedAnnotationNoOpinionYieldsSupported() {
    IPlugin plugin = pluginWith(new String[0], new String[0]);
    Function<IPlugin, EngineCompatibility> engine = p -> EngineCompatibility.supported();
    assertTrue(EngineCompatibilityResolver.resolve(plugin, "Local", engine).isSupported());
  }

  @Test
  void resolve_engineUnknownAnnotationNoOpinionYieldsUnknown() {
    IPlugin plugin = pluginWith(new String[0], new String[0]);
    Function<IPlugin, EngineCompatibility> engine = p -> EngineCompatibility.unknown();
    assertEquals(
        EngineCompatibility.Verdict.UNKNOWN,
        EngineCompatibilityResolver.resolve(plugin, "Local", engine).getVerdict());
  }

  @Test
  void resolve_engineUnsupportedWinsOverAnnotationSupported() {
    IPlugin plugin = pluginWith(new String[] {"Local"}, new String[0]);
    Function<IPlugin, EngineCompatibility> engine =
        p -> EngineCompatibility.unsupported("engine refuses");
    EngineCompatibility verdict = EngineCompatibilityResolver.resolve(plugin, "Local", engine);
    assertTrue(verdict.isUnsupported());
    assertEquals("engine refuses", verdict.getReason());
  }

  @Test
  void resolve_annotationUnsupportedWinsOverEngineSupported() {
    // the author's explicit "don't run me on Spark" survives even if the engine thinks it could
    IPlugin plugin = pluginWith(new String[0], new String[] {"BeamSparkPipelineEngine"});
    Function<IPlugin, EngineCompatibility> engine = p -> EngineCompatibility.supported();
    EngineCompatibility verdict =
        EngineCompatibilityResolver.resolve(plugin, "BeamSparkPipelineEngine", engine);
    assertTrue(verdict.isUnsupported());
  }

  @Test
  void resolve_nullEngineCheckFallsBackToAnnotation() {
    IPlugin plugin = pluginWith(new String[] {"Local"}, new String[0]);
    assertTrue(EngineCompatibilityResolver.resolve(plugin, "Local", null).isSupported());
    assertTrue(EngineCompatibilityResolver.resolve(plugin, "BeamSpark", null).isUnsupported());
  }

  @Test
  void resolve_nullPluginYieldsUnknown() {
    assertEquals(
        EngineCompatibility.Verdict.UNKNOWN,
        EngineCompatibilityResolver.resolve(null, "Local", null).getVerdict());
  }

  // ─── helpers ─────────────────────────────────────────────────────────────────

  private static IPlugin pluginWith(String[] supported, String[] excluded) {
    IPlugin plugin = mock(IPlugin.class);
    when(plugin.getSupportedEngines()).thenReturn(supported);
    when(plugin.getExcludedEngines()).thenReturn(excluded);
    return plugin;
  }
}
