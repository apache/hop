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

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.function.Function;

/**
 * Resolves whether a given transform or action plugin is supported on a given engine, by combining
 * three sources in order of authority:
 *
 * <ol>
 *   <li>The engine's programmatic verdict (its {@code supports()} method). The engine has runtime
 *       ground truth and can express UNSUPPORTED reasons that depend on per-runner detail.
 *   <li>Compatibility fragments declared by other plugins — already merged into the plugin's own
 *       {@code getSupportedEngines()} / {@code getExcludedEngines()} by {@link
 *       IPlugin#merge(IPlugin)}, so they are read transparently from those getters.
 *   <li>The plugin's own annotation declarations ({@code @Transform.supportedEngines()} and
 *       {@code @Transform.excludedEngines()}, mirrored on {@code @Action}).
 * </ol>
 *
 * Combination rule: <b>any UNSUPPORTED wins</b> (most-restrictive). An engine declaring SUPPORTED
 * does not override an annotation/fragment that explicitly excludes — the author's explicit "no" is
 * honored. Conversely an engine's UNSUPPORTED overrides an annotation SUPPORTED — if the engine
 * physically cannot run the transform, declaring it supported does not help. When neither side has
 * an opinion the verdict is UNKNOWN, which is the backwards-compatible default for every existing
 * plugin (both annotation arrays empty + engines returning UNKNOWN by default).
 */
public final class EngineCompatibilityResolver {

  private EngineCompatibilityResolver() {}

  /**
   * @param plugin the transform or action plugin to check
   * @param engineId the id of the target engine plugin (e.g. "BeamDirectPipelineEngine", "Local")
   * @param engineCheck the engine's own {@code supports} function, or null if no engine is
   *     available (annotation-only mode, useful for palette filtering before run-config selection)
   * @return the combined verdict
   */
  public static EngineCompatibility resolve(
      IPlugin plugin, String engineId, Function<IPlugin, EngineCompatibility> engineCheck) {
    if (plugin == null || engineId == null) {
      return EngineCompatibility.unknown();
    }

    EngineCompatibility annotationVerdict = resolveAnnotation(plugin, engineId);
    EngineCompatibility engineVerdict =
        engineCheck == null ? EngineCompatibility.unknown() : engineCheck.apply(plugin);
    if (engineVerdict == null) {
      engineVerdict = EngineCompatibility.unknown();
    }

    if (annotationVerdict.isUnsupported()) {
      return annotationVerdict;
    }
    if (engineVerdict.isUnsupported()) {
      return engineVerdict;
    }
    if (engineVerdict.isSupported() || annotationVerdict.isSupported()) {
      return EngineCompatibility.supported();
    }
    return EngineCompatibility.unknown();
  }

  /**
   * Annotation-only verdict — useful when no engine instance is available (palette filtering, doc
   * generation).
   */
  public static EngineCompatibility resolveAnnotation(IPlugin plugin, String engineId) {
    if (plugin == null || engineId == null) {
      return EngineCompatibility.unknown();
    }
    String[] excluded = plugin.getExcludedEngines();
    if (matchesAny(excluded, engineId)) {
      return EngineCompatibility.unsupported("declared excluded");
    }
    String[] supported = plugin.getSupportedEngines();
    if (supported != null && supported.length > 0) {
      return matchesAny(supported, engineId)
          ? EngineCompatibility.supported()
          : EngineCompatibility.unsupported("not in supportedEngines");
    }
    return EngineCompatibility.unknown();
  }

  /**
   * Trailing-wildcard glob match. A pattern of {@code "Beam*"} matches any engine id starting with
   * {@code "Beam"}; the pattern {@code "*"} matches every engine id; literal patterns must match
   * exactly. Case-sensitive — engine ids are case-sensitive throughout Hop.
   */
  public static boolean matchesPattern(String pattern, String engineId) {
    if (pattern == null || engineId == null) {
      return false;
    }
    if (pattern.endsWith("*")) {
      return engineId.startsWith(pattern.substring(0, pattern.length() - 1));
    }
    return pattern.equals(engineId);
  }

  /** True iff any pattern in {@code patterns} matches {@code engineId}. */
  public static boolean matchesAny(String[] patterns, String engineId) {
    if (patterns == null || engineId == null) {
      return false;
    }
    for (String pattern : patterns) {
      if (matchesPattern(pattern, engineId)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Order-preserving union of two engine-id pattern arrays — used by {@link IPlugin#merge} so that
   * compatibility fragments add to (rather than replace) the host plugin's declared lists. Returns
   * an empty array if both inputs are null/empty.
   */
  public static String[] unionEngineIds(String[] a, String[] b) {
    Set<String> set = new LinkedHashSet<>();
    if (a != null) {
      Collections.addAll(set, a);
    }
    if (b != null) {
      Collections.addAll(set, b);
    }
    return set.toArray(new String[0]);
  }
}
