/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hop.lint;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.ICheckResultSource;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.workflow.action.ActionMeta;

/**
 * Decides how the linter reports a remark from Hop's own {@code check()} methods.
 *
 * <p>Those remarks were written for the Verify button, where a person asked the question and reads
 * every answer in context. The linter asks it unprompted, on every save and on every build, and a
 * remark it repeats is a remark it stands behind. The two are not the same claim, so the severity a
 * transform chose is not automatically the severity the linter reports: {@code check()} works from
 * a row stream inferred statically at design time, which is right for a plain pipeline and wrong
 * whenever metadata injection, a mapping or a runtime-populated stream is involved.
 *
 * <p>So native remarks pass through the rules like everything else. The core pack caps them at
 * warning and drops the two checks known to be incorrect; a project raises, lowers or silences them
 * by id in its own {@code hop-lint.yml}.
 *
 * @see <a href="https://github.com/apache/hop/issues/8294">#8294</a>
 */
public final class NativeCheckClassifier {

  /** What a matching rule says should happen to a remark. */
  public record Classification(String severity, String ruleId) {}

  private final List<CustomLintRule> rules;

  public NativeCheckClassifier(List<CustomLintRule> rules) {
    List<CustomLintRule> nativeRules = new ArrayList<>();
    if (rules != null) {
      for (CustomLintRule rule : rules) {
        if (rule != null && rule.isNativeVerify()) {
          nativeRules.add(rule);
        }
      }
    }
    this.rules = nativeRules;
  }

  /** Whether any rule speaks about native remarks at all. */
  public boolean isEmpty() {
    return rules.isEmpty();
  }

  /**
   * How to report one remark, or null when the rules say to drop it.
   *
   * <p>With no rule matching, the remark keeps the severity the transform gave it. That is what a
   * pack which says nothing about native checks should mean, and it is what the linter did before
   * the core pack had an opinion.
   */
  public Classification classify(ICheckResult remark) {
    if (remark == null) {
      return null;
    }
    CustomLintRule match = bestMatch(remark);
    if (match == null) {
      return new Classification(LintSeverity.fromCheckResultType(remark.getType()), null);
    }
    if (!match.isEnabled()) {
      return null;
    }
    return new Classification(match.getSeverity(), match.generateRuleId());
  }

  /**
   * The most specific rule that covers this remark.
   *
   * <p>Specificity is what lets the pack hold both a blanket "native remarks are warnings" and a
   * "this one check is wrong, drop it" without the order of the YAML deciding which wins. A rule
   * naming the check beats one naming only the plugin, which beats the blanket rule.
   */
  private CustomLintRule bestMatch(ICheckResult remark) {
    CustomLintRule best = null;
    int bestScore = -1;
    for (CustomLintRule rule : rules) {
      int score = score(rule, remark);
      if (score > bestScore) {
        best = rule;
        bestScore = score;
      }
    }
    return bestScore < 0 ? null : best;
  }

  /** How specifically the rule matches, or -1 when it does not apply. */
  private static int score(CustomLintRule rule, ICheckResult remark) {
    int score = 0;
    if (!rule.getAppliesTo().isEmpty()) {
      String pluginId = pluginIdOf(remark.getSourceInfo());
      if (Utils.isEmpty(pluginId) || !containsIgnoreCase(rule.getAppliesTo(), pluginId)) {
        return -1;
      }
      score += 1;
    }
    if (!Utils.isEmpty(rule.getMessageKey())) {
      if (!printsMessage(remark.getText(), rule.getMessageKey(), bundleClassOf(remark))) {
        return -1;
      }
      score += 2;
    }
    return score;
  }

  /**
   * Whether the remark is the one that message key prints.
   *
   * <p>Matching the resolved message rather than a pattern is what keeps this working outside
   * English: the key is resolved in the running locale, so the rule names the check itself instead
   * of naming the English words a check happens to use. A key that resolves to nothing — the plugin
   * is not installed, or the key was renamed — matches nothing rather than everything, so a stale
   * rule loses its narrowing instead of silencing every remark.
   *
   * @param text the remark as the transform built it, usually a heading followed by detail lines
   * @param messageKey {@code <i18n package>:<key>}, the same form Hop's own plugin annotations use
   * @param bundleClass the class whose class loader holds the bundle, or null
   */
  static boolean printsMessage(String text, String messageKey, Class<?> bundleClass) {
    if (Utils.isEmpty(text)) {
      return false;
    }
    int separator = messageKey.lastIndexOf(':');
    if (separator <= 0 || separator == messageKey.length() - 1) {
      return false;
    }
    String packageName = messageKey.substring(0, separator).trim();
    String key = messageKey.substring(separator + 1).trim();
    String message = resolve(packageName, key, bundleClass);
    if (Utils.isEmpty(message)) {
      return false;
    }
    return text.contains(message.trim());
  }

  /**
   * The message, or null when it cannot be resolved.
   *
   * <p>A transform in its own plugin folder has its own class loader, and the bundle is only on
   * that one, so the lookup goes through the class the remark came from first and falls back to the
   * core loader — the order the plugin registry uses to resolve the same {@code package:key} form
   * in a plugin's annotations.
   */
  private static String resolve(String packageName, String key, Class<?> bundleClass) {
    try {
      if (bundleClass != null) {
        String message = BaseMessages.getString(packageName, key, bundleClass);
        if (!unresolved(message)) {
          return message;
        }
      }
      String message = BaseMessages.getString(packageName, key);
      return unresolved(message) ? null : message;
    } catch (Exception e) {
      return null;
    }
  }

  /**
   * BaseMessages answers {@code !key!} for a key it cannot resolve. A rule whose key has been
   * renamed, or whose plugin is not installed, then matches nothing rather than everything: it
   * loses its narrowing instead of silencing every remark.
   */
  private static boolean unresolved(String message) {
    return Utils.isEmpty(message) || (message.startsWith("!") && message.endsWith("!"));
  }

  /** The class whose loader can see the bundle the remark's message came from. */
  private static Class<?> bundleClassOf(ICheckResult remark) {
    ICheckResultSource source = remark.getSourceInfo();
    if (source instanceof TransformMeta transformMeta && transformMeta.getTransform() != null) {
      return transformMeta.getTransform().getClass();
    }
    if (source instanceof ActionMeta actionMeta && actionMeta.getAction() != null) {
      return actionMeta.getAction().getClass();
    }
    return null;
  }

  private static String pluginIdOf(ICheckResultSource source) {
    if (source instanceof TransformMeta transformMeta) {
      return transformMeta.getTransformPluginId();
    }
    if (source instanceof ActionMeta actionMeta) {
      return actionMeta.getAction() != null ? actionMeta.getAction().getPluginId() : null;
    }
    return null;
  }

  private static boolean containsIgnoreCase(List<String> values, String candidate) {
    for (String value : values) {
      if (value != null && value.trim().equalsIgnoreCase(candidate)) {
        return true;
      }
    }
    return false;
  }
}
