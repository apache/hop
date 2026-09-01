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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.function.BiConsumer;
import java.util.function.Predicate;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Every linter option has to survive being written and read back.
 *
 * <p>These options were inert: the configuration perspective fills its widgets from a fresh plugin
 * instance and relies on a listener to save them, and no listener was implemented. Unchecking an
 * option changed nothing and reverted when the dialog closed.
 */
class LinterConfigPluginPersistenceTest {

  @AfterEach
  void restoreDefaults() {
    LinterConfigPlugin config = LinterConfigPlugin.getInstance();
    config.setLinterEnabled(true);
    config.setLintOnEditEnabled(true);
    config.setShowProblemsBarEnabled(true);
    config.setPreCommitLintEnabled(false);
    config.setPreCommitBlockWarnings(false);
    config.setPreCommitIncludeMetadata(true);
    config.setIncludeLintInPipelineVerify(true);
    config.setIncludeLintInWorkflowVerify(true);
    config.setIncludeNativeChecks(true);
    config.saveToHopConfig();
  }

  private static void assertRoundTrips(
      String option,
      BiConsumer<LinterConfigPlugin, Boolean> setter,
      Predicate<LinterConfigPlugin> getter) {
    for (boolean value : new boolean[] {false, true}) {
      LinterConfigPlugin config = LinterConfigPlugin.getInstance();
      setter.accept(config, value);
      config.saveToHopConfig();

      assertEquals(
          value,
          getter.test(LinterConfigPlugin.getInstance()),
          option + " must survive being saved and read back");
    }
  }

  @Test
  void everyOptionSurvivesASaveAndReload() {
    assertRoundTrips(
        "Enable linter", LinterConfigPlugin::setLinterEnabled, LinterConfigPlugin::isLinterEnabled);
    assertRoundTrips(
        "Lint on edit",
        LinterConfigPlugin::setLintOnEditEnabled,
        LinterConfigPlugin::isLintOnEditEnabled);
    assertRoundTrips(
        "Show lint indicators on canvas",
        LinterConfigPlugin::setShowProblemsBarEnabled,
        LinterConfigPlugin::isShowProblemsBarEnabled);
    assertRoundTrips(
        "Block git commits",
        LinterConfigPlugin::setPreCommitLintEnabled,
        LinterConfigPlugin::isPreCommitLintEnabled);
    assertRoundTrips(
        "Block on warnings",
        LinterConfigPlugin::setPreCommitBlockWarnings,
        LinterConfigPlugin::isPreCommitBlockWarnings);
    assertRoundTrips(
        "Include metadata in commit checks",
        LinterConfigPlugin::setPreCommitIncludeMetadata,
        LinterConfigPlugin::isPreCommitIncludeMetadata);
    assertRoundTrips(
        "Add lint to pipeline Verify",
        LinterConfigPlugin::setIncludeLintInPipelineVerify,
        LinterConfigPlugin::isIncludeLintInPipelineVerify);
    assertRoundTrips(
        "Add lint to workflow Verify",
        LinterConfigPlugin::setIncludeLintInWorkflowVerify,
        LinterConfigPlugin::isIncludeLintInWorkflowVerify);
    assertRoundTrips(
        "Include Hop's own checks",
        LinterConfigPlugin::setIncludeNativeChecks,
        LinterConfigPlugin::isIncludeNativeChecks);
  }

  /** The canvas overlay reads the setting live, so switching it off has to take effect at once. */
  @Test
  void switchingOffTheCanvasIndicatorsDisablesTheOverlay() {
    LinterConfigPlugin config = LinterConfigPlugin.getInstance();
    config.setShowProblemsBarEnabled(true);
    config.saveToHopConfig();
    assertTrue(LintCanvasOverlayHelper.isEnabled());

    config = LinterConfigPlugin.getInstance();
    config.setShowProblemsBarEnabled(false);
    config.saveToHopConfig();

    assertFalse(
        LintCanvasOverlayHelper.isEnabled(),
        "unchecking the option must stop the badges being drawn");
  }
}
