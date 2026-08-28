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

package org.apache.hop.web.it;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.web.it.pages.PipelineGraphPage;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvFileSource;

/**
 * Adds every transform in transforms.csv to a pipeline, opens its dialog, closes it and removes it
 * again.
 *
 * <p>Tagged {@code full} because it takes the better part of an hour: the daily job runs {@link
 * HopWebSmokeTest} instead and this sweep is started explicitly with {@code mvn -Pwebtest-full}.
 */
@Tag("full")
@DisplayName("Transform dialogs")
class TransformDialogSweepTest extends HopWebTestBase {

  private static PipelineGraphPage graph;

  @BeforeAll
  static void openPipeline() {
    graph = hopGui.newPipeline();
  }

  /**
   * Starts every case on an empty canvas.
   *
   * <p>A transform left behind by a failed case sits exactly where the next case clicks, so that
   * click opens the leftover's context dialog instead of the one that creates a transform - and
   * every remaining case then fails for a reason that has nothing to do with it. Rather than unpick
   * that, abandon the pipeline and carry on with a clean one.
   *
   * <p>This has to happen before the test rather than after it: JUnit runs a subclass
   * {@code @AfterEach} before the base class one, so cleaning up here would race the dialog closing
   * that {@link HopWebTestBase} does.
   */
  @BeforeEach
  void startFromACleanCanvas() {
    if (!hopGui.openDialogTitles().isEmpty()) {
      // A dialog that will not close would break every later case. Start a fresh session.
      HopWebEnvironment.get().reopenUi();
      hopGui.dismissWelcomeDialog();
      graph = hopGui.newPipeline();
      return;
    }
    if (!graph.isEmpty()) {
      graph = hopGui.newPipeline();
    }
  }

  @ParameterizedTest(name = "{0}")
  @CsvFileSource(resources = "/transforms.csv")
  void opensAndClosesTheDialog(String transformName) {
    graph.addTransform(transformName);
    assertTrue(graph.contains(transformName), "transform was not added to the graph");

    graph.actOnTransform(transformName, "Edit", 0, 0);
    String title = hopGui.awaitDialog();
    // Not asserted to equal the transform name: a dialog is free to call itself something else,
    // as "Fake" does with its "Fake data" dialog. Opening at all is the thing under test, and
    // actOnTransform has already confirmed the click landed on the right transform.
    assertNotNull(title, "no dialog opened for " + transformName);

    // Some transforms put a notice in front of their dialog, so close whatever is stacked up.
    hopGui.closeAllDialogs();
    assertTrue(
        hopGui.openDialogTitles().isEmpty(),
        "dialogs left open after " + transformName + ": " + hopGui.openDialogTitles());

    graph.actOnTransform(transformName, "Delete", 0, 0);
    wait.until(d -> !graph.contains(transformName));
    assertFalse(graph.contains(transformName), "transform was not removed from the graph");
  }
}
