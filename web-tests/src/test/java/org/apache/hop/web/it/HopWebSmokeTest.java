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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.web.it.pages.HopGuiPage;
import org.apache.hop.web.it.pages.PipelineGraphPage;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The daily signal: can Hop Web still be started, opened, and used to build a pipeline?
 *
 * <p>Every test starts from a pipeline of its own, so a failure says what actually broke instead of
 * knocking over everything that runs after it.
 */
@DisplayName("Hop Web smoke")
class HopWebSmokeTest extends HopWebTestBase {

  private static final String TRANSFORM = "Generate rows";

  @Test
  @DisplayName("the Hop GUI is served and its main toolbar is usable")
  void hopGuiIsUp() {
    assertNotNull(driver.findElement(HopGuiPage.NEW_FILE), "new file toolbar item");
    assertNotNull(driver.findElement(HopGuiPage.OPEN_FILE), "open file toolbar item");
    assertNotNull(driver.findElement(HopGuiPage.SAVE_FILE), "save file toolbar item");
    assertTrue(hopGui.openDialogTitles().isEmpty(), "no dialog should be blocking the GUI");
  }

  @Test
  @DisplayName("a new pipeline opens on an empty canvas")
  void createsAPipeline() {
    PipelineGraphPage graph = hopGui.newPipeline();

    assertNotNull(graph.canvas(), "pipeline graph canvas");
    assertTrue(hopGui.hasTab("New pipeline"), "a tab named 'New pipeline' should be open");
    // An empty graph draws only the hint telling you where to click, never a transform.
    assertFalse(
        graph.contains(TRANSFORM), "a new pipeline should be empty but drew " + graph.labels());
  }

  @Test
  @DisplayName("a transform can be dropped on the canvas from the context dialog")
  void addsATransform() {
    PipelineGraphPage graph = hopGui.newPipeline();

    graph.addTransform(TRANSFORM);

    assertTrue(
        graph.contains(TRANSFORM), "graph should show " + TRANSFORM + ", drew " + graph.labels());
  }

  @Test
  @DisplayName("a transform dialog opens and closes again")
  void opensTheTransformDialog() {
    PipelineGraphPage graph = hopGui.newPipeline();
    graph.addTransform(TRANSFORM);

    graph.actOnTransform(TRANSFORM, "Edit", 0, 0);

    assertEquals(TRANSFORM, hopGui.topDialogTitle(), "the transform dialog should be on top");

    hopGui.closeTopDialog();

    assertTrue(hopGui.openDialogTitles().isEmpty(), "the transform dialog should be gone");
  }

  @Test
  @DisplayName("a transform can be deleted again")
  void deletesATransform() {
    PipelineGraphPage graph = hopGui.newPipeline();
    graph.addTransform(TRANSFORM);

    graph.actOnTransform(TRANSFORM, "Delete", 0, 0);
    wait.until(d -> !graph.contains(TRANSFORM));

    assertFalse(graph.contains(TRANSFORM), "graph still shows " + graph.labels());
  }
}
