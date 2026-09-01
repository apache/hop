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
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.web.it.pages.HopGuiPage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The shell around the editors: the perspective sidebar and the project and environment the GUI
 * says it is working in.
 *
 * <p>None of this used to be reachable from a test. The sidebar is a column of icons with no text,
 * the project and environment are icons with a label beside them, and the only thing that told them
 * apart was where they happened to sit on screen. They are addressed here by the names Hop Web puts
 * on them (see {@code TestIdFacade}).
 */
@DisplayName("Perspectives and project")
class HopWebNavigationTest extends HopWebTestBase {

  /** Where the GUI starts, and where every test here has to leave it. */
  private static final String EXPLORER = "explorer-perspective";

  private static final String METADATA = "metadata-perspective";

  private static final String CONFIGURATION = "configuration";

  /**
   * The perspective is session state: a test that left another one up would hand the next test a
   * GUI with no canvas in it.
   */
  @AfterEach
  void backToTheExplorer() {
    if (!EXPLORER.equals(hopGui.activePerspective())) {
      hopGui.switchToPerspective(EXPLORER);
    }
  }

  @Test
  @DisplayName("the GUI starts in the explorer perspective")
  void startsInTheExplorer() {
    assertEquals(EXPLORER, hopGui.activePerspective());
  }

  @Test
  @DisplayName("the sidebar switches perspective, and back again")
  void switchesPerspective() {
    hopGui.switchToPerspective(METADATA);
    assertEquals(METADATA, hopGui.activePerspective());

    hopGui.switchToPerspective(CONFIGURATION);
    assertEquals(CONFIGURATION, hopGui.activePerspective());

    hopGui.switchToPerspective(EXPLORER);
    assertEquals(EXPLORER, hopGui.activePerspective());
  }

  @Test
  @DisplayName("only one perspective is on screen at a time")
  void showsOnePerspective() {
    hopGui.switchToPerspective(METADATA);

    assertTrue(hopGui.isVisible(HopGuiPage.testId("perspective-content-" + METADATA)));
    assertFalse(
        hopGui.isVisible(HopGuiPage.testId("perspective-content-" + EXPLORER)),
        "the explorer perspective is still on screen behind the metadata one");
  }

  @Test
  @DisplayName("the bottom toolbar names the project being worked in")
  void namesTheProject() {
    // The container is configured with the default project and no environment; asserting on the
    // name rather than on "something is there" is what would catch a GUI that quietly lost track
    // of which project it has open.
    assertEquals("default", hopGui.projectName());
  }
}
