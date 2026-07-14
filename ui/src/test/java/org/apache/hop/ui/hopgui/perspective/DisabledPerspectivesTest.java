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

package org.apache.hop.ui.hopgui.perspective;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.ui.hopgui.perspective.configuration.ConfigurationPerspective;
import org.apache.hop.ui.hopgui.perspective.metadata.MetadataPerspective;
import org.junit.jupiter.api.Test;

/**
 * Any perspective can be switched off with an exclusion for its plugin id in
 * disabledGuiElements.xml. HopGui then skips it while loading the perspectives, so initialize() is
 * never called - but the singleton still exists, because the class is instantiated to register the
 * GUI elements it declares. Everything that reaches such a perspective through getInstance() gets
 * an instance with no HopGui, no tree and no tab folder.
 *
 * <p>These tests pin down that those instances stay inert instead of throwing a
 * NullPointerException. No display is needed: not calling initialize() is exactly what being
 * disabled amounts to.
 *
 * <p>The Execution perspective has its own test next to it; the git perspectives are covered in the
 * git plugin.
 */
class DisabledPerspectivesTest {

  /**
   * The metadata perspective is the one that hurts most: every metadata editor, every "edit" button
   * next to a metadata combo, and the audit log that restores tabs on startup all reach for it.
   */
  @Test
  void aDisabledMetadataPerspectiveStaysInert() {
    MetadataPerspective perspective = new MetadataPerspective();

    assertFalse(perspective.isActive());
    assertDoesNotThrow(
        () -> {
          perspective.activate();
          // HopGui#open restores the metadata tabs of the last session through the audit log.
          perspective.refresh();
          perspective.updateEditor(null);
          perspective.setActiveEditor(null);
        });
    assertNull(perspective.getActiveEditor());
    assertTrue(perspective.getItems().isEmpty());
    assertFalse(perspective.remove(null));
  }

  @Test
  void aDisabledConfigurationPerspectiveStaysInert() {
    ConfigurationPerspective perspective = new ConfigurationPerspective();

    assertFalse(perspective.isActive());
    assertDoesNotThrow(
        () -> {
          // The macOS Preferences menu item and the variable search results both land here.
          perspective.activate();
          perspective.showSystemVariablesTab();
        });
  }
}
