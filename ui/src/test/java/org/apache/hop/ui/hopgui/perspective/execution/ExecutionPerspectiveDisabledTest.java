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

package org.apache.hop.ui.hopgui.perspective.execution;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The Execution perspective can be switched off with an exclusion for its plugin id
 * ("150-HopExecutionPerspective") in disabledGuiElements.xml. HopGui#loadPerspectives() then skips
 * the perspective, so initialize() is never called on it. The singleton still exists, because the
 * class is instantiated to register the GUI elements it declares, which leaves callers that reach
 * it through getInstance() holding an instance without a HopGui or any widgets.
 *
 * <p>These tests pin down that such an instance stays inert instead of throwing a
 * NullPointerException. No display is needed: not calling initialize() is exactly what being
 * disabled amounts to.
 */
class ExecutionPerspectiveDisabledTest {

  /** A perspective as HopGui leaves it behind when it is disabled: constructed, not initialized. */
  private ExecutionPerspective disabledPerspective;

  @BeforeEach
  void createDisabledPerspective() {
    disabledPerspective = new ExecutionPerspective();

    // The constructor is what publishes the singleton, so every getInstance() caller in the code
    // base gets this uninitialized instance.
    assertSame(disabledPerspective, ExecutionPerspective.getInstance());
  }

  @Test
  void startingHopGuiDoesNotFail() {
    // HopGui#open restores the state of the perspective in its async open runnable.
    assertDoesNotThrow(() -> disabledPerspective.restoreState());
  }

  @Test
  void openingAProjectDoesNotFail() {
    // ProjectsGuiPlugin#enableHopGuiProject restores the state and refreshes when active.
    assertDoesNotThrow(
        () -> {
          disabledPerspective.restoreState();
          if (disabledPerspective.isActive()) {
            disabledPerspective.refresh();
          }
        });
  }

  @Test
  void closingAProjectDoesNotFail() {
    // ProjectsGuiPlugin#enableHopGuiProject and HopGuiFileDelegate#fileExit save the state.
    assertDoesNotThrow(() -> disabledPerspective.saveState());
  }

  @Test
  void closingAllTabsDoesNotFail() {
    // HopGuiFileDelegate#closeAllFiles closes execution tabs after file tabs.
    assertDoesNotThrow(() -> disabledPerspective.closeAllTabs());
  }

  @Test
  void theKeyboardShortcutDoesNotFail() {
    // The Ctrl-Shift-I shortcut on activate() is registered even though the perspective is not.
    assertDoesNotThrow(() -> disabledPerspective.activate());
  }

  @Test
  void aDisabledPerspectiveIsNeverActive() {
    assertFalse(disabledPerspective.isActive());
  }
}
