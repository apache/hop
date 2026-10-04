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

package org.apache.hop.ui.core.gui;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.abort;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

import java.awt.GraphicsEnvironment;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopRuntimeException;
import org.apache.hop.ui.hopgui.SessionDisplay;
import org.eclipse.swt.widgets.Display;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * A null namespace has to clear the display as well as the process. The daily build runs UI tests
 * in the same Surefire fork, and HopGui leaves that display and {@code HOP_PLATFORM_RUNTIME=GUI}
 * behind. Storing null in the display map crashes the build (issue #8752).
 */
@Tag("uitest")
class HopNamespaceDisplayTest {

  private String originalRuntime;
  private String previousNamespace;
  private Display createdDisplay;
  private boolean runtimeChanged;

  @BeforeEach
  void rememberRuntime() {
    originalRuntime = System.getProperty(Const.HOP_PLATFORM_RUNTIME);
    assumeFalse(
        GraphicsEnvironment.isHeadless(),
        "No display available (headless); skipping the namespace display test.");
    System.setProperty(Const.HOP_PLATFORM_RUNTIME, "GUI");
    runtimeChanged = true;
    try {
      previousNamespace = HopNamespace.getNamespace();
    } catch (RuntimeException e) {
      previousNamespace = null;
    }
  }

  @AfterEach
  void restoreRuntime() {
    if (!runtimeChanged) {
      return;
    }
    if (createdDisplay != null && !createdDisplay.isDisposed()) {
      createdDisplay.dispose();
      createdDisplay = null;
    }
    try {
      HopNamespace.setNamespace(previousNamespace);
    } catch (RuntimeException e) {
      // The display is gone, or there was nothing to restore.
    }
    if (originalRuntime == null) {
      System.clearProperty(Const.HOP_PLATFORM_RUNTIME);
    } else {
      System.setProperty(Const.HOP_PLATFORM_RUNTIME, originalRuntime);
    }
  }

  @Test
  void clearingTheNamespaceForgetsTheDisplayAndTheProcess() {
    ensureDisplay();

    HopNamespace.setNamespace("display-project");
    assertEquals("display-project", HopNamespace.getNamespace());

    HopNamespace.setNamespace(null);
    assertThrows(HopRuntimeException.class, HopNamespace::getNamespace);

    HopNamespace.setNamespace("");
    assertThrows(HopRuntimeException.class, HopNamespace::getNamespace);

    HopNamespace.setNamespace("display-project-again");
    assertEquals("display-project-again", HopNamespace.getNamespace());
  }

  /** The display of this thread, created when the UI tests have not already left one behind. */
  private void ensureDisplay() {
    Display display = SessionDisplay.current();
    if (display != null && !display.isDisposed()) {
      return;
    }
    try {
      createdDisplay = new Display();
    } catch (Throwable t) {
      abort("SWT display is not available: " + t.getMessage());
    }
  }
}
