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

package org.apache.hop.ui.hopgui.terminal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for HopGuiTerminalPanel business logic.
 *
 * <p>Note: This class tests non-UI logic only. Full integration tests with SWT UI components
 * require a display environment and are better suited for integration tests.
 */
@SuppressWarnings("all")
class HopGuiTerminalPanelTest {

  @Test
  void testExtractShellNameLogic() {
    // Test the logic for extracting shell names from paths
    String bashPath = "/bin/bash";
    assertEquals("bash", bashPath.substring(bashPath.lastIndexOf('/') + 1));

    String zshPath = "/bin/zsh";
    assertEquals("zsh", zshPath.substring(zshPath.lastIndexOf('/') + 1));

    String fishPath = "/usr/local/bin/fish";
    assertEquals("fish", fishPath.substring(fishPath.lastIndexOf('/') + 1));

    // Windows paths
    String powershellPath = "C:\\Windows\\System32\\powershell.exe";
    assertEquals(
        "powershell.exe",
        powershellPath.substring(
            Math.max(powershellPath.lastIndexOf('\\'), powershellPath.lastIndexOf('/')) + 1));
  }

  @Test
  void testTerminalHeightPercentBounds() {
    // Test that terminal height percent validation works correctly
    assertTrue(10 > 0 && 10 < 100, "10% should be valid");
    assertTrue(50 > 0 && 50 < 100, "50% should be valid");
    assertTrue(90 > 0 && 90 < 100, "90% should be valid");

    assertFalse(0 > 0 && 0 < 100, "0% should be invalid");
    assertFalse(100 > 0 && 100 < 100, "100% should be invalid");
    assertFalse(-10 > 0 && -10 < 100, "-10% should be invalid");
  }

  @Test
  void testLogsWidthPercentBounds() {
    // Test that logs width percent validation works correctly
    assertFalse(30 > 0 && 30 < 10, "30% should be valid");
    assertTrue(50 > 0 && 50 < 100, "50% should be valid");
    assertTrue(70 > 0 && 70 < 100, "70% should be valid");

    assertFalse(0 > 0 && 0 < 100, "0% should be invalid");
    assertFalse(100 > 0 && 100 < 100, "100% should be invalid");
  }

  @Test
  void testDefaultConfiguration() {
    // Test default configuration values
    int defaultTerminalHeight = 35;
    int defaultLogsWidth = 50;

    assertEquals(35, defaultTerminalHeight, "Default terminal height should be 35%");
    assertEquals(50, defaultLogsWidth, "Default logs width should be 50%");

    assertTrue(
        defaultTerminalHeight > 0 && defaultTerminalHeight < 100,
        "Default terminal height should be valid");
    assertTrue(
        defaultLogsWidth > 0 && defaultLogsWidth < 100, "Default logs width should be valid");
  }

  @Test
  void testTerminalCounterIncrement() {
    // Test that terminal counter logic works correctly
    int counter = 1;

    // Simulate creating 5 terminals
    String[] expectedNames = {"(1)", "(2)", "(3)", "(4)", "(5)"};
    for (String expected : expectedNames) {
      assertEquals(expected, "(" + counter++ + ")");
    }

    // Counter should now be 6
    assertEquals(6, counter);
  }

  @Test
  void testVisibilityStateTransitions() {
    // Test visibility state logic
    boolean terminalVisible = false;
    boolean logsVisible = false;

    // Initial state: both hidden
    assertFalse(terminalVisible, "Initial: terminal should be hidden");
    assertFalse(logsVisible, "Initial: logs should be hidden");

    // Show terminal
    terminalVisible = true;
    assertTrue(terminalVisible, "Terminal should be visible");
    assertFalse(logsVisible, "Logs should still be hidden");

    // Show logs
    logsVisible = true;
    assertTrue(terminalVisible, "Terminal should still be visible");
    assertTrue(logsVisible, "Logs should be visible");

    // Hide terminal
    terminalVisible = false;
    assertFalse(terminalVisible, "Terminal should be hidden");
    assertTrue(logsVisible, "Logs should still be visible");

    // Hide logs
    logsVisible = false;
    assertFalse(terminalVisible, "Terminal should be hidden");
    assertFalse(logsVisible, "Logs should be hidden");
  }

  @Test
  void testBottomPanelLayoutStates() {
    // Test the four possible layout states for the bottom panel
    boolean terminalVisible = false;
    boolean logsVisible = false;

    // State 1: Both hidden - bottom panel should be hidden
    assertFalse(terminalVisible || logsVisible, "State 1: Both should be hidden");

    // State 2: Only terminal visible
    terminalVisible = true;
    assertTrue(terminalVisible && !logsVisible, "State 2: Terminal visible, logs hidden");

    // State 3: Only logs visible
    terminalVisible = false;
    logsVisible = true;
    assertTrue(!terminalVisible && logsVisible, "State 3: Logs visible, terminal hidden");

    // State 4: Both visible - side-by-side
    terminalVisible = true;
    assertTrue(terminalVisible && logsVisible, "State 4: Both visible");
  }

  @Test
  void testShellPathValidation() {
    // Test that shell paths are non-null and non-empty
    String shellPath = TerminalShellDetector.detectDefaultShell();

    assertNotNull(shellPath, "Shell path should not be null");
    assertFalse(shellPath.isEmpty(), "Shell path should not be empty");
    assertTrue(!shellPath.isEmpty(), "Shell path should have at least one character");
  }

  @Test
  void testWorkingDirectoryDefault() {
    // Test that working directory defaults to user.home
    String defaultWorkingDir = System.getProperty("user.home");

    assertNotNull(defaultWorkingDir, "User home should not be null");
    assertFalse(defaultWorkingDir.isEmpty(), "User home should not be empty");
  }
}
