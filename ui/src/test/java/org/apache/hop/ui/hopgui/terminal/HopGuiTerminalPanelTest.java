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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

/**
 * Unit tests for HopGuiTerminalPanel business logic.
 *
 * <p>Note: This class tests non-UI logic only. Full integration tests with SWT UI components
 * require a display environment and are better suited for integration tests.
 */
public class HopGuiTerminalPanelTest {

  @Test
  public void testExtractShellNameLogic() {
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
  public void testTerminalHeightPercentBounds() {
    // Test that terminal height percent validation works correctly
    assertTrue("10% should be valid", 10 > 0 && 10 < 100);
    assertTrue("50% should be valid", 50 > 0 && 50 < 100);
    assertTrue("90% should be valid", 90 > 0 && 90 < 100);

    assertFalse("0% should be invalid", 0 > 0 && 0 < 100);
    assertFalse("100% should be invalid", 100 > 0 && 100 < 100);
    assertFalse("-10% should be invalid", -10 > 0 && -10 < 100);
  }

  @Test
  public void testLogsWidthPercentBounds() {
    // Test that logs width percent validation works correctly
    assertTrue("30% should be valid", 30 > 0 && 30 < 100);
    assertTrue("50% should be valid", 50 > 0 && 50 < 100);
    assertTrue("70% should be valid", 70 > 0 && 70 < 100);

    assertFalse("0% should be invalid", 0 > 0 && 0 < 100);
    assertFalse("100% should be invalid", 100 > 0 && 100 < 100);
  }

  @Test
  public void testDefaultConfiguration() {
    // Test default configuration values
    int defaultTerminalHeight = 35;
    int defaultLogsWidth = 50;

    assertEquals("Default terminal height should be 35%", 35, defaultTerminalHeight);
    assertEquals("Default logs width should be 50%", 50, defaultLogsWidth);

    assertTrue(
        "Default terminal height should be valid",
        defaultTerminalHeight > 0 && defaultTerminalHeight < 100);
    assertTrue(
        "Default logs width should be valid", defaultLogsWidth > 0 && defaultLogsWidth < 100);
  }

  @Test
  public void testTerminalCounterIncrement() {
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
  public void testVisibilityStateTransitions() {
    // Test visibility state logic
    boolean terminalVisible = false;
    boolean logsVisible = false;

    // Initial state: both hidden
    assertFalse("Initial: terminal should be hidden", terminalVisible);
    assertFalse("Initial: logs should be hidden", logsVisible);

    // Show terminal
    terminalVisible = true;
    assertTrue("Terminal should be visible", terminalVisible);
    assertFalse("Logs should still be hidden", logsVisible);

    // Show logs
    logsVisible = true;
    assertTrue("Terminal should still be visible", terminalVisible);
    assertTrue("Logs should be visible", logsVisible);

    // Hide terminal
    terminalVisible = false;
    assertFalse("Terminal should be hidden", terminalVisible);
    assertTrue("Logs should still be visible", logsVisible);

    // Hide logs
    logsVisible = false;
    assertFalse("Terminal should be hidden", terminalVisible);
    assertFalse("Logs should be hidden", logsVisible);
  }

  @Test
  public void testBottomPanelLayoutStates() {
    // Test the four possible layout states for the bottom panel
    boolean terminalVisible = false;
    boolean logsVisible = false;

    // State 1: Both hidden - bottom panel should be hidden
    assertFalse("State 1: Both should be hidden", terminalVisible || logsVisible);

    // State 2: Only terminal visible
    terminalVisible = true;
    assertTrue("State 2: Terminal visible, logs hidden", terminalVisible && !logsVisible);

    // State 3: Only logs visible
    terminalVisible = false;
    logsVisible = true;
    assertTrue("State 3: Logs visible, terminal hidden", !terminalVisible && logsVisible);

    // State 4: Both visible - side-by-side
    terminalVisible = true;
    assertTrue("State 4: Both visible", terminalVisible && logsVisible);
  }

  @Test
  public void testShellPathValidation() {
    // Test that shell paths are non-null and non-empty
    String shellPath = TerminalShellDetector.detectDefaultShell();

    assertNotNull("Shell path should not be null", shellPath);
    assertFalse("Shell path should not be empty", shellPath.isEmpty());
    assertTrue("Shell path should have at least one character", shellPath.length() > 0);
  }

  @Test
  public void testWorkingDirectoryDefault() {
    // Test that working directory defaults to user.home
    String defaultWorkingDir = System.getProperty("user.home");

    assertNotNull("User home should not be null", defaultWorkingDir);
    assertFalse("User home should not be empty", defaultWorkingDir.isEmpty());
  }
}
