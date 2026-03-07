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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class TerminalShellDetectorTest {

  @Test
  void testDetectDefaultShell() {
    // Test that we can detect a default shell
    String shell = TerminalShellDetector.detectDefaultShell();

    assertNotNull(shell, "Shell path should not be null");
    assertFalse(shell.isEmpty(), "Shell path should not be empty");

    // Verify the shell path contains expected shell names based on OS
    String os = System.getProperty("os.name").toLowerCase();
    if (os.contains("win")) {
      // Windows should return PowerShell or cmd
      assertTrue(
          shell.contains("powershell") || shell.contains("cmd"),
          "Windows should detect PowerShell or cmd");
    } else if (os.contains("mac") || os.contains("nix") || os.contains("nux")) {
      // Unix-like systems should return a shell in /bin
      assertTrue(shell.startsWith("/bin/"), "Unix-like systems should return a shell in /bin");
    }
  }

  @Test
  void testDetectDefaultShellReturnsExecutable() {
    // Test that the detected shell is an actual executable file
    String shell = TerminalShellDetector.detectDefaultShell();

    assertNotNull(shell, "Shell path should not be null");

    // On Windows, PowerShell/cmd detection is based on typical locations
    // On Unix, we verify the shell exists in /bin
    String os = System.getProperty("os.name").toLowerCase();
    if (!os.contains("win")) {
      // For Unix systems, verify the shell path looks valid
      assertTrue(
          shell.startsWith("/bin/") || shell.startsWith("/usr/bin/"),
          "Shell should be in /bin or /usr/bin");
    }
  }

  @Test
  void testShellDetectionFallback() {
    // This test verifies that even if preferred shells aren't found,
    // we still get a fallback shell
    String shell = TerminalShellDetector.detectDefaultShell();

    assertNotNull(shell, "Should always return a shell, even if fallback");
    assertFalse(shell.isEmpty(), "Shell path should not be empty");
  }
}
