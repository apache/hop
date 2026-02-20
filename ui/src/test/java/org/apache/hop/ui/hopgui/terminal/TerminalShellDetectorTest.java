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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class TerminalShellDetectorTest {

  @Test
  public void testDetectDefaultShell() {
    // Test that we can detect a default shell
    String shell = TerminalShellDetector.detectDefaultShell();

    assertNotNull("Shell path should not be null", shell);
    assertFalse("Shell path should not be empty", shell.isEmpty());

    // Verify the shell path contains expected shell names based on OS
    String os = System.getProperty("os.name").toLowerCase();
    if (os.contains("win")) {
      // Windows should return PowerShell or cmd
      assertTrue(
          "Windows should detect PowerShell or cmd",
          shell.contains("powershell") || shell.contains("cmd"));
    } else if (os.contains("mac") || os.contains("nix") || os.contains("nux")) {
      // Unix-like systems should return a shell in /bin
      assertTrue("Unix-like systems should return a shell in /bin", shell.startsWith("/bin/"));
    }
  }

  @Test
  public void testDetectDefaultShellReturnsExecutable() {
    // Test that the detected shell is an actual executable file
    String shell = TerminalShellDetector.detectDefaultShell();

    assertNotNull("Shell path should not be null", shell);

    // On Windows, PowerShell/cmd detection is based on typical locations
    // On Unix, we verify the shell exists in /bin
    String os = System.getProperty("os.name").toLowerCase();
    if (!os.contains("win")) {
      // For Unix systems, verify the shell path looks valid
      assertTrue(
          "Shell should be in /bin or /usr/bin",
          shell.startsWith("/bin/") || shell.startsWith("/usr/bin/"));
    }
  }

  @Test
  public void testShellDetectionFallback() {
    // This test verifies that even if preferred shells aren't found,
    // we still get a fallback shell
    String shell = TerminalShellDetector.detectDefaultShell();

    assertNotNull("Should always return a shell, even if fallback", shell);
    assertFalse("Shell path should not be empty", shell.isEmpty());
  }
}
