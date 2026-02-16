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

package org.apache.hop.ui.hopgui.terminal;

import java.io.File;

/** Utility class to detect the default shell for the current operating system. */
public class TerminalShellDetector {

  /**
   * Detect the default shell for the current OS
   *
   * @return Full path to the shell executable
   */
  public static String detectDefaultShell() {
    String os = System.getProperty("os.name").toLowerCase();

    if (os.contains("win")) {
      return detectWindowsShell();
    } else if (os.contains("mac")) {
      return detectMacShell();
    } else {
      return detectLinuxShell();
    }
  }

  /** Detect shell on Windows */
  private static String detectWindowsShell() {
    String programFiles = System.getenv("PROGRAMFILES");
    if (programFiles != null) {
      String pwsh7 = programFiles + "\\PowerShell\\7\\pwsh.exe";
      if (new File(pwsh7).exists()) {
        return pwsh7;
      }
    }

    String windir = System.getenv("WINDIR");
    if (windir != null) {
      String powershell = windir + "\\System32\\WindowsPowerShell\\v1.0\\powershell.exe";
      if (new File(powershell).exists()) {
        return powershell;
      }
    }

    return "cmd.exe";
  }

  /** Detect shell on macOS */
  private static String detectMacShell() {
    String shell = System.getenv("SHELL");
    if (shell != null && !shell.isEmpty() && new File(shell).exists()) {
      return shell;
    }

    if (new File("/bin/zsh").exists()) {
      return "/bin/zsh";
    }

    if (new File("/bin/bash").exists()) {
      return "/bin/bash";
    }

    return "/bin/sh";
  }

  /** Detect shell on Linux/Unix */
  private static String detectLinuxShell() {
    String shell = System.getenv("SHELL");
    if (shell != null && !shell.isEmpty() && new File(shell).exists()) {
      return shell;
    }

    String[] commonShells = {
      "/bin/bash", "/usr/bin/bash", "/bin/zsh", "/usr/bin/zsh", "/bin/sh", "/usr/bin/sh"
    };

    for (String shellPath : commonShells) {
      if (new File(shellPath).exists()) {
        return shellPath;
      }
    }

    return "/bin/sh";
  }

  /**
   * Check if a shell path exists and is executable
   *
   * @param shellPath Path to shell executable
   * @return true if shell exists and can be executed
   */
  public static boolean isValidShell(String shellPath) {
    if (shellPath == null || shellPath.isEmpty()) {
      return false;
    }

    File shellFile = new File(shellPath);
    return shellFile.exists() && shellFile.canExecute();
  }

  /**
   * Get a list of available shells on the system
   *
   * @return Array of shell paths that exist on this system
   */
  public static String[] getAvailableShells() {
    String os = System.getProperty("os.name").toLowerCase();

    if (os.contains("win")) {
      return getWindowsShells();
    } else {
      return getUnixShells();
    }
  }

  private static String[] getWindowsShells() {
    java.util.List<String> shells = new java.util.ArrayList<>();

    // PowerShell 7
    String programFiles = System.getenv("PROGRAMFILES");
    if (programFiles != null) {
      String pwsh7 = programFiles + "\\PowerShell\\7\\pwsh.exe";
      if (new File(pwsh7).exists()) {
        shells.add(pwsh7);
      }
    }

    // PowerShell 5.x
    String windir = System.getenv("WINDIR");
    if (windir != null) {
      String powershell = windir + "\\System32\\WindowsPowerShell\\v1.0\\powershell.exe";
      if (new File(powershell).exists()) {
        shells.add(powershell);
      }
    }

    // cmd.exe
    shells.add("cmd.exe");

    // Git Bash (if installed)
    String[] gitBashPaths = {
      programFiles + "\\Git\\bin\\bash.exe",
      "C:\\Program Files\\Git\\bin\\bash.exe",
      "C:\\Program Files (x86)\\Git\\bin\\bash.exe"
    };
    for (String path : gitBashPaths) {
      if (new File(path).exists()) {
        shells.add(path);
        break; // Only add once
      }
    }

    return shells.toArray(new String[0]);
  }

  private static String[] getUnixShells() {
    java.util.List<String> shells = new java.util.ArrayList<>();

    String[] commonShells = {
      "/bin/bash",
      "/usr/bin/bash",
      "/bin/zsh",
      "/usr/bin/zsh",
      "/bin/fish",
      "/usr/bin/fish",
      "/bin/sh",
      "/usr/bin/sh",
      "/bin/dash",
      "/bin/ksh",
      "/bin/tcsh",
      "/bin/csh"
    };

    for (String shell : commonShells) {
      if (new File(shell).exists()) {
        shells.add(shell);
      }
    }

    return shells.toArray(new String[0]);
  }
}
