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

import org.eclipse.swt.custom.StyledText;
import org.eclipse.swt.widgets.Composite;

/**
 * Common interface for terminal widget implementations.
 *
 * <p>Implementations:
 *
 * <ul>
 *   <li>{@link SimpleTerminalWidget} - Simple ProcessBuilder-based console (reliable, basic)
 *   <li>{@link TerminalWidget} - Advanced PTY-based terminal (full shell, experimental)
 * </ul>
 */
public interface ITerminalWidget {

  /**
   * Get the terminal composite widget
   *
   * @return The SWT composite containing the terminal
   */
  Composite getTerminalComposite();

  /** Dispose of terminal resources */
  void dispose();

  /**
   * Check if terminal is connected/running
   *
   * @return true if terminal process is alive
   */
  boolean isConnected();

  /**
   * Get the shell path being used
   *
   * @return Path to shell executable (e.g. /bin/zsh)
   */
  String getShellPath();

  /**
   * Get the working directory
   *
   * @return Current working directory
   */
  String getWorkingDirectory();

  /**
   * Get the output text widget for focus/copy operations
   *
   * @return The StyledText widget showing output
   */
  StyledText getOutputText();

  /**
   * Get terminal type description
   *
   * @return "Simple" or "Advanced (PTY)"
   */
  String getTerminalType();
}
