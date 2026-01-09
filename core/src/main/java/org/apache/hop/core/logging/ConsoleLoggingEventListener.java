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

package org.apache.hop.core.logging;

import org.apache.hop.core.Const;
import org.apache.hop.core.util.EnvUtil;

/**
 * Console logging event listener that writes log messages to stdout/stderr. Supports ANSI color
 * codes for error messages when configured.
 */
public class ConsoleLoggingEventListener implements IHopLoggingEventListener {

  // ANSI color codes for terminal output
  private static final String ANSI_RESET = "\u001B[0m";
  private static final String ANSI_RED = "\u001B[31m";

  private HopLogLayout layout;
  private boolean useColors;

  public ConsoleLoggingEventListener() {
    this.layout = new HopLogLayout(true);

    // Determine whether to use ANSI color codes based on configuration
    // Check both system property (-D flag) and environment variable
    String colorConfig = EnvUtil.getSystemProperty(Const.HOP_CONSOLE_COLORS);
    if (colorConfig == null) {
      colorConfig = System.getenv(Const.HOP_CONSOLE_COLORS);
    }
    if (colorConfig == null) {
      colorConfig = "auto"; // default
    }
    colorConfig = colorConfig.toLowerCase();

    if ("true".equals(colorConfig)) {
      // Always use colors
      this.useColors = true;
    } else if ("false".equals(colorConfig)) {
      // Never use colors
      this.useColors = false;
    } else {
      // Auto-detect: only use colors if output is to a terminal (not piped/redirected)
      // System.console() returns null when output is redirected to a file or pipe
      this.useColors = System.console() != null;
    }
  }

  @Override
  public void eventAdded(HopLoggingEvent event) {
    String logText = layout.format(event);

    if (event.getLevel() == LogLevel.ERROR) {
      // Apply red color to error messages if colors are enabled
      if (useColors) {
        logText = ANSI_RED + logText + ANSI_RESET;
      }
      HopLogStore.OriginalSystemErr.println(logText);
      HopLogStore.OriginalSystemErr.flush();
    } else {
      HopLogStore.OriginalSystemOut.println(logText);
      HopLogStore.OriginalSystemOut.flush();
    }
  }
}
