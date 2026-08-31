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
package org.apache.hop.lint;

import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.ui.hopgui.HopGui;

/** GUI plugin entry point for lint problems bar interactions. */
@GuiPlugin(description = "Lint Problems Bar GUI Integration")
public class LintProblemsBarPlugin {

  private static final ILogChannel log = LogChannel.GENERAL;

  public static void showLintResults() {
    try {
      HopGui hopGui = HopGui.getInstance();
      if (hopGui != null && hopGui.getShell() != null) {
        String filename = LintEditorGraphHelper.getActiveEditorFilename();
        if (filename != null) {
          LintResultsUi.showResultsForFile(filename);
        } else {
          LintResultsUi.showResults();
        }
      }
    } catch (Exception e) {
      log.logError("Error showing lint results: " + e.getMessage(), e);
    }
  }
}
