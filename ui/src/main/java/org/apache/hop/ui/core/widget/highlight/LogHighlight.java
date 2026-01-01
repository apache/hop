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

package org.apache.hop.ui.core.widget.highlight;

import org.apache.hop.ui.core.gui.GuiResource;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.LineStyleEvent;
import org.eclipse.swt.custom.LineStyleListener;
import org.eclipse.swt.custom.StyleRange;

public class LogHighlight implements LineStyleListener {
  private static final String[] ERROR_KEYWORDS = {"ERROR", "EXCEPTION", "FATAL", "SEVERE"};
  private static final String[] WARNING_KEYWORDS = {"WARN", "WARNING", "CAUTION"};

  private StyleAttribute errorStyle;
  private StyleAttribute warningStyle;

  public LogHighlight() {
    initializeStyles();
  }

  void initializeStyles() {
    GuiResource resource = GuiResource.getInstance();
    // Red color for errors
    errorStyle = new StyleAttribute(GuiResource.getInstance().getColorRed(), SWT.NORMAL);
    // Orange/Yellow color for warnings
    warningStyle = new StyleAttribute(GuiResource.getInstance().getColorOrange(), SWT.NORMAL);
  }

  @Override
  public void lineGetStyle(LineStyleEvent event) {
    String lineText = event.lineText;
    if (lineText == null || lineText.isEmpty()) {
      event.styles = new StyleRange[0];
      return;
    }

    String upperLine = lineText.toUpperCase();

    // Check for error keywords
    for (String keyword : ERROR_KEYWORDS) {
      if (upperLine.contains(keyword)) {
        StyleRange styleRange =
            new StyleRange(
                event.lineOffset,
                lineText.length(),
                errorStyle.getForeground(),
                null,
                errorStyle.getStyle());
        event.styles = new StyleRange[] {styleRange};
        return;
      }
    }

    // Check for warning keywords
    for (String keyword : WARNING_KEYWORDS) {
      if (upperLine.contains(keyword)) {
        StyleRange styleRange =
            new StyleRange(
                event.lineOffset,
                lineText.length(),
                warningStyle.getForeground(),
                null,
                warningStyle.getStyle());
        event.styles = new StyleRange[] {styleRange};
        return;
      }
    }

    // No highlighting needed
    event.styles = new StyleRange[0];
  }
}
